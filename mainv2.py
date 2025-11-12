import json
import boto3
import logging
import urllib.parse
import re
import pymysql  # Ensure pymysql is included in Lambda Layer or deployment package

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- RDS Configuration ---
RDS_HOST = "meter-db.cirfd7khe8qh.us-east-1.rds.amazonaws.com"
RDS_USER = "admin"
RDS_PASS = "12345678"  # Replace with Secrets Manager in production
RDS_NAME = "sach_meterdb"

# --- SNS Configuration ---
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:810751063582:meter-reading-alerts"
SMS_NUMBER = "+919566864646"
AWS_REGION = "us-east-1"

# --- AWS Clients ---
s3 = boto3.client('s3')
sns_client = boto3.client("sns", region_name=AWS_REGION)
sns_client.set_sms_attributes(attributes={'DefaultSMSType': 'Transactional'})
bedrock = boto3.client('bedrock-runtime', region_name=AWS_REGION)

# ----------------- Helper Functions -----------------
def send_sns_alert(customer_id, s3_key, reason, extracted_text=None):
    """Send both SNS topic (email) and direct SMS."""
    message = f"Customer: {customer_id}\nS3 Key: {s3_key}\nIssue: {reason}"
    if extracted_text:
        message += f"\nExtracted Text: {extracted_text}"

    # --- Send SNS Topic (email) ---
    try:
        resp_topic = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject="Meter Reading Alert"
        )
        logger.info(f"SNS Topic sent, MessageId: {resp_topic.get('MessageId')}")
    except Exception as e:
        logger.error(f"SNS Topic failed: {e}")

    # --- Send SMS explicitly ---
    try:
        resp_sms = sns_client.publish(
            PhoneNumber=SMS_NUMBER,
            Message=message
        )
        logger.info(f"SNS SMS sent, MessageId: {resp_sms.get('MessageId')}")
    except Exception as e:
        logger.error(f"SNS SMS failed: {e}")

def log_to_rds(customer_id, meter_value, s3_key):
    """Store successful readings in RDS MySQL."""
    try:
        conn = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASS,
            database=RDS_NAME,
            connect_timeout=5
        )
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS meter_readings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_id VARCHAR(255),
                    meter_reading FLOAT,
                    s3_key VARCHAR(512),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            cur.execute(
                "INSERT INTO meter_readings (customer_id, meter_reading, s3_key) VALUES (%s, %s, %s)",
                (customer_id, meter_value, s3_key)
            )
            conn.commit()
        conn.close()
        logger.info(f"[RDS] Logged reading for customer {customer_id}")
    except Exception as e:
        logger.error(f"RDS logging failed: {e}")
        send_sns_alert(customer_id, s3_key, "RDS logging failed")

def extract_text_from_bedrock_response(result):
    """Extract readable text from Bedrock response."""
    if isinstance(result, str):
        return result.strip()
    try:
        if isinstance(result, dict):
            for key in ('content', 'outputs', 'messages', 'generatedText', 'generated_text', 'text', 'body'):
                val = result.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
                if isinstance(val, list) and len(val) > 0:
                    first = val[0]
                    if isinstance(first, dict) and 'text' in first:
                        return str(first['text']).strip()
                    if isinstance(first, str):
                        return first.strip()
            # recursive
            for v in result.values():
                if isinstance(v, dict) or isinstance(v, list):
                    txt = extract_text_from_bedrock_response(v)
                    if txt:
                        return txt
    except Exception:
        pass
    try:
        return str(result).strip()
    except Exception:
        return ""

def find_numeric_in_text(text):
    """Find first numeric-looking token in text."""
    if not text:
        return None
    m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)', text)
    if m:
        return m.group(1)
    return None

# ----------------- Lambda Handler -----------------
def lambda_handler(event, context):
    try:
        logger.info("=" * 80)
        logger.info("METER READING LAMBDA STARTED")
        logger.info("=" * 80)

        model = "us.amazon.nova-lite-v1:0"

        if 'Records' not in event or len(event['Records']) == 0:
            send_sns_alert("UNKNOWN", "UNKNOWN", "No S3 Records in event")
            return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}

        rec = event['Records'][0]
        bucket = rec['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(rec['s3']['object']['key'])
        customer_id = key.split('/')[0] if '/' in key else key

        logger.info(f"[S3] Bucket: {bucket}, Key: {key}, Customer: {customer_id}")

        # Detect format
        try:
            obj = s3.head_object(Bucket=bucket, Key=key)
            ctype = obj.get('ContentType', 'image/jpeg') or 'image/jpeg'
            if 'jpeg' in ctype.lower() or 'jpg' in ctype.lower():
                fmt = 'jpeg'
            elif 'png' in ctype.lower():
                fmt = 'png'
            elif 'webp' in ctype.lower():
                fmt = 'jpeg'  # fallback for unsupported webp
            else:
                fmt = 'jpeg'
        except Exception:
            fmt = 'jpeg'

        s3_uri = f"s3://{bucket}/{key}"
        logger.info(f"[S3 URI] {s3_uri}")

        # Prepare Bedrock payload
        payload = {
            "schemaVersion": "messages-v1",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "text": (
                                "Extract ONLY the current kWh reading shown on the digital (or mechanical) display. "
                                "Return ONLY the numeric reading used for billing. "
                                "If the display is unclear, return EXACTLY 'UNCLEAR'."
                            )
                        },
                        {
                            "image": {
                                "format": fmt,
                                "source": {"s3Location": {"uri": s3_uri}}
                            }
                        }
                    ]
                }
            ],
            "inferenceConfig": {"temperature": 0.1, "maxTokens": 200}
        }

        # Invoke Bedrock
        try:
            resp = bedrock.invoke_model(
                modelId=model,
                body=json.dumps(payload),
                contentType="application/json",
                accept="application/json"
            )
            body_bytes = b""
            try:
                body_stream = resp.get('body')
                if body_stream is not None:
                    body_bytes = body_stream.read()
            except Exception:
                body_bytes = b""
            result = None
            try:
                body_text = body_bytes.decode('utf-8') if isinstance(body_bytes, (bytes, bytearray)) else str(body_bytes)
                result = json.loads(body_text)
            except Exception:
                result = body_bytes.decode('utf-8', errors='ignore') if body_bytes else ""
        except Exception as e:
            logger.error(f"Bedrock invocation failed: {e}")
            send_sns_alert(customer_id, key, "Bedrock invocation failed")
            return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}

        extracted = extract_text_from_bedrock_response(result)
        logger.info(f"[EXTRACTED RAW] {extracted}")

        if not extracted or "UNCLEAR" in extracted.upper():
            send_sns_alert(customer_id, key, "Meter reading unclear or unreadable", extracted)
            return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}

        num_candidate = None
        try:
            cleaned = extracted.replace(',', '').strip()
            num_candidate = float(cleaned)
        except Exception:
            numeric_text = find_numeric_in_text(extracted)
            if numeric_text:
                try:
                    num_candidate = float(numeric_text.replace(',', ''))
                except Exception:
                    num_candidate = None

        if num_candidate is None:
            send_sns_alert(customer_id, key, "Failed to parse meter reading", extracted)
            return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}

        value = float(num_candidate)
        reading_str = f"{value} kWh"
        logger.info(f"✓ SUCCESS! METER READING = {reading_str} ✓")

        log_to_rds(customer_id, value, key)

        return {
            "statusCode": 200,
            "status": "SUCCESS",
            "customer_id": customer_id,
            "meter_reading": reading_str,
            "s3_key": key
        }

    except Exception as e:
        logger.error(f"Unexpected Lambda error: {e}")
        send_sns_alert("UNKNOWN", "UNKNOWN", "Unexpected Lambda error")
        return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}
