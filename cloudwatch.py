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
    print("1, line 30")
    """Send both SNS topic (email) and direct SMS."""
    message = f"Customer: {customer_id}\nS3 Key: {s3_key}\nIssue: {reason}"
    if extracted_text:
        message += f"\nExtracted Text: {extracted_text}"
        print("2, line 34")

    # --- Send SNS Topic (email) ---
    try:
        print("3, line 38")
        resp_topic = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject="Meter Reading Alert"
        )
        print("4, line 44")
        logger.info(f"SNS Topic sent, MessageId: {resp_topic.get('MessageId')}")
    except Exception as e:
        print("5, line 47")
        logger.error(f"SNS Topic failed: {e}")

    # --- Send SMS explicitly ---
    try:
        print("6, line 52")
        resp_sms = sns_client.publish(
            PhoneNumber=SMS_NUMBER,
            Message=message
        )
        print("7, line 57")
        logger.info(f"SNS SMS sent, MessageId: {resp_sms.get('MessageId')}")
    except Exception as e:
        print("8, line 60")
        logger.error(f"SNS SMS failed: {e}")

def log_to_rds(customer_id, meter_value, s3_key):
    print("9, line 66")
    """Store successful readings in RDS MySQL."""
    try:
        print("10, line 69")
        conn = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASS,
            database=RDS_NAME,
            connect_timeout=5
        )
        with conn.cursor() as cur:
            print("11, line 78")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS meter_readings (
                    reading_id INT AUTO_INCREMENT PRIMARY KEY,
                    cust_id VARCHAR(50),
                    meter_reading DECIMAL(10,2),
                    reading_date DATE,
                    image_s3_key VARCHAR(500),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(20),
                    valid VARCHAR(20),
                    customer_id VARCHAR(50)
                )
            """)
            print("12, line 95")
            conn.commit()

            cur.execute(
                """
                INSERT INTO meter_readings
                (cust_id, customer_id, meter_reading, image_s3_key, reading_date, processed_at, status)
                VALUES (%s, %s, %s, %s, CURDATE(), NOW(), %s)
                """,
                (customer_id, customer_id, meter_value, s3_key, 'valid')
            )
            print("13, line 121")
            conn.commit()
        conn.close()
        print("14, line 125")
        logger.info(f"[RDS] Logged reading for customer {customer_id}")
    except Exception as e:
        print("15, line 128")
        logger.error(f"RDS logging failed: {e}")
        send_sns_alert(customer_id, s3_key, "RDS logging failed")

def extract_text_from_bedrock_response(result):
    print("16, line 133")
    """Extract readable text from Bedrock response."""
    if isinstance(result, str):
        print("17, line 136")
        return result.strip()
    try:
        print("18, line 139")
        if isinstance(result, dict):
            print("19, line 141")
            for key in ('content', 'outputs', 'messages', 'generatedText', 'generated_text', 'text', 'body'):
                print("20, line 143")
                val = result.get(key)
                if isinstance(val, str) and val.strip():
                    print("21, line 146")
                    return val.strip()
                if isinstance(val, list) and len(val) > 0:
                    print("22, line 149")
                    first = val[0]
                    if isinstance(first, dict) and 'text' in first:
                        print("23, line 152")
                        return str(first['text']).strip()
                    if isinstance(first, str):
                        print("24, line 155")
                        return first.strip()
            for v in result.values():
                print("25, line 159")
                if isinstance(v, dict) or isinstance(v, list):
                    print("26, line 161")
                    txt = extract_text_from_bedrock_response(v)
                    if txt:
                        print("27, line 164")
                        return txt
    except Exception:
        print("28, line 167")
        pass
    try:
        print("29, line 170")
        return str(result).strip()
    except Exception:
        print("30, line 173")
        return ""

def find_numeric_in_text(text):
    print("31, line 177")
    """Find first numeric-looking token in text."""
    if not text:
        print("32, line 180")
        return None
    m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)', text)
    print("33, line 183")
    if m:
        print("34, line 185")
        return m.group(1)
    return None

# ----------------- NEW FUNCTION -----------------
def check_anomaly_and_validate(customer_id, current_value, s3_key, override=False):
    print("A1: Checking anomaly")
    """Check last 3 readings and detect 50% difference anomaly."""
    try:
        conn = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASS,
            database=RDS_NAME,
            connect_timeout=5
        )
        with conn.cursor() as cur:
            print("A2: Fetching previous readings")
            cur.execute("""
                SELECT meter_reading 
                FROM meter_readings 
                WHERE customer_id = %s 
                ORDER BY processed_at DESC 
                LIMIT 3
            """, (customer_id,))
            prev_readings = [float(row[0]) for row in cur.fetchall() if row[0] is not None]
        conn.close()

        if not prev_readings:
            print("A3: No previous readings found, skipping anomaly check")
            return True

        last_value = prev_readings[0]
        diff_percent = abs(current_value - last_value) / max(last_value, 1) * 100
        print(f"A4: Difference = {diff_percent:.2f}%")

        if diff_percent >= 50 and not override:
            print("A5: Anomaly detected!")
            reason = f"⚠️ Anomaly Detected: Current reading ({current_value} kWh) differs by {diff_percent:.1f}% from last reading ({last_value} kWh)."
            send_sns_alert(customer_id, s3_key, reason)
            return False
        else:
            print("A6: Reading within range or override enabled")
            return True

    except Exception as e:
        print("A7: Anomaly check failed", e)
        logger.error(f"Anomaly check failed: {e}")
        return True

# ----------------- Lambda Handler -----------------
def lambda_handler(event, context):
    print("35, line 191")
    try:
        print("36, line 193")
        logger.info("=" * 80)
        logger.info("METER READING LAMBDA STARTED")
        logger.info("=" * 80)

        model = "us.amazon.nova-lite-v1:0"
        print("37, line 199")

        if 'Records' not in event or len(event['Records']) == 0:
            print("38, line 202")
            send_sns_alert("UNKNOWN", "UNKNOWN", "No S3 Records in event")
            return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}

        rec = event['Records'][0]
        bucket = rec['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(rec['s3']['object']['key'])
        customer_id = key.split('/')[0] if '/' in key else key
        print("39, line 210")

        logger.info(f"[S3] Bucket: {bucket}, Key: {key}, Customer: {customer_id}")

        try:
            print("40, line 216")
            obj = s3.head_object(Bucket=bucket, Key=key)
            ctype = obj.get('ContentType', 'image/jpeg') or 'image/jpeg'
            if 'jpeg' in ctype.lower() or 'jpg' in ctype.lower():
                print("41, line 221")
                fmt = 'jpeg'
            elif 'png' in ctype.lower():
                print("42, line 224")
                fmt = 'png'
            elif 'webp' in ctype.lower():
                print("43, line 227")
                fmt = 'jpeg'
            else:
                print("44, line 230")
                fmt = 'jpeg'
        except Exception:
            print("45, line 233")
            fmt = 'jpeg'

        s3_uri = f"s3://{bucket}/{key}"
        print("46, line 236")
        logger.info(f"[S3 URI] {s3_uri}")

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
        print("47, line 264")

        try:
            print("48, line 267")
            resp = bedrock.invoke_model(
                modelId=model,
                body=json.dumps(payload),
                contentType="application/json",
                accept="application/json"
            )
            print("49, line 274")
            body_bytes = b""
            try:
                print("50, line 277")
                body_stream = resp.get('body')
                if body_stream is not None:
                    print("51, line 280")
                    body_bytes = body_stream.read()
            except Exception:
                print("52, line 283")
                body_bytes = b""
            result = None
            try:
                print("53, line 287")
                body_text = body_bytes.decode('utf-8') if isinstance(body_bytes, (bytes, bytearray)) else str(body_bytes)
                result = json.loads(body_text)
            except Exception:
                print("54, line 291")
                result = body_bytes.decode('utf-8', errors='ignore') if body_bytes else ""
        except Exception as e:
            print("55, line 295")
            logger.error(f"Bedrock invocation failed: {e}")
            send_sns_alert(customer_id, key, "Bedrock invocation failed")
            return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}

        extracted = extract_text_from_bedrock_response(result)
        print("56, line 301")
        logger.info(f"[EXTRACTED RAW] {extracted}")

        if not extracted or "UNCLEAR" in extracted.upper():
            print("57, line 305")
            send_sns_alert(customer_id, key, "Meter reading unclear or unreadable", extracted)
            return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}

        num_candidate = None
        try:
            print("58, line 311")
            cleaned = extracted.replace(',', '').strip()
            num_candidate = float(cleaned)
        except Exception:
            print("59, line 315")
            numeric_text = find_numeric_in_text(extracted)
            if numeric_text:
                print("60, line 318")
                try:
                    num_candidate = float(numeric_text.replace(',', ''))
                except Exception:
                    print("61, line 321")
                    num_candidate = None

        if num_candidate is None:
            print("62, line 325")
            send_sns_alert(customer_id, key, "Failed to parse meter reading", extracted)
            return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}

        value = float(num_candidate)
        reading_str = f"{value} kWh"
        print("63, line 331")
        logger.info(f"✓ SUCCESS! METER READING = {reading_str} ✓")

        # ---------------- ANOMALY CHECK INSERTED HERE ----------------
        if not check_anomaly_and_validate(customer_id, value, key):
            return {"statusCode": 400, "status": "ANOMALY", "message": "Anomaly detected — please verify or override to continue"}
        # -------------------------------------------------------------

        log_to_rds(customer_id, value, key)
        print("64, line 335")

        return {
            "statusCode": 200,
            "status": "SUCCESS",
            "customer_id": customer_id,
            "meter_reading": reading_str,
            "s3_key": key
        }

    except Exception as e:
        print("65, line 344")
        logger.error(f"Unexpected Lambda error: {e}")
        send_sns_alert("UNKNOWN", "UNKNOWN", "Unexpected Lambda error")
        return {"statusCode": 400, "status": "UNCLEAR", "message": "Image not clear, please retake"}
