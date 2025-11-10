import json
import boto3
import logging
import urllib.parse
import re
import pymysql  # Ensure pymysql is available (Lambda Layer or included in package)



logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- RDS Configuration (replace with secure secret storage in prod) ---
RDS_HOST = "meter-db.cirfd7khe8qh.us-east-1.rds.amazonaws.com"
RDS_USER = "admin"
RDS_PASS = "12345678"  # Replace with actual password (use Secrets Manager in prod)
RDS_NAME = "sach_meterdb"

# --- SNS Configuration ---
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:810751063582:meter-reading-alerts"

# --- AWS Clients ---
bedrock = boto3.client('bedrock-runtime')
s3 = boto3.client('s3')
# <-- SNS fixed: explicitly specify the region
sns = boto3.client('sns', region_name="us-east-1")  # <--- ONLY FIXED THIS LINE

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
        logger.error(f"[RDS ERROR] {e}", exc_info=True)

def send_sns_notification(customer_id, s3_key, message):
    """Send notification via SNS. Logs full response and raises on suspicious/no response."""
    try:
        resp = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=f"Customer: {customer_id}\nS3 Key: {s3_key}\nIssue: {message}",
            Subject="Meter Reading Alert"
        )

        # Log full response for debugging
        logger.info(f"[SNS] publish response raw: {json.dumps(resp, default=str)}")

        # Basic sanity checks: AWS SDK should return a MessageId on success
        msg_id = resp.get('MessageId') if isinstance(resp, dict) else None
        if not msg_id:
            raise RuntimeError(f"SNS publish returned no MessageId: {resp}")

        logger.info(f"[SNS] Notification sent successfully. MessageId={msg_id}")
        return resp

    except Exception as e:
        logger.error(f"[SNS ERROR] Failed to send notification for customer {customer_id}, s3_key {s3_key}: {str(e)}", exc_info=True)
        raise

def extract_text_from_bedrock_response(result):
    """
    Robust extraction of textual answer from a variety of Bedrock response shapes.
    """
    if isinstance(result, str):
        return result.strip()

    try:
        if isinstance(result, dict):
            if 'content' in result and isinstance(result['content'], list) and len(result['content']) > 0:
                first = result['content'][0]
                if isinstance(first, dict) and 'text' in first:
                    return str(first['text']).strip()
                if isinstance(first, str):
                    return first.strip()

            if 'outputs' in result and isinstance(result['outputs'], list):
                for out in result['outputs']:
                    if isinstance(out, dict):
                        c = out.get('content')
                        if isinstance(c, str) and c.strip():
                            return c.strip()
                        if isinstance(c, list):
                            for item in c:
                                if isinstance(item, dict) and 'text' in item:
                                    return str(item['text']).strip()

            if 'messages' in result and isinstance(result['messages'], list):
                for m in result['messages']:
                    if isinstance(m, dict):
                        c = m.get('content')
                        if isinstance(c, str) and c.strip():
                            return c.strip()
                        if isinstance(c, list):
                            for item in c:
                                if isinstance(item, dict) and 'text' in item:
                                    return str(item['text']).strip()

            for k in ('generatedText', 'generated_text', 'text'):
                if k in result and isinstance(result[k], str) and result[k].strip():
                    return result[k].strip()

            if 'body' in result:
                b = result['body']
                if isinstance(b, dict):
                    return extract_text_from_bedrock_response(b)
                if isinstance(b, (bytes, str)):
                    try:
                        body_json = json.loads(b.decode() if isinstance(b, bytes) else b)
                        return extract_text_from_bedrock_response(body_json)
                    except Exception:
                        return (b.decode() if isinstance(b, bytes) else str(b)).strip()
    except Exception:
        logger.debug("Error while probing response shapes", exc_info=True)

    try:
        s = json.dumps(result) if not isinstance(result, str) else result
    except Exception:
        s = str(result)

    return s.strip()

def find_numeric_in_text(text):
    """
    Find the first numeric-looking token (e.g., 1,234.56) in the text using regex.
    Returns None if nothing numeric is found.
    """
    if not text:
        return None
    m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)', text)
    if m:
        return m.group(1)
    return None

def lambda_handler(event, context):
    try:
        logger.info("=" * 80)
        logger.info("METER READING LAMBDA STARTED")
        logger.info("=" * 80)

        model = "us.amazon.nova-lite-v1:0"

        # Parse S3 event (basic validation)
        if 'Records' not in event or len(event['Records']) == 0:
            logger.error("No Records in event")
            return {"statusCode": 400, "error": "No S3 Records"}

        rec = event['Records'][0]
        bucket = rec['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(rec['s3']['object']['key'])
        customer_id = key.split('/')[0] if '/' in key else key

        logger.info(f"[S3] Bucket: {bucket}, Key: {key}, Customer: {customer_id}")

        # Get image format (defensive)
        try:
            obj = s3.head_object(Bucket=bucket, Key=key)
            ctype = obj.get('ContentType', 'image/jpeg') or 'image/jpeg'
            fmt = 'jpeg' if ('jpeg' in ctype.lower() or 'jpg' in ctype.lower()) else 'png'
        except Exception:
            fmt = 'jpeg'

        s3_uri = f"s3://{bucket}/{key}"
        logger.info(f"[S3 URI] {s3_uri}")

        # ---------- Send only a 'user' role message which includes both the instruction text and the image ----------
        payload = {
            "schemaVersion": "messages-v1",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "text": (
                                "Extract ONLY the current kWh reading shown on the digital (or mechanical) display of this electricity meter. "
                                "Ignore every other number, text, label, serial number, date, or barcode. "
                                "Return ONLY the numeric reading used for billing (for example: 12345.67 or 1,234). "
                                "If the display is unclear, return EXACTLY the word UNCLEAR (uppercase) and nothing else."
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

        logger.info(f"[BEDROCK] Calling model {model}")
        resp = bedrock.invoke_model(
            modelId=model,
            body=json.dumps(payload),
            contentType="application/json",
            accept="application/json"
        )

        # read body safely
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

        logger.info(f"[BEDROCK RESPONSE RAW] {str(result)[:2000]}")

        extracted = extract_text_from_bedrock_response(result)
        logger.info(f"[EXTRACTED RAW] {extracted}")

        # If the model returned UNCLEAR or similar, handle as unclear
        if not extracted or "UNCLEAR" in extracted.upper():
            message = "Meter reading unclear or unreadable. Please retake the image."
            logger.warning(f"[REJECTED] {message} | Extracted: {extracted}")
            try:
                send_sns_notification(customer_id, key, message)
            except Exception:
                logger.exception("[SNS] Failed to send unclear notification")
            return {
                "statusCode": 400,
                "status": "UNCLEAR",
                "customer_id": customer_id,
                "message": message
            }

        # Try to parse numeric value
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
            message = f"Failed to parse meter reading. Content: {extracted}"
            logger.error(f"[PARSE ERROR] {message}")
            try:
                send_sns_notification(customer_id, key, message)
            except Exception:
                logger.exception("[SNS] Failed to send parse error notification")
            return {
                "statusCode": 400,
                "status": "PARSE_ERROR",
                "customer_id": customer_id,
                "response": extracted
            }

        value = float(num_candidate)
        logger.info(f"✓ SUCCESS! METER READING = {value} ✓")
        log_to_rds(customer_id, value, key)
        return {
            "statusCode": 200,
            "status": "SUCCESS",
            "customer_id": customer_id,
            "meter_reading": value,
            "s3_key": key
        }

    except Exception as e:
        logger.exception(f"[LAMBDA ERROR] {e}")
        return {"statusCode": 500, "error": str(e)}
