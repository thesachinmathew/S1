import json
import boto3
import logging
import urllib.parse
import pymysql  # Make sure pymysql is added via Lambda Layer


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- RDS Configuration ---
RDS_HOST = "meter-db.cirfd7khe8qh.us-east-1.rds.amazonaws.com"
RDS_USER = "admin"
RDS_PASS = "12345678"  # Replace with actual password
RDS_NAME = "sach_meterdb"

# --- SNS Configuration ---
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:810751063582:meter-reading-alerts"

# --- AWS Clients ---
bedrock = boto3.client('bedrock-runtime')
s3 = boto3.client('s3')
sns = boto3.client('sns')

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
        logger.error(f"[RDS ERROR] {e}")

def send_sns_notification(customer_id, s3_key, message):
    """Send notification via SNS."""
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=f"Customer: {customer_id}\nS3 Key: {s3_key}\nIssue: {message}",
            Subject="Meter Reading Alert"
        )
        logger.info("[SNS] Notification sent")
    except Exception as e:
        logger.error(f"[SNS ERROR] {e}")

def lambda_handler(event, context):
    try:
        logger.info("=" * 80)
        logger.info("METER READING LAMBDA STARTED")
        logger.info("=" * 80)

        model = "us.amazon.nova-lite-v1:0"

        # Parse S3 event
        rec = event['Records'][0]
        bucket = rec['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(rec['s3']['object']['key'])
        customer_id = key.split('/')[0]

        logger.info(f"[S3] Bucket: {bucket}, Key: {key}, Customer: {customer_id}")

        # Get image format
        try:
            obj = s3.head_object(Bucket=bucket, Key=key)
            ctype = obj.get('ContentType', 'image/jpeg')
            fmt = 'jpeg' if 'jpeg' in ctype.lower() or 'jpg' in ctype.lower() else 'jpeg'
        except:
            fmt = 'jpeg'

        s3_uri = f"s3://{bucket}/{key}"
        logger.info(f"[S3 URI] {s3_uri}")

        # Prepare Bedrock request
        payload = {
            "schemaVersion": "messages-v1",
            "system": [{
                "text": (
                    "Extract ONLY the current kWh reading shown on the digital (or mechanical) display of this electricity meter. "
                    "Ignore every other number, text, label, serial number, background writing, or barcode. "
                    "Only return the single numeric value SHOWN in the display window (the one used for customer billing). "
                    "Do NOT answer with any ID, code, meter number, date, or words. "
                    "If the reading is unclear or the display is not visible, respond with 'UNCLEAR' and nothing else."
                )
            }],
            "messages": [{
                "role": "user",
                "content": [{
                    "image": {
                        "format": fmt,
                        "source": {"s3Location": {"uri": s3_uri}}
                    }
                }]
            }],
            "inferenceConfig": {"temperature": 0.1, "maxTokens": 100}
        }

        logger.info(f"[BEDROCK] Calling model {model}")
        resp = bedrock.invoke_model(
            modelId=model,
            body=json.dumps(payload),
            contentType="application/json",
            accept="application/json"
        )

        result = json.loads(resp['body'].read())
        logger.info(f"[BEDROCK RESPONSE] {json.dumps(result)}")

        # Extract meter reading
        try:
            if 'content' in result and len(result['content']) > 0:
                text = result['content'][0]['text'].strip()
            else:
                text = ""
        except Exception as e:
            text = ""
            logger.error(f"Error extracting text: {e}")

        logger.info(f"[EXTRACTED] {text}")

        # Reject unclear or empty readings
        if not text or "UNCLEAR" in text.upper():
            message = "Meter reading unclear or unreadable. Please retake the image."
            logger.warning(f"[REJECTED] {message}")
            send_sns_notification(customer_id, key, message)
            return {
                "statusCode": 400,
                "status": "UNCLEAR",
                "customer_id": customer_id,
                "message": message
            }

        # Parse numeric value
        try:
            value = float(text.replace(',', '').strip())
            logger.info(f"✓ SUCCESS! METER READING = {value} ✓")
            log_to_rds(customer_id, value, key)
            return {
                "statusCode": 200,
                "status": "SUCCESS",
                "customer_id": customer_id,
                "meter_reading": value,
                "s3_key": key
            }
        except ValueError:
            message = "Failed to parse meter reading. Content: " + text
            logger.error(f"[PARSE ERROR] {message}")
            send_sns_notification(customer_id, key, message)
            return {
                "statusCode": 400,
                "status": "PARSE_ERROR",
                "customer_id": customer_id,
                "response": text
            }

    except Exception as e:
        logger.exception(f"[LAMBDA ERROR] {e}")
        return {"statusCode": 500, "error": str(e)}
