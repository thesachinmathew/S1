import json
import boto3
import logging
import urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        logger.info("=" * 80)
        logger.info("METER READING LAMBDA STARTED")
        logger.info("=" * 80)

        model = "us.amazon.nova-lite-v1:0"
        bedrock = boto3.client('bedrock-runtime')
        s3 = boto3.client('s3')

        # Parse event
        rec = event['Records'][0]
        bucket = rec['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(rec['s3']['object']['key'])
        cust = key.split('/')[0]

        logger.info(f"[S3] Bucket: {bucket}, Key: {key}, Customer: {cust}")

        # Get image format
        try:
            obj = s3.head_object(Bucket=bucket, Key=key)
            ctype = obj.get('ContentType', 'image/jpeg')
            fmt = 'jpeg' if 'jpeg' in ctype.lower() or 'jpg' in ctype.lower() else 'jpeg'
        except:
            fmt = 'jpeg'

        logger.info(f"[FORMAT] {fmt}")

        s3_uri = f"s3://{bucket}/{key}"
        logger.info(f"[S3 URI] {s3_uri}")

        # Call Bedrock
        payload = {
            "schemaVersion": "messages-v1",
            "system": [{"text": "Extract ONLY the current kWh reading shown on the digital (or mechanical) display of this electricity meter. "
    "Ignore every other number, text, label, serial number, background writing, or barcode. "
    "Only return the single numeric value SHOWN in the display window (the one used for customer billing). "
    "Do NOT answer with any ID, code, meter number, date, or words. "
    "If the reading is unclear or the display is not visible, respond with 'UNCLEAR' and nothing else."}],
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

        logger.info(f"[BEDROCK] Calling {model}")
        resp = bedrock.invoke_model(
            modelId=model,
            body=json.dumps(payload),
            contentType="application/json",
            accept="application/json"
        )

        # Parse response
        result = json.loads(resp['body'].read())
        logger.info(f"[BEDROCK RESPONSE] {json.dumps(result)}")

        # Handle different response formats
        try:
            if 'content' in result:
                text = result['content'][0]['text'].strip()
            else:
                logger.error(f"Unexpected response structure: {result}")
                return {"statusCode": 500, "error": "Invalid response format", "response": result}
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Error parsing response: {e}")
            return {"statusCode": 500, "error": str(e), "response": result}

        logger.info(f"[EXTRACTED] {text}")

        # Check if unclear
        if "UNCLEAR" in text.upper():
            logger.warning(f"[UNCLEAR] {cust}")
            return {"statusCode": 400, "status": "UNCLEAR", "customer_id": cust}

        # Parse number
        try:
            value = float(text.replace(',', '').strip())
            logger.info(f"")
            logger.info(f"███████████████████████████████████████████████████████")
            logger.info(f"✓✓✓ SUCCESS! METER READING = {value} ✓✓✓")
            logger.info(f"███████████████████████████████████████████████████████")
            logger.info(f"")

            return {
                "statusCode": 200,
                "status": "SUCCESS",
                "customer_id": cust,
                "meter_reading": value,
                "s3_key": key
            }
        except ValueError:
            logger.error(f"Could not parse: {text}")
            return {"statusCode": 400, "status": "PARSE_ERROR", "response": text}

    except Exception as e:
        logger.exception(f"ERROR: {e}")
        return {"statusCode": 500, "error": str(e)}
