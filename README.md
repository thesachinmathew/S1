When you upload a meter image to S3, it triggers a Lambda.
The Lambda sends the image to Bedrock, which reads the meter value.
if the image is blurry or unreadable bedrock should not even try to read a corrupt value.
instead send an sns notification saying the image is unclear retake the image.
Lambda then stores that reading with the customer ID in the RDS database.
