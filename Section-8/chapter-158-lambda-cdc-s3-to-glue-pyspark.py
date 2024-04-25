import json
import boto3

def lambda_handler(event, context):
    print(event["Records"][0]["s3"]["bucket"]["name"])
    print(event["Records"][0]["s3"]["object"]["key"])

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    filename = event["Records"][0]["s3"]["object"]["key"]

    glue_client = boto3.client('glue')
    response = glue_client.start_job_run(
        JobName = 'glueCDC-payspark-poc',
        Arguments = {
            '--s3_target_path_key': filename,
            '--s3_target_path_bucket': bucket
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
