import json
import boto3

def save_in_s3(time, rdd, s3_client):
    time = str(time)
    file = { time: rdd.collect() }

    responce = s3_client.put_object(
        Bucket="stream_data",
        Body=json.dumps(file),
        Key=f"{time}.json"
    )

def create_session(region_name, aws_access_key_id, aws_secret_access_key, endpoint_url):
    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url)

    return s3_client

