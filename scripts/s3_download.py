import boto3
import argparse
import json

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("--path", type=str, default="")
    return parser.parse_args()

args = parse_args()

session = boto3.session.Session()

s3_client = session.client(
    service_name='s3',
    region_name=app_config["region_name"],
    aws_access_key_id=app_config["aws_access_key_id"],
    aws_secret_access_key=app_config["aws_secret_access_key"],
    endpoint_url=app_config["endpoint_url_s3"]
)

with open('{}\\{}'.format(args.filename, args.filename), 'wb') as data:
    s3_client.download_fileobj('stream_data', args.filename, data)