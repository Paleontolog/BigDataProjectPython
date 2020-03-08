#!/usr/bin/env python
# coding: utf-8

import boto3
import json
import time
import pandas as pd
import argparse

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)



def put_to_stream(thing_id, data, s3_client, my_stream_name):
    put_response = s3_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(data),
                        PartitionKey=thing_id)

def arg_parse():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--aws_access_key_id', type=str,
                        default=app_config["aws_access_key_id"])
    parser.add_argument('--aws_secret_access_key', type=str,
                        default=app_config["aws_secret_access_key"])
    parser.add_argument('--region_name', type=str,
                        default=app_config["region_name"])
    parser.add_argument('--stream_name_kinesis', type=str,
                        default=app_config["stream_name_kinesis"])
    parser.add_argument('--endpoint_url_kinesis', type=str,
                        default=app_config["endpoint_url_kinesis"])
    parser.add_argument('--thing_id', type=str,
                        default=app_config["thing_id"])
    parser.add_argument('--delay', type=int,
                        default=app_config["kinesis_delay"])
    
    parser.add_argument('--filepath', type=str,
                        default=app_config["data_filepath"])
    
    return parser.parse_args()


if __name__ == "__main__":
    
    args = arg_parse()
    data = pd.read_csv(args.filepath)
    session = boto3.session.Session()

    s3_client = session.client(
        service_name='kinesis',
        region_name=args.region_name,
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
        endpoint_url=args.endpoint_url_kinesis
    )
    
    for row in data.iterrows():
        put_to_stream(args.thing_id, dict(row[1]), s3_client, args.stream_name_kinesis)
        time.sleep(args.delay)
