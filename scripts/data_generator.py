#!/usr/bin/env python
# coding: utf-8

import boto3
import json
import time
import pandas as pd
import argparse


def put_to_stream(thing_id, data, s3_client, my_stream_name):
    put_response = s3_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(data),
                        PartitionKey=thing_id)

def arg_parse():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--aws_access_key_id', type=str,
                        default='AKIAJ5V6NEAI3YNTWGDA')
    parser.add_argument('--aws_secret_access_key', type=str,
                        default='xdyXL4jP1SYhiKO9OGhOLYijVbG0BwPnq7J6oRDZ')
    parser.add_argument('--region_name', type=str,
                        default="eu-north-1")
    parser.add_argument('--stream_name_kinesis', type=str,
                        default="gunDataset_Kinesis")
    parser.add_argument('--endpoint_url_kinesis', type=str,
                        default="https://kinesis.eu-north-1.amazonaws.com")
    parser.add_argument('--thing_id', type=str,
                        default="aa-bb")
    parser.add_argument('--delay', type=int,
                        default=1)
    
    parser.add_argument('--filepath', type=str,
                        default=r"D:\PycharmProjects\BIG_DATA\gun-violence-data_01-2013_03-2018.csv")
    
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
