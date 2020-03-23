#!/usr/bin/env python
# coding: utf-8

import boto3
import json
import time
import pandas as pd

with open('../configPubSub.json') as json_data_file:
    app_config = json.load(json_data_file)


def put_to_stream(thing_id, data, s3_client, my_stream_name):
    put_response = s3_client.put_record(
        StreamName=my_stream_name,
        Data=json.dumps(data),
        PartitionKey=thing_id)


if __name__ == "__main__":


    data = pd.read_csv(app_config["data_filepath"])
    session = boto3.session.Session()

    s3_client = session.client(
        service_name='kinesis',
        region_name="eu-north-1",
        aws_access_key_id="AKIAJ5V6NEAI3YNTWGDA",
        aws_secret_access_key="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",
        endpoint_url="https://kinesis.eu-north-1.amazonaws.com"
        #"https://kinesis.eu-north-1.amazonaws.com"
    )

    for row in data.iterrows():
        put_to_stream("aa-bb", dict(row[1]), s3_client, "test_s")
        time.sleep(0.5)
