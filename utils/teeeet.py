import boto3
import json
from datetime import datetime
import time

my_stream_name = 'test_s'

kinesis_client = boto3.client(service_name='kinesis',
                              region_name="eu-north-1",
                              aws_access_key_id='AKIAJ5V6NEAI3YNTWGDA',
                              aws_secret_access_key='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',
                              endpoint_url="https://kinesis.eu-north-1.amazonaws.com")

response = kinesis_client.describe_stream(StreamName=my_stream_name)

my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='LATEST')

my_shard_iterator = shard_iterator['ShardIterator']

record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,
                                              Limit=10)

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                  Limit=10)

    print(record_response)

    # wait for 5 seconds
    time.sleep(10)