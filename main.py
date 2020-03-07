#!/usr/bin/env python
# coding: utf-8

import os
import findspark
import argparse
import re
from database.s3_utils import *
from metrics.calculate_metrics import *

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)

word_matcher = re.compile(".*::(Unknown|Stolen)(\|{2}|$)")


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', type=str,
                        help='Type of start environment')

    parser.add_argument('--aws_access_key_id', type=str,
                        default=app_config["aws_access_key_id"])
    parser.add_argument('--aws_secret_access_key', type=str,
                        default=app_config["aws_secret_access_key"])
    parser.add_argument('--region_name', type=str,
                        default=app_config["region_name"])
    parser.add_argument('--endpoint_url_s3', type=str,
                        default=app_config["endpoint_url_s3"])
    parser.add_argument('--app_name', type=str,
                        default=app_config["app_name"])
    parser.add_argument('--batch_duration', type=int, default=app_config["batch_duration"])
    parser.add_argument('--stream_name_kinesis', type=str,
                        default=app_config["stream_name_kinesis"])
    parser.add_argument('--endpoint_url_kinesis', type=str,
                        default=app_config["endpoint_url_kinesis"])
    parser.add_argument('--checkpoint_interval', type=int,
                        default=app_config["checkpoint_interval"])
    parser.add_argument('--redshift_host', type=str,
                        default=app_config["redshift_host"])
    parser.add_argument('--redshift_port', type=int,
                        default=app_config["redshift_port"])
    parser.add_argument('--redshift_user', type=str,
                        default=app_config["redshift_user"])
    parser.add_argument('--redshift_password', type=str,
                        default=app_config["redshift_password"])
    parser.add_argument('--redshift_db_name', type=str,
                        default=app_config["redshift_db_name"])

    return parser.parse_args()


def process(lines, s3_client, db_connection):
    # lines = ds.window(10, 20)
    lines = lines.map(lambda line: json.loads(line))

    lines.foreachRDD(lambda time, rdd: save_in_s3(time, rdd, s3_client))

    states = count(lines, "state")
    get_n_and_save(states, "state", 10, db_connection)

    city_or_county = count(lines, "city_or_county")
    get_n_and_save(city_or_county, "city_or_county", 10, db_connection)

    gun_stolen = count_if_contains(lines, "gun_stolen", word_matcher)
    get_n_and_save(gun_stolen, "gun_stolen", 1, db_connection)



if __name__ == "__main__":

    args = arg_parse()

    if args.mode == "local":
        os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk1.8.0_241"
        os.environ["SPARK_HOME"] = r"C:\spark-2.4.5-bin-hadoop2.7"
        os.environ['PYSPARK_SUBMIT_ARGS'] = ""
        findspark.init(r"C:\spark-2.4.5-bin-hadoop2.7")
        findspark.add_packages(["org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5"])

    from pyspark.streaming import StreamingContext
    from pyspark.sql import SparkSession
    from pyspark import RDD
    from pyspark import SparkConf, SparkContext
    from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

    s3_client = create_session(args.region_name,
                              args.aws_access_key_id,
                              args.aws_secret_access_key,
                              args.endpoint_url_s3)

    connect = create_db_connection(args.redshift_host,
                                   args.redshift_port,
                                   args.redshift_user,
                                   args.redshift_password,
                                   args.redshift_db_name)

    initialize_db(connect)

    if args.mode == "local":
        spark = SparkSession.builder.appName(args.app_name).master("local[*]").getOrCreate()
        ssc = StreamingContext(spark.sparkContext, args.batch_duration)
    else:
        conf = SparkConf().setAppName(args.app_name).setMaster("spark://master:7077")
        spark = SparkContext(conf=conf)
        ssc = StreamingContext(spark, args.batch_duration)

    # if args.mode == "local":
    #     cur = os.path.abspath(".")
    #     lines = ssc.textFileStream(rf"{cur}\emulation")
    # else:
    lines = KinesisUtils.createStream(ssc, args.app_name, args.stream_name_kinesis,
                                          args.endpoint_url_kinesis, args.region_name,
                                          InitialPositionInStream.AT_TIMESTAMP,
                                          awsAccessKeyId=args.aws_access_key_id,
                                          awsSecretKey=args.aws_secret_access_key,
                                          checkpointInterval=args.checkpoint_interval)
    lines.pprint()
    process(lines, s3_client, connect)

    ssc.start()
    ssc.awaitTermination()



