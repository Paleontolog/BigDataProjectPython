#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import re
import json

from database.s3_utils import *
from metrics.calculate_metrics import *

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)

word_matcher = re.compile(".*::(Unknown|Stolen)(\|{2}|$)")


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', type=str,
                        help='Type of start environment')
    parser.add_argument('type', type=str,
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


def process_rdd(lines, db_connection, spark, schema):
    # lines = ds.window(10, 20)
    lines = lines.map(lambda line: json.loads(line))

    lines.foreachRDD(lambda time, rdd: save_in_s3_rdd(time, rdd, spark, schema))

    states = count_rdd(lines, "state")
    get_n_and_save_rdd(states, "state", 10, db_connection)

    city_or_county = count_rdd(lines, "city_or_county")
    get_n_and_save_rdd(city_or_county, "city_or_county", 10, db_connection)

    gun_stolen = count_if_contains_rdd(lines, "gun_stolen", word_matcher)
    get_n_and_save_rdd(gun_stolen, "gun_stolen", 1, db_connection)



def process_dataframe(lines, db_connection, context, schema):
    # lines = ds.window(10, 20)
    lines = lines.map(lambda line: json.loads(line))

    def process(time, rdd):
        dataframe = context.createDataFrame(rdd, schema)

        save_in_s3_schema(time, dataframe)

        states = count_dataframe(dataframe, "state")
        get_n_and_save_dataframe(states, time, 10, db_connection)

        city_or_county = count_dataframe(dataframe, "city_or_county")
        get_n_and_save_dataframe(city_or_county, time, 10, db_connection)

        gun_stolen = count_if_contains_dataframe(dataframe, "gun_stolen", word_matcher)
        get_n_and_save_dataframe(gun_stolen, time, 1, db_connection)

    lines.foreachRDD(lambda time, rdd: process(time, rdd))


if __name__ == "__main__":

    args = arg_parse()

    if args.mode == "local":
        import findspark
        os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk1.8.0_241"
        os.environ["SPARK_HOME"] = r"C:\spark-2.4.5-bin-hadoop2.7"
        os.environ['PYSPARK_SUBMIT_ARGS'] = ""
        findspark.init(r"C:\spark-2.4.5-bin-hadoop2.7")
        findspark.add_packages(["org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5"])

    from pyspark.streaming import StreamingContext
    from pyspark.sql import SparkSession, DataFrame
    from pyspark import RDD
    from pyspark import SparkConf, SparkContext
    from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
    from pyspark.sql.context import SQLContext
    

    schema = st.StructType.fromJson(app_config["all_data_scheme"])

    connect = create_db_connection(args.redshift_host,
                                   args.redshift_port,
                                   args.redshift_user,
                                   args.redshift_password,
                                   args.redshift_db_name)

    initialize_db(connect)

    if args.mode == "local":
        spark = SparkSession.builder.appName(args.app_name).master("local[*]").getOrCreate()
        ssc = StreamingContext(spark.sparkContext, args.batch_duration)
        cur = os.path.abspath(".")
        lines = ssc.textFileStream(rf"{cur}\emulation")
        lines.pprint()

        if args.type == "rdd":
            process_rdd(lines, connect, spark, schema)
        else:
            process_dataframe(lines, connect, spark, schema)
    else:
        conf = SparkConf().setAppName(args.app_name).setMaster("spark://13.48.194.172:7077")
        spark = SparkContext(conf=conf)
        ssc = StreamingContext(spark, args.batch_duration)
        sql = SQLContext(spark)
        lines = KinesisUtils.createStream(ssc, args.app_name, args.stream_name_kinesis,
                                              args.endpoint_url_kinesis, args.region_name,
                                              InitialPositionInStream.LATEST,
                                              awsAccessKeyId=args.aws_access_key_id,
                                              awsSecretKey=args.aws_secret_access_key,
                                              checkpointInterval=args.checkpoint_interval)
        lines.pprint()
        if args.type == "rdd":
            process_rdd(lines, connect, spark, schema)
        else:
            process_dataframe(lines, connect, sql, schema)

    ssc.start()
    ssc.awaitTermination()



