from pyspark.sql import SparkSession
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)

lines = KinesisUtils.createStream(ssc, "test", "test_s",
                                             "https://kinesis.eu-north-1.amazonaws.com",
                                              "eu-north-1",
                                              InitialPositionInStream.LATEST,
                                              awsAccessKeyId="AKIAJ5V6NEAI3YNTWGDA",
                                              awsSecretKey="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",
                                              checkpointInterval=2)
