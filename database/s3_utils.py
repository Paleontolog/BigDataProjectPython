import json

import pyspark.sql.types as st

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)


def save_in_s3_rdd(time, rdd, context, schema):
    if rdd.isEmpty():
        return
    dataframe = context.createDataFrame(rdd, schema)
    dataframe.write.parquet("{}/{}.parquet".format(app_config["s3_filepath"], str(time)),
                            mode="overwrite")

def save_in_s3_schema(time, dataframe):
    if not dataframe.head(1):
        return
    dataframe.write.parquet("{}/{}.parquet".format(app_config["s3_filepath"], str(time)),
                            mode="overwrite")
