from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import BooleanType

from database.database_utils import *
import pyspark.sql.functions as F
from pyspark.sql.functions import isnan, when, count, col, desc, asc

import json

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)

database_tables = app_config["database_tables"]

def count_rdd(lines, field):
    states = lines.map(lambda x: x[field].strip().lower()) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    states = states.transform(lambda x: x.sortBy(lambda x: (-x[1], x[0]),
                                                 ascending=True))
    return states


def count_if_contains_rdd(lines, field, matcher):
    count = lines.map(lambda x: x[field]) \
        .filter(lambda x: isinstance(x, str)) \
        .filter(lambda x: matcher.match(x) is not None) \
        .count()
    return count


def save_metrics_rdd(time, lines, field, db_connection):
    def pack(obj):
        return obj if isinstance(obj, tuple) else (obj,)

    lines = [(str(time), *pack(ex)) for ex in lines]
    insert_into_table(lines,
                      database_tables[field]["table_name"],
                      database_tables[field]["fields"],
                      db_connection)


def get_n_and_save_rdd(lines, field, n, db_connection):
    lines.foreachRDD(lambda time, rdd: save_metrics_rdd(time,
                                                        rdd.take(n),
                                                        field,
                                                        db_connection))


def count_dataframe(dataframe, field):
    return dataframe.groupBy(field).count()\
            .sort(desc("count"), asc(field))



def get_n_and_save_dataframe(dataframe, time, n, db_connection):
    dataframe = dataframe.limit(n)
    insert_into_table_schema(dataframe, time, db_connection)


def count_if_contains_dataframe(dataframe, field, word_matcher):
    matcher = F.udf(lambda x: word_matcher.match(x) is not None, BooleanType())
    return dataframe\
            .where(col(f"{field}").isNotNull() & (~isnan(f"{field}"))) \
            .filter(matcher(field))\
            .agg(count("incident_id").alias('count'))



