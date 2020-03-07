from database.database_utils import *
import json

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)

database_tables = app_config["database_tables"]

# Отсортированный топ элементов в батче
def count(lines, field):
    states = lines.map(lambda x: x[field].strip().lower()) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    states = states.transform(lambda x: x.sortBy(lambda x: (-x[1], x[0]),
                                                 ascending=True))
    return states


# Подсчёт не nan элементов, удовлетворяющих условия в matcher
def count_if_contains(lines, field, matcher):
    count = lines.map(lambda x: x[field]) \
        .filter(lambda x: isinstance(x, str)) \
        .filter(lambda x: matcher.match(x) is not None) \
        .count()
    return count


def save_metrics(time, lines, field, db_connection):
    def pack(obj):
        return obj if isinstance(obj, tuple) else (obj,)

    lines = [(str(time), *pack(ex)) for ex in lines]
    insert_into_table(lines,
                      database_tables[field]["table_name"],
                      database_tables[field]["fields"],
                      db_connection)


def get_n_and_save(lines, field, n, db_connection):
    lines.foreachRDD(lambda time, rdd: save_metrics(time,
                                                    rdd.take(n),
                                                    field,
                                                    db_connection))
