import json
import sys
import psycopg2
from pyspark.sql.functions import lit

from database.database_utils import rename_columns

sys.path.append("database")
from database_utils import *

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)


def create_all_tables_global(cursor):
    #cursor.execute("DROP TABLE IF EXISTS TopState_CUR")
    #cursor.execute("DROP TABLE IF EXISTS TopCityOrCounty_CUR")
    cursor.execute("DROP TABLE IF EXISTS TopCityOrCounty_prev")
    cursor.execute("DROP TABLE IF EXISTS TopState_prev")
    create_table("TopState_CUR",
                 ["Name VARCHAR(255) NOT NULL",
                  "Quantity INT NOT NULL"],
                 cursor)

    create_table("TopCityOrCounty_CUR",
                 ["Name VARCHAR(255) NOT NULL",
                  "Quantity INT NOT NULL"],
                 cursor)

    create_table("GunStolen",
                 ["Time VARCHAR(255) NOT NULL",
                  "Quantity INT NOT NULL"],
                 cursor)


def execute(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
    conn.commit()


def insert_global_metrics(dataframe, conn):
    if not dataframe.head(1):
        return
    dataframe, table = rename_columns(dataframe, "glob")
    values = [str(tuple(i.asDict().values())) for i in dataframe.collect()]
    values = ", ".join(values)

    dataframe.show()
    rename_cur_to_prev = "ALTER TABLE {0}_CUR RENAME TO {0}_PREV;" \
        .format(table)

    insert_into_prev = "INSERT INTO {}_PREV {} VALUES {};" \
        .format(table,
                "({})".format(", ".join(dataframe.schema.names)),
                values)

    create_new_table = "CREATE TABLE IF NOT EXISTS {0}_CUR (LIKE {0}_PREV);"\
                            .format(table)

    recalculate_metrics = "INSERT INTO {0}_CUR (NAME, Quantity) " \
                          "SELECT Name, sum(Quantity) as Quantity " \
                          "FROM {0}_PREV " \
                          "GROUP BY Name;".format(table)

    drop_if_exist_old = "DROP TABLE IF EXISTS {}_OLD;".format(table)

    rename_prev_to_old = "ALTER TABLE {0}_PREV RENAME TO {0}_OLD;" \
        .format(table)

    execute(conn, rename_cur_to_prev)
    execute(conn, insert_into_prev)
    execute(conn, create_new_table)
    execute(conn, recalculate_metrics)
    execute(conn, drop_if_exist_old)
    execute(conn, rename_prev_to_old)
