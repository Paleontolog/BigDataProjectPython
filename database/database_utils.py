import psycopg2
from pyspark.sql.functions import lit
import json

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)


def create_table(table, fields, cursor):
    fields = ", ".join(fields)
    query = "CREATE TABLE IF NOT EXISTS {0} (" \
            "{0}_ID INT IDENTITY(0, 1) PRIMARY KEY, " \
            "Time VARCHAR(255) NOT NULL, " \
            "{1});".format(table, fields)
    cursor.execute(query)


def create_all_tables(cursor):
    create_table("TopState",
                 ["StateName VARCHAR(255) NOT NULL",
                  "Quantity INT NOT NULL"],
                 cursor)

    create_table("TopCityOrCounty",
                 ["CityOrCountyName VARCHAR(255) NOT NULL",
                  "Quantity INT NOT NULL"],
                 cursor)

    create_table("GunStolen",
                 ["Quantity INT NOT NULL"],
                 cursor)


def insert_into_table(data, table, columns, conn):
    if not data:
        return
    data = [str(tuple(example)) for example in data]
    values = "{}".format(", ".join(data))
    columns = "(Time, {})".format(", ".join(columns))

    query = "INSERT INTO {} {} VALUES {};" \
        .format(table, columns, values)

    print(query)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def initialize_db(conn):
    try:
        cursor = conn.cursor()
        create_all_tables(cursor)
        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_db_connection(host, port, user, password, database):
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, user=user,
                                password=password, database=database)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return conn


def rename_columns(dataframe):
    names = dataframe.schema.names
    dataframe = dataframe.withColumnRenamed('count', 'Quantity')
    print(names)
    if len(names) > 2:
        table = app_config["database_tables"][names[0]]["table_name"]
        new_name = app_config["database_tables"][names[0]]["fields"][0]
        dataframe = dataframe.withColumnRenamed(names[0], new_name)
    else:
        table = app_config["database_tables"]["gun_stolen"]["table_name"]
    return dataframe, table


def insert_into_table_schema(dataframe, time, conn):
    if not dataframe.head(1):
        return
    dataframe = dataframe.withColumn("Time", lit(str(time)))
    dataframe, table = rename_columns(dataframe)
    values = [str(tuple(i.asDict().values())) for i in dataframe.collect()]
    values = ", ".join(values)
    query = "INSERT INTO {} {} VALUES {};" \
        .format(table,
                "({})".format(", ".join(dataframe.schema.names)),
                values)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)