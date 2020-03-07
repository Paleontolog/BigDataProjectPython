import psycopg2

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
        print("Empty")
        return
    data = [str(tuple(example)) for example in data]
    values = "{}".format(", ".join(data))
    columns = "(Time, {})".format(", ".join(columns))

    query = "INSERT INTO {} {} VALUES {};" \
        .format(table, columns, values)

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


def insert_into_table_schema(dataframe, conn):
    if dataframe.head(1).isEmpty():
        return

    query = "INSERT INTO {} {} VALUES {};" \
        .format(table, columns, values)

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)