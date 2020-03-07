import psycopg2
import json

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)


def create_db_connection(host, port, user, password, database):
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, user=user,
                                password=password, database=database)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn


def execute_select(query):
    cursor = connect.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    cursor.close()
    return sorted(res, key=lambda x: x[-1], reverse=True)


if __name__ == "__main__":

    connect = create_db_connection(app_config["redshift_host"],
                                   app_config["redshift_port"],
                                   app_config["redshift_user"],
                                   app_config["redshift_password"],
                                   app_config["redshift_db_name"])

    query_top_state = "SELECT StateName, sum(Quantity) as ALL_SUM " \
                      "FROM TopState " \
                      "GROUP BY StateName " \
                      "ORDER BY ALL_SUM DESC " \
                      "LIMIT 10;"

    query_top_city_or_county = "SELECT CityOrCountyName, sum(Quantity) as ALL_SUM " \
                               "FROM TopCityOrCounty " \
                               "GROUP BY CityOrCountyName " \
                               "ORDER BY ALL_SUM DESC " \
                               "LIMIT 10;"

    query_guns = "SELECT sum(Quantity) " \
                 "FROM TopCityOrCounty;"


    print("Топ штатов: ")
    state = [str(i) for i in execute_select(query_top_state)]
    print("\n".join(state))
    print("-"*100)
    print("\nТоп городов и посёлков: ")
    city_or_county = [str(i) for i in execute_select(query_top_city_or_county)]
    print("\n".join(city_or_county))
    print("-" * 100)
    print("\nКоличество случаев когда оружие было украдено или статус неизвестен: ")
    print(execute_select(query_guns))
    connect.close()
