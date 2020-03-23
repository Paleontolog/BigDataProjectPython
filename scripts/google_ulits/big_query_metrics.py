from google.cloud import bigquery
import json
import argparse

with open('configPubSub.json') as json_data_file:
    app_config = json.load(json_data_file)

def arg_parse():
    parser = argparse.ArgumentParser()

    parser.add_argument('all_or_batch', type=str,
                        help='Type of metrics (all or batch metrics)')

    return parser.parse_args()

def batch_metrics():
    query_top_state = """SELECT StateName, sum(Quantity) as ALL_SUM
                         FROM TopState 
                         GROUP BY StateName 
                         ORDER BY ALL_SUM DESC
                         LIMIT 10;"""

    query_top_city_or_county = """SELECT CityOrCountyName, sum(Quantity) as ALL_SUM
                                  FROM TopCityOrCounty
                                  GROUP BY CityOrCountyName
                                  ORDER BY ALL_SUM DESC
                                  LIMIT 10;"""

    return query_top_state, query_top_city_or_county


def all_metrics():
    query_top_state = """SELECT Name, Quantity
                         FROM TopState_CUR
                         ORDER BY Quantity DESC
                         LIMIT 10;"""

    query_top_city_or_county = """SELECT Name, Quantity
                                  FROM TopCityOrCounty_CUR
                                  ORDER BY Quantity DESC
                                  LIMIT 10;"""

    return query_top_state, query_top_city_or_county


if __name__ == "__main__":

    client = bigquery.Client()

    args = arg_parse()
    if args.all_or_batch == "batch":
        query_top_state, query_top_city_or_county = batch_metrics()
    else:
        query_top_state, query_top_city_or_county = all_metrics()

    query_guns = """ SELECT sum(Quantity) 
                     FROM GunStolen; """

    print("Топ штатов: ")
    state = [str(i) for i in client.query(query_top_state)]
    print("\n".join(state))
    print("-"*100)
    print("\nТоп городов и посёлков: ")
    city_or_county = [str(i) for i in client.query(query_top_city_or_county)]
    print("\n".join(city_or_county))
    print("-" * 100)
    print("\nКоличество случаев когда оружие было украдено или статус неизвестен: ")
    print(client.query(query_guns))

