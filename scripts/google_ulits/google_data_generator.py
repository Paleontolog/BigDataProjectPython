import json
import time
import pandas as pd
from google.cloud import pubsub

with open('configPubSub.json') as json_data_file:
    app_config = json.load(json_data_file)


def create_topic(publisher, topic_path):
    try:
        topic = publisher.create_topic(topic_path)
        print("Topic created: {}".format(topic))
    except Exception as e:
        print(e)


def delete_topic(publisher, topic_path):
    publisher.delete_topic(topic_path)


def list_topics(publisher):
    project_path = publisher.project_path(app_config["project_id"])

    for topic in publisher.list_topics(project_path):
        print(topic)


def create_subscription(subscriber, topic_path):
    subscription_path = subscriber.subscription_path(
        app_config["project_id"], app_config["subscription_name"]
    )
    try:
        subscription = subscriber.create_subscription(
            subscription_path, topic_path
        )
        print(subscription)
    except Exception as e:
        print(e)


def publish_messages(publisher, data, topic_path):
    for row in data.iterrows():
        data = json.dumps(dict(row[1])).encode("utf-8")
        future = publisher.publish(topic_path,
                                   data=data)
        print(future.result())
        time.sleep(app_config["pubsub_delay"])


if __name__ == "__main__":
    data = pd.read_csv(app_config["data_filepath"])
    publisher = pubsub.PublisherClient(client_config=app_config["retry_settings"])
    subscriber = pubsub.SubscriberClient()
    topic_path = f'projects/{app_config["project_id"]}/topics/{app_config["topic_name"]}'
    create_topic(publisher, topic_path)
    create_subscription(subscriber, topic_path)
    publish_messages(publisher, data, topic_path)
