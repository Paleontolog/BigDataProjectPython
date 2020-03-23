import json
import time
import pandas as pd
from google.cloud import pubsub

with open('configPubSub.json') as json_data_file:
    app_config = json.load(json_data_file)


def receive_message(subscriber, timeout=None):

        subscription_path = subscriber.subscription_path(
            app_config["project_id"], app_config["subscription_name"]
        )

        def callback(message):
            print("Received message: {}".format(message))
            message.ack()

        streaming_pull_future = subscriber.subscribe(
            subscription_path, callback=callback
        )
        print("Listening for messages on {}..\n".format(subscription_path))

        try:
            streaming_pull_future.result(timeout=timeout)
        except:  # noqa
            streaming_pull_future.cancel()


if __name__ == "__main__":
    subscriber = pubsub.SubscriberClient()
    topic_path = f'projects/{app_config["project_id"]}/topics/{app_config["topic_name"]}'
    receive_message(subscriber, 10)
