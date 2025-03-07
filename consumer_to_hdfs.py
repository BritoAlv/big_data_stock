import time
from datetime import datetime
import json
from hdfs import InsecureClient
from kafka import KafkaConsumer

from common import KAFKA_TOPIC, KAFKA_SERVER, CurrencyOHLCV

HDFS_URL = "http://localhost:9870"
HDFS_DIR = "/user/data/"

try:
    client = InsecureClient(HDFS_URL, user="root")
    if client.status(HDFS_DIR, strict=False) is None:
        client.makedirs(HDFS_DIR)
except Exception as e:
    print(e)


def save_data(data_to_persist):
    file_name = f"data{data_to_persist["symbol"]}{data_to_persist["date"]}.json"
    filepath = f"{HDFS_DIR}{file_name}"
    try:
        with client.write(filepath, encoding="utf-8") as writer:
            json.dump(data_to_persist, writer)
    except Exception as e:
        print(f"Error {e} while writing {file_name} to HDFS")


consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER, value_deserializer=lambda x: json.loads(x.decode("utf-8")))

while True:
    time.sleep(5)
    for message in consumer:
        data = message.value
        save_data(data)