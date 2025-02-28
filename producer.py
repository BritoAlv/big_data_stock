import json
import os
import time

import requests
from kafka import KafkaProducer

with open("api_key.txt", "r") as f:
    API_KEY = f.read().strip()

STOCK_SYMBOL = "AAPL"
KAFKA_TOPIC = "stock_prices"

producer = KafkaProducer(
    bootstrap_servers=os.environ.get('KAFKA_HOST', 'localhost:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def fetch_stock_data():
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_SYMBOL}&interval=5min&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()
    return data.get('Time Series (5min)', {})


while True:
    stock_data = fetch_stock_data()
    if stock_data:
        for timestamp, values in stock_data.items():
            record = {
                "timestamp": timestamp,
                "symbol": STOCK_SYMBOL,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"]),
            }
            print(f"Producing: {record}")
            producer.send(KAFKA_TOPIC, record)
        time.sleep(60)