import datetime
import json
import os
import random
import time
import requests
from kafka import KafkaProducer

# Read API Key from file
with open("api_key.txt", "r") as f:
    API_KEY = f.read().strip()

# List of cryptocurrency symbols to fetch
CRYPTO_SYMBOLS = ["BTC", "ETH", "LTC"]  # Bitcoin, Ethereum, Litecoin

KAFKA_TOPIC = "stock_prices"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=os.environ.get("KAFKA_HOST", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_mock_data(symbol):
    """
    Generates mock cryptocurrency data.
    """
    now = datetime.datetime.now()
    data = {}
    for i in range(20):  # Generate data for the last 100 minutes
        timestamp = now - datetime.timedelta(minutes=5 * i)
        open_price = random.uniform(100, 500)
        high_price = open_price + random.uniform(0, 50)
        low_price = open_price - random.uniform(0, 50)
        close_price = random.uniform(low_price, high_price)
        volume = random.randint(1000, 5000)
        data[timestamp.strftime("%Y-%m-%d %H:%M:%S")] = {
            "1. open": open_price,
            "2. high": high_price,
            "3. low": low_price,
            "4. close": close_price,
            "5. volume": volume,
        }
    return data

def fetch_crypto_data(symbol):
    """
    Fetches cryptocurrency data from Alpha Vantage (free-tier).
    """
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Error fetching {symbol}: {response.text}")
        return {}

    data = response.json()

    # Extract the time series data from the response
    time_series_key = "Time Series (5min)"
    return data.get(time_series_key, {})

while True:
    for symbol in CRYPTO_SYMBOLS:
        crypto_data = generate_mock_data(symbol)

        if crypto_data:
            for timestamp, values in crypto_data.items():
                try:
                    record = {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"]),
                    }
                    print(f"Producing: {record}")
                    producer.send(KAFKA_TOPIC, record)
                except (KeyError, ValueError) as e:
                    print(f"Skipping invalid data for {symbol} at {timestamp}: {e}")

        time.sleep(1)  # Prevent API rate limit issues``

    print("Cycle complete, sleeping for 60 seconds...")
    time.sleep(1)  # Main sleep to avoid API rate limit