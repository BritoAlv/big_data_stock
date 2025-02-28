import json
import os
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
        crypto_data = fetch_crypto_data(symbol)

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

        time.sleep(1)  # Prevent API rate limit issues

    print("Cycle complete, sleeping for 60 seconds...")
    time.sleep(60)  # Main sleep to avoid API rate limit