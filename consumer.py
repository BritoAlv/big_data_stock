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
MARKET = "USD"  # Market currency

KAFKA_TOPIC = "crypto_prices"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=os.environ.get("KAFKA_HOST", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_crypto_data(symbol):
    """
    Fetches intraday crypto prices from Alpha Vantage.
    """
    url = f"https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol={symbol}&market={MARKET}&interval=5min&apikey={API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Error fetching {symbol}: {response.text}")
        return {}

    data = response.json()
    return data.get("Time Series Crypto (5min)", {})

while True:
    for symbol in CRYPTO_SYMBOLS:
        crypto_data = fetch_crypto_data(symbol)

        if crypto_data:
            for timestamp, values in crypto_data.items():
                record = {
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": float(values["5. volume"]),  # Crypto volume can be decimal
                }
                print(f"Producing: {record}")
                producer.send(KAFKA_TOPIC, record)

        time.sleep(20)  # Prevent exceeding API rate limits

    print("Cycle complete, sleeping for 60 seconds...")
    time.sleep(60)  # Main sleep to avoid API rate limit
