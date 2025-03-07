import json
import time

import requests
from kafka import KafkaProducer

from common import CurrencyOHLCV, Currency_Symbols, KAFKA_SERVER, KAFKA_TOPIC

with open("api_key.txt") as f:
    api_key = f.read()

_DATA_KEY = 'Time Series (5min)'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v.__dict__).encode("utf-8"),
)

def fetch_data(currency_symbol : str, interval: int):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={currency_symbol}&interval={interval}min&apikey={api_key}"
    r = requests.get(url)
    if r.status_code == 200:
        parsed = r.json()
        if _DATA_KEY not in parsed:
            print(f"Error: {parsed}")
            return
        for row in parsed[_DATA_KEY]:
            yield CurrencyOHLCV(currency_symbol, row, parsed[_DATA_KEY][row]['1. open'], parsed[_DATA_KEY][row]['2. high'], parsed[_DATA_KEY][row]['3. low'], parsed[_DATA_KEY][row]['4. close'], parsed[_DATA_KEY][row]['5. volume'])

if __name__ == '__main__':
    while True:
        for symbol in Currency_Symbols:
            for data in fetch_data(symbol, 5):
                producer.send(KAFKA_TOPIC, data)
                print(f"Sent {data} to Kafka")
                file_name = f"data{data.symbol}{data.date}.json"
                with open("./" + file_name, mode = "w") as f:
                    json.dump(data.__dict__, f)
            print("Waiting 10 seconds to do another API Request")
            time.sleep(10)