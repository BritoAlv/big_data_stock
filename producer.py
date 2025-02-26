import os

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_HOST', 'localhost:9092'))
producer.send("stock_prices", b"Test message")
producer.flush()

print("Message sent!")