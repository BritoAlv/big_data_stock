import os
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    'stock_prices',
    bootstrap_servers=os.environ.get('KAFKA_HOST', 'localhost:9092'),
    auto_offset_reset='earliest',  # Start reading at the earliest message
    enable_auto_commit=True,       # Automatically commit offsets
    group_id='my-group',           # Consumer group ID
    value_deserializer=lambda x: x.decode('utf-8')  # Deserialize bytes to string
)

try:
    # Continuously read messages from the topic
    for message in consumer:
        # Print the message value
        print(f"Received message: {message.value}")
except KafkaError as e:
    print(f"An error occurred: {e}")
finally:
    # Close the consumer
    consumer.close()
