from kafka.admin import KafkaAdminClient, NewTopic

from common import KAFKA_TOPIC


def create_kafka_topic(topic_n, num_partitions=1, replication_factor=1, bootstrap_servers="localhost:9092"):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    for topic in admin_client.list_topics():
        if topic_n == topic:
            print(f"Topic '{topic_n}' already exists.")
            return
    topic_list = [NewTopic(name=topic_n, num_partitions=num_partitions, replication_factor=replication_factor)]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_n}' created successfully.")
    except Exception as e:
        print(f"An error occurred while creating topic '{topic_n}': {e}")
    finally:
        admin_client.close()

if __name__ == '__main__':
    create_kafka_topic(KAFKA_TOPIC)