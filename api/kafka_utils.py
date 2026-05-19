"""
Kafka utility functions for the API
"""

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9091', 'localhost:9092', 'localhost:9093']
TOPIC_NAME = 'sensor-events'


def get_topic_metadata(topic_name=TOPIC_NAME):
    """Get metadata for a specific topic"""
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='api_metadata_fetcher'
    )

    try:
        metadata = admin.describe_topics([topic_name])
        return metadata[0] if metadata else None
    except Exception as e:
        print(f"Error fetching topic metadata: {e}")
        return None
    finally:
        admin.close()


def publish_to_kafka(topic, key, value, bootstrap_servers=None):
    """Publish a message to Kafka"""
    servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS

    producer = KafkaProducer(
        bootstrap_servers=servers,
        acks='all',
        retries=3,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

    try:
        future = producer.send(topic, key=key, value=value)
        result = future.get(timeout=10)
        return {
            'partition': result.partition,
            'offset': result.offset,
            'timestamp': result.timestamp
        }
    finally:
        producer.close()