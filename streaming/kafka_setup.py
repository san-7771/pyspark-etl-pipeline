"""
Creates Kafka topic with explicit partition and replication settings.
In Pub/Sub you never think about partitions — Google manages it.
In Kafka you decide this upfront based on your throughput needs.
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME   = "orders-stream"
NUM_PARTITIONS      = 3   # 3 partitions = 3 consumers can work in parallel
REPLICATION_FACTOR  = 1   # 1 = no replication (fine for local dev)
                           # production uses 3 = survive 2 broker failures

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_BROKER,
    client_id="etl-admin"
)

topic = NewTopic(
    name=TOPIC_NAME,
    num_partitions=NUM_PARTITIONS,
    replication_factor=REPLICATION_FACTOR
)

try:
    admin_client.create_topics([topic])
    print(f"✅ Topic '{TOPIC_NAME}' created")
    print(f"   Partitions: {NUM_PARTITIONS}")
    print(f"   Replication: {REPLICATION_FACTOR}")
except TopicAlreadyExistsError:
    print(f"ℹ️  Topic '{TOPIC_NAME}' already exists")

# List all topics to confirm
topics = admin_client.list_topics()
print(f"\nAll topics in Kafka: {topics}")

admin_client.close()