"""
Kafka producer — functionally identical to Pub/Sub producer
but notice the architectural differences:
- No managed service setup, connects directly to broker
- Messages have keys (for partition assignment)
- No futures/callbacks — fire and forget by default
"""
import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(message)s")
log = logging.getLogger(__name__)

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME   = "orders-stream"

PRODUCTS   = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]
CATEGORIES = {"Laptop": "Electronics", "Phone": "Electronics",
              "Tablet": "Electronics", "Monitor": "Electronics",
              "Keyboard": "Accessories"}
PRICES     = {"Laptop": 999, "Phone": 499, "Tablet": 299,
              "Monitor": 199, "Keyboard": 49}
COUNTRIES  = ["India", "USA", "UK", "Germany", "Canada"]
STATUSES   = ["Completed", "Pending", "Cancelled"]


def generate_order(order_id: int) -> dict:
    product = random.choice(PRODUCTS)
    return {
        "order_id":    order_id,
        "customer_id": random.randint(1, 200),
        "product":     product,
        "category":    CATEGORIES[product],
        "quantity":    random.randint(1, 5),
        "unit_price":  PRICES[product],
        "order_date":  datetime.now().strftime("%Y-%m-%d"),
        "country":     random.choice(COUNTRIES),
        "status":      random.choices(
                           STATUSES, weights=[0.7, 0.2, 0.1]
                       )[0],
        "timestamp":   datetime.now().isoformat()
    }


# KafkaProducer config:
# bootstrap_servers → one or more brokers to connect to initially
#                     Kafka discovers the rest automatically
# value_serializer  → how to convert your Python dict to bytes
#                     json.dumps converts dict → JSON string
#                     .encode("utf-8") converts string → bytes
# key_serializer    → how to convert the message key to bytes
#                     keys are used to determine which partition
#                     messages with same key → same partition
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

log.info(f"Starting Kafka producer → topic: {TOPIC_NAME}")

for i in range(50):
    order = generate_order(order_id=3000 + i)

    # key = customer_id ensures all orders for same customer
    # go to the same partition → ordered per customer
    producer.send(
        TOPIC_NAME,
        key=order["customer_id"],
        value=order
    )

    log.info(f"Sent order {order['order_id']} "
             f"| {order['product']:8s} "
             f"| {order['country']:7s} "
             f"| partition key: customer_{order['customer_id']}")

    time.sleep(0.5)

# flush() waits until all pending messages are actually sent
# Without this: script might exit before Kafka receives everything
producer.flush()
producer.close()
log.info(f"✅ All 50 orders sent to Kafka topic '{TOPIC_NAME}'")