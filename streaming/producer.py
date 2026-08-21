"""
Simulates a live e-commerce order stream.
Publishes one random order per second to Pub/Sub.
In production this would be your actual order management system.
"""
import json
import time
import random
import logging
from datetime import datetime
from google.cloud import pubsub_v1
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
    r'C:\Users\Sanyam\etl_project\gcp-key.json'

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(message)s")
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────
PROJECT_ID = "de-learning-project-499519"
TOPIC_ID   = "orders-topic"

PRODUCTS   = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]
CATEGORIES = {"Laptop": "Electronics", "Phone": "Electronics",
              "Tablet": "Electronics", "Monitor": "Electronics",
              "Keyboard": "Accessories"}
PRICES     = {"Laptop": 999, "Phone": 499, "Tablet": 299,
              "Monitor": 199, "Keyboard": 49}
COUNTRIES  = ["India", "USA", "UK", "Germany", "Canada"]
STATUSES   = ["Completed", "Pending", "Cancelled"]


def generate_order(order_id: int) -> dict:
    """
    Generates a realistic random order.
    Returns a dict — will be serialized to JSON for Pub/Sub.
    """
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
                           STATUSES,
                           weights=[0.7, 0.2, 0.1]  # 70% completed
                       )[0],
        "timestamp":   datetime.now().isoformat()    # when this event occurred
    }


def publish_orders(num_orders: int = 50, delay_seconds: float = 1.0):
    """
    Publishes orders to Pub/Sub one at a time with a delay.

    PublisherClient: the connection to Pub/Sub
    topic_path: full resource path GCP needs to identify the topic
    message bytes: Pub/Sub only accepts bytes, not strings or dicts
                   so we serialize dict → JSON string → UTF-8 bytes
    future.result(): waits for publish confirmation before continuing
                     ensures message was actually received by Pub/Sub
    """
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    log.info(f"Starting order stream → {topic_path}")
    log.info(f"Publishing {num_orders} orders, 1 per {delay_seconds}s")

    for i in range(num_orders):
        order     = generate_order(order_id=2000 + i)
        # Convert dict → JSON string → bytes
        message   = json.dumps(order).encode("utf-8")

        future    = publisher.publish(topic_path, message)
        msg_id    = future.result()  # blocks until confirmed

        log.info(f"Published order {order['order_id']} "
                 f"| {order['product']} | {order['country']} "
                 f"| {order['status']} | msg_id={msg_id}")

        time.sleep(delay_seconds)

    log.info(f"✅ Published {num_orders} orders successfully")


if __name__ == "__main__":
    publish_orders(num_orders=50, delay_seconds=1.0)