"""
PySpark Structured Streaming consumer.
Reads orders from Pub/Sub in real-time and writes to Delta Lake on GCS.

IMPORTANT CONCEPT:
Structured Streaming uses the SAME DataFrame API as batch PySpark.
The difference is readStream instead of read, writeStream instead of write.
Your existing transformation logic works with zero changes.
"""
import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['JAVA_HOME']   = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
    r'C:\Users\Sanyam\etl_project\gcp-key.json'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    upper, trim, when, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from delta import configure_spark_with_delta_pip

GCS_JAR = r"C:\hadoop\bin\gcs-connector-hadoop3-latest.jar"
BQ_JAR  = r"C:\hadoop\bin\spark-bigquery-connector.jar"
GCP_KEY = r"C:\Users\Sanyam\etl_project\gcp-key.json"

PROJECT_ID    = "de-learning-project-499519"
SUBSCRIPTION  = "projects/de-learning-project-499519/subscriptions/orders-subscription"
BUCKET        = "gs://de-pipeline-sanyam-2026"
CHECKPOINT    = f"{BUCKET}/checkpoints/streaming_orders"
OUTPUT_PATH   = f"{BUCKET}/streaming/orders_delta"

# ── Schema of messages coming from Pub/Sub ───────────────
# Must match exactly what the producer sends
# Defining schema explicitly = faster than inferSchema on streaming
ORDER_SCHEMA = StructType([
    StructField("order_id",    IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product",     StringType(),  True),
    StructField("category",    StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("unit_price",  IntegerType(), True),
    StructField("order_date",  StringType(),  True),
    StructField("country",     StringType(),  True),
    StructField("status",      StringType(),  True),
    StructField("timestamp",   StringType(),  True),
])


def get_streaming_spark() -> SparkSession:
    builder = SparkSession.builder \
        .appName("StreamingOrdersConsumer") \
        .master("local[*]") \
        .config("spark.driver.host",        "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.jars", f"{GCS_JAR},{BQ_JAR}") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable",
                "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                GCP_KEY) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT) \
        .config("spark.driver.memory",         "4g") \
        .config("spark.executor.memory",        "4g") \
        .config("spark.driver.maxResultSize",   "2g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size",    "2g")

    return configure_spark_with_delta_pip(builder).getOrCreate()


# def run_streaming_pipeline():
#     spark = get_streaming_spark()
#     spark.sparkContext.setLogLevel("ERROR")

#     print("🚀 Starting streaming consumer...")
#     print(f"   Reading from: {SUBSCRIPTION}")
#     print(f"   Writing to:   {OUTPUT_PATH}")

#     # ── READ STREAM FROM PUB/SUB ─────────────────────────
#     # Pub/Sub source requires the pubsub spark connector
#     # Each message arrives as a Row with these fields:
#     #   data         → your message bytes (the order JSON)
#     #   messageId    → unique Pub/Sub message identifier
#     #   publishTime  → when Pub/Sub received the message
#     #   attributes   → optional key-value metadata

#     raw_stream = spark.readStream \
#         .format("pubsub") \
#         .option("pubsub.project.id",      PROJECT_ID) \
#         .option("pubsub.subscription.id", "orders-subscription") \
#         .option("pubsub.subscription.create", "false") \
#         .load()

#     # ── PARSE JSON FROM BYTES ─────────────────────────────
#     # data column arrives as binary → decode to string → parse as JSON
#     # from_json() converts JSON string into proper DataFrame columns
#     # using the schema we defined above
#     parsed_stream = raw_stream \
#         .select(
#             from_json(
#                 col("data").cast("string"),  # bytes → string
#                 ORDER_SCHEMA                  # string → structured columns
#             ).alias("order"),
#             col("publishTime").alias("event_time")
#         ) \
#         .select("order.*", "event_time")     # flatten nested struct

#     # ── TRANSFORM (same logic as your batch pipeline!) ───
#     transformed_stream = parsed_stream \
#         .filter(col("status").isin(["Completed", "Pending", "Cancelled"])) \
#         .withColumn("product", trim(upper(col("product")))) \
#         .withColumn("country", trim(upper(col("country")))) \
#         .withColumn("status",  trim(upper(col("status")))) \
#         .withColumn("total_revenue",
#             col("quantity") * col("unit_price")) \
#         .withColumn("discount",
#             when(col("category") == "Electronics",
#                  col("total_revenue") * 0.10)
#             .otherwise(0)) \
#         .withColumn("net_revenue",
#             spark_round(col("total_revenue") - col("discount"), 2))

#     # ── WRITE STREAM TO DELTA ON GCS ─────────────────────
#     # outputMode:
#     #   "append"  → only write NEW rows (correct for event streams)
#     #   "update"  → write rows that changed (for aggregations)
#     #   "complete"→ rewrite entire result (for small aggregation tables)
#     #
#     # trigger:
#     #   processingTime="10 seconds" → micro-batch every 10 seconds
#     #   (not truly real-time but close enough for most use cases)
#     #   once=True → process all available data once then stop
#     #
#     # checkpointLocation:
#     #   CRITICAL — stores which Pub/Sub messages were already processed
#     #   If consumer crashes and restarts, it resumes from checkpoint
#     #   Without this: messages processed twice (duplicates in Delta)

#     query = transformed_stream.writeStream \
#         .format("delta") \
#         .outputMode("append") \
#         .trigger(processingTime="10 seconds") \
#         .option("checkpointLocation", CHECKPOINT) \
#         .start(OUTPUT_PATH)

#     print("✅ Streaming query started!")
#     print("   Waiting for messages from Pub/Sub...")
#     print("   Run producer.py in another terminal to send orders")
#     print("   Press Ctrl+C to stop\n")

#     # awaitTermination() keeps the streaming job running forever
#     # until you press Ctrl+C or an error occurs
#     query.awaitTermination()

def run_streaming_pipeline():
    """
    Micro-batch streaming using Pub/Sub Python client.
    Pulls messages in batches every 10 seconds, processes as a DataFrame.
    Not true streaming but identical to how many production pipelines work —
    'near real-time' micro-batching is the industry standard, not millisecond streaming.
    """
    from google.cloud import pubsub_v1
    import json

    spark = get_streaming_spark()
    spark.sparkContext.setLogLevel("ERROR")

    subscriber   = pubsub_v1.SubscriberClient()
    sub_path     = subscriber.subscription_path(PROJECT_ID, "orders-subscription")

    print("🚀 Starting micro-batch streaming consumer (every 10 seconds)")
    print(f"   Subscription: {sub_path}")
    print(f"   Output: {OUTPUT_PATH}")

    batch_num = 0

    while True:
        # Pull up to 50 messages from Pub/Sub
        response = subscriber.pull(
            request={
                "subscription": sub_path,
                "max_messages": 50
            }
        )

        if not response.received_messages:
            print(f"   Batch {batch_num}: no messages, waiting 10s...")
            import time; time.sleep(10)
            continue

        # Parse messages into a list of dicts
        orders = []
        ack_ids = []
        for msg in response.received_messages:
            order = json.loads(msg.message.data.decode("utf-8"))
            orders.append(order)
            ack_ids.append(msg.ack_id)

        # Convert to Spark DataFrame (same API you already know)
        df = spark.createDataFrame(orders)

        # ── ADD THIS LINE ──
        df = df.coalesce(1)    # merge into 1 partition before writing
                                # reduces memory needed during Delta write

        # Apply same transformations as batch pipeline
        df_transformed = df \
            .filter(col("status").isin(["Completed","Pending","Cancelled"])) \
            .withColumn("product", trim(upper(col("product")))) \
            .withColumn("country", trim(upper(col("country")))) \
            .withColumn("status",  trim(upper(col("status")))) \
            .withColumn("total_revenue", col("quantity") * col("unit_price")) \
            .withColumn("discount",
                when(col("category") == "Electronics",
                     col("total_revenue") * 0.10).otherwise(0)) \
            .withColumn("net_revenue",
                spark_round(col("total_revenue") - col("discount"), 2))

        # Write batch to Delta (append mode — each batch adds new rows)
        df_transformed.write \
            .format("delta") \
            .mode("append") \
            .save(OUTPUT_PATH)

        # Acknowledge messages — tells Pub/Sub "we processed these"
        # If you don't ack, Pub/Sub re-delivers them (exactly-once guarantee)
        subscriber.acknowledge(
            request={"subscription": sub_path, "ack_ids": ack_ids}
        )

        batch_num += 1
        print(f"   Batch {batch_num}: processed {len(orders)} orders "
              f"→ Delta append ✅")

        import time; time.sleep(10)  # wait before next pull


if __name__ == "__main__":
    run_streaming_pipeline()