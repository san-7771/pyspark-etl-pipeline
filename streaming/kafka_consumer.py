"""
PySpark Structured Streaming from Kafka.
This version uses TRUE Structured Streaming (readStream)
unlike the Pub/Sub version which used micro-batch polling.

Key difference from Pub/Sub consumer:
- No manual polling loop (while True)
- No manual acknowledgment (Kafka tracks offsets automatically)
- Spark manages checkpointing and offset tracking
- More scalable and production-grade
"""
import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['JAVA_HOME']   = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
    r'C:\Users\Sanyam\etl_project\gcp-key.json'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, upper, trim,
    when, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)
# from delta import configure_spark_with_delta_pip

GCS_JAR    = r"C:\hadoop\bin\gcs-connector-hadoop3-latest.jar"
BQ_JAR     = r"C:\hadoop\bin\spark-bigquery-connector.jar"
GCP_KEY    = r"C:\Users\Sanyam\etl_project\gcp-key.json"
# KAFKA_JAR        = r"C:\hadoop\bin\spark-sql-kafka.jar"
# KAFKA_CLIENT_JAR = r"C:\hadoop\bin\kafka-clients-3.4.1.jar"

KAFKA_BROKER  = "localhost:9092"
TOPIC_NAME    = "orders-stream"
BUCKET        = "gs://de-pipeline-sanyam-2026"
CHECKPOINT    = f"{BUCKET}/checkpoints/kafka_orders"
OUTPUT_PATH   = f"{BUCKET}/streaming/kafka_orders_delta"

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


def get_spark() -> SparkSession:
    builder = SparkSession.builder \
        .appName("KafkaOrdersConsumer") \
        .master("local[*]") \
        .config("spark.driver.host",        "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory",      "4g") \
        .config("spark.executor.memory",    "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars", f"{GCS_JAR},{BQ_JAR}") \
        .config("spark.jars.packages", # Delta + Kafka dependencies
                "io.delta:delta-spark_2.12:3.0.0,"
                # ── Kafka + all dependencies auto-downloaded ───────
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.extensions", # Delta configuration
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.gs.impl", # GCS configuration
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable",
                "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                GCP_KEY)

    return builder.getOrCreate()


def run_kafka_streaming():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("🚀 Starting Kafka Structured Streaming consumer")
    print(f"   Broker: {KAFKA_BROKER}")
    print(f"   Topic:  {TOPIC_NAME}")
    print(f"   Output: {OUTPUT_PATH}")

    # ── READ STREAM FROM KAFKA ───────────────────────────
    # startingOffsets="latest" → only process NEW messages
    # startingOffsets="earliest" → reprocess ALL messages from beginning
    # This is Kafka's replay capability — Pub/Sub can't do this!

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe",               TOPIC_NAME) \
        .option("startingOffsets",         "earliest") \
        .option("failOnDataLoss",          "false") \
        .load()

    # Kafka gives you these columns automatically:
    # key       → message key (customer_id in our case) as bytes
    # value     → message content (order JSON) as bytes
    # topic     → which topic this came from
    # partition → which partition (0, 1, or 2)
    # offset    → position within that partition
    # timestamp → when Kafka received it

    # We only need value — cast bytes to string, then parse JSON
    parsed_stream = raw_stream \
        .select(
            from_json(
                col("value").cast("string"),
                ORDER_SCHEMA
            ).alias("order"),
            col("partition"),   # keep for monitoring
            col("offset")       # keep for monitoring
        ) \
        .select("order.*", "partition", "offset")

    # ── TRANSFORM ────────────────────────────────────────
    transformed = parsed_stream \
        .filter(col("status").isin(["Completed","Pending","Cancelled"])) \
        .withColumn("product", trim(upper(col("product")))) \
        .withColumn("country", trim(upper(col("country")))) \
        .withColumn("status",  trim(upper(col("status")))) \
        .withColumn("total_revenue",
            col("quantity") * col("unit_price")) \
        .withColumn("discount",
            when(col("category") == "Electronics",
                 col("total_revenue") * 0.10).otherwise(0)) \
        .withColumn("net_revenue",
            spark_round(col("total_revenue") - col("discount"), 2))

    # ── WRITE STREAM TO DELTA ────────────────────────────
    # Spark tracks Kafka offsets in the checkpoint directory
    # No manual ack needed — Spark handles offset management
    query = transformed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", CHECKPOINT) \
        .option("mergeSchema", "true") \
        .start(OUTPUT_PATH)

    print("✅ Streaming query running!")
    print("   Run kafka_producer.py in another terminal")
    print("   Watch Kafka UI at localhost:8081")
    print("   Press Ctrl+C to stop\n")

    query.awaitTermination()


if __name__ == "__main__":
    run_kafka_streaming()