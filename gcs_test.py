import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['JAVA_HOME']   = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot'
# Tell PySpark where your GCP credentials are
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Sanyam\etl_project\gcp-key.json'

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Path to the manually downloaded JAR
GCS_JAR = r"C:\hadoop\bin\gcs-connector-hadoop3-latest.jar"

builder = SparkSession.builder \
    .appName("GCS_Test") \
    .master("local[*]") \
    .config("spark.driver.host",        "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars",               GCS_JAR) \
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
            r"C:\Users\Sanyam\etl_project\gcp-key.json")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BUCKET = "gs://de-pipeline-sanyam-2026"

# ── READ CSV FROM GCS ───────────────────────────────────
print("Reading orders.csv from GCS...")
df = spark.read.csv(
    f"{BUCKET}/raw/orders.csv",
    header=True,
    inferSchema=True
)
print(f"✅ Successfully read from GCS! Rows: {df.count()}")
df.show(5)

# ── WRITE DELTA TO GCS ──────────────────────────────────
print("\nWriting Delta table to GCS Bronze layer...")
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BUCKET}/bronze/orders_raw_delta")
print("✅ Delta table written to GCS!")

# ── READ DELTA BACK FROM GCS ────────────────────────────
print("\nReading Delta table back from GCS...")
df_back = spark.read.format("delta").load(f"{BUCKET}/bronze/orders_raw_delta")
print(f"✅ Delta read from GCS: {df_back.count()} rows")

spark.stop()