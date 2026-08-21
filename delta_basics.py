import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['JAVA_HOME']   = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

builder = SparkSession.builder \
    .appName("DeltaBasics") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── CREATE SAMPLE DATA ─────────────────────────────────────────
# Simulating Day 1 customer data
day1_data = [
    (1, "Sanyam",  "sanyam@email.com",  "Delhi",   "Gold"),
    (2, "Priya",   "priya@email.com",   "Mumbai",  "Silver"),
    (3, "Rahul",   "rahul@email.com",   "Chennai", "Bronze"),
    (4, "Anita",   "anita@email.com",   "Pune",    "Gold"),
    (5, "Vikram",  "vikram@email.com",  "Delhi",   "Silver"),
]
columns = ["customer_id", "name", "email", "city", "tier"]

df_day1 = spark.createDataFrame(day1_data, columns)

# ── WRITE AS DELTA TABLE ───────────────────────────────────────
DELTA_PATH = "data/delta/customers"

df_day1.write \
    .format("delta").mode("overwrite").save(DELTA_PATH)
# ← only change from .parquet() is adding the .format("delta")

print("✅ Delta table written!")
print(f"   Path: {DELTA_PATH}")

# ── READ IT BACK ───────────────────────────────────────────────
df_read = spark.read.format("delta").load(DELTA_PATH)
print("\n=== Day 1 Customers ===")
df_read.show()

# ── INSPECT THE TRANSACTION LOG ────────────────────────────────
print("=== Delta Transaction History ===")
# spark.sql(f"DESCRIBE HISTORY delta.`{DELTA_PATH}`").show(truncate=False)
# replacing the above line with the next line since DESCRIBE HISTORY on Windows — backslashes confuse the SQL parser
# Replace the DESCRIBE HISTORY line with the Python API which handles Windows paths correctly:
DeltaTable.forPath(spark, DELTA_PATH).history().show(truncate=False)

# Block A — APPEND a new record (Version 1)
# Day 2 — one new customer joins
day2_new = [(6, "Neha", "neha@email.com", "Bangalore", "Bronze")]
df_day2 = spark.createDataFrame(day2_new, columns)

df_day2.write \
    .format("delta") \
    .mode("append").save(DELTA_PATH)
# append = add rows, don't replace existing

print("=== After Append (6 customers now) ===")
spark.read.format("delta").load(DELTA_PATH).orderBy("customer_id").show()

# Check history — should now show new versions (version 1 if above code ran once only before it)
print("=== History after append ===")
DeltaTable.forPath(spark, DELTA_PATH).history(3).show(truncate=False)

# Block B — TIME TRAVEL (The Most Impressive Feature)
# Read data AS IT WAS at version 0 — before Neha was added
print("=== Time Travel: Version 0 (before append) ===")
df_v0 = spark.read \
    .format("delta") \
    .option("versionAsOf", 0).load(DELTA_PATH)
# ← go back to version 0

df_v0.orderBy("customer_id").show()
# Should show only 5 customers — Neha doesn't exist yet

print("=== Time Travel: Version 1 (after append) ===")
df_v1 = spark.read \
    .format("delta") \
    .option("versionAsOf", 1).load(DELTA_PATH)
# ← current version

df_v1.orderBy("customer_id").show()
# Should show 6 customers including Neha

# Block C — MERGE / Upsert (Most Used in Production)
# Day 3 — two things happen simultaneously:
# 1. Sanyam upgrades from Gold to Platinum tier
# 2. A brand new customer Amit joins
day3_updates = [
    (1, "Sanyam", "sanyam@email.com", "Delhi",     "Platinum"),  # existing — tier changed
    (7, "Amit",   "amit@email.com",   "Hyderabad", "Silver"),    # new customer
]
df_updates = spark.createDataFrame(day3_updates, columns)

# Load the existing Delta table as the TARGET
delta_table = DeltaTable.forPath(spark, DELTA_PATH)

# MERGE: match on customer_id
# If match found  → UPDATE that row with new values
# If no match     → INSERT as new row
delta_table.alias("target") \
    .merge(
        df_updates.alias("source"),
        "target.customer_id = source.customer_id"  # join condition
    ) \
    .whenMatchedUpdateAll() .whenNotMatchedInsertAll(). execute()
# if customer_id exists → update ALL columns
# if customer_id is new → insert full row

print("=== After MERGE ===")
spark.read.format("delta").load(DELTA_PATH).orderBy("customer_id").show()
# Sanyam should show Platinum now
# Amit should appear as new row
# Everyone else unchanged

# Day 4 — business adds a new "loyalty_points" column
# Without schema evolution this would FAIL
day4_data = [(8, "Deepa", "deepa@email.com", "Kolkata", "Gold", 500)]
new_columns = ["customer_id", "name", "email", "city", "tier", "loyalty_points"]
df_day4 = spark.createDataFrame(day4_data, new_columns)

# mergeSchema=True tells Delta: accept new columns, add them to table
df_day4.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true").save(DELTA_PATH)   # ← allow new columns

print("=== After Schema Evolution ===")
final_df = spark.read.format("delta").load(DELTA_PATH).orderBy("customer_id")
final_df.show()
# Old customers have null for loyalty_points — that's correct
# Deepa has 500 loyalty_points

print("=== Final Schema ===")
final_df.printSchema()
# loyalty_points column should now appear

print("=== Complete History — All 4 Versions ===")
DeltaTable.forPath(spark, DELTA_PATH).history().show(truncate=False)





# Add this to delta_basics.py and run it

print("=== Before OPTIMIZE ===")
import os
delta_files_before = [f for f in os.listdir("data/delta/customers")
                      if f.endswith(".parquet")]
print(f"Number of Parquet files: {len(delta_files_before)}")

# OPTIMIZE compacts all small files into fewer large files
# Rule of thumb: aim for files of ~128MB each
DeltaTable.forPath(spark, DELTA_PATH).optimize().executeCompaction()

print("=== After OPTIMIZE ===")
delta_files_after = [f for f in os.listdir("data/delta/customers")
                     if f.endswith(".parquet")]
print(f"Number of Parquet files: {len(delta_files_after)}")

# Read still works perfectly after optimize
print("=== Data intact after OPTIMIZE ===")
spark.read.format("delta").load(DELTA_PATH).orderBy("customer_id").show()


# VACUUM removes files no longer needed by time travel
# Default retention = 7 days
# Files older than 7 days = permanently deleted

# For learning, we'll use 0 hours (delete everything not in current version)
# In production NEVER go below 7 days — breaks time travel

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

DeltaTable.forPath(spark, DELTA_PATH).vacuum(retentionHours=0)

print("=== After VACUUM ===")
delta_files_vacuum = [f for f in os.listdir("data/delta/customers")
                      if f.endswith(".parquet")]
print(f"Files remaining: {len(delta_files_vacuum)}")
print("Old versions now deleted — time travel only works for recent versions")

# Verify current data still intact
spark.read.format("delta").load(DELTA_PATH).orderBy("customer_id").show()

spark.stop()