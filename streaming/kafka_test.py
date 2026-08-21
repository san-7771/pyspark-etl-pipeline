from kafka_consumer import get_spark

spark = get_spark()

df = spark.read.format("delta").load(
    "gs://de-pipeline-sanyam-2026/streaming/kafka_orders_delta"
)

print("Total Rows =", df.count())

# df.show(10, truncate=False)

# df.selectExpr(
#     "min(offset) as min_offset",
#     "max(offset) as max_offset"
# ).show()

# df.groupBy("partition").count().show()

from pyspark.sql.functions import min, max, count

df.groupBy("partition").agg(
    count("*").alias("rows"),
    min("offset").alias("min_offset"),
    max("offset").alias("max_offset")
).orderBy("partition").show()