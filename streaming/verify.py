from consumer import get_streaming_spark

spark = get_streaming_spark()

df = spark.read.format("delta").load(
    "gs://de-pipeline-sanyam-2026/streaming/orders_delta"
)
print(f"Total streaming orders received: {df.count()}")
df.groupBy("country", "status").count().orderBy("count", ascending=False).show()