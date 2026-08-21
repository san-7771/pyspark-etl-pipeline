import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['JAVA_HOME']   = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot'

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Delta Lake requires extra Spark configuration
# configure_spark_with_delta_pip handles this automatically
builder = SparkSession.builder \
    .appName("DeltaTest") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ Delta Lake is working! Version:", spark.version)
spark.stop()