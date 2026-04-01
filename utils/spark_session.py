import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str, master: str) -> SparkSession:
    """
    Factory function — always use this to create SparkSession.
    Never create SparkSession directly in pipeline code.
    """
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    os.environ['JAVA_HOME']   = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot'

    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.driver.host",        "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark