import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name: str, master: str) -> SparkSession:
    """
    Delta + GCS enabled SparkSession.
    Same pattern as gcs_test.py, now reusable across the whole pipeline.
    """
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    os.environ['JAVA_HOME']   = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot'

    GCP_KEY_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "gcp-key.json"
    )
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_KEY_PATH

    GCS_JAR = r"C:\hadoop\bin\gcs-connector-hadoop3-latest.jar"
    BQ_JAR  = r"C:\hadoop\bin\spark-bigquery-connector.jar"

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.driver.host",        "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "4") \
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
                GCP_KEY_PATH)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark