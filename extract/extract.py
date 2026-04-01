import logging
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

def extract_orders(spark: SparkSession, source_path: str) -> DataFrame:
    """]
    Extract raw orders from CSV source.
    Returns a raw DataFrame — no transformations here.
    """
    log.info(f"Extracting data from: {source_path}")

    df = spark.read.csv(
        source_path,
        header=True,
        inferSchema=True
    )

    row_count = df.count()
    log.info(f"Extracted {row_count} rows, {len(df.columns)} columns")

    return df


def save_bronze(df: DataFrame, bronze_path: str) -> None:
    """
    Save raw data as-is to Bronze layer.
    Bronze = exact copy of source, never modified.
    """
    log.info(f"Saving Bronze layer to: {bronze_path}")

    df.write.mode("overwrite").parquet(bronze_path)

    log.info("Bronze layer saved successfully")