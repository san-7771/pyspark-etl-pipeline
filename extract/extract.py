import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)


def extract_orders(spark: SparkSession, source_path: str) -> DataFrame:
    """
    Extract raw orders from CSV source.
    Reading never changes — Delta only affects writes.
    """
    log.info(f"Extracting data from: {source_path}")

    df = spark.read.csv(
        source_path,
        header=True,
        inferSchema=True
    )
      # ← ADD THIS: force order_date to DateType always
    df = df.withColumn("order_date", to_date(col("order_date")))

    row_count = df.count()
    log.info(f"Extracted {row_count} rows, {len(df.columns)} columns")

    return df


def save_bronze(df: DataFrame, bronze_path: str) -> None:
    """
    Save raw data to Bronze layer as Delta table.

    WHY DELTA FOR BRONZE?
    - Every source file ingestion = new Delta version
    - If transform has a bug you can time-travel to exact ingestion moment
    - Schema enforcement catches source sending wrong column types
    - Overwrite is atomic — reader never sees half-written Bronze
    """
    log.info(f"Saving Bronze layer to: {bronze_path}")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_path)             # ← was .parquet()

    # Log exactly what version was created
    from delta.tables import DeltaTable
    version = DeltaTable.forPath(df.sparkSession, bronze_path) \
                        .history(1) \
                        .collect()[0]["version"]

    log.info(f"Bronze saved as Delta version {version} ✅")