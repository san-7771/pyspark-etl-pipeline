import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, round as spark_round,
    to_date, upper, trim
)
from delta.tables import DeltaTable

log = logging.getLogger(__name__)


def clean_orders(df: DataFrame, valid_statuses: list) -> DataFrame:
    """
    Silver layer — exactly the same logic as before.
    Transformations don't care whether output is Parquet or Delta.
    Silver layer transformations:
    - Drop nulls in critical columns
    - Standardize text fields
    - Fix data types
    - Filter invalid statuses
    """
    log.info("Starting Silver transformations...")
    before = df.count()

    df_clean = df \
        .dropna(subset=["order_id", "customer_id", "product"]) \
        .filter(col("status").isin(valid_statuses)) \
        .withColumn("product",    trim(upper(col("product")))) \
        .withColumn("country",    trim(upper(col("country")))) \
        .withColumn("status",     trim(upper(col("status")))) \
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
        .dropDuplicates(["order_id"])

    after = df_clean.count()
    log.info(f"Silver: {before} → {after} rows (dropped {before - after})")

    return df_clean


def enrich_orders(df: DataFrame,
                  discount_rate: float,
                  electronics_category: str) -> DataFrame:
    """
    Add business metric columns — unchanged from Phase 1.
    - total_revenue
    - discount
    - net_revenue
    """
    log.info("Enriching with revenue columns...")

    df_enriched = df \
        .withColumn("total_revenue",
            col("quantity") * col("unit_price")) \
        .withColumn("discount",
            when(col("category") == electronics_category,
                 col("total_revenue") * discount_rate)
            .otherwise(0)) \
        .withColumn("net_revenue",
            spark_round(col("total_revenue") - col("discount"), 2))

    return df_enriched


def save_silver(df: DataFrame, silver_path: str) -> None:
    """
    Save Silver layer as Delta table.

    WHY OVERWRITE FOR SILVER?
    Because we reprocess ALL orders every run.
    Silver = full clean snapshot of today's data.
    If we used append, we'd duplicate rows on every run.
    """
    log.info(f"Saving Silver layer to: {silver_path}")

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true").save(silver_path)         # safe schema changes

    version = DeltaTable.forPath(df.sparkSession, silver_path) \
                        .history(1) \
                        .collect()[0]["version"]

    log.info(f"Silver saved as Delta version {version} ✅")

def export_silver_for_dbt(spark, silver_path: str,
                           bq_table: str, temp_gcs_bucket: str) -> None:
    """
    Load Silver layer into BigQuery for dbt to transform.
    dbt works on row-level data — it does its own aggregations.
    Never point dbt at an already-aggregated Gold table.
    """
    log.info(f"Exporting Silver to BigQuery for dbt: {bq_table}")

    df = spark.read.format("delta").load(silver_path)

    df.write \
        .format("bigquery") \
        .option("table", bq_table) \
        .option("temporaryGcsBucket", temp_gcs_bucket) \
        .mode("overwrite") \
        .save()

    log.info(f"Silver exported to BigQuery ✅ rows: {df.count()}")