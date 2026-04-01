import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, round as spark_round,
    to_date, upper, trim
)

log = logging.getLogger(__name__)

def clean_orders(df: DataFrame, valid_statuses: list) -> DataFrame:
    """
    Silver layer transformations:
    - Drop nulls in critical columns
    - Standardize text fields
    - Fix data types
    - Filter invalid statuses
    """
    log.info("Starting Silver transformations...")
    before = df.count()

     # ⚠️ Convert valid_statuses to uppercase to match our standardization
    valid_statuses_upper = [s.upper() for s in valid_statuses]

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
    Add business metric columns:
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
    log.info(f"Saving Silver layer to: {silver_path}")
    df.write.mode("overwrite").parquet(silver_path)
    log.info("Silver layer saved successfully")