import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count, round as spark_round

log = logging.getLogger(__name__)

def build_gold(df: DataFrame) -> DataFrame:
    """
    Gold layer — business-ready aggregated summary.
    This is what analysts and Power BI connect to.
    """
    log.info("Building Gold layer aggregations...")

    # ── Data Quality Gate ──────────────────────────────────
    completed_count = df.filter(col("status") == "COMPLETED").count()

    if completed_count == 0:
        raise ValueError(
            "PIPELINE ABORTED: Zero COMPLETED orders found in Silver layer. "
            "Possible cause: status column not standardized to uppercase. "
            "Check transform.py clean_orders() function."
        )

    log.info(f"Quality check passed: {completed_count} COMPLETED orders found")
    # ───────────────────────────────────────────────────────

    gold_df = df \
        .filter(col("status") == "COMPLETED") \
        .groupBy("country", "product", "category") \
        .agg(
            spark_sum("net_revenue").alias("total_revenue"),
            spark_sum("quantity").alias("units_sold"),
            count("order_id").alias("order_count"),
            spark_round(
                spark_sum("net_revenue") / count("order_id"), 2
            ).alias("avg_order_value")
        ) \
        .orderBy("total_revenue", ascending=False)

    log.info(f"Gold layer: {gold_df.count()} summary rows built")
    return gold_df


def save_gold(df: DataFrame, gold_path: str) -> None:
    log.info(f"Saving Gold layer to: {gold_path}")

    df.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .parquet(gold_path)

    log.info("Gold layer saved successfully ✅")