import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count, round as spark_round
from delta.tables import DeltaTable
# import os

log = logging.getLogger(__name__)


def build_gold(df: DataFrame) -> DataFrame:
    """
    Build Gold aggregation — same logic as Phase 1.
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
            "Check Silver layer status column standardization."
        )
    log.info(f"Quality gate passed: {completed_count} COMPLETED orders")
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

    log.info(f"Gold: {gold_df.count()} summary rows built")
    return gold_df


def save_gold(df: DataFrame, gold_path: str) -> None:
    """
    Save Gold layer using MERGE instead of overwrite.
                        — works for local disk AND cloud storage.

    WHY MERGE FOR GOLD?
    Gold is an aggregated summary table that accumulates over time.
    Each day's pipeline run should UPDATE existing country+product rows
    and INSERT new combinations — not wipe and recreate the entire table.

    This means downstream Power BI / BigQuery connections always have
    a valid table to query — even while the pipeline is writing.
    Overwrite would cause a window where the table is empty.
    MERGE is atomic — table is always complete and consistent.
    """
    log.info(f"Saving Gold layer to: {gold_path}")

    # if not os.path.exists(os.path.join(gold_path, "_delta_log")):
    #     log.info("Gold table doesn't exist — creating fresh Delta table")
    #     df.write \
    #         .format("delta") \
    #         .mode("overwrite") \
    #         .partitionBy("country") \
    #         .save(gold_path)

    """    
    WHY THE CHANGE?
    os.path.exists() only checks local filesystem paths.
    DeltaTable.isDeltaTable() asks Spark itself to check —
    Spark already knows how to talk to GCS, S3, ADLS, or local disk.
    This makes the check storage-agnostic.
    """
    spark = df.sparkSession

    # ✅ Works for local paths AND gs://, s3://, abfss:// paths
    table_exists = DeltaTable.isDeltaTable(spark, gold_path)

    # First run — Delta table doesn't exist yet, just write it
    if not table_exists:
        log.info("Gold table doesn't exist — creating fresh Delta table")
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("country") \
            .save(gold_path)
    else:
        # Subsequent runs — MERGE new aggregations into existing table
        log.info("Gold table exists — running MERGE (upsert)")

        gold_table = DeltaTable.forPath(spark, gold_path)

        gold_table.alias("target") \
            .merge(
                df.alias("source"),
                # Match on the business key: country + product + category
                """target.country  = source.country  AND
                   target.product  = source.product  AND
                   target.category = source.category"""
            ) \
            .whenMatchedUpdate(set={
                # Update all metric columns when combination already exists
                "total_revenue":   "source.total_revenue",
                "units_sold":      "source.units_sold",
                "order_count":     "source.order_count",
                "avg_order_value": "source.avg_order_value"
            }) \
            .whenNotMatchedInsertAll().execute()  # insert if new country+product combo
            

    version = DeltaTable.forPath(spark, gold_path) \
                        .history(1) \
                        .collect()[0]["version"]

    log.info(f"Gold saved as Delta version {version} ✅")


# def export_gold_for_bigquery(spark, gold_path: str, export_path: str) -> None:
#     """
#     Export current Gold Delta table as plain Parquet for BigQuery.

#     WHY A SEPARATE EXPORT?
#     BigQuery's native GCS loader reads Parquet/CSV/JSON directly,
#     but doesn't understand Delta's transaction log format.

#     This reads the LATEST version of the Delta table (always correct,
#     always consistent thanks to ACID) and writes a clean Parquet
#     snapshot that BigQuery can load with a simple command.

#     Delta remains your source of truth. Parquet export is just
#     a "read-only mirror" for BigQuery's convenience.
#     """
#     log.info(f"Exporting Gold Delta table to Parquet for BigQuery...")

#     df = spark.read.format("delta").load(gold_path)

#     df.write \
#         .mode("overwrite") \
#         .parquet(export_path)

#     log.info(f"Parquet export complete: {export_path} ✅")


def export_gold_for_bigquery(spark, gold_path: str,
                              bq_table: str, temp_gcs_bucket: str) -> None:
    """
    Write Gold Delta table directly into BigQuery using the
    spark-bigquery-connector. No manual `bq load` step needed.

    HOW THIS WORKS UNDER THE HOOD:
    Spark can't write directly row-by-row into BigQuery —
    instead it stages the data as temporary Parquet/Avro files
    in a GCS bucket, then tells BigQuery to ingest from there.
    This 'temporaryGcsBucket' is just scratch space, auto-cleaned.
    """
    log.info(f"Writing Gold table directly to BigQuery: {bq_table}")

    df = spark.read.format("delta").load(gold_path)

    df.write \
        .format("bigquery") \
        .option("table", bq_table) \
        .option("temporaryGcsBucket", temp_gcs_bucket) \
        .mode("overwrite") \
        .save()

    log.info(f"BigQuery table updated: {bq_table} ✅")