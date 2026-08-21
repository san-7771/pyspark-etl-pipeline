"""
Dataproc-specific entry point.
Unlike local Windows runs, Dataproc clusters already have:
- Java pre-installed and configured
- Hadoop/GCS connector built-in (no manual JAR needed)
- Correct PYTHONPATH set up automatically

So the SparkSession here is MUCH simpler than your Windows version.
"""
import sys
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_dataproc_spark_session(app_name: str) -> SparkSession:
    """
    Simplified SparkSession for Dataproc.
    No HADOOP_HOME, no JAVA_HOME, no GCS connector JAR path —
    Dataproc images come with all of this pre-configured.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_pipeline():
    from extract.extract import extract_orders, save_bronze
    from transform.transform import clean_orders, enrich_orders, save_silver
    from load.load import build_gold, save_gold, export_gold_for_bigquery
    from utils.data_quality import run_silver_quality_checks

    # ── Config (hardcoded here for simplicity — env vars in real prod) ──
    GCS_BUCKET   = "gs://de-pipeline-sanyam-2026"
    SOURCE_FILE  = f"{GCS_BUCKET}/raw/orders.csv"     # ← now reads from GCS, not local!
    BRONZE_PATH  = f"{GCS_BUCKET}/bronze/orders_raw_delta"
    SILVER_PATH  = f"{GCS_BUCKET}/silver/orders_clean_delta"
    GOLD_PATH    = f"{GCS_BUCKET}/gold/orders_summary_delta"
    BQ_TABLE     = "de-learning-project-499519.orders_warehouse.gold_orders_summary"

    VALID_STATUSES       = ["Completed", "Pending", "Cancelled"]
    DISCOUNT_RATE        = 0.10
    ELECTRONICS_CATEGORY = "Electronics"

    log.info("=" * 50)
    log.info("ETL PIPELINE STARTED ON DATAPROC CLUSTER")
    log.info("=" * 50)

    spark = get_dataproc_spark_session("ETL_Pipeline_Dataproc")

    log.info("--- PHASE: EXTRACT ---")
    raw_df = extract_orders(spark, SOURCE_FILE)
    save_bronze(raw_df, BRONZE_PATH)

    log.info("--- PHASE: TRANSFORM ---")
    clean_df    = clean_orders(raw_df, VALID_STATUSES)
    enriched_df = enrich_orders(clean_df, DISCOUNT_RATE, ELECTRONICS_CATEGORY)

    log.info("--- PHASE: QUALITY CHECKS ---")
    run_silver_quality_checks(enriched_df)
    save_silver(enriched_df, SILVER_PATH)

    log.info("--- PHASE: LOAD ---")
    gold_df = build_gold(enriched_df)
    save_gold(gold_df, GOLD_PATH)

    export_gold_for_bigquery(
        spark, GOLD_PATH,
        bq_table=BQ_TABLE,
        temp_gcs_bucket="de-pipeline-sanyam-2026"
    )

    log.info("=" * 50)
    log.info("ETL PIPELINE COMPLETED SUCCESSFULLY ON DATAPROC ✅")
    log.info("=" * 50)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()