import logging
from config.config         import *
from utils.spark_session   import get_spark_session
from utils.data_quality    import run_silver_quality_checks  # ← ADD THIS
from extract.extract       import extract_orders, save_bronze
from transform.transform   import clean_orders, enrich_orders, save_silver
from load.load             import build_gold, save_gold

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)


def run_pipeline():
    log.info("=" * 50)
    log.info("ETL PIPELINE STARTED")
    log.info("=" * 50)

    # ── Init Spark ─────────────────────────────────
    spark = get_spark_session(SPARK_APP_NAME, SPARK_MASTER)

    # ── EXTRACT ────────────────────────────────────
    log.info("--- PHASE: EXTRACT ---")
    raw_df = extract_orders(spark, SOURCE_FILE)
    save_bronze(raw_df, BRONZE_PATH)

    # ── TRANSFORM ──────────────────────────────────
    log.info("--- PHASE: TRANSFORM ---")
    clean_df    = clean_orders(raw_df, VALID_STATUSES)
    enriched_df = enrich_orders(clean_df, DISCOUNT_RATE, ELECTRONICS_CATEGORY)

    # ── QUALITY CHECKS ─────────────────────────────
    log.info("--- PHASE: QUALITY CHECKS ---")
    run_silver_quality_checks(enriched_df)   # ← pipeline stops here if anything fails

    save_silver(enriched_df, SILVER_PATH)   # ← only saves if all checks pass

    # ── LOAD ───────────────────────────────────────
    log.info("--- PHASE: LOAD ---")
    gold_df = build_gold(enriched_df)
    save_gold(gold_df, GOLD_PATH)

    log.info("=" * 50)
    log.info("ETL PIPELINE COMPLETED SUCCESSFULLY ✅")
    log.info("=" * 50)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
# ```

# ---

# ## Step 8 — Copy your orders.csv into the project

# Copy the `orders.csv` you created earlier into `etl_project/data/`

# Then run the pipeline from VS Code terminal:
# ```
# cd etl_project
# python main.py
# ```

# ---

# You should see clean logs like:
# ```
# 2026-03-07 | INFO | ETL PIPELINE STARTED
# 2026-03-07 | INFO | Extracting data from: ...orders.csv
# 2026-03-07 | INFO | Extracted 500 rows, 9 columns
# 2026-03-07 | INFO | Bronze layer saved successfully
# 2026-03-07 | INFO | Silver: 500 → 476 rows (dropped 24)
# ...
# 2026-03-07 | INFO | ETL PIPELINE COMPLETED SUCCESSFULLY ✅

## Notice the Order in main.py
# ```
# extract  →  transform  →  QUALITY CHECKS  →  save silver  →  load
