import os

# ── Spark ──────────────────────────────────────────
SPARK_APP_NAME  = "ETL_Pipeline"
SPARK_MASTER    = "local[*]"

# ── Paths ──────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_FILE = os.path.join(BASE_DIR, "data", "orders.csv")

BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "orders_raw")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver", "orders_clean")
GOLD_PATH   = os.path.join(BASE_DIR, "data", "gold",   "orders_summary")

# ── Business Rules ─────────────────────────────────
VALID_STATUSES      = ["Completed", "Pending", "Cancelled"]
DISCOUNT_RATE       = 0.10
ELECTRONICS_CATEGORY = "Electronics"