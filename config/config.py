import os

SPARK_APP_NAME  = "ETL_Pipeline"
SPARK_MASTER    = "local[*]"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── GCP Settings ─────────────────────────────────────────
GCP_KEY_PATH = os.path.join(BASE_DIR, "gcp-key.json")
GCS_BUCKET   = "gs://de-pipeline-sanyam-2026"

# Source can stay local for now — only OUTPUT moves to cloud
SOURCE_FILE = os.path.join(BASE_DIR, "data", "orders.csv")

# Bronze/Silver/Gold now point to GCS instead of local disk
BRONZE_PATH = f"{GCS_BUCKET}/bronze/orders_raw_delta"
SILVER_PATH = f"{GCS_BUCKET}/silver/orders_clean_delta"
GOLD_PATH   = f"{GCS_BUCKET}/gold/orders_summary_delta"

VALID_STATUSES        = ["Completed", "Pending", "Cancelled"]
DISCOUNT_RATE         = 0.10
ELECTRONICS_CATEGORY  = "Electronics"