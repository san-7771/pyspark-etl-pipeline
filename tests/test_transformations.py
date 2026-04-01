import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform.transform import clean_orders, enrich_orders

# ── Setup: create a SparkSession for testing ────────────
@pytest.fixture(scope="session")  # fixture is a reusable setup block in pytest (it is basically like preparation before the test )
def spark():
    """
    Creates ONE SparkSession shared across all tests.
    scope="session" means it's created once and reused —
    not recreated for every single test function.
    """
    spark = SparkSession.builder \
        .appName("ETL_Tests") \
        .master("local[2]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark       # give spark to the test
    spark.stop()      # clean up after all tests finish


@pytest.fixture
def sample_df(spark):
    """
    Small fake DataFrame for testing.
    Never use real data in tests — slow and unreliable.
    This gives us full control over what data looks like.
    """
    data = [
        (1001, 1, "Laptop",   "Electronics", 2, 999, "2024-01-15", "India",   "Completed"),
        (1002, 2, "Phone",    "Electronics", 1, 499, "2024-01-16", "USA",     "Cancelled"),
        (1003, 3, "Keyboard", "Accessories", 3, 49,  "2024-01-17", "UK",      "Pending"),
        (1004, 4, "Tablet",   "Electronics", 1, 299, "2024-01-18", "Germany", None),       # null status
        (1005, 5, "Monitor",  "Accessories", 2, 199, "2024-01-19", "Canada",  "Completed"),
        (1001, 1, "Laptop",   "Electronics", 2, 999, "2024-01-15", "India",   "Completed"), # duplicate
    ]
    columns = ["order_id", "customer_id", "product", "category",
               "quantity", "unit_price", "order_date", "country", "status"]
    return spark.createDataFrame(data, columns)


# ── Tests for clean_orders() ────────────────────────────

def test_clean_orders_removes_nulls(spark, sample_df):
    """
    Null status rows must be dropped in Silver layer.
    Row with order_id=1004 has null status — should be gone.
    """
    valid_statuses = ["Completed", "Pending", "Cancelled"]
    cleaned = clean_orders(sample_df, valid_statuses)

    # Check no nulls remain in status column
    null_count = cleaned.filter(col("status").isNull()).count()
    assert null_count == 0, f"Expected 0 nulls in status, got {null_count}"


def test_clean_orders_removes_duplicates(spark, sample_df):
    """
    Duplicate order_ids must be removed.
    order_id=1001 appears twice — only one should remain.
    """
    valid_statuses = ["Completed", "Pending", "Cancelled"]
    cleaned = clean_orders(sample_df, valid_statuses)

    total    = cleaned.count()
    distinct = cleaned.select("order_id").distinct().count()
    assert total == distinct, f"Duplicates found: {total} rows but {distinct} distinct order_ids"


def test_clean_orders_uppercases_columns(spark, sample_df):
    """
    Product, country and status must be uppercased in Silver.
    This prevents the silent filter failure we fixed earlier.
    """
    valid_statuses = ["Completed", "Pending", "Cancelled"]
    cleaned = clean_orders(sample_df, valid_statuses)

    # Collect distinct values and check all are uppercase
    products = [r.product for r in cleaned.select("product").distinct().collect()]
    countries = [r.country for r in cleaned.select("country").distinct().collect()]

    for p in products:
        assert p == p.upper(), f"Product not uppercased: {p}"
    for c in countries:
        assert c == c.upper(), f"Country not uppercased: {c}"


def test_clean_orders_row_count(spark, sample_df):
    """
    Input has 6 rows:
    - 1 null status (dropped)
    - 1 duplicate order_id (dropped)
    = 4 rows expected after cleaning
    """
    valid_statuses = ["Completed", "Pending", "Cancelled"]
    cleaned = clean_orders(sample_df, valid_statuses)

    assert cleaned.count() == 4, f"Expected 4 rows, got {cleaned.count()}"


# ── Tests for enrich_orders() ───────────────────────────

def test_enrich_adds_revenue_columns(spark, sample_df):
    """
    enrich_orders() must add 3 new columns:
    total_revenue, discount, net_revenue
    """
    valid_statuses = ["Completed", "Pending", "Cancelled"]
    cleaned  = clean_orders(sample_df, valid_statuses)
    enriched = enrich_orders(cleaned, 0.10, "Electronics")

    # Check all 3 columns exist
    assert "total_revenue" in enriched.columns, "Missing column: total_revenue"
    assert "discount"      in enriched.columns, "Missing column: discount"
    assert "net_revenue"   in enriched.columns, "Missing column: net_revenue"


def test_enrich_electronics_gets_discount(spark, sample_df):
    """
    Electronics category must get 10% discount.
    Laptop: 2 × 999 = 1998 total, discount = 199.8, net = 1798.2
    """
    valid_statuses = ["Completed", "Pending", "Cancelled"]
    cleaned  = clean_orders(sample_df, valid_statuses)
    enriched = enrich_orders(cleaned, 0.10, "Electronics")

    laptop_row = enriched.filter(col("order_id") == 1001).collect()[0]

    assert laptop_row["total_revenue"] == 1998.0,  f"Wrong total_revenue: {laptop_row['total_revenue']}"
    assert laptop_row["discount"]      == 199.8,   f"Wrong discount: {laptop_row['discount']}"
    assert laptop_row["net_revenue"]   == 1798.2,  f"Wrong net_revenue: {laptop_row['net_revenue']}"


def test_enrich_accessories_no_discount(spark, sample_df):
    """
    Accessories category must have zero discount.
    Keyboard: 3 × 49 = 147 total, discount = 0, net = 147
    """
    valid_statuses = ["Completed", "Pending", "Cancelled"]
    cleaned  = clean_orders(sample_df, valid_statuses)
    enriched = enrich_orders(cleaned, 0, "Accessories")

    keyboard_row = enriched.filter(col("order_id") == 1003).collect()[0]

    assert keyboard_row["discount"]    == 0,     f"Accessories should have 0 discount"
    assert keyboard_row["net_revenue"] == 147.0, f"Wrong net_revenue: {keyboard_row['net_revenue']}"


def test_no_negative_revenue(spark, sample_df):
    """
    Business rule: net_revenue must never be negative.
    Negative revenue = pricing or discount bug.
    """
    valid_statuses = ["Completed", "Pending", "Cancelled"]
    cleaned  = clean_orders(sample_df, valid_statuses)
    enriched = enrich_orders(cleaned, 0.10, "Electronics")

    negative_count = enriched.filter(col("net_revenue") < 0).count()
    assert negative_count == 0, f"Found {negative_count} rows with negative revenue"
# ```

# ---

## Run the Tests

# In your `etl_project` folder:
# ```
# pip install pytest
# pytest tests/test_transformations.py -v
# ```

# The `-v` flag means verbose — shows each test name and result:
# ```
# tests/test_transformations.py::test_clean_orders_removes_nulls        PASSED ✅
# tests/test_transformations.py::test_clean_orders_removes_duplicates   PASSED ✅
# tests/test_transformations.py::test_clean_orders_uppercases_columns   PASSED ✅
# tests/test_transformations.py::test_clean_orders_row_count            PASSED ✅
# tests/test_transformations.py::test_enrich_adds_revenue_columns       PASSED ✅
# tests/test_transformations.py::test_enrich_electronics_gets_discount  PASSED ✅
# tests/test_transformations.py::test_enrich_accessories_no_discount    PASSED ✅
# tests/test_transformations.py::test_no_negative_revenue               PASSED ✅
# ```

# ---

# ## Why These Tests Matter

# Each test locks in a **specific behavior**:
# ```
# test_clean_orders_removes_nulls
# → If someone accidentally removes the dropna() line
#   from transform.py, this test FAILS immediately.
#   You catch it before it reaches production.

# test_enrich_electronics_gets_discount
# → If business changes discount to 15% but forgets
#   to update config, this test FAILS.
#   Mismatch caught immediately.

# test_no_negative_revenue
# → If a pricing bug creates negative prices,
#   this test FAILS before bad data hits Gold layer.