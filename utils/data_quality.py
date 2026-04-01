import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan

log = logging.getLogger(__name__)

def check_row_count(df: DataFrame, min_rows: int, layer_name: str) -> None:
    """
    Fails pipeline if row count drops below a minimum threshold.
    Catches cases where source sends empty or near-empty files.
    """
    actual = df.count()
    if actual < min_rows:
        raise ValueError(
            f"[{layer_name}] Row count check FAILED: "
            f"got {actual} rows, expected at least {min_rows}"
        )
    log.info(f"[{layer_name}] Row count check PASSED: {actual} rows")


def check_nulls(df: DataFrame, critical_cols: list, layer_name: str) -> None:
    """
    Fails pipeline if any critical column contains nulls.
    These are columns that must NEVER be null in a clean layer.
    """
    for col_name in critical_cols:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"[{layer_name}] Null check FAILED: "
                f"column '{col_name}' has {null_count} null values"
            )
        log.info(f"[{layer_name}] Null check PASSED: '{col_name}' has no nulls")


def check_duplicates(df: DataFrame, key_col: str, layer_name: str) -> None:
    """
    Fails pipeline if duplicate keys exist.
    Each order_id must appear exactly once in Silver/Gold.
    """
    total    = df.count()
    distinct = df.select(key_col).distinct().count()

    if total != distinct:
        raise ValueError(
            f"[{layer_name}] Duplicate check FAILED: "
            f"{total - distinct} duplicate '{key_col}' values found"
        )
    log.info(f"[{layer_name}] Duplicate check PASSED: all '{key_col}' values unique")


def check_valid_values(df: DataFrame, col_name: str,
                       valid_values: list, layer_name: str) -> None:
    """
    Fails pipeline if unexpected values exist in a column.
    Catches upstream data sending new unexpected categories.
    """
    invalid_count = df.filter(~col(col_name).isin(valid_values)).count()
    if invalid_count > 0:
        # Show what the invalid values actually are
        invalid_vals = df.filter(~col(col_name).isin(valid_values)) \
                         .select(col_name).distinct().collect()
        raise ValueError(
            f"[{layer_name}] Valid values check FAILED: "
            f"column '{col_name}' has {invalid_count} invalid rows. "
            f"Unexpected values: {[r[col_name] for r in invalid_vals]}"
        )
    log.info(f"[{layer_name}] Valid values check PASSED: '{col_name}' all valid")


def check_revenue_positive(df: DataFrame, layer_name: str) -> None:
    """
    Business rule: net_revenue must always be positive.
    Negative revenue = data error or pricing bug.
    """
    negative_count = df.filter(col("net_revenue") < 0).count()
    if negative_count > 0:
        raise ValueError(
            f"[{layer_name}] Revenue check FAILED: "
            f"{negative_count} rows have negative net_revenue"
        )
    log.info(f"[{layer_name}] Revenue check PASSED: all net_revenue values positive")


def run_silver_quality_checks(df: DataFrame) -> None:
    """
    Master function — runs ALL quality checks on Silver layer.
    Call this once after clean_orders() in main.py
    """
    log.info("Running Silver layer quality checks...")

    check_row_count(df,  min_rows=100,   layer_name="Silver")
    check_nulls(df,
        critical_cols=["order_id", "customer_id", "product", "status"],
        layer_name="Silver"
    )
    check_duplicates(df,   key_col="order_id",  layer_name="Silver")
    check_valid_values(df,
        col_name="status",
        valid_values=["COMPLETED", "PENDING", "CANCELLED"],
        layer_name="Silver"
    )
    check_revenue_positive(df, layer_name="Silver")

    log.info("✅ All Silver quality checks PASSED")