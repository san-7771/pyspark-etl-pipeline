from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore

# ── Default settings applied to every task ──────────────
default_args = {
    "owner":            "sanyam",        # who owns this pipeline
    "retries":          2,               # retry twice before failing
    "retry_delay":      timedelta(minutes=5),  # wait 5 min between retries
    "email_on_failure": False,           # set True + add email when ready
}

# ── Define the DAG ───────────────────────────────────────
dag = DAG(
    dag_id="etl_orders_pipeline",        # unique name shown in Airflow UI
    description="Daily orders ETL: CSV → Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),     # when scheduling starts from
    schedule_interval="0 6 * * *",       # run every day at 6:00 AM
    catchup=False,                       # don't backfill missed runs
    tags=["etl", "orders", "daily"],     # for filtering in UI
)

# ── Task functions ───────────────────────────────────────
# Each function is one step of your pipeline.
# Airflow calls these functions at the scheduled time.

def extract_task_fn(**context):
    """
    **context gives access to Airflow metadata:
    - context['execution_date'] = when this run was triggered
    - context['task_instance']  = this specific task's info
    Useful for logging and passing data between tasks.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/dags")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("ETL_Extract") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Running extract for: {context['execution_date']}")

    df = spark.read.csv("/opt/airflow/dags/data/orders.csv",
                        header=True, inferSchema=True)
    row_count = df.count()
    print(f"Extracted {row_count} rows")

    df.write.mode("overwrite").parquet("/opt/airflow/dags/data/bronze/orders_raw")
    print("Bronze layer saved ✅")

    spark.stop()
    return row_count   # returned value stored by Airflow (XCom)


def transform_task_fn(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, trim, upper, to_date, when, round as r
    import sys
    sys.path.insert(0, "/opt/airflow/dags")

    spark = SparkSession.builder \
        .appName("ETL_Transform") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Read from Bronze (not original CSV — always read previous layer)
    df = spark.read.parquet("/opt/airflow/dags/data/bronze/orders_raw")

    # Clean
    df_clean = df \
        .dropna(subset=["order_id", "customer_id", "product"]) \
        .filter(col("status").isin(["Completed", "Pending", "Cancelled"])) \
        .withColumn("product",    trim(upper(col("product")))) \
        .withColumn("country",    trim(upper(col("country")))) \
        .withColumn("status",     trim(upper(col("status")))) \
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
        .dropDuplicates(["order_id"])

    # Enrich
    df_enriched = df_clean \
        .withColumn("total_revenue", col("quantity") * col("unit_price")) \
        .withColumn("discount",
            when(col("category") == "Electronics",
                 col("total_revenue") * 0.10).otherwise(0)) \
        .withColumn("net_revenue", r(col("total_revenue") - col("discount"), 2))

    df_enriched.write.mode("overwrite").parquet(
        "/opt/airflow/dags/data/silver/orders_clean")
    print(f"Silver layer saved: {df_enriched.count()} rows ✅")

    spark.stop()


def quality_check_task_fn(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    spark = SparkSession.builder \
        .appName("ETL_QualityCheck") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet("/opt/airflow/dags/data/silver/orders_clean")

    # Check 1: minimum row count
    row_count = df.count()
    if row_count < 100:
        raise ValueError(f"Quality check FAILED: only {row_count} rows in Silver")

    # Check 2: no nulls in critical columns
    for col_name in ["order_id", "product", "status"]:
        nulls = df.filter(col(col_name).isNull()).count()
        if nulls > 0:
            raise ValueError(f"Quality check FAILED: {nulls} nulls in {col_name}")

    # Check 3: no negative revenue
    neg = df.filter(col("net_revenue") < 0).count()
    if neg > 0:
        raise ValueError(f"Quality check FAILED: {neg} negative revenue rows")

    print(f"All quality checks PASSED ✅ ({row_count} rows)")
    spark.stop()


def load_task_fn(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as s, count, round as r

    spark = SparkSession.builder \
        .appName("ETL_Load") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet("/opt/airflow/dags/data/silver/orders_clean")

    gold_df = df \
        .filter(col("status") == "COMPLETED") \
        .groupBy("country", "product", "category") \
        .agg(
            s("net_revenue").alias("total_revenue"),
            s("quantity").alias("units_sold"),
            count("order_id").alias("order_count"),
            r(s("net_revenue") / count("order_id"), 2).alias("avg_order_value")
        ) \
        .orderBy("total_revenue", ascending=False)

    gold_df.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .parquet("/opt/airflow/dags/data/gold/orders_summary")

    print(f"Gold layer saved: {gold_df.count()} summary rows ✅")
    spark.stop()


# ── Wire tasks together ──────────────────────────────────
# Create task objects using PythonOperator
# task_id    = unique name shown in Airflow UI
# python_callable = the function to run
# provide_context = passes execution_date and other metadata

t1_extract = PythonOperator(
    task_id="extract_orders",
    python_callable=extract_task_fn,
    provide_context=True,
    dag=dag,
)

t2_transform = PythonOperator(
    task_id="transform_orders",
    python_callable=transform_task_fn,
    provide_context=True,
    dag=dag,
)

t3_quality = PythonOperator(
    task_id="quality_checks",
    python_callable=quality_check_task_fn,
    provide_context=True,
    dag=dag,
)

t4_load = PythonOperator(
    task_id="load_to_gold",
    python_callable=load_task_fn,
    provide_context=True,
    dag=dag,
)

# ── Define execution order ───────────────────────────────
# >> means "then run". This is the pipeline flow.
# If t1 fails, t2, t3, t4 never run.
t1_extract >> t2_transform >> t3_quality >> t4_load