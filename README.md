# PySpark ETL Pipeline

A production-style ETL pipeline built with PySpark, implementing the
Medallion Architecture (Bronze → Silver → Gold) with automated
orchestration via Apache Airflow.

---

## Architecture Overview
```
Source (CSV/Database)
        │
        ▼
┌───────────────┐
│    BRONZE     │  Raw data — exact copy of source, never modified
│   (Parquet)   │  Audit trail, reprocessing safety net
└───────┬───────┘
        │
        ▼
┌───────────────┐
│    SILVER     │  Cleaned & enriched data
│   (Parquet)   │  Nulls dropped, duplicates removed,
│               │  types standardized, revenue calculated
└───────┬───────┘
        │
        ▼
┌───────────────┐
│     GOLD      │  Business-ready aggregations
│   (Parquet)   │  Partitioned by country for fast queries
│               │  Ready for Power BI / dashboards
└───────────────┘
```

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| PySpark | 3.5.0 | Distributed data processing |
| Apache Airflow | 2.8.0 | Pipeline orchestration & scheduling |
| Docker | latest | Containerized, reproducible environment |
| Python | 3.11 | Core language |
| Pytest | latest | Unit testing |

---

## Project Structure
```
pyspark-etl-pipeline/
├── config/
│   └── config.py           # Centralized settings & business rules
├── extract/
│   └── extract.py          # Read from source, save Bronze layer
├── transform/
│   └── transform.py        # Clean, standardize, enrich data
├── load/
│   └── load.py             # Aggregate, save Gold layer
├── utils/
│   ├── spark_session.py    # SparkSession factory
│   └── data_quality.py     # Data quality validation checks
├── airflow/
│   ├── Dockerfile          # Custom Airflow image with Java + PySpark
│   ├── docker-compose.yml  # Full Airflow stack definition
│   └── dags/
│       └── etl_orders_dag.py  # Scheduled DAG: runs daily at 6AM
├── tests/
│   └── test_transformations.py  # 8 unit tests for transform logic
├── main.py                 # Pipeline orchestrator
└── README.md
```

---

## Pipeline Stages

### Extract
- Reads raw orders data from CSV source
- Logs row count on ingestion
- Saves exact copy to Bronze layer as Parquet
- No transformations — raw data preserved as-is

### Transform (Silver)
- Drops rows with null critical columns (order_id, customer_id, product)
- Removes duplicate order_ids
- Standardizes text to uppercase (product, country, status)
- Converts order_date string to proper Date type
- Calculates business metrics:
  - `total_revenue = quantity × unit_price`
  - `discount = 10% for Electronics category`
  - `net_revenue = total_revenue - discount`

### Quality Checks
Five automated checks run after transformation — pipeline aborts
if any check fails, preventing bad data from reaching Gold:
- Minimum row count threshold
- No nulls in critical columns
- No duplicate order_ids
- Valid status values only
- No negative revenue values

### Load (Gold)
- Filters Completed orders only
- Aggregates by country + product + category
- Metrics: total_revenue, units_sold, order_count, avg_order_value
- Saves partitioned by country for optimized query performance

---

## Data Quality Checks
```python
# Pipeline aborts loudly if any check fails
check_row_count(df, min_rows=100, layer_name="Silver")
check_nulls(df, critical_cols=["order_id", "product", "status"])
check_duplicates(df, key_col="order_id")
check_valid_values(df, col_name="status", valid_values=[...])
check_revenue_positive(df)
```

---

## Running Locally

### Prerequisites
- Python 3.11
- Java JDK 11
- PySpark 3.5.0
- Docker Desktop

### Setup
```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/pyspark-etl-pipeline.git
cd pyspark-etl-pipeline

# Install dependencies
pip install pyspark==3.5.0 pytest

# Run the pipeline
python main.py
```

### Run Tests
```bash
pytest tests/ -v
```

Expected output:
```
test_clean_orders_removes_nulls            PASSED
test_clean_orders_removes_duplicates       PASSED
test_clean_orders_uppercases_columns       PASSED
test_clean_orders_row_count                PASSED
test_enrich_adds_revenue_columns           PASSED
test_enrich_electronics_gets_discount      PASSED
test_enrich_accessories_no_discount        PASSED
test_no_negative_revenue                   PASSED

8 passed in 12.34s
```

### Run with Airflow (Docker)
```bash
cd airflow

# First time setup
docker-compose up airflow-init

# Start all services
docker-compose up -d

# Open Airflow UI
# http://localhost:8080
# Username: admin / Password: admin
```

---

## Key Concepts Demonstrated

**Medallion Architecture** — Three-layer data organization pattern
used by Databricks, Netflix, and Uber. Each layer has a clear purpose
and quality guarantee.

**Modular Pipeline Design** — Each stage (extract, transform, load)
is fully independent. A bug in load.py never affects extract.py.
Easy to test, debug, and modify individually.

**Fail Loud, Fail Early** — Quality checks sit between Transform and
Load. Bad data never reaches Gold layer or downstream dashboards.

**Partition Pruning** — Gold layer partitioned by country. Queries
filtering by country read only relevant files, skipping all others.

**Idempotent Writes** — All writes use `mode("overwrite")`. Running
the pipeline twice produces identical results — no duplicate data.

---

## What I Learned Building This

- Distributed computing fundamentals (partitions, lazy evaluation, DAGs)
- PySpark DataFrame API and Spark SQL
- Production ETL architecture patterns
- Docker containerization for data engineering
- Pipeline orchestration with Apache Airflow
- Unit testing distributed data pipelines with pytest
- Data quality validation strategies

---


## Author

**Sanyam Jain**
Data Engineer
[GitHub](https://github.com/san-7771)
```

---

## Now Commit the README
```
git add README.md
git commit -m "Add comprehensive README with architecture docs and setup guide"
git push origin main
