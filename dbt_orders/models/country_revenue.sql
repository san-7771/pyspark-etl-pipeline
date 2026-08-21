/*
  Country Revenue Summary
  -----------------------
  Aggregates total revenue and order metrics per country.
  Built on top of the Gold layer from our PySpark ETL pipeline.

  This model is materialized as a TABLE — dbt runs a full
  CREATE OR REPLACE TABLE on every `dbt run`.
  Use 'view' for lightweight models, 'table' for frequently queried ones.
*/

{{ config(materialized='table') }}

SELECT
    country,
    SUM(total_revenue)                          AS total_revenue,
    SUM(units_sold)                             AS total_units_sold,
    SUM(order_count)                            AS total_orders,
    ROUND(SUM(total_revenue) / SUM(order_count), 2) AS avg_order_value,
    COUNT(DISTINCT product)                     AS unique_products

FROM {{ source('raw', 'gold_orders_summary') }}

GROUP BY country
ORDER BY total_revenue DESC