/*
  Mart: Revenue summary by country
  Materialized as TABLE — persisted in BigQuery, fast to query
  Used by: dashboards, analyst queries, Power BI

  {{ ref('stg_orders') }} tells dbt:
  "this model depends on stg_orders — run that first"
  dbt builds the execution DAG automatically from these references
*/

WITH completed_orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}    -- reference staging model
    WHERE status = 'COMPLETED'

),

country_metrics AS (

    SELECT
        country,
        COUNT(DISTINCT order_id)        AS total_orders,
        COUNT(DISTINCT customer_id)     AS unique_customers,
        SUM(net_revenue)                AS total_revenue,
        ROUND(AVG(net_revenue), 2)      AS avg_order_value,
        SUM(quantity)                   AS total_units_sold,
        MIN(order_date)                 AS first_order_date,
        MAX(order_date)                 AS last_order_date

    FROM completed_orders
    GROUP BY country

)

SELECT
    country,
    total_orders,
    unique_customers,
    total_revenue,
    avg_order_value,
    total_units_sold,
    first_order_date,
    last_order_date,
    -- rank countries by revenue — useful for dashboards
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank

FROM country_metrics
ORDER BY revenue_rank