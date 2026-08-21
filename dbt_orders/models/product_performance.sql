/*
  Product Performance
  -------------------
  Breaks down revenue and units by product across all countries.
  References country_revenue to add country-level context.

  Notice {{ ref('country_revenue') }} — this creates a dependency.
  dbt will ALWAYS build country_revenue before this model.
  You never need to manage build order manually.
*/

{{ config(materialized='table') }}

WITH product_totals AS (
    -- Aggregate at product level from Gold source
    SELECT
        product,
        category,
        SUM(total_revenue)  AS product_revenue,
        SUM(units_sold)     AS product_units,
        SUM(order_count)    AS product_orders
    FROM {{ source('raw', 'gold_orders_summary') }}
    GROUP BY product, category
),

country_totals AS (
    -- Pull total revenue from the country_revenue model
    -- This is how dbt chains models together
    SELECT SUM(total_revenue) AS grand_total_revenue
    FROM {{ ref('country_revenue') }}
)

SELECT
    p.product,
    p.category,
    p.product_revenue,
    p.product_units,
    p.product_orders,
    ROUND(p.product_revenue / c.grand_total_revenue * 100, 2) AS revenue_share_pct,
    ROUND(p.product_revenue / p.product_orders, 2)            AS avg_order_value
FROM product_totals p
CROSS JOIN country_totals c
ORDER BY p.product_revenue DESC