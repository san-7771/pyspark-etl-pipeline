/*
  Mart: Revenue summary by product
  Shows which products drive the most revenue across all markets
*/

WITH completed_orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}
    WHERE status = 'COMPLETED'

),

product_metrics AS (

    SELECT
        product,
        category,
        COUNT(DISTINCT order_id)    AS total_orders,
        SUM(quantity)               AS total_units_sold,
        SUM(total_revenue)          AS gross_revenue,
        SUM(discount_amount)        AS total_discounts,
        SUM(net_revenue)            AS net_revenue,
        ROUND(AVG(net_revenue), 2)  AS avg_order_value,
        -- discount rate = what % of gross revenue was discounted
        ROUND(
            SAFE_DIVIDE(SUM(discount_amount), SUM(total_revenue)) * 100,
        2)                          AS discount_rate_pct

    FROM completed_orders
    GROUP BY product, category

)

SELECT
    product,
    category,
    total_orders,
    total_units_sold,
    gross_revenue,
    total_discounts,
    net_revenue,
    avg_order_value,
    discount_rate_pct,
    RANK() OVER (ORDER BY net_revenue DESC) AS revenue_rank

FROM product_metrics
ORDER BY revenue_rank