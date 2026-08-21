WITH raw_orders AS (

    SELECT
        order_id,
        customer_id,
        UPPER(TRIM(product))  AS product,
        UPPER(TRIM(category)) AS category,
        quantity,
        unit_price,
        CAST(order_date AS DATE) AS order_date,
        UPPER(TRIM(country))  AS country,
        UPPER(TRIM(status))   AS status,

        -- Revenue calculations — defined once here, reused in marts
        CAST(quantity AS INT64) *
        CAST(unit_price AS INT64)                      AS total_revenue,

        CASE
            WHEN UPPER(TRIM(category)) = 'ELECTRONICS'
            THEN CAST(quantity AS INT64) *
                 CAST(unit_price AS INT64) * 0.10
            ELSE 0
        END                                            AS discount_amount,

        ROUND(
            CAST(quantity AS INT64) * CAST(unit_price AS INT64) -
            CASE
                WHEN UPPER(TRIM(category)) = 'ELECTRONICS'
                THEN CAST(quantity AS INT64) *
                     CAST(unit_price AS INT64) * 0.10
                ELSE 0
            END,
        2)                                             AS net_revenue

    FROM {{ source('raw', 'silver_orders') }}

    WHERE status IS NOT NULL
      AND order_id IS NOT NULL

)

SELECT * FROM raw_orders