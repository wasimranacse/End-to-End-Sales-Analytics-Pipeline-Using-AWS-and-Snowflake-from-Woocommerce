-- models/marts/facts/order_metrics.sql

WITH orders AS (
    -- Reference the cleaned fact orders table
    SELECT *
    FROM {{ ref('fct_orders') }}
),

formatted_orders AS (
    SELECT
        *,
        -- Attempt to cast the order_date string to a DATE data type
        -- If the cast fails (invalid date string), TRY_TO_DATE returns NULL
        TRY_TO_DATE(order_date) AS order_date_casted
    FROM orders
)

SELECT
    -- Extract the year part from the successfully casted order date
    DATE_PART('year', order_date_casted) AS order_year,

    -- Extract the month part from the successfully casted order date
    DATE_PART('month', order_date_casted) AS order_month,

    -- Count of unique orders placed in the given year and month
    COUNT(DISTINCT order_id) AS total_orders,

    -- Total revenue generated from these orders
    SUM(net_revenue) AS total_revenue,

    -- Average order value = total revenue divided by number of orders
    AVG(net_revenue) AS avg_order_value,

    -- Count of orders with status 'refunded' (case-insensitive check)
    SUM(
        CASE 
            WHEN LOWER(order_status) = 'refunded' THEN 1 
            ELSE 0 
        END
    ) AS total_refunds

FROM formatted_orders

-- Exclude rows where order_date_casted is NULL to avoid NULL year/month values
WHERE order_date_casted IS NOT NULL

-- Group results by year and month for aggregation
GROUP BY 1, 2

-- Order the output chronologically by year and month
ORDER BY 1, 2
