-- models/marts/facts/fct_revenue.sql
WITH revenue AS (
    SELECT *
    FROM {{ ref('stg_revenue') }}
)

SELECT
    order_date,
    order_datetime,
    ORDERS_QUANTITY,
    GROSS_SALES,
    ORDER_RETURNS,
    COUPON_AMOUNT,
    NET_SALES,
    TAXES,
    SHIPPING,
    TOTAL_SALES,
    order_year,
    order_month,
    order_week,
    order_quarter
FROM revenue
