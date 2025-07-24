WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
)

SELECT
    ORDER_ID,
    order_date,
    order_datetime,
    CUSTOMER_NAME,
    CUSTOMER_TYPE,
    PRODUCT_NAME,
    ITEM_COUNT,
    NET_REVENUE,
    COUPON_AMOUNT,
    Total_Discount,
    NET_SALES_AMOUNT,
    SALES_ATTRIBUTION,
    ORDER_STATUS,
    order_year,
    order_month,
    order_week,
    order_quarter
FROM orders
