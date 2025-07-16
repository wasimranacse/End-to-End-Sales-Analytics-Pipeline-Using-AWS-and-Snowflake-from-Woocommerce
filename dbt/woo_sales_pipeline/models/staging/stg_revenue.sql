-- models/staging/stg_revenue.sql

SELECT
    -- Format order date to match stg_orders (DD-MM-YYYY as string)
    TO_CHAR(TO_TIMESTAMP(ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS'), 'DD-MM-YYYY') AS order_date,

    -- Number of orders placed on that date
    ORDERS_QUANTITY,

    -- Gross sales before returns or discounts
    GROSS_SALES,

    -- Total number of returned orders
    ORDER_RETURNS,

    -- Total value of coupons applied on this day
    COUPON_AMOUNT,

    -- Net sales (after coupon discounts)
    NET_SALES,

    -- Tax amount collected on this day
    TAXES,

    -- Shipping amount charged on this day
    SHIPPING,

    -- Total sales (NET_SALES + TAXES + SHIPPING)
    TOTAL_SALES,

     -- Convert original string to timestamp for time-based aggregations
    TO_TIMESTAMP(ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS') AS order_datetime,

    -- Extract year, month, week, and quarter from the order timestamp
    EXTRACT(YEAR FROM TO_TIMESTAMP(ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS')) AS order_year,
    EXTRACT(MONTH FROM TO_TIMESTAMP(ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS')) AS order_month,
    EXTRACT(WEEK FROM TO_TIMESTAMP(ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS')) AS order_week,
    EXTRACT(QUARTER FROM TO_TIMESTAMP(ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS')) AS order_quarter,

    -- File origin metadata
    FILENAME,
    LOAD_TIMESTAMP

FROM {{ source('raw', 'raw_revenue') }}


