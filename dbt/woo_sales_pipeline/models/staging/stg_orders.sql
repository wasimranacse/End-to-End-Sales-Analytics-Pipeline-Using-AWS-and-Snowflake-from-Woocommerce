-- models/staging/stg_orders.sql
SELECT
    TO_CHAR(TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI'), 'DD-MM-YYYY') AS order_date,
    ORDER_ID,
    NET_REVENUE,

    CASE
        WHEN DISCOUNT_AMOUNT > 0 THEN DISCOUNT_AMOUNT
        ELSE 0
    END AS Total_Discount,

    CASE
        WHEN ORDER_STATUS ILIKE 'processing' THEN 'Completed'
        WHEN ORDER_STATUS ILIKE 'cancelled' THEN 'Cancelled'
        WHEN net_revenue < 0 THEN 'Refunded'
        ELSE 'Completed'
    END AS ORDER_STATUS,

    CUSTOMER_NAME,
    CUSTOMER_TYPE,

    REPLACE(
        REGEXP_REPLACE(PRODUCTS, '\\d+\\xD7\\s*', ''),
        '"', ''
    ) AS PRODUCT_NAME,

    ITEM_COUNT,

    CASE
        WHEN COUPONS_APPLIED = 'ACESAVE5' THEN 5
        WHEN COUPONS_APPLIED = 'PROMO10' THEN 10
        WHEN COUPONS_APPLIED = 'ACE410' THEN 10
        ELSE 0
    END AS COUPON_AMOUNT,

    NET_SALES_AMOUNT,

    CASE
        WHEN MARKETING_ATTRIBUTION = 'Referral: Bing.com' THEN 'Bing'
        WHEN MARKETING_ATTRIBUTION = 'Organic: Google' THEN 'Google'
        WHEN MARKETING_ATTRIBUTION = 'Direct' THEN 'Direct' 
        WHEN MARKETING_ATTRIBUTION = 'Referral: Duckduckgo.com' THEN 'Duckduckgo'
        WHEN MARKETING_ATTRIBUTION = 'Referral: Acetransco.com' THEN 'Acetransco'
        WHEN MARKETING_ATTRIBUTION = 'Referral: Search.yahoo.com' THEN 'Yahoo'
        ELSE 'Direct'
    END AS SALES_ATTRIBUTION,

    FILENAME,
    LOAD_TIMESTAMP,

    -- New columns: convert order_date string back to timestamp to extract parts
    TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI') AS order_datetime,
    EXTRACT(year FROM TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI')) AS order_year,
    EXTRACT(month FROM TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI')) AS order_month,
    EXTRACT(week FROM TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI')) AS order_week,
    EXTRACT(quarter FROM TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI')) AS order_quarter

FROM {{ source('raw', 'raw_orders') }}




