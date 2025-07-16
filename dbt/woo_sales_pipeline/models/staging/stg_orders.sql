-- models/staging/stg_orders.sql

SELECT
    -- Convert ORDER_DATE string to formatted date (DD-MM-YYYY)
    TO_CHAR(TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI'), 'DD-MM-YYYY') AS order_date,

    -- Order unique identifier
    ORDER_ID,

    -- Final revenue after discounts and before tax/shipping
    NET_REVENUE,

    -- Standardize discount value, defaulting to 0 if NULL or not greater than 0
    CASE
        WHEN DISCOUNT_AMOUNT > 0 THEN DISCOUNT_AMOUNT
        ELSE 0
    END AS Total_Discount,

    -- Normalize and categorize order status
    CASE
        WHEN ORDER_STATUS ILIKE 'processing' THEN 'Completed'
        WHEN ORDER_STATUS ILIKE 'cancelled' THEN 'Cancelled'
        WHEN NET_REVENUE < 0 THEN 'Refunded'
        ELSE 'Completed'
    END AS ORDER_STATUS,

    -- Customer name and type (e.g., new or returning)
    CUSTOMER_NAME,
    CUSTOMER_TYPE,

    -- Clean up product names: remove quantities (e.g., "2×") and quotation marks
    REPLACE(
        REGEXP_REPLACE(PRODUCTS, '\\d+\\xD7\\s*', ''),
        '"', ''
    ) AS PRODUCT_NAME,

    -- Total number of individual items in the order
    ITEM_COUNT,

    -- Derive standard coupon discount value from code
    CASE
        WHEN COUPONS_APPLIED = 'ACESAVE5' THEN 5
        WHEN COUPONS_APPLIED = 'PROMO10' THEN 10
        WHEN COUPONS_APPLIED = 'ACE410' THEN 10
        ELSE 0
    END AS COUPON_AMOUNT,

    -- Net sales after applying coupon discounts
    NET_SALES_AMOUNT,

    -- Simplify marketing attribution sources
    CASE
        WHEN MARKETING_ATTRIBUTION = 'Referral: Bing.com' THEN 'Bing'
        WHEN MARKETING_ATTRIBUTION = 'Organic: Google' THEN 'Google'
        WHEN MARKETING_ATTRIBUTION = 'Direct' THEN 'Direct'
        WHEN MARKETING_ATTRIBUTION = 'Referral: Duckduckgo.com' THEN 'Duckduckgo'
        WHEN MARKETING_ATTRIBUTION = 'Referral: Acetransco.com' THEN 'Acetransco'
        WHEN MARKETING_ATTRIBUTION = 'Referral: Search.yahoo.com' THEN 'Yahoo'
        ELSE 'Direct'
    END AS SALES_ATTRIBUTION,

    -- Convert original string to timestamp for time-based aggregations
    TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI') AS order_datetime,

    -- Extract year, month, week, and quarter from the order timestamp
    EXTRACT(YEAR FROM TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI')) AS order_year,
    EXTRACT(MONTH FROM TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI')) AS order_month,
    EXTRACT(WEEK FROM TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI')) AS order_week,
    EXTRACT(QUARTER FROM TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI')) AS order_quarter,

    -- Source file and data ingestion timestamp
    FILENAME,
    LOAD_TIMESTAMP

-- Load from raw_orders source defined in sources.yml
FROM {{ source('raw', 'raw_orders') }}

