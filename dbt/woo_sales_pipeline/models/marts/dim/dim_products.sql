-- models/marts/facts/dim_products.sql
WITH products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
)

SELECT
    PRODUCT_TITLE,
    CATEGORY,
    VARIATIONS,
    ORDERS,
    ITEMS_SOLD,
    NET_REVENUE
FROM products
