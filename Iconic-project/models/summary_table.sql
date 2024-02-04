{{ config(materialized='table', bind=false) }}


WITH base AS (
    SELECT
        t.product_category,
        COUNT(product_viewed) AS total_product_viewed,
        COUNT(product_added) AS total_product_added,
        COUNT(cart_viewed) AS total_cart_viewed,
        COUNT(checkout_started) AS total_checkout_started,
        COUNT(order_completed) AS total_order_completed,
        SUM(CASE WHEN product_added NOTNULL AND checkout_started NOTNULL THEN 1 END) AS total_orders,
        COUNT(DISTINCT CASE WHEN order_completed NOTNULL THEN user_id END) AS total_customers_ordered,
        SUM(CASE WHEN order_completed NOTNULL THEN t.product_price END) AS total_revenue
    FROM {{ ref('transaction_journey') }} AS t
    LEFT JOIN {{ ref('stg_products') }} AS p ON t.item_id = p.item_id
    GROUP BY 1
)

SELECT
    *,
    total_product_added
    + total_product_viewed
    + total_cart_viewed
    + total_checkout_started
    + total_order_completed AS total_interactions
FROM base
