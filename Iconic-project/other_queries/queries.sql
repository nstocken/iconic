-- Transition Rates
WITH total AS (
    SELECT
        SUM(total_product_viewed) AS total_product_viewed,
        SUM(total_product_added) AS total_product_added,
        SUM(total_cart_viewed) AS total_cart_viewed,
        SUM(total_checkout_started) AS total_checkout_started,
        SUM(total_order_completed) AS total_order_completed
    FROM
        summary_table
)

SELECT
    (total_product_added / total_product_viewed) * 100 AS product_viewed_to_added_percentage,
    (total_cart_viewed / total_product_added) * 100 AS product_added_to_cart_viewed_percentage,
    (total_checkout_started / total_cart_viewed) * 100 AS cart_viewed_to_checkout_started_percentage,
    (total_order_completed / total_checkout_started) * 100 AS checkout_started_to_order_completed_percentage
FROM
    total;
-- Cart abadonment
SELECT (SUM(total_orders) - SUM(total_order_completed)) / SUM(total_orders) AS cart_abandonment_rate
FROM
    summary_table;
SELECT
    AVG(
        EXTRACT(
            EPOCH
            FROM
            (order_completed - checkout_started)
        )
    ) AS avg_time_to_purchase
FROM
    transaction_journey
WHERE
    checkout_started NOTNULL
    AND order_completed NOTNULL
  