{{ config(materialized='table', bind=false) }}


WITH numbered AS (
    SELECT
        *,
        CASE WHEN event_type = 'ProductViewed' THEN 1
             WHEN event_type = 'ProductAdded' THEN 2
             WHEN event_type = 'CartViewed' THEN 3
             WHEN event_type = 'CheckoutStarted' THEN 4
             WHEN event_type = 'OrderCompleted' THEN 5
        END AS event_type_number,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, item_id
            ORDER BY timestamp
        ) AS interaction_number
    FROM
        {{ ref('stg_interactions') }}
),

lagged AS (
    SELECT
        *,
        LAG(timestamp, 1) OVER (
            PARTITION BY user_id, item_id
            ORDER BY interaction_number
        ) AS previous_timestamp,
        LAG(event_type_number, 1) OVER (
            PARTITION BY user_id, item_id
            ORDER BY interaction_number
        ) AS previous_event_type_number
    FROM
        numbered
),

diffed AS (
    SELECT
        *,
        EXTRACT(EPOCH FROM timestamp - previous_timestamp) AS period_of_inactivity
    FROM
        lagged
),

new_journeys AS (
    SELECT
        *,
        CASE
            WHEN (
                period_of_inactivity >= 60 * 30
                OR previous_timestamp IS NULL OR event_type_number <= previous_event_type_number
            ) THEN 1
            ELSE 0
        END AS is_new_journey
    FROM
        diffed
),

journey_numbers AS (
    SELECT
        *,
        SUM(is_new_journey) OVER (
            PARTITION BY user_id, item_id
            ORDER BY interaction_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS journey_number
    FROM
        new_journeys
),

journey_ids AS (
    SELECT
        *,
        CONCAT(
            user_id,
            '_',
            item_id,
            '_',
            journey_number
        ) AS journey_id,
        BOOL_OR(discount) OVER ( PARTITION BY user_id,item_id,journey_number ) AS journey_discount
    FROM
        journey_numbers
),


pivot AS (
    SELECT
        journey_id,
        j.item_id,
        user_id,
        product_name,
        product_category,
        MAX(CASE WHEN event_type = 'ProductViewed' THEN timestamp END) AS product_viewed,
        MAX(CASE WHEN event_type = 'ProductAdded' THEN timestamp END) AS product_added,
        MAX(CASE WHEN event_type = 'CartViewed' THEN timestamp END) AS cart_viewed,
        MAX(CASE WHEN event_type = 'CheckoutStarted' THEN timestamp END) AS checkout_started,
        MAX(CASE WHEN event_type = 'OrderCompleted' THEN timestamp END) AS order_completed,
        journey_discount AS discount,
        product_price
    FROM
        journey_ids j
    LEFT JOIN {{ ref('stg_products') }} p ON j.item_id = p.item_id
    GROUP BY
        1,2,3,4,5,11,12
)

SELECT * FROM pivot
