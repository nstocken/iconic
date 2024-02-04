
  
    

  create  table "postgres"."public"."user_journey__dbt_tmp"
  as (
    

WITH user_data AS (
    SELECT
        user_id,
        username,
        email,
        first_name,
        last_name,
        address1,
        country,
        city,
        state,
        zipcode
    FROM "postgres"."public"."stg_users"
),

total_orders AS (
    SELECT
        user_id,
        COUNT(*) AS total_orders
    FROM "postgres"."public"."stg_interactions"
    WHERE event_type = 'OrderCompleted'
    GROUP BY 1
)

SELECT
    u.*,
    COALESCE(t.total_orders, 0) AS total_orders
FROM user_data AS u
LEFT JOIN total_orders AS t
    ON u.user_id = t.user_id
  );
  