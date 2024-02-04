{{ config(materialized='table', bind=false) }}

WITH
source AS (
    SELECT * FROM {{ source('public', 'interactions') }}
),

renamed AS (
    SELECT
    DISTINCT
     item_id,
     user_id,
     event_type,
     CAST(TO_TIMESTAMP(timestamp) AS TIMESTAMP) AS timestamp,
     discount
    FROM
        source
    WHERE user_id NOTNULL
)

SELECT * FROM renamed