{{ config(materialized='table', bind=false) }}

WITH
source AS (
    SELECT * FROM {{ source('public', 'users') }}
),

renamed AS (
    SELECT
     id AS user_id,
     username,
     email,
     first_name,
     last_name,
     (REPLACE(REPLACE(addresses, '''', '"'),'True','true')::json ->> 0)::json->>'address1' AS address1,
     (REPLACE(REPLACE(addresses, '''', '"'),'True','true')::json ->> 0)::json->>'address2' AS address2,
     (REPLACE(REPLACE(addresses, '''', '"'),'True','true')::json ->> 0)::json->>'country' AS country,
     (REPLACE(REPLACE(addresses, '''', '"'),'True','true')::json ->> 0)::json->>'city' AS city,
     (REPLACE(REPLACE(addresses, '''', '"'),'True','true')::json ->> 0)::json->>'state' AS state,
     (REPLACE(REPLACE(addresses, '''', '"'),'True','true')::json ->> 0)::json->>'zipcode' AS zipcode,
     (REPLACE(REPLACE(addresses, '''', '"'),'True','true')::json ->> 0)::json->>'default' AS is_default,
     age,
     gender,
     persona,
     discount_persona
    FROM
        source
)

SELECT * FROM renamed