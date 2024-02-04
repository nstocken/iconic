{{ config(materialized='table', bind=false) }}

WITH
source AS (
    SELECT * FROM {{ source('public', 'products') }}
),

renamed AS (
    SELECT
     id AS item_id,
     url,
     name AS product_name,
     category AS product_category,
     style AS product_style,
     description AS product_description,
     ABS(price) AS product_price,
     image AS product_image,
     gender_affinity,
     ABS(current_stock) AS current_stock
    FROM
        source
)

SELECT * FROM renamed