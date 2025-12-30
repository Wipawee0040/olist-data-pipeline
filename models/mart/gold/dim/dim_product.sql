{{ config(materialized='table') }}

SELECT
    product_id,
    category,
    name_length,
    desc_length,
    photos_qty,
    weight_g,
    length_cm,
    height_cm,
    width_cm
FROM {{ ref('stg_product') }}
