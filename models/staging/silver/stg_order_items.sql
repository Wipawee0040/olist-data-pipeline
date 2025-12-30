SELECT
*
FROM {{ source('silver', 'order_items') }}