SELECT
*
FROM {{ source('silver', 'order_reviews') }}