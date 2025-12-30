SELECT
*
FROM {{ source('silver', 'order_payments') }}