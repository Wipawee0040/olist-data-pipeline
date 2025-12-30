SELECT 
*
FROM {{ source('silver', 'orders') }}
