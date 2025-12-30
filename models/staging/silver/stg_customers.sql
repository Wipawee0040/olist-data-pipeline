SELECT
*
FROM {{ source('silver', 'customers') }}