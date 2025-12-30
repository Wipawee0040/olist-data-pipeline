SELECT
    customer_id,
    city,
    state,
    zip_code
FROM {{ ref('stg_customers') }}
