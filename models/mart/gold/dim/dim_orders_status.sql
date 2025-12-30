{{ config(materialized='table') }}

SELECT DISTINCT
    order_status,
    delivery_phase
FROM {{ ref('stg_orders') }}
