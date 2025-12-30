{{ config(materialized='table') }}

WITH dates AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2019-12-31' as date)"
    ) }}

)

SELECT
    date_day,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day,
    EXTRACT(week FROM date_day) AS week,
    TO_CHAR(date_day, 'Day') AS day_name,
    CASE
        WHEN EXTRACT(dow FROM date_day) IN (0,6) THEN true
        ELSE false
    END AS is_weekend
FROM dates
