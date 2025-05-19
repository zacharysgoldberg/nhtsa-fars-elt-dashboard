-- models/facts/fct_fatal_accident_by_weather.sql
{{ config(materialized='view') }}

SELECT
    weather_condition,
    COUNT(*) AS total_accidents,
    SUM(fatalities) AS total_fatalities,
    AVG(fatalities) AS avg_fatalities
FROM {{ ref('stg_accident') }}
WHERE weather_condition IS NOT NULL
AND weather_condition NOT IN ('Unknown', 'Not Reported', 'NOT APPLICABLE')
GROUP BY weather_condition
ORDER BY total_accidents desc
