-- models/marts/accidents/mrt_avg_fatalities_by_weather.sql
{{ config(materialized = 'table') }}

SELECT
    weather_condition,
    ROUND(AVG(fatalities), 2) AS avg_fatalities
FROM {{ ref('stg_accident') }}
GROUP BY weather_condition
ORDER BY avg_fatalities DESC
