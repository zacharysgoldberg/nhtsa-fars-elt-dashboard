-- models/facts/fct_fatal_accidents.sql
{{ config(materialized='view') }}

SELECT
    accident_year,
    state,
    county,
    COUNT(*) AS total_accidents,
    SUM(fatalities) AS total_fatalities,
    AVG(fatalities) AS avg_fatalities_per_accident
FROM {{ ref('stg_accident') }}
WHERE fatalities > 0
GROUP BY accident_year, state, county
