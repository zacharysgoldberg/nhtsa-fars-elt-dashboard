-- models/marts/mrt_top_vehicle_makes.sql
{{ config(materialized = 'table') }}

SELECT
    LOWER(TRIM(make)) AS vehicle_make,
    COUNT(*) AS total_vehicles_involved,
    COUNT(DISTINCT accident_id) AS distinct_accidents
FROM {{ ref('stg_vehicle') }}
WHERE make IS NOT NULL AND make != ''
GROUP BY vehicle_make
ORDER BY total_vehicles_involved DESC
LIMIT 10
