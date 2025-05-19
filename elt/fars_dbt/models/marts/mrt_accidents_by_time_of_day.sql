-- models/marts/mrt_accidents_by_time_of_day.sql
{{ config(materialized='view') }}

SELECT
    hour,
    CASE
        WHEN hour BETWEEN 6 AND 9 THEN 'Morning Rush'
        WHEN hour BETWEEN 9 AND 16 THEN 'Afternoon'
        WHEN hour BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN hour BETWEEN 19 AND 23 THEN 'Night'
        ELSE 'Late Night'
    END AS time_of_day,
    CASE
        WHEN hour BETWEEN 6 AND 9 THEN 1
        WHEN hour BETWEEN 9 AND 16 THEN 2
        WHEN hour BETWEEN 16 AND 19 THEN 3
        WHEN hour BETWEEN 19 AND 23 THEN 4
        ELSE 5
    END AS time_of_day_order,
    COUNT(accident_id) AS total_accidents,
    COUNT(vehicle_id) AS total_vehicles, 
    SUM(fatalities) AS total_fatalities
FROM {{ ref('int_accident_vehicle_joined') }}
GROUP BY time_of_day, time_of_day_order, hour
ORDER BY time_of_day_order, hour
