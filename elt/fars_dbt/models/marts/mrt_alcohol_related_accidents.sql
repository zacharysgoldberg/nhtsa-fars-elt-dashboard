-- models/marts/mrt_alcohol_related_accidents.sql
{{ config(materialized = 'view') }}

WITH alcohol_vehicles AS (
    SELECT *
    FROM {{ ref('int_accident_vehicle_joined') }}
    WHERE driver_had_alcohol = TRUE
    AND city is not null and city not in ('NOT APPLICABLE', 'Not Reported', 'Unknown')
),

accident_stats AS (
    SELECT
        COUNT(*) AS total_accidents,
        SUM(fatalities) AS total_fatalities,
        AVG(fatalities) AS avg_fatalities_per_accident,
        SUM(CASE WHEN at_junction THEN 1 ELSE 0 END) AS junction_related_count,
        SUM(CASE WHEN school_bus_involved THEN 1 ELSE 0 END) AS school_bus_incidents,

        -- Most common values
        MODE() WITHIN GROUP (ORDER BY state) AS most_common_state,
        MODE() WITHIN GROUP (ORDER BY county) AS most_common_county,
        MODE() WITHIN GROUP (ORDER BY city) AS most_common_city,
        MODE() WITHIN GROUP (ORDER BY day_of_week) AS most_common_day,
        MODE() WITHIN GROUP (ORDER BY hour) AS peak_hour,
        MODE() WITHIN GROUP (ORDER BY rur_urb) AS dominant_area_type,
        MODE() WITHIN GROUP (ORDER BY light_condition) AS most_common_light_condition,
        MODE() WITHIN GROUP (ORDER BY weather_condition) AS most_common_weather_condition
    FROM alcohol_vehicles
)

SELECT * FROM accident_stats