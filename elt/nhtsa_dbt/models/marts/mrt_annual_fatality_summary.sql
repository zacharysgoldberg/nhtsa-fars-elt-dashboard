-- models/marts/mrt_annual_fatality_summary.sql
{{ config(materialized = 'table') }}

SELECT
    accident_year,
    COUNT(DISTINCT CASE WHEN driver_had_alcohol THEN accident_id END) AS alcohol_accidents,
    COUNT(DISTINCT CASE WHEN NOT driver_had_alcohol THEN accident_id END) AS non_alcohol_accidents,
    COUNT(DISTINCT vehicle_id) AS total_vehicles,
    COUNT(DISTINCT accident_id) AS total_accidents,
    SUM(fatalities) AS total_fatalities,
    SUM(deaths) AS total_vehicle_deaths,
    COUNT(DISTINCT CASE WHEN school_bus_involved THEN accident_id END) AS school_bus_accidents
FROM {{ ref('int_accident_vehicle_joined') }}
GROUP BY accident_year
