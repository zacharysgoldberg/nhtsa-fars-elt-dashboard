-- models/marts/mrt_body_type_accident_outcomes.sql
{{ config(materialized='view') }}

WITH vehicle_data AS (
    SELECT *
    FROM {{ ref('int_accident_vehicle_joined') }}
    WHERE body_typ IS NOT NULL AND body_typ NOT LIKE '%Unknown%'
),

body_type_summary AS (
    SELECT
        body_typ,
        COUNT(*) AS total_vehicles,
        SUM(CASE WHEN rolled_over THEN 1 ELSE 0 END) AS rollover_count,
        SUM(CASE WHEN towed THEN 1 ELSE 0 END) AS towed_count,
        SUM(deaths) AS total_deaths,
        ROUND(AVG(travel_spd_mph), 2) AS avg_travel_speed
    FROM vehicle_data
    GROUP BY body_typ
),

percentages AS (
    SELECT
        body_typ,
        total_vehicles,
        rollover_count,
        towed_count,
        total_deaths,
        avg_travel_speed,

        ROUND(100.0 * rollover_count / total_vehicles, 2) AS pct_rollovers,
        ROUND(100.0 * towed_count / total_vehicles, 2) AS pct_towed,
        ROUND(100.0 * total_deaths / NULLIF(total_vehicles, 0), 2) AS pct_fatalities
    FROM body_type_summary
)

SELECT * FROM percentages
ORDER BY total_vehicles DESC
