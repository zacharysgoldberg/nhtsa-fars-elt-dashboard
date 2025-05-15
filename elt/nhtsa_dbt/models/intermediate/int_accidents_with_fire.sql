-- models/intermediate/int_accidents_with_fire_summary.sql
{{ config(materialized = 'view') }}

WITH fire_vehicles AS (
    SELECT
        accident_id,
        COUNT(*) AS vehicles_with_fire
    FROM {{ ref('stg_vehicle') }}
    WHERE fire_or_explosion = True
    GROUP BY accident_id
)

SELECT
    a.accident_id,
    a.st_case,
    a.accident_year,
    a.city,
    a.state,
    a.weather_condition,
    a.light_condition,
    a.fatalities,
    fv.vehicles_with_fire
FROM {{ ref('stg_accident') }} a
INNER JOIN fire_vehicles fv
    ON a.accident_id = fv.accident_id
