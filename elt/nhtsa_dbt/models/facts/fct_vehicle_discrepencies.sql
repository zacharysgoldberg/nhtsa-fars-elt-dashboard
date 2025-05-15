-- models/facts/fct_vehicle_discrepencies.sql
{{ config(materialized='view') }}

WITH multi_vehicle_accidents AS (
    SELECT accident_id
    FROM {{ ref('stg_vehicle') }}
    GROUP BY accident_id
    HAVING COUNT(*) > 1
),

joined AS (
    SELECT
        accident_id,
        vehicle_id,
        deaths,
        fatalities
    FROM {{ ref('int_accident_vehicle_joined') }}
    WHERE accident_id IN (SELECT accident_id FROM multi_vehicle_accidents)
)

SELECT *
FROM joined
WHERE fatalities = 0
  AND deaths > 0
