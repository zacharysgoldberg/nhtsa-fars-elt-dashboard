-- models/quality/qa_anomalies_vehicle_deaths.sql
{{ config(materialized='view') }}

SELECT vehicle_id,
    accident_id,
    num_of_occupants,
    deaths,
    CASE
        WHEN deaths > num_of_occupants THEN 'Too many fatalities'
        WHEN deaths < 0 OR num_of_occupants < 0 THEN 'Negative values'
        ELSE 'Valid'
    END AS inconsistency_reason
FROM {{ ref('stg_vehicle') }}
WHERE
    deaths > num_of_occupants
    OR deaths < 0
