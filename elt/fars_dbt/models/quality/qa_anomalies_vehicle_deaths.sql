-- models/quality/qa_anomalies_vehicle_deaths.sql
{{ config(materialized='view') }}

select vehicle_id,
    accident_id,
    num_of_occupants,
    deaths,
    case
        when deaths > num_of_occupants then 'Too many fatalities'
        when deaths < 0 or num_of_occupants < 0 then 'Negative values'
        else 'Valid'
    end as inconsistency_reason
from {{ ref('stg_vehicle') }}
where
    deaths > num_of_occupants
    or deaths < 0
