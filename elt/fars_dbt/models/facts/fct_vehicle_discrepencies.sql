-- models/facts/fct_vehicle_discrepencies.sql
{{ config(materialized='view') }}

with multi_vehicle_accidents as (
    select accident_id
    from {{ ref('stg_vehicle') }}
    group by accident_id
    having count(*) > 1
),

joined as (
    select
        accident_id,
        vehicle_id,
        deaths,
        fatalities
    from {{ ref('int_accident_vehicle_joined') }}
    where accident_id in (select accident_id from multi_vehicle_accidents)
)

select *
from joined
where fatalities = 0
  and deaths > 0
