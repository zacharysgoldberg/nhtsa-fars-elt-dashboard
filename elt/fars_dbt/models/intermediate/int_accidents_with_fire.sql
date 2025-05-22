-- models/intermediate/int_accidents_with_fire_summary.sql
{{ config(materialized = 'view') }}

with fire_vehicles as (
    select
        accident_id,
        count(*) as vehicles_with_fire
    from {{ ref('stg_vehicle') }}
    where fire_or_explosion = True
    group by accident_id
)

select
    a.accident_id,
    a.st_case,
    a.accident_year,
    a.city,
    a.state,
    a.weather_condition,
    a.light_condition,
    a.fatalities,
    fv.vehicles_with_fire
from {{ ref('stg_accident') }} a
inner join fire_vehicles fv
    on a.accident_id = fv.accident_id
