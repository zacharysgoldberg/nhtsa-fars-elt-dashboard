-- models/intermediate/int_accident_vehicle_joined.sql
{{ config(materialized = 'view') }}

select
    a.*,
    v.vehicle_id,
    v.num_of_occupants,
    v.make,
    v.model,
    v.model_year,
    v.body_typ,
    v.travel_spd_mph,
    v.hit_and_run,
    v.rolled_over,
    v.towed,
    v.deaths,
    v.driver_had_alcohol
from {{ ref('stg_accident') }} a
left join {{ ref('stg_vehicle') }} v
    on a.accident_id = v.accident_id
