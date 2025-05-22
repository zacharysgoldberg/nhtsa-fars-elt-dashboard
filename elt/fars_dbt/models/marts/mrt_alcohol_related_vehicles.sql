-- models/marts/mrt_alcohol_related_vehicles.sql
{{ config(materialized = 'view') }}

with alcohol_vehicles as (
    select *
    from {{ ref('int_accident_vehicle_joined') }}
    where driver_had_alcohol = TRUE
),

vehicle_stats as (
    select
        count(*) as alcohol_vehicle_count,
        sum(case when hit_and_run then 1 else 0 end) as hit_and_run_count,
        sum(case when rolled_over then 1 else 0 end) as rollover_count,
        sum(case when towed then 1 else 0 end) as towed_count,
        sum(num_of_occupants) as total_occupants,
        sum(deaths) as total_vehicle_deaths,
        round(avg(model_year), 0) as avg_model_year,
        round(avg(travel_spd_mph), 1) as avg_travel_speed
    from alcohol_vehicles
),

make_ranked as (
    select make,
        count(*) as make_count,
        row_number() over (order by count(*) desc) as rn 
    from alcohol_vehicles
    group by make
),

body_type_ranked as (
    select body_typ,
        count(*) as body_type_count,
        row_number() over (order by count(*) desc) as rn 
    from alcohol_vehicles
    group by body_typ
),

top_make as (
    select make as most_common_make
    from make_ranked
    where rn = 1
),

top_model_for_top_make AS (
    select model AS most_common_model
    from alcohol_vehicles
    where make = (select most_common_make from top_make)
    group by model
    order by count(*) desc
    limit 1
),

top_body_type_of_top_model as (
    select body_typ as most_common_body_type
    from alcohol_vehicles
    where model = (select most_common_model from top_model_for_top_make)
    group by body_typ
    order by count(*) desc
    limit 1
)

select vs.*,
    tmk.most_common_make,
    tmd.most_common_model,
    tb.most_common_body_type
from vehicle_stats vs
left join top_make tmk on true
left join top_model_for_top_make tmd on true
left join top_body_type_of_top_model tb on true

