{{ config(materialized = 'view') }}

with alcohol_vehicles as (
    select *
    from {{ ref('int_accident_vehicle_joined') }}
    where driver_had_alcohol = 1
),

vehicle_stats as (
    select
        count(*) as alcohol_vehicle_count,
        coalesce(sum(case when hit_and_run = 1 then 1 else 0 end), 0) as hit_and_run_count,
        coalesce(sum(case when rolled_over = 1 then 1 else 0 end), 0) as rollover_count,
        coalesce(sum(case when towed = 1 then 1 else 0 end), 0) as towed_count,
        coalesce(sum(num_of_occupants), 0) as total_occupants,
        coalesce(sum(deaths), 0) as total_vehicle_deaths,
        coalesce(cast(avg(cast(travel_spd_mph as decimal(18, 2))) as decimal(18, 1)), cast(0 as decimal(18, 1))) as avg_travel_speed
    from alcohol_vehicles
),

top_vehicle_profile as (
    select
        make as top_vehicle_make,
        model_year as top_model_year,
        body_typ as top_body_type,
        row_number() over (
            order by count(*) desc, make, model_year desc, body_typ
        ) as rn
    from alcohol_vehicles
    where make is not null
      and model_year is not null
      and body_typ is not null
    group by make, model_year, body_typ
)

select
    vs.*,
    tvp.top_vehicle_make,
    tvp.top_model_year,
    tvp.top_body_type
from vehicle_stats vs
left join (
    select
        top_vehicle_make,
        top_model_year,
        top_body_type
    from top_vehicle_profile
    where rn = 1
) tvp on 1 = 1