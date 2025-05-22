-- models/marts/mrt_body_type_accident_outcomes.sql
{{ config(materialized='view') }}

with vehicle_data as (
    select *
    from {{ ref('int_accident_vehicle_joined') }}
    where body_typ is not null and body_typ not like '%Unknown%'
),

body_type_summary AS (
    SELECT
        body_typ,
        count(*) AS total_vehicles,
        sum(case when rolled_over then 1 else 0 end) as rollover_count,
        sum(case when towed then 1 else 0 end) as towed_count,
        sum(deaths) as total_deaths,
        round(avg(travel_spd_mph), 2) as avg_travel_speed
    from vehicle_data
    group by body_typ
),

percentages as (
    select
        body_typ,
        total_vehicles,
        rollover_count,
        towed_count,
        total_deaths,
        avg_travel_speed,

        round(100.0 * rollover_count / total_vehicles, 2) as pct_rollovers,
        round(100.0 * towed_count / total_vehicles, 2) as pct_towed,
        round(100.0 * total_deaths / nullif(total_vehicles, 0), 2) as pct_fatalities
    from body_type_summary
)

select * from percentages
order by total_vehicles desc
