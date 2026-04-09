-- models/marts/mrt_top_vehicle_makes.sql
{{ config(materialized = 'table') }}

with grouped_makes as (
    select
        lower(ltrim(rtrim(make))) as vehicle_make,
        count(*) as total_vehicles_involved,
        count(distinct accident_id) as distinct_accidents
    from {{ ref('stg_vehicle') }}
    where make is not null and ltrim(rtrim(make)) != ''
    group by lower(ltrim(rtrim(make)))
),
ranked_makes as (
    select
        *,
        row_number() over (order by total_vehicles_involved desc, distinct_accidents desc, vehicle_make) as rn
    from grouped_makes
)

select
    vehicle_make,
    total_vehicles_involved,
    distinct_accidents
from ranked_makes
where rn <= 10
