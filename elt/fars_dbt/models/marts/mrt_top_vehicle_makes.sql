-- models/marts/mrt_top_vehicle_makes.sql
{{ config(materialized = 'table') }}

select
    lower(trim(make)) as vehicle_make,
    count(*) as total_vehicles_involved,
    count(distinct accident_id) as distinct_accidents
from {{ ref('stg_vehicle') }}
where make is not null and make != ''
group by vehicle_make
order by total_vehicles_involved desc
limit 10
