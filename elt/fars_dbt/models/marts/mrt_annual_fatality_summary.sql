-- models/marts/mrt_annual_fatality_summary.sql
{{ config(materialized = 'table') }}

select
    accident_year,
    count(distinct case when driver_had_alcohol then accident_id end) as alcohol_accidents,
    count(distinct case when not driver_had_alcohol then accident_id end) as non_alcohol_accidents,
    count(distinct vehicle_id) as total_vehicles,
    count(distinct accident_id) as total_accidents,
    sum(fatalities) as total_fatalities,
    sum(deaths) as total_vehicle_deaths,
    count(distinct case when school_bus_involved then accident_id end) as school_bus_accidents
from {{ ref('int_accident_vehicle_joined') }}
group by accident_year
