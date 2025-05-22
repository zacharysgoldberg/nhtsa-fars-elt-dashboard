-- models/marts/mrt_accidents_by_time_of_day.sql
{{ config(materialized='view') }}

select
    hour,
    case
        when hour between 6 and 9 then 'Morning Rush'
        when hour between 9 and 16 then 'Afternoon'
        when hour between 16 and 19 then 'Evening Rush'
        when hour between 19 and 23 then 'Night'
        else 'Late Night'
    end as time_of_day,
    case
        when hour between 6 and 9 then 1
        when hour between 9 and 16 then 2
        when hour between 16 and 19 then 3
        when hour between 19 and 23 then 4
        else 5
    end as time_of_day_order,
    count(accident_id) as total_accidents,
    count(vehicle_id) as total_vehicles, 
    sum(fatalities) as total_fatalities
from {{ ref('int_accident_vehicle_joined') }}
group by time_of_day, time_of_day_order, hour
order by time_of_day_order, hour
