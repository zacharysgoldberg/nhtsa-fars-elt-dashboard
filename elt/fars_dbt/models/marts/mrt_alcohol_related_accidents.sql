-- models/marts/mrt_alcohol_related_accidents.sql
{{ config(materialized = 'view') }}

with alcohol_vehicles as (
    select *
    from {{ ref('int_accident_vehicle_joined') }}
    where driver_had_alcohol = 1
    and city is not null and city not in ('NOT APPLICABLE', 'Not Reported', 'Unknown')
),

location_ranked as (
    select
        state,
        county,
        city,
        row_number() over (
            order by count(*) desc, state, county, city
        ) as rn
    from alcohol_vehicles
    where state is not null
      and county is not null
      and city is not null
    group by state, county, city
),

day_ranked as (
    select day_of_week,
        row_number() over (order by count(*) desc, day_of_week) as rn
    from alcohol_vehicles
    where day_of_week is not null
    group by day_of_week
),

hour_ranked as (
    select hour,
        row_number() over (order by count(*) desc, hour) as rn
    from alcohol_vehicles
    where hour is not null
    group by hour
),

area_ranked as (
    select rur_urb,
        row_number() over (order by count(*) desc, rur_urb) as rn
    from alcohol_vehicles
    where rur_urb is not null
    group by rur_urb
),

light_ranked as (
    select light_condition,
        row_number() over (order by count(*) desc, light_condition) as rn
    from alcohol_vehicles
    where light_condition is not null
    group by light_condition
),

weather_ranked as (
    select weather_condition,
        row_number() over (order by count(*) desc, weather_condition) as rn
    from alcohol_vehicles
    where weather_condition is not null
    group by weather_condition
),

accident_stats as (
    select
        count(*) as total_accidents,
        sum(fatalities) as total_fatalities,
        avg(fatalities) as avg_fatalities_per_accident,
        sum(case when at_junction = 1 then 1 else 0 end) as junction_related_count,
        sum(case when school_bus_involved = 1 then 1 else 0 end) as school_bus_incidents
    from alcohol_vehicles
)

select
    stats.*,
    location_top.state as most_common_state,
    location_top.county as most_common_county,
    location_top.city as most_common_city,
    day_top.day_of_week as most_common_day,
    hour_top.hour as peak_hour,
    area_top.rur_urb as dominant_area_type,
    light_top.light_condition as most_common_light_condition,
    weather_top.weather_condition as most_common_weather_condition
from accident_stats stats
left join (
    select state, county, city
    from location_ranked
    where rn = 1
) location_top on 1 = 1
left join (select day_of_week from day_ranked where rn = 1) day_top on 1 = 1
left join (select hour from hour_ranked where rn = 1) hour_top on 1 = 1
left join (select rur_urb from area_ranked where rn = 1) area_top on 1 = 1
left join (select light_condition from light_ranked where rn = 1) light_top on 1 = 1
left join (select weather_condition from weather_ranked where rn = 1) weather_top on 1 = 1
