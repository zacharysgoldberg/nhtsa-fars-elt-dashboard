-- models/marts/mrt_alcohol_related_accidents.sql
{{ config(materialized = 'view') }}

with alcohol_vehicles as (
    select *
    from {{ ref('int_accident_vehicle_joined') }}
    where driver_had_alcohol = TRUE
    and city is not null and city not in ('NOT APPLICABLE', 'Not Reported', 'Unknown')
),

accident_stats as (
    select
        count(*) as total_accidents,
        sum(fatalities) as total_fatalities,
        avg(fatalities) as avg_fatalities_per_accident,
        sum(case when at_junction then 1 else 0 end) as junction_related_count,
        sum(case when school_bus_involved then 1 else 0 end) as school_bus_incidents,

        -- Most common values
        mode() within group (order by state) as most_common_state,
        mode() within group (order by county) as most_common_county,
        mode() within group (order by city) as most_common_city,
        mode() within group (order by day_of_week) as most_common_day,
        mode() within group (order by hour) as peak_hour,
        mode() within group (order by rur_urb) as dominant_area_type,
        mode() within group (order by light_condition) as most_common_light_condition,
        mode() within group (order by weather_condition) as most_common_weather_condition
    from alcohol_vehicles
)

select * from accident_stats