-- models/facts/fct_fatal_accident_by_weather.sql
{{ config(materialized='view') }}

select
    weather_condition,
    count(*) AS total_accidents,
    sum(fatalities) AS total_fatalities,
    avg(fatalities) AS avg_fatalities
from {{ ref('stg_accident') }}
where weather_condition is not null
and weather_condition not in ('Unknown', 'Not Reported', 'NOT APPLICABLE')
group by weather_condition
order by total_accidents desc
