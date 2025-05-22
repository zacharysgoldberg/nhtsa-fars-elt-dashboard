-- models/marts/accidents/mrt_avg_fatalities_by_weather.sql
{{ config(materialized = 'table') }}

select
    weather_condition,
    round(avg(fatalities), 2) as avg_fatalities
from {{ ref('stg_accident') }}
group by weather_condition
order by avg_fatalities desc
