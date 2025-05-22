-- models/facts/fct_fatal_accidents.sql
{{ config(materialized='view') }}

select
    accident_year,
    state,
    county,
    count(*) AS total_accidents,
    sum(fatalities) AS total_fatalities,
    avg(fatalities) AS avg_fatalities_per_accident
from {{ ref('stg_accident') }}
where fatalities > 0
group by accident_year, state, county
