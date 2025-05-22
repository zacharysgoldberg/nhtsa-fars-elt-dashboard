-- models/marts/mrt_top_accident_collision_manners.sql
{{ config(materialized = 'view') }}

with base_accidents as (
    select
        month,
        collision_manner
    from {{ ref('int_accident_vehicle_joined') }}
    where collision_manner is not null
    and collision_manner not in ('Not Reported', 'Unknown', 'Reported as Unknown') 
    and collision_manner not like '%The First Harmful Event%'
),

top_collision_manners as (
    select
        collision_manner,
        count(*) as count,
        row_number() over (order by count(*) desc) as rn
    from base_accidents
    group by collision_manner
)

select *
from top_collision_manners
