-- models/marts/mrt_top_accident_collision_manners.sql
{{ config(materialized = 'view') }}

with base_accidents as (
    select
        month,
        collision_manner
    FROM {{ ref('int_accident_vehicle_joined') }}
    where collision_manner is not null
    and collision_manner not in ('Not Reported', 'Unknown', 'Reported as Unknown') 
    and collision_manner not like '%The First Harmful Event%'
),

top_collision_manners AS (
    SELECT
        collision_manner,
        COUNT(*) AS count,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM base_accidents
    GROUP BY collision_manner
)

SELECT *
FROM top_collision_manners
