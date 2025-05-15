-- models/marts/mrt_alcohol_related_vehicles.sql
{{ config(materialized = 'view') }}

WITH alcohol_vehicles AS (
    SELECT *
    FROM {{ ref('int_accident_vehicle_joined') }}
    WHERE driver_had_alcohol = TRUE
),

vehicle_stats as (
    SELECT
        COUNT(*) AS alcohol_vehicle_count,
        SUM(CASE WHEN hit_and_run THEN 1 ELSE 0 END) AS hit_and_run_count,
        SUM(CASE WHEN rolled_over THEN 1 ELSE 0 END) AS rollover_count,
        SUM(CASE WHEN towed THEN 1 ELSE 0 END) AS towed_count,
        SUM(num_of_occupants) AS total_occupants,
        SUM(deaths) AS total_vehicle_deaths,
        ROUND(AVG(model_year), 0) AS avg_model_year,
        ROUND(AVG(travel_spd_mph), 1) AS avg_travel_speed
    FROM alcohol_vehicles
),

make_ranked as (
    select make,
        count(*) as make_count,
        row_number() over (order by count(*) desc) as rn 
    from alcohol_vehicles
    group by make
),

body_type_ranked as (
    select body_typ,
        count(*) as body_type_count,
        row_number() over (order by count(*) desc) as rn 
    from alcohol_vehicles
    group by body_typ
),

top_make as (
    select make as most_common_make
    from make_ranked
    where rn = 1
),

top_model_for_top_make AS (
    SELECT model AS most_common_model
    FROM alcohol_vehicles
    WHERE make = (SELECT most_common_make FROM top_make)
    GROUP BY model
    ORDER BY COUNT(*) DESC
    LIMIT 1
),

top_body_type_of_top_model as (
    select body_typ as most_common_body_type
    from alcohol_vehicles
    WHERE model = (SELECT most_common_model FROM top_model_for_top_make)
    GROUP BY body_typ
    ORDER BY COUNT(*) DESC
    LIMIT 1
)

select vs.*,
    tmk.most_common_make,
    tmd.most_common_model,
    tb.most_common_body_type
from vehicle_stats vs
left join top_make tmk on true
left join top_model_for_top_make tmd on true
left join top_body_type_of_top_model tb on true

