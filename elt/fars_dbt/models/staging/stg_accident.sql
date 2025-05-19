-- models/staging/stg_accident.sql
{{ config(materialized = 'view') }}

SELECT
    id AS accident_id,  -- keep as-is, primary key
    st_case,
    state,
    county,
    city,
    CAST(year AS INTEGER) AS accident_year,
    CAST(month AS INTEGER) AS month,
    CAST(day AS INTEGER) AS day,
    day_week AS day_of_week,
    CAST(hour AS INTEGER) AS hour,
    CAST(minute AS INTEGER) AS minute,
    CAST(latitude AS FLOAT) AS lat,
    CAST(longitud AS FLOAT) AS lon,
    rur_urb,
    func_sys,
    rd_owner,
    CAST(milept AS INTEGER) AS milept,
    nhs,
    sp_jur,
    harm_ev,
    man_coll AS collision_manner,

    CASE 
        WHEN reljct1 ILIKE 'yes' THEN TRUE
        ELSE FALSE
    END AS at_junction,

    typ_int AS intersection_type,
    rel_road AS road_relation,
    lgt_cond AS light_condition,
    weather AS weather_condition,

    CASE 
        WHEN sch_bus ILIKE 'yes' THEN TRUE
        ELSE FALSE
    END AS school_bus_involved,

    CAST(drunk_dr AS INTEGER) AS drunk_drivers,
    fatals AS fatalities
FROM public.accident

