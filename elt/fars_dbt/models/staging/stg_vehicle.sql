-- models/staging/stg_vehicle.sql
{{ config(materialized = 'view') }}

SELECT
    id AS vehicle_id,
    accident_id,
    st_case,
    CAST(numoccs AS INTEGER) AS num_of_occupants,
    CAST(year AS INTEGER) AS year,
    CAST(month AS INTEGER) AS month,
    CAST(day AS INTEGER) AS day,
    CAST(hour AS INTEGER) AS hour,
    CAST(minute AS INTEGER) AS minute,
    state,
    harm_ev,
    man_coll AS collision_manner,
    unittype,

    CASE 
        WHEN hit_run ILIKE 'yes' THEN TRUE
        ELSE FALSE
    END AS hit_and_run,

    owner,
    make,
    model,
    CAST(mod_year AS INTEGER) AS model_year,
    body_typ,
    vin,
    CAST(j_knife AS BOOLEAN) AS jackknifed,
    CAST(tow_veh AS BOOLEAN) AS towed,

    CASE 
        WHEN haz_inv ILIKE 'yes' THEN TRUE
        ELSE FALSE
    END AS hazmat_involved,

    bus_use,
    spec_use AS special_use,
    CAST(trav_sp AS INTEGER) AS travel_spd_mph,

    CASE 
        WHEN rollover ILIKE 'Rollover, Tripped by Object/Vehicle' THEN TRUE
        ELSE FALSE
    END AS rolled_over,

    rolinloc AS rollover_location,
    impact1 AS initial_impact,
    deformed,
    m_harm AS main_harmful_event,

    CASE 
        WHEN fire_exp ILIKE 'yes' THEN TRUE
        ELSE FALSE
    END AS fire_or_explosion,

    CAST(deaths AS INTEGER) AS deaths,

    CASE 
        WHEN dr_drink ILIKE 'yes' THEN TRUE
        ELSE FALSE
    END AS driver_had_alcohol

FROM public.vehicle

