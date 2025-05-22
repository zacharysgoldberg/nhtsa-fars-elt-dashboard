-- models/staging/stg_vehicle.sql
{{ config(materialized = 'view') }}

select
    id as vehicle_id,
    accident_id,
    st_case,
    cast(numoccs as integer) as num_of_occupants,
    cast(year as integer) as year,
    cast(month as integer) as month,
    cast(day as integer) as day,
    cast(hour as integer) as hour,
    cast(minute as integer) as minute,
    state,
    harm_ev,
    man_coll as collision_manner,
    unittype,

    case 
        when hit_run ilike 'yes' then TRUE
        else FALSE
    end as hit_and_run,

    owner,
    make,
    model,
    cast(mod_year as integer) as model_year,
    body_typ,
    vin,
    cast(j_knife as BOOLEAN) as jackknifed,
    cast(tow_veh as BOOLEAN) as towed,

    case 
        when haz_inv ilike 'yes' then TRUE
        else FALSE
    end as hazmat_involved,

    bus_use,
    spec_use as special_use,
    cast(trav_sp as integer) as travel_spd_mph,

    case 
        when rollover ilike 'Rollover, Tripped by Object/Vehicle' then TRUE
        else FALSE
    end as rolled_over,

    rolinloc as rollover_location,
    impact1 as initial_impact,
    deformed,
    m_harm as main_harmful_event,

    case 
        when fire_exp ilike 'yes' then TRUE
        else FALSE
    end as fire_or_explosion,

    cast(deaths as integer) as deaths,

    case 
        when dr_drink ilike 'yes' then TRUE
        else FALSE
    end as driver_had_alcohol

from public.vehicle

