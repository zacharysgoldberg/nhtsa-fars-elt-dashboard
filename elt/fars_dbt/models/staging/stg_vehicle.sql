-- models/staging/stg_vehicle.sql
{{ config(materialized = 'view') }}

select
    id as vehicle_id,
    accident_id,
    st_case,
    try_cast(numoccs as int) as num_of_occupants,
    try_cast(year as int) as year,
    try_cast(month as int) as month,
    try_cast(day as int) as day,
    try_cast(hour as int) as hour,
    try_cast(minute as int) as minute,
    state,
    harm_ev,
    man_coll as collision_manner,
    unittype,

    case 
        when lower(hit_run) = 'yes' then cast(1 as bit)
        else cast(0 as bit)
    end as hit_and_run,

    owner,
    make,
    model,
    try_cast(mod_year as int) as model_year,
    body_typ,
    vin,
    try_cast(j_knife as bit) as jackknifed,
    try_cast(tow_veh as bit) as towed,

    case 
        when lower(haz_inv) = 'yes' then cast(1 as bit)
        else cast(0 as bit)
    end as hazmat_involved,

    bus_use,
    spec_use as special_use,
    try_cast(trav_sp as int) as travel_spd_mph,

    case 
        when rollover like 'Rollover, Tripped by Object/Vehicle' then cast(1 as bit)
        else cast(0 as bit)
    end as rolled_over,

    rolinloc as rollover_location,
    impact1 as initial_impact,
    deformed,
    m_harm as main_harmful_event,

    case 
        when lower(fire_exp) = 'yes' then cast(1 as bit)
        else cast(0 as bit)
    end as fire_or_explosion,

    try_cast(deaths as int) as deaths,

    case 
        when lower(dr_drink) = 'yes' then cast(1 as bit)
        else cast(0 as bit)
    end as driver_had_alcohol

from dbo.vehicle
