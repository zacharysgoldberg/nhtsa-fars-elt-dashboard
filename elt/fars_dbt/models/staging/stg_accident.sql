-- models/staging/stg_accident.sql
{{ config(materialized = 'view') }}

select
    id AS accident_id,  -- keep as-is, primary key
    st_case,
    state,
    county,
    city,
    cast(year as integer) as accident_year,
    cast(month as integer) as month,
    cast(day as integer) as day,
    day_week as day_of_week,
    cast(hour as integer) as hour,
    cast(minute as integer) as minute,
    cast(latitude as float) as lat,
    cast(longitud as float) as lon,
    rur_urb,
    func_sys,
    rd_owner,
    cast(milept as integer) as milept,
    nhs,
    sp_jur,
    harm_ev,
    man_coll as collision_manner,

    case 
        when reljct1 ilike 'yes' then TRUE
        else FALSE
    end as at_junction,

    typ_int as intersection_type,
    rel_road as road_relation,
    lgt_cond as light_condition,
    weather as weather_condition,

    case 
        when sch_bus ilike 'yes' then TRUE
        else FALSE
    end as school_bus_involved,

    cast(drunk_dr as integer) as drunk_drivers,
    fatals as fatalities
from public.accident

