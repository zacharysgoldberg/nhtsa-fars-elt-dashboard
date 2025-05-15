from glob import glob
import re
import pandas as pd
import os

# Define standardized shared columns (match names you want in final output)
shared_accident_columns = [
    'st_case', "state", "county", "city", "month", "day", "year", "day_of_week",
    "hour", "minute", "rural_urban", "func_sys", "road_owner", "nhs",
    "sp_jur", "latitude", "longitude", "milept", "tway_id", "tway_id2",
    "harm_event", "collision_manner", "rel_jct_1", "rel_jct_2", "intersection_type",
    "road_relation", "work_zone", "light_condition", "weather", "school_bus", "rail",
    "notify_hour", "notify_minute", "arrival_hour", "arrival_minute",
    "hospital_hour", "hospital_minute", "cf1", "cf2", "cf3", "fatalities"
]

# Define a rename map for variations across datasets
rename_accident_map = {
    'st_case': 'st_case', 'statename': 'state', 'countyname': 'county', 'cityname': 'city', 'year': 'year',
    'monthname': 'month', 'day': 'day', 'day_weekname': 'day_of_week',
    'hour': 'hour', 'minute': 'minute', 'rur_urbname': 'rural_urban',
    'func_sysname': 'func_sys', 'rd_ownername': 'road_owner', 'nhsname': 'nhs',
    'sp_jurname': 'sp_jur', 'mileptname': 'milept', 'longitud': 'longitude', 'latitude': 'latitude',
    'harm_evname': 'harm_event', 'man_collname': 'collision_manner', 'reljct1': 'rel_jct_1',
    'reljct2name': 'rel_jct_2', 'typ_intname': 'intersection_type', 'rel_roadname': 'road_relation',
    'wrk_zonename': 'work_zone', 'lgt_condname': 'light_condition', 'weathername': 'weather',
    'sch_bus': 'school_bus', 'railname': 'rail', 'not_hour': 'notify_hour',
    'not_min': 'notify_minute', 'arr_hour': 'arrival_hour', 'arr_min': 'arrival_minute',
    'hosp_hr': 'hospital_hour', 'hosp_mn': 'hospital_minute', 'cf1name': 'cf1',
    'cf2name': 'cf2', 'cf3name': 'cf3', 'fatals': 'fatalities'
}

shared_vehicle_columns = [
    'st_case', 'state', 'num_occupants', 'month', 'day', 'hour', 'minute', 'year',
    'harm_event', 'collision_manner', 'unit_type', 'hit_and_run', 'registration_status',
    'owner', 'make', 'model', 'model_year', 'body_type', 'gvwr_from', 'gvwr_to',
    'jackknife', 'vehicle_config', 'cargo_body_type', 'hazmat_involved', 'bus_use',
    'special_use', 'emergency_use', 'travel_spd_mph', 'under_override', 'rollover',
    'rollover_location', 'initial_impact', 'deformed', 'towed', 'most_harmful_event',
    'fire_explosion', 'deaths', 'driver_alcohol'
]

rename_vehicle_map = {
    'st_case': 'st_case', 'statename': 'state', 'numoccs': 'num_occupants', 'monthname': 'month',
    'dayname': 'day', 'hourname': 'hour', 'minutename': 'minute', 'year': 'year',
    'harm_evname': 'harm_event', 'man_collname': 'collision_manner', 'unittypename': 'unit_type',
    'hit_run': 'hit_and_run', 'reg_statname': 'registration_status', 'ownername': 'owner',
    'makename': 'make', 'mak_modname': 'model', 'mod_yearname': 'model_year',
    'body_typname': 'body_type', 'gvwr_fromname': 'gvwr_from', 'gvwr_toname': 'gvwr_to',
    'j_knife': 'jackknife', 'v_configname': 'vehicle_config', 'cargo_btname': 'cargo_body_type',
    'haz_inv': 'hazmat_involved', 'bus_use': 'bus_use', 'spec_use': 'special_use',
    'emer_use': 'emergency_use', 'trav_sp': 'travel_spd_mph', 'underoverridename': 'under_override',
    'rollover': 'rollover', 'rolinlocname': 'rollover_location', 'impact1name': 'initial_impact',
    'deformedname': 'deformed', 'tow_veh': 'towed', 'm_harmname': 'most_harmful_event',
               'fire_exp': 'fire_explosion', 'icfinalbody': 'ic_final_body', 'deaths': 'deaths',
               'dr_drink': 'driver_alcohol'
}


def _hybrid_filter_and_rename(df: pd.DataFrame, numeric_whitelist: set) -> pd.DataFrame:
    """
    1) Keep any column in numeric_whitelist.
    2) Keep any column that endswith 'name' *only if* its base column (col[:-4]) is NOT in numeric_whitelist.
    3) Keep any other column that has NO corresponding 'name' variant.
    4) Drop numeric columns that have a 'name' variant but are NOT whitelisted.
    5) Finally, rename kept 'name' columns by stripping the 'name' suffix.
    """
    cols = df.columns.tolist()
    filtered = []
    for col in cols:
        # 1) numeric whitelist
        if col in numeric_whitelist:
            filtered.append(col)
            continue

        # 2) name columns
        if col.endswith('name'):
            base = col[:-4]
            if base not in numeric_whitelist:
                filtered.append(col)
            continue

        # 3) other columns: keep if they have no 'name' counterpart
        if f"{col}name" not in cols:
            filtered.append(col)
        # else: numeric col with a name-variant, drop it

    # Subset DF
    df = df[filtered]

    # 5) rename name-columns
    rename_map = {col: col[:-4] for col in filtered if col.endswith('name')}
    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def standardize_fars_accident_data():
    input_dir = 'data/raw/fars_data/'
    output_file = 'data/processed/fars_data/standardized/standardized_accident_data.csv'

    data_files = glob(os.path.join(
        input_dir, '**', 'accident.csv'), recursive=True)

    numeric_whitelist = {
        'month', 'day', 'hour', 'minute', 'not_hour', 'not_minute', 'arr_hour', 'arr_minute',
        'hosp_hour', 'hosp_minute'
    }

    all_dfs = []

    for fp in data_files:
        try:
            df = pd.read_csv(fp, encoding='ISO-8859-1')
        except Exception as e:
            print(f"Failed to read {fp}: {e}")
            continue

        df.columns = [c.lower() for c in df.columns]
        df = _hybrid_filter_and_rename(df, numeric_whitelist)
        all_dfs.append(df)

    if not all_dfs:
        print("No accident data files were processed.")
        return None

    final = pd.concat(all_dfs, ignore_index=True)

    if final.empty:
        print("No data after filtering; the final dataframe is empty.")
        return None

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    final.to_csv(output_file, index=False)
    print(f"Standardized accident data saved to {output_file}")


def standardize_fars_vehicle_data():
    input_dir = 'data/raw/fars_data/'
    output_file = 'data/processed/fars_data/standardized/standardized_vehicle_data.csv'

    data_files = glob(os.path.join(
        input_dir, '**', 'vehicle.csv'), recursive=True)

    numeric_whitelist = {
        'month', 'day', 'hour', 'minute', 'trav_sp', 'mod_year', 'dr_weight', 'tow_veh', 'j_knife'
    }

    all_dfs = []

    for fp in data_files:
        try:
            df = pd.read_csv(fp, encoding='ISO-8859-1')
        except Exception as e:
            print(f"Failed to read {fp}: {e}")
            continue

        df.columns = [c.lower() for c in df.columns]

        # Extract model from mak_modname column if present
        if 'mak_modname' in df.columns:

            def extract_model(val):
                if isinstance(val, str):
                    parts = val.split(' ', 1)
                    return parts[1].strip() if len(parts) > 1 else None
                return None

            df['model'] = df['mak_modname'].apply(extract_model)

        df = _hybrid_filter_and_rename(df, numeric_whitelist)

        all_dfs.append(df)

    if not all_dfs:
        print("No vehicle data files were processed.")
        return None

    final = pd.concat(all_dfs, ignore_index=True)

    if final.empty:
        print("No data after filtering; the final dataframe is empty.")
        return None

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    final.to_csv(output_file, index=False)
    print(f"Standardized vehicle data saved to {output_file}")
