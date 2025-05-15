import pandas as pd
import psycopg2
from config.config import DB_CONFIG


def map_dtype_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        return "TEXT"


def build_create_table_sql(table_name, df, extra_fields=None, foreign_keys=None, unique_constraint=None):
    fields = []
    if extra_fields:
        fields.extend(extra_fields)

    for col in df.columns:
        col_type = map_dtype_to_postgres(df[col].dtype)
        fields.append(f'"{col}" {col_type}')

    if foreign_keys:
        fields.extend(foreign_keys)

    if unique_constraint:
        fields.append(
            f'CONSTRAINT {table_name}_unique UNIQUE ({", ".join(unique_constraint)})')

    field_defs = ",\n    ".join(fields)
    return f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {field_defs}
        )
    '''


def init_db(accident_df, vehicle_df):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Define foreign key and unique constraints
    vehicle_fk = [
        'FOREIGN KEY (accident_id) REFERENCES accident(id) ON DELETE CASCADE'
    ]

    # Accident uniqueness: each st_case in a given year should be unique
    accident_unique = ['st_case', 'year']
    vehicle_unique = ['accident_id', 'vin']

    # Create accident table
    accident_sql = build_create_table_sql(
        'accident',
        accident_df,
        extra_fields=['id SERIAL PRIMARY KEY'],
        unique_constraint=accident_unique
    )
    cursor.execute(accident_sql)

    # Create vehicle table
    vehicle_sql = build_create_table_sql(
        'vehicle',
        vehicle_df,
        extra_fields=['id SERIAL PRIMARY KEY', 'accident_id BIGINT NOT NULL'],
        foreign_keys=vehicle_fk,
        unique_constraint=vehicle_unique
    )
    cursor.execute(vehicle_sql)

    # Indexes
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_accident_st_case ON accident(st_case);')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_vehicle_st_case ON vehicle(st_case);')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_accident_case_year ON accident(st_case, year);')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_accident_year ON accident(year);')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_vehicle_year ON vehicle(year);')

    conn.commit()
    conn.close()
    print("PostgreSQL tables created with uniqueness constraints using dynamic schema.")
