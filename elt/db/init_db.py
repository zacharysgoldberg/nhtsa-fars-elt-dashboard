import pandas as pd


def map_dtype_to_sqlserver(column_name: str, dtype) -> str:
    column_name = column_name.lower()

    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME2"

    # SQL Server cannot use VARCHAR(MAX)/NVARCHAR(MAX) in unique constraints
    # or normal index keys, so keep string columns bounded.
    bounded_text_columns = {
        "vin": "VARCHAR(64)",
        "st_case": "VARCHAR(64)",
        "state": "VARCHAR(128)",
        "county": "VARCHAR(128)",
        "city": "VARCHAR(128)",
        "make": "VARCHAR(128)",
        "model": "VARCHAR(128)",
    }

    return bounded_text_columns.get(column_name, "VARCHAR(255)")


def build_create_table_sql(
    table_name: str,
    df: pd.DataFrame,
    extra_fields=None,
    foreign_keys=None,
    unique_constraint=None
):
    fields = []

    # Add predefined fields (like PK)
    if extra_fields:
        fields.extend(extra_fields)

    # Add dataframe columns
    for col in df.columns:
        col_type = map_dtype_to_sqlserver(col, df[col].dtype)
        fields.append(f"[{col}] {col_type}")

    # Foreign keys
    if foreign_keys:
        fields.extend(foreign_keys)

    # Unique constraints
    if unique_constraint:
        fields.append(
            f"CONSTRAINT {table_name}_unique UNIQUE ({', '.join(unique_constraint)})"
        )

    field_defs = ",\n    ".join(fields)

    return f"""
    IF NOT EXISTS (
        SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U'
    )
    CREATE TABLE dbo.{table_name} (
        {field_defs}
    )
    """


def init_db(accident_df: pd.DataFrame, vehicle_df: pd.DataFrame, conn):
    cursor = conn.cursor()

    # Ensure NaNs become NULL
    accident_df = accident_df.where(pd.notnull(accident_df), None)
    vehicle_df = vehicle_df.where(pd.notnull(vehicle_df), None)

    # Foreign key definition
    vehicle_fk = [
        "FOREIGN KEY (accident_id) REFERENCES dbo.accident(id) ON DELETE CASCADE"
    ]

    # Constraints
    accident_unique = ['st_case', 'year']
    vehicle_unique = ['accident_id', 'veh_no'] if 'veh_no' in vehicle_df.columns else ['accident_id', 'vin']

    # =========================
    # Create Accident Table
    # =========================
    accident_sql = build_create_table_sql(
        'accident',
        accident_df,
        extra_fields=[
            "id INT IDENTITY(1,1) PRIMARY KEY"
        ],
        unique_constraint=accident_unique
    )
    cursor.execute(accident_sql)

    # =========================
    # Create Vehicle Table
    # =========================
    vehicle_sql = build_create_table_sql(
        'vehicle',
        vehicle_df,
        extra_fields=[
            "id INT IDENTITY(1,1) PRIMARY KEY",
            "accident_id INT NOT NULL"
        ],
        foreign_keys=vehicle_fk,
        unique_constraint=vehicle_unique
    )
    cursor.execute(vehicle_sql)

    # =========================
    # Indexes (SQL Server safe)
    # =========================

    cursor.execute("""
    IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'idx_accident_st_case')
    CREATE INDEX idx_accident_st_case ON dbo.accident(st_case)
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'idx_vehicle_st_case')
    CREATE INDEX idx_vehicle_st_case ON dbo.vehicle(st_case)
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'idx_accident_case_year')
    CREATE INDEX idx_accident_case_year ON dbo.accident(st_case, year)
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'idx_accident_year')
    CREATE INDEX idx_accident_year ON dbo.accident(year)
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'idx_vehicle_year')
    CREATE INDEX idx_vehicle_year ON dbo.vehicle(year)
    """)

    conn.commit()

    print("✅ SQL Server tables created with constraints and indexes.")
