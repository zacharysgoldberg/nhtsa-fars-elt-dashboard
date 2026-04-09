import pandas as pd

INSERT_BATCH_SIZE = 5000


def save_to_db(df: pd.DataFrame, table: str, conn):
    if df.empty:
        print(f"No data to insert into '{table}' table.")
        return

    # Replace NaN with None for SQL Server
    df = df.where(pd.notnull(df), None)

    cursor = conn.cursor()

    # =========================
    # Handle table-specific logic
    # =========================
    if table == 'accident':
        conflict_keys = ['st_case', 'year']

    elif table == 'vehicle':
        acc_df = pd.read_sql(
            "SELECT id AS accident_id, st_case, year FROM dbo.accident", conn
        )

        if acc_df is None or acc_df.empty:
            raise ValueError(
                "Accident table must be populated before vehicle load.")

        df = df.merge(acc_df, on=['st_case', 'year'], how='left')

        if df['accident_id'].isnull().any():
            raise ValueError(
                "Some vehicle records could not be matched to accidents.")

        df['accident_id'] = df['accident_id'].astype(int)
        conflict_keys = ['accident_id', 'vin']

    else:
        raise ValueError(f"Unknown table '{table}'")

    # =========================
    # Create staging table
    # =========================
    staging_table = f"{table}_staging"

    try:
        print(f"Starting load for '{table}' with {len(df):,} rows...")

        # Drop staging table if exists
        cursor.execute(f"""
        IF OBJECT_ID('dbo.{staging_table}', 'U') IS NOT NULL
            DROP TABLE dbo.{staging_table}
        """)

        # Create staging table using the target table's column types.
        columns = list(df.columns)
        target_cols = ", ".join([f"[{col}]" for col in columns])

        cursor.execute(f"""
        SELECT TOP 0 {target_cols}
        INTO dbo.{staging_table}
        FROM dbo.{table}
        """)

        # =========================
        # Bulk insert into staging
        # =========================
        placeholders = ",".join(["?"] * len(columns))
        insert_sql = f"""
            INSERT INTO dbo.{staging_table} ({",".join([f"[{c}]" for c in columns])})
            VALUES ({placeholders})
        """

        rows = df.values.tolist()
        cursor.fast_executemany = False

        for start in range(0, len(rows), INSERT_BATCH_SIZE):
            batch = rows[start:start + INSERT_BATCH_SIZE]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            end = min(start + INSERT_BATCH_SIZE, len(rows))
            print(
                f"Loaded staging rows for '{table}': {end:,}/{len(rows):,}")

        # =========================
        # Insert only new rows from staging
        # =========================
        print(f"Applying deduplicated insert into dbo.{table}...")

        exists_clause = " AND ".join(
            [f"target.[{k}] = source.[{k}]" for k in conflict_keys]
        )

        insert_cols = ", ".join([f"[{c}]" for c in columns])
        source_cols = ", ".join([f"source.[{c}]" for c in columns])

        insert_new_sql = f"""
        INSERT INTO dbo.{table} ({insert_cols})
        SELECT {source_cols}
        FROM dbo.{staging_table} AS source
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.{table} AS target
            WHERE {exists_clause}
        );
        """

        cursor.execute(insert_new_sql)
        conn.commit()
        print(f"Inserted new rows into dbo.{table}.")

        cursor.execute(f"""
        IF OBJECT_ID('dbo.{staging_table}', 'U') IS NOT NULL
            DROP TABLE dbo.{staging_table}
        """)

        conn.commit()

        print(f"✅ Saved {len(df):,} records to '{table}' table.")

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"❌ Error inserting data into {table}: {e}")
        raise
