import io
from psycopg2 import sql
from psycopg2.extras import execute_batch
from psycopg2.extensions import connection
import pandas as pd


# def save_to_db(df: pd.DataFrame, table: str, conn: connection):
#     if df.empty:
#         print(f"No data to insert into '{table}' table.")
#         return

#     df = df.where(pd.notnull(df), None)  # Replace NaN with None

#     cursor = conn.cursor()

#     # Add conflict resolution
#     if table == 'accident':
#         id1 = sql.SQL("st_case")
#         id2 = sql.SQL("year")

#     elif table == 'vehicle':
#         if 'st_case' not in df.columns:
#             raise ValueError("Vehicle data must include 'st_case' column.")

#         # Load accident mapping
#         acc_df = pd.read_sql(
#             "SELECT id AS accident_id, st_case, year FROM accident", conn)
#         df = df.merge(acc_df, on=['st_case', 'year'], how='left')

#         if df['accident_id'].isnull().any():
#             raise ValueError(
#                 "Some vehicle records could not be matched to accidents.")

#         id1 = sql.SQL("accident_id")
#         id2 = sql.SQL("vin")

#     columns = list(df.columns)
#     values = [tuple(row) for row in df.itertuples(index=False)]

#     # Construct the SQL safely
#     insert_sql = sql.SQL(
#         "INSERT INTO {table} ({fields}) VALUES ({placeholders}) ON CONFLICT ({id1}, {id2}) DO NOTHING")\
#         .format(
#         table=sql.Identifier(table),
#         fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
#         placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns)),
#         id1=id1,
#         id2=id2
#     )

#     try:
#         print("Executing query:", insert_sql.as_string(conn))
#         print("\nStarting bulk insert...\n")
#         execute_batch(cursor, insert_sql, values, page_size=1000)
#         conn.commit()
#         print("Insert completed.\n")
#     except Exception as e:
#         print(f"Error inserting data into {table}: {e}\n")
#         conn.rollback()
#     finally:
#         conn.close()

#     print(f"\nSaved {len(df):,} records to the '{table}' table.\n")


def save_to_db(df: pd.DataFrame, table: str, conn: connection):
    if df.empty:
        print(f"No data to insert into '{table}' table.")
        return

    df = df.where(pd.notnull(df), None)  # Replace NaN with None

    cursor = conn.cursor()

    if table == 'accident':
        conflict_keys = ['st_case', 'year']

    elif table == 'vehicle':
        acc_df = pd.read_sql(
            "SELECT id AS accident_id, st_case, year FROM accident", conn)

        if acc_df is None:
            raise ValueError(
                "acc_df (accident mapping) must be provided for vehicle data.")

        df = df.merge(acc_df, on=['st_case', 'year'], how='left')

        if df['accident_id'].isnull().any():
            raise ValueError(
                "Some vehicle records could not be matched to accidents.")

        conflict_keys = ['accident_id', 'vin']

    else:
        raise ValueError(f"Unknown table '{table}'")

    # Create temporary staging table with same structure as target table
    staging_table = f"{table}_staging"

    try:
        # Drop staging table if exists
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(staging_table)))

        # Create staging table like the target table (excluding constraints)
        cursor.execute(sql.SQL(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS EXCLUDING CONSTRAINTS)"
        ).format(
            sql.Identifier(staging_table),
            sql.Identifier(table)
        ))

        # Prepare buffer with only columns in the DataFrame
        columns = list(df.columns)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)

        # Use COPY with explicit column names
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL '\\N')").format(
            sql.Identifier(staging_table),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )

        cursor.copy_expert(copy_sql.as_string(conn), buffer)

        # Insert from staging table into main table with ON CONFLICT DO NOTHING
        insert_sql = sql.SQL("""
            INSERT INTO {table} ({fields})
            SELECT {fields} FROM {staging_table}
            ON CONFLICT ({conflict_keys}) DO NOTHING
        """).format(
            table=sql.Identifier(table),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            staging_table=sql.Identifier(staging_table),
            conflict_keys=sql.SQL(', ').join(
                map(sql.Identifier, conflict_keys))
        )

        cursor.execute(insert_sql)
        conn.commit()
        print(f"Saved/Wrote {len(df):,} records to the '{table}' table.")

    except Exception as e:
        conn.rollback()
        print(f"Error inserting data into {table}: {e}")
