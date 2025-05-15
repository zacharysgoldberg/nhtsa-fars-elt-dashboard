import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import pandas as pd
from typing import Literal
from config.config import DB_CONFIG


def save_to_db(df: pd.DataFrame, table: str):
    if df.empty:
        print(f"No data to insert into '{table}' table.")
        return

    df = df.where(pd.notnull(df), None)  # Replace NaN with None

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Add conflict resolution
    if table == 'accident':
        id1 = sql.SQL("st_case")
        id2 = sql.SQL("year")

    elif table == 'vehicle':
        if 'st_case' not in df.columns:
            raise ValueError("Vehicle data must include 'st_case' column.")

        # Load accident mapping
        acc_df = pd.read_sql(
            "SELECT id AS accident_id, st_case, year FROM accident", conn)
        df = df.merge(acc_df, on=['st_case', 'year'], how='left')

        if df['accident_id'].isnull().any():
            raise ValueError(
                "Some vehicle records could not be matched to accidents.")

        id1 = sql.SQL("accident_id")
        id2 = sql.SQL("vin")

    columns = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False)]

    # Construct the SQL safely
    insert_sql = sql.SQL(
        "INSERT INTO {table} ({fields}) VALUES ({placeholders}) ON CONFLICT ({id1}, {id2}) DO NOTHING")\
        .format(
        table=sql.Identifier(table),
        fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns)),
        id1=id1,
        id2=id2
    )

    try:
        print("Executing query:", insert_sql.as_string(conn))
        print("\nStarting bulk insert...\n")
        execute_batch(cursor, insert_sql, values, page_size=1000)
        conn.commit()
        print("Insert completed.\n")
    except Exception as e:
        print(f"Error inserting data into {table}: {e}\n")
        conn.rollback()
    finally:
        conn.close()

    print(f"\nSaved {len(df):,} records to the '{table}' table.\n")
