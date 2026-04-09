from typing import Iterable

import pandas as pd

from db.init_db import map_dtype_to_sqlserver


def _column_defs(df: pd.DataFrame) -> str:
    return ",\n    ".join(
        f"[{col}] {map_dtype_to_sqlserver(col, df[col].dtype)}"
        for col in df.columns
    )


def _quoted_columns(columns: Iterable[str]) -> str:
    return ", ".join(f"[{col}]" for col in columns)


def create_stage_tables(conn, accident_df: pd.DataFrame, vehicle_df: pd.DataFrame):
    cursor = conn.cursor()

    cursor.execute(
        f"""
        IF OBJECT_ID('dbo.accident_stage', 'U') IS NULL
        CREATE TABLE dbo.accident_stage (
            {_column_defs(accident_df)}
        )
        """
    )

    cursor.execute(
        f"""
        IF OBJECT_ID('dbo.vehicle_stage', 'U') IS NULL
        CREATE TABLE dbo.vehicle_stage (
            {_column_defs(vehicle_df)}
        )
        """
    )

    conn.commit()
    print("ADF staging tables verified: dbo.accident_stage, dbo.vehicle_stage")


def create_merge_procedures(
    conn,
    accident_df: pd.DataFrame,
    vehicle_df: pd.DataFrame,
):
    cursor = conn.cursor()

    accident_columns = list(accident_df.columns)
    accident_insert_columns = _quoted_columns(accident_columns)
    accident_source_columns = ", ".join(
        f"source.[{col}]" for col in accident_columns
    )

    vehicle_stage_columns = list(vehicle_df.columns)
    vehicle_target_columns = ["accident_id"] + vehicle_stage_columns
    vehicle_insert_columns = _quoted_columns(vehicle_target_columns)
    vehicle_source_columns = ", ".join(
        ["src.[accident_id]"] + [f"src.[{col}]" for col in vehicle_stage_columns]
    )
    vehicle_match_columns = (
        ["accident_id", "veh_no"]
        if "veh_no" in vehicle_stage_columns
        else ["accident_id", "vin"]
    )
    vehicle_exists_clause = " AND ".join(
        [f"target.[{col}] = src.[{col}]" for col in vehicle_match_columns]
    )

    cursor.execute(
        """
        CREATE OR ALTER PROCEDURE dbo.usp_prepare_adf_stage_tables
        AS
        BEGIN
            SET NOCOUNT ON;

            TRUNCATE TABLE dbo.accident_stage;
            TRUNCATE TABLE dbo.vehicle_stage;
        END
        """
    )

    cursor.execute(
        f"""
        CREATE OR ALTER PROCEDURE dbo.usp_merge_accident_stage
        AS
        BEGIN
            SET NOCOUNT ON;

            INSERT INTO dbo.accident ({accident_insert_columns})
            SELECT {accident_source_columns}
            FROM dbo.accident_stage AS source
            WHERE NOT EXISTS (
                SELECT 1
                FROM dbo.accident AS target
                WHERE target.[st_case] = source.[st_case]
                  AND target.[year] = source.[year]
            );

            TRUNCATE TABLE dbo.accident_stage;
        END
        """
    )

    cursor.execute(
        f"""
        CREATE OR ALTER PROCEDURE dbo.usp_merge_vehicle_stage
        AS
        BEGIN
            SET NOCOUNT ON;

            WITH source_rows AS (
                SELECT
                    accident.[id] AS accident_id,
                    {", ".join(f"stage.[{col}]" for col in vehicle_stage_columns)}
                FROM dbo.vehicle_stage AS stage
                INNER JOIN dbo.accident AS accident
                    ON accident.[st_case] = stage.[st_case]
                   AND accident.[year] = stage.[year]
            )
            INSERT INTO dbo.vehicle ({vehicle_insert_columns})
            SELECT {vehicle_source_columns}
            FROM source_rows AS src
            WHERE NOT EXISTS (
                SELECT 1
                FROM dbo.vehicle AS target
                WHERE {vehicle_exists_clause}
            );

            TRUNCATE TABLE dbo.vehicle_stage;
        END
        """
    )

    conn.commit()
    print("ADF merge procedures verified.")
