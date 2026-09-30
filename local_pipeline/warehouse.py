"""
warehouse.py

REAL, RUNNABLE. Local stand-in for Snowflake: loads the cleaned MES/LIMS/QMS
DataFrames into a real DuckDB database file (data/warehouse.duckdb), i.e. what
the DAG's `load_to_snowflake` task (COPY INTO) would do against a real
Snowflake account. DuckDB is used here as a genuine embedded SQL warehouse --
tables really exist on disk and are really queried with SQL, just not
Snowflake specifically.
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd

WAREHOUSE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "warehouse.duckdb")


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(WAREHOUSE_PATH), exist_ok=True)
    return duckdb.connect(WAREHOUSE_PATH, read_only=read_only)


def load_staging_tables(con: duckdb.DuckDBPyConnection, mes: pd.DataFrame,
                         lims: pd.DataFrame, qms: pd.DataFrame) -> None:
    """Equivalent of `COPY INTO CMC_RAW.STAGING.*` -- (re)creates staging tables
    from the cleaned DataFrames the ETL step produced.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.register("mes_df", mes)
    con.register("lims_df", lims)
    con.register("qms_df", qms)
    con.execute("CREATE OR REPLACE TABLE staging.stg_mes_batches AS SELECT * FROM mes_df")
    con.execute("CREATE OR REPLACE TABLE staging.stg_lims_results AS SELECT * FROM lims_df")
    con.execute("CREATE OR REPLACE TABLE staging.stg_qms_deviations AS SELECT * FROM qms_df")
    con.unregister("mes_df")
    con.unregister("lims_df")
    con.unregister("qms_df")
