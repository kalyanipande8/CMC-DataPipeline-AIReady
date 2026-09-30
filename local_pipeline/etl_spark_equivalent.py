"""
etl_spark_equivalent.py

REAL, RUNNABLE. This is the pandas stand-in for the PySpark ETL job the DAG's
`spark_transform` task would submit (`/opt/spark_jobs/cmc_etl_job.py`). There is
no real Spark cluster here, so this module performs the same transformation
LOGIC (clean nulls/invalid values, type-conform columns, derive flags) using
pandas DataFrames instead of Spark DataFrames. The operations (filter, groupby,
join, withColumn-style derived columns) are a 1:1 conceptual match for what the
PySpark job would do with `df.filter()/.groupBy()/.join()/.withColumn()`.
"""
from __future__ import annotations

import pandas as pd


def clean_mes(mes_df: pd.DataFrame) -> pd.DataFrame:
    """Mirrors dbt_models/staging/stg_mes_batches.sql."""
    df = mes_df.copy()
    df["batch_start_ts"] = pd.to_datetime(df["batch_start_ts"])
    df["batch_end_ts"] = pd.to_datetime(df["batch_end_ts"])
    # negative cycle times are a known MES extract defect -- null them out,
    # same rule as the dbt staging model
    df.loc[df["cycle_time_actual_hr"] < 0, "cycle_time_actual_hr"] = pd.NA
    df["cycle_time_actual_hr"] = pd.to_numeric(df["cycle_time_actual_hr"])
    df["batch_date"] = df["batch_start_ts"].dt.floor("D")
    return df


def clean_lims(lims_df: pd.DataFrame) -> pd.DataFrame:
    """Mirrors dbt_models/staging/stg_lims_results.sql."""
    df = lims_df.copy()
    df["test_ts"] = pd.to_datetime(df["test_ts"])

    def _oos(row):
        if pd.isna(row["result_value"]):
            return pd.NA
        return bool(row["result_value"] < row["spec_lower"] or row["result_value"] > row["spec_upper"])

    df["is_oos"] = df.apply(_oos, axis=1)
    return df


def clean_qms(qms_df: pd.DataFrame) -> pd.DataFrame:
    """Mirrors dbt_models/staging/stg_qms_deviations.sql."""
    df = qms_df.copy()
    df["opened_ts"] = pd.to_datetime(df["opened_ts"])
    df["closed_ts"] = pd.to_datetime(df["closed_ts"].replace("", pd.NA))
    df["is_open"] = df["closed_ts"].isna()
    now = pd.Timestamp.now()
    df["age_days"] = (df["closed_ts"].fillna(now) - df["opened_ts"]).dt.days
    return df


def run_etl(mes_raw: pd.DataFrame, lims_raw: pd.DataFrame, qms_raw: pd.DataFrame):
    """Full conform+clean pass across all three sources, i.e. the equivalent of
    the Spark job's main() before it writes staged output for the load step.
    """
    mes = clean_mes(mes_raw)
    lims = clean_lims(lims_raw)
    qms = clean_qms(qms_raw)
    return mes, lims, qms
