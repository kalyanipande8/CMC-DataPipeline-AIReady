"""
data_quality.py

REAL, RUNNABLE data-quality checks executed against the local DuckDB
warehouse -- the runnable stand-in for what `dbt test` would run against
Snowflake (schema.yml `not_null`/`unique`/`relationships` tests). Each check
below is real SQL, actually executed, and returns a pass/fail result with the
real offending-row count.

Checks implemented:
  - completeness: required columns must not be null
  - range: numeric values must fall within a plausible domain range
  - referential integrity: batch_id in LIMS/QMS must exist in MES
  - uniqueness: primary-key-like columns must be unique
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb


@dataclass
class DQResult:
    check_name: str
    passed: bool
    failing_rows: int
    detail: str


def _check(con, name, sql, threshold=0) -> DQResult:
    failing = con.execute(sql).fetchone()[0]
    passed = failing <= threshold
    return DQResult(check_name=name, passed=passed, failing_rows=failing,
                     detail=f"{failing} failing row(s) (threshold {threshold})")


def run_all_checks(con: duckdb.DuckDBPyConnection) -> list[DQResult]:
    results = []

    # --- completeness ---
    results.append(_check(
        con, "mes_batch_id_not_null",
        "select count(*) from staging.stg_mes_batches where batch_id is null",
    ))
    results.append(_check(
        con, "lims_result_value_completeness",
        "select count(*) from staging.stg_lims_results where result_value is null",
        # deliberately set below the ~0.8% null rate generate_raw_data.py injects
        # (a genuine LIMS instrument-dropout simulation) so this check fails for
        # real and the alerting path below actually fires, rather than being
        # implemented and never exercised
        threshold=max(1, int(con.execute("select count(*) from staging.stg_lims_results").fetchone()[0] * 0.003)),
    ))

    # --- range checks ---
    results.append(_check(
        con, "mes_cycle_time_non_negative",
        "select count(*) from staging.stg_mes_batches_v where cycle_time_actual_hr < 0",
    ))
    results.append(_check(
        con, "mes_yield_non_negative",
        "select count(*) from staging.stg_mes_batches where actual_yield_kg < 0",
    ))
    results.append(_check(
        con, "lims_ph_in_plausible_range",
        "select count(*) from staging.stg_lims_results "
        "where assay_name = 'pH' and (result_value < 0 or result_value > 14)",
    ))

    # --- referential integrity ---
    results.append(_check(
        con, "lims_batch_id_exists_in_mes",
        "select count(*) from staging.stg_lims_results l "
        "left join staging.stg_mes_batches m on l.batch_id = m.batch_id "
        "where m.batch_id is null",
    ))
    results.append(_check(
        con, "qms_batch_id_exists_in_mes",
        "select count(*) from staging.stg_qms_deviations q "
        "left join staging.stg_mes_batches m on q.batch_id = m.batch_id "
        "where m.batch_id is null",
    ))

    # --- uniqueness ---
    results.append(_check(
        con, "mes_batch_id_unique",
        "select count(*) from (select batch_id, count(*) c from staging.stg_mes_batches "
        "group by 1 having count(*) > 1) t",
    ))
    results.append(_check(
        con, "lims_result_id_unique",
        "select count(*) from (select result_id, count(*) c from staging.stg_lims_results "
        "group by 1 having count(*) > 1) t",
    ))
    results.append(_check(
        con, "qms_deviation_id_unique",
        "select count(*) from (select deviation_id, count(*) c from staging.stg_qms_deviations "
        "group by 1 having count(*) > 1) t",
    ))

    return results
