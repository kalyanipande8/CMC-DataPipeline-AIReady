"""
Real tests against the real local pipeline. No mocking of Airflow/dbt/Snowflake
is needed because those are not invoked by the code under test -- the local
pipeline (generate_raw_data -> etl_spark_equivalent -> warehouse -> dbt
equivalent -> data_quality -> sla_monitor) is what actually runs.
"""
import duckdb
import pandas as pd
import pytest

from local_pipeline import data_quality, etl_spark_equivalent, generate_raw_data
from local_pipeline import run_dbt_equivalent as dbt_equiv
from local_pipeline import warehouse


@pytest.fixture(scope="module")
def raw_data():
    return generate_raw_data.main()


@pytest.fixture(scope="module")
def cleaned(raw_data):
    mes_raw, lims_raw, qms_raw = raw_data
    return etl_spark_equivalent.run_etl(mes_raw, lims_raw, qms_raw)


@pytest.fixture(scope="module")
def warehouse_con(cleaned):
    mes, lims, qms = cleaned
    con = duckdb.connect(":memory:")
    warehouse.load_staging_tables(con, mes, lims, qms)
    dbt_equiv.run_dbt_equivalent(con)
    yield con
    con.close()


def test_raw_data_has_three_sites(raw_data):
    mes_raw, lims_raw, qms_raw = raw_data
    assert set(mes_raw["site_id"].unique()) == {"SITE_US_RTP", "SITE_IE_CORK", "SITE_SG_TUAS"}
    assert len(mes_raw) > 300
    assert len(lims_raw) > 300


def test_raw_schemas_are_genuinely_different(raw_data):
    mes_raw, lims_raw, qms_raw = raw_data
    mes_cols = set(mes_raw.columns)
    lims_cols = set(lims_raw.columns)
    qms_cols = set(qms_raw.columns)
    # each source has fields the others don't -- genuinely different schemas
    assert "actual_yield_kg" in mes_cols and "actual_yield_kg" not in lims_cols
    assert "assay_name" in lims_cols and "assay_name" not in mes_cols
    assert "capa_required" in qms_cols and "capa_required" not in mes_cols
    # but all three carry batch_id, the join key
    assert "batch_id" in mes_cols and "batch_id" in lims_cols and "batch_id" in qms_cols


def test_etl_removes_negative_cycle_times(cleaned):
    mes, lims, qms = cleaned
    assert (mes["cycle_time_actual_hr"].dropna() >= 0).all()


def test_etl_derives_oos_flag(cleaned):
    mes, lims, qms = cleaned
    assert "is_oos" in lims.columns
    assert lims["is_oos"].dropna().isin([True, False]).all()


def test_warehouse_tables_exist(warehouse_con):
    tables = warehouse_con.execute(
        "select table_schema, table_name from information_schema.tables"
    ).fetchdf()
    names = set(tables["table_name"])
    assert "stg_mes_batches" in names
    assert "mart_site_quality_scorecard" in names


def test_all_six_marts_populated(warehouse_con):
    for mart in [
        "mart_mes_site_daily_metrics",
        "mart_mes_product_metrics",
        "mart_lims_assay_quality_metrics",
        "mart_qms_deviation_metrics",
        "mart_cross_system_batch_quality",
        "mart_site_quality_scorecard",
    ]:
        count = warehouse_con.execute(f"select count(*) from marts.{mart}").fetchone()[0]
        assert count > 0, f"{mart} produced no rows"


def test_total_metrics_defined_is_at_least_50():
    assert sum(dbt_equiv.METRIC_COUNTS.values()) >= 50


def test_right_first_time_rate_is_a_valid_proportion(warehouse_con):
    rows = warehouse_con.execute(
        "select right_first_time_rate from marts.mart_site_quality_scorecard"
    ).fetchall()
    assert len(rows) == 3  # one per site
    for (rate,) in rows:
        assert 0.0 <= rate <= 1.0


def test_data_quality_checks_run_and_return_results(warehouse_con):
    results = data_quality.run_all_checks(warehouse_con)
    assert len(results) >= 8
    # referential integrity between LIMS/QMS and MES must hold since QMS/LIMS
    # batch_ids are generated only from existing MES batches
    ref_checks = [r for r in results if "exists_in_mes" in r.check_name]
    assert all(r.passed for r in ref_checks)


def test_uniqueness_checks_pass(warehouse_con):
    results = data_quality.run_all_checks(warehouse_con)
    uniq_checks = [r for r in results if r.check_name.endswith("_unique")]
    assert len(uniq_checks) == 3
    assert all(r.passed for r in uniq_checks)
