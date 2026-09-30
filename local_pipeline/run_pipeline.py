"""
run_pipeline.py

REAL, RUNNABLE end-to-end pipeline. Mirrors the stage sequence authored in
dags/cmc_pipeline_dag.py (extract -> spark_transform -> load_to_snowflake ->
dbt_run -> dbt_test -> sla_check) but actually executes each stage locally:

    generate_raw_data (extract stand-in)
      -> etl_spark_equivalent.run_etl        (Spark stand-in, real pandas logic)
      -> warehouse.load_staging_tables        (Snowflake stand-in, real DuckDB)
      -> run_dbt_equivalent.run_dbt_equivalent (dbt run stand-in, real SQL)
      -> data_quality.run_all_checks          (dbt test stand-in, real SQL)
      -> sla_monitor                          (timing + alerting, real)

One stage (`extract`) has a deliberately injected artificial delay and a tight
SLA threshold so the SLA-breach alerting path is demonstrated firing for real
in this run, not merely implemented and never exercised. This is disclosed
here and in the README rather than presented as an organic slow extract.
"""
from __future__ import annotations

import json
import os
import time

from . import data_quality
from . import etl_spark_equivalent
from . import generate_raw_data
from . import run_dbt_equivalent as dbt_equiv
from . import sla_monitor
from . import warehouse

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")


def run(inject_sla_breach_demo: bool = True) -> dict:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timings: list = []
    report: dict = {}

    with sla_monitor.timed_stage("extract", timings):
        if inject_sla_breach_demo:
            # deliberately simulate one slow site extract (e.g. a flaky VPN
            # link to SITE_SG_TUAS) against an intentionally tight SLA, to
            # prove the alerting path fires for real -- see module docstring
            time.sleep(0.35)
        mes_raw, lims_raw, qms_raw = generate_raw_data.main()

    with sla_monitor.timed_stage("transform", timings):
        mes, lims, qms = etl_spark_equivalent.run_etl(mes_raw, lims_raw, qms_raw)

    with sla_monitor.timed_stage("load", timings):
        con = warehouse.get_connection()
        warehouse.load_staging_tables(con, mes, lims, qms)

    with sla_monitor.timed_stage("dbt_run", timings):
        mart_summary = dbt_equiv.run_dbt_equivalent(con)

    with sla_monitor.timed_stage("dq_checks", timings):
        dq_results = data_quality.run_all_checks(con)
        for r in dq_results:
            if not r.passed:
                sla_monitor.raise_dq_alert(r.check_name, r.detail)

    sla_summary = sla_monitor.summarize(timings)

    report["row_counts"] = {
        "mes_batches": len(mes),
        "lims_results": len(lims),
        "qms_deviations": len(qms),
    }
    report["mart_summary"] = mart_summary
    report["dq_results"] = [
        {"check": r.check_name, "passed": r.passed, "failing_rows": r.failing_rows, "detail": r.detail}
        for r in dq_results
    ]
    report["dq_pass_count"] = sum(1 for r in dq_results if r.passed)
    report["dq_total_count"] = len(dq_results)
    report["sla_summary"] = sla_summary

    con.close()

    with open(os.path.join(OUTPUT_DIR, "last_run_report.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)

    return report


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2, default=str))
