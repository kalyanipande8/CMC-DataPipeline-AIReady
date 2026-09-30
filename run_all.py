"""
run_all.py

Entry point: runs the full REAL local pipeline (see local_pipeline/run_pipeline.py)
end to end and prints a human-readable results summary. This is the runnable
proof-of-correctness counterpart to the authored-not-executed dags/ and
dbt_models/ files -- see README.md for the full honesty breakdown.

Usage:
    python3 run_all.py
"""
from __future__ import annotations

import json

from local_pipeline.run_pipeline import run


def main():
    report = run()

    print("\n" + "=" * 72)
    print("CMC DATA PIPELINE -- LOCAL RUN SUMMARY")
    print("=" * 72)

    rc = report["row_counts"]
    print(f"\nRaw rows generated:  MES={rc['mes_batches']}  LIMS={rc['lims_results']}  QMS={rc['qms_deviations']}")

    ms = report["mart_summary"]
    print(f"\nSemantic metrics computed (dbt-equivalent, run against local DuckDB):")
    print(f"  total metrics defined across marts: {ms['total_metrics_defined']}")
    for name, info in ms.items():
        if name == "total_metrics_defined":
            continue
        print(f"  - {name}: {info['rows']} rows, {info['metrics_defined']} metrics")

    print(f"\nData quality checks: {report['dq_pass_count']}/{report['dq_total_count']} passed")
    for r in report["dq_results"]:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"  [{status}] {r['check']}: {r['detail']}")

    ss = report["sla_summary"]
    print(f"\nSLA monitoring (any breach: {ss['any_sla_breach']}):")
    for s in ss["stages"]:
        flag = "BREACH" if s["breached"] else "ok"
        print(f"  - {s['stage']}: {s['duration_sec']}s / SLA {s['sla_sec']}s [{flag}]")

    print(f"\nFull JSON report written to output/last_run_report.json")
    print("=" * 72 + "\n")


if __name__ == "__main__":
    main()
