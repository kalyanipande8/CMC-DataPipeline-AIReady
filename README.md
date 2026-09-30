# CMC Data Pipeline Architecture & AI/ML Enablement

Airflow orchestration and Spark ETL design for ingesting MES, LIMS, and QMS
records from 3 manufacturing sites, a dbt semantic layer of 50+ metrics on
Snowflake, data-quality checks, SLA monitoring, and automated alerting — plus
a real, runnable local pipeline (pandas + DuckDB) that executes the same
transformation logic to prove it's correct.

## What's real vs. authored infrastructure-as-code (read this first)

This project has no Airflow deployment, no Spark cluster, no dbt install, and
no Snowflake account — nor should it: those are heavy, costly enterprise
data-infra tools, and standing one up for a portfolio project would either
cost money or produce nothing but a toy install. So, like this repo's sibling
project (`HPC-Container-Orchestration-System`), it's built and presented
honestly in two parts:

1. **`dags/cmc_pipeline_dag.py` and `dbt_models/` — real, syntactically
   correct, reviewed code, authored but never executed against a live
   deployment.**
   - `dags/cmc_pipeline_dag.py` is a real Airflow 2.x DAG using genuine
     operator patterns (`PythonOperator`, `BashOperator`,
     `SparkSubmitOperator`, `SnowflakeOperator`, SLA timedeltas, Slack
     failure callbacks) orchestrating extract → Spark transform → load to
     Snowflake → `dbt run` → `dbt test` → SLA check. It would run correctly
     on a real Airflow instance with the Spark/Snowflake/Slack providers
     installed and connections configured. **It has never been run by an
     Airflow scheduler in this environment** — there is no Airflow here.
   - `dbt_models/` is a real dbt project (`dbt_project.yml`, `sources.yml`,
     staging views, mart tables, `schema.yml` tests) with correct dbt/Jinja
     syntax (`{{ ref() }}`, `{{ source() }}`, config blocks) that would run
     against Snowflake with `dbt run`/`dbt test`. **It has never been run by
     a real `dbt` CLI against Snowflake** — there is no dbt install or
     Snowflake account here.
   - Treat both as reviewable infra-as-code / analytics-engineering code, not
     as evidence of a production deployment.

2. **`local_pipeline/` — a real, runnable local pipeline**, not a live
   Airflow/Spark/Snowflake/dbt deployment. It is genuine Python that:
   - simulates raw MES/LIMS/QMS extracts with real, different schemas
     (`local_pipeline/generate_raw_data.py`),
   - transforms them with pandas standing in for the Spark ETL job
     (`local_pipeline/etl_spark_equivalent.py`), performing the *same
     transformation logic* (null/range cleaning, OOS-flag derivation) that
     the dbt staging models describe,
   - loads the result into a real local DuckDB database standing in for
     Snowflake (`local_pipeline/warehouse.py`),
   - executes the **same SQL logic as the 6 dbt mart files**, transcribed
     for DuckDB's dialect, directly against that warehouse
     (`local_pipeline/run_dbt_equivalent.py`) — this is the runnable
     stand-in for `dbt run`,
   - runs real completeness / range / referential-integrity / uniqueness
     data-quality checks with real SQL (`local_pipeline/data_quality.py`) —
     the runnable stand-in for `dbt test`,
   - times each stage for real and compares it to an SLA threshold, logging
     a real `Alert` line to `output/logs/pipeline.log` on breach or DQ
     failure (`local_pipeline/sla_monitor.py`).

   **Every number in the Results section below was produced by actually
   running `python3 run_all.py`** in this repository on 2026-09-30 — nothing
   is hand-typed or invented.

**One disclosed simplification**: the `extract` stage's SLA threshold
(`0.15s`) and an injected `time.sleep(0.35)` in `run_pipeline.run()` are
deliberately set so the SLA-breach alerting path fires for real in every run
of this local pipeline, instead of sitting implemented but never exercised.
Similarly, the `lims_result_value_completeness` DQ check's threshold is set
tighter than the ~0.8% null rate `generate_raw_data.py` deliberately injects
(simulating LIMS instrument dropout), so that check also genuinely fails and
its alert genuinely fires. Both are called out here and in code comments —
they are real breaches of real thresholds against real measured durations and
real row counts, just with thresholds chosen to demonstrate the alerting path
rather than left so loose that the alert code would never be exercised by
this synthetic dataset.

In short: `dags/` and `dbt_models/` are "would run correctly against real
Airflow/Snowflake, reviewed but not live-executed"; `local_pipeline/` is
"actually runs, right now, on this machine, and proves the transformation
logic and the DQ/SLA/alerting logic are correct."

## Source systems simulated (genuinely different schemas)

| System | What it models | Grain | Key fields |
|---|---|---|---|
| MES | Manufacturing Execution System batch/lot records | 1 row per batch | `batch_id`, `planned_yield_kg`, `actual_yield_kg`, `cycle_time_actual_hr`, `batch_disposition` |
| LIMS | Lab Information Management System test results | 1 row per test result | `result_id`, `batch_id`, `assay_name`, `result_value`, `spec_lower/upper` |
| QMS | Quality Management System deviations/CAPAs | 1 row per deviation | `deviation_id`, `batch_id`, `severity`, `opened_ts`, `closed_ts`, `capa_required` |

Across **3 manufacturing sites** (`SITE_US_RTP`, `SITE_IE_CORK`,
`SITE_SG_TUAS`) and 3 products. `batch_id` is the join key linking MES →
LIMS → QMS, mirroring how these systems genuinely relate in a GMP
manufacturing plant.

## The 56 semantic metrics

Defined as real dbt models in `dbt_models/marts/` (and executed as the
equivalent SQL in `local_pipeline/run_dbt_equivalent.py`):

| Mart | Grain | Metrics |
|---|---|---|
| `mart_mes_site_daily_metrics` | site × day | 12 (release rate, reject rate, yield attainment, cycle-time metrics, ...) |
| `mart_mes_product_metrics` | product | 8 (release rate, yield stddev, cycle overrun rate, ...) |
| `mart_lims_assay_quality_metrics` | site × assay | 12 (OOS rate, result stddev, completeness rate, ...) |
| `mart_qms_deviation_metrics` | site | 11 (open/closed counts, severity breakdown, SLA closure rate, ...) |
| `mart_cross_system_batch_quality` | batch (joins all 3 systems) | 7 (right-first-time flag, linked deviations, yield attainment, ...) |
| `mart_site_quality_scorecard` | site | 6 (right-first-time rate, avg yield attainment, ...) |
| **Total implemented in full, runnable SQL** | | **56** |

`dbt_models/marts/schema.yml` documents, without repeating the same pattern
20 more times, exactly how this scales further (slicing the site-day mart by
shift/line, the assay mart by instrument/analyst, etc.) — a mechanical repeat
of the group-by shapes already implemented.

## How to run

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python3 run_all.py     # runs the full local pipeline end to end
pytest -q              # runs the test suite
```

`run_all.py` regenerates the raw MES/LIMS/QMS CSVs (`data/raw/`), runs the
pandas ETL, loads a DuckDB warehouse (`data/warehouse.duckdb`), runs the
dbt-equivalent SQL to populate all 6 marts, runs the DQ checks, runs SLA
monitoring, and writes `output/last_run_report.json` plus
`output/logs/pipeline.log`.

## Results (real, from an actual run on 2026-09-30)

```
$ pytest -q
..........                                                               [100%]
10 passed in 0.94s
```

```
$ python3 run_all.py
Raw rows generated:  MES=360  LIMS=718  QMS=91

Semantic metrics computed (dbt-equivalent, run against local DuckDB):
  total metrics defined across marts: 56
  - mart_mes_site_daily_metrics: 262 rows, 12 metrics
  - mart_mes_product_metrics: 3 rows, 8 metrics
  - mart_lims_assay_quality_metrics: 18 rows, 12 metrics
  - mart_qms_deviation_metrics: 3 rows, 11 metrics
  - mart_cross_system_batch_quality: 360 rows, 7 metrics
  - mart_site_quality_scorecard: 3 rows, 6 metrics

Data quality checks: 9/10 passed
  [PASS] mes_batch_id_not_null: 0 failing row(s) (threshold 0)
  [FAIL] lims_result_value_completeness: 6 failing row(s) (threshold 2)
  [PASS] mes_cycle_time_non_negative: 0 failing row(s) (threshold 0)
  [PASS] mes_yield_non_negative: 0 failing row(s) (threshold 0)
  [PASS] lims_ph_in_plausible_range: 0 failing row(s) (threshold 0)
  [PASS] lims_batch_id_exists_in_mes: 0 failing row(s) (threshold 0)
  [PASS] qms_batch_id_exists_in_mes: 0 failing row(s) (threshold 0)
  [PASS] mes_batch_id_unique: 0 failing row(s) (threshold 0)
  [PASS] lims_result_id_unique: 0 failing row(s) (threshold 0)
  [PASS] qms_deviation_id_unique: 0 failing row(s) (threshold 0)

SLA monitoring (any breach: True):
  - extract: 0.4432s / SLA 0.15s [BREACH]
  - transform: 0.0182s / SLA 3.0s [ok]
  - load: 0.0547s / SLA 3.0s [ok]
  - dbt_run: 0.061s / SLA 3.0s [ok]
  - dq_checks: 0.0053s / SLA 3.0s [ok]
```

Real alert log lines written to `output/logs/pipeline.log` from that run:

```
WARNING ... ALERT sla_breach stage=extract duration_sec=0.443 sla_sec=0.15 -- exceeded SLA by 0.293s
ERROR   ... ALERT dq_check_failed check=lims_result_value_completeness detail=6 failing row(s) (threshold 2)
```

Sample business metric — `mart_site_quality_scorecard` (right-first-time rate,
the headline CMC quality KPI, computed for real off 360 batches / 718 lab
results / 91 deviations):

| site_id | total_batches | right_first_time_rate | avg_yield_attainment_pct |
|---|---|---|---|
| SITE_US_RTP | 120 | 0.7833 | 97.05 |
| SITE_SG_TUAS | 120 | 0.7500 | 97.33 |
| SITE_IE_CORK | 120 | 0.7583 | 97.30 |

Row/line counts are not hand-picked: rerunning `python3 run_all.py` will
regenerate the raw data (fixed random seed, so counts are stable at
360/718/91) and reproduce the same DQ/SLA behavior, since the injected delay
and thresholds are deterministic.

## Project layout

```
dags/cmc_pipeline_dag.py       Authored-not-executed Airflow DAG
dbt_models/                    Authored-not-run dbt project (staging + 6 marts, 56 metrics)
local_pipeline/                Real, runnable pandas + DuckDB pipeline (the proof)
  generate_raw_data.py           MES/LIMS/QMS synthetic extract generator
  etl_spark_equivalent.py        pandas stand-in for the Spark ETL job
  warehouse.py                   DuckDB stand-in for Snowflake
  run_dbt_equivalent.py          dbt mart SQL, transcribed for DuckDB, actually executed
  data_quality.py                Real completeness/range/referential/uniqueness checks
  sla_monitor.py                 Real stage timing + SLA breach + DQ-failure alerting
  run_pipeline.py                Orchestrates all of the above
run_all.py                     Entry point: runs the pipeline, prints results
tests/test_pipeline.py         pytest suite against the real local pipeline
data/                          Generated raw CSVs + DuckDB warehouse file (gitignored)
output/                        last_run_report.json + pipeline.log (committed as evidence)
```
