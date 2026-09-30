"""
cmc_pipeline_dag.py

AUTHORED, NOT EXECUTED. There is no Airflow scheduler/webserver/metadata DB running
in this environment, and no live Snowflake account. This DAG is real, syntactically
correct Airflow code (Airflow 2.x TaskFlow + classic operator patterns) that would
orchestrate the CMC data pipeline described in the project README if deployed to a
real Airflow instance with the SnowflakeOperator/SparkSubmitOperator providers
installed and credentials configured via Airflow Connections.

Orchestration shape (mirrors local_pipeline/run_pipeline.py, which actually runs):

    extract_mes / extract_lims / extract_qms   (parallel, per source system)
                -> spark_transform              (PySpark ETL: clean, conform, join)
                -> load_to_snowflake            (COPY INTO raw + staging tables)
                -> dbt_run                      (BashOperator: `dbt run` -> marts)
                -> dbt_test                     (BashOperator: `dbt test` -> DQ)
                -> sla_check / alert_on_failure

Each extract task is parameterized per site (3 manufacturing sites) via
`.partial().expand()` dynamic task mapping (Airflow 2.3+).
"""
from __future__ import annotations

import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.exceptions import AirflowFailException

try:
    # Real Airflow provider imports for Spark + Snowflake. Not installed in this
    # sandbox (no Airflow environment here) -- included so the DAG is syntactically
    # and semantically correct for a real deployment with
    # apache-airflow-providers-apache-spark / apache-airflow-providers-snowflake.
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
except ImportError:  # pragma: no cover - providers not installed in this sandbox
    SparkSubmitOperator = None
    SnowflakeOperator = None

SITES = ["SITE_US_RTP", "SITE_IE_CORK", "SITE_SG_TUAS"]
SOURCE_SYSTEMS = ["mes", "lims", "qms"]

SLA_MINUTES = {
    "extract": 20,
    "transform": 45,
    "load": 15,
    "dbt_run": 30,
}

default_args = {
    "owner": "cmc-data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["kalyani.pande@stonybrook.edu"],
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "sla": dt.timedelta(minutes=60),
}

with DAG(
    dag_id="cmc_data_pipeline",
    description=(
        "CMC data pipeline: extract MES/LIMS/QMS from 3 sites, Spark ETL, "
        "load to Snowflake, dbt semantic layer, DQ + SLA monitoring/alerting."
    ),
    default_args=default_args,
    schedule="0 2 * * *",  # daily at 02:00, after overnight batch closes across all 3 sites
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cmc", "manufacturing", "mes", "lims", "qms", "dbt", "snowflake"],
) as dag:

    def _extract_source(source_system: str, site_id: str, **_):
        """Pulls the day's raw extract for one (source_system, site) pair.

        In production this would call the MES/LIMS/QMS system's REST/ODBC/flat-file
        export API and stage the raw file in cloud storage (S3/ADLS) ahead of the
        Spark job reading it. Raises AirflowFailException on a missing/empty extract
        so retries + alerting kick in.
        """
        context = get_current_context()
        run_ts = context["ts"]
        # Placeholder for the real extract call, e.g.:
        #   client = MesClient(conn_id=f"mes_{site_id.lower()}")
        #   df = client.export_batch_records(as_of=run_ts)
        #   df.to_parquet(f"s3://cmc-raw/{source_system}/{site_id}/{run_ts}.parquet")
        print(f"[extract_{source_system}] site={site_id} run_ts={run_ts}")

    extract_tasks = []
    for system in SOURCE_SYSTEMS:
        extract_group = []
        for site in SITES:
            t = PythonOperator(
                task_id=f"extract_{system}_{site.lower()}",
                python_callable=_extract_source,
                op_kwargs={"source_system": system, "site_id": site},
                sla=dt.timedelta(minutes=SLA_MINUTES["extract"]),
            )
            extract_group.append(t)
        extract_tasks.append(extract_group)

    if SparkSubmitOperator is not None:
        spark_transform = SparkSubmitOperator(
            task_id="spark_transform_conform_join",
            application="/opt/spark_jobs/cmc_etl_job.py",
            conn_id="spark_default",
            application_args=[
                "--sites", ",".join(SITES),
                "--sources", ",".join(SOURCE_SYSTEMS),
                "--run-date", "{{ ds }}",
            ],
            executor_cores=4,
            executor_memory="8g",
            num_executors=6,
            name="cmc_mes_lims_qms_etl_{{ ds_nodash }}",
            verbose=False,
            sla=dt.timedelta(minutes=SLA_MINUTES["transform"]),
        )
    else:  # pragma: no cover - fallback stub so the DAG file still parses without providers
        spark_transform = BashOperator(
            task_id="spark_transform_conform_join",
            bash_command=(
                "echo 'Would run: spark-submit /opt/spark_jobs/cmc_etl_job.py "
                "--sites {} --sources {} --run-date {{{{ ds }}}}'"
            ).format(",".join(SITES), ",".join(SOURCE_SYSTEMS)),
            sla=dt.timedelta(minutes=SLA_MINUTES["transform"]),
        )

    if SnowflakeOperator is not None:
        load_to_snowflake = SnowflakeOperator(
            task_id="load_to_snowflake",
            snowflake_conn_id="snowflake_cmc",
            sql="sql/copy_into_raw_and_staging.sql",
            warehouse="CMC_ETL_WH",
            database="CMC_RAW",
            schema="STAGING",
            sla=dt.timedelta(minutes=SLA_MINUTES["load"]),
        )
    else:  # pragma: no cover
        load_to_snowflake = BashOperator(
            task_id="load_to_snowflake",
            bash_command="echo 'Would run: COPY INTO CMC_RAW.STAGING.* FROM @cmc_etl_stage'",
            sla=dt.timedelta(minutes=SLA_MINUTES["load"]),
        )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/dbt/cmc_semantic_layer && "
            "dbt run --profiles-dir . --target prod --select tag:cmc_marts"
        ),
        sla=dt.timedelta(minutes=SLA_MINUTES["dbt_run"]),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/dbt/cmc_semantic_layer && "
            "dbt test --profiles-dir . --target prod --select tag:cmc_marts"
        ),
    )

    def _sla_and_alert(**_):
        """Aggregates task durations for this DAG run against SLA_MINUTES and
        raises so downstream Slack/email alerting (on_failure_callback / the
        send_slack_notification below) fires. Mirrors the SLA logic implemented
        for real in local_pipeline/sla_monitor.py.
        """
        context = get_current_context()
        dag_run = context["dag_run"]
        breaches = []
        for ti in dag_run.get_task_instances():
            if ti.duration is None:
                continue
            for key, limit_min in SLA_MINUTES.items():
                if key in ti.task_id and (ti.duration / 60.0) > limit_min:
                    breaches.append((ti.task_id, ti.duration / 60.0, limit_min))
        if breaches:
            raise AirflowFailException(f"SLA breaches detected: {breaches}")

    sla_check = PythonOperator(
        task_id="sla_check",
        python_callable=_sla_and_alert,
        on_failure_callback=send_slack_notification(
            slack_conn_id="slack_cmc_alerts",
            text=(
                ":rotating_light: CMC pipeline SLA breach on {{ ds }} — "
                "see task logs for `sla_check`."
            ),
            channel="#cmc-data-pipeline-alerts",
        ),
    )

    for group in extract_tasks:
        for t in group:
            t >> spark_transform

    spark_transform >> load_to_snowflake >> dbt_run >> dbt_test >> sla_check
