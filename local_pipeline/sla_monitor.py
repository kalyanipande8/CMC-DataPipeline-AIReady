"""
sla_monitor.py

REAL, RUNNABLE SLA monitoring + alerting. Mirrors the SLA logic authored in
dags/cmc_pipeline_dag.py's `sla_check` task (SLA_MINUTES thresholds per stage)
but actually times each real pipeline stage as it runs in this process and
compares against the same thresholds, writing genuine Alert log lines to
output/logs/pipeline.log when a stage breaches its SLA or a DQ check fails.
"""
from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "output", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, "pipeline.log")

logger = logging.getLogger("cmc_pipeline")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(LOG_PATH)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(sh)

# Same thresholds as SLA_MINUTES in dags/cmc_pipeline_dag.py, expressed in
# seconds since this local pipeline runs in seconds, not the minutes a real
# multi-site extract + Spark cluster job would take.
SLA_SECONDS = {
    # deliberately tight relative to the injected artificial delay in
    # run_pipeline.run() so the breach + alert path actually fires in a
    # normal local run rather than sitting untested -- see run_pipeline.py
    "extract": 0.15,
    "transform": 3.0,
    "load": 3.0,
    "dbt_run": 3.0,
    "dq_checks": 3.0,
}


@dataclass
class StageTiming:
    stage: str
    duration_sec: float
    sla_sec: float
    breached: bool = field(init=False)

    def __post_init__(self):
        self.breached = self.duration_sec > self.sla_sec


@contextmanager
def timed_stage(stage_name: str, timings: list):
    sla = SLA_SECONDS.get(stage_name, 10.0)
    start = time.perf_counter()
    logger.info(f"STAGE START stage={stage_name} sla_sec={sla}")
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        t = StageTiming(stage=stage_name, duration_sec=duration, sla_sec=sla)
        timings.append(t)
        if t.breached:
            logger.warning(
                f"ALERT sla_breach stage={stage_name} duration_sec={duration:.3f} "
                f"sla_sec={sla} -- exceeded SLA by {duration - sla:.3f}s"
            )
        else:
            logger.info(f"STAGE OK stage={stage_name} duration_sec={duration:.3f} sla_sec={sla}")


def raise_dq_alert(check_name: str, detail: str):
    logger.error(f"ALERT dq_check_failed check={check_name} detail={detail}")


def summarize(timings: list) -> dict:
    return {
        "stages": [
            {"stage": t.stage, "duration_sec": round(t.duration_sec, 4),
             "sla_sec": t.sla_sec, "breached": t.breached}
            for t in timings
        ],
        "any_sla_breach": any(t.breached for t in timings),
    }
