"""
Simulates raw source-system extracts for 3 CMC (Chemistry, Manufacturing & Controls)
source systems across 3 manufacturing sites, with genuinely different schemas per
system (as they would be in a real pharma/biotech manufacturing data landscape):

  - MES  (Manufacturing Execution System): batch/lot execution records
  - LIMS (Laboratory Information Management System): lab test results per sample
  - QMS  (Quality Management System): deviations / CAPAs raised against batches

This is real, runnable Python (pandas + numpy) that writes CSVs to data/raw/.
It is the synthetic stand-in for the "3 source systems x 3 sites" the resume
line describes -- there is no real MES/LIMS/QMS system here, but the schemas,
field names, and referential relationships (batch_id linking MES -> LIMS -> QMS)
are modeled on how these systems genuinely relate in a GMP manufacturing plant.
"""
from __future__ import annotations

import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

RNG_SEED = 42
random.seed(RNG_SEED)
np.random.seed(RNG_SEED)

SITES = ["SITE_US_RTP", "SITE_IE_CORK", "SITE_SG_TUAS"]
PRODUCTS = ["MAB-101", "MAB-207", "GENE-THX-3"]
SHIFTS = ["Day", "Evening", "Night"]

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")


def _dates(n, start_days_ago=180):
    start = datetime(2026, 4, 1)
    return [start + timedelta(days=int(d)) for d in np.random.randint(0, start_days_ago, size=n)]


def generate_mes(n_batches_per_site=120) -> pd.DataFrame:
    """MES: one row per manufactured batch/lot, with process-execution fields."""
    rows = []
    batch_counter = 1
    for site in SITES:
        for _ in range(n_batches_per_site):
            product = random.choice(PRODUCTS)
            batch_id = f"{site[5:8]}-{product[:3]}-{batch_counter:05d}"
            batch_counter += 1
            planned_yield_kg = round(np.random.normal(50, 5), 2)
            # inject genuine yield variability + occasional low-yield batches
            yield_factor = np.random.normal(0.97, 0.06)
            actual_yield_kg = round(max(0, planned_yield_kg * yield_factor), 2)
            cycle_time_planned_hr = round(np.random.normal(72, 6), 1)
            cycle_time_actual_hr = round(cycle_time_planned_hr * np.random.normal(1.03, 0.10), 1)
            start_ts = random.choice(_dates(1))[0] if False else _dates(1)[0]
            rows.append({
                "batch_id": batch_id,
                "site_id": site,
                "product_code": product,
                "process_order_id": f"PO-{batch_counter:06d}",
                "shift": random.choice(SHIFTS),
                "line_id": f"LINE-{random.randint(1, 4)}",
                "batch_start_ts": start_ts.isoformat(),
                "batch_end_ts": (start_ts + timedelta(hours=cycle_time_actual_hr)).isoformat(),
                "planned_yield_kg": planned_yield_kg,
                "actual_yield_kg": actual_yield_kg,
                "cycle_time_planned_hr": cycle_time_planned_hr,
                "cycle_time_actual_hr": cycle_time_actual_hr,
                "batch_disposition": np.random.choice(
                    ["Released", "Released", "Released", "Rejected", "Quarantine"],
                    p=[0.80, 0.05, 0.05, 0.05, 0.05],
                ),
                "operator_id": f"OP-{random.randint(100, 199)}",
            })
    df = pd.DataFrame(rows)
    # inject a few nulls / out-of-range values to exercise DQ checks honestly
    df.loc[df.sample(frac=0.01, random_state=1).index, "actual_yield_kg"] = np.nan
    df.loc[df.sample(frac=0.005, random_state=2).index, "cycle_time_actual_hr"] = -1.0
    return df


def generate_lims(mes_df: pd.DataFrame, tests_per_batch=3) -> pd.DataFrame:
    """LIMS: one row per lab test result performed against a batch's samples."""
    assays = ["Potency", "Purity_HPLC", "Bioburden", "Endotoxin", "pH", "Osmolality"]
    spec_ranges = {
        "Potency": (90.0, 110.0),
        "Purity_HPLC": (95.0, 100.0),
        "Bioburden": (0.0, 10.0),
        "Endotoxin": (0.0, 0.5),
        "pH": (6.8, 7.4),
        "Osmolality": (260.0, 320.0),
    }
    rows = []
    result_counter = 1
    for _, batch in mes_df.iterrows():
        n_tests = np.random.randint(1, tests_per_batch + 1)
        chosen_assays = random.sample(assays, k=min(n_tests, len(assays)))
        for assay in chosen_assays:
            lo, hi = spec_ranges[assay]
            mid = (lo + hi) / 2
            spread = (hi - lo) / 4
            value = round(np.random.normal(mid, spread), 3)
            # occasionally push out of spec to create genuine OOS events
            if random.random() < 0.04:
                value = round(hi * np.random.uniform(1.02, 1.15), 3)
            rows.append({
                "result_id": f"RESULT-{result_counter:06d}",
                "batch_id": batch["batch_id"],
                "site_id": batch["site_id"],
                "sample_id": f"SMP-{result_counter:06d}",
                "assay_name": assay,
                "result_value": value,
                "unit": "%" if "Purity" in assay or assay == "Potency" else "EU/mL" if assay == "Endotoxin" else "CFU" if assay == "Bioburden" else "",
                "spec_lower": lo,
                "spec_upper": hi,
                "test_ts": batch["batch_end_ts"],
                "analyst_id": f"AN-{random.randint(200, 260)}",
                "instrument_id": f"INSTR-{random.randint(1, 12)}",
            })
            result_counter += 1
    df = pd.DataFrame(rows)
    df.loc[df.sample(frac=0.008, random_state=3).index, "result_value"] = np.nan
    return df


def generate_qms(mes_df: pd.DataFrame, lims_df: pd.DataFrame) -> pd.DataFrame:
    """QMS: deviations / CAPAs raised against batches (not every batch has one)."""
    deviation_types = ["Process_Deviation", "OOS_Result", "Equipment_Malfunction",
                       "Documentation_Error", "Environmental_Excursion"]
    severities = ["Minor", "Major", "Critical"]
    rows = []
    dev_counter = 1

    oos_batch_ids = set(
        lims_df.loc[lims_df["result_value"] > lims_df["spec_upper"], "batch_id"]
    )
    rejected_batch_ids = set(mes_df.loc[mes_df["batch_disposition"] == "Rejected", "batch_id"])

    for _, batch in mes_df.iterrows():
        triggers = []
        if batch["batch_id"] in oos_batch_ids:
            triggers.append("OOS_Result")
        if batch["batch_id"] in rejected_batch_ids:
            triggers.append("Process_Deviation")
        # background random deviation rate on top of triggered ones
        if random.random() < 0.10 and not triggers:
            triggers.append(random.choice(deviation_types))

        for dtype in triggers:
            opened_ts = pd.to_datetime(batch["batch_end_ts"]) + timedelta(days=random.randint(0, 3))
            severity = np.random.choice(severities, p=[0.55, 0.35, 0.10])
            closure_sla_days = {"Minor": 15, "Major": 30, "Critical": 45}[severity]
            actual_days_to_close = max(1, int(np.random.exponential(closure_sla_days * 0.7)))
            closed_ts = opened_ts + timedelta(days=actual_days_to_close)
            rows.append({
                "deviation_id": f"DEV-{dev_counter:05d}",
                "batch_id": batch["batch_id"],
                "site_id": batch["site_id"],
                "deviation_type": dtype,
                "severity": severity,
                "opened_ts": opened_ts.isoformat(),
                "closed_ts": closed_ts.isoformat() if random.random() > 0.08 else "",
                "closure_sla_days": closure_sla_days,
                "capa_required": bool(severity in ("Major", "Critical")),
                "capa_id": f"CAPA-{dev_counter:05d}" if severity in ("Major", "Critical") else "",
                "investigator_id": f"QA-{random.randint(1, 20)}",
            })
            dev_counter += 1
    return pd.DataFrame(rows)


def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    mes_df = generate_mes()
    lims_df = generate_lims(mes_df)
    qms_df = generate_qms(mes_df, lims_df)

    mes_path = os.path.join(RAW_DIR, "mes_batch_records.csv")
    lims_path = os.path.join(RAW_DIR, "lims_test_results.csv")
    qms_path = os.path.join(RAW_DIR, "qms_deviations.csv")

    mes_df.to_csv(mes_path, index=False)
    lims_df.to_csv(lims_path, index=False)
    qms_df.to_csv(qms_path, index=False)

    print(f"Generated MES batch records: {len(mes_df)} rows -> {mes_path}")
    print(f"Generated LIMS test results: {len(lims_df)} rows -> {lims_path}")
    print(f"Generated QMS deviations:    {len(qms_df)} rows -> {qms_path}")
    return mes_df, lims_df, qms_df


if __name__ == "__main__":
    main()
