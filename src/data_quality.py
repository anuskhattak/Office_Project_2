"""
Stage 2 — Data Quality
Reads cleaned Parquet, runs all quality contracts, writes output/quality_report.json
and saves charts to output/charts/.
"""

import json
import logging
import os
from datetime import datetime, date

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── Config ──────────────────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR   = os.path.dirname(_SCRIPT_DIR)

CLEANED_DIR = os.getenv("CLEANED_DIR", os.path.join(_BASE_DIR, "output", "cleaned"))
REPORT_PATH = os.getenv("REPORT_PATH", os.path.join(_BASE_DIR, "output", "quality_report.json"))
CHARTS_DIR  = os.getenv("CHARTS_DIR",  os.path.join(_BASE_DIR, "output", "charts"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Spark ────────────────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("healthcare_stage2_quality")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )

# ── Check result helpers ─────────────────────────────────────────────────────────
def _mask(id_val) -> str:
    """First 4 chars only — agent never sees full patient/encounter IDs."""
    return str(id_val)[:4] + "***"

def _sample(df: DataFrame, id_col: str, limit: int = 5):
    return [
        _mask(row[id_col])
        for row in df.select(id_col).limit(limit).collect()
        if row[id_col] is not None
    ]

def _result(count: int, df_failed: DataFrame = None, id_col: str = None) -> dict:
    r = {"pass": count == 0, "count": int(count)}
    if count > 0 and df_failed is not None and id_col:
        r["sample_ids_masked"] = _sample(df_failed, id_col)
    return r

# ── Individual check functions ───────────────────────────────────────────────────

def chk_null_pk(df: DataFrame, pk_col: str) -> dict:
    count = df.filter(F.col(pk_col).isNull()).count()
    return _result(count)  # no masked sample — the ID itself is null

def chk_duplicates(df: DataFrame, pk_col: str) -> dict:
    dup_groups = (
        df.groupBy(pk_col)
          .count()
          .filter(F.col("count") > 1)
          .cache()
    )
    agg_val   = dup_groups.agg(F.sum(F.col("count") - 1)).collect()[0][0]
    dup_count = int(agg_val) if agg_val is not None else 0
    result = {"pass": dup_count == 0, "count": dup_count}
    if dup_count > 0:
        result["sample_ids_masked"] = [
            _mask(row[pk_col]) for row in dup_groups.limit(5).collect()
        ]
    dup_groups.unpersist()
    return result

def chk_date_range(df: DataFrame, date_col: str, id_col: str,
                   min_date: str = None, max_date: str = None) -> dict:
    """date_col is a YYYY-MM-DD string written by Stage 1."""
    parsed = F.to_date(F.col(date_col), "yyyy-MM-dd")
    cond = F.lit(False)
    if min_date:
        cond = cond | (parsed < F.lit(min_date).cast("date"))
    if max_date:
        cond = cond | (parsed > F.lit(max_date).cast("date"))
    failed = df.filter(F.col(date_col).isNotNull() & cond)
    count  = failed.count()
    return _result(count, failed, id_col)

def chk_numeric_range(df: DataFrame, num_col: str, id_col: str,
                      lo: float, hi: float) -> dict:
    col_d  = F.col(num_col).cast(DoubleType())
    failed = df.filter(col_d.isNotNull() & ((col_d < lo) | (col_d > hi)))
    count  = failed.count()
    return _result(count, failed, id_col)

def chk_zscore_outliers(df: DataFrame, num_col: str, threshold: float = 3.0) -> dict:
    """Collect numeric column to driver, flag rows where |z-score| > threshold."""
    vals = [
        row[0] for row in
        df.select(F.col(num_col).cast(DoubleType())).dropna().collect()
    ]
    if len(vals) < 2:
        return {"pass": True, "count": 0}
    arr   = np.array(vals, dtype=np.float64)
    z     = np.abs((arr - arr.mean()) / arr.std(ddof=1))
    count = int(np.sum(z > threshold))
    return {"pass": count == 0, "count": count}

def chk_fk(child_df: DataFrame, parent_df: DataFrame,
           join_col: str, child_pk: str) -> dict:
    """Rows in child_df where join_col has no matching row in parent_df."""
    parent_keys = parent_df.select(join_col).distinct()
    failed = child_df.join(parent_keys, on=join_col, how="left_anti")
    count  = failed.count()
    return _result(count, failed, child_pk)

def chk_icd10_format(df: DataFrame) -> dict:
    # ICD-10-CM: letter + 2 digits, optional dot + 1-4 alphanumerics
    pattern = r"^[A-Z][0-9]{2}(\.[A-Z0-9]{1,4})?$"
    failed  = df.filter(
        F.col("icd10_code").isNotNull() & ~F.col("icd10_code").rlike(pattern)
    )
    count = failed.count()
    return _result(count, failed, "diagnosis_id")

# ── Per-table runners ────────────────────────────────────────────────────────────

def run_patients(df: DataFrame) -> dict:
    today = date.today().isoformat()
    return {
        "null_patient_id": chk_null_pk(df, "patient_id"),
        "future_dob":      chk_date_range(df, "date_of_birth", "patient_id", max_date=today),
        "dob_before_1900": chk_date_range(df, "date_of_birth", "patient_id", min_date="1900-01-01"),
        "duplicate_pk":    chk_duplicates(df, "patient_id"),
    }

def run_encounters(df: DataFrame, patients_df: DataFrame, providers_df: DataFrame) -> dict:
    today = date.today().isoformat()
    return {
        "null_encounter_id":          chk_null_pk(df, "encounter_id"),
        "future_encounter_date":      chk_date_range(df, "encounter_date", "encounter_id", max_date=today),
        "encounter_date_before_1900": chk_date_range(df, "encounter_date", "encounter_id", min_date="1900-01-01"),
        "fk_patient_id":              chk_fk(df, patients_df,  "patient_id",  "encounter_id"),
        "fk_provider_id":             chk_fk(df, providers_df, "provider_id", "encounter_id"),
        "duplicate_pk":               chk_duplicates(df, "encounter_id"),
    }

def run_diagnoses(df: DataFrame, encounters_df: DataFrame) -> dict:
    return {
        "null_diagnosis_id":      chk_null_pk(df, "diagnosis_id"),
        "onset_date_before_1900": chk_date_range(df, "onset_date", "diagnosis_id", min_date="1900-01-01"),
        "fk_encounter_id":        chk_fk(df, encounters_df, "encounter_id", "diagnosis_id"),
        "icd10_format":           chk_icd10_format(df),
        "duplicate_pk":           chk_duplicates(df, "diagnosis_id"),
    }

def run_lab_results(df: DataFrame, encounters_df: DataFrame, patients_df: DataFrame) -> dict:
    return {
        "null_lab_id":     chk_null_pk(df, "lab_id"),
        "fk_encounter_id": chk_fk(df, encounters_df, "encounter_id", "lab_id"),
        "fk_patient_id":   chk_fk(df, patients_df,  "patient_id",   "lab_id"),
        "duplicate_pk":    chk_duplicates(df, "lab_id"),
    }

def run_medications(df: DataFrame, encounters_df: DataFrame, patients_df: DataFrame) -> dict:
    return {
        "null_medication_id":    chk_null_pk(df, "medication_id"),
        "fk_encounter_id":       chk_fk(df, encounters_df, "encounter_id", "medication_id"),
        "fk_patient_id":         chk_fk(df, patients_df,  "patient_id",   "medication_id"),
        "adherence_rate_range":  chk_numeric_range(df, "adherence_rate_pct", "medication_id", 0.0, 100.0),
        "adherence_rate_zscore": chk_zscore_outliers(df, "adherence_rate_pct"),
        "duplicate_pk":          chk_duplicates(df, "medication_id"),
    }

def run_providers(df: DataFrame) -> dict:
    return {
        "null_provider_id": chk_null_pk(df, "provider_id"),
        "duplicate_pk":     chk_duplicates(df, "provider_id"),
    }

def run_vitals(df: DataFrame, encounters_df: DataFrame) -> dict:
    return {
        "null_vitals_id":               chk_null_pk(df, "vitals_id"),
        "fk_encounter_id":              chk_fk(df, encounters_df, "encounter_id", "vitals_id"),
        "spo2_range":                   chk_numeric_range(df, "spo2_pct",           "vitals_id",  0.0, 100.0),
        "spo2_zscore_outliers":         chk_zscore_outliers(df, "spo2_pct"),
        "heart_rate_range":             chk_numeric_range(df, "heart_rate_bpm",     "vitals_id", 20.0, 300.0),
        "heart_rate_zscore_outliers":   chk_zscore_outliers(df, "heart_rate_bpm"),
        "temperature_range":            chk_numeric_range(df, "temperature_celsius","vitals_id", 34.0,  42.0),
        "temperature_zscore_outliers":  chk_zscore_outliers(df, "temperature_celsius"),
        "systolic_bp_range":            chk_numeric_range(df, "systolic_bp_mmhg",   "vitals_id", 50.0, 250.0),
        "systolic_bp_zscore_outliers":  chk_zscore_outliers(df, "systolic_bp_mmhg"),
        "diastolic_bp_range":           chk_numeric_range(df, "diastolic_bp_mmhg",  "vitals_id", 30.0, 150.0),
        "diastolic_bp_zscore_outliers": chk_zscore_outliers(df, "diastolic_bp_mmhg"),
        "duplicate_pk":                 chk_duplicates(df, "vitals_id"),
    }

# ── Charts ───────────────────────────────────────────────────────────────────────

def _save(fig, name: str):
    path = os.path.join(CHARTS_DIR, name)
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    log.info("  chart → %s", path)

def chart_row_counts(table_counts: dict):
    tables = list(table_counts)
    counts = [table_counts[t] for t in tables]
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(tables, counts, color="steelblue")
    ax.bar_label(bars, fmt="%d", padding=4)
    ax.set_xlabel("Row Count")
    ax.set_title("Cleaned Row Counts per Table")
    _save(fig, "row_counts.png")

def chart_check_failures(report: dict):
    tables = list(report["tables"])
    fails  = [
        sum(1 for c in report["tables"][t]["checks"].values() if not c["pass"])
        for t in tables
    ]
    colors = ["tomato" if f > 0 else "mediumseagreen" for f in fails]
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(tables, fails, color=colors)
    ax.bar_label(bars, fmt="%d", padding=4)
    ax.set_xlabel("Failed Checks")
    ax.set_title("Quality Check Failures per Table  (green = all pass)")
    _save(fig, "check_failures.png")

def chart_numeric_distributions(vitals_df: DataFrame, meds_df: DataFrame):
    specs = [
        (vitals_df, "spo2_pct",            "SpO₂ (%)",            0,    100),
        (vitals_df, "heart_rate_bpm",       "Heart Rate (bpm)",    20,   300),
        (vitals_df, "temperature_celsius",  "Temperature (°C)",    34,    42),
        (vitals_df, "systolic_bp_mmhg",     "Systolic BP (mmHg)",  50,   250),
        (vitals_df, "diastolic_bp_mmhg",    "Diastolic BP (mmHg)", 30,   150),
        (meds_df,   "adherence_rate_pct",   "Adherence (%)",        0,   100),
    ]
    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    for ax, (df, col, label, lo, hi) in zip(axes.flatten(), specs):
        vals = [
            row[0] for row in
            df.select(F.col(col).cast(DoubleType())).dropna().collect()
        ]
        arr = np.array(vals, dtype=np.float64)
        ax.hist(arr, bins=50, color="steelblue", alpha=0.75, edgecolor="none")
        ax.axvline(lo, color="red",    linestyle="--", linewidth=1.2, label=f"min={lo}")
        ax.axvline(hi, color="orange", linestyle="--", linewidth=1.2, label=f"max={hi}")
        ax.set_title(label)
        ax.set_xlabel(label)
        ax.set_ylabel("Count")
        ax.legend(fontsize=7)
    fig.suptitle("Numeric Distributions — hard range bounds marked", fontsize=13)
    plt.tight_layout()
    _save(fig, "numeric_distributions.png")

def chart_fk_violations(report: dict):
    labels, counts = [], []
    for tname, tdata in report["tables"].items():
        for cname, cdata in tdata["checks"].items():
            if cname.startswith("fk_"):
                labels.append(f"{tname}.{cname}")
                counts.append(cdata["count"])
    if not labels:
        return
    colors = ["tomato" if c > 0 else "mediumseagreen" for c in counts]
    fig, ax = plt.subplots(figsize=(12, max(4, len(labels) * 0.45)))
    bars = ax.barh(labels, counts, color=colors)
    ax.bar_label(bars, fmt="%d", padding=4)
    ax.set_xlabel("Violation Count")
    ax.set_title("Foreign Key Violations  (green = no violations)")
    _save(fig, "fk_violations.png")

# ── Main ─────────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark()
    log.info("Stage 2 — Data Quality starting")
    log.info("  cleaned : %s", CLEANED_DIR)
    log.info("  report  : %s", REPORT_PATH)
    log.info("  charts  : %s", CHARTS_DIR)

    os.makedirs(CHARTS_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    patients_df   = spark.read.parquet(os.path.join(CLEANED_DIR, "patients"))
    encounters_df = spark.read.parquet(os.path.join(CLEANED_DIR, "encounters"))
    diagnoses_df  = spark.read.parquet(os.path.join(CLEANED_DIR, "diagnoses"))
    lab_df        = spark.read.parquet(os.path.join(CLEANED_DIR, "lab_results"))
    meds_df       = spark.read.parquet(os.path.join(CLEANED_DIR, "medications"))
    providers_df  = spark.read.parquet(os.path.join(CLEANED_DIR, "providers"))
    vitals_df     = spark.read.parquet(os.path.join(CLEANED_DIR, "vitals"))

    # Cache the three tables hit by multiple FK joins.
    patients_df.cache()
    encounters_df.cache()
    providers_df.cache()

    table_jobs = [
        ("patients",    patients_df,   lambda d: run_patients(d)),
        ("encounters",  encounters_df, lambda d: run_encounters(d, patients_df, providers_df)),
        ("diagnoses",   diagnoses_df,  lambda d: run_diagnoses(d, encounters_df)),
        ("lab_results", lab_df,        lambda d: run_lab_results(d, encounters_df, patients_df)),
        ("medications", meds_df,       lambda d: run_medications(d, encounters_df, patients_df)),
        ("providers",   providers_df,  lambda d: run_providers(d)),
        ("vitals",      vitals_df,     lambda d: run_vitals(d, encounters_df)),
    ]

    tables_report: dict = {}
    for name, df, runner in table_jobs:
        log.info("[%s] running checks ...", name)
        row_count = df.count()
        checks    = runner(df)
        tables_report[name] = {"row_count": row_count, "checks": checks}
        failed_names = [k for k, v in checks.items() if not v["pass"]]
        log.info("[%-15s]  rows=%d  failures=%s", name, row_count, failed_names or "none")

    overall_pass = all(
        chk["pass"]
        for tdata in tables_report.values()
        for chk in tdata["checks"].values()
    )

    report = {
        "run_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "overall_pass":  overall_pass,
        "tables":        tables_report,
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    log.info("Report → %s  (overall_pass=%s)", REPORT_PATH, overall_pass)

    table_counts = {n: tables_report[n]["row_count"] for n in tables_report}
    chart_row_counts(table_counts)
    chart_check_failures(report)
    chart_numeric_distributions(vitals_df, meds_df)
    chart_fk_violations(report)

    patients_df.unpersist()
    encounters_df.unpersist()
    providers_df.unpersist()

    log.info("Stage 2 — Data Quality complete  overall_pass=%s", overall_pass)
    spark.stop()


if __name__ == "__main__":
    main()
