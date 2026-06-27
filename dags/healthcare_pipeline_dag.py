"""
healthcare_messy_to_gold — Airflow DAG
=======================================

File connections: how this DAG wires the three src scripts together
--------------------------------------------------------------------

  dags/healthcare_pipeline_dag.py
  │
  ├── clean_task  ──calls──►  src/clean.py
  │                               reads  : healthcare_dataset/  (7 CSVs)
  │                               writes : output/cleaned/      (Parquet)
  │                               writes : output/quarantine/   (bad rows)
  │
  ├── quality_task ──calls──►  src/data_quality.py
  │                               reads  : output/cleaned/
  │                               writes : output/quality_report.json
  │                               writes : output/charts/*.png
  │
  ├── agent_task  ──calls──►  src/root_cause_agent.py
  │                               reads  : output/quality_report.json
  │                               applies: CRITICAL/MINOR Python rules → publish verdict
  │                               writes : output/agent_verdict.json
  │
  ├── publish_decision  reads agent_verdict.json → branch
  │       │
  │   publish=true              publish=false
  │       │                         │
  ├── write_gold                halt_task
  │   reads  output/cleaned/    logs verdict, raises AirflowSkipException
  │   writes output/gold/fact_cdr.parquet
  │
  └── (end)

Env vars passed to each script (Airflow Variables → env var fallback):
  clean.py          : SOURCE_DIR, CLEANED_DIR, QUARANTINE_DIR
  data_quality.py   : CLEANED_DIR, REPORT_PATH, CHARTS_DIR
  root_cause_agent  : REPORT_PATH, VERDICT_PATH
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _paths() -> dict[str, str]:
    """Resolve all pipeline paths — Airflow Variables first, env vars as fallback."""
    def v(key: str, default: str) -> str:
        return Variable.get(key, default_var=os.environ.get(key, default))

    return {
        "source_dir":     v("SOURCE_DIR",     os.path.join(_BASE_DIR, "healthcare_dataset")),
        "cleaned_dir":    v("CLEANED_DIR",    os.path.join(_BASE_DIR, "output", "cleaned")),
        "quarantine_dir": v("QUARANTINE_DIR", os.path.join(_BASE_DIR, "output", "quarantine")),
        "report_path":    v("REPORT_PATH",    os.path.join(_BASE_DIR, "output", "quality_report.json")),
        "charts_dir":     v("CHARTS_DIR",     os.path.join(_BASE_DIR, "output", "charts")),
        "verdict_path":   v("VERDICT_PATH",   os.path.join(_BASE_DIR, "output", "agent_verdict.json")),
        "gold_dir":       v("GOLD_DIR",       os.path.join(_BASE_DIR, "output", "gold")),
    }


def _run_script(script_rel: str, env_overrides: dict[str, str]) -> None:
    """Run a Python script as a subprocess with env overrides; relay its logs."""
    script = os.path.join(_BASE_DIR, script_rel)
    env = {**os.environ, **env_overrides}
    result = subprocess.run(
        [sys.executable, script],
        env=env,
        cwd=_BASE_DIR,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        log.info("%s stdout:\n%s", script_rel, result.stdout.rstrip())
    if result.stderr:
        log.info("%s stderr:\n%s", script_rel, result.stderr.rstrip())
    return result


# ---------------------------------------------------------------------------
# Stage 1 — Clean
# ---------------------------------------------------------------------------

def _clean(**context) -> None:
    p = _paths()
    result = _run_script(
        "src/clean.py",
        {
            "SOURCE_DIR":     p["source_dir"],
            "CLEANED_DIR":    p["cleaned_dir"],
            "QUARANTINE_DIR": p["quarantine_dir"],
        },
    )
    if result.returncode != 0:
        raise RuntimeError(f"clean.py exited with code {result.returncode}")
    log.info("clean_task — completed successfully")


# ---------------------------------------------------------------------------
# Stage 2 — Data Quality
# ---------------------------------------------------------------------------

def _quality(**context) -> None:
    p = _paths()
    result = _run_script(
        "src/data_quality.py",
        {
            "CLEANED_DIR": p["cleaned_dir"],
            "REPORT_PATH": p["report_path"],
            "CHARTS_DIR":  p["charts_dir"],
        },
    )
    if result.returncode != 0:
        raise RuntimeError(f"data_quality.py exited with code {result.returncode}")

    report: dict = {}
    if os.path.exists(p["report_path"]):
        with open(p["report_path"]) as f:
            report = json.load(f)
        for table, info in report.get("tables", {}).items():
            log.info("quality_task row_count — %s: %d", table, info.get("row_count", 0))

    log.info("quality_task — overall_pass=%s", report.get("overall_pass"))


# ---------------------------------------------------------------------------
# Stage 3 — Root Cause Agent
# ---------------------------------------------------------------------------

def _agent(**context) -> None:
    p = _paths()
    result = _run_script(
        "src/root_cause_agent.py",
        {
            "REPORT_PATH":  p["report_path"],
            "VERDICT_PATH": p["verdict_path"],
        },
    )
    # exit 0 = publish approved; exit 1 = publish blocked — both are valid agent outcomes
    if result.returncode not in (0, 1):
        raise RuntimeError(f"root_cause_agent.py exited with code {result.returncode}")

    if os.path.exists(p["verdict_path"]):
        with open(p["verdict_path"]) as f:
            verdict = json.load(f)
        log.info(
            "agent_task — publish=%s summary=%s",
            verdict.get("publish"),
            verdict.get("summary"),
        )


# ---------------------------------------------------------------------------
# Stage 4a — Branch
# ---------------------------------------------------------------------------

def _branch(**context) -> str:
    p = _paths()
    with open(p["verdict_path"]) as f:
        verdict = json.load(f)
    publish = bool(verdict.get("publish", False))
    log.info("publish_decision — routing to %s", "write_gold" if publish else "halt_task")
    return "write_gold" if publish else "halt_task"


# ---------------------------------------------------------------------------
# Stage 4b — Write Gold
# ---------------------------------------------------------------------------

def _write_gold(**context) -> None:
    from pyspark.sql import SparkSession  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    p = _paths()
    cleaned = p["cleaned_dir"]
    gold_path = os.path.join(p["gold_dir"], "fact_cdr.parquet")

    spark = (
        SparkSession.builder
        .appName("healthcare_write_gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        encounters = spark.read.parquet(os.path.join(cleaned, "encounters"))
        patients   = spark.read.parquet(os.path.join(cleaned, "patients"))
        providers  = spark.read.parquet(os.path.join(cleaned, "providers"))

        rows_in = encounters.count()
        log.info("write_gold rows_in — encounters: %d", rows_in)

        gold = (
            encounters
            .join(
                patients.select(
                    "patient_id",
                    "date_of_birth",
                    "gender",
                    "insurance",
                ),
                on="patient_id",
                how="left",
            )
            .join(
                providers.select(
                    "provider_id",
                    F.col("specialty").alias("provider_specialty"),
                    F.col("board_certified").alias("provider_board_certified"),
                ),
                on="provider_id",
                how="left",
            )
        )

        os.makedirs(p["gold_dir"], exist_ok=True)
        gold.write.mode("overwrite").parquet(gold_path)

        rows_out = gold.count()
        log.info("write_gold rows_out — fact_cdr: %d", rows_out)
        log.info("write_gold — Gold Parquet written to: %s", gold_path)
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Stage 4c — Halt
# ---------------------------------------------------------------------------

def _halt(**context) -> None:
    p = _paths()
    if os.path.exists(p["verdict_path"]):
        with open(p["verdict_path"]) as f:
            verdict = json.load(f)
        log.warning("PIPELINE HALTED — publish=false")
        log.warning("Summary    : %s", verdict.get("summary"))
        for item in verdict.get("root_causes", []):
            log.warning("Root cause : %s", item)
        for item in verdict.get("recommendations", []):
            log.warning("Recommend  : %s", item)
    raise AirflowSkipException(
        "Gold write blocked by agent verdict — review output/agent_verdict.json"
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="healthcare_messy_to_gold",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["healthcare", "pipeline"],
    default_args={
        "owner": "data-engineering",
        "retries": 0,
    },
) as dag:

    clean_task = PythonOperator(
        task_id="clean_task",
        python_callable=_clean,
    )

    quality_task = PythonOperator(
        task_id="quality_task",
        python_callable=_quality,
    )

    agent_task = PythonOperator(
        task_id="agent_task",
        python_callable=_agent,
    )

    publish_decision = BranchPythonOperator(
        task_id="publish_decision",
        python_callable=_branch,
    )

    write_gold = PythonOperator(
        task_id="write_gold",
        python_callable=_write_gold,
    )

    halt_task = PythonOperator(
        task_id="halt_task",
        python_callable=_halt,
    )

    clean_task >> quality_task >> agent_task >> publish_decision
    publish_decision >> [write_gold, halt_task]
