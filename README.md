# Healthcare Data Pipeline — Messy CSV to Gold Layer

A 4-stage PySpark + Airflow pipeline that ingests 7 messy healthcare CSVs, enforces data quality contracts, runs an automated root-cause agent, and writes a warehouse-ready Gold layer Parquet file only when data quality passes.

---

## Pipeline Overview

```
healthcare_dataset/  (7 raw CSVs)
        │
        ▼
[Stage 1]  src/clean.py
           • Fixes encoding, nulls, date formats, sentinel strings
           • Writes cleaned rows  → output/cleaned/<table>/
           • Quarantines bad rows → output/quarantine/<table>/  (with rejection_reason)
        │
        ▼
[Stage 2]  src/data_quality.py
           • Runs quality contracts: null PKs, duplicates, FK integrity,
             ICD-10 format, outlier z-scores, range checks
           • Writes output/quality_report.json + output/charts/*.png
        │
        ▼
[Stage 3]  src/root_cause_agent.py
           • Reads quality_report.json (never sees raw patient data — masked IDs only)
           • Classifies failures as CRITICAL or MINOR
           • Writes output/agent_verdict.json  { "publish": true | false }
        │
        ▼
[Stage 4]  dags/healthcare_pipeline_dag.py  (Airflow)
           ├── publish: true  → output/gold/fact_cdr.parquet  ✓
           └── publish: false → HALT, nothing written          ✗
```

---

## Folder Structure

```
Office_Project_2/
├── healthcare_dataset/          ← source CSVs (read-only, never modified)
│   ├── messy_patients.csv
│   ├── messy_encounters.csv
│   ├── messy_diagnoses.csv
│   ├── messy_lab_results.csv
│   ├── messy_medications.csv
│   ├── messy_providers.csv
│   └── messy_vitals.csv
├── src/
│   ├── clean.py                 ← Stage 1: cleaning + quarantine
│   ├── data_quality.py          ← Stage 2: quality contracts + charts
│   └── root_cause_agent.py      ← Stage 3: verdict agent
├── dags/
│   └── healthcare_pipeline_dag.py  ← Stage 4: Airflow DAG
├── output/
│   ├── cleaned/                 ← intermediate cleaned Parquet (per table)
│   ├── quarantine/              ← rejected rows with rejection_reason
│   ├── charts/                  ← quality check charts (PNG)
│   ├── quality_report.json      ← full quality check results
│   ├── agent_verdict.json       ← { "publish": true/false, "reasons": [...] }
│   └── gold/
│       └── fact_cdr.parquet     ← final Gold output (only written on pass)
└── venv/                        ← Python virtual environment
```

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| PySpark | 3.x | Read CSVs, clean at scale, write Parquet |
| Apache Airflow | 2.x | Orchestrate the 4-stage DAG |
| NumPy | latest | Numeric range checks, z-score outlier detection |
| Matplotlib | latest | Quality charts saved alongside report |
| Python | 3.10+ | Glue and agent logic |

---

## Setup

### 1. Create and activate the virtual environment

```bash
python -m venv venv
# Windows
venv\Scripts\Activate.ps1
# Linux / macOS
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install pyspark apache-airflow numpy matplotlib
```

### 3. Configure environment variables (optional — defaults work out of the box)

| Variable | Default | Description |
|---|---|---|
| `SOURCE_DIR` | `healthcare_dataset/` | Path to raw CSVs |
| `CLEANED_DIR` | `output/cleaned/` | Cleaned Parquet output |
| `QUARANTINE_DIR` | `output/quarantine/` | Quarantined rows output |
| `REPORT_PATH` | `output/quality_report.json` | Quality report path |
| `CHARTS_DIR` | `output/charts/` | Chart output directory |
| `VERDICT_PATH` | `output/agent_verdict.json` | Agent verdict path |
| `GOLD_PATH` | `output/gold/fact_cdr.parquet` | Final Gold Parquet path |

---

## Running the Pipeline

### Option A — Airflow (recommended)

```bash
# Set AIRFLOW_HOME and initialize
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow dags list

# Start the scheduler and webserver
airflow scheduler &
airflow webserver --port 8080

# Trigger the DAG
airflow dags trigger healthcare_messy_to_gold
```

### Option B — Run each stage manually

```bash
python src/clean.py
python src/data_quality.py
python src/root_cause_agent.py
# Gold write only happens via the DAG's publish gate
```

---

## Quality Checks (Stage 2)

| Check | Tables | Criticality if Failing |
|---|---|---|
| Null primary key | all | CRITICAL |
| Duplicate primary key | all | CRITICAL |
| Foreign key integrity | encounters, diagnoses, labs, meds, vitals | CRITICAL |
| ICD-10 code format | diagnoses | CRITICAL |
| Outlier z-score (numeric cols) | labs, vitals | CRITICAL if > 5% of rows |
| Numeric range checks | vitals, labs | MINOR |

Any CRITICAL failure sets `publish: false` in the agent verdict and halts the Gold write.

---

## Agent Verdict Logic (Stage 3)

The agent never reads raw patient data — only row counts and masked sample IDs (first 4 characters).

**CRITICAL** (→ `publish: false`) if any check:
- Has name `duplicate_pk` or `icd10_format`
- Starts with `null_` or `fk_`
- Affects more than 5% of the table's rows

**MINOR** — logged but does not block publishing.

The verdict is written to `output/agent_verdict.json`:

```json
{
  "publish": false,
  "critical_issues": ["patients.null_patient_id: 12 rows (2.4%) — CRITICAL: null primary key."],
  "minor_issues": [],
  "timestamp": "2026-06-27T10:00:00Z"
}
```

---

## Design Principles

- **PySpark DataFrames only** — no Pandas for large-scale processing
- **All paths from env vars** — no hardcoded file paths
- **Row counts logged in/out** at every stage
- **Source data is immutable** — `healthcare_dataset/` is never modified
- **Silent drops are forbidden** — every rejected row gets a `rejection_reason`
- **Agent verdict is the sole gate** — the Gold write cannot be bypassed
