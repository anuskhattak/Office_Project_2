# CLAUDE.md — Healthcare Data Pipeline (Messy → Gold Layer)

## Role
I am a **Data Engineer Specialist** building a pipeline that takes 7 messy healthcare CSVs and produces a single warehouse-ready Gold layer Parquet file.

---

## Pipeline (4 Stages)

```
Messy CSVs
    ↓
[Stage 1] src/clean.py              → cleans, fixes, quarantines bad rows
    ↓
[Stage 2] src/data_quality.py       → contract checks → quality_report.json
    ↓
[Stage 3] src/root_cause_agent.py   → reads report → publish: true/false
    ↓
[Stage 4] Airflow publish gate
    ├── true  → output/gold/fact_cdr.parquet  ✓
    └── false → HALT, nothing written          ✗
```

The agent never sees raw patient data — only counts and masked sample IDs.

---

## Tech Stack

| Tool | Role |
|---|---|
| PySpark 3.x | Read CSVs, clean at scale, write Parquet |
| Airflow 2.x | Orchestrate the 4-stage DAG |
| NumPy | Numeric range checks, z-score outlier detection |
| Matplotlib | Charts saved alongside quality report |

---

## Folder Structure

```
E:/Office_Project_2/
├── healthcare_dataset/       ← source CSVs — never modify
├── src/
│   ├── clean.py
│   ├── data_quality.py
│   ├── root_cause_agent.py
│   └── utils/
├── dags/
│   └── healthcare_pipeline_dag.py
├── output/
│   ├── cleaned/              ← intermediate Parquet
│   ├── quarantine/           ← rejected rows with rejection_reason
│   ├── quality_report.json
│   ├── agent_verdict.json
│   └── gold/fact_cdr.parquet ← final output
└── .claude/commands/         ← skills (load on demand)
```

---

## Coding Rules

- PySpark DataFrames only — no Pandas for large files
- All file paths from config or env vars — no hardcoded paths
- Log row counts in and out for every stage
- Never overwrite `healthcare_dataset/`
- Quarantined rows always get a `rejection_reason` — never drop silently
- Agent verdict is the only gate for Gold write — never bypass it

---

## Skills (load when needed)

| Skill | Load when... |
|---|---|
| `/dataset-schema` | Working with any of the 7 source files |
| `/clean-rules` | Writing or reviewing `clean.py` |
| `/quality-contracts` | Writing or reviewing `data_quality.py` |
| `/agent-spec` | Writing or reviewing `root_cause_agent.py` |
| `/dag-spec` | Writing or reviewing the Airflow DAG |
