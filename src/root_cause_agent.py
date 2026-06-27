"""Stage 3 — Root Cause Agent: reads quality_report.json, applies quality rules, writes agent_verdict.json."""

import json
import logging
import os
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# CRITICAL by name — regardless of row count
_CRITICAL_CHECKS = {"duplicate_pk", "icd10_format"}
_CRITICAL_PREFIXES = ("null_", "fk_")

# Threshold: a check affecting > 5% of a table's rows is always CRITICAL
_CRITICAL_PCT = 0.05


def _is_critical(check_name: str, count: int, row_count: int) -> bool:
    if check_name in _CRITICAL_CHECKS:
        return True
    if any(check_name.startswith(p) for p in _CRITICAL_PREFIXES):
        return True
    if row_count > 0 and count / row_count > _CRITICAL_PCT:
        return True
    return False


def _analyze_report(report: dict) -> dict:
    tables = report.get("tables", {})
    critical = []
    minor = []

    for table_name, table_data in tables.items():
        row_count = table_data.get("row_count", 0)
        for check_name, check_data in table_data.get("checks", {}).items():
            if check_data.get("pass"):
                continue

            count = check_data.get("count", 0)
            label = f"{table_name}.{check_name}"

            if _is_critical(check_name, count, row_count):
                pct = f"{count / row_count:.1%}" if row_count else "N/A"
                critical.append(
                    f"{label}: {count} rows ({pct}) — CRITICAL: "
                    + (
                        "null primary key." if check_name.startswith("null_") else
                        "duplicate primary keys." if check_name == "duplicate_pk" else
                        "foreign key violation — referential integrity broken." if check_name.startswith("fk_") else
                        "malformed ICD-10 code." if check_name == "icd10_format" else
                        f"exceeds 5% failure threshold ({pct} of table)."
                    )
                )
            else:
                minor.append(
                    f"{label}: {count} rows — MINOR: low count, no referential integrity impact."
                )

    publish = len(critical) == 0

    root_causes = critical + minor

    recommendations = []
    for item in critical:
        check_ref = item.split(":")[0]
        recommendations.append(f"Investigate and resolve {check_ref} before re-running the pipeline.")
    for item in minor:
        check_ref = item.split(":")[0]
        recommendations.append(f"Review {check_ref} — acceptable to publish with monitoring.")

    if not critical and not minor:
        summary = "All quality checks passed — data is safe to publish to the Gold layer."
    elif publish:
        summary = (
            f"{len(minor)} minor quality issue(s) detected but no critical failures — "
            "data is safe to publish with warnings."
        )
    else:
        summary = (
            f"{len(critical)} critical failure(s) detected — "
            "Gold layer write is blocked until issues are resolved."
        )

    return {
        "publish": publish,
        "summary": summary,
        "root_causes": root_causes,
        "recommendations": recommendations,
    }


def _load_report(report_path: str) -> dict:
    if not os.path.exists(report_path):
        raise FileNotFoundError(f"Quality report not found: {report_path}")
    with open(report_path) as f:
        return json.load(f)


def _write_verdict(verdict: dict, verdict_path: str) -> None:
    parent = os.path.dirname(verdict_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(verdict_path, "w") as f:
        json.dump(verdict, f, indent=2)


def main() -> None:
    report_path  = os.environ.get("REPORT_PATH",  "output/quality_report.json")
    verdict_path = os.environ.get("VERDICT_PATH", "output/agent_verdict.json")

    logger.info("Stage 3 — Root Cause Agent starting")
    logger.info("Input  report : %s", report_path)
    logger.info("Output verdict: %s", verdict_path)

    report = _load_report(report_path)

    overall_pass = report.get("overall_pass", False)
    run_ts       = report.get("run_timestamp", "unknown")
    table_count  = len(report.get("tables", {}))

    logger.info(
        "Report loaded — run_timestamp=%s overall_pass=%s tables=%d",
        run_ts, overall_pass, table_count,
    )

    if overall_pass:
        verdict = {
            "publish": True,
            "summary": "All quality checks passed with no failures detected across all tables.",
            "root_causes": [],
            "recommendations": [],
        }
    else:
        logger.info("Quality failures detected — analyzing report")
        verdict = _analyze_report(report)

    logger.info("Agent verdict — publish=%s", verdict.get("publish"))
    logger.info("Summary: %s", verdict.get("summary"))
    logger.info(
        "Root causes identified: %d  Recommendations: %d",
        len(verdict.get("root_causes", [])),
        len(verdict.get("recommendations", [])),
    )

    verdict["agent_timestamp"]      = datetime.now(timezone.utc).isoformat()
    verdict["report_run_timestamp"] = run_ts

    _write_verdict(verdict, verdict_path)
    logger.info("Verdict written to: %s", verdict_path)

    if not verdict.get("publish"):
        logger.warning(
            "PUBLISH BLOCKED — Gold layer write will not proceed. Review: %s",
            verdict_path,
        )
        sys.exit(1)

    logger.info("PUBLISH APPROVED — Gold layer write may proceed")


if __name__ == "__main__":
    main()
