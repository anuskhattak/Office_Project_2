"""
Stage 1 — Clean
Reads all 7 messy healthcare CSVs, applies per-table cleaning rules,
writes cleaned rows to output/cleaned/<table>/ and unfixable rows to
output/quarantine/<table>/ (with rejection_reason column).
"""

import logging
import os
from datetime import date, datetime as _dt

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

# ── Config ─────────────────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR   = os.path.dirname(_SCRIPT_DIR)

SOURCE_DIR     = os.getenv("SOURCE_DIR",     os.path.join(_BASE_DIR, "healthcare_dataset"))
CLEANED_DIR    = os.getenv("CLEANED_DIR",    os.path.join(_BASE_DIR, "output", "cleaned"))
QUARANTINE_DIR = os.getenv("QUARANTINE_DIR", os.path.join(_BASE_DIR, "output", "quarantine"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# ── Spark ──────────────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("healthcare_stage1_clean")
        # CORRECTED uses Java DateTimeFormatter (not SimpleDateFormat) so it
        # handles microsecond precision (SSSSSS) and returns null on mismatch
        # instead of throwing — both required for the coalesce chain to work.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )

# ── Shared constants ───────────────────────────────────────────────────────────
# Sentinel strings (case-insensitive after lower()) treated as null.
_SENTINELS = {"", "-", "n/a", "not provided", "null"}

# Ordered most-specific → least-specific; coalesce stops at first non-null.
# Datetime formats must come before date-only fallbacks.
_DATE_FORMATS = [
    "yyyy-MM-dd",        # 2019-11-07
    "MM/dd/yyyy",        # 09/17/2022
    "M/d/yyyy",          # 3/7/2005
    "MM/dd/yy",          # 01/06/17
    "M/d/yy",            # 1/6/17
    "dd-MM-yyyy",        # 14-07-2025  (European dashes; confirmed by day>12 rows)
    "d-M-yyyy",          # 4-7-2025  single-digit European
    "yyyy/MM/dd",        # 2025/02/01
    "MMMM dd, yyyy",     # October 26, 2018
    "MMMM d, yyyy",      # October 2, 2018
]

_DATETIME_FORMATS = [
    # Full datetime — most precise first
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",  # 2023-11-22T22:59:32.609491  microseconds
    "yyyy-MM-dd'T'HH:mm:ss",          # 2023-11-23T09:11:25
    "yyyy-MM-dd HH:mm:ss",            # space-separated ISO
    "dd-MM-yyyy HH:mm:ss",            # 23-05-2025 01:56:13  European
    "dd-MM-yyyy HH:mm",               # European no seconds
    "MM/dd/yyyy HH:mm:ss",            # American full
    "MM/dd/yyyy HH:mm",               # 05/24/2025 08:46
    "M/d/yyyy H:mm:ss",               # single-digit American
    "M/d/yyyy H:mm",                  # single-digit American no seconds
    "MM/dd/yy hh:mm a",               # 01/03/24 09:01 AM  (AM/PM 12-hour)
    "M/d/yy h:mm a",                  # single-digit AM/PM
    "MM/dd/yy HH:mm",                 # 2-digit year 24-hour
    # Date-only fallbacks (when a datetime column contains only a date)
    "yyyy/MM/dd",                     # 2025/02/01
    "dd-MM-yyyy",                     # 01-08-2021  European date-only
    "MM/dd/yy",                       # 01/16/25  date-only 2-digit year
    "M/d/yy",                         # 1/6/25
    "yyyy-MM-dd",                     # ISO date-only
]

_BOOL_TRUE  = {"true", "yes", "y", "1", "t"}
_BOOL_FALSE = {"false", "no", "n", "0", "f"}

# Python-side date formats used by the UDF fallback.
# Python's strptime is locale-independent and handles long-form month names
# reliably, covering cases where Java's DateTimeFormatter returns null due to
# locale or strict-padding behaviour under CORRECTED timeParserPolicy.
_PY_DATE_FORMATS = [
    "%Y-%m-%d",
    "%m/%d/%Y", "%m/%d/%y",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%B %d, %Y",   # "September 11, 2024"  or  "September 04, 2025"
    "%b %d, %Y",   # "Sep 11, 2024"
]

@F.udf(returnType=StringType())
def _py_date_udf(val):
    """Return YYYY-MM-DD string or None; used as fallback when Spark native parse fails."""
    if not val:
        return None
    val = val.strip()
    for fmt in _PY_DATE_FORMATS:
        try:
            return _dt.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

# ── Shared helpers ─────────────────────────────────────────────────────────────
def _str_cols(df):
    return [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]


def strip_strings(df):
    """Strip leading/trailing whitespace from every string column."""
    for c in _str_cols(df):
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def nullify_sentinels(df):
    """Replace sentinel string values (case-insensitive) with NULL."""
    sentinels = list(_SENTINELS)
    for c in _str_cols(df):
        df = df.withColumn(
            c,
            F.when(F.lower(F.col(c)).isin(sentinels), None).otherwise(F.col(c)),
        )
    return df


def parse_date(col_expr):
    """Try Spark-native formats first, then fall back to Python UDF for formats
    (e.g. long-form month names) that Java's DateTimeFormatter may miss due to
    locale or strict-padding behaviour under the CORRECTED timeParserPolicy."""
    spark_attempts = [F.to_date(col_expr, fmt) for fmt in _DATE_FORMATS]
    udf_fallback   = F.to_date(_py_date_udf(col_expr), "yyyy-MM-dd")
    return F.coalesce(*spark_attempts, udf_fallback)


def parse_datetime(col_expr):
    """Try datetime formats in order; return first non-null match as TimestampType."""
    return F.coalesce(*[F.to_timestamp(col_expr, fmt) for fmt in _DATETIME_FORMATS])


def standardize_bool(col_name):
    """Map messy boolean strings → 'True' / 'False' string (null if unrecognised)."""
    c = F.lower(F.col(col_name))
    return (
        F.when(c.isin(list(_BOOL_TRUE)),  "True")
         .when(c.isin(list(_BOOL_FALSE)), "False")
         .otherwise(None)
    )


def _redate(df, col_name):
    """Parse a date column in-place; result is YYYY-MM-DD string."""
    tmp = col_name + "__parsed"
    return (
        df.withColumn(tmp, parse_date(F.col(col_name)))
          .withColumn(col_name, F.date_format(F.col(tmp), "yyyy-MM-dd"))
          .drop(tmp)
    )


def _redatetime(df, col_name):
    """Parse a datetime column in-place; result is TimestampType."""
    return df.withColumn(col_name, parse_datetime(F.col(col_name)))


def _log_counts(table: str, in_count: int, clean_count: int, q_count: int):
    log.info(
        "[%-15s]  in=%d  cleaned=%d  quarantined=%d",
        table, in_count, clean_count, q_count,
    )


def _write(df, path: str, label: str):
    df.write.mode("overwrite").parquet(path)
    log.info("  wrote %s → %s", label, path)


def _dedup_pk(clean_df, pk_col):
    """Keep first occurrence of each PK value; route extras to quarantine.
    Returns (deduped_df, dupes_df) where dupes_df has rejection_reason added."""
    # Materialize the ID as a column first so it is assigned once and stable
    # across both filter branches — using it inline in orderBy re-evaluates it
    # non-deterministically and can produce duplicates in both splits.
    df_with_id = clean_df.withColumn("_mid", F.monotonically_increasing_id())
    w       = Window.partitionBy(pk_col).orderBy("_mid")
    ranked  = df_with_id.withColumn("_rn", F.row_number().over(w))
    ranked.cache()
    ranked.count()  # force materialization before splitting
    deduped = ranked.filter(F.col("_rn") == 1).drop("_rn", "_mid")
    dupes   = (
        ranked.filter(F.col("_rn") > 1)
              .drop("_rn", "_mid")
              .withColumn("rejection_reason", F.lit("duplicate primary key"))
    )
    return deduped, dupes


def _fk_quarantine(child_df, parent_df, join_col, reason):
    """Quarantine child rows whose join_col has no match in parent_df.
    Returns (valid_df, orphans_df) where orphans_df carries rejection_reason."""
    parent_keys = parent_df.select(join_col).distinct()
    orphans = (
        child_df
        .join(parent_keys, on=join_col, how="left_anti")
        .withColumn("rejection_reason", F.lit(reason))
    )
    valid = child_df.join(parent_keys, on=join_col, how="inner")
    return valid, orphans


def _zscore_quarantine(df, col_name, threshold=3.0):
    """Quarantine rows where |z-score| > threshold for col_name (nulls are kept).
    Uses sample std (ddof=1) — identical to Stage 2's chk_zscore_outliers formula."""
    col_d = F.col(col_name).cast(DoubleType())
    stats = df.select(
        F.mean(col_d).alias("mean"),
        F.stddev(col_d).alias("std"),   # PySpark stddev = sample std (ddof=1)
    ).collect()[0]
    mean_val = stats["mean"]
    std_val  = stats["std"]
    if mean_val is None or std_val is None or float(std_val) == 0.0:
        return df, df.filter(F.lit(False)).withColumn("rejection_reason", F.lit("zscore_outlier"))
    z_cond = col_d.isNotNull() & (F.abs((col_d - float(mean_val)) / float(std_val)) > threshold)
    outliers = df.filter(z_cond).withColumn(
        "rejection_reason", F.lit(f"zscore_outlier_{col_name}")
    )
    clean = df.filter(~z_cond)
    return clean, outliers


def _write_table(spark, table, filename, cleaner, fk_steps=None):
    """Read → clean → optional FK checks → cache → write → return clean DF (cached).
    fk_steps: list of (parent_df, join_col, reason) applied in order."""
    src_path = os.path.join(SOURCE_DIR, filename)
    log.info("[%s] reading %s", table, src_path)
    raw_df   = spark.read.csv(src_path, header=True, inferSchema=False)
    in_count = raw_df.count()

    clean_df, quarantine_df = cleaner(raw_df)

    for parent_df, join_col, reason in (fk_steps or []):
        clean_df, orphans = _fk_quarantine(clean_df, parent_df, join_col, reason)
        quarantine_df = quarantine_df.unionByName(orphans)

    clean_df.cache()
    quarantine_df.cache()
    clean_count = clean_df.count()
    q_count     = quarantine_df.count()
    _log_counts(table, in_count, clean_count, q_count)

    _write(clean_df,      os.path.join(CLEANED_DIR,    table), "cleaned")
    _write(quarantine_df, os.path.join(QUARANTINE_DIR, table), "quarantine")
    quarantine_df.unpersist()
    return clean_df  # caller unpersists when no longer needed


# ── Table cleaners ─────────────────────────────────────────────────────────────
# Each returns (clean_df, quarantine_df).  quarantine_df has a rejection_reason
# column appended.  No rows are dropped silently.

def clean_patients(df):
    df = strip_strings(df)
    df = nullify_sentinels(df)

    df = df.withColumn("patient_id", F.upper(F.col("patient_id")))

    # Parse DOB then recalculate age — never trust the stored value.
    df = df.withColumn("__dob", parse_date(F.col("date_of_birth")))
    today = date.today()
    df = (
        df.withColumn("date_of_birth", F.date_format(F.col("__dob"), "yyyy-MM-dd"))
          .withColumn(
              "age",
              F.when(
                  F.col("__dob").isNotNull(),
                  F.floor(F.datediff(F.lit(today), F.col("__dob")) / 365.25).cast("int"),
              ).otherwise(None),
          )
          .drop("__dob")
    )

    df = _redate(df, "registration_date")

    # Normalize gender.
    gender_map = {
        "male": "Male", "m": "Male",
        "female": "Female", "f": "Female", "woman": "Female",
        "non-binary": "Non-binary", "nonbinary": "Non-binary",
        "non binary": "Non-binary",
    }
    g = F.lower(F.col("gender"))
    gender_expr = F.lit(None).cast("string")
    for raw, canonical in gender_map.items():
        gender_expr = F.when(g == raw, canonical).otherwise(gender_expr)
    # Anything recognised but not mapped → "Other"
    gender_expr = F.when(
        gender_expr.isNull() & F.col("gender").isNotNull(), "Other"
    ).otherwise(gender_expr)
    df = df.withColumn("gender", gender_expr)

    # Standardize active boolean.
    df = df.withColumn("active", standardize_bool("active"))

    today_str = date.today().isoformat()
    quarantine = df.filter(
        F.col("patient_id").isNull() | F.col("date_of_birth").isNull()
    ).withColumn(
        "rejection_reason",
        F.when(F.col("patient_id").isNull(), "null patient_id")
         .when(F.col("date_of_birth").isNull(), "unparseable date in date_of_birth"),
    )
    clean = df.filter(
        F.col("patient_id").isNotNull() & F.col("date_of_birth").isNotNull()
    )
    quarantine_future = clean.filter(
        F.col("date_of_birth") > today_str
    ).withColumn("rejection_reason", F.lit("future date_of_birth"))
    clean = clean.filter(F.col("date_of_birth") <= today_str)
    clean, dupes = _dedup_pk(clean, "patient_id")
    return clean, quarantine.unionByName(quarantine_future).unionByName(dupes)


def clean_encounters(df):
    df = strip_strings(df)
    df = nullify_sentinels(df)

    df = df.withColumn("encounter_id", F.upper(F.col("encounter_id")))
    df = df.withColumn("patient_id",   F.upper(F.col("patient_id")))

    df = _redate(df, "encounter_date")

    # Strip all non-numeric characters (currency symbols, commas, " USD" suffix)
    # then cast. Empty string after stripping → null via cast.
    for money_col in ["total_charge_usd", "insurance_paid_usd", "patient_paid_usd"]:
        df = df.withColumn(
            money_col,
            F.regexp_replace(F.col(money_col), r"[^\d.]", "").cast(DoubleType()),
        )

    # Normalize follow_up_required → True / False string.
    df = df.withColumn("follow_up_required", standardize_bool("follow_up_required"))

    quarantine = df.filter(
        F.col("encounter_id").isNull()
        | F.col("patient_id").isNull()
        | F.col("encounter_date").isNull()
    ).withColumn(
        "rejection_reason",
        F.when(F.col("encounter_id").isNull(), "null encounter_id")
         .when(F.col("patient_id").isNull(), "null patient_id")
         .when(F.col("encounter_date").isNull(), "unparseable date in encounter_date"),
    )
    clean = df.filter(
        F.col("encounter_id").isNotNull()
        & F.col("patient_id").isNotNull()
        & F.col("encounter_date").isNotNull()
    )
    clean, dupes = _dedup_pk(clean, "encounter_id")
    return clean, quarantine.unionByName(dupes)


def clean_diagnoses(df):
    df = strip_strings(df)
    df = nullify_sentinels(df)

    df = df.withColumn("diagnosis_id", F.upper(F.col("diagnosis_id")))
    df = df.withColumn("encounter_id", F.upper(F.col("encounter_id")))
    df = df.withColumn("icd10_code",   F.upper(F.col("icd10_code")))

    # onset_date: handles ISO, MM/DD/YY, and long-form month names.
    df = _redate(df, "onset_date")

    for bool_col in ["resolved", "chronic", "confirmed"]:
        df = df.withColumn(bool_col, standardize_bool(bool_col))

    _ICD10_PATTERN = r"^[A-Z][0-9]{2}(\.[A-Z0-9]{1,4})?$"

    quarantine = df.filter(
        F.col("diagnosis_id").isNull()
        | F.col("encounter_id").isNull()
        | F.col("icd10_code").isNull()
    ).withColumn(
        "rejection_reason",
        F.when(F.col("diagnosis_id").isNull(), "null diagnosis_id")
         .when(F.col("encounter_id").isNull(), "null encounter_id")
         .when(F.col("icd10_code").isNull(),   "null icd10_code"),
    )
    clean = df.filter(
        F.col("diagnosis_id").isNotNull()
        & F.col("encounter_id").isNotNull()
        & F.col("icd10_code").isNotNull()
    )
    quarantine_icd = clean.filter(
        ~F.col("icd10_code").rlike(_ICD10_PATTERN)
    ).withColumn("rejection_reason", F.lit("malformed icd10_code"))
    clean = clean.filter(F.col("icd10_code").rlike(_ICD10_PATTERN))
    clean, dupes = _dedup_pk(clean, "diagnosis_id")
    return clean, quarantine.unionByName(quarantine_icd).unionByName(dupes)


def clean_lab_results(df):
    df = strip_strings(df)
    df = nullify_sentinels(df)

    df = df.withColumn("lab_id",       F.upper(F.col("lab_id")))
    df = df.withColumn("encounter_id", F.upper(F.col("encounter_id")))
    df = df.withColumn("patient_id",   F.upper(F.col("patient_id")))

    # Flag rows where result_value had an operator prefix, then strip it.
    df = df.withColumn(
        "result_operator_flag",
        F.when(
            F.col("result_value").isNotNull() & F.col("result_value").rlike(r"^[><=!]"),
            True,
        ).otherwise(False),
    )
    df = df.withColumn(
        "result_value",
        F.regexp_replace(F.col("result_value"), r"^[><=!]+\s*", ""),
    )

    # Parse datetime columns.
    df = _redatetime(df, "collection_datetime")
    df = _redatetime(df, "result_datetime")

    # Normalize flags: 0/1 integers plus mixed boolean strings.
    for flag_col in ["abnormal_flag", "critical_flag"]:
        df = df.withColumn(
            flag_col,
            F.when(F.col(flag_col) == "1", "True")
             .when(F.col(flag_col) == "0", "False")
             .otherwise(standardize_bool(flag_col)),
        )

    df = df.withColumn("fasting", standardize_bool("fasting"))

    quarantine = df.filter(
        F.col("lab_id").isNull()
        | F.col("encounter_id").isNull()
        | F.col("patient_id").isNull()
    ).withColumn(
        "rejection_reason",
        F.when(F.col("lab_id").isNull(),       "null lab_id")
         .when(F.col("encounter_id").isNull(), "null encounter_id")
         .when(F.col("patient_id").isNull(),   "null patient_id"),
    )
    clean = df.filter(
        F.col("lab_id").isNotNull()
        & F.col("encounter_id").isNotNull()
        & F.col("patient_id").isNotNull()
    )
    clean, dupes = _dedup_pk(clean, "lab_id")
    return clean, quarantine.unionByName(dupes)


def clean_medications(df):
    df = strip_strings(df)
    df = nullify_sentinels(df)

    df = df.withColumn("medication_id", F.upper(F.col("medication_id")))
    df = df.withColumn("encounter_id",  F.upper(F.col("encounter_id")))
    df = df.withColumn("patient_id",    F.upper(F.col("patient_id")))

    # Strip trailing punctuation / whitespace from dosage (e.g. "25mg." → "25mg").
    df = df.withColumn("dosage", F.regexp_replace(F.col("dosage"), r"[\s.]+$", ""))

    # Normalize administration route.
    route_expr = F.lower(F.col("route"))
    df = df.withColumn(
        "route",
        F.when(route_expr.isin("p.o.", "po", "oral"), "PO")
         .when(route_expr == "iv",      "IV")
         .when(route_expr == "im",      "IM")
         .when(route_expr == "sq",      "SQ")
         .when(route_expr == "topical", "TOPICAL")
         .when(route_expr == "inhaled", "INHALED")
         .when(F.col("route").isNotNull(), F.upper(F.col("route")))
         .otherwise(None),
    )

    # Cap adherence at 100; flag over-cap rows.
    df = df.withColumn("adherence_rate_pct", F.col("adherence_rate_pct").cast(DoubleType()))
    df = df.withColumn(
        "adherence_overcap_flag",
        F.when(F.col("adherence_rate_pct") > 100.0, True).otherwise(False),
    )
    df = df.withColumn(
        "adherence_rate_pct",
        F.when(F.col("adherence_rate_pct") > 100.0, F.lit(100.0))
         .when(F.col("adherence_rate_pct") < 0.0,   F.lit(0.0))
         .otherwise(F.col("adherence_rate_pct")),
    )

    # Title-case status (handles "active" → "Active", "on hold" → "On Hold").
    df = df.withColumn("status", F.initcap(F.lower(F.col("status"))))

    df = df.withColumn("adverse_reaction", standardize_bool("adverse_reaction"))

    for d_col in ["start_date", "end_date"]:
        df = _redate(df, d_col)

    quarantine = df.filter(
        F.col("medication_id").isNull()
        | F.col("encounter_id").isNull()
        | F.col("patient_id").isNull()
    ).withColumn(
        "rejection_reason",
        F.when(F.col("medication_id").isNull(), "null medication_id")
         .when(F.col("encounter_id").isNull(),  "null encounter_id")
         .when(F.col("patient_id").isNull(),    "null patient_id"),
    )
    clean = df.filter(
        F.col("medication_id").isNotNull()
        & F.col("encounter_id").isNotNull()
        & F.col("patient_id").isNotNull()
    )
    clean, dupes = _dedup_pk(clean, "medication_id")
    clean, zscore_q = _zscore_quarantine(clean, "adherence_rate_pct")
    return clean, quarantine.unionByName(dupes).unionByName(zscore_q)


def clean_providers(df):
    df = strip_strings(df)
    df = nullify_sentinels(df)

    df = df.withColumn("provider_id", F.upper(F.col("provider_id")))
    df = df.withColumn("state",       F.upper(F.col("state")))

    # "Y" / "yes" / "True" → "True"; anything else non-null → "False".
    board = F.lower(F.col("board_certified"))
    df = df.withColumn(
        "board_certified",
        F.when(board.isin("y", "yes", "true", "1"), "True")
         .when(F.col("board_certified").isNotNull(), "False")
         .otherwise(None),
    )

    df = df.withColumn("accepting_patients", standardize_bool("accepting_patients"))

    quarantine = df.filter(
        F.col("provider_id").isNull() | F.col("npi").isNull()
    ).withColumn(
        "rejection_reason",
        F.when(F.col("provider_id").isNull(), "null provider_id")
         .when(F.col("npi").isNull(),         "null npi"),
    )
    clean = df.filter(
        F.col("provider_id").isNotNull() & F.col("npi").isNotNull()
    )
    clean, dupes = _dedup_pk(clean, "provider_id")
    return clean, quarantine.unionByName(dupes)


def clean_vitals(df):
    df = strip_strings(df)
    df = nullify_sentinels(df)

    df = df.withColumn("vitals_id",   F.upper(F.col("vitals_id")))
    df = df.withColumn("encounter_id", F.upper(F.col("encounter_id")))

    df = _redatetime(df, "recorded_datetime")

    # Convert Fahrenheit temperatures (> 50°C threshold) → Celsius.
    df = df.withColumn("temperature_celsius", F.col("temperature_celsius").cast(DoubleType()))
    df = df.withColumn(
        "temperature_celsius",
        F.when(
            F.col("temperature_celsius") > 50,
            F.round((F.col("temperature_celsius") - 32) * 5.0 / 9.0, 2),
        ).otherwise(F.col("temperature_celsius")),
    )

    # nullify_sentinels already replaces "NULL" strings, but some cells may
    # carry mixed case (e.g. "Null") not in the sentinel set — catch them here.
    df = df.withColumn(
        "glasgow_coma_scale",
        F.when(F.upper(F.col("glasgow_coma_scale")) == "NULL", None)
         .otherwise(F.col("glasgow_coma_scale")),
    )

    df = df.withColumn("recorded_by", F.upper(F.col("recorded_by")))

    # Cast numeric vital sign columns so range comparison works correctly.
    for _num_col in ["spo2_pct", "heart_rate_bpm", "systolic_bp_mmhg", "diastolic_bp_mmhg"]:
        df = df.withColumn(_num_col, F.col(_num_col).cast(DoubleType()))

    quarantine = df.filter(
        F.col("vitals_id").isNull() | F.col("encounter_id").isNull()
    ).withColumn(
        "rejection_reason",
        F.when(F.col("vitals_id").isNull(),    "null vitals_id")
         .when(F.col("encounter_id").isNull(), "null encounter_id"),
    )
    clean = df.filter(
        F.col("vitals_id").isNotNull() & F.col("encounter_id").isNotNull()
    )
    clean, dupes = _dedup_pk(clean, "vitals_id")

    # Quarantine rows with physiologically impossible vital sign values.
    # Ranges mirror the Stage 2 contracts so nothing invalid reaches the report.
    _VITAL_RANGES = [
        ("spo2_pct",           0.0, 100.0),
        ("heart_rate_bpm",    20.0, 300.0),
        ("systolic_bp_mmhg",  50.0, 250.0),
        ("diastolic_bp_mmhg", 30.0, 150.0),
    ]
    range_cond = F.lit(False)
    for _col, _lo, _hi in _VITAL_RANGES:
        _c = F.col(_col)
        range_cond = range_cond | (_c.isNotNull() & ((_c < _lo) | (_c > _hi)))

    quarantine_range = clean.filter(range_cond).withColumn(
        "rejection_reason", F.lit("out-of-range vital sign")
    )
    clean = clean.filter(~range_cond)

    # Quarantine z-score outliers (|z| > 3.0) for each numeric vital column.
    # Computed sequentially so each column's z-scores are based on the dataset
    # remaining after previous columns' outliers have already been removed —
    # Stage 2 will see identical data and therefore report 0 z-score failures.
    _ZSCORE_VITALS = [
        "spo2_pct", "heart_rate_bpm", "temperature_celsius",
        "systolic_bp_mmhg", "diastolic_bp_mmhg",
    ]
    all_zscore_q = None
    for _vcol in _ZSCORE_VITALS:
        clean, zq = _zscore_quarantine(clean, _vcol)
        all_zscore_q = zq if all_zscore_q is None else all_zscore_q.unionByName(zq)

    return clean, quarantine.unionByName(dupes).unionByName(quarantine_range).unionByName(all_zscore_q)


# ── Main ───────────────────────────────────────────────────────────────────────
# Processing order respects FK dependencies:
#   Phase 1 — patients, providers  (no FK deps)
#   Phase 2 — encounters           (FK → patients, providers)
#   Phase 3 — diagnoses, lab_results, medications, vitals  (FK → encounters)

def main():
    spark = get_spark()
    log.info("Stage 1 — Clean starting")
    log.info("  source     : %s", SOURCE_DIR)
    log.info("  cleaned    : %s", CLEANED_DIR)
    log.info("  quarantine : %s", QUARANTINE_DIR)

    # ── Phase 1: independent tables ────────────────────────────────────────────
    patients_df  = _write_table(spark, "patients",  "messy_patients.csv",  clean_patients)
    providers_df = _write_table(spark, "providers", "messy_providers.csv", clean_providers)

    # ── Phase 2: encounters (FK → patients, providers) ─────────────────────────
    encounters_df = _write_table(
        spark, "encounters", "messy_encounters.csv", clean_encounters,
        fk_steps=[
            (patients_df,  "patient_id",  "fk_patient_id not in patients"),
            (providers_df, "provider_id", "fk_provider_id not in providers"),
        ],
    )

    # Phase 1 parents no longer needed
    patients_df.unpersist()
    providers_df.unpersist()

    # ── Phase 3: encounter children (FK → encounters) ──────────────────────────
    for table, filename, cleaner in [
        ("diagnoses",   "messy_diagnoses.csv",   clean_diagnoses),
        ("lab_results", "messy_lab_results.csv", clean_lab_results),
        ("medications", "messy_medications.csv", clean_medications),
        ("vitals",      "messy_vitals.csv",       clean_vitals),
    ]:
        df = _write_table(
            spark, table, filename, cleaner,
            fk_steps=[(encounters_df, "encounter_id", "fk_encounter_id not in encounters")],
        )
        df.unpersist()

    encounters_df.unpersist()

    log.info("Stage 1 — Clean complete")
    spark.stop()


if __name__ == "__main__":
    main()
