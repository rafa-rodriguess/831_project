from __future__ import annotations

"""
Proposal export — Stage P6.

Purpose:
- select the final 22 columns from panel_with_demographics
- validate all data contracts before export
- generate column_schema.json
- export engagement_panel_weekly.csv
- finalize pipeline_audit.json with full run summary

Input contract (DuckDB table):
- panel_with_demographics  (from P5)

Output contract:
- ../outputs/engagement_panel_weekly.csv   ← FINAL DELIVERABLE
- ../outputs/metadata/column_schema.json
- ../outputs/metadata/pipeline_audit.json  (P6 section + summary appended)

Failure policy:
- any NULL in engagement_state or total_clicks: raise immediately
- any duplicate rows: raise immediately
- invalid engagement_state categories: raise immediately
- invalid final_result categories: raise immediately
- no silent degradation paths are permitted
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
SCRIPT_PATH   = Path(__file__).resolve()
SRC_DIR       = SCRIPT_PATH.parent
PROPOSAL_ROOT = SRC_DIR.parent

OUTPUT_DIR    = PROPOSAL_ROOT / "outputs"
DATA_DIR      = OUTPUT_DIR / "data"
METADATA_DIR  = OUTPUT_DIR / "metadata"
DUCKDB_PATH   = DATA_DIR / "engagement.duckdb"

CSV_OUTPUT_PATH = OUTPUT_DIR / "engagement_panel_weekly.csv"

STAGE_PREFIX  = "P6"
SCRIPT_NAME   = Path(__file__).name

VALID_ENGAGEMENT_STATES = {"low", "medium", "high"}
VALID_FINAL_RESULTS     = {"Pass", "Fail", "Withdrawn", "Distinction"}

# Final column order (matches plano.md schema)
FINAL_COLUMNS = [
    "enrollment_id",
    "id_student",
    "code_module",
    "code_presentation",
    "week",
    "total_clicks",
    "active_days",
    "log_clicks",
    "click_intensity",
    "recency",
    "streak",
    "cumulative_clicks",
    "assessment_score",
    "submission_timeliness",
    "has_assessment_this_week",
    "age_band",
    "gender",
    "highest_education",
    "imd_band",
    "num_of_prev_attempts",
    "studied_credits",
    "final_result",
    "engagement_state",
]


# ==============================================================
# Logging helpers
# ==============================================================

def log_stage_start(number: str, title: str) -> None:
    print(f"[START] {STAGE_PREFIX}.{number} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("# " + "=" * 62)
    print(f"# {STAGE_PREFIX}.{number} - {title}")
    print("# " + "=" * 62)


def log_stage_end(number: str) -> None:
    print(f"[END] {STAGE_PREFIX}.{number} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def print_kv(label: str, value: Any) -> None:
    print(f"- {label}: {value}")


def print_artifact(label: str, location: Any) -> None:
    print(f"ARTIFACT | {label} | {location}")


def print_section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def save_json(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def append_to_audit(stage_key: str, payload: dict) -> None:
    audit_path = METADATA_DIR / "pipeline_audit.json"
    existing: dict = {}
    if audit_path.exists():
        try:
            existing = json.loads(audit_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
    existing[stage_key] = payload
    save_json(existing, audit_path)


# ==============================================================
# Stage P6.1 — Open DuckDB and load final table
# ==============================================================

def stage_p6_1_open_and_load(pd: Any) -> tuple[Any, Any]:
    log_stage_start("1", "Open DuckDB and Load Final Table")

    import duckdb

    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB not found: {DUCKDB_PATH}.")

    con = duckdb.connect(str(DUCKDB_PATH))
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "panel_with_demographics" not in tables:
        raise RuntimeError("Table 'panel_with_demographics' not found. Run P5 first.")

    # Verify all final columns exist before selecting
    existing_cols = [r[0] for r in con.execute("DESCRIBE panel_with_demographics").fetchall()]
    missing = [c for c in FINAL_COLUMNS if c not in existing_cols]
    if missing:
        raise KeyError(
            f"panel_with_demographics is missing columns required for export: "
            + ", ".join(missing)
        )

    cols_sql = ", ".join(FINAL_COLUMNS)
    df = con.execute(
        f"SELECT {cols_sql} FROM panel_with_demographics "
        f"ORDER BY enrollment_id ASC, week ASC"
    ).df()

    print_kv("rows loaded", len(df))
    print_kv("columns",     len(df.columns))
    log_stage_end("1")
    return con, df


# ==============================================================
# Stage P6.2 — Mandatory validations
# ==============================================================

def stage_p6_2_validate(df: Any) -> None:
    log_stage_start("2", "Mandatory Validations")

    import numpy as np

    errors = []

    # 1. No NULL enrollment_id
    n_null_eid = df["enrollment_id"].isna().sum()
    if n_null_eid > 0:
        errors.append(f"enrollment_id has {n_null_eid} NULL values.")
    else:
        print_kv("enrollment_id nulls",   0)

    # 2. week >= 0
    n_neg_week = (df["week"] < 0).sum()
    if n_neg_week > 0:
        errors.append(f"week < 0: {n_neg_week} rows.")
    else:
        print_kv("week < 0",              0)

    # 3. total_clicks >= 0 and not null
    n_null_tc = df["total_clicks"].isna().sum()
    if n_null_tc > 0:
        errors.append(f"total_clicks has {n_null_tc} NULL values.")
    n_neg_tc = (df["total_clicks"] < 0).sum()
    if n_neg_tc > 0:
        errors.append(f"total_clicks < 0: {n_neg_tc} rows.")
    print_kv("total_clicks nulls",        n_null_tc)

    # 4. engagement_state: no nulls, valid categories
    n_null_es = df["engagement_state"].isna().sum()
    if n_null_es > 0:
        errors.append(f"engagement_state has {n_null_es} NULL values. BLOCKING.")
    bad_es = set(df["engagement_state"].dropna().unique()) - VALID_ENGAGEMENT_STATES
    if bad_es:
        errors.append(f"engagement_state has invalid values: {bad_es}.")
    print_kv("engagement_state nulls",    n_null_es)

    # 5. final_result: valid categories (NULLs allowed for orphans but we know there are none)
    bad_fr = set(df["final_result"].dropna().unique()) - VALID_FINAL_RESULTS
    if bad_fr:
        errors.append(f"final_result has invalid values: {bad_fr}.")
    print_kv("final_result invalid cats", len(bad_fr))

    # 6. No fully duplicate rows
    n_dup = df.duplicated().sum()
    if n_dup > 0:
        errors.append(f"Duplicate rows: {n_dup}.")
    print_kv("duplicate rows",             n_dup)

    # 7. No duplicate enrollment_id + week (logical key)
    n_key_dup = df.duplicated(subset=["enrollment_id", "week"]).sum()
    if n_key_dup > 0:
        errors.append(f"Duplicate (enrollment_id, week) pairs: {n_key_dup}.")
    print_kv("(enrollment_id, week) dups", n_key_dup)

    if errors:
        raise ValueError("P6 validation failed:\n  " + "\n  ".join(errors))

    print("\nAll validations passed. ✓")
    log_stage_end("2")


# ==============================================================
# Stage P6.3 — NaN report per column
# ==============================================================

def stage_p6_3_nan_report(df: Any) -> dict:
    log_stage_start("3", "NaN Report per Column")

    n_total = len(df)
    nan_report = {}
    for col in df.columns:
        n_null = int(df[col].isna().sum())
        pct = round(100.0 * n_null / n_total, 2) if n_total > 0 else 0.0
        nan_report[col] = {"n_null": n_null, "pct_null": pct}
        if n_null > 0:
            print_kv(f"  {col}", f"{n_null} nulls ({pct}%)")

    blocking_nulls = ["engagement_state", "total_clicks", "enrollment_id"]
    for col in blocking_nulls:
        if nan_report.get(col, {}).get("n_null", 0) > 0:
            raise ValueError(f"BLOCKING: column '{col}' has NULLs.")

    print("\nNaN report complete. No blocking NULLs found. ✓")
    log_stage_end("3")
    return nan_report


# ==============================================================
# Stage P6.4 — Generate column_schema.json
# ==============================================================

def stage_p6_4_column_schema(df: Any, nan_report: dict) -> None:
    log_stage_start("4", "Generate column_schema.json")

    schema = {}
    for col in df.columns:
        col_data = df[col]
        dtype_str = str(col_data.dtype)
        is_numeric = col_data.dtype.kind in ("i", "u", "f")
        is_categorical = col_data.dtype == object or col_data.dtype.name == "string"

        entry: dict = {
            "dtype":   dtype_str,
            "n_null":  nan_report.get(col, {}).get("n_null", 0),
            "pct_null": nan_report.get(col, {}).get("pct_null", 0.0),
        }

        if is_numeric:
            non_null = col_data.dropna()
            if len(non_null) > 0:
                entry["min"]  = float(non_null.min())
                entry["max"]  = float(non_null.max())
                entry["mean"] = round(float(non_null.mean()), 4)
        elif is_categorical:
            n_distinct = int(col_data.nunique(dropna=True))
            entry["n_distinct"] = n_distinct
            if n_distinct <= 20:
                entry["distinct_values"] = sorted(
                    [str(v) for v in col_data.dropna().unique().tolist()]
                )

        schema[col] = entry

    schema_path = METADATA_DIR / "column_schema.json"
    save_json(schema, schema_path)
    print_kv("columns documented",  len(schema))
    print_artifact("column_schema.json", schema_path)

    log_stage_end("4")


# ==============================================================
# Stage P6.5 — Export final CSV
# ==============================================================

def stage_p6_5_export_csv(df: Any) -> None:
    log_stage_start("5", "Export engagement_panel_weekly.csv")

    df.to_csv(CSV_OUTPUT_PATH, index=False, encoding="utf-8")

    file_size_mb = round(CSV_OUTPUT_PATH.stat().st_size / (1024 * 1024), 2)
    print_kv("output path",      CSV_OUTPUT_PATH)
    print_kv("rows",             len(df))
    print_kv("columns",          len(df.columns))
    print_kv("file size",        f"{file_size_mb} MB")

    # Spot-check: read back first row
    import pandas as pd
    df_check = pd.read_csv(CSV_OUTPUT_PATH, nrows=1)
    assert list(df_check.columns) == FINAL_COLUMNS, (
        f"Column mismatch in written CSV: {list(df_check.columns)}"
    )
    print("Read-back column check passed. ✓")
    print_artifact("engagement_panel_weekly.csv", CSV_OUTPUT_PATH)

    log_stage_end("5")


# ==============================================================
# Stage P6.6 — Final summary and audit
# ==============================================================

def stage_p6_6_final_audit(df: Any, con: Any) -> None:
    log_stage_start("6", "Final Summary and Audit")

    n_total       = len(df)
    n_enrollments = int(df["enrollment_id"].nunique())
    n_weeks       = int(df["week"].nunique())

    engagement_dist = (
        df["engagement_state"].value_counts(normalize=True)
        .mul(100).round(2).to_dict()
    )
    result_dist = (
        df["final_result"].value_counts(normalize=True)
        .mul(100).round(2).to_dict()
    )

    print_kv("total rows",            n_total)
    print_kv("unique enrollments",    n_enrollments)
    print_kv("unique weeks",          n_weeks)
    print_kv("engagement_state dist", engagement_dist)
    print_kv("final_result dist",     result_dist)

    audit = {
        "stage":                  "P6",
        "timestamp":              datetime.now(timezone.utc).astimezone().isoformat(),
        "status":                 "completed",
        "output_csv":             str(CSV_OUTPUT_PATH),
        "total_rows":             n_total,
        "unique_enrollments":     n_enrollments,
        "unique_weeks":           n_weeks,
        "engagement_state_dist":  engagement_dist,
        "final_result_dist":      result_dist,
        "columns":                FINAL_COLUMNS,
    }
    append_to_audit("P6", audit)

    # Top-level summary
    summary = {
        "pipeline_status":    "COMPLETE",
        "completed_at":       datetime.now(timezone.utc).astimezone().isoformat(),
        "final_output":       str(CSV_OUTPUT_PATH),
        "total_rows":         n_total,
        "unique_enrollments": n_enrollments,
    }
    append_to_audit("SUMMARY", summary)

    print_artifact("pipeline_audit.json (complete)", METADATA_DIR / "pipeline_audit.json")
    log_stage_end("6")


# ==============================================================
# Main entry point
# ==============================================================

def main() -> None:
    import pandas as pd
    import warnings
    warnings.simplefilter("default")
    pd.set_option("display.max_columns", 200)
    pd.set_option("display.width", 200)

    print_section("PROPOSAL STAGE P6 — Export")
    print_kv("started_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    con, df = stage_p6_1_open_and_load(pd)

    try:
        stage_p6_2_validate(df)
        nan_report = stage_p6_3_nan_report(df)
        stage_p6_4_column_schema(df, nan_report)
        stage_p6_5_export_csv(df)
        stage_p6_6_final_audit(df, con)
    finally:
        con.close()

    print_section("P6 COMPLETE — PIPELINE DONE")
    print_kv("completed_at",  datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_kv("output",        CSV_OUTPUT_PATH)
    print_kv("rows",          len(df))


if __name__ == "__main__":
    main()
