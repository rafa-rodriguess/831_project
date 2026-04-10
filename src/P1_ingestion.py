from __future__ import annotations

"""
Proposal ingestion module — Stage P1.

Purpose:
- load the 5 required OULAD CSV files into a DuckDB database
- validate row counts, required columns, and basic data contracts
- persist table-level ingestion audit to pipeline_audit.json

Input contract (read-only):
- ../../content/studentVle.csv
- ../../content/studentInfo.csv
- ../../content/studentAssessment.csv
- ../../content/assessments.csv
- ../../content/courses.csv

Output contract:
- ../outputs/data/engagement.duckdb
    Tables created:
      raw_student_vle
      raw_student_info
      raw_student_assessment
      raw_assessments
      raw_courses
- ../outputs/metadata/pipeline_audit.json  (P1 section appended)

Failure policy:
- missing required columns raise immediately
- failed data-contract checks raise immediately
- no silent degradation paths are permitted
"""

import json
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Path bootstrap (mirrors P0 layout)
# ---------------------------------------------------------------------------
SCRIPT_PATH = Path(__file__).resolve()
SRC_DIR     = SCRIPT_PATH.parent
PROPOSAL_ROOT = SRC_DIR.parent
PROJECT_ROOT  = PROPOSAL_ROOT.parent

OUTPUT_DIR  = PROPOSAL_ROOT / "outputs"
DATA_DIR    = OUTPUT_DIR / "data"
METADATA_DIR = OUTPUT_DIR / "metadata"
DUCKDB_PATH = DATA_DIR / "engagement.duckdb"

OULAD_DATA_DIR = PROJECT_ROOT / "content"

STAGE_PREFIX = "P1"
SCRIPT_NAME  = Path(__file__).name
SEED = 42
ENROLLMENT_KEY = ["id_student", "code_module", "code_presentation"]


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
# Table ingestion spec
# ==============================================================

TABLE_SPECS: dict[str, dict] = {
    "raw_student_vle": {
        "csv":      "studentVle.csv",
        "required": ["id_student", "code_module", "code_presentation", "id_site", "date", "sum_click"],
        "checks": [
            ("sum_click >= 0", "sum_click must be non-negative"),
        ],
    },
    "raw_student_info": {
        "csv":      "studentInfo.csv",
        "required": [
            "id_student", "code_module", "code_presentation",
            "gender", "region", "highest_education", "imd_band",
            "age_band", "num_of_prev_attempts", "studied_credits", "final_result",
        ],
        "checks": [
            ("num_of_prev_attempts >= 0", "num_of_prev_attempts must be non-negative"),
            ("studied_credits > 0",       "studied_credits must be positive"),
        ],
    },
    "raw_student_assessment": {
        "csv":      "studentAssessment.csv",
        "required": ["id_student", "id_assessment", "date_submitted", "is_banked", "score"],
        "checks": [
            ("score >= 0 OR score IS NULL",  "score must be >= 0 when not null"),
            ("score <= 100 OR score IS NULL", "score must be <= 100 when not null"),
        ],
    },
    "raw_assessments": {
        "csv":      "assessments.csv",
        "required": ["id_assessment", "code_module", "code_presentation", "assessment_type", "date", "weight"],
        "checks": [],
    },
    "raw_courses": {
        "csv":      "courses.csv",
        "required": ["code_module", "code_presentation", "module_presentation_length"],
        "checks": [
            ("module_presentation_length > 0", "module_presentation_length must be positive"),
        ],
    },
}


# ==============================================================
# Stage P1.1 — Initialize DuckDB
# ==============================================================

def stage_p1_1_init_duckdb() -> Any:
    log_stage_start("1", "Initialize DuckDB")

    import duckdb

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))

    # Verify connectivity
    version = con.execute("SELECT version()").fetchone()[0]
    print_kv("duckdb_path",    DUCKDB_PATH)
    print_kv("duckdb_version", version)

    log_stage_end("1")
    return con


# ==============================================================
# Stage P1.2 — Load and validate each CSV into DuckDB
# ==============================================================

def stage_p1_2_load_tables(con: Any, pd: Any) -> dict:
    log_stage_start("2", "Load OULAD CSVs into DuckDB")

    audit_tables: dict[str, dict] = {}

    for table_name, spec in TABLE_SPECS.items():
        csv_path = OULAD_DATA_DIR / spec["csv"]
        print(f"\n  Loading {spec['csv']} → {table_name}")

        if not csv_path.exists():
            raise FileNotFoundError(f"Required CSV not found: {csv_path}")

        # Load via DuckDB read_csv_auto for efficiency
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(
            f"CREATE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{csv_path}', header=true, sample_size=-1)"
        )

        # Fetch column names from DuckDB
        col_rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
        actual_cols = [r[1] for r in col_rows]

        # Validate required columns
        missing_cols = [c for c in spec["required"] if c not in actual_cols]
        if missing_cols:
            raise KeyError(
                f"{table_name}: missing required columns: {', '.join(missing_cols)}"
            )

        # Row count
        n_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        # Data contract checks
        contract_violations = []
        for condition, description in spec["checks"]:
            n_violations = con.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE NOT ({condition})"
            ).fetchone()[0]
            if n_violations > 0:
                contract_violations.append(
                    f"{description}: {n_violations} violation(s)"
                )

        if contract_violations:
            raise ValueError(
                f"{table_name} data contract violations:\n  "
                + "\n  ".join(contract_violations)
            )

        print_kv("  rows",    n_rows)
        print_kv("  columns", len(actual_cols))
        print_kv("  cols",    ", ".join(actual_cols))
        print_artifact(table_name, f"duckdb://{table_name}")

        audit_tables[table_name] = {
            "csv":              str(csv_path),
            "n_rows":           n_rows,
            "n_columns":        len(actual_cols),
            "columns":          actual_cols,
            "contract_checks":  [c for c, _ in spec["checks"]],
            "violations_found": 0,
        }

    log_stage_end("2")
    return audit_tables


# ==============================================================
# Stage P1.3 — Uniqueness audit on studentInfo enrollment key
# ==============================================================

def stage_p1_3_enrollment_key_audit(con: Any) -> None:
    log_stage_start("3", "Enrollment Key Uniqueness Audit (raw_student_info)")

    total = con.execute("SELECT COUNT(*) FROM raw_student_info").fetchone()[0]
    distinct = con.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT DISTINCT id_student, code_module, code_presentation"
        "  FROM raw_student_info"
        ")"
    ).fetchone()[0]

    print_kv("total_rows",      total)
    print_kv("distinct_keys",   distinct)
    print_kv("duplicates",      total - distinct)

    if total != distinct:
        raise ValueError(
            f"raw_student_info: enrollment key is not unique. "
            f"total={total}, distinct={distinct}, duplicates={total - distinct}"
        )

    print("Enrollment key is unique in raw_student_info. ✓")
    log_stage_end("3")


# ==============================================================
# Stage P1.4 — Quick preview of each table
# ==============================================================

def stage_p1_4_preview_tables(con: Any, pd: Any) -> None:
    log_stage_start("4", "Table Previews")

    for table_name in TABLE_SPECS:
        df_preview = con.execute(f"SELECT * FROM {table_name} LIMIT 3").df()
        print(f"\n  [{table_name}] — first 3 rows:")
        print(df_preview.to_string(index=False))

    log_stage_end("4")


# ==============================================================
# Stage P1.5 — Persist audit
# ==============================================================

def stage_p1_5_persist_audit(audit_tables: dict) -> None:
    log_stage_start("5", "Persist P1 Audit")

    audit = {
        "stage":      "P1",
        "timestamp":  datetime.now(timezone.utc).astimezone().isoformat(),
        "status":     "completed",
        "duckdb_path": str(DUCKDB_PATH),
        "tables":     audit_tables,
    }
    append_to_audit("P1", audit)
    print_artifact("pipeline_audit.json (P1 section)", METADATA_DIR / "pipeline_audit.json")

    log_stage_end("5")


# ==============================================================
# Main entry point
# ==============================================================

def main() -> None:
    import pandas as pd

    print_section("PROPOSAL STAGE P1 — OULAD Ingestion")
    print_kv("started_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    warnings.simplefilter("default")
    pd.set_option("display.max_columns", 200)
    pd.set_option("display.width", 200)

    con = stage_p1_1_init_duckdb()

    try:
        audit_tables = stage_p1_2_load_tables(con, pd)
        stage_p1_3_enrollment_key_audit(con)
        stage_p1_4_preview_tables(con, pd)
        stage_p1_5_persist_audit(audit_tables)
    finally:
        con.close()

    print_section("P1 COMPLETE")
    print_kv("completed_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_kv("duckdb_path",  DUCKDB_PATH)
    print_kv("next_step",    "Run src/P2_panel_builder.py")


if __name__ == "__main__":
    main()
