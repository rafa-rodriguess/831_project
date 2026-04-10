from __future__ import annotations

"""
Proposal demographics join — Stage P5.

Purpose:
- join panel_with_assessment with studentInfo demographics
- normalize final_result and imd_band string values
- validate no orphan enrollments
- persist final pre-export table 'panel_with_demographics' in DuckDB

Input contract (DuckDB tables):
- panel_with_assessment  (from P4)
- raw_student_info

Output contract:
- DuckDB table: panel_with_demographics
    New columns added to panel_with_assessment:
      age_band, gender, highest_education, imd_band,
      num_of_prev_attempts, studied_credits, final_result
- ../outputs/metadata/pipeline_audit.json  (P5 section appended)

Failure policy:
- orphan enrollments (panel row without studentInfo match): logged, not silently dropped
- invalid final_result values: raise immediately
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

STAGE_PREFIX  = "P5"
SCRIPT_NAME   = Path(__file__).name

VALID_FINAL_RESULTS = {"Pass", "Fail", "Withdrawn", "Distinction"}


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
# Stage P5.1 — Open DuckDB
# ==============================================================

def stage_p5_1_open_duckdb() -> Any:
    log_stage_start("1", "Open DuckDB")

    import duckdb

    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB not found: {DUCKDB_PATH}.")

    con = duckdb.connect(str(DUCKDB_PATH))
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    for required in ("panel_with_assessment", "raw_student_info"):
        if required not in tables:
            raise RuntimeError(f"Required table '{required}' not found.")

    n = con.execute("SELECT COUNT(*) FROM panel_with_assessment").fetchone()[0]
    print_kv("panel_with_assessment rows", n)
    log_stage_end("1")
    return con


# ==============================================================
# Stage P5.2 — Check final_result values in studentInfo
# ==============================================================

def stage_p5_2_check_final_result(con: Any) -> None:
    log_stage_start("2", "Validate final_result Values")

    result = con.execute("""
        SELECT final_result, COUNT(*) AS n
        FROM raw_student_info
        GROUP BY final_result
        ORDER BY n DESC
    """).df()
    print("\nfinal_result distribution in raw_student_info:")
    print(result.to_string(index=False))

    # Normalize: strip + title-case
    found_values = set(result["final_result"].str.strip().str.title().tolist())
    unexpected = found_values - VALID_FINAL_RESULTS
    if unexpected:
        raise ValueError(
            f"Unexpected final_result values (after strip+title): {unexpected}\n"
            f"Expected: {VALID_FINAL_RESULTS}"
        )

    print("All final_result values are valid. ✓")
    log_stage_end("2")


# ==============================================================
# Stage P5.3 — Join demographics onto panel
# ==============================================================

def stage_p5_3_join_demographics(con: Any) -> tuple[int, int]:
    log_stage_start("3", "Join Demographics")

    con.execute("DROP TABLE IF EXISTS panel_with_demographics")
    con.execute("""
        CREATE TABLE panel_with_demographics AS
        SELECT
            pa.*,
            si.age_band,
            si.gender,
            si.highest_education,
            -- Normalize imd_band: strip whitespace
            TRIM(si.imd_band)          AS imd_band,
            si.num_of_prev_attempts,
            si.studied_credits,
            -- Normalize final_result: strip + title-case handled in Python already validated
            TRIM(si.final_result)      AS final_result
        FROM panel_with_assessment pa
        LEFT JOIN raw_student_info si
          ON  pa.id_student        = si.id_student
          AND pa.code_module       = si.code_module
          AND pa.code_presentation = si.code_presentation
    """)

    n_total = con.execute("SELECT COUNT(*) FROM panel_with_demographics").fetchone()[0]

    # Check for orphan enrollments (no match in studentInfo)
    n_orphan = con.execute(
        "SELECT COUNT(*) FROM panel_with_demographics WHERE final_result IS NULL"
    ).fetchone()[0]

    print_kv("panel_with_demographics rows", n_total)
    print_kv("orphan rows (no studentInfo match)", n_orphan)

    if n_orphan > 0:
        orphan_sample = con.execute("""
            SELECT DISTINCT enrollment_id, id_student, code_module, code_presentation
            FROM panel_with_demographics
            WHERE final_result IS NULL
            LIMIT 5
        """).df()
        print("  Sample orphan enrollments:")
        print(orphan_sample.to_string(index=False))
        print(
            f"  WARNING: {n_orphan} rows have no match in raw_student_info. "
            "They will be retained with NULL demographics."
        )

    print_artifact("panel_with_demographics", "duckdb://panel_with_demographics")
    log_stage_end("3")
    return n_total, n_orphan


# ==============================================================
# Stage P5.4 — Validate demographics columns
# ==============================================================

def stage_p5_4_validate_demographics(con: Any) -> None:
    log_stage_start("4", "Validate Demographics")

    # num_of_prev_attempts >= 0
    bad_attempts = con.execute(
        "SELECT COUNT(*) FROM panel_with_demographics WHERE num_of_prev_attempts < 0"
    ).fetchone()[0]
    if bad_attempts > 0:
        raise ValueError(f"num_of_prev_attempts < 0: {bad_attempts} rows.")

    # studied_credits > 0
    bad_credits = con.execute(
        "SELECT COUNT(*) FROM panel_with_demographics WHERE studied_credits <= 0"
    ).fetchone()[0]
    if bad_credits > 0:
        raise ValueError(f"studied_credits <= 0: {bad_credits} rows.")

    # final_result distribution
    dist = con.execute("""
        SELECT final_result, COUNT(*) AS n,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
        FROM panel_with_demographics
        GROUP BY final_result
        ORDER BY n DESC
    """).df()
    print("\nfinal_result distribution in panel:")
    print(dist.to_string(index=False))

    # imd_band distinct values
    imd_vals = con.execute(
        "SELECT DISTINCT imd_band FROM panel_with_demographics ORDER BY imd_band"
    ).df()
    print("\nimd_band distinct values:")
    print(imd_vals.to_string(index=False))

    print("Demographics validation passed. ✓")
    log_stage_end("4")


# ==============================================================
# Stage P5.5 — Preview and persist audit
# ==============================================================

def stage_p5_5_preview_and_audit(con: Any, n_total: int, n_orphan: int) -> None:
    log_stage_start("5", "Preview and Persist P5 Audit")

    df_preview = con.execute(
        "SELECT * FROM panel_with_demographics ORDER BY enrollment_id, week LIMIT 5"
    ).df()
    print("\npanel_with_demographics — first 5 rows:")
    print(df_preview.to_string(index=False))

    cols = [r[0] for r in con.execute("DESCRIBE panel_with_demographics").fetchall()]
    print_kv("columns", len(cols))
    print_kv("column list", ", ".join(cols))

    audit = {
        "stage":                              "P5",
        "timestamp":                          datetime.now(timezone.utc).astimezone().isoformat(),
        "status":                             "completed",
        "panel_with_demographics_rows":       n_total,
        "orphan_rows":                        n_orphan,
        "columns_added": [
            "age_band", "gender", "highest_education", "imd_band",
            "num_of_prev_attempts", "studied_credits", "final_result",
        ],
    }
    append_to_audit("P5", audit)
    print_artifact("pipeline_audit.json (P5 section)", METADATA_DIR / "pipeline_audit.json")

    log_stage_end("5")


# ==============================================================
# Main entry point
# ==============================================================

def main() -> None:
    import pandas as pd
    import warnings
    warnings.simplefilter("default")
    pd.set_option("display.max_columns", 200)
    pd.set_option("display.width", 200)

    print_section("PROPOSAL STAGE P5 — Demographics Join")
    print_kv("started_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    con = stage_p5_1_open_duckdb()

    try:
        stage_p5_2_check_final_result(con)
        n_total, n_orphan = stage_p5_3_join_demographics(con)
        stage_p5_4_validate_demographics(con)
        stage_p5_5_preview_and_audit(con, n_total, n_orphan)
    finally:
        con.close()

    print_section("P5 COMPLETE")
    print_kv("completed_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_kv("next_step",    "Run src/P6_export.py")


if __name__ == "__main__":
    main()
