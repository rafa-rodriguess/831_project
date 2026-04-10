from __future__ import annotations

"""
Proposal assessment join — Stage P4.

Purpose:
- join student assessments with assessment deadlines
- convert submission dates to week numbers
- align assessments to panel weeks
- compute assessment_score, submission_timeliness, has_assessment_this_week
- persist intermediate table 'panel_with_assessment' in DuckDB

Input contract (DuckDB tables):
- panel_indicators    (from P3)
- raw_student_assessment
- raw_assessments

Output contract:
- DuckDB table: panel_with_assessment
    New columns added to panel_indicators:
      assessment_score, submission_timeliness, has_assessment_this_week
- ../outputs/metadata/pipeline_audit.json  (P4 section appended)

Failure policy:
- has_assessment_this_week must be 0 or 1 everywhere
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

STAGE_PREFIX  = "P4"
SCRIPT_NAME   = Path(__file__).name


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
# Stage P4.1 — Open DuckDB
# ==============================================================

def stage_p4_1_open_duckdb() -> Any:
    log_stage_start("1", "Open DuckDB")

    import duckdb

    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB not found: {DUCKDB_PATH}.")

    con = duckdb.connect(str(DUCKDB_PATH))
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    for required in ("panel_indicators", "raw_student_assessment", "raw_assessments"):
        if required not in tables:
            raise RuntimeError(f"Required table '{required}' not found.")

    n = con.execute("SELECT COUNT(*) FROM panel_indicators").fetchone()[0]
    print_kv("panel_indicators rows", n)
    log_stage_end("1")
    return con


# ==============================================================
# Stage P4.2 — Build assessment_weekly view
# ==============================================================

def stage_p4_2_build_assessment_weekly(con: Any) -> None:
    log_stage_start("2", "Build Assessment Weekly Table")

    # Join student assessments with assessment deadlines
    # week_submitted = FLOOR(date_submitted / 7)
    # submission_timeliness = deadline_day - date_submitted (positive = submitted early)
    # Where multiple assessments per enrollment×week: keep the one with highest weight
    con.execute("DROP TABLE IF EXISTS assessment_weekly")
    con.execute("""
        CREATE TABLE assessment_weekly AS
        WITH joined AS (
            SELECT
                sa.id_student,
                a.code_module,
                a.code_presentation,
                sa.id_assessment,
                sa.date_submitted,
                sa.score,
                a.date        AS deadline_day,
                a.weight,
                a.assessment_type,
                CAST(sa.date_submitted / 7 AS INTEGER) AS week_submitted,
                (a.date - sa.date_submitted)          AS submission_timeliness
            FROM raw_student_assessment sa
            JOIN raw_assessments a ON sa.id_assessment = a.id_assessment
            WHERE sa.date_submitted IS NOT NULL
              AND sa.score IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY id_student, code_module, code_presentation, week_submitted
                    ORDER BY weight DESC, id_assessment ASC
                ) AS rn
            FROM joined
        )
        SELECT
            id_student,
            code_module,
            code_presentation,
            week_submitted,
            score           AS assessment_score,
            submission_timeliness
        FROM ranked
        WHERE rn = 1
    """)

    n = con.execute("SELECT COUNT(*) FROM assessment_weekly").fetchone()[0]
    print_kv("assessment_weekly rows", n)
    print_artifact("assessment_weekly", "duckdb://assessment_weekly")

    log_stage_end("2")


# ==============================================================
# Stage P4.3 — Join assessment_weekly onto panel_indicators
# ==============================================================

def stage_p4_3_join_assessments(con: Any) -> int:
    log_stage_start("3", "Join Assessments onto Panel")

    con.execute("DROP TABLE IF EXISTS panel_with_assessment")
    con.execute("""
        CREATE TABLE panel_with_assessment AS
        SELECT
            pi.*,
            aw.assessment_score,
            aw.submission_timeliness,
            CASE WHEN aw.id_student IS NOT NULL THEN 1 ELSE 0 END
                AS has_assessment_this_week
        FROM panel_indicators pi
        LEFT JOIN assessment_weekly aw
          ON  pi.id_student        = aw.id_student
          AND pi.code_module       = aw.code_module
          AND pi.code_presentation = aw.code_presentation
          AND pi.week              = aw.week_submitted
    """)

    n = con.execute("SELECT COUNT(*) FROM panel_with_assessment").fetchone()[0]

    # Contract: has_assessment_this_week ∈ {0, 1}
    bad = con.execute(
        "SELECT COUNT(*) FROM panel_with_assessment WHERE has_assessment_this_week NOT IN (0, 1)"
    ).fetchone()[0]
    if bad > 0:
        raise ValueError(f"has_assessment_this_week has {bad} invalid values.")

    n_with = con.execute(
        "SELECT COUNT(*) FROM panel_with_assessment WHERE has_assessment_this_week = 1"
    ).fetchone()[0]

    print_kv("panel_with_assessment rows",  n)
    print_kv("weeks with assessment",        n_with)
    print_kv("weeks without assessment",     n - n_with)
    print_kv("pct weeks with assessment",    f"{100 * n_with / n:.2f}%")
    print_artifact("panel_with_assessment", "duckdb://panel_with_assessment")

    log_stage_end("3")
    return n


# ==============================================================
# Stage P4.4 — Preview and persist audit
# ==============================================================

def stage_p4_4_preview_and_audit(con: Any, n_panel: int) -> None:
    log_stage_start("4", "Preview and Persist P4 Audit")

    # Show a few rows that have assessments
    df_preview = con.execute("""
        SELECT *
        FROM panel_with_assessment
        WHERE has_assessment_this_week = 1
        ORDER BY enrollment_id, week
        LIMIT 6
    """).df()
    print("\npanel_with_assessment — rows with assessments (first 6):")
    print(df_preview.to_string(index=False))

    # NaN counts for assessment columns
    for col in ("assessment_score", "submission_timeliness"):
        n_null = con.execute(
            f"SELECT COUNT(*) FROM panel_with_assessment WHERE {col} IS NULL"
        ).fetchone()[0]
        print_kv(f"  NaN in {col}", n_null)

    audit = {
        "stage":                         "P4",
        "timestamp":                     datetime.now(timezone.utc).astimezone().isoformat(),
        "status":                        "completed",
        "panel_with_assessment_rows":    n_panel,
        "columns_added": [
            "assessment_score", "submission_timeliness", "has_assessment_this_week",
        ],
        "notes": (
            "assessment_score and submission_timeliness are NULL for weeks "
            "without a submitted assessment."
        ),
    }
    append_to_audit("P4", audit)
    print_artifact("pipeline_audit.json (P4 section)", METADATA_DIR / "pipeline_audit.json")

    log_stage_end("4")


# ==============================================================
# Main entry point
# ==============================================================

def main() -> None:
    import pandas as pd
    import warnings
    warnings.simplefilter("default")
    pd.set_option("display.max_columns", 200)
    pd.set_option("display.width", 200)

    print_section("PROPOSAL STAGE P4 — Assessment Join")
    print_kv("started_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    con = stage_p4_1_open_duckdb()

    try:
        stage_p4_2_build_assessment_weekly(con)
        n_panel = stage_p4_3_join_assessments(con)
        stage_p4_4_preview_and_audit(con, n_panel)
    finally:
        con.close()

    print_section("P4 COMPLETE")
    print_kv("completed_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_kv("next_step",    "Run src/P5_demographics_join.py")


if __name__ == "__main__":
    main()
