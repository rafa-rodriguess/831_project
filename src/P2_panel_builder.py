from __future__ import annotations

"""
Proposal panel builder — Stage P2.

Purpose:
- convert studentVle 'date' (days since module start) into week numbers
- aggregate clicks per enrollment × week (total_clicks, active_days)
- build a complete grade of all weeks per enrollment (including zero-click weeks)
- persist intermediate table 'panel_base' in DuckDB
- emit sparsity audit

Input contract (DuckDB tables, created by P1):
- raw_student_vle
- raw_courses

Output contract:
- DuckDB table: panel_base
    Columns: enrollment_id, id_student, code_module, code_presentation,
             week, total_clicks, active_days
- ../outputs/metadata/pipeline_audit.json  (P2 section appended)

Failure policy:
- zero-row panel raises immediately
- week < 0 after filtering raises immediately
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
PROJECT_ROOT  = PROPOSAL_ROOT.parent

OUTPUT_DIR    = PROPOSAL_ROOT / "outputs"
DATA_DIR      = OUTPUT_DIR / "data"
METADATA_DIR  = OUTPUT_DIR / "metadata"
DUCKDB_PATH   = DATA_DIR / "engagement.duckdb"

STAGE_PREFIX  = "P2"
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
# Stage P2.1 — Open DuckDB
# ==============================================================

def stage_p2_1_open_duckdb() -> Any:
    log_stage_start("1", "Open DuckDB")

    import duckdb

    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB not found at {DUCKDB_PATH}. Run P1_ingestion.py first."
        )

    con = duckdb.connect(str(DUCKDB_PATH))
    version = con.execute("SELECT version()").fetchone()[0]
    print_kv("duckdb_path",    DUCKDB_PATH)
    print_kv("duckdb_version", version)

    # Verify required tables
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    for required in ("raw_student_vle", "raw_courses"):
        if required not in tables:
            raise RuntimeError(
                f"Required table '{required}' not found. Run P1_ingestion.py first."
            )

    log_stage_end("1")
    return con


# ==============================================================
# Stage P2.2 — Aggregate clicks per enrollment × week
# ==============================================================

def stage_p2_2_aggregate_weekly_clicks(con: Any) -> int:
    log_stage_start("2", "Aggregate Clicks per Enrollment × Week")

    # Filter out pre-module activity (date < 0) and compute week = date // 7
    # CAST integer division: DuckDB uses // for integer floor division
    con.execute("DROP TABLE IF EXISTS vle_weekly_agg")
    con.execute("""
        CREATE TABLE vle_weekly_agg AS
        SELECT
            id_student,
            code_module,
            code_presentation,
            CAST(date / 7 AS INTEGER)   AS week,
            SUM(sum_click)              AS total_clicks,
            COUNT(DISTINCT date)        AS active_days
        FROM raw_student_vle
        WHERE date >= 0
        GROUP BY id_student, code_module, code_presentation, CAST(date / 7 AS INTEGER)
    """)

    n_rows = con.execute("SELECT COUNT(*) FROM vle_weekly_agg").fetchone()[0]
    n_enrollments = con.execute(
        "SELECT COUNT(DISTINCT id_student || '_' || code_module || '_' || code_presentation)"
        " FROM vle_weekly_agg"
    ).fetchone()[0]

    print_kv("vle_weekly_agg rows",        n_rows)
    print_kv("enrollments with activity",  n_enrollments)

    # Sanity: no negative weeks
    neg_weeks = con.execute("SELECT COUNT(*) FROM vle_weekly_agg WHERE week < 0").fetchone()[0]
    if neg_weeks > 0:
        raise ValueError(f"vle_weekly_agg: {neg_weeks} rows with week < 0 after filtering.")

    print_artifact("vle_weekly_agg", "duckdb://vle_weekly_agg")
    log_stage_end("2")
    return n_enrollments


# ==============================================================
# Stage P2.3 — Build full enrollment × week grade (including zero-click weeks)
# ==============================================================

def stage_p2_3_build_full_grade(con: Any) -> int:
    log_stage_start("3", "Build Full Enrollment × Week Grade")

    # Step 1: derive max_week per enrollment from courses.module_presentation_length
    # max_week = FLOOR(module_presentation_length / 7)
    # Join raw_student_info with raw_courses to get all enrollment × max_week combos
    con.execute("DROP TABLE IF EXISTS enrollment_week_bounds")
    con.execute("""
        CREATE TABLE enrollment_week_bounds AS
        SELECT
            si.id_student,
            si.code_module,
            si.code_presentation,
            CAST(rc.module_presentation_length / 7 AS INTEGER) AS max_week
        FROM raw_student_info si
        JOIN raw_courses rc
          ON si.code_module = rc.code_module
         AND si.code_presentation = rc.code_presentation
    """)

    n_bounds = con.execute("SELECT COUNT(*) FROM enrollment_week_bounds").fetchone()[0]
    print_kv("enrollment_week_bounds rows", n_bounds)

    # Step 2: generate series 0..max_week for each enrollment
    # DuckDB supports generate_series(0, max_week) in a lateral join
    con.execute("DROP TABLE IF EXISTS enrollment_week_grid")
    con.execute("""
        CREATE TABLE enrollment_week_grid AS
        SELECT
            ewb.id_student,
            ewb.code_module,
            ewb.code_presentation,
            gs.week
        FROM enrollment_week_bounds ewb,
             LATERAL (SELECT UNNEST(generate_series(0, ewb.max_week)) AS week) gs
    """)

    n_grid = con.execute("SELECT COUNT(*) FROM enrollment_week_grid").fetchone()[0]
    print_kv("enrollment_week_grid rows (all weeks)", n_grid)
    print_artifact("enrollment_week_grid", "duckdb://enrollment_week_grid")

    log_stage_end("3")
    return n_grid


# ==============================================================
# Stage P2.4 — Left join grid with aggregated activity → panel_base
# ==============================================================

def stage_p2_4_build_panel_base(con: Any) -> int:
    log_stage_start("4", "Build panel_base (grid LEFT JOIN activity)")

    con.execute("DROP TABLE IF EXISTS panel_base")
    con.execute("""
        CREATE TABLE panel_base AS
        SELECT
            g.id_student || '_' || g.code_module || '_' || g.code_presentation
                AS enrollment_id,
            g.id_student,
            g.code_module,
            g.code_presentation,
            g.week,
            COALESCE(a.total_clicks, 0) AS total_clicks,
            COALESCE(a.active_days,  0) AS active_days
        FROM enrollment_week_grid g
        LEFT JOIN vle_weekly_agg a
          ON  g.id_student        = a.id_student
          AND g.code_module       = a.code_module
          AND g.code_presentation = a.code_presentation
          AND g.week              = a.week
        ORDER BY g.id_student, g.code_module, g.code_presentation, g.week
    """)

    n_panel = con.execute("SELECT COUNT(*) FROM panel_base").fetchone()[0]
    n_enrollments = con.execute(
        "SELECT COUNT(DISTINCT enrollment_id) FROM panel_base"
    ).fetchone()[0]
    n_weeks_with_activity = con.execute(
        "SELECT COUNT(*) FROM panel_base WHERE total_clicks > 0"
    ).fetchone()[0]

    if n_panel == 0:
        raise ValueError("panel_base is empty. Check upstream tables.")

    sparsity_pct = round(100.0 * (1 - n_weeks_with_activity / n_panel), 2)

    print_kv("panel_base rows",           n_panel)
    print_kv("distinct enrollments",      n_enrollments)
    print_kv("active weeks",              n_weeks_with_activity)
    print_kv("zero-click weeks",          n_panel - n_weeks_with_activity)
    print_kv("sparsity_pct",              f"{sparsity_pct}%")
    print_artifact("panel_base",          "duckdb://panel_base")

    log_stage_end("4")
    return n_panel


# ==============================================================
# Stage P2.5 — Preview and persist audit
# ==============================================================

def stage_p2_5_preview_and_audit(con: Any, n_panel: int) -> None:
    log_stage_start("5", "Preview and Persist P2 Audit")

    import pandas as pd

    df_preview = con.execute(
        "SELECT * FROM panel_base ORDER BY enrollment_id, week LIMIT 10"
    ).df()
    print("\npanel_base — first 10 rows:")
    print(df_preview.to_string(index=False))

    # Week distribution
    df_week_dist = con.execute("""
        SELECT
            COUNT(DISTINCT enrollment_id) AS n_enrollments,
            MIN(week) AS min_week,
            MAX(week) AS max_week,
            ROUND(AVG(week), 2) AS avg_week,
            COUNT(*) AS total_rows
        FROM panel_base
    """).df()
    print("\npanel_base distribution:")
    print(df_week_dist.to_string(index=False))

    audit = {
        "stage":           "P2",
        "timestamp":       datetime.now(timezone.utc).astimezone().isoformat(),
        "status":          "completed",
        "panel_base_rows": n_panel,
        "notes":           "Full enrollment × week grid with zero-click weeks included.",
    }
    append_to_audit("P2", audit)
    print_artifact("pipeline_audit.json (P2 section)", METADATA_DIR / "pipeline_audit.json")

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

    print_section("PROPOSAL STAGE P2 — Panel Builder")
    print_kv("started_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    con = stage_p2_1_open_duckdb()

    try:
        stage_p2_2_aggregate_weekly_clicks(con)
        stage_p2_3_build_full_grade(con)
        n_panel = stage_p2_4_build_panel_base(con)
        stage_p2_5_preview_and_audit(con, n_panel)
    finally:
        con.close()

    print_section("P2 COMPLETE")
    print_kv("completed_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_kv("next_step",    "Run src/P3_indicators.py")


if __name__ == "__main__":
    main()
