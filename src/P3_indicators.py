from __future__ import annotations

"""
Proposal engagement indicators — Stage P3.

Purpose:
- derive behavioral engagement indicators from panel_base:
    log_clicks, click_intensity, cumulative_clicks,
    recency, streak, engagement_state
- persist intermediate table 'panel_indicators' in DuckDB
- validate engagement_state distribution

Input contract (DuckDB table, created by P2):
- panel_base

Output contract:
- DuckDB table: panel_indicators
    New columns added to panel_base:
      log_clicks, click_intensity, cumulative_clicks,
      recency, streak, engagement_state
- ../outputs/metadata/pipeline_audit.json  (P3 section appended)

Failure policy:
- any NaN in engagement_state raises immediately
- any engagement_state category below 5% raises a warning (not fatal)
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

STAGE_PREFIX  = "P3"
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
# Stage P3.1 — Open DuckDB
# ==============================================================

def stage_p3_1_open_duckdb() -> Any:
    log_stage_start("1", "Open DuckDB")

    import duckdb

    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB not found: {DUCKDB_PATH}. Run P1 and P2 first.")

    con = duckdb.connect(str(DUCKDB_PATH))
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "panel_base" not in tables:
        raise RuntimeError("Table 'panel_base' not found. Run P2_panel_builder.py first.")

    n = con.execute("SELECT COUNT(*) FROM panel_base").fetchone()[0]
    print_kv("panel_base rows", n)
    log_stage_end("1")
    return con


# ==============================================================
# Stage P3.2 — log_clicks and click_intensity
# ==============================================================

def stage_p3_2_basic_indicators(con: Any) -> None:
    log_stage_start("2", "log_clicks and click_intensity")

    # click_intensity: total_clicks / MAX(total_clicks) within module×presentation×week
    # Uses window function — max within context, not global max
    con.execute("DROP TABLE IF EXISTS panel_basic")
    con.execute("""
        CREATE TABLE panel_basic AS
        SELECT
            *,
            LN(total_clicks + 1.0)  AS log_clicks,
            CASE
                WHEN MAX(total_clicks) OVER (
                    PARTITION BY code_module, code_presentation, week
                ) = 0 THEN 0.0
                ELSE total_clicks * 1.0 / MAX(total_clicks) OVER (
                    PARTITION BY code_module, code_presentation, week
                )
            END AS click_intensity
        FROM panel_base
    """)

    n = con.execute("SELECT COUNT(*) FROM panel_basic").fetchone()[0]
    print_kv("panel_basic rows", n)

    # Spot-check: verify log_clicks range
    stats = con.execute("""
        SELECT
            MIN(log_clicks) AS min_log,
            MAX(log_clicks) AS max_log,
            MIN(click_intensity) AS min_intensity,
            MAX(click_intensity) AS max_intensity
        FROM panel_basic
    """).fetchone()
    print_kv("log_clicks   [min, max]",   f"[{stats[0]:.4f}, {stats[1]:.4f}]")
    print_kv("click_intensity [min, max]", f"[{stats[2]:.4f}, {stats[3]:.4f}]")

    log_stage_end("2")


# ==============================================================
# Stage P3.3 — cumulative_clicks
# ==============================================================

def stage_p3_3_cumulative_clicks(con: Any) -> None:
    log_stage_start("3", "cumulative_clicks")

    con.execute("DROP TABLE IF EXISTS panel_cumulative")
    con.execute("""
        CREATE TABLE panel_cumulative AS
        SELECT
            *,
            SUM(total_clicks) OVER (
                PARTITION BY enrollment_id
                ORDER BY week
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_clicks
        FROM panel_basic
    """)

    n = con.execute("SELECT COUNT(*) FROM panel_cumulative").fetchone()[0]
    print_kv("panel_cumulative rows", n)

    max_cum = con.execute("SELECT MAX(cumulative_clicks) FROM panel_cumulative").fetchone()[0]
    print_kv("max cumulative_clicks", max_cum)

    log_stage_end("3")


# ==============================================================
# Stage P3.4 — recency (weeks since last active week)
# ==============================================================

def stage_p3_4_recency(con: Any) -> None:
    log_stage_start("4", "recency")

    # recency = week - last_active_week
    # where last_active_week = most recent week (including current) where total_clicks > 0
    # If current week is active: recency = 0
    # If no prior active week: recency = week + 1 (full duration since start)
    con.execute("DROP TABLE IF EXISTS panel_recency")
    con.execute("""
        CREATE TABLE panel_recency AS
        SELECT
            *,
            week - COALESCE(
                MAX(CASE WHEN total_clicks > 0 THEN week ELSE NULL END) OVER (
                    PARTITION BY enrollment_id
                    ORDER BY week
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                -1
            ) AS recency
        FROM panel_cumulative
    """)

    # Verify: recency >= 0 always
    neg_recency = con.execute(
        "SELECT COUNT(*) FROM panel_recency WHERE recency < 0"
    ).fetchone()[0]
    if neg_recency > 0:
        raise ValueError(f"recency has {neg_recency} negative values — check logic.")

    stats = con.execute("""
        SELECT MIN(recency) AS min_r, MAX(recency) AS max_r, ROUND(AVG(recency),2) AS avg_r
        FROM panel_recency
    """).fetchone()
    print_kv("recency [min, max, avg]", f"[{stats[0]}, {stats[1]}, {stats[2]}]")

    log_stage_end("4")


# ==============================================================
# Stage P3.5 — streak (consecutive active weeks)
# ==============================================================

def stage_p3_5_streak(con: Any) -> None:
    log_stage_start("5", "streak")

    # Gaps-and-islands: streak resets to 0 on inactive weeks.
    # group_id = week - ROW_NUMBER() over active weeks only
    # All rows in same island share the same group_id
    # streak = count of rows in island up to and including current row
    # For inactive weeks: streak = 0
    con.execute("DROP TABLE IF EXISTS panel_streak")
    con.execute("""
        CREATE TABLE panel_streak AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY enrollment_id
                    ORDER BY week
                ) AS rn_all,
                CASE WHEN total_clicks > 0
                     THEN ROW_NUMBER() OVER (
                            PARTITION BY enrollment_id,
                                         CASE WHEN total_clicks > 0 THEN 1 ELSE 0 END
                            ORDER BY week
                          )
                     ELSE NULL
                END AS rn_active
            FROM panel_recency
        ),
        with_island AS (
            SELECT
                *,
                CASE WHEN total_clicks > 0
                     THEN rn_all - rn_active
                     ELSE NULL
                END AS island_id
            FROM ranked
        )
        SELECT
            enrollment_id,
            id_student,
            code_module,
            code_presentation,
            week,
            total_clicks,
            active_days,
            log_clicks,
            click_intensity,
            cumulative_clicks,
            recency,
            CASE WHEN total_clicks > 0
                 THEN COUNT(*) OVER (
                        PARTITION BY enrollment_id, island_id
                        ORDER BY week
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                      )
                 ELSE 0
            END AS streak
        FROM with_island
    """)

    stats = con.execute("""
        SELECT MIN(streak) AS min_s, MAX(streak) AS max_s, ROUND(AVG(streak),2) AS avg_s
        FROM panel_streak
    """).fetchone()
    print_kv("streak [min, max, avg]", f"[{stats[0]}, {stats[1]}, {stats[2]}]")

    neg_streak = con.execute("SELECT COUNT(*) FROM panel_streak WHERE streak < 0").fetchone()[0]
    if neg_streak > 0:
        raise ValueError(f"streak has {neg_streak} negative values.")

    log_stage_end("5")


# ==============================================================
# Stage P3.6 — engagement_state discretization
# ==============================================================

def stage_p3_6_engagement_state(con: Any) -> None:
    log_stage_start("6", "engagement_state Discretization")

    # Rule:
    #   total_clicks == 0 → 'low'  (directly, excluded from tertile calc)
    #   non-zero rows: compute tertile of log_clicks within module×presentation×week
    #     tertile 1 (bottom) → 'low', tertile 2 (middle) → 'medium', tertile 3 (top) → 'high'
    # NTILE(3) on log_clicks for non-zero rows, partitioned by module×presentation×week

    con.execute("DROP TABLE IF EXISTS panel_indicators")
    con.execute("""
        CREATE TABLE panel_indicators AS
        WITH nonzero_tertiles AS (
            SELECT
                enrollment_id,
                week,
                NTILE(3) OVER (
                    PARTITION BY code_module, code_presentation, week
                    ORDER BY log_clicks
                ) AS tertile
            FROM panel_streak
            WHERE total_clicks > 0
        )
        SELECT
            ps.*,
            CASE
                WHEN ps.total_clicks = 0 THEN 'low'
                WHEN nt.tertile = 1      THEN 'low'
                WHEN nt.tertile = 2      THEN 'medium'
                WHEN nt.tertile = 3      THEN 'high'
                ELSE NULL
            END AS engagement_state
        FROM panel_streak ps
        LEFT JOIN nonzero_tertiles nt
          ON ps.enrollment_id = nt.enrollment_id
         AND ps.week          = nt.week
    """)

    # Validate: no nulls in engagement_state
    n_null = con.execute(
        "SELECT COUNT(*) FROM panel_indicators WHERE engagement_state IS NULL"
    ).fetchone()[0]
    if n_null > 0:
        raise ValueError(
            f"engagement_state has {n_null} NULL values. "
            "Check NTILE join for non-zero rows."
        )

    # Distribution check
    dist = con.execute("""
        SELECT
            engagement_state,
            COUNT(*) AS n,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM panel_indicators
        GROUP BY engagement_state
        ORDER BY engagement_state
    """).df()
    print("\nengagement_state distribution:")
    print(dist.to_string(index=False))

    for _, row in dist.iterrows():
        if float(row["pct"]) < 5.0:
            print(
                f"  WARNING: engagement_state='{row['engagement_state']}' "
                f"is below 5% ({row['pct']}%). Consider reviewing discretization."
            )

    log_stage_end("6")


# ==============================================================
# Stage P3.7 — Preview and persist audit
# ==============================================================

def stage_p3_7_preview_and_audit(con: Any) -> None:
    log_stage_start("7", "Preview and Persist P3 Audit")

    n_total = con.execute("SELECT COUNT(*) FROM panel_indicators").fetchone()[0]
    n_enrollments = con.execute(
        "SELECT COUNT(DISTINCT enrollment_id) FROM panel_indicators"
    ).fetchone()[0]

    df_preview = con.execute(
        "SELECT * FROM panel_indicators ORDER BY enrollment_id, week LIMIT 8"
    ).df()
    print("\npanel_indicators — first 8 rows:")
    print(df_preview.to_string(index=False))

    cols = [r[0] for r in con.execute("DESCRIBE panel_indicators").fetchall()]
    print_kv("columns", ", ".join(cols))
    print_kv("total rows", n_total)
    print_kv("distinct enrollments", n_enrollments)
    print_artifact("panel_indicators", "duckdb://panel_indicators")

    audit = {
        "stage":                          "P3",
        "timestamp":                      datetime.now(timezone.utc).astimezone().isoformat(),
        "status":                         "completed",
        "panel_indicators_rows":          n_total,
        "panel_indicators_n_enrollments": n_enrollments,
        "columns_added": [
            "log_clicks", "click_intensity", "cumulative_clicks",
            "recency", "streak", "engagement_state",
        ],
    }
    append_to_audit("P3", audit)
    print_artifact("pipeline_audit.json (P3 section)", METADATA_DIR / "pipeline_audit.json")

    log_stage_end("7")


# ==============================================================
# Main entry point
# ==============================================================

def main() -> None:
    import pandas as pd
    import warnings
    warnings.simplefilter("default")
    pd.set_option("display.max_columns", 200)
    pd.set_option("display.width", 200)

    print_section("PROPOSAL STAGE P3 — Engagement Indicators")
    print_kv("started_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    con = stage_p3_1_open_duckdb()

    try:
        stage_p3_2_basic_indicators(con)
        stage_p3_3_cumulative_clicks(con)
        stage_p3_4_recency(con)
        stage_p3_5_streak(con)
        stage_p3_6_engagement_state(con)
        stage_p3_7_preview_and_audit(con)
    finally:
        con.close()

    print_section("P3 COMPLETE")
    print_kv("completed_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_kv("next_step",    "Run src/P4_assessment_join.py")


if __name__ == "__main__":
    main()
