"""
S6_persona_enrollment_bridge.py
────────────────────────────────
Links each of the 1300 synthetic personas to matching OULAD enrollments
(engagement_panel_weekly.csv) using a weighted similarity score.

Matching dimensions
───────────────────
  Hard filter  (must match, no score):
    • gender       : Male→M  /  Female→F

  Weighted score  (0–10 per dimension):
    • final_result : Pass/Distinction/Fail alignment          weight 3
    • engagement   : low / medium / high bucket alignment     weight 3
    • exam_score   : |persona_score − enroll_avg_score| → 0–10  weight 2
    • activity     : |persona_login_pct − enroll_click_pct|   weight 2

  Total possible: 80 pts  (normalised to 0–1 in output)

Output tables
─────────────
  outputs/data/synthetic/persona_enrollment_bridge.csv
    persona_id | enrollment_id | match_score | n_weeks | rank

  outputs/metadata/s6_bridge_audit.json
    • total personas        : 1300
    • fully matched         : N
    • unmatched (0 cands)   : list  ← to fix in future OULAD patch

Configuration
─────────────
  TOP_N        : max enrollments per persona  (default 10)
  MIN_SCORE    : minimum normalised score to accept a match  (default 0.40)

Usage
─────
  python S6_persona_enrollment_bridge.py
"""

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

# ── configuration ─────────────────────────────────────────────────────────────
TOP_N     = 10    # max matching enrollments per persona
MIN_SCORE = 0.40  # minimum normalised score (0–1) to keep a match

# weights (must sum to 10 for easy reasoning)
W_RESULT     = 3
W_ENGAGEMENT = 3
W_EXAM       = 2
W_ACTIVITY   = 2
MAX_RAW_SCORE = (W_RESULT + W_ENGAGEMENT + W_EXAM + W_ACTIVITY) * 10  # = 100

# ── paths ─────────────────────────────────────────────────────────────────────
BASE      = Path(__file__).resolve().parent.parent
OUT_DATA  = BASE / "outputs" / "data" / "synthetic"
META      = BASE / "outputs" / "metadata"
QUANT_CSV = BASE / "outputs" / "engagement_panel_weekly.csv"
PERSONAS  = OUT_DATA / "student_personas.csv"
BRIDGE_OUT = OUT_DATA / "persona_enrollment_bridge.csv"
AUDIT_PATH = META / "s6_bridge_audit.json"

OUT_DATA.mkdir(parents=True, exist_ok=True)
META.mkdir(parents=True, exist_ok=True)

# ── value maps ────────────────────────────────────────────────────────────────

# Persona gender → OULAD gender
GENDER_MAP = {"Male": "M", "Female": "F"}

# Persona final_grade → OULAD final_result bucket
# Merit and Pass both map to "pass_tier"; Withdrawn does not exist in personas
GRADE_MAP = {
    "Distinction": "Distinction",
    "Merit"       : "Pass",
    "Pass"        : "Pass",
    "Fail"        : "Fail",
}

# OULAD final_result → bucket (Withdrawn treated as Fail for matching)
RESULT_BUCKET = {
    "Distinction": "Distinction",
    "Pass"        : "Pass",
    "Fail"        : "Fail",
    "Withdrawn"   : "Fail",
}

# Persona engagement_level (1–5) → bucket
def persona_eng_bucket(level: int) -> str:
    if level <= 2:
        return "low"
    if level == 3:
        return "medium"
    return "high"


# ── helpers ───────────────────────────────────────────────────────────────────

def score_result(p_bucket: str, e_bucket: str) -> float:
    """Exact match → 10; same tier → 5; miss → 0."""
    if p_bucket == e_bucket:
        return 10.0
    # adjacent tiers
    tiers = ["Fail", "Pass", "Distinction"]
    if abs(tiers.index(p_bucket) - tiers.index(e_bucket)) == 1:
        return 5.0
    return 0.0


def score_engagement(p_bucket: str, e_bucket: str) -> float:
    if p_bucket == e_bucket:
        return 10.0
    order = ["low", "medium", "high"]
    if abs(order.index(p_bucket) - order.index(e_bucket)) == 1:
        return 4.0
    return 0.0


def score_exam(p_score: float, e_score: float) -> float:
    """0–10 inversely proportional to absolute difference (max diff = 60 pts)."""
    diff = abs(p_score - e_score)
    return max(0.0, 10.0 - (diff / 6.0))


def score_activity(p_pct: float, e_pct: float) -> float:
    """Both are 0–1 percentile ranks. 0–10 inversely proportional to diff."""
    diff = abs(p_pct - e_pct)
    return max(0.0, 10.0 - diff * 20.0)   # diff=0.5 → score=0


# ── main ──────────────────────────────────────────────────────────────────────

print("█" * 70)
print("  S6 — Persona ↔ Enrollment Bridge")
print("█" * 70)
print()

t0 = time.time()

# ── [1] Load and aggregate quant panel to enrollment level ───────────────────
print("  [1] Loading engagement_panel_weekly.csv …")
q = pd.read_csv(QUANT_CSV, usecols=[
    "enrollment_id", "id_student", "code_module", "code_presentation",
    "week", "total_clicks", "assessment_score", "gender",
    "final_result", "engagement_state",
])
print(f"    Shape: {q.shape}")

print("  [1b] Aggregating to enrollment level …")

def agg_engagement(series):
    """Modal engagement state per enrollment."""
    return series.mode().iloc[0] if len(series) > 0 else "low"

agg = q.groupby("enrollment_id").agg(
    id_student         = ("id_student",       "first"),
    code_module        = ("code_module",       "first"),
    code_presentation  = ("code_presentation", "first"),
    gender             = ("gender",            "first"),
    final_result       = ("final_result",      "first"),
    engagement_state   = ("engagement_state",  agg_engagement),
    avg_assessment     = ("assessment_score",  "mean"),   # NaN if no assessments
    total_clicks_sum   = ("total_clicks",      "sum"),
    n_weeks            = ("week",              "count"),
).reset_index()

# Fill NaN avg_assessment with global median
global_median = agg["avg_assessment"].median()
agg["avg_assessment"] = agg["avg_assessment"].fillna(global_median)

# Percentile rank of total_clicks within the full enrollment pool
agg["click_pct"] = agg["total_clicks_sum"].rank(pct=True)

# Map result bucket
agg["result_bucket"] = agg["final_result"].map(RESULT_BUCKET).fillna("Fail")

print(f"    Enrollments: {len(agg):,}")
print(f"    Gender dist : {agg['gender'].value_counts().to_dict()}")
print(f"    Result dist : {agg['result_bucket'].value_counts().to_dict()}")
print()

# Pre-split by gender for speed
agg_M = agg[agg["gender"] == "M"].copy()
agg_F = agg[agg["gender"] == "F"].copy()

# ── [2] Load personas ─────────────────────────────────────────────────────────
print("  [2] Loading student_personas.csv …")
p = pd.read_csv(PERSONAS)
print(f"    Shape: {p.shape}")
print()

# Persona-level derived features
p["gender_oulad"]   = p["gender"].map(GENDER_MAP)
p["result_bucket"]  = p["final_grade"].map(GRADE_MAP).fillna("Fail")
p["eng_bucket"]     = p["engagement_level"].apply(persona_eng_bucket)

# Percentile rank of login_frequency (proxy for activity intensity)
p["login_pct"] = p["login_frequency"].rank(pct=True)

# Normalise exam score to 0–100 (already is)
p["exam_norm"] = p["exam_score_harmonized"].clip(0, 100)

# ── [3] Match each persona to top-N enrollments ───────────────────────────────
print(f"  [3] Matching 1300 personas → top {TOP_N} enrollments each …")

records = []
unmatched = []

for idx, row in p.iterrows():
    pid       = str(row["persona_id"])
    g         = row["gender_oulad"]
    p_result  = row["result_bucket"]
    p_eng     = row["eng_bucket"]
    p_exam    = row["exam_norm"]
    p_lpct    = row["login_pct"]

    # Hard filter: same gender pool
    pool = agg_M if g == "M" else agg_F

    if pool.empty:
        unmatched.append({"persona_id": pid, "reason": f"no {g} enrollments"})
        continue

    # Vectorised scoring
    s_result = pool["result_bucket"].apply(lambda x: score_result(p_result, x))
    s_eng    = pool["engagement_state"].apply(lambda x: score_engagement(p_eng, x))
    s_exam   = pool["avg_assessment"].apply(lambda x: score_exam(p_exam, x))
    s_act    = pool["click_pct"].apply(lambda x: score_activity(p_lpct, x))

    raw = (s_result * W_RESULT +
           s_eng    * W_ENGAGEMENT +
           s_exam   * W_EXAM +
           s_act    * W_ACTIVITY)

    norm = raw / MAX_RAW_SCORE

    # Apply minimum score filter
    mask = norm >= MIN_SCORE
    filtered = pool[mask].copy()
    filtered_score = norm[mask]

    if filtered.empty:
        # Relax: take best available even below threshold
        best_idx = norm.nlargest(TOP_N).index
        filtered = pool.loc[best_idx].copy()
        filtered_score = norm.loc[best_idx]
        unmatched.append({
            "persona_id": pid,
            "reason": f"below MIN_SCORE ({MIN_SCORE}); best={norm.max():.3f}"
        })

    # Top-N by score
    top_idx   = filtered_score.nlargest(TOP_N).index
    top_pool  = filtered.loc[top_idx]
    top_score = filtered_score.loc[top_idx]

    for rank_i, (ei, enroll_row) in enumerate(top_pool.iterrows(), start=1):
        records.append({
            "persona_id"    : pid,
            "enrollment_id" : enroll_row["enrollment_id"],
            "gender"        : g,
            "match_score"   : round(float(top_score.loc[ei]), 4),
            "result_match"  : p_result == enroll_row["result_bucket"],
            "engagement_match": p_eng == enroll_row["engagement_state"],
            "n_weeks"       : int(enroll_row["n_weeks"]),
            "rank"          : rank_i,
        })

    if (idx + 1) % 100 == 0:
        print(f"    {idx+1}/1300 …")

bridge = pd.DataFrame(records)
print(f"    Done. Bridge rows: {len(bridge):,}")
print()

# ── [4] Coverage audit ────────────────────────────────────────────────────────
print("  [4] Coverage audit …")

matched_pids   = set(bridge["persona_id"].unique())
all_pids       = set(p["persona_id"].astype(str))
truly_unmatched = all_pids - matched_pids  # personas with 0 rows in bridge

n_fully_matched  = len(matched_pids)
n_unmatched      = len(truly_unmatched)
n_below_threshold = len([u for u in unmatched if "below MIN_SCORE" in u.get("reason", "")])

print(f"    Total personas         : {len(all_pids)}")
print(f"    Matched (≥1 enroll)    : {n_fully_matched}")
print(f"    Unmatched (0 enrolls)  : {n_unmatched}")
print(f"    Below threshold (relaxed): {n_below_threshold}")
print()

if truly_unmatched:
    print("  [!] UNMATCHED personas (need OULAD patch):")
    for pid in sorted(truly_unmatched):
        row = p[p["persona_id"].astype(str) == pid].iloc[0]
        print(f"       {pid}  gender={row['gender']}  grade={row['final_grade']}  eng={row['engagement_level']}")
    print()
else:
    print("  ✓ All 1300 personas matched to at least 1 enrollment.")
    print()

# Score distribution
score_stats = bridge.groupby("rank")["match_score"].agg(["mean", "min", "max"]).round(4)
print("  Score stats by rank:")
print(score_stats.to_string())
print()

# ── [5] Save ──────────────────────────────────────────────────────────────────
print("  [5] Saving …")
bridge.to_csv(BRIDGE_OUT, index=False)
print(f"    {BRIDGE_OUT}")
print(f"    Shape: {bridge.shape}")
print()

# ── [6] Audit JSON ────────────────────────────────────────────────────────────
elapsed = round(time.time() - t0, 1)
audit = {
    "status"              : "COMPLETE" if n_unmatched == 0 else "PARTIAL",
    "total_personas"      : len(all_pids),
    "matched"             : n_fully_matched,
    "unmatched"           : n_unmatched,
    "unmatched_ids"       : sorted(truly_unmatched),
    "below_threshold_relaxed": n_below_threshold,
    "top_n"               : TOP_N,
    "min_score_threshold" : MIN_SCORE,
    "bridge_rows"         : len(bridge),
    "elapsed_s"           : elapsed,
    "score_stats"         : {
        "mean" : round(float(bridge["match_score"].mean()), 4),
        "min"  : round(float(bridge["match_score"].min()),  4),
        "max"  : round(float(bridge["match_score"].max()),  4),
        "p25"  : round(float(bridge["match_score"].quantile(0.25)), 4),
        "p75"  : round(float(bridge["match_score"].quantile(0.75)), 4),
    },
    "below_threshold_details": [u for u in unmatched if "below MIN_SCORE" in u.get("reason","")],
    "outputs": {
        "bridge_csv" : str(BRIDGE_OUT),
        "audit_json" : str(AUDIT_PATH),
    }
}
with open(AUDIT_PATH, "w") as f:
    json.dump(audit, f, indent=2)

print(f"  Audit: {AUDIT_PATH}")
print(f"  Elapsed: {elapsed}s")
print(f"  Status: {audit['status']} ✓" if audit["status"] == "COMPLETE" else f"  Status: {audit['status']} — {n_unmatched} personas need OULAD patch")
