"""
S7_le_jd_assembly.py
─────────────────────
Assembles the Latent Engagement Joint Display (LE-JD) artifact by integrating
quantitative (OULAD) and qualitative (S5 LLM survey) strands.

LE-JD columns (per proposal_artifact.md):
  Col 1 — Behavioral Indicators  : observable LMS data from engagement_panel_weekly
  Col 2 — Latent Engagement State : engagement_state proxy (DBN output placeholder)
  Col 3 — Mechanism               : dominant_themes from S5 qualitative responses
  Col 4 — Meta-Inference          : convergence / expansion / discordance

Two output granularities:
  • le_jd_enrollment.csv  — 1 row per persona (enrollment-level summary)
  • le_jd_weekly.csv      — 1 row per persona × week (temporal, rank-1 enrollment)

Integration uses rank-1 enrollment from persona_enrollment_bridge.csv (best match).

Inputs
──────
  outputs/data/synthetic/llm_survey_responses.csv
  outputs/data/synthetic/persona_enrollment_bridge.csv
  outputs/data/synthetic/student_personas.csv
  outputs/engagement_panel_weekly.csv

Outputs
───────
  outputs/data/synthetic/le_jd_enrollment.csv
  outputs/data/synthetic/le_jd_weekly.csv
  outputs/metadata/s7_le_jd_audit.json
"""

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

# ── paths ─────────────────────────────────────────────────────────────────────
BASE      = Path(__file__).resolve().parent.parent
SYN       = BASE / "outputs" / "data" / "synthetic"
META      = BASE / "outputs" / "metadata"

QUALI_CSV   = SYN / "llm_survey_responses.csv"
BRIDGE_CSV  = SYN / "persona_enrollment_bridge.csv"
PERSONAS_CSV= SYN / "student_personas.csv"
PROMPTS_CSV = SYN / "student_personas_with_prompts.csv"
QUANT_CSV   = BASE / "outputs" / "engagement_panel_weekly.csv"

ENROLL_OUT  = SYN / "le_jd_enrollment.csv"
WEEKLY_OUT  = SYN / "le_jd_weekly.csv"
AUDIT_PATH  = META / "s7_le_jd_audit.json"

SYN.mkdir(parents=True, exist_ok=True)
META.mkdir(parents=True, exist_ok=True)

# ── helpers — Col 1: Behavioral Profile label ─────────────────────────────────

def _click_tier(total_clicks, p25, p75):
    if total_clicks >= p75:
        return "high-clicks"
    if total_clicks >= p25:
        return "medium-clicks"
    return "low-clicks"

def _streak_tier(streak):
    if streak >= 8:
        return "sustained"
    if streak >= 3:
        return "intermittent"
    return "absent"

def _timeliness_label(timeliness):
    """submission_timeliness: positive=early, negative=late, NaN=no submission."""
    if pd.isna(timeliness):
        return "no-submission"
    if timeliness > 0:
        return "early-submitter"
    if timeliness == 0:
        return "on-time"
    return "late-submitter"

def _score_tier(score):
    if pd.isna(score):
        return "unassessed"
    if score >= 70:
        return "high-score"
    if score >= 50:
        return "mid-score"
    return "low-score"

def make_behavioral_profile(row: pd.Series, click_p25: float, click_p75: float) -> str:
    """Build a compact behavioral profile label for Col 1."""
    parts = [
        _click_tier(row["total_clicks_mean"], click_p25, click_p75),
        _streak_tier(row["streak_max"]),
        _timeliness_label(row["submission_timeliness_mean"]),
        _score_tier(row["assessment_score_mean"]),
    ]
    return " | ".join(parts)

# ── helpers — Col 4: Meta-Inference ──────────────────────────────────────────

# Ordinal mapping for comparison
ENG_ORD = {"low": 0, "medium": 1, "high": 2}

def meta_inference(quant_eng: str, quali_eng: str, themes: str) -> tuple:
    """
    Compare quantitative engagement state with self-assessed engagement.
    Returns (verdict, rationale).

    Convergence : quant ≈ quali  → behavioral data aligns with student experience
    Expansion   : quali > quant  → student reports more engagement than LMS shows
                                   (offline study, intrinsic motivation invisible to LMS)
    Discordance : quali < quant  → LMS shows activity without meaningful engagement
                                   (surface compliance, anxiety-driven clicks)
    """
    q_ord = ENG_ORD.get(str(quant_eng).lower(), 1)
    s_ord = ENG_ORD.get(str(quali_eng).lower(), 1)
    diff  = s_ord - q_ord

    theme_list = [t.strip() for t in str(themes).split("|") if t.strip()]

    if diff == 0:
        verdict   = "Convergence"
        rationale = (f"LMS engagement ({quant_eng}) aligns with self-assessed "
                     f"engagement ({quali_eng}). Themes: {', '.join(theme_list[:3])}.")
    elif diff > 0:
        verdict   = "Expansion"
        rationale = (f"Self-assessed engagement ({quali_eng}) exceeds LMS signal "
                     f"({quant_eng}), suggesting learning activity invisible to the "
                     f"platform (e.g., offline study, peer interaction). "
                     f"Themes: {', '.join(theme_list[:3])}.")
    else:
        verdict   = "Discordance"
        rationale = (f"LMS activity ({quant_eng}) overstates meaningful engagement "
                     f"relative to self-assessment ({quali_eng}), suggesting surface "
                     f"compliance or anxiety-driven clicks. "
                     f"Themes: {', '.join(theme_list[:3])}.")

    return verdict, rationale

# ── main ──────────────────────────────────────────────────────────────────────

print("█" * 70)
print("  S7 — LE-JD Assembly  (Quantitative + Qualitative Integration)")
print("█" * 70)
print()

t0 = time.time()

# ── [1] Load all sources ──────────────────────────────────────────────────────
print("  [1] Loading sources …")

quali   = pd.read_csv(QUALI_CSV)
bridge  = pd.read_csv(BRIDGE_CSV)
personas= pd.read_csv(PERSONAS_CSV)
prompts = pd.read_csv(PROMPTS_CSV, usecols=["persona_id", "persona_name"])
personas = personas.merge(prompts, on="persona_id", how="left")
print(f"    quali    : {quali.shape}")
print(f"    bridge   : {bridge.shape}")
print(f"    personas : {personas.shape}")

# Keep only rank-1 matches (best enrollment per persona)
bridge1 = bridge[bridge["rank"] == 1].copy()
print(f"    bridge-1 : {bridge1.shape}  (rank-1 only)")
print()

# ── [2] Load quant — only enrollments we need ─────────────────────────────────
print("  [2] Loading engagement_panel_weekly (rank-1 enrollments only) …")
target_enrollments = set(bridge1["enrollment_id"])
chunks = []
for chunk in pd.read_csv(QUANT_CSV, chunksize=100_000):
    sub = chunk[chunk["enrollment_id"].isin(target_enrollments)]
    if not sub.empty:
        chunks.append(sub)
quant = pd.concat(chunks, ignore_index=True)
print(f"    Rows loaded : {len(quant):,}  (from {quant['enrollment_id'].nunique()} enrollments)")
print()

# ── [3] Aggregate quant to enrollment level for Col 1 + Col 2 ─────────────────
print("  [3] Aggregating quant to enrollment level …")

def modal(s):
    m = s.mode()
    return m.iloc[0] if len(m) > 0 else np.nan

enroll_agg = quant.groupby("enrollment_id").agg(
    total_clicks_sum          = ("total_clicks",          "sum"),
    total_clicks_mean         = ("total_clicks",          "mean"),
    active_days_mean          = ("active_days",            "mean"),
    log_clicks_mean           = ("log_clicks",             "mean"),
    click_intensity_mean      = ("click_intensity",        "mean"),
    recency_last              = ("recency",                "max"),
    streak_max                = ("streak",                 "max"),
    cumulative_clicks_final   = ("cumulative_clicks",      "max"),
    assessment_score_mean     = ("assessment_score",       "mean"),
    submission_timeliness_mean= ("submission_timeliness",  "mean"),
    has_assessment_weeks      = ("has_assessment_this_week","sum"),
    n_weeks                   = ("week",                   "count"),
    engagement_state_modal    = ("engagement_state",       modal),
    final_result              = ("final_result",           "first"),
    code_module               = ("code_module",            "first"),
    code_presentation         = ("code_presentation",      "first"),
    age_band                  = ("age_band",               "first"),
    gender_oulad              = ("gender",                 "first"),
).reset_index()

# Percentile thresholds for behavioral profile labels
click_p25 = enroll_agg["total_clicks_mean"].quantile(0.25)
click_p75 = enroll_agg["total_clicks_mean"].quantile(0.75)

enroll_agg["behavioral_profile"] = enroll_agg.apply(
    make_behavioral_profile, axis=1, click_p25=click_p25, click_p75=click_p75
)

print(f"    Enrollment-level rows: {len(enroll_agg)}")
print()

# ── [4] Assemble enrollment-level LE-JD ──────────────────────────────────────
print("  [4] Assembling enrollment-level LE-JD …")

# Join: persona → bridge-1 → quant_agg + quali
le_jd = (
    bridge1[["persona_id", "enrollment_id", "match_score", "result_match", "engagement_match"]]
    .merge(enroll_agg, on="enrollment_id", how="left")
    .merge(
        quali[["persona_id", "overall_engagement_self_assessment", "dominant_themes"]
              + [f"Q{i}" for i in range(1, 25)]],
        on="persona_id", how="left"
    )
    .merge(
        personas[["persona_id", "persona_name", "persona_archetype_label",
                  "gender", "age", "learning_style", "risk_level",
                  "final_grade", "engagement_level", "motivation_level",
                  "stress_level_label", "intervention_type"]],
        on="persona_id", how="left"
    )
)

# Col 4: Meta-Inference
meta_results = le_jd.apply(
    lambda r: meta_inference(
        r["engagement_state_modal"],
        r["overall_engagement_self_assessment"],
        r["dominant_themes"]
    ),
    axis=1
)
le_jd["meta_inference_verdict"]   = meta_results.apply(lambda x: x[0])
le_jd["meta_inference_rationale"] = meta_results.apply(lambda x: x[1])

# ── Reorder columns into LE-JD logical structure ──────────────────────────────
COL1 = ["behavioral_profile", "total_clicks_sum", "total_clicks_mean",
        "active_days_mean", "log_clicks_mean", "click_intensity_mean",
        "recency_last", "streak_max", "cumulative_clicks_final",
        "assessment_score_mean", "submission_timeliness_mean",
        "has_assessment_weeks", "n_weeks"]

COL2 = ["engagement_state_modal"]

COL3 = ["dominant_themes"] + [f"Q{i}" for i in range(1, 25)]

COL4 = ["meta_inference_verdict", "meta_inference_rationale"]

CONTEXT = ["persona_id", "persona_name", "persona_archetype_label",
           "gender", "age", "learning_style", "risk_level",
           "final_grade", "engagement_level", "motivation_level",
           "stress_level_label", "intervention_type",
           "enrollment_id", "code_module", "code_presentation",
           "age_band", "gender_oulad", "final_result",
           "match_score", "result_match", "engagement_match",
           "overall_engagement_self_assessment"]

ordered_cols = CONTEXT + COL1 + COL2 + COL3 + COL4
le_jd = le_jd[[c for c in ordered_cols if c in le_jd.columns]]

print(f"    LE-JD shape: {le_jd.shape}")
print(f"    Cols: {len(le_jd.columns)}  ({len(CONTEXT)} context | {len(COL1)} col1 | {len(COL2)} col2 | {len(COL3)} col3 | {len(COL4)} col4)")
print()

# ── [5] Assemble weekly LE-JD ─────────────────────────────────────────────────
print("  [5] Assembling weekly LE-JD …")

# Join quant weekly rows with bridge-1 (to get persona_id per enrollment)
quant_with_pid = quant.merge(
    bridge1[["persona_id", "enrollment_id", "match_score"]],
    on="enrollment_id", how="inner"
)

# Add quali (persona-level, repeated per week)
weekly = quant_with_pid.merge(
    quali[["persona_id", "overall_engagement_self_assessment", "dominant_themes"]],
    on="persona_id", how="left"
).merge(
    personas[["persona_id", "persona_name", "persona_archetype_label",
              "risk_level", "final_grade", "learning_style",
              "intervention_type"]],
    on="persona_id", how="left"
)

# Col 1 weekly: behavioral profile per week
week_click_p25 = weekly["total_clicks"].quantile(0.25)
week_click_p75 = weekly["total_clicks"].quantile(0.75)

def weekly_behavioral_label(row):
    parts = [
        _click_tier(row["total_clicks"], week_click_p25, week_click_p75),
        _streak_tier(row["streak"]),
        _timeliness_label(row["submission_timeliness"]),
        _score_tier(row["assessment_score"]),
    ]
    return " | ".join(parts)

weekly["behavioral_profile_week"] = weekly.apply(weekly_behavioral_label, axis=1)

# Col 4 weekly: meta-inference (same verdict as enrollment-level per persona)
meta_w = weekly.apply(
    lambda r: meta_inference(
        r["engagement_state"],
        r["overall_engagement_self_assessment"],
        r["dominant_themes"]
    ), axis=1
)
weekly["meta_inference_verdict"]   = meta_w.apply(lambda x: x[0])
weekly["meta_inference_rationale"] = meta_w.apply(lambda x: x[1])

# Reorder
WEEKLY_CONTEXT = ["persona_id", "persona_name", "persona_archetype_label",
                  "enrollment_id", "code_module", "code_presentation", "week",
                  "final_result", "final_grade", "risk_level",
                  "learning_style", "intervention_type", "match_score"]
WEEKLY_COL1 = ["behavioral_profile_week", "total_clicks", "active_days",
               "log_clicks", "click_intensity", "recency", "streak",
               "cumulative_clicks", "assessment_score", "submission_timeliness",
               "has_assessment_this_week"]
WEEKLY_COL2 = ["engagement_state"]
WEEKLY_COL3 = ["dominant_themes", "overall_engagement_self_assessment"]
WEEKLY_COL4 = ["meta_inference_verdict", "meta_inference_rationale"]

weekly_cols = WEEKLY_CONTEXT + WEEKLY_COL1 + WEEKLY_COL2 + WEEKLY_COL3 + WEEKLY_COL4
weekly = weekly[[c for c in weekly_cols if c in weekly.columns]].sort_values(
    ["persona_id", "week"]
).reset_index(drop=True)

print(f"    Weekly LE-JD shape: {weekly.shape}")
print()

# ── [6] Save ──────────────────────────────────────────────────────────────────
print("  [6] Saving …")
le_jd.to_csv(ENROLL_OUT, index=False)
weekly.to_csv(WEEKLY_OUT, index=False)
print(f"    {ENROLL_OUT}  {le_jd.shape}")
print(f"    {WEEKLY_OUT}  {weekly.shape}")
print()

# ── [7] Summary stats ─────────────────────────────────────────────────────────
print("  [7] Integration summary …")
verdict_dist = le_jd["meta_inference_verdict"].value_counts()
print(f"    Meta-inference distribution:")
for v, n in verdict_dist.items():
    print(f"      {v:15s}: {n:4d}  ({n/len(le_jd)*100:.1f}%)")
print()
print(f"    Behavioral profiles (top 5):")
for bp, n in le_jd["behavioral_profile"].value_counts().head(5).items():
    print(f"      {n:4d}  {bp}")
print()
print("  Sample LE-JD row (P0001):")
s = le_jd[le_jd["persona_id"] == "P0001"].iloc[0]
print(f"    Name                  : {s['persona_name']}")
print(f"    Archetype             : {s['persona_archetype_label']}")
print(f"    Enrollment            : {s['enrollment_id']}")
print(f"    Col 1 — Behavioral    : {s['behavioral_profile']}")
print(f"    Col 2 — Latent State  : {s['engagement_state_modal']}")
print(f"    Col 3 — Mechanism     : {s['dominant_themes']}")
print(f"    Col 4 — Meta-Inference: {s['meta_inference_verdict']}")
print(f"    Rationale             : {s['meta_inference_rationale'][:100]}…")
print()

# ── [8] Audit ─────────────────────────────────────────────────────────────────
elapsed = round(time.time() - t0, 1)
audit = {
    "status"           : "COMPLETE",
    "elapsed_s"        : elapsed,
    "enrollment_level" : {
        "rows"      : len(le_jd),
        "cols"      : len(le_jd.columns),
        "output"    : str(ENROLL_OUT),
    },
    "weekly_level"     : {
        "rows"      : len(weekly),
        "cols"      : len(weekly.columns),
        "output"    : str(WEEKLY_OUT),
    },
    "meta_inference_distribution": verdict_dist.to_dict(),
    "le_jd_columns": {
        "Col1_Behavioral_Indicators" : COL1,
        "Col2_Latent_State"          : COL2,
        "Col3_Mechanism"             : ["dominant_themes", "Q1-Q24"],
        "Col4_Meta_Inference"        : COL4,
        "Context"                    : CONTEXT,
    },
    "sources": {
        "quali"   : str(QUALI_CSV),
        "bridge"  : str(BRIDGE_CSV),
        "personas": str(PERSONAS_CSV),
        "quant"   : str(QUANT_CSV),
    }
}
with open(AUDIT_PATH, "w") as f:
    json.dump(audit, f, indent=2)

print(f"  Audit  : {AUDIT_PATH}")
print(f"  Elapsed: {elapsed}s")
print(f"  Status : COMPLETE ✓")
