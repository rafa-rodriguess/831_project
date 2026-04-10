"""
S3_persona_assembly.py
──────────────────────
Assembles 1,300 synthetic student personas from the matched Kaggle–Zenodo pairs
produced by S2. Each persona merges columns from one Kaggle row and one Zenodo
row, adds harmonized bridge metrics, and is labelled with an archetype.

Inputs
──────
outputs/data/synthetic/persona_pairs.csv      (1300 × 5)
outputs/data/synthetic/zenodo_clustered.csv   (14003 × 34)
outputs/data/synthetic/kaggle_clustered.csv   (1300 × 27)

Outputs
───────
outputs/data/synthetic/student_personas.csv   (1300 × 37)
outputs/metadata/s3_persona_audit.json
"""

import json
import os
import sys

import numpy as np
import pandas as pd

# ── paths ────────────────────────────────────────────────────────────────────
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SYN  = os.path.join(BASE, "outputs", "data", "synthetic")
META = os.path.join(BASE, "outputs", "metadata")

PAIRS_IN    = os.path.join(SYN, "persona_pairs.csv")
ZENODO_IN   = os.path.join(SYN, "zenodo_clustered.csv")
KAGGLE_IN   = os.path.join(SYN, "kaggle_clustered.csv")
PERSONAS_OUT = os.path.join(SYN, "student_personas.csv")
AUDIT_PATH   = os.path.join(META, "s3_persona_audit.json")

os.makedirs(META, exist_ok=True)

# ── helper ────────────────────────────────────────────────────────────────────

def _json_safe(obj):
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    return obj


def classify_archetype(risk: str, motiv_idx: float, stress_score: float) -> str:
    """
    8-archetype labelling based on (risk_level × motivation × stress):

    Flourishing Achiever   : low risk    + high motivation + low stress
    Engaged but Pressured  : low risk    + high motivation + high stress
    Routine Complier       : low risk    + low motivation  (any stress)
    Resilient Climber      : medium risk + high motivation
    Disengaged Drifter     : medium risk + low motivation
    Overwhelmed Striver    : high risk   + high motivation
    Crisis Learner         : high risk   + low motivation  + high stress
    Steady Performer       : all other cases
    """
    risk = str(risk).strip().lower()
    hi_mot = motiv_idx >= 3.5
    lo_mot = motiv_idx < 2.5
    hi_str = stress_score >= 3.5
    lo_str = stress_score < 2.5

    if risk == "low":
        if hi_mot and lo_str:
            return "Flourishing Achiever"
        if hi_mot and hi_str:
            return "Engaged but Pressured"
        if lo_mot:
            return "Routine Complier"
    elif risk == "medium":
        if hi_mot:
            return "Resilient Climber"
        if lo_mot:
            return "Disengaged Drifter"
    elif risk == "high":
        if hi_mot:
            return "Overwhelmed Striver"
        if lo_mot and hi_str:
            return "Crisis Learner"

    return "Steady Performer"


# ── main ─────────────────────────────────────────────────────────────────────

print("█" * 70)
print("  S3 — Persona Assembly")
print("█" * 70)
print()

# [1] Load inputs
print("  [1] Loading inputs …")
pairs = pd.read_csv(PAIRS_IN)
zc    = pd.read_csv(ZENODO_IN)
kc    = pd.read_csv(KAGGLE_IN)
print(f"    persona_pairs  : {pairs.shape}")
print(f"    zenodo_clustered: {zc.shape}")
print(f"    kaggle_clustered: {kc.shape}")
print()

# Set integer index on zenodo/kaggle by their row-index column
zc = zc.reset_index(drop=True)
kc = kc.reset_index(drop=True)

N = len(pairs)  # 1300

# [2] Assemble persona rows
print("  [2] Assembling persona rows …")
rows = []
for i, p in pairs.iterrows():
    z_idx = int(p["zenodo_idx"])
    k_idx = int(p["kaggle_idx"])
    zrow  = zc.iloc[z_idx]
    krow  = kc.iloc[k_idx]

    # Harmonized bridge metrics
    att_harm  = ((zrow["attendance_norm"] + krow["attendance_norm"]) / 2) * 100
    asg_harm  = ((zrow["assignment_norm"] + krow["assignment_norm"]) / 2) * 100
    exam_harm = ((zrow["exam_norm"] + krow["exam_norm"]) / 2) * 60 + 40

    # Archetype label
    arch = classify_archetype(
        risk       = krow["risk_level"],
        motiv_idx  = float(krow["motivation_index"]),
        stress_score = float(krow["stress_level"]),
    )

    rows.append({
        # ── Metadata ──────────────────────────────────────────────────────────
        "persona_id"             : f"P{i+1:04d}",
        "zenodo_row_idx"         : z_idx,
        "kaggle_row_idx"         : k_idx,
        "bridge_distance"        : round(float(p["bridge_distance"]), 6),
        "zenodo_cluster_id"      : int(p["zenodo_cluster_id"]),
        "kaggle_cluster_id"      : int(p["kaggle_cluster_id"]),
        "persona_archetype_label": arch,

        # ── Dimension 1: Demographics (Zenodo) ────────────────────────────────
        "age"                    : int(zrow["Age"]),
        "gender"                 : zrow["Gender_lbl"],
        "learning_style"         : zrow["LearningStyle_lbl"],
        "internet_access"        : zrow["Internet_lbl"],
        "extracurricular"        : zrow["Extracurricular_lbl"],
        "uses_edutech"           : zrow["EduTech_lbl"],
        "resources_availability" : zrow["Resources_lbl"],

        # ── Dimension 2: Academic (both) ──────────────────────────────────────
        "study_hours_per_week"   : int(zrow["StudyHours"]),
        "online_courses_enrolled": int(zrow["OnlineCourses"]),
        "attendance_pct"         : round(att_harm, 2),
        "assignment_completion_pct": round(asg_harm, 2),
        "exam_score_harmonized"  : round(exam_harm, 2),
        "final_grade"            : zrow["FinalGrade_lbl"],
        "risk_level"             : krow["risk_level"],
        "avg_response_time_hours": float(krow["avg_response_time"]),
        "assignment_score"       : float(krow["assignment_score"]),

        # ── Dimension 3: LMS Engagement (Kaggle) ──────────────────────────────
        "login_frequency"        : int(krow["login_frequency"]),
        "video_watch_time_min"   : float(krow["video_watch_time"]),
        "discussion_posts"       : int(krow["discussion_posts"]),
        "peer_interaction_count" : int(krow["peer_interaction_count"]),
        "task_completion_rate"   : round(float(krow["task_completion_rate"]), 4),
        "engagement_level"       : int(krow["engagement_level"]),

        # ── Dimension 4: Psychological (both) ─────────────────────────────────
        "motivation_level"       : zrow["Motivation_lbl"],
        "motivation_index"       : float(krow["motivation_index"]),
        "stress_level_label"     : zrow["StressLevel_lbl"],
        "stress_score"           : float(krow["stress_level"]),
        "anxiety_score"          : float(krow["anxiety_score"]),
        "resilience_score"       : float(krow["resilience_score"]),
        "post_intervention_mood" : float(krow["post_intervention_mood"]),

        # ── Dimension 5: Emotion & Intervention (Kaggle) ──────────────────────
        "dominant_emotion"       : krow["emotion"],
        "intervention_type"      : krow["intervention_type"],
    })

personas = pd.DataFrame(rows)
print(f"    personas shape: {personas.shape}")
print(f"    Columns ({len(personas.columns)}): {list(personas.columns)}")
print()

# [3] Validate
print("  [3] Validating …")
n_nan = personas.isna().sum().sum()
assert n_nan == 0, f"NaN found: {personas.isna().sum()[personas.isna().sum()>0]}"
print(f"    NaN count: {n_nan} ✓")

assert (personas["attendance_pct"]  >= 0).all() and (personas["attendance_pct"]  <= 100).all()
assert (personas["exam_score_harmonized"] >= 40).all() and (personas["exam_score_harmonized"] <= 100).all()
assert personas["persona_id"].nunique() == N, "Duplicate persona_id!"
print(f"    Ranges: attendance_pct [0–100] ✓  exam_score_harmonized [40–100] ✓")
print(f"    Unique persona_ids: {N} ✓")
print()

# [4] Archetype distribution
print("  [4] Archetype distribution:")
arch_dist = personas["persona_archetype_label"].value_counts()
for label, cnt in arch_dist.items():
    print(f"    {label:<28s}: {cnt:4d}  ({cnt/N*100:.1f}%)")
print()

# [5] Quick profile
print("  [5] Schema preview:")
print(personas.head(3).to_string())
print()

# [6] Save
print("  [6] Saving outputs …")
personas.to_csv(PERSONAS_OUT, index=False)
print(f"    student_personas.csv : {personas.shape}  → {PERSONAS_OUT}")
print()

# [7] Audit
audit = {
    "status"          : "COMPLETE",
    "n_personas"      : N,
    "n_columns"       : len(personas.columns),
    "columns"         : list(personas.columns),
    "nan_total"       : int(n_nan),
    "archetype_distribution": arch_dist.to_dict(),
    "stats": {
        col: {
            "min": round(float(personas[col].min()), 4),
            "max": round(float(personas[col].max()), 4),
            "mean": round(float(personas[col].mean()), 4),
        }
        for col in ["attendance_pct", "assignment_completion_pct",
                    "exam_score_harmonized", "motivation_index",
                    "stress_score", "bridge_distance"]
    },
    "outputs": {"student_personas": PERSONAS_OUT},
}
with open(AUDIT_PATH, "w") as f:
    json.dump(_json_safe(audit), f, indent=2)

print(f"  Audit: {AUDIT_PATH}")
print(f"  Status: COMPLETE ✓")
