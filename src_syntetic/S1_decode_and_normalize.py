"""
S1_decode_and_normalize.py
──────────────────────────
Decodes label-encoded categorical columns in the Zenodo dataset and
normalizes the 6 bridge variables in both datasets to a common [0,1] scale.

Strategy
────────
• Zenodo: all columns are int64 (LabelEncoder output). Categorical columns are
  decoded back to semantic labels using data-driven validation (mean ExamScore
  per code determines ordering direction).
• Both datasets: 6 shared bridge constructs are normalized to [0,1] via
  MinMaxScaler so the two datasets share a common comparison space.

Bridge constructs (6 variables):
  1. attendance      Zenodo:Attendance(60-100)      Kaggle:attendance_rate(0.5-1.0)
  2. assignment      Zenodo:AssignmentCompletion     Kaggle:task_completion_rate
  3. exam            Zenodo:ExamScore(40-100)        Kaggle:exam_score(40-100)
  4. motivation      Zenodo:Motivation(0-2)          Kaggle:motivation_index(1-5)
  5. stress          Zenodo:StressLevel(0-2)         Kaggle:stress_level(1-5)
  6. discussion      Zenodo:Discussions(0/1)         Kaggle:discussion_posts(binarized)

Outputs
───────
outputs/data/synthetic/zenodo_decoded.csv       14,003 rows, ~26 cols
outputs/data/synthetic/kaggle_normalized.csv     1,300 rows, ~25 cols
outputs/metadata/s1_decode_audit.json
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# ── paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = Path(__file__).resolve().parent
PROPOSAL_ROOT = SCRIPT_DIR.parent
RAW_DIR       = PROPOSAL_ROOT / "outputs" / "data" / "raw"
SYN_DIR       = PROPOSAL_ROOT / "outputs" / "data" / "synthetic"
META_DIR      = PROPOSAL_ROOT / "outputs" / "metadata"

SYN_DIR.mkdir(parents=True, exist_ok=True)

ZENODO_RAW  = RAW_DIR / "merged_dataset.csv"
KAGGLE_RAW  = RAW_DIR / "psychological_cbi_dataset.csv"
ZENODO_OUT  = SYN_DIR / "zenodo_decoded.csv"
KAGGLE_OUT  = SYN_DIR / "kaggle_normalized.csv"
AUDIT_PATH  = META_DIR / "s1_decode_audit.json"

audit = {
    "script"  : "S1_decode_and_normalize.py",
    "run_at"  : datetime.now(timezone.utc).isoformat(),
    "status"  : "INCOMPLETE",
    "zenodo"  : {},
    "kaggle"  : {},
    "bridge_cols": [],
}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def infer_ordinal_direction(df: pd.DataFrame, code_col: str, score_col: str = "ExamScore") -> str:
    """Return 'ascending'  if higher code = better grade,
              'descending' if lower  code = better grade,
              'unordered'  otherwise.
    """
    means = df.groupby(code_col)[score_col].mean().sort_index()
    diffs = means.diff().dropna()
    if (diffs >= 0).all():
        return "ascending"
    if (diffs <= 0).all():
        return "descending"
    return "unordered"


def apply_mapping(series: pd.Series, mapping: dict) -> pd.Series:
    return series.map(mapping).fillna("Unknown")


def make_scaler(data: np.ndarray) -> MinMaxScaler:
    scaler = MinMaxScaler()
    scaler.fit(data.reshape(-1, 1))
    return scaler


# ──────────────────────────────────────────────────────────────────────────────
# Step 1 — Load
# ──────────────────────────────────────────────────────────────────────────────

print("\n" + "█" * 70)
print("  S1 — Decode & Normalize")
print("█" * 70)

z = pd.read_csv(ZENODO_RAW)
k = pd.read_csv(KAGGLE_RAW)
print(f"\n  Zenodo : {z.shape}   Kaggle : {k.shape}")

# ──────────────────────────────────────────────────────────────────────────────
# Step 2 — Decode Zenodo binary columns
# ──────────────────────────────────────────────────────────────────────────────

print("\n  [2] Decoding Zenodo binary columns …")

binary_maps = {
    "Gender"        : {0: "Female", 1: "Male"},      # F < M alphabetically
    "Internet"      : {0: "No",     1: "Yes"},
    "Extracurricular": {0: "No",    1: "Yes"},
    "EduTech"       : {0: "No",     1: "Yes"},
    "Discussions"   : {0: "No",     1: "Yes"},
}

for col, mapping in binary_maps.items():
    z[f"{col}_lbl"] = apply_mapping(z[col], mapping)
    print(f"    {col}: {dict(z[col].value_counts().sort_index().to_dict())} → labels: {set(z[col+'_lbl'])}")

# ──────────────────────────────────────────────────────────────────────────────
# Step 3 — Decode Zenodo ordinal 3-level columns (data-driven direction)
# ──────────────────────────────────────────────────────────────────────────────

print("\n  [3] Decoding Zenodo ordinal 3-level columns …")

# Possible label sets (alphabetical order by LabelEncoder: H=0, L=1, M=2 if values="High/Low/Medium")
# But direction detected from ExamScore correlation
ordinal3_candidates = {
    "Motivation" : ["High", "Low", "Medium"],   # alphabetical LabelEncoder order
    "StressLevel": ["High", "Low", "Medium"],
    "Resources"  : ["High", "Low", "Medium"],
}

for col, alpha_labels in ordinal3_candidates.items():
    direction = infer_ordinal_direction(z, col, "ExamScore")
    codes     = sorted(z[col].unique())

    if direction == "ascending":
        # lower code = worse outcome relative to ExamScore → code 0 is lowest grade of motivation/resource
        # For Motivation: ascending = code 0 = Low motivation (makes sense: low motivation → low score)
        labels = {0: "Low", 1: "Medium", 2: "High"}
        note   = "ordinal_ascending: code 0=Low"
    elif direction == "descending":
        # lower code = better outcome → code 0 = High (alphabetical: High=0)
        labels = {c: alpha_labels[c] for c in codes}
        note   = f"descending: alphabetical {alpha_labels}"
    else:
        labels = {c: alpha_labels[c] for c in codes}
        note   = f"unordered: fallback alphabetical {alpha_labels}"

    z[f"{col}_lbl"] = apply_mapping(z[col], labels)
    corr = z[col].corr(z["ExamScore"])
    print(f"    {col}: direction={direction}, corr={corr:.3f}, labels={labels}  [{note}]")
    audit["zenodo"][f"{col}_decode"] = {"direction": direction, "corr_with_exam": round(corr, 4), "mapping": labels, "note": note}

# ──────────────────────────────────────────────────────────────────────────────
# Step 4 — Decode FinalGrade (4 levels) and LearningStyle (4 levels)
# ──────────────────────────────────────────────────────────────────────────────

print("\n  [4] Decoding FinalGrade and LearningStyle …")

# FinalGrade: detect direction relative to ExamScore
direction_fg = infer_ordinal_direction(z, "FinalGrade", "ExamScore")
if direction_fg == "ascending":
    fg_map = {0: "Fail", 1: "Pass", 2: "Merit", 3: "Distinction"}
elif direction_fg == "descending":
    fg_map = {0: "Distinction", 1: "Merit", 2: "Pass", 3: "Fail"}
else:
    # inspect means    
    means_fg = z.groupby("FinalGrade")["ExamScore"].mean()
    ranked   = means_fg.rank().astype(int) - 1
    grade_order = ["Fail", "Pass", "Merit", "Distinction"]
    fg_map   = {code: grade_order[rank] for code, rank in ranked.items()}

z["FinalGrade_lbl"] = apply_mapping(z["FinalGrade"], fg_map)
corr_fg = z["FinalGrade"].corr(z["ExamScore"])
print(f"    FinalGrade: direction={direction_fg}, corr={corr_fg:.3f}, mapping={fg_map}")
audit["zenodo"]["FinalGrade_decode"] = {"direction": direction_fg, "corr_with_exam": round(corr_fg, 4), "mapping": fg_map}

# LearningStyle: nominal — alphabetical (Auditory=0, Kinesthetic=1, ReadWrite=2, Visual=3)
ls_map = {0: "Auditory", 1: "Kinesthetic", 2: "ReadWrite", 3: "Visual"}
z["LearningStyle_lbl"] = apply_mapping(z["LearningStyle"], ls_map)
print(f"    LearningStyle: {ls_map}  distribution: {z['LearningStyle_lbl'].value_counts().to_dict()}")
audit["zenodo"]["LearningStyle_decode"] = {"mapping": ls_map}

# ──────────────────────────────────────────────────────────────────────────────
# Step 5 — Compute bridge _norm columns (Zenodo)
# ──────────────────────────────────────────────────────────────────────────────

print("\n  [5] Normalizing Zenodo bridge variables …")

bridge_z = {
    "attendance_norm"  : "Attendance",
    "assignment_norm"  : "AssignmentCompletion",
    "exam_norm"        : "ExamScore",
    "motivation_norm"  : "Motivation",
    "stress_norm"      : "StressLevel",
    "discussion_norm"  : "Discussions",   # already 0/1 — copy directly
}

for norm_col, raw_col in bridge_z.items():
    if raw_col == "Discussions":
        z[norm_col] = z[raw_col].astype(float)   # already binary
    else:
        scaler = MinMaxScaler()
        z[norm_col] = scaler.fit_transform(z[[raw_col]]).flatten()
    print(f"    {norm_col}: min={z[norm_col].min():.3f}  max={z[norm_col].max():.3f}  mean={z[norm_col].mean():.3f}")

BRIDGE_COLS = list(bridge_z.keys())

# ──────────────────────────────────────────────────────────────────────────────
# Step 6 — Compute bridge _norm columns (Kaggle)
# ──────────────────────────────────────────────────────────────────────────────

print("\n  [6] Normalizing Kaggle bridge variables …")

bridge_k_raw = {
    "attendance_norm" : "attendance_rate",
    "assignment_norm" : "task_completion_rate",
    "exam_norm"       : "exam_score",
    "motivation_norm" : "motivation_index",
    "stress_norm"     : "stress_level",
}

for norm_col, raw_col in bridge_k_raw.items():
    scaler = MinMaxScaler()
    k[norm_col] = scaler.fit_transform(k[[raw_col]]).flatten()
    print(f"    {norm_col}: min={k[norm_col].min():.3f}  max={k[norm_col].max():.3f}  mean={k[norm_col].mean():.3f}")

# discussion_posts → binarize (any post = 1)
k["discussion_norm"] = (k["discussion_posts"] > 0).astype(float)
print(f"    discussion_norm: binarized from discussion_posts  pct_active={k['discussion_norm'].mean():.3f}")

audit["bridge_cols"] = BRIDGE_COLS

# ──────────────────────────────────────────────────────────────────────────────
# Step 7 — Validate: no NaN in bridge columns
# ──────────────────────────────────────────────────────────────────────────────

print("\n  [7] Validating bridge columns …")

for col in BRIDGE_COLS:
    z_null = z[col].isna().sum()
    k_null = k[col].isna().sum()
    assert z_null == 0, f"NaN in zenodo {col}: {z_null}"
    assert k_null == 0, f"NaN in kaggle {col}: {k_null}"

print("    All bridge columns: 0 NaN ✓")

# ──────────────────────────────────────────────────────────────────────────────
# Step 8 — Save outputs
# ──────────────────────────────────────────────────────────────────────────────

print("\n  [8] Saving outputs …")

z.to_csv(ZENODO_OUT, index=True, index_label="zenodo_row_idx")
k.to_csv(KAGGLE_OUT, index=True, index_label="kaggle_row_idx")

print(f"    zenodo_decoded.csv    : {z.shape}  → {ZENODO_OUT}")
print(f"    kaggle_normalized.csv : {k.shape}  → {KAGGLE_OUT}")

audit["zenodo"]["output_shape"]  = list(z.shape)
audit["kaggle"]["output_shape"]  = list(k.shape)
audit["status"] = "COMPLETE"

def _json_safe(obj):
    """Recursively convert numpy types to Python native types for JSON."""
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    return obj

with open(AUDIT_PATH, "w") as f:
    json.dump(_json_safe(audit), f, indent=2)

print(f"\n  Audit: {AUDIT_PATH}")
print("  Status: COMPLETE ✓")
