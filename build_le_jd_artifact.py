"""
build_le_jd_artifact.py  (next_proposal_paper/)
─────────────────────────────────────────────────
Root-level entrypoint for building the Latent Engagement Joint Display (LE-JD) artifact.

Validates all prerequisite input files, delegates to S7_le_jd_assembly.py, then
renders two PNGs in this directory:
  le_jd_artifact_sample.png — analytical table (8 representative rows)
  le_jd_artifact_macro.png  — population-level macro view (4 analytical panels)

Outputs
───────
  outputs/data/synthetic/le_jd_enrollment.csv   — 1 row per persona (65 cols)
  outputs/data/synthetic/le_jd_weekly.csv        — 1 row per persona × week (29 cols)
  outputs/metadata/s7_le_jd_audit.json
  le_jd_artifact_sample.png                      — visual sample table (this directory)
  le_jd_artifact_macro.png                       — macro analytical figure (this directory)

Usage
─────
  /Users/rafars/.pyenv/versions/3.9.13/bin/python next_proposal_paper/build_le_jd_artifact.py
"""

import subprocess
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as mcolors
import numpy as np
import pandas as pd

BASE        = Path(__file__).resolve().parent          # next_proposal_paper/
SYN         = BASE / "outputs" / "data" / "synthetic"
S7_SCRIPT   = BASE / "src_syntetic" / "S7_le_jd_assembly.py"
PNG_OUT     = BASE / "le_jd_artifact_sample.png"
PNG_MACRO   = BASE / "le_jd_artifact_macro.png"

COLORS = {
    "Convergence" : "#d4edda",
    "Discordance" : "#f8d7da",
    "Expansion"   : "#fff3cd",
}
COLORS_DARK = {
    "Convergence" : "#28a745",
    "Discordance" : "#dc3545",
    "Expansion"   : "#ffc107",
}

# ── Prerequisite input files ──────────────────────────────────────────────────
PREREQUISITES = {
    "Engagement panel (quantitative strand)":
        BASE / "outputs" / "engagement_panel_weekly.csv",
    "LLM survey responses (S5 output)":
        SYN / "llm_survey_responses.csv",
    "Persona enrollment bridge (S6 output)":
        SYN / "persona_enrollment_bridge.csv",
    "Student personas (S3 output)":
        SYN / "student_personas.csv",
}

# ── Pre-flight check ──────────────────────────────────────────────────────────
print("=" * 70)
print("  LE-JD Artifact Builder")
print("=" * 70)
print()
print("  Checking prerequisites …")

missing = []
for label, path in PREREQUISITES.items():
    status = "✓" if path.exists() else "✗ MISSING"
    print(f"    [{status}]  {label}")
    if not path.exists():
        missing.append((label, path))

print()

if missing:
    print("  ERROR — missing input files:")
    for label, path in missing:
        print(f"    {label}")
        print(f"      → {path}")
    print()
    print("  Run the pipeline stages that produce these files first.")
    print("  See proposal_artifact.md §3 (Step 0) for details.")
    sys.exit(1)

# ── Run S7 ────────────────────────────────────────────────────────────────────
print("  All prerequisites satisfied — launching S7_le_jd_assembly.py …")
print("=" * 70)
print()

result = subprocess.run([sys.executable, str(S7_SCRIPT)], check=False)

print()
print("=" * 70)
if result.returncode != 0:
    print(f"  BUILD FAILED — S7 exited with code {result.returncode}")
    print("=" * 70)
    sys.exit(result.returncode)

# ── Generate PNG sample ───────────────────────────────────────────────────────
print("  Generating PNG sample …")

ENROLL_CSV = SYN / "le_jd_enrollment.csv"
df = pd.read_csv(ENROLL_CSV)

# Select 8 representative rows (stratified by meta-inference verdict)
sample_rows = []
for verdict in ["Convergence", "Discordance", "Expansion"]:
    subset = df[df["meta_inference_verdict"] == verdict]
    n = 3 if verdict == "Convergence" else 2
    if not subset.empty:
        sample_rows.append(subset.head(n))
sample = pd.concat(sample_rows, ignore_index=True).head(8)

# Columns to display (one per LE-JD column)
display_cols = {
    "Student"        : "persona_name",
    "Archetype"      : "persona_archetype_label",
    "Behavioral\nIndicators (Col 1)": "behavioral_profile",
    "Latent State\n(Col 2)"         : "engagement_state_modal",
    "Mechanism\n(Col 3)"            : "dominant_themes",
    "Meta-Inference\n(Col 4)"       : "meta_inference_verdict",
}

tbl = pd.DataFrame(
    {header: sample[col].astype(str).str[:45] for header, col in display_cols.items()}
)

# Truncate dominant_themes for readability
mech_col = "Mechanism\n(Col 3)"
tbl[mech_col] = tbl[mech_col].apply(
    lambda v: " | ".join(v.split("|")[:3]) + ("…" if len(v.split("|")) > 3 else "")
)

# ── Layout ────────────────────────────────────────────────────────────────────
n_rows, n_cols = tbl.shape
fig_h = 1.6 + n_rows * 0.55
fig, ax = plt.subplots(figsize=(18, fig_h))
ax.axis("off")

# Title
fig.suptitle(
    "Latent Engagement Joint Display (LE-JD) — Artifact Sample",
    fontsize=14, fontweight="bold", y=0.98
)
fig.text(
    0.5, 0.93,
    "One row per student persona  |  Col 1: Behavioral Indicators  |  Col 2: Latent Engagement State  |  "
    "Col 3: Qualitative Mechanism  |  Col 4: Meta-Inference",
    ha="center", fontsize=8, color="#555555"
)

# Verdict colours (use module-level COLORS dict)
cell_colors = []
header_color = "#343a40"
for _, row in tbl.iterrows():
    verdict = row["Meta-Inference\n(Col 4)"]
    row_bg  = COLORS.get(verdict, "#ffffff")
    cell_colors.append([row_bg] * n_cols)

col_widths = [0.12, 0.14, 0.24, 0.10, 0.28, 0.12]

table = ax.table(
    cellText   = tbl.values,
    colLabels  = tbl.columns.tolist(),
    cellLoc    = "left",
    loc        = "center",
    colWidths  = col_widths,
    cellColours= cell_colors,
)
table.auto_set_font_size(False)
table.set_fontsize(8)
table.scale(1, 2.0)

# Style header
for col_idx in range(n_cols):
    cell = table[(0, col_idx)]
    cell.set_facecolor(header_color)
    cell.set_text_props(color="white", fontweight="bold", fontsize=8)

# Legend
patches = [
    mpatches.Patch(color=COLORS["Convergence"], label="Convergence — LMS aligns with self-assessment"),
    mpatches.Patch(color=COLORS["Discordance"], label="Discordance — LMS overstates engagement"),
    mpatches.Patch(color=COLORS["Expansion"],   label="Expansion — self-assessment exceeds LMS signal"),
]
fig.legend(handles=patches, loc="lower center", ncol=3,
           fontsize=8, frameon=True, bbox_to_anchor=(0.5, 0.01))

plt.tight_layout(rect=[0, 0.06, 1, 0.92])
fig.savefig(PNG_OUT, dpi=150, bbox_inches="tight")
plt.close(fig)

print(f"    Saved → {PNG_OUT.relative_to(BASE.parent)}")

# ── Generate macro PNG ─────────────────────────────────────────────────────────
print("  Generating macro analytical figure …")

VERDICTS   = ["Convergence", "Discordance", "Expansion"]
BAR_COLORS = [COLORS_DARK[v] for v in VERDICTS]
HEADER_COL = "#343a40"
ENG_ORDER  = ["low", "medium", "high"]

fig = plt.figure(figsize=(20, 14))
fig.patch.set_facecolor("#f8f9fa")

# Main title
fig.suptitle(
    "LE-JD Integration Analysis — Population-Level Macro View  (N = 1,300 personas)",
    fontsize=15, fontweight="bold", y=0.97,
)
fig.text(
    0.5, 0.935,
    "Meta-inference verdicts across four analytical dimensions  |  "
    "Green = Convergence  ·  Red = Discordance  ·  Yellow = Expansion",
    ha="center", fontsize=9, color="#555555",
)

gs = fig.add_gridspec(2, 2, hspace=0.38, wspace=0.32,
                      left=0.07, right=0.97, top=0.90, bottom=0.08)

# ── Panel helpers ─────────────────────────────────────────────────────────────
def _panel_title(ax, title, subtitle=""):
    ax.set_title(title, fontsize=11, fontweight="bold", pad=8, color=HEADER_COL)
    if subtitle:
        ax.text(0.5, 1.02, subtitle, transform=ax.transAxes,
                ha="center", fontsize=7.5, color="#666666")

def _add_count_labels(ax, bars_list, totals, fmt="{n}"):
    """Put count label in the centre of each bar segment."""
    for bars in bars_list:
        for bar in bars:
            h = bar.get_height()
            if h < 8:
                continue
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_y() + h / 2,
                fmt.format(n=int(round(h))),
                ha="center", va="center", fontsize=7, color="#333333",
            )

def _stacked_pct(ax, ct, title, subtitle="", xlabel="", max_pct_label=4):
    """Normalised stacked bar (%) for a crosstab df columns=VERDICTS."""
    ct = ct.reindex(columns=VERDICTS, fill_value=0)
    pct = ct.div(ct.sum(axis=1), axis=0) * 100
    x   = np.arange(len(pct))
    bars_list = []
    bottom = np.zeros(len(pct))
    for v, col in zip(VERDICTS, BAR_COLORS):
        vals = pct[v].values
        raw  = ct[v].values
        bars = ax.bar(x, vals, bottom=bottom, color=col,
                      edgecolor="white", linewidth=0.6, label=v)
        # centre labels
        for i, (bar, h, raw_n) in enumerate(zip(bars, vals, raw)):
            if h >= max_pct_label:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_y() + h / 2,
                    f"{raw_n}",
                    ha="center", va="center", fontsize=7.5, color="#1a1a1a",
                )
        bars_list.append(bars)
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels(pct.index, fontsize=8.5, rotation=20, ha="right")
    ax.set_ylabel("% of group", fontsize=8.5)
    ax.set_ylim(0, 105)
    ax.yaxis.grid(True, alpha=0.4, linestyle="--")
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=8.5)
    _panel_title(ax, title, subtitle)
    return bars_list

# ── Panel A: Donut — overall distribution ─────────────────────────────────────
ax_a = fig.add_subplot(gs[0, 0])
ax_a.set_aspect("equal")

total_n = len(df)
sizes   = [df["meta_inference_verdict"].value_counts().get(v, 0) for v in VERDICTS]
wedges, texts, autotexts = ax_a.pie(
    sizes,
    labels     = None,
    colors     = BAR_COLORS,
    autopct    = "%1.1f%%",
    startangle = 90,
    pctdistance= 0.75,
    wedgeprops = {"edgecolor": "white", "linewidth": 2},
)
for at in autotexts:
    at.set_fontsize(11)
    at.set_fontweight("bold")
    at.set_color("#1a1a1a")

# Inner donut hole
centre_circle = plt.Circle((0, 0), 0.52, color="#f8f9fa")
ax_a.add_artist(centre_circle)
ax_a.text(0, 0.06, "N", ha="center", va="center", fontsize=10, color="#555")
ax_a.text(0, -0.12, f"{total_n:,}", ha="center", va="center",
          fontsize=16, fontweight="bold", color=HEADER_COL)

# External labels with counts
for wedge, v, n in zip(wedges, VERDICTS, sizes):
    ang  = (wedge.theta2 + wedge.theta1) / 2
    x    = 1.22 * np.cos(np.deg2rad(ang))
    y    = 1.22 * np.sin(np.deg2rad(ang))
    ax_a.text(x, y, f"{v}\n({n})", ha="center", va="center",
              fontsize=8.5, fontweight="bold",
              color=COLORS_DARK[v])

legend_patches = [
    mpatches.Patch(color=COLORS_DARK[v],
                   label=f"{v}: {n} ({n/total_n*100:.1f}%)")
    for v, n in zip(VERDICTS, sizes)
]
ax_a.legend(handles=legend_patches, loc="lower center",
            bbox_to_anchor=(0.5, -0.14), fontsize=8, frameon=False, ncol=1)
_panel_title(ax_a,
             "A — Overall Meta-Inference Distribution",
             "Population-level integration verdict  |  N = 1,300 personas")

# ── Panel B: Stacked bar — by archetype ──────────────────────────────────────
ax_b = fig.add_subplot(gs[0, 1])
ct_arch = pd.crosstab(df["persona_archetype_label"], df["meta_inference_verdict"])
# Sort by total desc
ct_arch = ct_arch.loc[ct_arch.sum(axis=1).sort_values(ascending=False).index]
_stacked_pct(ax_b, ct_arch,
             "B — Verdict by Student Archetype",
             "Each bar = 100%  |  counts inside segments",
             xlabel="Persona archetype")

# ── Panel C: 3×3 integration matrix ──────────────────────────────────────────
ax_c = fig.add_subplot(gs[1, 0])

ct_3x3 = pd.crosstab(df["engagement_state_modal"],
                      df["overall_engagement_self_assessment"])
ct_3x3 = ct_3x3.reindex(index=ENG_ORDER, columns=ENG_ORDER, fill_value=0)

# Dominant verdict per cell
ct_dom = pd.crosstab(
    df["engagement_state_modal"], df["overall_engagement_self_assessment"],
    values=df["meta_inference_verdict"],
    aggfunc=lambda x: x.mode().iloc[0] if len(x) > 0 else "—",
)
ct_dom = ct_dom.reindex(index=ENG_ORDER, columns=ENG_ORDER, fill_value="—")

n_r, n_c = ct_3x3.shape
cell_w, cell_h = 1.0, 1.0
for ri, row_label in enumerate(ENG_ORDER):
    for ci, col_label in enumerate(ENG_ORDER):
        count   = ct_3x3.loc[row_label, col_label]
        verdict = ct_dom.loc[row_label, col_label]
        bg      = COLORS.get(verdict, "#eeeeee")
        dc      = COLORS_DARK.get(verdict, "#888888")
        rect = plt.Rectangle(
            (ci * cell_w, (n_r - 1 - ri) * cell_h),
            cell_w, cell_h,
            facecolor=bg, edgecolor="white", linewidth=2,
        )
        ax_c.add_patch(rect)
        ax_c.text(ci + 0.5, (n_r - 1 - ri) + 0.62, f"n = {count}",
                  ha="center", va="center", fontsize=9.5, fontweight="bold",
                  color="#1a1a1a")
        ax_c.text(ci + 0.5, (n_r - 1 - ri) + 0.34, verdict,
                  ha="center", va="center", fontsize=8,
                  color=dc, fontstyle="italic")

ax_c.set_xlim(0, n_c * cell_w)
ax_c.set_ylim(0, n_r * cell_h)
ax_c.set_xticks([i + 0.5 for i in range(n_c)])
ax_c.set_xticklabels([f"Self: {l}" for l in ENG_ORDER], fontsize=9)
ax_c.set_yticks([i + 0.5 for i in range(n_r)])
ax_c.set_yticklabels([f"LMS: {l}" for l in reversed(ENG_ORDER)], fontsize=9)
ax_c.tick_params(length=0)
ax_c.spines[:].set_visible(False)

# Diagonal label
for i in range(n_r):
    ax_c.text((n_c - 1 - i) + 0.5 + 0.35, i + 0.5 + 0.2,
              "▲", ha="center", va="center", fontsize=7, color="#28a745", alpha=0.5)

_panel_title(ax_c,
             "C — Integration Matrix  (LMS vs Self-Assessment)",
             "Cell = dominant verdict  |  diagonal = Convergence  |  "
             "LMS > self → Discordance  |  self > LMS → Expansion")

# ── Panel D: Stacked bar — by final result ────────────────────────────────────
ax_d = fig.add_subplot(gs[1, 1])
RESULT_ORDER = ["Distinction", "Pass", "Fail", "Withdrawn"]
ct_res = pd.crosstab(df["final_result"], df["meta_inference_verdict"])
ct_res = ct_res.reindex(RESULT_ORDER, fill_value=0)
_stacked_pct(ax_d, ct_res,
             "D — Verdict by Academic Outcome",
             "Each bar = 100%  |  counts inside segments",
             xlabel="Final result")

# ── Global legend ─────────────────────────────────────────────────────────────
legend_handles = [
    mpatches.Patch(color=COLORS_DARK[v],
                   label=f"{v} — {['LMS ≈ self-assessment', 'LMS overstates engagement', 'self-assessment exceeds LMS'][i]}")
    for i, v in enumerate(VERDICTS)
]
fig.legend(handles=legend_handles, loc="lower center", ncol=3,
           fontsize=9, frameon=True, bbox_to_anchor=(0.5, 0.005),
           framealpha=0.9, edgecolor="#cccccc")

fig.savefig(PNG_MACRO, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
plt.close(fig)

print(f"    Saved → {PNG_MACRO.relative_to(BASE.parent)}")

# ── Final summary ─────────────────────────────────────────────────────────────
print()
print("  Outputs:")
ENROLL_CSV = SYN / "le_jd_enrollment.csv"
for f in (ENROLL_CSV, SYN / "le_jd_weekly.csv", PNG_OUT, PNG_MACRO):
    if f.exists():
        size_kb = f.stat().st_size // 1024
        print(f"    {f.relative_to(BASE.parent)}  ({size_kb:,} KB)")
print("=" * 70)
print("  LE-JD artifact build COMPLETE ✓")
print("=" * 70)
