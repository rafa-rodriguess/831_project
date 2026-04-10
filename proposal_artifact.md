# Latent Engagement Joint Display (LE-JD): Artifact Definition, Data Mapping, and Construction Guide

> **Status:** Files fully assembled as of April 2026.
> This document describes the LE-JD artifact, maps every CSV column to its role in the artifact, and provides step-by-step guidance on how to reproduce it from the pipeline outputs.

---

## Quick Reference — Output Files

| File | Rows | Cols | Granularity | Purpose |
|---|---|---|---|---|
| `outputs/data/synthetic/le_jd_enrollment.csv` | 1,300 | 63 | 1 row per persona | Primary LE-JD artifact |
| `outputs/data/synthetic/le_jd_weekly.csv` | 48,042 | 29 | 1 row per persona × week | Temporal LE-JD for trajectory analysis |
| `outputs/metadata/s7_le_jd_audit.json` | — | — | — | Build audit and column manifest |

**Assembly script:** `next_proposal_paper/src_syntetic/S7_le_jd_assembly.py`

---

## 1. The Artifact: Latent Engagement Joint Display (LE-JD)

The **Latent Engagement Joint Display (LE-JD)** is the central integrative artifact of this study. It is conceived as a structured analytical device—not merely a reporting table—that explicitly aligns quantitative and qualitative evidence to produce mixed methods meta-inferences.

The LE-JD is organized around **four analytical columns**, each representing a distinct level of evidence:

| Column | Level | Source |
|---|---|---|
| **Behavioral Indicators** | Observable | Quantitative strand — directly from CSV |
| **Latent Engagement State** | Inferred | Quantitative strand — DBN output |
| **Mechanism** | Explanatory | Qualitative strand — thematic analysis of interviews |
| **Meta-Inference** | Integrated | Integration process — convergence, expansion, or discordance |

The artifact is designed according to four methodological principles (Guetterman et al., 2021):
1. Integration must be **explicit**, not implied
2. The display must facilitate **meta-inferences**, not just side-by-side presentation
3. Both strands must be represented at a **comparable level of aggregation**
4. The display must **reduce cognitive burden**, not introduce unnecessary complexity

The LE-JD functions both as a **reporting mechanism** (presenting integrated findings) and as an **analytical tool** (the act of populating it generates the meta-inferences).

---

## 2. Structure of the LE-JD

Each row of the LE-JD represents a **case unit**: an enrollment-period (a student in a given module, at a given point in time). Rows may be aggregated at different granularities depending on the analytical purpose:

- **Week-level**: for temporal pattern analysis (e.g., drop-off detection)
- **Phase-level**: for trajectory segmentation (e.g., early, mid, late course)
- **Enrollment-level**: for overall profile comparison

A simplified prototype of the artifact:

| Student | Week | Behavioral Profile | Latent State | Mechanism | Meta-Inference |
|---|---|---|---|---|---|
| S1 | W3 | High clicks, low submission | Medium | Cognitive overload | Engagement without effective processing |
| S2 | W5 | No activity, high recency | Low | Low perceived value | Disengagement driven by utility perception |
| S3 | W2–6 | Stable activity, high streak | High | Habit formation | Sustained engagement through routine |

---

## 3. How the Quantitative Data Constructs the LE-JD

### 3.1 Source File

**`outputs/engagement_panel_weekly.csv`**
- 1,212,577 rows — one per enrollment × week
- 32,593 unique enrollments
- 39 unique weeks (0–38)
- 23 columns

### 3.2 Column 1 — Behavioral Indicators (Observable Layer)

These columns feed directly into the **Behavioral Indicators** column of the LE-JD. They are displayed as an aggregated behavioral profile per case unit, not as raw individual values.

| CSV Column | Data Type | Range / Values | Role in LE-JD |
|---|---|---|---|
| `total_clicks` | float | 0 – 6,991 | Primary activity signal; defines presence or absence of engagement |
| `active_days` | int | 0 – 7 | Within-week dispersion; distinguishes binge vs. distributed activity |
| `log_clicks` | float | 0 – 8.85 | Smoothed activity for DBN input; reduces skew from extreme values |
| `click_intensity` | float | 0 – 1 | Relative intensity vs. peers in the same module/presentation/week |
| `recency` | int | 0 – 39 | Weeks since last active; captures inactivity duration |
| `streak` | int | 0 – 39 | Consecutive active weeks; captures habit formation in behavioral trace |
| `cumulative_clicks` | float | 0 – 23,481 | Cumulative engagement trajectory; drives temporal plots in the artifact |
| `has_assessment_this_week` | int | 0 / 1 | Flags deadline-driven activity windows |
| `assessment_score` | int | 0 – 100 (87.75% null) | Academic outcome of engagement; null when no assessment submitted |
| `submission_timeliness` | int | −174 to +244 days | Deadline management behavior; positive = early, negative = late |

**Aggregation rule for the LE-JD:** individual columns are collapsed into a **behavioral profile label** (e.g., "High clicks, low submission, early streak") to maintain comparability with the qualitative strand, which operates at theme level. Raw values support the label and appear in supplemental visualizations.

### 3.3 Column 2 — Latent Engagement State (Inferred Layer)

The latent state is **not a column in the CSV**. It is the output of the Dynamic Bayesian Network (DBN) trained on the observable columns above. The CSV supports this column in two ways:

**DBN Input Variables** (columns that enter the DBN as observed nodes):

| CSV Column | Node Type in DBN | Rationale |
|---|---|---|
| `log_clicks` | Continuous evidence | Primary behavioral signal |
| `click_intensity` | Continuous evidence | Contextual normalization |
| `recency` | Continuous evidence | Captures inactivity dynamics |
| `streak` | Continuous evidence | Captures persistence dynamics |
| `active_days` | Continuous evidence | Within-week distribution |
| `has_assessment_this_week` | Binary evidence | Deadline context |
| `assessment_score` | Continuous evidence (sparse) | Academic performance signal |
| `submission_timeliness` | Continuous evidence (sparse) | Behavioral response to deadlines |

**DBN Output (Latent State):** a discrete state per enrollment × week — e.g., `{Disengaged, Low-Engagement, Moderate-Engagement, High-Engagement}`. This state populates Column 2 of the LE-JD.

**Validation baseline:** `engagement_state` (CSV column) is a pre-computed proxy via contextual NTILE(3) on `log_clicks`. It is used to **validate DBN output consistency**, not as the latent state itself.

| CSV Column | Role |
|---|---|
| `engagement_state` | Proxy baseline {low, medium, high}; validation reference only |

**Correct flow:**

```
CSV observable columns → DBN training → Latent State per row → LE-JD Column 2
                                                 ↑
                          engagement_state (CSV) compared here for consistency check
```

### 3.4 Stratification and Case Selection Context

These columns do not appear as LE-JD columns but are essential for **selecting cases**, **stratifying profiles**, and **interpreting meta-inferences** in context.

| CSV Column | Values | Use in LE-JD |
|---|---|---|
| `enrollment_id` | 32,593 unique | Row identifier; links quantitative profile to interview participant |
| `week` | 0 – 38 | Temporal axis for trajectory plots and phase segmentation |
| `code_module` | AAA–GGG (7 modules) | Stratification by subject area |
| `code_presentation` | 2013B/J, 2014B/J | Stratification by cohort |
| `final_result` | Pass / Fail / Withdrawn / Distinction | Outcome lens for interpreting engagement trajectories |
| `age_band` | 0-35 / 35-55 / 55≤ | Demographic segmentation |
| `gender` | F / M | Demographic segmentation |
| `highest_education` | 5 categories | Prior educational capital |
| `imd_band` | 10 categories (3.41% null) | Socioeconomic context |
| `num_of_prev_attempts` | 0 – 6 | Experience with the module |
| `studied_credits` | 30 – 655 | Student workload context |

---

## 4. Columns 3 and 4 — Qualitative Strand and Integration

### 4.1 Column 3 — Mechanism (Qualitative Strand)

**Source (synthetic phase):** `outputs/data/synthetic/llm_survey_responses.csv`  
Produced by S5 (`S5_run_llm_survey.py`) — 1,300 Claude-generated persona responses.

| Field | Column in le_jd_enrollment.csv | Description |
|---|---|---|
| `dominant_themes` | `dominant_themes` | Pipe-separated engagement themes (e.g., `offline-study\|deadline-driven\|competing-responsibilities`) |
| `overall_engagement_self_assessment` | `overall_engagement_self_assessment` | Self-reported level: `low / medium / high` |
| Q1–Q24 | `Q1` … `Q24` | Full narrative responses to engagement survey questions |

**Themes surfaced in the synthetic corpus:**  
`offline-study`, `deadline-driven`, `resource-constraints`, `habit-formation`, `cognitive-overload`, `low-perceived-value`, `strategic-compliance`, `competing-responsibilities`, `auditory-learning-preference`, `peer-interaction`, `anxiety-driven-activity`

**Future step (real interviews):** in the empirical phase, `dominant_themes` and `Q1–Q24` will be replaced by thematic codes derived from semi-structured interviews with enrolled students. The column schema and meta-inference logic remain identical.

### 4.2 Column 4 — Meta-Inference (Integration Output)

Meta-inference is generated by `meta_inference()` in S7, comparing quantitative and qualitative engagement signals:

| Verdict | Condition | Interpretation |
|---|---|---|
| **Convergence** | `engagement_state_modal` == `overall_engagement_self_assessment` | Behavioral LMS data aligns with student experience. Both strands tell the same story. |
| **Expansion** | self-assessed engagement > LMS engagement | Student reports more engagement than LMS can measure. Suggests invisible activity: offline study, peer learning, intrinsic motivation. |
| **Discordance** | LMS engagement > self-assessed engagement | LMS registers activity that does not translate to meaningful engagement. Suggests surface compliance, anxiety-driven clicks, or ritual without intention. |

**Distribution in the synthetic corpus (n = 1,300):**

| Verdict | n | % |
|---|---|---|
| Convergence | 551 | 42.4% |
| Discordance | 426 | 32.8% |
| Expansion | 323 | 24.8% |

The `meta_inference_rationale` column in `le_jd_enrollment.csv` provides a one-sentence textual rationale per case, citing both the quantitative state and top-3 qualitative themes.

---

## 5. Coverage Summary

| LE-JD Component | Covered by | Status |
|---|---|---|
| Behavioral Indicators | 10 columns from `engagement_panel_weekly.csv` | ✅ Complete |
| Behavioral Profile Label (Col 1) | `behavioral_profile` / `behavioral_profile_week` | ✅ Generated by S7 |
| Latent Engagement State — DBN inputs | 8 columns from `engagement_panel_weekly.csv` | ✅ Complete |
| Latent Engagement State — proxy output | `engagement_state_modal` (NTILE-3) | ✅ Proxy populated; DBN is next step |
| Latent Engagement State — DBN output | To be produced by DBN training | ⚙️ Placeholder |
| Mechanism — themes | `dominant_themes` from LLM survey (S5) | ✅ Synthetic — 1,300 personas |
| Mechanism — narrative | Q1–Q24 from LLM survey (S5) | ✅ Synthetic — 1,300 personas |
| Meta-Inference — verdict | `meta_inference_verdict` (rule-based) | ✅ Generated by S7 |
| Meta-Inference — rationale | `meta_inference_rationale` | ✅ Generated by S7 |
| Stratification context | 11 columns (module, result, demographics) | ✅ Complete |

---

## 6. Concrete LE-JD Row Example — P0001 (Silvia Greco)

The following is a real populated row from `le_jd_enrollment.csv`:

| LE-JD Field | Value |
|---|---|
| **Persona** | P0001 — Silvia Greco |
| **Archetype** | Resilient Climber |
| **Enrollment** | `528971_DDD_2013J` (rank-1 match, score 0.988) |
| **Module / Cohort** | DDD / 2013J |
| **Final Result** | — *(from OULAD enrollment)* |
| **n_weeks** | 38 |
| **Col 1 — Behavioral Profile** | `medium-clicks \| sustained \| early-submitter \| mid-score` |
| `total_clicks_mean` | — |
| `streak_max` | — |
| `assessment_score_mean` | — |
| **Col 2 — Latent State** | `medium` *(proxy: engagement_state modal)* |
| **Col 3 — Mechanism** | `offline-study \| resource-constraints \| deadline-driven \| auditory-learning-preference \| competing-responsibilities` |
| `overall_engagement_self_assessment` | `medium` |
| **Col 4 — Meta-Inference** | **Convergence** |
| `meta_inference_rationale` | *LMS engagement (medium) aligns with self-assessed engagement (medium). Themes: offline-study, resource-constraints, deadline-driven.* |

---

## 7. How to Reproduce the LE-JD (Step-by-Step)

### Prerequisites — Pipeline must be complete through S6

| Stage | Script | Output | Status |
|---|---|---|---|
| S3 | `S3_persona_assembly.py` | `student_personas.csv` (1,300×38) | ✅ |
| S4 | `S4_generate_prompts.py` | `student_personas_with_prompts.csv` (1,300×40) | ✅ |
| S5 | `S5_run_llm_survey.py` | `llm_survey_responses.csv` (1,300×27) + `llm_responses/P*.json` | ✅ |
| S6 | `S6_persona_enrollment_bridge.py` | `persona_enrollment_bridge.csv` (13,000×8) | ✅ |

Also required: `outputs/engagement_panel_weekly.csv` (OULAD quantitative panel, 1,212,577 rows).

---

### Step 1 — Run S7 to assemble the artifact

```bash
cd next_proposal_paper/src_syntetic
/Users/rafars/.pyenv/versions/3.9.13/bin/python S7_le_jd_assembly.py
```

Expected output:

```
[1] Loading sources …  quali (1300×27) | bridge (13000×8) | personas (1300×39)
[2] Loading engagement_panel_weekly (rank-1 enrollments only) …  29,363 rows
[3] Aggregating quant to enrollment level …  788 enrollments
[4] Assembling enrollment-level LE-JD …  (1300, 63)
[5] Assembling weekly LE-JD …  (48042, 29)
[6] Saving …
[7] Summary — Convergence 42.4% | Discordance 32.8% | Expansion 24.8%
Status : COMPLETE ✓
```

---

### Step 2 — Inspect the outputs

**Enrollment-level LE-JD** (one row per persona, best-match enrollment):

```python
import pandas as pd
le = pd.read_csv("outputs/data/synthetic/le_jd_enrollment.csv")
print(le.shape)          # (1300, 63)
print(le.columns.tolist())
le[["persona_id","persona_name","behavioral_profile",
    "engagement_state_modal","dominant_themes","meta_inference_verdict"]].head()
```

**Weekly LE-JD** (temporal panel per persona):

```python
week = pd.read_csv("outputs/data/synthetic/le_jd_weekly.csv")
print(week.shape)        # (48042, 29)
# Single persona trajectory
week[week["persona_id"]=="P0001"].sort_values("week")[
    ["week","behavioral_profile_week","engagement_state","meta_inference_verdict"]
]
```

---

### Step 3 — Column Schema Reference

#### `le_jd_enrollment.csv` (63 columns)

**Context (22 cols):**
`persona_id, persona_name, persona_archetype_label, gender, age, learning_style, risk_level, final_grade, engagement_level, motivation_level, stress_level_label, intervention_type, enrollment_id, code_module, code_presentation, age_band, gender_oulad, final_result, match_score, result_match, engagement_match, overall_engagement_self_assessment`

**Col 1 — Behavioral Indicators (13 cols):**
`behavioral_profile` *(label)*, `total_clicks_sum`, `total_clicks_mean`, `active_days_mean`, `log_clicks_mean`, `click_intensity_mean`, `recency_last`, `streak_max`, `cumulative_clicks_final`, `assessment_score_mean`, `submission_timeliness_mean`, `has_assessment_weeks`, `n_weeks`

**Col 2 — Latent Engagement State (1 col):**
`engagement_state_modal` ← modal engagement state across all weeks (`low / medium / high`)

**Col 3 — Mechanism (25 cols):**
`dominant_themes`, `Q1` … `Q24`

**Col 4 — Meta-Inference (2 cols):**
`meta_inference_verdict` (`Convergence / Expansion / Discordance`), `meta_inference_rationale`

#### `le_jd_weekly.csv` (29 columns)

`persona_id, persona_name, persona_archetype_label, enrollment_id, code_module, code_presentation, week, final_result, final_grade, risk_level, learning_style, intervention_type, match_score` *(context)*  
`behavioral_profile_week, total_clicks, active_days, log_clicks, click_intensity, recency, streak, cumulative_clicks, assessment_score, submission_timeliness, has_assessment_this_week` *(Col 1)*  
`engagement_state` *(Col 2)*  
`dominant_themes, overall_engagement_self_assessment` *(Col 3)*  
`meta_inference_verdict, meta_inference_rationale` *(Col 4)*

---

### Step 4 — Replace proxy with DBN output (future)

When the Dynamic Bayesian Network is trained:

1. Run the DBN on `engagement_panel_weekly.csv` using the 8 input columns listed in Section 3.3.
2. The DBN produces a discrete latent state per `enrollment_id × week`.
3. Replace `engagement_state_modal` in `le_jd_enrollment.csv` and `engagement_state` in `le_jd_weekly.csv` with the DBN output.
4. Re-run S7 (or apply the update in-place) to regenerate `meta_inference_verdict` against the true inferred state.

The `engagement_state` column in `engagement_panel_weekly.csv` remains as the validation reference (not the artifact value).

---

## 8. Non-Blocking Gap: `assessment_type`

The CSV does not include `assessment_type` (TMA / CMA / Exam). This field would enrich the interpretation of `submission_timeliness` and `assessment_score` within the LE-JD (e.g., a late TMA has a different meaning than a late Exam). The field is available in `raw_assessments` inside the project DuckDB and can be added via a supplemental join step (P6b) without modifying the existing CSV.
