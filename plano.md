# Construction Plan — Quantitative Foundation (Quant Foundation)

## Paper: From Clicks to Constructs — Mixed-Methods Engagement Modeling (OULAD + DBN)

---

## Final Objective

Produce a single CSV file:

```
next_proposal_paper/outputs/engagement_panel_weekly.csv
```

This file is the **direct input to the DBN** and to the **LE-JD** construction.
It is a person×week panel with behavioral, demographic, and assessment indicators,
plus the discretized engagement state.

---

## Directory Structure to Create

```
next_proposal_paper/
├── plano.md                          ← this file
├── proposal.md                       ← paper proposal
├── outputs/
│   ├── engagement_panel_weekly.csv   ← FINAL OUTPUT
│   ├── data/                         ← intermediate data (DuckDB + parquets)
│   │   └── engagement.duckdb
│   └── metadata/
│       ├── environment_summary.json
│       ├── pipeline_audit.json
│       └── column_schema.json
└── src/
    ├── P0_foundation.py              ← infra, paths, packages
    ├── P1_ingestion.py               ← reading OULAD CSVs → DuckDB
    ├── P2_panel_builder.py           ← person-week panel construction
    ├── P3_indicators.py              ← engagement indicator derivation
    ├── P4_assessment_join.py         ← studentAssessment integration
    ├── P5_demographics_join.py       ← studentInfo integration
    └── P6_export.py                  ← final validation and CSV export
```

---

## Final CSV Schema

| Column | Source | Type | Description |
|---|---|---|---|
| `id_student` | studentVle | int | Student identifier |
| `code_module` | studentVle | str | Module (e.g., AAA, BBB) |
| `code_presentation` | studentVle | str | Presentation (e.g., 2013J) |
| `week` | derived | int | Week relative to module start (0-based) |
| `total_clicks` | studentVle.sum_click | int | Total clicks in the week |
| `active_days` | studentVle.date | int | Distinct days with activity in the week |
| `log_clicks` | derived | float | log1p(total_clicks) |
| `click_intensity` | derived | float | total_clicks / max_clicks_in_module_week |
| `recency` | derived | int | Weeks since last activity (0 = active this week) |
| `streak` | derived | int | Consecutive active weeks with ≥1 click up to this week |
| `cumulative_clicks` | derived | int | Cumulative click total up to current week |
| `assessment_score` | studentAssessment.score | float | Score of nearest assessment in the week (or NaN) |
| `submission_timeliness` | studentAssessment | float | Days before(+)/after(−) deadline; NaN if no assessment |
| `has_assessment_this_week` | derived | int | 1 if an assessment was submitted this week |
| `age_band` | studentInfo | str | Age bracket (0-35, 35-55, 55<=) |
| `gender` | studentInfo | str | M / F |
| `highest_education` | studentInfo | str | Highest education level |
| `imd_band` | studentInfo | str | IMD band (socioeconomic indicator) |
| `num_of_prev_attempts` | studentInfo | int | Previous attempts at the module |
| `studied_credits` | studentInfo | int | Credits studied by the student |
| `final_result` | studentInfo | str | Pass / Fail / Withdrawn / Distinction |
| `engagement_state` | derived | str | **high / medium / low** — discretized state for DBN |
| `enrollment_id` | derived | str | Composite key: `{id_student}_{code_module}_{code_presentation}` |

> **Note:** `engagement_state` is derived via contextual tertile discretization of `log_clicks`
> within each `code_module × code_presentation × week`. This is the central observable
> node for the DBN.

---

## Implementation Checklist

### PHASE 0 — Infrastructure (`P0_foundation.py`)

- [x] **0.1** Define `PROJECT_ROOT` relative to `next_proposal_paper/`
- [x] **0.2** Define all `Path` constants (`OUTPUT_DIR`, `DATA_DIR`, `METADATA_DIR`, `SRC_DIR`)
- [x] **0.3** Map `OULAD_DATA_DIR` to `../content/` (reuse CSVs already downloaded by the previous paper — **read-only**)
- [x] **0.4** Define `REQUIRED_SOURCE_FILES` with the 5 required OULAD files
- [x] **0.5** Implement source file existence check (raise if absent)
- [x] **0.6** List required packages for this foundation (subset of original requirements.txt):
  - `pandas`, `numpy`, `duckdb`, `pyarrow`, `scikit-learn`
  - Remove survival analysis dependencies (scikit-survival, pycox, lifelines, torch) — NOT needed here
- [x] **0.7** Implement `ensure_packages()` — installs only what is missing (reuse `install_package()` logic from A1)
- [x] **0.8** Define `SEED = 42`, `ENROLLMENT_KEY = ["id_student", "code_module", "code_presentation"]`
- [x] **0.9** Implement `log_stage_start()` / `log_stage_end()` (copy pattern from A1)
- [x] **0.10** Create `outputs/metadata/environment_summary.json` with package versions and timestamp

---

### PHASE 1 — OULAD Ingestion (`P1_ingestion.py`)

- [x] **1.1** Initialize DuckDB at `outputs/data/engagement.duckdb`
- [x] **1.2** Load `studentVle.csv` → DuckDB table `raw_student_vle` (10,655,280 rows)
- [x] **1.3** Load `studentInfo.csv` → DuckDB table `raw_student_info` (32,593 rows, unique key ✓)
- [x] **1.4** Load `studentAssessment.csv` → DuckDB table `raw_student_assessment` (173,912 rows)
- [x] **1.5** Load `assessments.csv` → DuckDB table `raw_assessments` (206 rows)
- [x] **1.6** Load `courses.csv` → DuckDB table `raw_courses` (22 rows)
- [x] **1.7** Save ingestion summary to `outputs/metadata/pipeline_audit.json`

---

### PHASE 2 — Person-Week Panel Construction (`P2_panel_builder.py`)

- [x] **2.1** Convert `date` column to `week = date // 7`; dates < 0 excluded
- [x] **2.2** Aggregate `studentVle` at `enrollment_key + week` level → 592,331 rows
- [x] **2.3** Build complete week grid per enrollment (weeks 0..max_week)
- [x] **2.4** Left join grid with activity → zeros where no clicks
- [x] **2.5** Create composite `enrollment_id`
- [x] **2.6** Save `panel_base` table to DuckDB (1,212,577 rows, 51.15% sparsity)
- [x] **2.7** Audit: 32,593 enrollments, weeks 0–38, avg week 18.15

---

### PHASE 3 — Engagement Indicators (`P3_indicators.py`)

- [x] **3.1** `log_clicks = ln(total_clicks + 1)` ✓ range [0.0, 8.85]
- [x] **3.2** `click_intensity` normalized per module×presentation×week window ✓ range [0.0, 1.0]
- [x] **3.3** `cumulative_clicks` per enrollment ordered by week ✓ max=23,481
- [x] **3.4** `recency` via window function ✓ range [0, 39], avg 5.84
- [x] **3.5** `streak` via gaps-and-islands ✓ range [0, 39], avg 3.88
- [x] **3.6** `engagement_state` via contextual tertiles: low=67.46%, medium=16.28%, high=16.26% ✓
- [x] **3.7** Validation: no NULL in engagement_state ✓

---

### PHASE 4 — Assessment Integration (`P4_assessment_join.py`)

- [x] **4.1** Join `raw_student_assessment` × `raw_assessments` via `id_assessment`
- [x] **4.2** `week_submitted = date_submitted // 7`
- [x] **4.3** Multiple assessments per week: keep highest weight (ROW_NUMBER)
- [x] **4.4** `submission_timeliness = deadline_day - date_submitted`
- [x] **4.5** `has_assessment_this_week ∈ {0, 1}` ✓
- [x] **4.6** Weeks without assessment: NaN in score/timeliness, 0 in flag ✓
- [x] **4.7** Table `panel_with_assessment` in DuckDB (1,212,577 rows, 12.25% with assessment)

---

### PHASE 5 — Demographics Integration (`P5_demographics_join.py`)

- [x] **5.1** Left join `panel_with_assessment` × `raw_student_info` via `ENROLLMENT_KEY`
- [x] **5.2** 0 orphan enrollments ✓
- [x] **5.3** `final_result` validated: Pass/Fail/Withdrawn/Distinction ✓
- [x] **5.4** `imd_band` preserved as ordinal string (11 distinct values)
- [x] **5.5** `num_of_prev_attempts >= 0` and `studied_credits > 0` ✓
- [x] **5.6** Table `panel_with_demographics` in DuckDB (23 columns)

---

### PHASE 6 — Final Export and Validation (`P6_export.py`)

- [x] **6.1** 23 final columns selected in schema order
- [x] **6.2** Sorted by `enrollment_id ASC, week ASC`
- [x] **6.3** All validations passed ✓
  - [x] No null `enrollment_id`
  - [x] `week >= 0` for all rows
  - [x] `total_clicks >= 0` with no nulls
  - [x] `engagement_state` ∈ {low, medium, high}, 0 nulls
  - [x] `final_result` ∈ valid categories
  - [x] 0 duplicate rows
- [x] **6.4** `column_schema.json` generated (23 columns documented)
- [x] **6.5** `pipeline_audit.json` complete with final summary
- [x] **6.6** `engagement_panel_weekly.csv` exported — 1,212,577 rows, 152.57 MB ✓

---

## Design Decisions

| Decision | Rationale |
|---|---|
| Reuse `content/` from the previous paper (read-only) | OULAD CSVs are already downloaded and validated — no need to re-download |
| DuckDB as intermediate storage | Consistent with previous paper; allows efficient SQL queries over ~1M+ row panels |
| Do not reuse `outputs_benchmark_survival/` | Full isolation — zero dependency on previous paper's outputs |
| Remove survival analysis packages from infra | This foundation does not need scikit-survival, pycox, lifelines, torch — lighter and faster environment |
| Contextual tertile discretization (per module×week) | Avoids comparative bias between modules with very different click volumes; required for DBN with meaningful categorical states |
| Include zero-click weeks in the grid | DBN needs absence of activity as a signal — series with gaps produce biased transition estimates |
| `assessment_score` as NaN when no assessment | DBN can use this as partial evidence — the framework handles missing data via inference |

---

## Execution Sequence

```
P0_foundation.py
    ↓
P1_ingestion.py
    ↓
P2_panel_builder.py
    ↓
P3_indicators.py
    ↓
P4_assessment_join.py
    ↓
P5_demographics_join.py
    ↓
P6_export.py
    ↓
outputs/engagement_panel_weekly.csv  ✓
```

Each Px script can be run independently as long as the previous stage has completed
(reads from DuckDB, not from in-memory variables).

---

## What NOT to Do Here

- ❌ Train the DBN — that is a later stage
- ❌ Qualitative analysis — this plan only covers the quant foundation
- ❌ Copy or modify any file outside `next_proposal_paper/`
- ❌ Use `outputs_benchmark_survival/` outputs as input
- ❌ Install or reconfigure packages that could break the previous paper's environment
