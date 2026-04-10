# Plan: Synthetic Student Profile Generation
## Pipeline src_syntetic — Stage S0 → S3

> **Final objective:** produce a CSV with N synthetic student profiles, where each profile combines complementary variables from two external sources (Zenodo + Kaggle) across 5 analytical dimensions, with no shared primary key, via clustering in the bridge-construct space.

---

## Isolation

This pipeline is **completely isolated** from the OULAD pipeline (`src/`). It does not read from or write to `outputs/data/engagement.duckdb` or `engagement_panel_weekly.csv`. It reads only from `outputs/data/raw/` and writes to `outputs/data/synthetic/`.

---

## Data Sources

| ID | Source | File | Records | Columns |
|---|---|---|---|---|
| Zenodo | NAJEM et al. (2025), Zenodo DOI 10.5281/zenodo.16459132 | `merged_dataset.csv` | 14,003 | 16 (all int64, label-encoded) |
| Kaggle | programmer3, Psychological CBI Student Dataset | `psychological_cbi_dataset.csv` | 1,300 | 19 (mixed: float, int, object) |

---

## Pipeline Architecture

```
S0_ingest_external_sources.py    [COMPLETE ✅]
        ↓
S1_decode_and_normalize.py       [COMPLETE ✅]
        ↓
S2_cluster_and_map.py            [COMPLETE ✅]
        ↓
S3_persona_assembly.py           [COMPLETE ✅]
        ↓
outputs/data/synthetic/
    student_personas.csv          ← 1,300 personas (38 columns) ✅
    s3_persona_audit.json         ✅
```

---

## STAGE S0 — Ingestion ✅

**Script:** `S0_ingest_external_sources.py`
**Status:** complete

- [x] Automatic Zenodo download via HTTP
- [x] Kaggle dataset read from manually placed file in `outputs/data/raw/`
- [x] Audit in `outputs/metadata/s0_ingest_audit.json` (status: COMPLETE)

---

## STAGE S1 — Decoding and Normalization ✅

**Script:** `S1_decode_and_normalize.py`

### Context

Zenodo stored all variables as `int64` via `LabelEncoder`. Before any analysis or clustering, these codes must be reverted to human-readable values and bridge variable scales normalized to a common range (0–1 or z-score).

### 1.1 Zenodo column decoding

| Zenodo Column | Codes | Human-readable values |
|---|---|---|
| `Gender` | 0, 1 | Female, Male |
| `Internet` | 0, 1 | No, Yes |
| `Extracurricular` | 0, 1 | No, Yes |
| `EduTech` | 0, 1 | No, Yes |
| `Discussions` | 0, 1 | No, Yes |
| `Motivation` | 0, 1, 2 | Low, Medium, High |
| `StressLevel` | 0, 1, 2 | Low, Medium, High |
| `Resources` | 0, 1, 2 | Low, Medium, High |
| `LearningStyle` | 0, 1, 2, 3 | Visual, Auditory, ReadWrite, Kinesthetic |
| `FinalGrade` | 0, 1, 2, 3 | Fail, Pass, Merit, Distinction |

> **Note:** The mappings above are inferred by distribution and LabelEncoder convention (alphabetical or ordinal order). They will be empirically validated via correlation with `ExamScore`.

### 1.2 Bridge variables — scale harmonization

Bridge variables exist in both datasets but with different scales. Normalization creates `_norm` columns in [0, 1] for both.

| Construct | Zenodo Column | Scale Z | Kaggle Column | Scale K | Normalization |
|---|---|---|---|---|---|
| Attendance/frequency | `Attendance` | 60–100 | `attendance_rate` | 0.50–1.0 | Min-Max → [0,1] |
| Task completion | `AssignmentCompletion` | 50–100 | `task_completion_rate` | 0.30–1.0 | Min-Max → [0,1] |
| Academic performance | `ExamScore` | 40–100 | `exam_score` | 40–100 | Min-Max → [0,1] |
| Motivation | `Motivation` | 0–2 | `motivation_index` | 1–5 | Min-Max → [0,1] |
| Stress | `StressLevel` | 0–2 | `stress_level` | 1–5 | Min-Max → [0,1] |
| Participation | `Discussions` | 0–1 | `discussion_posts` | 0–19 | Binarize K (>0=1) |

### 1.3 S1 Checklist

- [x] **1.1** Load `merged_dataset.csv` and `psychological_cbi_dataset.csv`
- [x] **1.2** Apply decoding mapping to Zenodo categorical columns
- [x] **1.3** Validate decoding: FinalGrade corr=-0.968 (descending → Distinction=0, Fail=3) ✓
- [x] **1.4** Create `*_norm` columns for 6 bridge variables in both datasets
- [x] **1.5** Check `*_norm` column distributions: range [0,1], 0 NaN ✓
- [x] **1.6** Save intermediate tables:
  - `outputs/data/synthetic/zenodo_decoded.csv` (14,003 × 32)
  - `outputs/data/synthetic/kaggle_normalized.csv` (1,300 × 25)
- [x] **1.7** Record mappings in `outputs/metadata/s1_decode_audit.json`

---

## STAGE S2 — Clustering and Mapping ✅

> **Note:** Strategy updated to maximize N. Instead of centroid-level matching (N=K_Z×K_K), adopted **individual-level nearest-neighbour** (Kaggle→Zenodo) resulting in N=1,300 pairs. Clustering retained for interpretability.

**Script:** `S2_cluster_and_map.py`

### Context

Without a primary key, the union of both datasets is done through the **normalized bridge construct space**. Each dataset is clustered independently; clusters with nearby centroids in the bridge space are considered compatible and matchable.

### 2.1 Zenodo Clustering

**Input:** `zenodo_decoded.csv`
**Clustering features:** the 6 `*_norm` columns of bridge variables

Steps:
1. Determine optimal K via Elbow (inertia) + Silhouette Score in range K ∈ [3, 12]
2. Apply K-Means with optimal K (fixed seed for reproducibility)
3. Compute cluster centroid in the 6 bridge dimensions
4. Additionally compute mean of other Zenodo variables per cluster (for persona composition)

### 2.2 Kaggle Clustering

**Input:** `kaggle_normalized.csv`
**Clustering features:** the 6 `*_norm` columns of bridge variables

Steps:
1. Determine optimal K via Elbow + Silhouette in range K ∈ [3, 12]
2. Apply K-Means with optimal K (fixed seed)
3. Compute cluster centroid

> Zenodo and Kaggle K values may differ. Mapping is done by centroid similarity, not by index.

### 2.3 Compatible cluster mapping (cross-cluster matching)

For each Zenodo cluster, find the closest Kaggle cluster in bridge space (Euclidean distance between centroids). Result: table of pairs `(zenodo_cluster_id → kaggle_cluster_id)`.

- Relationship: M:1 is allowed (multiple Zenodo clusters may map to the same Kaggle cluster if it is nearest)
- Document pairs with no close match (distance > threshold)

### 2.4 Defining N

Final persona count N = number of `(Zenodo_cluster, Kaggle_cluster)` pairs from the mapping. Typically N ∈ [4, 10] depending on optimal K values found.

### 2.5 S2 Checklist

- [x] **2.1** Load `zenodo_decoded.csv` + `kaggle_normalized.csv`
- [x] **2.2** Determine optimal Zenodo K (Elbow + Silhouette) → K=3 (silhouette=0.2365) ✓
- [x] **2.3** Apply Zenodo K-Means; assign `zenodo_cluster_id` to each row
- [x] **2.4** Elbow chart saved to `outputs/plots/s2_elbow_zenodo.png`
- [x] **2.5** Determine optimal Kaggle K (Elbow + Silhouette) → K=5 (silhouette=0.2365) ✓
- [x] **2.6** Apply Kaggle K-Means; assign `kaggle_cluster_id` to each row
- [x] **2.7** Elbow chart saved to `outputs/plots/s2_elbow_kaggle.png`
- [x] **2.8** Individual NN matching (NearestNeighbors k=1, Euclidean, Kaggle→Zenodo)
- [x] **2.9** 1,300 pairs generated; 1,106 unique Zenodo rows used; dist mean=0.216
- [x] **2.10** Save:
  - `outputs/data/synthetic/zenodo_clustered.csv` (14,003 × 34)
  - `outputs/data/synthetic/kaggle_clustered.csv` (1,300 × 27)
  - `outputs/data/synthetic/persona_pairs.csv` (1,300 × 5)
- [x] **2.11** Record metrics in `outputs/metadata/s2_cluster_audit.json`

---

## STAGE S3 — Persona Composition and Export ✅

**Script:** `S3_persona_assembly.py`

### Context

For each `(Zenodo_cluster, Kaggle_cluster)` pair:
- Represents 1 persona
- Zenodo variables (Dims 1–2 + part of 4) → mean/mode of Zenodo cluster
- Kaggle variables (Dim 3 + part of 4 + Dim 5) → mean/mode of Kaggle cluster
- Harmonized bridge variables → average of the two centroids

### 3.1 Final CSV schema — `student_personas.csv`

Each row = 1 persona. Total rows = N.

#### Dimension 1 — Demographic Profile *(Zenodo)*

| Field | Type | Values |
|---|---|---|
| `persona_id` | string | P01, P02, ..., PN |
| `age` | float | 18–29 (cluster mean) |
| `gender` | string | Female / Male (mode) |
| `learning_style` | string | Visual / Auditory / ReadWrite / Kinesthetic |
| `internet_access` | string | Yes / No |
| `extracurricular` | string | Yes / No |
| `uses_edutech` | string | Yes / No |
| `resources_availability` | string | Low / Medium / High |

#### Dimension 2 — Academic Behavior *(both)*

| Field | Type | Source |
|---|---|---|
| `study_hours_per_week` | float | Zenodo — cluster mean |
| `attendance_pct` | float | harmonic Z+K (0–100%) |
| `assignment_completion_pct` | float | harmonic Z+K (0–100%) |
| `online_courses_enrolled` | float | Zenodo — cluster mean |
| `exam_score` | float | harmonic Z+K (0–100) |
| `final_grade` | string | Zenodo — cluster mode |
| `risk_level` | string | Kaggle — cluster mode |
| `avg_response_time_hours` | float | Kaggle — cluster mean |

#### Dimension 3 — LMS Engagement *(Kaggle)*

| Field | Type | Values |
|---|---|---|
| `login_frequency` | float | 1–49 |
| `video_watch_time_min` | float | 10–300 |
| `discussion_posts` | float | 0–19 |
| `peer_interaction_count` | float | 0–49 |
| `task_completion_rate` | float | 0.30–1.0 |
| `engagement_level` | float | 1–5 Likert |

#### Dimension 4 — Psychological Profile *(both)*

| Field | Type | Source |
|---|---|---|
| `motivation_level` | string | Zenodo — mode (Low/Med/High) |
| `motivation_index` | float | Kaggle — mean (1–5) |
| `stress_level_label` | string | Zenodo — mode (Low/Med/High) |
| `stress_score` | float | Kaggle — mean (1–5) |
| `anxiety_score` | float | Kaggle — mean (1–5) |
| `resilience_score` | float | Kaggle — mean (1–5) |

#### Dimension 5 — Emotional & Intervention Context *(Kaggle)*

| Field | Type | Values |
|---|---|---|
| `dominant_emotion` | string | Happy/Sad/Bored/Normal/Scared/Surprised (mode) |
| `intervention_type` | string | CBT_Session/Mindfulness/Peer_Support/Gamified_Task (mode) |
| `post_intervention_mood` | float | 1–5 Likert |

#### Persona metadata

| Field | Type | Description |
|---|---|---|
| `zenodo_cluster_id` | int | Origin in Zenodo clustering |
| `kaggle_cluster_id` | int | Origin in Kaggle clustering |
| `cluster_distance` | float | Euclidean distance between centroids |
| `zenodo_n_students` | int | Number of records in Zenodo cluster |
| `kaggle_n_students` | int | Number of records in Kaggle cluster |
| `persona_archetype_label` | string | Rule-generated narrative label |

**Total columns:** ~35

### 3.2 Narrative label generation (archetype label)

Each persona receives a rule-based label combining risk_level + motivation + stress:

| risk_level | motivation | stress | → label |
|---|---|---|---|
| low | High | Low | `Flourishing Learner` |
| low | Medium | Low | `Steady Performer` |
| medium | High | Medium | `Driven but Pressured` |
| medium | Low | High | `Disengaged at Risk` |
| high | Low | High | `Crisis Learner` |
| high | Medium | Medium | `Struggling Resilient` |
| ... | ... | ... | (full table in script) |

### 3.3 S3 Checklist

- [x] **3.1** Load `zenodo_clustered.csv`, `kaggle_clustered.csv`, `persona_pairs.csv`
- [x] **3.2** For each individual pair (kaggle_idx → zenodo_idx), combine columns from both sources
- [x] **3.3** Compute harmonic bridge variables (mean Z_norm + K_norm → original scale)
- [x] **3.4** Final schema: 38 columns across 5 dimensions + metadata
- [x] **3.5** Apply archetype label rules (8 types by risk × motivation × stress)
- [x] **3.6** Validations:
  - [x] 1,300 rows, 0 NaN ✓
  - [x] `bridge_distance` documented per persona ✓
  - [x] `exam_score_harmonized` ∈ [40, 100] ✓
  - [x] 1,300 unique `persona_id` ✓
- [x] **3.7** Export `outputs/data/synthetic/student_personas.csv` (1300 × 38) ✓
- [x] **3.8** Export `outputs/metadata/s3_persona_audit.json` ✓
- [x] **3.9** Archetypes: Disengaged Drifter(32.9%), Resilient Climber(30.2%), Steady Performer(19.3%), Routine Complier(6.5%), Flourishing Achiever(6.2%), Crisis Learner(2.5%), Overwhelmed Striver(2.3%)

---

## Output Structure

```
next_proposal_paper/outputs/
├── data/
│   ├── raw/
│   │   ├── merged_dataset.csv              [S0 ✅]
│   │   └── psychological_cbi_dataset.csv   [S0 ✅]
│   └── synthetic/
│       ├── zenodo_decoded.csv              [S1]
│       ├── kaggle_normalized.csv           [S1]
│       ├── zenodo_clustered.csv            [S2]
│       ├── kaggle_clustered.csv            [S2]
│       ├── cluster_mapping.json            [S2]
│       └── student_personas.csv            [S3] ← final deliverable
└── metadata/
    ├── s0_ingest_audit.json                [S0 ✅]
    ├── s1_decode_audit.json                [S1]
    ├── s2_cluster_audit.json               [S2]
    └── persona_audit.json                  [S3]
```

---

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Clustering method | K-Means | Simple, well-defined centroids for cross-mapping |
| K selection | Elbow + Silhouette | Avoids arbitrary choice |
| Clustering features | 6 normalized bridge variables | Shared space between both sources |
| Intra-cluster aggregation | Mean (continuous), Mode (categorical) | Persona as representative archetype |
| Cross-cluster matching | Nearest centroid (Euclidean) | M:1 allowed for full coverage |
| N not fixed a priori | N = f(K_Z, K_K, matching) | N emerges from data, typically 4–10 |
| Harmonic bridge scale | Mean of *_norm Z and *_norm K | Neither source dominates |
| Reproducibility | random_state=42 fixed in all scripts | |

---

## Python Dependencies

All available in the current Python 3.9.13 environment:
- `pandas`, `numpy` — data manipulation
- `scikit-learn` — KMeans, MinMaxScaler, silhouette_score
- `matplotlib` — Elbow charts (saved to `outputs/data/synthetic/`)
- `json`, `pathlib` — audit and I/O

---

## Pipeline Status

| Script | Status |
|---|---|
| S0 — Ingestion | ✅ COMPLETE |
| S1 — Decoding + Normalization | ⬜ to create |
| S2 — Clustering + Mapping | ⬜ to create |
| S3 — Composition + Export | ⬜ to create |
| `student_personas.csv` | ⬜ pending |
