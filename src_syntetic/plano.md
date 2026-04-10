# Plano: Criação de Perfis de Estudantes Sintéticos
## Pipeline src_syntetic — Fase S0 → S3

> **Objetivo final:** produzir um CSV com N perfis sintéticos de estudantes, onde cada perfil combina variáveis complementares das duas fontes externas (Zenodo + Kaggle) em 5 dimensões analíticas, sem chave primária compartilhada, via clusterização por espaço de construtos-ponte.

---

## Isolamento

Este pipeline é **completamente isolado** do pipeline OULAD (`src/`). Não lê nem escreve em `outputs/data/engagement.duckdb` ou em `engagement_panel_weekly.csv`. Lê apenas de `outputs/data/raw/` e escreve em `outputs/data/synthetic/`.

---

## Fontes de Dados

| ID | Fonte | Arquivo | Registros | Colunas |
|---|---|---|---|---|
| Zenodo | NAJEM et al. (2025), Zenodo DOI 10.5281/zenodo.16459132 | `merged_dataset.csv` | 14.003 | 16 (todos int64, label-encoded) |
| Kaggle | programmer3, Psychological CBI Student Dataset | `psychological_cbi_dataset.csv` | 1.300 | 19 (misto: float, int, object) |

---

## Arquitetura do Pipeline

```
S0_ingest_external_sources.py    [COMPLETO ✅]
        ↓
S1_decode_and_normalize.py       [COMPLETO ✅]
        ↓
S2_cluster_and_map.py            [COMPLETO ✅]
        ↓
S3_persona_assembly.py           [COMPLETO ✅]
        ↓
outputs/data/synthetic/
    student_personas.csv          ← 1.300 personas (38 colunas) ✅
    s3_persona_audit.json         ✅
```

---

## FASE S0 — Ingestão ✅

**Script:** `S0_ingest_external_sources.py`
**Status:** completo

- [x] Download automático do Zenodo via HTTP
- [x] Leitura do Kaggle via arquivo manual colocado em `outputs/data/raw/`
- [x] Audit em `outputs/metadata/s0_ingest_audit.json` (status: COMPLETE)

---

## FASE S1 — Decodificação e Normalização ✅

**Script:** `S1_decode_and_normalize.py`

### Contexto

O Zenodo armazenou todas as variáveis como `int64` via `LabelEncoder`. Antes de qualquer análise ou clustering, é necessário reverter esses códigos para valores semânticos e normalizar as escalas das variáveis-ponte para uma escala comum (0–1 ou z-score).

### 1.1 Decodificação das colunas Zenodo

| Coluna Zenodo | Códigos | Valores semânticos |
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

> **Nota:** os mapeamentos acima são inferidos por distribuição e convenção de LabelEncoder (ordem alfabética ou ordinal). Serão validados empiricamente via correlação com `ExamScore`.

### 1.2 Variáveis-ponte — Harmonização de escala

As variáveis-ponte existem nos dois datasets mas com escalas diferentes. A normalização cria colunas `_norm` em escala [0, 1] para ambas.

| Construto | Coluna Zenodo | Escala Z | Coluna Kaggle | Escala K | Normalização |
|---|---|---|---|---|---|
| Presença/frequência | `Attendance` | 60–100 | `attendance_rate` | 0.50–1.0 | Min-Max → [0,1] |
| Conclusão de tarefas | `AssignmentCompletion` | 50–100 | `task_completion_rate` | 0.30–1.0 | Min-Max → [0,1] |
| Desempenho academic | `ExamScore` | 40–100 | `exam_score` | 40–100 | Min-Max → [0,1] |
| Motivação | `Motivation` | 0–2 | `motivation_index` | 1–5 | Min-Max → [0,1] |
| Estresse | `StressLevel` | 0–2 | `stress_level` | 1–5 | Min-Max → [0,1] |
| Participação | `Discussions` | 0–1 | `discussion_posts` | 0–19 | Binarizar K (>0=1) |

### 1.3 Checklist S1

- [x] **1.1** Carregar `merged_dataset.csv` e `psychological_cbi_dataset.csv`
- [x] **1.2** Aplicar mapeamento de decodificação nas colunas Zenodo categóricas
- [x] **1.3** Validar decodificação: FinalGrade corr=-0.968 (descending → Distinction=0, Fail=3) ✓
- [x] **1.4** Criar colunas `*_norm` para as 6 variáveis-ponte em ambas as bases
- [x] **1.5** Verificar distribuição das colunas `*_norm`: range [0,1], 0 NaN ✓
- [x] **1.6** Salvar tabelas intermediárias:
  - `outputs/data/synthetic/zenodo_decoded.csv` (14003 × 32)
  - `outputs/data/synthetic/kaggle_normalized.csv` (1300 × 25)
- [x] **1.7** Registrar mapeamentos em `outputs/metadata/s1_decode_audit.json`

---

## FASE S2 — Clusterização e Mapeamento ✅

> **Nota:** estratégia atualizada para maximizar N. Em vez de centroid-level matching (N=K_Z×K_K), adotou-se **individual-level nearest-neighbour** (Kaggle→Zenodo) resultando em N=1.300 pares. Clustering mantido para interpretabilidade.

**Script:** `S2_cluster_and_map.py`

### Contexto

Sem chave primária, a união dos dois datasets é feita pelo **espaço de construtos-ponte normalizados**. Cada dataset é clusterizado independentemente; clusters com centróides próximos no espaço-ponte são considerados compatíveis e emparelháveis.

### 2.1 Clusterização do Zenodo

**Input:** `zenodo_decoded.csv`  
**Features de clustering:** as 6 colunas `*_norm` das variáveis-ponte

Passos:
1. Determinar K ótimo via Elbow (inertia) + Silhouette Score no intervalo K ∈ [3, 12]
2. Aplicar K-Means com K ótimo (seed fixo para reprodutibilidade)
3. Calcular centróide de cada cluster nas 6 dimensões-ponte
4. Calcular, adicionalmente, média das outras variáveis Zenodo por cluster (para composição de persona)

### 2.2 Clusterização do Kaggle

**Input:** `kaggle_normalized.csv`  
**Features de clustering:** as 6 colunas `*_norm` das variáveis-ponte

Passos:
1. Determinar K ótimo via Elbow + Silhouette no intervalo K ∈ [3, 12]
2. Aplicar K-Means com K ótimo (seed fixo)
3. Calcular centróide de cada cluster

> Os K de Zenodo e Kaggle podem ser diferentes. O mapeamento é feito por similaridade de centróide, não por índice.

### 2.3 Mapeamento de clusters compatíveis (cross-cluster matching)

Para cada cluster Zenodo, encontrar o cluster Kaggle mais próximo no espaço-ponte (distância euclidiana entre centróides). Resultado: tabela de pares `(zenodo_cluster_id → kaggle_cluster_id)`.

- Relacionamento: M:1 é permitido (múltiplos clusters Zenodo podem mapear para o mesmo cluster Kaggle se mais próximo)
- Documentar pares sem match próximo (distância > threshold)

### 2.4 Definição de N

O número N de personas finais = número de pares `(Zenodo_cluster, Kaggle_cluster)` resultantes do mapeamento. Tipicamente N ∈ [4, 10] dependendo dos K ótimos encontrados.

### 2.5 Checklist S2

- [x] **2.1** Carregar `zenodo_decoded.csv` + `kaggle_normalized.csv`
- [x] **2.2** Determinar K ótimo Zenodo (Elbow + Silhouette) → K=3 (silhouette=0.2365) ✓
- [x] **2.3** Aplicar K-Means Zenodo; atribuir `zenodo_cluster_id` a cada row
- [x] **2.4** Gráfico de Elbow salvo em `outputs/plots/s2_elbow_zenodo.png`
- [x] **2.5** Determinar K ótimo Kaggle (Elbow + Silhouette) → K=5 (silhouette=0.2365) ✓
- [x] **2.6** Aplicar K-Means Kaggle; atribuir `kaggle_cluster_id` a cada row
- [x] **2.7** Gráfico de Elbow salvo em `outputs/plots/s2_elbow_kaggle.png`
- [x] **2.8** Individual NN matching (NearestNeighbors k=1, Euclidean, Kaggle→Zenodo)
- [x] **2.9** 1.300 pares gerados; 1.106 linhas Zenodo únicas usadas; dist mean=0.216
- [x] **2.10** Salvar:
  - `outputs/data/synthetic/zenodo_clustered.csv` (14003 × 34)
  - `outputs/data/synthetic/kaggle_clustered.csv` (1300 × 27)
  - `outputs/data/synthetic/persona_pairs.csv` (1300 × 5)
- [x] **2.11** Registrar métricas em `outputs/metadata/s2_cluster_audit.json`

---

## FASE S3 — Composição de Personas e Exportação ✅

**Script:** `S3_persona_assembly.py`

### Contexto

Para cada par `(Zenodo_cluster, Kaggle_cluster)`:
- Representa 1 persona
- Variáveis Zenodo (Dimensões 1–2 + parte da 4) → média/moda do cluster Zenodo
- Variáveis Kaggle (Dimensão 3 + parte da 4 + Dimensão 5) → média/moda do cluster Kaggle
- Variáveis-ponte harmonizadas → média entre os dois centróides

### 3.1 Schema do CSV final — `student_personas.csv`

Cada linha = 1 persona. Total de linhas = N.

#### Dimensão 1 — Demographic Profile *(Zenodo)*

| Campo | Tipo | Valores |
|---|---|---|
| `persona_id` | string | P01, P02, ..., PN |
| `age` | float | 18–29 (média do cluster) |
| `gender` | string | Female / Male (moda) |
| `learning_style` | string | Visual / Auditory / ReadWrite / Kinesthetic |
| `internet_access` | string | Yes / No |
| `extracurricular` | string | Yes / No |
| `uses_edutech` | string | Yes / No |
| `resources_availability` | string | Low / Medium / High |

#### Dimensão 2 — Academic Behavior *(ambos)*

| Campo | Tipo | Fonte |
|---|---|---|
| `study_hours_per_week` | float | Zenodo — média cluster |
| `attendance_pct` | float | harmônico Z+K (0–100%) |
| `assignment_completion_pct` | float | harmônico Z+K (0–100%) |
| `online_courses_enrolled` | float | Zenodo — média cluster |
| `exam_score` | float | harmônico Z+K (0–100) |
| `final_grade` | string | Zenodo — moda cluster |
| `risk_level` | string | Kaggle — moda cluster |
| `avg_response_time_hours` | float | Kaggle — média cluster |

#### Dimensão 3 — LMS Engagement *(Kaggle)*

| Campo | Tipo | Valores |
|---|---|---|
| `login_frequency` | float | 1–49 |
| `video_watch_time_min` | float | 10–300 |
| `discussion_posts` | float | 0–19 |
| `peer_interaction_count` | float | 0–49 |
| `task_completion_rate` | float | 0.30–1.0 |
| `engagement_level` | float | 1–5 Likert |

#### Dimensão 4 — Psychological Profile *(ambos)*

| Campo | Tipo | Fonte |
|---|---|---|
| `motivation_level` | string | Zenodo — moda (Low/Med/High) |
| `motivation_index` | float | Kaggle — média (1–5) |
| `stress_level_label` | string | Zenodo — moda (Low/Med/High) |
| `stress_score` | float | Kaggle — média (1–5) |
| `anxiety_score` | float | Kaggle — média (1–5) |
| `resilience_score` | float | Kaggle — média (1–5) |

#### Dimensão 5 — Emotional & Intervention Context *(Kaggle)*

| Campo | Tipo | Valores |
|---|---|---|
| `dominant_emotion` | string | Happy/Sad/Bored/Normal/Scared/Surprised (moda) |
| `intervention_type` | string | CBT_Session/Mindfulness/Peer_Support/Gamified_Task (moda) |
| `post_intervention_mood` | float | 1–5 Likert |

#### Metadados da persona

| Campo | Tipo | Descrição |
|---|---|---|
| `zenodo_cluster_id` | int | Origem no clustering Zenodo |
| `kaggle_cluster_id` | int | Origem no clustering Kaggle |
| `cluster_distance` | float | Distância euclidiana entre centróides |
| `zenodo_n_students` | int | Nº de registros no cluster Zenodo |
| `kaggle_n_students` | int | Nº de registros no cluster Kaggle |
| `persona_archetype_label` | string | Rótulo narrativo gerado por regra |

**Total de colunas:** ~35

### 3.2 Geração do rótulo narrativo (archetype label)

Cada persona recebe um rótulo baseado em regras combinando risk_level + motivation + stress:

| risk_level | motivation | stress | → label |
|---|---|---|---|
| low | High | Low | `Flourishing Learner` |
| low | Medium | Low | `Steady Performer` |
| medium | High | Medium | `Driven but Pressured` |
| medium | Low | High | `Disengaged at Risk` |
| high | Low | High | `Crisis Learner` |
| high | Medium | Medium | `Struggling Resilient` |
| ... | ... | ... | (tabela completa no script) |

### 3.3 Checklist S3

- [x] **3.1** Carregar `zenodo_clustered.csv`, `kaggle_clustered.csv`, `persona_pairs.csv`
- [x] **3.2** Para cada par individual (kaggle_idx → zenodo_idx), combinar colunas das duas fontes
- [x] **3.3** Calcular variáveis-ponte harmônicas (média Z_norm + K_norm → escala original)
- [x] **3.4** Schema final: 38 colunas em 5 dimensões + metadados
- [x] **3.5** Aplicar regras de archetype label (8 tipos por risk × motivation × stress)
- [x] **3.6** Validações:
  - [x] 1.300 linhas, 0 NaN ✓
  - [x] `bridge_distance` documentado por persona ✓
  - [x] `exam_score_harmonized` ∈ [40, 100] ✓
  - [x] 1.300 `persona_id` únicos ✓
- [x] **3.7** Exportar `outputs/data/synthetic/student_personas.csv` (1300 × 38) ✓
- [x] **3.8** Exportar `outputs/metadata/s3_persona_audit.json` ✓
- [x] **3.9** Archetypes: Disengaged Drifter(32.9%), Resilient Climber(30.2%), Steady Performer(19.3%), Routine Complier(6.5%), Flourishing Achiever(6.2%), Crisis Learner(2.5%), Overwhelmed Striver(2.3%)

---

## Estrutura de Outputs

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
│       └── student_personas.csv            [S3] ← entregável final
└── metadata/
    ├── s0_ingest_audit.json                [S0 ✅]
    ├── s1_decode_audit.json                [S1]
    ├── s2_cluster_audit.json               [S2]
    └── persona_audit.json                  [S3]
```

---

## Decisões de Design

| Decisão | Escolha | Justificativa |
|---|---|---|
| Método de clustering | K-Means | Simples, centróides bem definidos para cross-mapping |
| Seleção de K | Elbow + Silhouette | Evita escolha arbitrária |
| Feature de clustering | 6 variáveis-ponte normalizadas | Espaço compartilhado entre as duas fontes |
| Agregação intra-cluster | Média (contínuas), Moda (categóricas) | Persona como arquétipo representativo |
| Matching entre clusters | Nearest centroid (euclidiana) | M:1 permitido para cobertura total |
| N ≠ fixo a priori | N = f(K_Z, K_K, matching) | N emergente dos dados, típico 4–10 |
| Escala-ponte harmônica | Média de *_norm Z e *_norm K | Nenhuma fonte domina |
| Reprodutibilidade | random_state=42 fixo em todos os scripts | |

---

## Dependências Python

Todas disponíveis no ambiente Python 3.9.13 atual:
- `pandas`, `numpy` — manipulação
- `scikit-learn` — KMeans, MinMaxScaler, silhouette_score
- `matplotlib` — gráficos de Elbow (salvos em `outputs/data/synthetic/`)
- `json`, `pathlib` — audit e I/O

---

## Status do Pipeline

| Script | Status |
|---|---|
| S0 — Ingestão | ✅ COMPLETO |
| S1 — Decodificação + Normalização | ⬜ a criar |
| S2 — Clusterização + Mapeamento | ⬜ a criar |
| S3 — Composição + Exportação | ⬜ a criar |
| `student_personas.csv` | ⬜ pendente |
