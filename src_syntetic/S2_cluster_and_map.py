"""
S2_cluster_and_map.py
─────────────────────
Clusters both datasets in bridge-variable space and performs individual-level
nearest-neighbour matching (Kaggle → Zenodo) to generate 1,300 persona pairs.

Strategy:
  • KMeans on Zenodo bridge features → zenodo_cluster_id (K chosen by elbow + silhouette)
  • KMeans on Kaggle bridge features → kaggle_cluster_id
  • NearestNeighbors(k=1) fit on Zenodo bridge, queried with all 1,300 Kaggle rows
  • Each Kaggle student is matched to 1 Zenodo student (re-use of Zenodo rows is allowed)
  • Result: exactly 1,300 persona pairs

Inputs
──────
outputs/data/synthetic/zenodo_decoded.csv   (14003 × 32)
outputs/data/synthetic/kaggle_normalized.csv (1300 × 25)

Outputs
───────
outputs/data/synthetic/zenodo_clustered.csv   (14003 rows, adds zenodo_cluster_id)
outputs/data/synthetic/kaggle_clustered.csv   (1300  rows, adds kaggle_cluster_id)
outputs/data/synthetic/persona_pairs.csv      (1300  rows: matching + cluster IDs)
outputs/plots/s2_elbow_zenodo.png
outputs/plots/s2_elbow_kaggle.png
outputs/metadata/s2_cluster_audit.json
"""

import json
import os
import sys

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.neighbors import NearestNeighbors

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPLOT = True
except ImportError:
    HAS_MPLOT = False

# ── paths ────────────────────────────────────────────────────────────────────
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SYN  = os.path.join(BASE, "outputs", "data", "synthetic")
META = os.path.join(BASE, "outputs", "metadata")
PLOT = os.path.join(BASE, "outputs", "plots")

ZENODO_IN   = os.path.join(SYN,  "zenodo_decoded.csv")
KAGGLE_IN   = os.path.join(SYN,  "kaggle_normalized.csv")
ZENODO_OUT  = os.path.join(SYN,  "zenodo_clustered.csv")
KAGGLE_OUT  = os.path.join(SYN,  "kaggle_clustered.csv")
PAIRS_OUT   = os.path.join(SYN,  "persona_pairs.csv")
AUDIT_PATH  = os.path.join(META, "s2_cluster_audit.json")
PLOT_Z      = os.path.join(PLOT, "s2_elbow_zenodo.png")
PLOT_K      = os.path.join(PLOT, "s2_elbow_kaggle.png")

for d in [SYN, META, PLOT]:
    os.makedirs(d, exist_ok=True)

BRIDGE_COLS = [
    "attendance_norm",
    "assignment_norm",
    "exam_norm",
    "motivation_norm",
    "stress_norm",
    "discussion_norm",
]

RANDOM_STATE = 42

# ── helpers ──────────────────────────────────────────────────────────────────

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


def find_optimal_k(X: np.ndarray, k_range: range, label: str) -> dict:
    """Return elbow / silhouette stats and recommended K."""
    inertias, sils = [], []
    for k in k_range:
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = km.fit_predict(X)
        inertias.append(km.inertia_)
        sil = silhouette_score(X, labels, sample_size=min(len(X), 5000),
                               random_state=RANDOM_STATE)
        sils.append(sil)
        print(f"    K={k:2d}  inertia={km.inertia_:>14.1f}  silhouette={sil:.4f}")

    # Elbow: second-order difference
    inertia_arr = np.array(inertias)
    delta2 = np.diff(np.diff(inertia_arr))
    elbow_idx = int(np.argmax(delta2)) + 1           # offset by 2 differences
    elbow_k   = list(k_range)[elbow_idx]

    # Silhouette: max
    sil_k = list(k_range)[int(np.argmax(sils))]

    # Recommendation: prefer silhouette; if they agree → either; if not → silhouette
    recommended = sil_k
    print(f"\n    elbow K={elbow_k}  |  best-silhouette K={sil_k}  →  recommended K={recommended}")

    return {
        "ks": list(k_range),
        "inertias": inertias,
        "silhouettes": sils,
        "elbow_k": elbow_k,
        "sil_k": sil_k,
        "recommended_k": recommended,
    }


def plot_elbow(stats: dict, label: str, path: str):
    if not HAS_MPLOT:
        return
    ks = stats["ks"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    axes[0].plot(ks, stats["inertias"], "bo-")
    axes[0].axvline(stats["recommended_k"], color="red", linestyle="--",
                    label=f"K={stats['recommended_k']}")
    axes[0].set_title(f"{label} — Elbow (Inertia)")
    axes[0].set_xlabel("K")
    axes[0].set_ylabel("Inertia")
    axes[0].legend()

    axes[1].plot(ks, stats["silhouettes"], "gs-")
    axes[1].axvline(stats["recommended_k"], color="red", linestyle="--",
                    label=f"K={stats['recommended_k']}")
    axes[1].set_title(f"{label} — Silhouette")
    axes[1].set_xlabel("K")
    axes[1].set_ylabel("Silhouette Score")
    axes[1].legend()

    fig.tight_layout()
    fig.savefig(path, dpi=100)
    plt.close(fig)
    print(f"    Plot saved: {path}")


# ── main ─────────────────────────────────────────────────────────────────────

print("█" * 70)
print("  S2 — Cluster & Match")
print("█" * 70)
print()

# [1] Load inputs
print("  [1] Loading decoded / normalized datasets …")
zd = pd.read_csv(ZENODO_IN)
kd = pd.read_csv(KAGGLE_IN)
print(f"    Zenodo : {zd.shape}   Kaggle : {kd.shape}")

# Guard: check bridge cols exist
for col in BRIDGE_COLS:
    if col not in zd.columns:
        sys.exit(f"[ERROR] Zenodo missing bridge col: {col}. Re-run S1 first.")
    if col not in kd.columns:
        sys.exit(f"[ERROR] Kaggle missing bridge col: {col}. Re-run S1 first.")

Z_bridge = zd[BRIDGE_COLS].values.astype(float)
K_bridge = kd[BRIDGE_COLS].values.astype(float)
print(f"    Bridge feature matrix Z: {Z_bridge.shape}")
print(f"    Bridge feature matrix K: {K_bridge.shape}")
print()

# [2] Cluster Zenodo
print("  [2] Finding optimal K for Zenodo (range 3–15) …")
z_stats = find_optimal_k(Z_bridge, range(3, 16), "Zenodo")
K_Z = z_stats["recommended_k"]
print()

km_z = KMeans(n_clusters=K_Z, random_state=RANDOM_STATE, n_init=20)
zd["zenodo_cluster_id"] = km_z.fit_predict(Z_bridge)
z_counts = zd["zenodo_cluster_id"].value_counts().sort_index().to_dict()
print(f"  Zenodo cluster sizes (K={K_Z}): {z_counts}")
plot_elbow(z_stats, "Zenodo", PLOT_Z)
print()

# [3] Cluster Kaggle
print("  [3] Finding optimal K for Kaggle (range 3–10) …")
k_stats = find_optimal_k(K_bridge, range(3, 11), "Kaggle")
K_K = k_stats["recommended_k"]
print()

km_k = KMeans(n_clusters=K_K, random_state=RANDOM_STATE, n_init=20)
kd["kaggle_cluster_id"] = km_k.fit_predict(K_bridge)
k_counts = kd["kaggle_cluster_id"].value_counts().sort_index().to_dict()
print(f"  Kaggle cluster sizes (K={K_K}): {k_counts}")
plot_elbow(k_stats, "Kaggle", PLOT_K)
print()

# [4] Individual NN matching  Kaggle → Zenodo  (1,300 pairs)
print("  [4] Nearest-neighbour matching (Kaggle → Zenodo, k=1) …")
nn = NearestNeighbors(n_neighbors=1, metric="euclidean", algorithm="ball_tree")
nn.fit(Z_bridge)
distances, indices = nn.kneighbors(K_bridge)   # (1300, 1) each
print(f"    Distances: min={distances.min():.4f}  mean={distances.mean():.4f}"
      f"  median={np.median(distances):.4f}  max={distances.max():.4f}")

zenodo_match_idx = indices.ravel().astype(int)
bridge_dist      = distances.ravel()

# How many unique Zenodo rows are matched?
n_unique_z = len(np.unique(zenodo_match_idx))
print(f"    Unique Zenodo rows matched: {n_unique_z} / {len(zd)}")
print(f"    Persona pairs generated  : {len(zenodo_match_idx)}")
print()

# [5] Build persona_pairs.csv
print("  [5] Building persona_pairs.csv …")
pairs_df = pd.DataFrame({
    "kaggle_idx"         : np.arange(len(kd)),
    "zenodo_idx"         : zenodo_match_idx,
    "bridge_distance"    : np.round(bridge_dist, 6),
    "kaggle_cluster_id"  : kd["kaggle_cluster_id"].values,
    "zenodo_cluster_id"  : zd.loc[zenodo_match_idx, "zenodo_cluster_id"].values,
})
print(f"    pairs_df shape: {pairs_df.shape}")
print(f"    Sample:\n{pairs_df.head(5).to_string(index=False)}")
print()

# [6] Save outputs
print("  [6] Saving outputs …")
zd.to_csv(ZENODO_OUT, index=False)
kd.to_csv(KAGGLE_OUT, index=False)
pairs_df.to_csv(PAIRS_OUT, index=False)
print(f"    zenodo_clustered.csv : {zd.shape}  → {ZENODO_OUT}")
print(f"    kaggle_clustered.csv : {kd.shape}  → {KAGGLE_OUT}")
print(f"    persona_pairs.csv    : {pairs_df.shape}  → {PAIRS_OUT}")
print()

# [7] Audit
audit = {
    "status"       : "COMPLETE",
    "zenodo_rows"  : len(zd),
    "kaggle_rows"  : len(kd),
    "n_personas"   : len(pairs_df),
    "bridge_cols"  : BRIDGE_COLS,
    "zenodo_cluster": {
        "K"       : K_Z,
        "sizes"   : z_counts,
        "elbow_k" : z_stats["elbow_k"],
        "sil_k"   : z_stats["sil_k"],
    },
    "kaggle_cluster": {
        "K"       : K_K,
        "sizes"   : k_counts,
        "elbow_k" : k_stats["elbow_k"],
        "sil_k"   : k_stats["sil_k"],
    },
    "nn_matching": {
        "metric"              : "euclidean",
        "unique_zenodo_matched": n_unique_z,
        "distance_min"        : float(round(distances.min(), 6)),
        "distance_mean"       : float(round(distances.mean(), 6)),
        "distance_max"        : float(round(distances.max(), 6)),
    },
    "outputs": {
        "zenodo_clustered" : ZENODO_OUT,
        "kaggle_clustered" : KAGGLE_OUT,
        "persona_pairs"    : PAIRS_OUT,
    },
}
with open(AUDIT_PATH, "w") as f:
    json.dump(_json_safe(audit), f, indent=2)
print(f"  Audit: {AUDIT_PATH}")
print(f"  Status: COMPLETE ✓")
