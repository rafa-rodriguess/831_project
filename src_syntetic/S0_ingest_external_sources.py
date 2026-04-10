"""
S0_ingest_external_sources.py
─────────────────────────────
Downloads and audits the two external datasets used in the synthetic strand
of the LE-JD quantitative pipeline.

Sources
───────
[1] Zenodo — Student Performance and Learning Behavior Dataset
    DOI  : https://doi.org/10.5281/zenodo.16459132
    File : merged_dataset.csv  (14,003 rows × 16 cols)
    Download: direct HTTP (no auth required, CC-BY 4.0)

[2] Kaggle — Psychological CBI Student Dataset
    URL  : https://www.kaggle.com/datasets/programmer3/psychological-cbi-student-dataset
    File : psychological_cbi_dataset.csv  (1,300 rows × 19 cols)
    Download: Kaggle API (requires ~/.kaggle/kaggle.json) or manual placement

Output
──────
next_proposal_paper/outputs/data/raw/
    merged_dataset.csv
    psychological_cbi_dataset.csv
    s0_ingest_audit.json
"""

import json
import os
import sys
import hashlib
from pathlib import Path
from datetime import datetime, timezone

# ── paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).resolve().parent
PROPOSAL_ROOT = SCRIPT_DIR.parent
RAW_DIR      = PROPOSAL_ROOT / "outputs" / "data" / "raw"
AUDIT_PATH   = PROPOSAL_ROOT / "outputs" / "metadata" / "s0_ingest_audit.json"

RAW_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── source definitions ─────────────────────────────────────────────────────────
ZENODO_URL  = "https://zenodo.org/records/16459132/files/merged_dataset.csv?download=1"
ZENODO_FILE = RAW_DIR / "merged_dataset.csv"

KAGGLE_SLUG = "programmer3/psychological-cbi-student-dataset"
KAGGLE_FILE = RAW_DIR / "psychological_cbi_dataset.csv"

audit = {
    "script"    : "S0_ingest_external_sources.py",
    "run_at"    : datetime.now(timezone.utc).isoformat(),
    "sources"   : {},
    "status"    : "INCOMPLETE",
}


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _audit_dataframe(df, label: str) -> dict:
    """Return basic schema/stats dict for audit log."""
    info = {
        "rows"    : int(len(df)),
        "columns" : int(len(df.columns)),
        "col_names": list(df.columns),
        "dtypes"  : {c: str(df[c].dtype) for c in df.columns},
        "null_counts": {c: int(df[c].isna().sum()) for c in df.columns},
    }
    print(f"\n  [{label}] shape: {df.shape}")
    print(f"  columns: {list(df.columns)}")
    null_cols = {c: v for c, v in info["null_counts"].items() if v > 0}
    if null_cols:
        print(f"  nulls  : {null_cols}")
    else:
        print("  nulls  : none")
    return info


# ══════════════════════════════════════════════════════════════════════════════
# Source 1 — Zenodo (direct HTTP download)
# ══════════════════════════════════════════════════════════════════════════════

def ingest_zenodo():
    import urllib.request

    print("\n" + "="*70)
    print("SOURCE 1 — Zenodo: Student Performance and Learning Behavior")
    print(f"  DOI  : https://doi.org/10.5281/zenodo.16459132")
    print(f"  File : {ZENODO_FILE.name}")
    print("="*70)

    if ZENODO_FILE.exists():
        print(f"  [SKIP] already present: {ZENODO_FILE}")
    else:
        print(f"  Downloading from {ZENODO_URL} …")
        try:
            urllib.request.urlretrieve(ZENODO_URL, ZENODO_FILE)
            print(f"  [OK] saved to {ZENODO_FILE}")
        except Exception as exc:
            print(f"  [FAIL] download error: {exc}")
            audit["sources"]["zenodo"] = {"status": "DOWNLOAD_FAILED", "error": str(exc)}
            return

    import pandas as pd
    df = pd.read_csv(ZENODO_FILE)
    schema = _audit_dataframe(df, "Zenodo")

    audit["sources"]["zenodo"] = {
        "status"     : "OK",
        "url"        : ZENODO_URL,
        "local_file" : str(ZENODO_FILE),
        "md5"        : _md5(ZENODO_FILE),
        "size_bytes" : ZENODO_FILE.stat().st_size,
        "schema"     : schema,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Source 2 — Kaggle (API or manual placement)
# ══════════════════════════════════════════════════════════════════════════════

def ingest_kaggle():
    print("\n" + "="*70)
    print("SOURCE 2 — Kaggle: Psychological CBI Student Dataset")
    print(f"  URL  : https://www.kaggle.com/datasets/{KAGGLE_SLUG}")
    print(f"  File : {KAGGLE_FILE.name}")
    print("="*70)

    if KAGGLE_FILE.exists():
        print(f"  [SKIP] already present: {KAGGLE_FILE}")
    else:
        # Try Kaggle API first
        _kaggle_api_download()

    if not KAGGLE_FILE.exists():
        msg = (
            f"\n  [ACTION REQUIRED] File not found: {KAGGLE_FILE}\n"
            f"\n  Option A — Kaggle API (recommended):\n"
            f"    1. Get your API token from https://www.kaggle.com/settings → API → Create New Token\n"
            f"    2. Save it to ~/.kaggle/kaggle.json\n"
            f"    3. Run: pip install kaggle\n"
            f"    4. Re-run this script\n"
            f"\n  Option B — Manual download:\n"
            f"    1. Download from https://www.kaggle.com/datasets/{KAGGLE_SLUG}\n"
            f"    2. Place the CSV at:\n"
            f"       {KAGGLE_FILE}\n"
        )
        print(msg)
        audit["sources"]["kaggle"] = {
            "status"     : "FILE_MISSING",
            "action"     : "manual_download_or_api_token_required",
            "local_file" : str(KAGGLE_FILE),
        }
        return

    import pandas as pd
    df = pd.read_csv(KAGGLE_FILE)
    schema = _audit_dataframe(df, "Kaggle")

    audit["sources"]["kaggle"] = {
        "status"     : "OK",
        "slug"       : KAGGLE_SLUG,
        "local_file" : str(KAGGLE_FILE),
        "md5"        : _md5(KAGGLE_FILE),
        "size_bytes" : KAGGLE_FILE.stat().st_size,
        "schema"     : schema,
    }


def _kaggle_api_download():
    """Attempt programmatic download via kaggle package."""
    try:
        import kaggle  # noqa: F401 — triggers auth check via ~/.kaggle/kaggle.json
        from kaggle.api.kaggle_api_extended import KaggleApiExtended
        api = KaggleApiExtended()
        api.authenticate()
        print("  Kaggle API authenticated. Downloading …")
        api.dataset_download_files(
            KAGGLE_SLUG,
            path=str(RAW_DIR),
            unzip=True,
            quiet=False,
        )
        print("  [OK] Kaggle download complete.")
    except ImportError:
        print("  [INFO] kaggle package not installed (pip install kaggle).")
    except Exception as exc:
        print(f"  [INFO] Kaggle API not available: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "█"*70)
    print("  S0 — Ingest External Sources")
    print("█"*70)

    ingest_zenodo()
    ingest_kaggle()

    # Determine overall status
    statuses = [v.get("status") for v in audit["sources"].values()]
    if all(s == "OK" for s in statuses):
        audit["status"] = "COMPLETE"
    elif "OK" in statuses:
        audit["status"] = "PARTIAL"
    else:
        audit["status"] = "FAILED"

    # Write audit
    with open(AUDIT_PATH, "w") as f:
        json.dump(audit, f, indent=2)

    print("\n" + "─"*70)
    print(f"  Audit written : {AUDIT_PATH}")
    print(f"  Overall status: {audit['status']}")
    print("─"*70)

    if audit["status"] != "COMPLETE":
        print("\n  [WARNING] Not all sources are ready. See action items above.")
        sys.exit(0)   # non-blocking — allows partial runs


if __name__ == "__main__":
    main()
