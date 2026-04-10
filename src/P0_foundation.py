from __future__ import annotations

"""
Proposal foundation module — Stage P0.

Purpose:
- establish the project runtime deterministically for the new paper proposal
- validate that the required OULAD source files are accessible (read-only)
- install any missing lightweight dependencies (no survival-analysis stack)
- create the output directory tree
- persist environment_summary.json and pipeline_audit.json

Input contract (read-only, from parent project):
- ../../content/studentVle.csv
- ../../content/studentInfo.csv
- ../../content/studentAssessment.csv
- ../../content/assessments.csv
- ../../content/courses.csv

Output contract:
- ../outputs/data/               (created, empty — used by P1 onwards)
- ../outputs/metadata/environment_summary.json
- ../outputs/metadata/pipeline_audit.json

Failure policy:
- missing required source files raise immediately
- missing required packages raise immediately after install attempt
- no silent degradation paths are permitted
"""

import importlib.metadata
import json
import os
import platform
import random
import re
import subprocess
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


# ==============================================================
# Constants
# ==============================================================

STAGE_PREFIX = "P0"
SCRIPT_NAME = Path(__file__).name
SCRIPT_PATH = Path(__file__).resolve()
SRC_DIR = SCRIPT_PATH.parent                  # next_proposal_paper/src/
PROPOSAL_ROOT = SRC_DIR.parent                # next_proposal_paper/
PROJECT_ROOT = PROPOSAL_ROOT.parent           # dropout_test-1/  (original project, read-only)

OUTPUT_DIR = PROPOSAL_ROOT / "outputs"
DATA_DIR = OUTPUT_DIR / "data"
METADATA_DIR = OUTPUT_DIR / "metadata"

OULAD_DATA_DIR = PROJECT_ROOT / "content"    # read-only; CSVs already present from paper 1

SEED = 42
ENROLLMENT_KEY = ["id_student", "code_module", "code_presentation"]

# Only the OULAD files required for this proposal (subset of the full OULAD set)
REQUIRED_SOURCE_FILES = {
    "studentVle":        "studentVle.csv",
    "studentInfo":       "studentInfo.csv",
    "studentAssessment": "studentAssessment.csv",
    "assessments":       "assessments.csv",
    "courses":           "courses.csv",
}

# Lightweight package requirements — deliberately excludes survival/neural libs
REQUIRED_PACKAGES = [
    "pandas",
    "numpy",
    "duckdb",
    "pyarrow",
    "scikit-learn",
]


# ==============================================================
# Logging helpers  (same convention as dropout_bench_v3_A_1)
# ==============================================================

def log_stage_start(number: str, title: str) -> None:
    print(f"[START] {STAGE_PREFIX}.{number} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("# " + "=" * 62)
    print(f"# {STAGE_PREFIX}.{number} - {title}")
    print("# " + "=" * 62)


def log_stage_end(number: str) -> None:
    print(f"[END] {STAGE_PREFIX}.{number} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def print_kv(label: str, value: Any) -> None:
    print(f"- {label}: {value}")


def print_artifact(label: str, location: Any) -> None:
    print(f"ARTIFACT | {label} | {location}")


def print_section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ==============================================================
# Package helpers
# ==============================================================

def package_version(distribution_name: str) -> str:
    try:
        return importlib.metadata.version(distribution_name)
    except importlib.metadata.PackageNotFoundError:
        return "not-installed"


def distribution_is_installed(distribution_name: str) -> bool:
    return package_version(distribution_name) != "not-installed"


def install_package(package_spec: str) -> tuple[bool, str]:
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", package_spec],
        capture_output=True,
        text=True,
        check=False,
    )
    output = (result.stdout or "").strip() or (result.stderr or "").strip()
    return result.returncode == 0, output


def parse_distribution_name(package_spec: str) -> str:
    """Extract the bare distribution name from a pip requirement spec."""
    normalized = package_spec.split(";", 1)[0].strip()
    match = re.match(r"^[A-Za-z0-9][A-Za-z0-9_.-]*", normalized)
    if match is None:
        raise ValueError(f"Invalid package spec: {package_spec!r}")
    return match.group(0)


# ==============================================================
# Utilities
# ==============================================================

def save_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def collect_environment_summary() -> dict[str, Any]:
    return {
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "python_executable": sys.executable,
        "seed": SEED,
        "pandas_version":      package_version("pandas"),
        "numpy_version":       package_version("numpy"),
        "duckdb_version":      package_version("duckdb"),
        "pyarrow_version":     package_version("pyarrow"),
        "scikit_learn_version": package_version("scikit-learn"),
    }


# ==============================================================
# Stage P0.1 — Runtime bootstrap
# ==============================================================

def stage_p0_1_runtime_bootstrap() -> None:
    log_stage_start("1", "Runtime Bootstrap")
    print_kv("SCRIPT_NAME",    SCRIPT_NAME)
    print_kv("SCRIPT_PATH",    SCRIPT_PATH)
    print_kv("SRC_DIR",        SRC_DIR)
    print_kv("PROPOSAL_ROOT",  PROPOSAL_ROOT)
    print_kv("PROJECT_ROOT",   PROJECT_ROOT)
    print_kv("OULAD_DATA_DIR", OULAD_DATA_DIR)
    print_kv("OUTPUT_DIR",     OUTPUT_DIR)
    print_kv("PYTHON",         sys.executable)
    print_kv("SEED",           SEED)
    log_stage_end("1")


# ==============================================================
# Stage P0.2 — Dependency bootstrap
# ==============================================================

def stage_p0_2_dependency_bootstrap() -> None:
    log_stage_start("2", "Dependency Validation and Install")

    missing = [pkg for pkg in REQUIRED_PACKAGES if not distribution_is_installed(parse_distribution_name(pkg))]

    if not missing:
        print("All required packages are already installed.")
        for pkg in REQUIRED_PACKAGES:
            name = parse_distribution_name(pkg)
            print_kv(name, package_version(name))
        log_stage_end("2")
        return

    failures: list[tuple[str, str]] = []
    for pkg in missing:
        print(f"Installing: {pkg}")
        success, output = install_package(pkg)
        if not success:
            failures.append((pkg, output))
        else:
            print(f"  → OK: {pkg}")

    still_missing = [pkg for pkg in REQUIRED_PACKAGES if not distribution_is_installed(parse_distribution_name(pkg))]
    if still_missing:
        for pkg, output in failures:
            print(f"FAILED | {pkg}")
            print(output.splitlines()[-1] if output else "[no output]")
        raise RuntimeError(
            "P0.2 could not provision required packages: "
            + ", ".join(parse_distribution_name(p) for p in still_missing)
        )

    for pkg in REQUIRED_PACKAGES:
        name = parse_distribution_name(pkg)
        print_kv(name, package_version(name))

    log_stage_end("2")


# ==============================================================
# Stage P0.3 — Import runtime stack and set determinism
# ==============================================================

def stage_p0_3_import_and_determinism() -> tuple[Any, Any]:
    log_stage_start("3", "Runtime Imports and Determinism")

    import numpy as np
    import pandas as pd

    random.seed(SEED)
    np.random.seed(SEED)
    os.environ.setdefault("PYTHONHASHSEED", str(SEED))

    warnings.simplefilter("default")
    pd.set_option("display.max_columns", 200)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_rows", 100)

    print_kv("SEED",           SEED)
    print_kv("PYTHONHASHSEED", os.environ.get("PYTHONHASHSEED"))
    print_kv("numpy",          np.__version__)
    print_kv("pandas",         pd.__version__)

    log_stage_end("3")
    return np, pd


# ==============================================================
# Stage P0.4 — Validate OULAD source files (read-only check)
# ==============================================================

def stage_p0_4_validate_source_files(pd: Any) -> None:
    log_stage_start("4", "OULAD Source File Validation (read-only)")

    if not OULAD_DATA_DIR.exists():
        raise FileNotFoundError(
            f"OULAD data directory not found: {OULAD_DATA_DIR}\n"
            "Expected the original project's content/ folder to be present."
        )

    rows = []
    missing = []
    for table_name, filename in REQUIRED_SOURCE_FILES.items():
        path = OULAD_DATA_DIR / filename
        exists = path.exists()
        size_kb = round(path.stat().st_size / 1024, 1) if exists else None
        rows.append({
            "table":    table_name,
            "filename": filename,
            "exists":   exists,
            "size_kb":  size_kb,
            "path":     str(path),
        })
        if not exists:
            missing.append(filename)

    contract_df = pd.DataFrame(rows)
    print("\nSource file contract:")
    print(contract_df.to_string(index=False))

    if missing:
        raise FileNotFoundError(
            "Missing required OULAD source files in " + str(OULAD_DATA_DIR) + ": "
            + ", ".join(missing)
        )

    print("\nAll required source files are present.")
    log_stage_end("4")


# ==============================================================
# Stage P0.5 — Create output directory tree
# ==============================================================

def stage_p0_5_create_output_tree() -> None:
    log_stage_start("5", "Output Directory Tree")

    required_dirs = [
        OUTPUT_DIR,
        DATA_DIR,
        METADATA_DIR,
    ]

    for directory in required_dirs:
        directory.mkdir(parents=True, exist_ok=True)
        print_artifact("directory", directory)

    log_stage_end("5")


# ==============================================================
# Stage P0.6 — Persist environment_summary.json and pipeline_audit.json
# ==============================================================

def stage_p0_6_persist_metadata(run_id: str) -> None:
    log_stage_start("6", "Metadata Persistence")

    env_summary = collect_environment_summary()
    env_summary["run_id"] = run_id
    env_summary["proposal_root"] = str(PROPOSAL_ROOT)
    env_summary["oulad_data_dir"] = str(OULAD_DATA_DIR)

    env_path = METADATA_DIR / "environment_summary.json"
    save_json(env_summary, env_path)
    print_artifact("environment_summary.json", env_path)

    audit = {
        "run_id": run_id,
        "stage": "P0",
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
        "status": "completed",
        "source_files": {
            table_name: str(OULAD_DATA_DIR / filename)
            for table_name, filename in REQUIRED_SOURCE_FILES.items()
        },
        "output_dirs": {
            "output_dir":   str(OUTPUT_DIR),
            "data_dir":     str(DATA_DIR),
            "metadata_dir": str(METADATA_DIR),
        },
        "packages": {
            parse_distribution_name(pkg): package_version(parse_distribution_name(pkg))
            for pkg in REQUIRED_PACKAGES
        },
    }

    audit_path = METADATA_DIR / "pipeline_audit.json"
    # Merge with existing audit if it already exists (later stages append to it)
    if audit_path.exists():
        try:
            existing = json.loads(audit_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        if isinstance(existing, dict):
            existing["P0"] = audit
            save_json(existing, audit_path)
        else:
            save_json({"P0": audit}, audit_path)
    else:
        save_json({"P0": audit}, audit_path)

    print_artifact("pipeline_audit.json", audit_path)
    log_stage_end("6")


# ==============================================================
# Main entry point
# ==============================================================

def main() -> None:
    print_section("PROPOSAL STAGE P0 — Foundation")
    print_kv("started_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    run_id = uuid4().hex

    stage_p0_1_runtime_bootstrap()
    stage_p0_2_dependency_bootstrap()
    np, pd = stage_p0_3_import_and_determinism()
    stage_p0_4_validate_source_files(pd)
    stage_p0_5_create_output_tree()
    stage_p0_6_persist_metadata(run_id)

    print_section("P0 COMPLETE")
    print_kv("run_id",       run_id)
    print_kv("completed_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_kv("next_step",    "Run src/P1_ingestion.py")


if __name__ == "__main__":
    main()
