"""
S5_run_llm_survey.py
─────────────────────
Calls the Anthropic Claude API with each persona prompt from
student_personas_with_prompts.csv and stores the structured JSON responses.
Each response contains answers to Q1–Q24 (proposal.md Appendix A) in character.

Features
────────
• Parallel execution via ThreadPoolExecutor (--workers N, default 5)
• Sequential fallback with --workers 1
• Resume-safe: skips personas whose JSON file already exists (safe to interrupt)
• Rate-limit / server-error handling: exponential backoff with MAX_RETRIES
• JSON validation: retries if Claude returns malformed JSON or missing fields
• Linkback: every response JSON contains persona_id → join on student_personas.csv
• Aggregated output: llm_survey_responses.csv (one flat row per persona)
• Progress reporting every PROGRESS_EVERY calls with elapsed time + ETA

Setup
─────
  pip install anthropic
  export ANTHROPIC_API_KEY="sk-ant-..."

Usage
─────
  python S5_run_llm_survey.py                         # run all 1300 (5 workers)
  python S5_run_llm_survey.py --workers 10              # faster if tier allows
  python S5_run_llm_survey.py --workers 1               # sequential (safe)
  python S5_run_llm_survey.py --dry-run                 # test first 3 (mock)
  python S5_run_llm_survey.py --limit 10                # first 10 only
  python S5_run_llm_survey.py --limit 10 --workers 3    # first 10, 3 parallel

Inputs
──────
outputs/data/synthetic/student_personas_with_prompts.csv

Outputs
───────
outputs/data/synthetic/llm_responses/P{id}.json   (one per persona)
outputs/data/synthetic/llm_survey_responses.csv    (aggregated flat CSV)
outputs/metadata/s5_llm_audit.json
"""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# ── configuration ─────────────────────────────────────────────────────────────
MODEL          = "claude-opus-4-5"   # use claude-3-5-sonnet-20241022 for lower cost
MAX_TOKENS     = 4096                # Q1–Q24 narrative answers need room
TEMPERATURE    = 0.7
MAX_RETRIES    = 5
BACKOFF_BASE      = 2.0   # seconds; doubles each retry
INTER_CALL_DELAY  = 0.3   # seconds between successful calls (per worker)
PROGRESS_EVERY    = 25    # print progress every N personas
DEFAULT_WORKERS   = 5    # parallel threads (each makes independent API calls)

# Q1–Q24 field names (must match S4 JSON schema)
QUESTION_IDS = [f"Q{i}" for i in range(1, 25)]

REQUIRED_FIELDS = set(QUESTION_IDS) | {
    "persona_id", "overall_engagement_self_assessment", "dominant_themes"
}
ENUM_ENGAGEMENT = {"high", "medium", "low"}

# ── paths ─────────────────────────────────────────────────────────────────────
BASE          = Path(__file__).resolve().parent.parent
SYN           = BASE / "outputs" / "data" / "synthetic"
RESPONSES_DIR = SYN / "llm_responses"
META          = BASE / "outputs" / "metadata"

PROMPTS_IN    = SYN / "student_personas_with_prompts.csv"
AGGREGATE_OUT = SYN / "llm_survey_responses.csv"
AUDIT_PATH    = META / "s5_llm_audit.json"

SYN.mkdir(parents=True, exist_ok=True)
RESPONSES_DIR.mkdir(parents=True, exist_ok=True)
META.mkdir(parents=True, exist_ok=True)

# ── helpers ───────────────────────────────────────────────────────────────────

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


def validate_response(data: dict) -> list:
    errors = []
    missing = REQUIRED_FIELDS - set(data.keys())
    if missing:
        errors.append(f"Missing fields: {sorted(missing)}")
    if "overall_engagement_self_assessment" in data:
        val = str(data["overall_engagement_self_assessment"]).lower().strip()
        if val not in ENUM_ENGAGEMENT:
            errors.append(f"Invalid overall_engagement_self_assessment: {val!r}")
    if "dominant_themes" in data and not isinstance(data["dominant_themes"], list):
        errors.append("dominant_themes must be a list")
    return errors


def extract_json(raw_text: str) -> dict:
    """Extract JSON from Claude response, stripping markdown fences if present."""
    text = raw_text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(
            line for line in lines if not line.strip().startswith("```")
        ).strip()
    return json.loads(text)


def response_path(pid: str) -> Path:
    return RESPONSES_DIR / f"{pid}.json"


def already_done(pid: str) -> bool:
    return response_path(pid).exists()


def call_claude(client, prompt: str, pid: str) -> dict:
    """Call Claude with exponential-backoff retry. Returns validated JSON dict."""
    import anthropic

    system_msg = (
        "You are a method actor who fully embodies student personas. "
        "You always respond in first person, completely in character, "
        "and return ONLY valid JSON — no text before or after the JSON object."
    )

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.messages.create(
                model      = MODEL,
                max_tokens = MAX_TOKENS,
                temperature= TEMPERATURE,
                system     = system_msg,
                messages   = [{"role": "user", "content": prompt}],
            )
            raw  = response.content[0].text
            data = extract_json(raw)
            errors = validate_response(data)
            if errors:
                raise ValueError(f"Schema validation failed: {errors}")
            data["persona_id"] = pid   # enforce correct ID
            return data

        except Exception as e:
            err_type = type(e).__name__
            wait = BACKOFF_BASE ** attempt
            print(f"    [{err_type}] attempt {attempt}/{MAX_RETRIES} "
                  f"for {pid}: {e} — retrying in {wait:.0f}s …")
            time.sleep(wait)
            last_error = e

    raise RuntimeError(
        f"Failed after {MAX_RETRIES} attempts for {pid}: {last_error}"
    )


def mock_response(pid: str) -> dict:
    """Deterministic mock for --dry-run mode (no API call)."""
    base = {
        "persona_id"                         : pid,
        "overall_engagement_self_assessment" : "medium",
        "dominant_themes"                    : ["deadline-driven", "offline-study"],
    }
    for qid in QUESTION_IDS:
        base[qid] = f"[DRY-RUN] This is a mock answer for {qid} from persona {pid}."
    return base


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="S5 — Run semi-structured interview survey via Anthropic Claude"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Process first 3 personas with mock responses (no API call)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only first N personas")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel threads (default {DEFAULT_WORKERS}; use 1 for sequential)")
    args = parser.parse_args()

    print("█" * 70)
    print("  S5 — LLM Survey  |  Anthropic Claude  |  24-question interview")
    print("█" * 70)
    print()

    # [1] Load prompts
    print("  [1] Loading student_personas_with_prompts.csv …")
    if not PROMPTS_IN.exists():
        sys.exit(f"[ERROR] {PROMPTS_IN} not found — run S4 first.")
    df = pd.read_csv(PROMPTS_IN)
    if "persona_prompt" not in df.columns:
        sys.exit("[ERROR] Column 'persona_prompt' missing — re-run S4.")
    print(f"    Loaded: {df.shape}")
    print()

    # Apply mode restrictions
    if args.dry_run:
        df      = df.head(3)
        dry_run = True
        print("  [DRY-RUN] First 3 personas — mock responses only.\n")
    else:
        dry_run = False
        if args.limit:
            df = df.head(args.limit)
            print(f"  [--limit {args.limit}]\n")

    # [2] Claude client
    client = None
    if not dry_run:
        try:
            import anthropic
        except ImportError:
            sys.exit("[ERROR] anthropic not installed — run: pip install anthropic")
        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            sys.exit("[ERROR] ANTHROPIC_API_KEY environment variable not set.")
        client = anthropic.Anthropic(api_key=api_key)
        print(f"  [2] Claude client ready. Model: {MODEL}\n")

    n_workers = 1 if dry_run else max(1, args.workers)
    print(f"  [3] Processing personas … (workers={n_workers})")

    n_total   = len(df)
    n_done    = 0
    n_skipped = 0
    n_errors  = 0
    error_log = []
    _lock     = threading.Lock()
    start_ts  = time.time()

    def _process_one(row):
        """Worker function — called once per persona row."""
        pid    = str(row["persona_id"])
        prompt = str(row["persona_prompt"])

        # Resume-safe: skip if file already written
        if already_done(pid) and not dry_run:
            with _lock:
                nonlocal n_skipped
                n_skipped += 1
            return "skipped", pid, None

        try:
            data = mock_response(pid) if dry_run else call_claude(client, prompt, pid)
            with open(response_path(pid), "w", encoding="utf-8") as f:
                json.dump(_json_safe(data), f, indent=2, ensure_ascii=False)
            if not dry_run:
                time.sleep(INTER_CALL_DELAY)
            return "done", pid, None
        except RuntimeError as e:
            return "error", pid, str(e)

    rows = [row for _, row in df.iterrows()]

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(_process_one, row): row["persona_id"] for row in rows}
        for future in as_completed(futures):
            status, pid, err = future.result()
            with _lock:
                if status == "done":
                    n_done += 1
                elif status == "skipped":
                    n_skipped += 1
                else:
                    n_errors += 1
                    error_log.append({"persona_id": pid, "error": err})
                    print(f"    [SKIP] {pid}: max retries exceeded.")

                total_processed = n_done + n_skipped
                if total_processed % PROGRESS_EVERY == 0 or total_processed == n_total:
                    elapsed = time.time() - start_ts
                    rate    = n_done / elapsed if elapsed > 0 else float("inf")
                    eta_s   = (n_total - total_processed) / rate if rate > 0 else float("inf")
                    print(f"    [{total_processed:4d}/{n_total}] "
                          f"done={n_done}  skipped={n_skipped}  errors={n_errors}  "
                          f"rate={rate:.2f}/s  ETA={eta_s:.0f}s")

    elapsed_total = time.time() - start_ts
    print()
    print(f"  Summary: {n_done} new | {n_skipped} skipped | "
          f"{n_errors} errors | {elapsed_total:.1f}s")
    print()

    # [4] Aggregate into flat CSV
    print("  [4] Aggregating responses …")
    all_data = []
    for p in sorted(RESPONSES_DIR.glob("P*.json")):
        with open(p, encoding="utf-8") as f:
            all_data.append(json.load(f))

    if all_data:
        agg = pd.DataFrame(all_data)
        # Ensure Q1–Q24 columns appear in order right after persona_id
        cols_ordered = (
            ["persona_id"]
            + QUESTION_IDS
            + ["overall_engagement_self_assessment", "dominant_themes"]
        )
        # Add any extra cols that might exist
        extra = [c for c in agg.columns if c not in cols_ordered]
        agg = agg[[c for c in cols_ordered if c in agg.columns] + extra]
        # Flatten dominant_themes list → pipe-separated string
        if "dominant_themes" in agg.columns:
            agg["dominant_themes"] = agg["dominant_themes"].apply(
                lambda x: "|".join(x) if isinstance(x, list) else str(x)
            )
        agg.to_csv(AGGREGATE_OUT, index=False)
        print(f"    llm_survey_responses.csv : {agg.shape}  → {AGGREGATE_OUT}")
        print(f"    Columns: persona_id + Q1–Q24 + 2 summary cols")
    else:
        print("    No responses to aggregate yet.")
    print()

    # [5] Audit
    print("  [5] Writing audit …")
    audit = {
        "status"         : "COMPLETE" if n_errors == 0 else "PARTIAL",
        "run_at"         : datetime.now(timezone.utc).isoformat(),
        "dry_run"        : dry_run,
        "workers"        : n_workers,
        "model"          : MODEL,
        "temperature"    : TEMPERATURE,
        "max_tokens"     : MAX_TOKENS,
        "n_total_in_csv" : int(len(pd.read_csv(PROMPTS_IN))),
        "n_new_responses": int(n_done),
        "n_skipped"      : int(n_skipped),
        "n_errors"       : int(n_errors),
        "elapsed_seconds": round(elapsed_total, 2),
        "responses_dir"  : str(RESPONSES_DIR),
        "aggregate_csv"  : str(AGGREGATE_OUT),
        "errors"         : error_log,
    }
    with open(AUDIT_PATH, "w") as f:
        json.dump(_json_safe(audit), f, indent=2)

    print(f"  Audit : {AUDIT_PATH}")
    status = "COMPLETE ✓" if n_errors == 0 else "PARTIAL ⚠  (check errors in audit)"
    print(f"  Status: {status}")


if __name__ == "__main__":
    main()
