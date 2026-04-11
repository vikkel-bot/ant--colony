"""
AC-135: Queen Candidate Decision Snapshot (Research-Only)

Reads the AC-134 regime-aware allocation summary and derives a single
queen candidate decision: the timeframe with the highest allocation_weight.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No new allocation or regime logic — selection from existing AC-134 output only.

Input:  data/research/regime_aware_allocation_summary.json
Output: data/research/queen_candidate_decision_snapshot.json

Fail-closed:
  - missing input file         → clean error, exit non-zero
  - missing allocation_weight  → treated as 0.0
  - empty/incomplete input     → valid minimal output, no crash

Usage:
    python ant_colony/research/build_queen_candidate_decision_snapshot_lite.py
    python ant_colony/research/build_queen_candidate_decision_snapshot_lite.py \\
        --in  data/research/regime_aware_allocation_summary.json \\
        --out data/research/queen_candidate_decision_snapshot.json
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_RESEARCH_DIR = Path(__file__).resolve().parent
_REPO_ROOT    = _RESEARCH_DIR.parent.parent

DEFAULT_INPUT_PATH  = _REPO_ROOT / "data" / "research" / \
                      "regime_aware_allocation_summary.json"
DEFAULT_OUTPUT_PATH = _REPO_ROOT / "data" / "research" / \
                      "queen_candidate_decision_snapshot.json"

SNAPSHOT_VERSION = "queen_candidate_decision_snapshot_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

RATIONALE_SUMMARY = {
    "selection_basis": "highest_allocation_weight",
    "tie_break":       "alphabetical_timeframe",
}

# ---------------------------------------------------------------------------
# Reader (same pattern as AC-131–134)
# ---------------------------------------------------------------------------

def _load_snapshot(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(
            f"[AC-135] Input snapshot not found: {path}\n"
            "Run AC-134 first to generate the regime-aware allocation summary."
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"[AC-135] Input snapshot is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("[AC-135] Input snapshot must be a JSON object.")
    return data


# ---------------------------------------------------------------------------
# Candidate selection
# ---------------------------------------------------------------------------

def _choose(
    tf_list: list[str],
    alloc_per_tf: dict[str, dict],
) -> Optional[str]:
    """
    Return the timeframe with the highest allocation_weight.
    Tie-break: alphabetically smallest timeframe string.
    Returns None if tf_list is empty.
    """
    if not tf_list:
        return None
    return min(
        tf_list,
        key=lambda tf: (
            -(alloc_per_tf.get(tf) or {}).get("allocation_weight", 0.0),
            tf,
        ),
    )


# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

def build_snapshot(
    source: dict,
    ts_utc: Optional[str] = None,
) -> dict:
    """
    Build queen candidate decision snapshot from AC-134 allocation summary.
    Selection only — no new allocation or regime logic. Input not mutated.
    """
    tf_list      = list(source.get("timeframes") or [])
    alloc_per_tf = dict(source.get("allocation_per_timeframe") or {})
    alloc_sum    = dict(source.get("allocation_summary") or {})

    chosen_tf   = _choose(tf_list, alloc_per_tf)
    chosen_rec  = (alloc_per_tf.get(chosen_tf) or {}) if chosen_tf else {}

    return {
        "version":    SNAPSHOT_VERSION,
        "ts_utc":     ts_utc or _now_utc_iso(),
        "market":     source.get("market", ""),
        "timeframes": tf_list,
        "candidate_decision": {
            "chosen_timeframe":        chosen_tf,
            "chosen_strategy":         chosen_rec.get("selected_strategy"),
            "chosen_regime":           chosen_rec.get("regime", "unknown"),
            "chosen_allocation_weight": chosen_rec.get("allocation_weight", 0.0),
        },
        "decision_context": {
            "dominant_strategy": alloc_sum.get("dominant_strategy"),
            "dominant_regime":   alloc_sum.get("dominant_regime"),
            "weights_sum":       alloc_sum.get("weights_sum", 0.0),
        },
        "rationale_summary": dict(RATIONALE_SUMMARY),
        "flags": dict(FLAGS),
    }


def write_snapshot(snapshot: dict, out_path: Path) -> None:
    """Write snapshot as pretty-printed JSON. Creates parent dirs if absent."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
        f.write("\n")


def build_and_write_snapshot(
    in_path:  Path          = DEFAULT_INPUT_PATH,
    out_path: Path          = DEFAULT_OUTPUT_PATH,
    ts_utc:   Optional[str] = None,
) -> dict:
    """End-to-end: load AC-134 → build decision snapshot → write."""
    source   = _load_snapshot(in_path)
    snapshot = build_snapshot(source, ts_utc=ts_utc)
    write_snapshot(snapshot, out_path)
    return snapshot


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-135: Queen candidate decision snapshot (research-only)."
    )
    p.add_argument("--in",  dest="input",  default=str(DEFAULT_INPUT_PATH))
    p.add_argument("--out", dest="output", default=str(DEFAULT_OUTPUT_PATH))
    return p


def main(argv: list[str] | None = None) -> int:
    args     = _build_parser().parse_args(argv)
    in_path  = Path(args.input)
    out_path = Path(args.output)

    try:
        snapshot = build_and_write_snapshot(in_path=in_path, out_path=out_path)
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    cd = snapshot["candidate_decision"]
    print()
    print("=== AC-135 QUEEN CANDIDATE DECISION SNAPSHOT ===")
    print(f"{'market':<10}: {snapshot['market']}")
    print(f"{'timeframes':<10}: {len(snapshot['timeframes'])}")
    print(
        f"{'chosen':<10}: {cd['chosen_timeframe']} / "
        f"{cd['chosen_strategy']} / "
        f"{cd['chosen_regime']} / "
        f"{cd['chosen_allocation_weight']}"
    )
    print(f"{'file':<10}: {out_path}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
