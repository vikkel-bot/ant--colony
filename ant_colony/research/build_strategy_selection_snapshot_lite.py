"""
AC-131: Strategy Selection Snapshot (Research-Only)

Reads the AC-130 multi-timeframe comparison snapshot and writes a compact
strategy selection snapshot. No new logic — direct field passthrough.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.

Input:  data/research/multi_timeframe_comparison_snapshot.json
Output: data/research/strategy_selection_snapshot.json

Fail-closed:
  - missing input file → clean error message, exit non-zero
  - empty or incomplete snapshot → valid output structure, no crash

Usage:
    python ant_colony/research/build_strategy_selection_snapshot_lite.py
    python ant_colony/research/build_strategy_selection_snapshot_lite.py \\
        --in  data/research/multi_timeframe_comparison_snapshot.json \\
        --out data/research/strategy_selection_snapshot.json
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

DEFAULT_INPUT_PATH    = _REPO_ROOT / "data" / "research" / \
                        "multi_timeframe_comparison_snapshot.json"
DEFAULT_OUTPUT_PATH   = _REPO_ROOT / "data" / "research" / \
                        "strategy_selection_snapshot.json"

SNAPSHOT_VERSION = "strategy_selection_snapshot_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

def load_comparison_snapshot(in_path: Path) -> dict:
    """
    Load the AC-130 snapshot from disk.
    Returns parsed dict on success.
    Raises FileNotFoundError if file is absent.
    Raises ValueError if content is not valid JSON or not a dict.
    """
    if not in_path.exists():
        raise FileNotFoundError(
            f"[AC-131] Input snapshot not found: {in_path}\n"
            "Run AC-130 first to generate the multi-timeframe comparison snapshot."
        )
    try:
        raw = in_path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"[AC-131] Input snapshot is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("[AC-131] Input snapshot must be a JSON object.")
    return data


# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

def build_snapshot(
    source: dict,
    ts_utc: Optional[str] = None,
) -> dict:
    """
    Build the selection snapshot from an AC-130 comparison snapshot.
    Direct field passthrough — no new ranking or filtering.
    Input is not mutated.
    """
    top_per_tf  = dict(source.get("top_per_timeframe") or {})
    frequency   = dict(source.get("frequency") or {})
    tf_list     = list(source.get("timeframes") or [])

    selection_detail = [
        {
            "timeframe":          tf,
            "selected_strategy":  top_per_tf.get(tf),
        }
        for tf in tf_list
    ]

    return {
        "version":               SNAPSHOT_VERSION,
        "ts_utc":                ts_utc or _now_utc_iso(),
        "market":                source.get("market", ""),
        "timeframes":            tf_list,
        "selected_per_timeframe": top_per_tf,
        "selection_frequency":   frequency,
        "selection_detail":      selection_detail,
        "flags":                 dict(FLAGS),
    }


def write_snapshot(snapshot: dict, out_path: Path) -> None:
    """Write snapshot as pretty-printed JSON. Creates parent dirs if absent."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
        f.write("\n")


def build_and_write_snapshot(
    in_path:  Path         = DEFAULT_INPUT_PATH,
    out_path: Path         = DEFAULT_OUTPUT_PATH,
    ts_utc:   Optional[str] = None,
) -> dict:
    """
    End-to-end: load AC-130 snapshot → build selection snapshot → write.
    Propagates FileNotFoundError / ValueError on bad input.
    Returns the written snapshot dict.
    """
    source   = load_comparison_snapshot(in_path)
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
        description="AC-131: Strategy selection snapshot (research-only)."
    )
    p.add_argument("--in",  dest="input",  default=str(DEFAULT_INPUT_PATH))
    p.add_argument("--out", dest="output", default=str(DEFAULT_OUTPUT_PATH))
    return p


def main(argv: list[str] | None = None) -> int:
    args     = _build_parser().parse_args(argv)
    in_path  = Path(args.input)
    out_path = Path(args.output)

    try:
        snapshot = build_and_write_snapshot(
            in_path  = in_path,
            out_path = out_path,
        )
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    selected = snapshot["selected_per_timeframe"]
    top_parts = ", ".join(
        f"{tf}={selected.get(tf) or '—'}"
        for tf in snapshot["timeframes"]
    )
    print()
    print("=== AC-131 STRATEGY SELECTION SNAPSHOT ===")
    print(f"{'market':<10}: {snapshot['market']}")
    print(f"{'timeframes':<10}: {len(snapshot['timeframes'])}")
    print(f"{'selected':<10}: {top_parts}")
    print(f"{'file':<10}: {out_path}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
