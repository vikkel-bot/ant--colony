"""
AC-133: Regime-Aware Strategy Selection Summary (Research-Only)

Reads AC-131 (strategy selection) and AC-132 (regime annotation) snapshots
and merges them into one compact, deterministic summary snapshot.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No new ranking, no new regime inference — direct field passthrough.

Input:  data/research/strategy_selection_snapshot.json
        data/research/regime_annotation_snapshot.json
Output: data/research/regime_aware_strategy_selection_summary.json

Fail-closed:
  - missing input file → clean error, exit non-zero
  - missing timeframe in one snapshot → regime falls back to "unknown"
  - empty/incomplete input → valid minimal output, no crash

Usage:
    python ant_colony/research/build_regime_aware_strategy_selection_summary_lite.py
    python ant_colony/research/build_regime_aware_strategy_selection_summary_lite.py \\
        --selection data/research/strategy_selection_snapshot.json \\
        --regime    data/research/regime_annotation_snapshot.json \\
        --out       data/research/regime_aware_strategy_selection_summary.json
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

DEFAULT_SELECTION_PATH = _REPO_ROOT / "data" / "research" / \
                         "strategy_selection_snapshot.json"
DEFAULT_REGIME_PATH    = _REPO_ROOT / "data" / "research" / \
                         "regime_annotation_snapshot.json"
DEFAULT_OUTPUT_PATH    = _REPO_ROOT / "data" / "research" / \
                         "regime_aware_strategy_selection_summary.json"

SNAPSHOT_VERSION = "regime_aware_strategy_selection_summary_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# ---------------------------------------------------------------------------
# Reader (same pattern as AC-131/132)
# ---------------------------------------------------------------------------

def _load_snapshot(path: Path, label: str) -> dict:
    """
    Load a JSON snapshot from disk.
    Raises FileNotFoundError (with label) if absent.
    Raises ValueError if not valid JSON or not a dict.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"[AC-133] {label} snapshot not found: {path}\n"
            f"Run the appropriate upstream step to generate it."
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"[AC-133] {label} snapshot is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"[AC-133] {label} snapshot must be a JSON object.")
    return data


# ---------------------------------------------------------------------------
# Dominant helper (same logic as AC-132 _dominant_regime)
# ---------------------------------------------------------------------------

def _dominant(freq: dict[str, int]) -> Optional[str]:
    """Most frequent key; tie-break alphabetically. None if empty."""
    if not freq:
        return None
    return min(freq, key=lambda k: (-freq[k], k))


# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

def build_snapshot(
    selection: dict,
    regime:    dict,
    ts_utc:    Optional[str] = None,
) -> dict:
    """
    Merge AC-131 selection snapshot and AC-132 regime snapshot.
    No new logic — only field combination. Neither input is mutated.
    """
    tf_list:   list[str]            = list(selection.get("timeframes") or [])
    selected:  dict[str, Optional[str]] = dict(selection.get("selected_per_timeframe") or {})
    annotated: dict[str, dict]      = dict(regime.get("annotated_per_timeframe") or {})

    # summary_per_timeframe: merge strategy + regime per timeframe
    summary_per_tf: dict[str, dict] = {
        tf: {
            "selected_strategy": selected.get(tf),
            "regime":            (annotated.get(tf) or {}).get("regime", "unknown"),
        }
        for tf in tf_list
    }

    # summary_detail: flat list in timeframe order
    summary_detail = [
        {
            "timeframe":         tf,
            "selected_strategy": summary_per_tf[tf]["selected_strategy"],
            "regime":            summary_per_tf[tf]["regime"],
        }
        for tf in tf_list
    ]

    # Frequencies: direct passthrough from source snapshots
    strategy_freq = dict(selection.get("selection_frequency") or {})
    regime_freq   = dict(regime.get("regime_frequency") or {})

    return {
        "version":              SNAPSHOT_VERSION,
        "ts_utc":               ts_utc or _now_utc_iso(),
        "market":               selection.get("market", ""),
        "timeframes":           tf_list,
        "summary_per_timeframe": summary_per_tf,
        "strategy_frequency":   strategy_freq,
        "regime_frequency":     regime_freq,
        "summary_detail":       summary_detail,
        "summary": {
            "dominant_strategy": _dominant(strategy_freq),
            "dominant_regime":   _dominant(regime_freq),
        },
        "flags": dict(FLAGS),
    }


def write_snapshot(snapshot: dict, out_path: Path) -> None:
    """Write snapshot as pretty-printed JSON. Creates parent dirs if absent."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
        f.write("\n")


def build_and_write_snapshot(
    selection_path: Path         = DEFAULT_SELECTION_PATH,
    regime_path:    Path         = DEFAULT_REGIME_PATH,
    out_path:       Path         = DEFAULT_OUTPUT_PATH,
    ts_utc:         Optional[str] = None,
) -> dict:
    """End-to-end: load AC-131 + AC-132 → merge → write. Returns snapshot."""
    selection = _load_snapshot(selection_path, "AC-131 selection")
    regime    = _load_snapshot(regime_path,    "AC-132 regime")
    snapshot  = build_snapshot(selection, regime, ts_utc=ts_utc)
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
        description="AC-133: Regime-aware strategy selection summary (research-only)."
    )
    p.add_argument("--selection", default=str(DEFAULT_SELECTION_PATH))
    p.add_argument("--regime",    default=str(DEFAULT_REGIME_PATH))
    p.add_argument("--out",       default=str(DEFAULT_OUTPUT_PATH))
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        snapshot = build_and_write_snapshot(
            selection_path = Path(args.selection),
            regime_path    = Path(args.regime),
            out_path       = Path(args.out),
        )
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    per_tf   = snapshot["summary_per_timeframe"]
    tf_parts = ", ".join(
        f"{tf}={per_tf[tf]['selected_strategy'] or '—'}/{per_tf[tf]['regime']}"
        for tf in snapshot["timeframes"]
    )
    dom = snapshot["summary"]

    print()
    print("=== AC-133 REGIME-AWARE STRATEGY SELECTION SUMMARY ===")
    print(f"{'market':<10}: {snapshot['market']}")
    print(f"{'timeframes':<10}: {len(snapshot['timeframes'])}")
    print(f"{'summary':<10}: {tf_parts}")
    print(f"{'dominant':<10}: strategy={dom['dominant_strategy'] or '—'}, "
          f"regime={dom['dominant_regime'] or '—'}")
    print(f"{'file':<10}: {args.out}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
