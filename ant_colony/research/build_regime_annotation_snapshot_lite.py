"""
AC-132: Regime Annotation Snapshot (Research-Only)

Reads the AC-131 strategy selection snapshot and annotates each timeframe's
selected strategy with a fixed regime label.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No new ranking or inference — fixed mapping only.

Input:  data/research/strategy_selection_snapshot.json
Output: data/research/regime_annotation_snapshot.json

Fail-closed:
  - missing input file → clean error message, exit non-zero
  - unknown strategy    → regime = "unknown"
  - empty/incomplete input → valid output structure, no crash

Usage:
    python ant_colony/research/build_regime_annotation_snapshot_lite.py
    python ant_colony/research/build_regime_annotation_snapshot_lite.py \\
        --in  data/research/strategy_selection_snapshot.json \\
        --out data/research/regime_annotation_snapshot.json
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
                      "strategy_selection_snapshot.json"
DEFAULT_OUTPUT_PATH = _REPO_ROOT / "data" / "research" / \
                      "regime_annotation_snapshot.json"

SNAPSHOT_VERSION = "regime_annotation_snapshot_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# Fixed, deterministic strategy → regime mapping.
# Unknown strategies fall back to "unknown".
STRATEGY_REGIME_MAP: dict[str, str] = {
    "mean_reversion":          "range",
    "volatility_breakout_lite": "volatile_trend",
    "trend_follow_lite":        "trend",
    "trend_following":          "trend",
    "breakout":                 "volatile_trend",
    "momentum":                 "trend",
}

# ---------------------------------------------------------------------------
# Reader (same pattern as AC-131)
# ---------------------------------------------------------------------------

def load_selection_snapshot(in_path: Path) -> dict:
    """
    Load the AC-131 selection snapshot from disk.
    Raises FileNotFoundError if file is absent.
    Raises ValueError if content is not valid JSON or not a dict.
    """
    if not in_path.exists():
        raise FileNotFoundError(
            f"[AC-132] Input snapshot not found: {in_path}\n"
            "Run AC-131 first to generate the strategy selection snapshot."
        )
    try:
        data = json.loads(in_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"[AC-132] Input snapshot is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("[AC-132] Input snapshot must be a JSON object.")
    return data


# ---------------------------------------------------------------------------
# Regime helpers
# ---------------------------------------------------------------------------

def _map_regime(strategy_name: Optional[str]) -> str:
    """Return regime label for a strategy name. Unknown → 'unknown'."""
    if not strategy_name:
        return "unknown"
    return STRATEGY_REGIME_MAP.get(strategy_name, "unknown")


def _dominant_regime(freq: dict[str, int]) -> Optional[str]:
    """
    Most frequent regime label.
    Tie-break: alphabetically smallest name.
    Returns None if freq is empty.
    """
    if not freq:
        return None
    return min(freq, key=lambda k: (-freq[k], k))


# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

def build_snapshot(
    source: dict,
    ts_utc: Optional[str] = None,
) -> dict:
    """
    Build the regime annotation snapshot from an AC-131 selection snapshot.
    Fixed regime mapping only — no new ranking, no inference.
    Input is not mutated.
    """
    selected: dict[str, Optional[str]] = dict(source.get("selected_per_timeframe") or {})
    tf_list: list[str]                  = list(source.get("timeframes") or [])

    # annotated_per_timeframe: { tf: { selected_strategy, regime } }
    annotated: dict[str, dict] = {
        tf: {
            "selected_strategy": selected.get(tf),
            "regime":            _map_regime(selected.get(tf)),
        }
        for tf in tf_list
    }

    # regime_detail: flat list in timeframe order
    regime_detail = [
        {
            "timeframe":          tf,
            "selected_strategy":  selected.get(tf),
            "regime":             _map_regime(selected.get(tf)),
        }
        for tf in tf_list
    ]

    # regime_frequency: count of each regime label
    freq: dict[str, int] = {}
    for entry in regime_detail:
        label = entry["regime"]
        freq[label] = freq.get(label, 0) + 1

    return {
        "version":               SNAPSHOT_VERSION,
        "ts_utc":                ts_utc or _now_utc_iso(),
        "market":                source.get("market", ""),
        "timeframes":            tf_list,
        "annotated_per_timeframe": annotated,
        "regime_frequency":      freq,
        "regime_detail":         regime_detail,
        "regime_summary": {
            "dominant_regime": _dominant_regime(freq),
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
    in_path:  Path          = DEFAULT_INPUT_PATH,
    out_path: Path          = DEFAULT_OUTPUT_PATH,
    ts_utc:   Optional[str] = None,
) -> dict:
    """End-to-end: load AC-131 snapshot → annotate → write. Returns snapshot."""
    source   = load_selection_snapshot(in_path)
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
        description="AC-132: Regime annotation snapshot (research-only)."
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

    regimes   = snapshot["annotated_per_timeframe"]
    reg_parts = ", ".join(
        f"{tf}={regimes[tf]['regime']}"
        for tf in snapshot["timeframes"]
    )
    dominant = snapshot["regime_summary"]["dominant_regime"] or "—"

    print()
    print("=== AC-132 REGIME ANNOTATION SNAPSHOT ===")
    print(f"{'market':<10}: {snapshot['market']}")
    print(f"{'timeframes':<10}: {len(snapshot['timeframes'])}")
    print(f"{'regimes':<10}: {reg_parts}")
    print(f"{'dominant':<10}: {dominant}")
    print(f"{'file':<10}: {out_path}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
