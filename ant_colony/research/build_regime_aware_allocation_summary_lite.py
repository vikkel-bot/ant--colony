"""
AC-134: Regime-Aware Allocation Research Summary (Research-Only)

Reads the AC-133 regime-aware strategy selection summary and derives a
compact allocation weight per timeframe using a fixed strategy+regime mapping.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No optimizer, no inference — fixed mapping + normalization only.

Input:  data/research/regime_aware_strategy_selection_summary.json
Output: data/research/regime_aware_allocation_summary.json

Fail-closed:
  - missing input file          → clean error, exit non-zero
  - unknown strategy/regime     → base_weight = 0.25
  - all weights zero            → equal distribution
  - empty/incomplete input      → valid minimal output, no crash

Usage:
    python ant_colony/research/build_regime_aware_allocation_summary_lite.py
    python ant_colony/research/build_regime_aware_allocation_summary_lite.py \\
        --in  data/research/regime_aware_strategy_selection_summary.json \\
        --out data/research/regime_aware_allocation_summary.json
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
                      "regime_aware_strategy_selection_summary.json"
DEFAULT_OUTPUT_PATH = _REPO_ROOT / "data" / "research" / \
                      "regime_aware_allocation_summary.json"

SNAPSHOT_VERSION = "regime_aware_allocation_summary_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# Fixed, deterministic (strategy, regime) → base_weight mapping.
# Unknown combinations fall back to FALLBACK_WEIGHT.
WEIGHT_MAP: dict[tuple[str, str], float] = {
    ("mean_reversion",          "range"):          1.0,
    ("volatility_breakout_lite","volatile_trend"):  1.0,
    ("trend_following",         "trend"):           1.0,
    ("trend_follow_lite",       "trend"):           1.0,
    ("momentum",                "trend"):           1.0,
    ("mean_reversion",          "volatile_trend"):  0.5,
    ("volatility_breakout_lite","range"):           0.5,
    ("breakout",                "range"):           0.5,
    ("unknown",                 "unknown"):         0.25,
}
FALLBACK_WEIGHT = 0.25

# ---------------------------------------------------------------------------
# Reader (same pattern as AC-131/132/133)
# ---------------------------------------------------------------------------

def _load_snapshot(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(
            f"[AC-134] Input snapshot not found: {path}\n"
            "Run AC-133 first to generate the regime-aware strategy selection summary."
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"[AC-134] Input snapshot is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("[AC-134] Input snapshot must be a JSON object.")
    return data


# ---------------------------------------------------------------------------
# Weight helpers
# ---------------------------------------------------------------------------

def _base_weight(strategy: Optional[str], regime: Optional[str]) -> float:
    """Return base weight for a strategy+regime pair. Unknown → FALLBACK_WEIGHT."""
    key = (strategy or "unknown", regime or "unknown")
    return WEIGHT_MAP.get(key, FALLBACK_WEIGHT)


def _normalize(weights: list[float]) -> list[float]:
    """
    Normalize a list of weights to sum to 1.0.
    If total is zero, return equal distribution.
    """
    total = sum(weights)
    if total <= 0.0:
        n = len(weights)
        return [1.0 / n] * n if n > 0 else []
    return [w / total for w in weights]


def _top_timeframe(tf_list: list[str], alloc_weights: list[float]) -> Optional[str]:
    """Timeframe with highest allocation_weight. Tie-break: alphabetical."""
    if not tf_list:
        return None
    return min(
        tf_list,
        key=lambda tf: (-alloc_weights[tf_list.index(tf)], tf),
    )


# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

def build_snapshot(
    source: dict,
    ts_utc: Optional[str] = None,
) -> dict:
    """
    Build allocation summary from AC-133 regime-aware summary.
    Fixed mapping + normalization only. Input is not mutated.
    """
    tf_list    = list(source.get("timeframes") or [])
    per_tf     = dict(source.get("summary_per_timeframe") or {})
    ac133_sum  = dict(source.get("summary") or {})

    # base weights per timeframe
    base_weights = [
        _base_weight(
            (per_tf.get(tf) or {}).get("selected_strategy"),
            (per_tf.get(tf) or {}).get("regime"),
        )
        for tf in tf_list
    ]

    alloc_weights = _normalize(base_weights)

    # allocation_per_timeframe + allocation_detail
    allocation_per_tf: dict[str, dict] = {}
    allocation_detail: list[dict]      = []

    for i, tf in enumerate(tf_list):
        entry_src = per_tf.get(tf) or {}
        entry = {
            "selected_strategy": entry_src.get("selected_strategy"),
            "regime":            entry_src.get("regime", "unknown"),
            "base_weight":       round(base_weights[i], 6),
            "allocation_weight": round(alloc_weights[i], 6),
        }
        allocation_per_tf[tf] = entry
        allocation_detail.append({"timeframe": tf, **entry})

    weights_sum = round(sum(alloc_weights), 6) if alloc_weights else 0.0
    top_tf      = _top_timeframe(tf_list, alloc_weights)

    return {
        "version":                SNAPSHOT_VERSION,
        "ts_utc":                 ts_utc or _now_utc_iso(),
        "market":                 source.get("market", ""),
        "timeframes":             tf_list,
        "allocation_per_timeframe": allocation_per_tf,
        "allocation_detail":      allocation_detail,
        "allocation_summary": {
            "dominant_strategy": ac133_sum.get("dominant_strategy"),
            "dominant_regime":   ac133_sum.get("dominant_regime"),
            "top_timeframe":     top_tf,
            "weights_sum":       weights_sum,
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
    """End-to-end: load AC-133 snapshot → build allocation summary → write."""
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
        description="AC-134: Regime-aware allocation summary (research-only)."
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

    alloc = snapshot["allocation_per_timeframe"]
    w_parts = ", ".join(
        f"{tf}={alloc[tf]['allocation_weight']:.4f}"
        for tf in snapshot["timeframes"]
    )
    top = snapshot["allocation_summary"]["top_timeframe"] or "—"

    print()
    print("=== AC-134 REGIME-AWARE ALLOCATION SUMMARY ===")
    print(f"{'market':<10}: {snapshot['market']}")
    print(f"{'timeframes':<10}: {len(snapshot['timeframes'])}")
    print(f"{'weights':<10}: {w_parts}")
    print(f"{'top':<10}: {top}")
    print(f"{'file':<10}: {out_path}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
