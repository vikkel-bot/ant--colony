"""
AC-130: Multi-Timeframe Comparison Snapshot (Research-Only)

Writes AC-129 multi-timeframe comparison output to a compact JSON snapshot.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No new ranking logic — direct passthrough from AC-129.

Output:
  data/research/multi_timeframe_comparison_snapshot.json

Usage:
    python ant_colony/research/build_multi_timeframe_comparison_snapshot_lite.py
    python ant_colony/research/build_multi_timeframe_comparison_snapshot_lite.py \\
        --market ETH-EUR --timeframes 1h 4h
    python ant_colony/research/build_multi_timeframe_comparison_snapshot_lite.py \\
        --out data/research/my_snapshot.json
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths and import resolution
# ---------------------------------------------------------------------------

_RESEARCH_DIR = Path(__file__).resolve().parent
_REPO_ROOT    = _RESEARCH_DIR.parent.parent

for _p in (str(_REPO_ROOT), str(_RESEARCH_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from ant_colony.research.run_multi_timeframe_comparison_lite import (
        run_multi_timeframe_comparison,
        DEFAULT_EXCHANGE,
        DEFAULT_MARKET,
        DEFAULT_TIMEFRAMES,
    )
except ImportError:
    from run_multi_timeframe_comparison_lite import (  # type: ignore[no-redef]
        run_multi_timeframe_comparison,
        DEFAULT_EXCHANGE,
        DEFAULT_MARKET,
        DEFAULT_TIMEFRAMES,
    )

DEFAULT_DB_PATH       = _REPO_ROOT / "data" / "ohlcv" / "ohlcv.sqlite"
DEFAULT_SNAPSHOT_PATH = _REPO_ROOT / "data" / "research" / \
                        "multi_timeframe_comparison_snapshot.json"

SNAPSHOT_VERSION = "multi_timeframe_comparison_snapshot_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

def build_snapshot(
    comparison: dict,
    ts_utc: Optional[str] = None,
) -> dict:
    """
    Build a deterministic snapshot dict from a run_multi_timeframe_comparison()
    result. No new calculations — direct passthrough from AC-129.

    Input is not mutated.
    """
    tf_results = comparison.get("timeframes") or []
    summary    = comparison.get("summary") or {}

    # top_per_timeframe: { "1h": "strategy_name_or_null", ... }
    top_per_timeframe: dict[str, Optional[str]] = {
        tr["timeframe"]: tr.get("top_strategy")
        for tr in tf_results
        if isinstance(tr, dict) and "timeframe" in tr
    }

    # timeframes_detail: one entry per timeframe with top strategy metrics
    timeframes_detail = []
    for tr in tf_results:
        if not isinstance(tr, dict):
            continue
        top_name = tr.get("top_strategy")
        # Find the top strategy's metrics from the strategies list
        strategies = tr.get("strategies") or []
        top_metrics: dict = {}
        if top_name:
            top_metrics = next(
                (s for s in strategies if isinstance(s, dict)
                 and s.get("name") == top_name),
                {},
            )
        timeframes_detail.append({
            "timeframe":    tr.get("timeframe", ""),
            "top_strategy": top_name,
            "trades":       top_metrics.get("trades", 0),
            "winrate":      top_metrics.get("winrate", 0.0),
            "total_return": top_metrics.get("total_return", 0.0),
            "max_drawdown": top_metrics.get("max_drawdown", 0.0),
        })

    frequency = dict(summary.get("top_strategy_frequency") or {})
    tf_names  = [tr["timeframe"] for tr in tf_results
                 if isinstance(tr, dict) and "timeframe" in tr]

    return {
        "version":            SNAPSHOT_VERSION,
        "ts_utc":             ts_utc or _now_utc_iso(),
        "market":             comparison.get("market", ""),
        "timeframes":         tf_names,
        "top_per_timeframe":  top_per_timeframe,
        "frequency":          frequency,
        "timeframes_detail":  timeframes_detail,
        "flags":              dict(FLAGS),
    }


def write_snapshot(snapshot: dict, out_path: Path) -> None:
    """Write snapshot as pretty-printed JSON. Creates parent dirs if absent."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
        f.write("\n")


def build_and_write_snapshot(
    db_path:    Path         = DEFAULT_DB_PATH,
    exchange:   str          = DEFAULT_EXCHANGE,
    market:     str          = DEFAULT_MARKET,
    timeframes: list[str]    = DEFAULT_TIMEFRAMES,
    out_path:   Path         = DEFAULT_SNAPSHOT_PATH,
    ts_utc:     Optional[str] = None,
) -> dict:
    """End-to-end: run AC-129 → build snapshot → write to disk. Returns snapshot."""
    comparison = run_multi_timeframe_comparison(
        db_path    = db_path,
        exchange   = exchange,
        market     = market,
        timeframes = timeframes,
    )
    snapshot = build_snapshot(comparison, ts_utc=ts_utc)
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
        description="AC-130: Multi-timeframe comparison snapshot (research-only)."
    )
    p.add_argument("--exchange",   default=DEFAULT_EXCHANGE)
    p.add_argument("--market",     default=DEFAULT_MARKET)
    p.add_argument("--timeframes", nargs="+", default=DEFAULT_TIMEFRAMES)
    p.add_argument("--db",         default=str(DEFAULT_DB_PATH))
    p.add_argument("--out",        default=str(DEFAULT_SNAPSHOT_PATH))
    return p


def main(argv: list[str] | None = None) -> int:
    args     = _build_parser().parse_args(argv)
    out_path = Path(args.out)

    snapshot = build_and_write_snapshot(
        db_path    = Path(args.db),
        exchange   = args.exchange,
        market     = args.market,
        timeframes = args.timeframes,
        out_path   = out_path,
    )

    top_parts = ", ".join(
        f"{tf}={snapshot['top_per_timeframe'].get(tf) or '—'}"
        for tf in snapshot["timeframes"]
    )
    print()
    print("=== AC-130 MULTI-TIMEFRAME SNAPSHOT ===")
    print(f"{'market':<10}: {snapshot['market']}")
    print(f"{'timeframes':<10}: {len(snapshot['timeframes'])}")
    print(f"{'top':<10}: {top_parts}")
    print(f"{'file':<10}: {out_path}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
