"""
AC-128: Multi-Market Comparison Snapshot (Research-Only)

Builds a compact, deterministic JSON snapshot from AC-127 multi-market
comparison results.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.

Output:
  data/research/multi_market_comparison_snapshot.json

Fail-closed:
  - empty or missing market results → snapshot written with empty markets
  - output directory created automatically if absent
  - no crash on bad input

Usage:
    python ant_colony/research/build_multi_market_comparison_snapshot_lite.py
    python ant_colony/research/build_multi_market_comparison_snapshot_lite.py \\
        --exchange bitvavo --markets BTC-EUR ETH-EUR ADA-EUR --timeframe 4h
    python ant_colony/research/build_multi_market_comparison_snapshot_lite.py \\
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

_RESEARCH_DIR = Path(__file__).resolve().parent   # ant_colony/research/
_REPO_ROOT    = _RESEARCH_DIR.parent.parent       # repo root

for _p in (str(_REPO_ROOT), str(_RESEARCH_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from ant_colony.research.run_multi_market_comparison_lite import (
        run_multi_market_comparison,
        DEFAULT_MARKETS,
        DEFAULT_EXCHANGE,
        DEFAULT_TIMEFRAME,
    )
except ImportError:
    from run_multi_market_comparison_lite import (  # type: ignore[no-redef]
        run_multi_market_comparison,
        DEFAULT_MARKETS,
        DEFAULT_EXCHANGE,
        DEFAULT_TIMEFRAME,
    )

DEFAULT_DB_PATH       = _REPO_ROOT / "data" / "ohlcv" / "ohlcv.sqlite"
DEFAULT_SNAPSHOT_PATH = _REPO_ROOT / "data" / "research" / "multi_market_comparison_snapshot.json"

SNAPSHOT_VERSION = "multi_market_comparison_snapshot_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

def build_snapshot(
    comparison: dict,
    ts_utc: Optional[str] = None,
) -> dict:
    """
    Build a deterministic snapshot dict from a run_multi_market_comparison() result.

    Handles empty or malformed input gracefully (fail-closed).
    Input is not mutated.
    equity_curve is stripped from strategy entries.
    """
    markets_raw = comparison.get("markets") or []
    summary_raw = comparison.get("summary") or {}

    markets = []
    for mr in markets_raw:
        if not isinstance(mr, dict):
            continue
        strategies_raw = mr.get("strategies") or []
        strategies = [
            {
                "name":         s.get("name", ""),
                "trades":       s.get("trades", 0),
                "winrate":      s.get("winrate", 0.0),
                "total_return": s.get("total_return", 0.0),
                "max_drawdown": s.get("max_drawdown", 0.0),
            }
            for s in strategies_raw
            if isinstance(s, dict)
        ]
        ranking     = [str(n) for n in (mr.get("ranking") or []) if n is not None]
        top: Optional[str] = ranking[0] if ranking else None
        markets.append({
            "market":       mr.get("market", ""),
            "top_strategy": top,
            "ranking":      ranking,
            "strategies":   strategies,
        })

    freq = dict(summary_raw.get("top_strategy_frequency") or {})

    return {
        "version":   SNAPSHOT_VERSION,
        "ts_utc":    ts_utc or _now_utc_iso(),
        "exchange":  comparison.get("exchange", ""),
        "timeframe": comparison.get("timeframe", ""),
        "markets":   markets,
        "summary": {
            "market_count":           len(markets),
            "top_strategy_frequency": freq,
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
    db_path:   Path         = DEFAULT_DB_PATH,
    exchange:  str          = DEFAULT_EXCHANGE,
    markets:   list[str]    = DEFAULT_MARKETS,
    timeframe: str          = DEFAULT_TIMEFRAME,
    out_path:  Path         = DEFAULT_SNAPSHOT_PATH,
    ts_utc:    Optional[str] = None,
) -> dict:
    """
    End-to-end: run AC-127 comparison → build snapshot → write to disk.
    Returns the snapshot dict.
    """
    comparison = run_multi_market_comparison(
        db_path   = db_path,
        exchange  = exchange,
        markets   = markets,
        timeframe = timeframe,
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
        description="AC-128: Multi-market comparison snapshot (research-only)."
    )
    p.add_argument("--exchange",  default=DEFAULT_EXCHANGE)
    p.add_argument("--markets",   nargs="+", default=DEFAULT_MARKETS)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--db",        default=str(DEFAULT_DB_PATH))
    p.add_argument("--out",       default=str(DEFAULT_SNAPSHOT_PATH))
    return p


def main(argv: list[str] | None = None) -> int:
    args     = _build_parser().parse_args(argv)
    out_path = Path(args.out)

    snapshot = build_and_write_snapshot(
        db_path   = Path(args.db),
        exchange  = args.exchange,
        markets   = args.markets,
        timeframe = args.timeframe,
        out_path  = out_path,
    )

    print()
    print("=== AC-128 MULTI-MARKET COMPARISON SNAPSHOT ===")
    print(f"{'timeframe':<12}: {snapshot['timeframe']}")
    print(f"{'markets':<12}: {snapshot['summary']['market_count']}")
    for mr in snapshot["markets"]:
        top = mr["top_strategy"] or "—"
        print(f"  {mr['market']:<12}  →  {top}")
    print(f"{'written to':<12}: {out_path}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
