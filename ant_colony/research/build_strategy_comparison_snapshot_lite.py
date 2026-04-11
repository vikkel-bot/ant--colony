"""
AC-126: Strategy Comparison Snapshot (Research Review Output)

Builds a compact, deterministic JSON snapshot from AC-125 comparison results.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.

Output:
  data/research/strategy_comparison_snapshot.json

Fail-closed:
  - empty or missing results → snapshot written with empty ranking, top_strategy=null
  - output directory created automatically if absent
  - no crash on bad input

Usage:
    python ant_colony/research/build_strategy_comparison_snapshot_lite.py
    python ant_colony/research/build_strategy_comparison_snapshot_lite.py \\
        --exchange bitvavo --market ETH-EUR --timeframe 4h
    python ant_colony/research/build_strategy_comparison_snapshot_lite.py \\
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
    from ant_colony.research.run_strategy_comparison_lite import run_comparison
except ImportError:
    from run_strategy_comparison_lite import run_comparison  # type: ignore[no-redef]

DEFAULT_DB_PATH      = _REPO_ROOT / "data" / "ohlcv" / "ohlcv.sqlite"
DEFAULT_SNAPSHOT_PATH = _REPO_ROOT / "data" / "research" / "strategy_comparison_snapshot.json"
DEFAULT_EXCHANGE      = "bitvavo"
DEFAULT_MARKET        = "BTC-EUR"
DEFAULT_TIMEFRAME     = "1h"

SNAPSHOT_VERSION = "strategy_comparison_snapshot_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

def build_snapshot(
    comparison: dict,
    ts_utc: Optional[str] = None,
) -> dict:
    """
    Build a deterministic snapshot dict from a run_comparison() result.

    Handles empty or malformed comparison gracefully (fail-closed).
    Input is not mutated.

    Returns a snapshot dict matching the AC-126 schema.
    """
    strategies_raw = comparison.get("strategies") or []
    ranking_raw    = comparison.get("ranking") or []

    # Extract clean strategy metrics (only the defined keys, no equity_curve)
    strategies = []
    for s in strategies_raw:
        if not isinstance(s, dict):
            continue
        strategies.append({
            "name":         s.get("name", ""),
            "trades":       s.get("trades", 0),
            "winrate":      s.get("winrate", 0.0),
            "total_return": s.get("total_return", 0.0),
            "max_drawdown": s.get("max_drawdown", 0.0),
        })

    ranking    = [str(n) for n in ranking_raw if n is not None]
    top_strategy: Optional[str] = ranking[0] if ranking else None

    return {
        "version":      SNAPSHOT_VERSION,
        "ts_utc":       ts_utc or _now_utc_iso(),
        "exchange":     comparison.get("exchange", ""),
        "market":       comparison.get("market", ""),
        "timeframe":    comparison.get("timeframe", ""),
        "top_strategy": top_strategy,
        "ranking":      ranking,
        "strategies":   strategies,
        "summary": {
            "strategy_count": len(strategies),
            "ranked_by":      "total_return_winrate_drawdown_name",
        },
        "flags": dict(FLAGS),
    }


def write_snapshot(snapshot: dict, out_path: Path) -> None:
    """
    Write snapshot dict as pretty-printed JSON to out_path.
    Creates parent directories if they do not exist.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
        f.write("\n")


def build_and_write_snapshot(
    db_path:       Path = DEFAULT_DB_PATH,
    exchange:      str  = DEFAULT_EXCHANGE,
    market:        str  = DEFAULT_MARKET,
    timeframe:     str  = DEFAULT_TIMEFRAME,
    out_path:      Path = DEFAULT_SNAPSHOT_PATH,
    ts_utc:        Optional[str] = None,
) -> dict:
    """
    End-to-end: run AC-125 comparison → build snapshot → write to disk.
    Returns the snapshot dict.
    """
    comparison = run_comparison(
        db_path   = db_path,
        exchange  = exchange,
        market    = market,
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
        description="AC-126: Strategy comparison snapshot (research-only)."
    )
    p.add_argument("--exchange",  default=DEFAULT_EXCHANGE)
    p.add_argument("--market",    default=DEFAULT_MARKET)
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
        market    = args.market,
        timeframe = args.timeframe,
        out_path  = out_path,
    )

    print()
    print("=== AC-126 STRATEGY COMPARISON SNAPSHOT ===")
    print(f"{'market':<12}: {snapshot['market']}")
    print(f"{'tf':<12}: {snapshot['timeframe']}")
    print(f"{'top_strategy':<12}: {snapshot['top_strategy']}")
    print(f"{'ranking':<12}: {snapshot['ranking']}")
    print(f"{'written to':<12}: {out_path}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
