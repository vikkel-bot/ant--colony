"""
AC-127: Multi-Market Strategy Comparison Runner (Research-Only)

Runs the AC-125 strategy comparison across multiple markets (and optionally
multiple timeframes) and bundles results into one compact overview.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No file output — this is a runner, not a snapshot writer.

Default markets : ["BTC-EUR", "ETH-EUR", "ADA-EUR"]
Default timeframe: "1h"

Fail-closed:
  - market with no data → included with empty strategies, top_strategy=null
  - individual market error → skipped silently, warning to stderr

Usage:
    python ant_colony/research/run_multi_market_comparison_lite.py
    python ant_colony/research/run_multi_market_comparison_lite.py \\
        --exchange bitvavo --markets BTC-EUR ETH-EUR --timeframe 4h
"""
from __future__ import annotations

import argparse
import sys
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

DEFAULT_DB_PATH   = _REPO_ROOT / "data" / "ohlcv" / "ohlcv.sqlite"
DEFAULT_EXCHANGE  = "bitvavo"
DEFAULT_MARKETS   = ["BTC-EUR", "ETH-EUR", "ADA-EUR"]
DEFAULT_TIMEFRAME = "1h"

FLAGS = {"research_only": True, "pipeline_impact": False}

# ---------------------------------------------------------------------------
# Per-market result builder
# ---------------------------------------------------------------------------

def _run_single_market(
    db_path:   Path,
    exchange:  str,
    market:    str,
    timeframe: str,
) -> dict:
    """
    Run AC-125 comparison for one market.
    Returns a market-result dict.
    On any error: returns fail-closed dict (empty strategies, top_strategy=null).
    """
    try:
        comp = run_comparison(
            db_path   = db_path,
            exchange  = exchange,
            market    = market,
            timeframe = timeframe,
        )
        strategies_raw = comp.get("strategies") or []
        ranking        = comp.get("ranking") or []
        top_strategy: Optional[str] = ranking[0] if ranking else None

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

        return {
            "market":       market,
            "top_strategy": top_strategy,
            "ranking":      ranking,
            "strategies":   strategies,
        }

    except Exception as exc:  # noqa: BLE001
        print(f"[AC-127] WARNING: market {market!r} skipped — {exc}", file=sys.stderr)
        return {
            "market":       market,
            "top_strategy": None,
            "ranking":      [],
            "strategies":   [],
        }


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------

def _build_frequency(market_results: list[dict]) -> dict[str, int]:
    """
    Count how often each strategy is #1 across all markets.
    Markets with top_strategy=None are excluded from the count.
    Returns a dict sorted descending by count, then alphabetically.
    """
    freq: dict[str, int] = {}
    for r in market_results:
        top = r.get("top_strategy")
        if top:
            freq[top] = freq.get(top, 0) + 1
    return dict(
        sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_multi_market_comparison(
    db_path:   Path         = DEFAULT_DB_PATH,
    exchange:  str          = DEFAULT_EXCHANGE,
    markets:   list[str]    = DEFAULT_MARKETS,
    timeframe: str          = DEFAULT_TIMEFRAME,
) -> dict:
    """
    Run AC-125 comparison for every market and bundle into one result.

    Returns:
    {
      "exchange":   str,
      "timeframe":  str,
      "markets":    [{ market, top_strategy, ranking, strategies }, ...],
      "summary":    { market_count, top_strategy_frequency },
      "flags":      { research_only, pipeline_impact },
    }
    """
    market_results = [
        _run_single_market(db_path, exchange, m, timeframe)
        for m in (markets or [])
    ]

    return {
        "exchange":  exchange,
        "timeframe": timeframe,
        "markets":   market_results,
        "summary": {
            "market_count":          len(market_results),
            "top_strategy_frequency": _build_frequency(market_results),
        },
        "flags": dict(FLAGS),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-127: Multi-market strategy comparison (research-only)."
    )
    p.add_argument("--exchange",  default=DEFAULT_EXCHANGE)
    p.add_argument("--markets",   nargs="+", default=DEFAULT_MARKETS)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--db",        default=str(DEFAULT_DB_PATH))
    return p


def main(argv: list[str] | None = None) -> int:
    args   = _build_parser().parse_args(argv)
    result = run_multi_market_comparison(
        db_path   = Path(args.db),
        exchange  = args.exchange,
        markets   = args.markets,
        timeframe = args.timeframe,
    )

    print()
    print("=== AC-127 MULTI-MARKET COMPARISON ===")
    print(f"tf : {result['timeframe']}")
    print()
    for mr in result["markets"]:
        top = mr["top_strategy"] or "—"
        print(f"  {mr['market']:<10}  →  {top}")
    print()
    print("Top frequency:")
    freq = result["summary"]["top_strategy_frequency"]
    if freq:
        for name, count in freq.items():
            print(f"  {name:<30}: {count}")
    else:
        print("  (no data)")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
