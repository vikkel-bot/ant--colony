"""
AC-129: Multi-Timeframe Strategy Comparison Runner (Research-Only)

Runs the AC-125 strategy comparison across multiple timeframes on the same
market and bundles results into one compact overview.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No file output — this is a runner, not a snapshot writer.

Default timeframes: ["1h", "4h", "1d"]
Default market    : "BTC-EUR"

Fail-closed:
  - timeframe with no data → included with empty strategies, top_strategy=null
  - individual timeframe error → skipped silently, warning to stderr

Usage:
    python ant_colony/research/run_multi_timeframe_comparison_lite.py
    python ant_colony/research/run_multi_timeframe_comparison_lite.py \\
        --exchange bitvavo --market ETH-EUR --timeframes 1h 4h
"""
from __future__ import annotations

import argparse
import sys
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
    from ant_colony.research.run_strategy_comparison_lite import run_comparison
except ImportError:
    from run_strategy_comparison_lite import run_comparison  # type: ignore[no-redef]

DEFAULT_DB_PATH    = _REPO_ROOT / "data" / "ohlcv" / "ohlcv.sqlite"
DEFAULT_EXCHANGE   = "bitvavo"
DEFAULT_MARKET     = "BTC-EUR"
DEFAULT_TIMEFRAMES = ["1h", "4h", "1d"]

FLAGS = {"research_only": True, "pipeline_impact": False}

# ---------------------------------------------------------------------------
# Per-timeframe result builder
# ---------------------------------------------------------------------------

def _run_single_timeframe(
    db_path:   Path,
    exchange:  str,
    market:    str,
    timeframe: str,
) -> dict:
    """
    Run AC-125 comparison for one timeframe.
    Returns a timeframe-result dict.
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
            "timeframe":    timeframe,
            "top_strategy": top_strategy,
            "ranking":      ranking,
            "strategies":   strategies,
        }

    except Exception as exc:  # noqa: BLE001
        print(
            f"[AC-129] WARNING: timeframe {timeframe!r} skipped — {exc}",
            file=sys.stderr,
        )
        return {
            "timeframe":    timeframe,
            "top_strategy": None,
            "ranking":      [],
            "strategies":   [],
        }


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------

def _build_frequency(timeframe_results: list[dict]) -> dict[str, int]:
    """
    Count how often each strategy is #1 across all timeframes.
    Timeframes with top_strategy=None are excluded.
    Returns dict sorted descending by count, then alphabetically.
    """
    freq: dict[str, int] = {}
    for r in timeframe_results:
        top = r.get("top_strategy")
        if top:
            freq[top] = freq.get(top, 0) + 1
    return dict(
        sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_multi_timeframe_comparison(
    db_path:    Path      = DEFAULT_DB_PATH,
    exchange:   str       = DEFAULT_EXCHANGE,
    market:     str       = DEFAULT_MARKET,
    timeframes: list[str] = DEFAULT_TIMEFRAMES,
) -> dict:
    """
    Run AC-125 comparison for every timeframe and bundle into one result.

    Returns:
    {
      "exchange":   str,
      "market":     str,
      "timeframes": [{ timeframe, top_strategy, ranking, strategies }, ...],
      "summary":    { timeframe_count, top_strategy_frequency },
      "flags":      { research_only, pipeline_impact },
    }
    """
    tf_results = [
        _run_single_timeframe(db_path, exchange, market, tf)
        for tf in (timeframes or [])
    ]

    return {
        "exchange":   exchange,
        "market":     market,
        "timeframes": tf_results,
        "summary": {
            "timeframe_count":        len(tf_results),
            "top_strategy_frequency": _build_frequency(tf_results),
        },
        "flags": dict(FLAGS),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-129: Multi-timeframe strategy comparison (research-only)."
    )
    p.add_argument("--exchange",   default=DEFAULT_EXCHANGE)
    p.add_argument("--market",     default=DEFAULT_MARKET)
    p.add_argument("--timeframes", nargs="+", default=DEFAULT_TIMEFRAMES)
    p.add_argument("--db",         default=str(DEFAULT_DB_PATH))
    return p


def main(argv: list[str] | None = None) -> int:
    args   = _build_parser().parse_args(argv)
    result = run_multi_timeframe_comparison(
        db_path    = Path(args.db),
        exchange   = args.exchange,
        market     = args.market,
        timeframes = args.timeframes,
    )

    print()
    print("=== AC-129 MULTI-TIMEFRAME COMPARISON ===")
    print(f"market : {result['market']}")
    print()
    for tr in result["timeframes"]:
        top = tr["top_strategy"] or "—"
        print(f"  {tr['timeframe']:<6}  →  {top}")
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
