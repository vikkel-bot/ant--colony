"""
AC-125: Multi-Strategy Comparison Layer (Research-Only, Standalone)

Runs three simple strategies on the same OHLCV dataset and ranks results.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.

Strategies:
  mean_reversion        — buy oversold; exit on RSI recovery
  trend_follow_lite     — buy when price above both moving averages; exit on EMA cross-under
  volatility_breakout_lite — buy on upper Bollinger break; exit on SMA cross-under

Shared simulation rules (all strategies):
  - One position at a time
  - No shorting
  - Full capital per trade (equity *= 1 + pnl)
  - Force-exit at end of data if still in position
  - equity_start = 1.0

Ranking (deterministic):
  1. highest total_return
  2. highest winrate       (tie-break)
  3. highest max_drawdown  (less negative = less damage, tie-break)
  4. alphabetical name     (tie-break)

Usage:
    python ant_colony/research/run_strategy_comparison_lite.py
    python ant_colony/research/run_strategy_comparison_lite.py \\
        --exchange bitvavo --market ETH-EUR --timeframe 4h
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable, Optional

# ---------------------------------------------------------------------------
# Paths and import resolution
# ---------------------------------------------------------------------------

_RESEARCH_DIR = Path(__file__).resolve().parent   # ant_colony/research/
_REPO_ROOT    = _RESEARCH_DIR.parent.parent       # repo root

for _p in (str(_REPO_ROOT), str(_RESEARCH_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from ant_colony.research.ta_indicators_lite import add_indicators
    from ant_colony.research.run_research_backtest_lite import load_ohlcv_rows
except ImportError:
    from ta_indicators_lite import add_indicators          # type: ignore[no-redef]
    from run_research_backtest_lite import load_ohlcv_rows # type: ignore[no-redef]

DEFAULT_DB_PATH   = _REPO_ROOT / "data" / "ohlcv" / "ohlcv.sqlite"
DEFAULT_EXCHANGE  = "bitvavo"
DEFAULT_MARKET    = "BTC-EUR"
DEFAULT_TIMEFRAME = "1h"

# ---------------------------------------------------------------------------
# Shared simulation helpers
# ---------------------------------------------------------------------------

def _max_drawdown(equity_curve: list[float]) -> float:
    """Peak-to-trough max drawdown as a negative fraction (or 0.0)."""
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    mdd  = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        if peak > 0.0:
            dd = (v - peak) / peak
            if dd < mdd:
                mdd = dd
    return mdd


def _simulate(
    rows:     list[dict],
    entry_fn: Callable[[dict], bool],
    exit_fn:  Callable[[dict], bool],
) -> dict:
    """
    Core simulation loop shared by all strategies.
    Input rows must already contain indicator fields from add_indicators().
    Input is not mutated.
    """
    if not rows:
        return {
            "trades": 0, "winrate": 0.0, "total_return": 0.0,
            "max_drawdown": 0.0, "equity_curve": [1.0],
        }

    equity: float            = 1.0
    curve:  list[float]      = [1.0]
    pnls:   list[float]      = []
    in_pos: bool             = False
    entry_price: Optional[float] = None

    for row in rows:
        close = row.get("close")
        if close is None:
            continue
        if in_pos:
            if exit_fn(row):
                pnl = (close - entry_price) / entry_price  # type: ignore[operator]
                equity *= 1.0 + pnl
                curve.append(round(equity, 10))
                pnls.append(pnl)
                in_pos      = False
                entry_price = None
        else:
            if entry_fn(row):
                in_pos      = True
                entry_price = close

    # Force-exit at end of data if still holding
    if in_pos and entry_price is not None:
        last_close = next(
            (r.get("close") for r in reversed(rows) if r.get("close") is not None),
            None,
        )
        if last_close is not None:
            pnl = (last_close - entry_price) / entry_price
            equity *= 1.0 + pnl
            curve.append(round(equity, 10))
            pnls.append(pnl)

    n    = len(pnls)
    wins = sum(1 for p in pnls if p > 0.0)
    return {
        "trades":       n,
        "winrate":      round(wins / n, 6) if n > 0 else 0.0,
        "total_return": round(equity - 1.0, 6),
        "max_drawdown": round(_max_drawdown(curve), 6),
        "equity_curve": curve,
    }


# ---------------------------------------------------------------------------
# Strategy predicates
# ---------------------------------------------------------------------------

def _entry_mean_reversion(row: dict) -> bool:
    rsi      = row.get("rsi_14")
    close    = row.get("close")
    bb_lower = row.get("bb_lower")
    return (rsi is not None and bb_lower is not None and close is not None
            and rsi < 30.0 and close < bb_lower)


def _exit_mean_reversion(row: dict) -> bool:
    rsi = row.get("rsi_14")
    return rsi is not None and rsi > 50.0


def _entry_trend_follow(row: dict) -> bool:
    close = row.get("close")
    sma20 = row.get("sma_20")
    ema20 = row.get("ema_20")
    return (close is not None and sma20 is not None and ema20 is not None
            and close > sma20 and close > ema20)


def _exit_trend_follow(row: dict) -> bool:
    close = row.get("close")
    ema20 = row.get("ema_20")
    return close is not None and ema20 is not None and close < ema20


def _entry_volatility_breakout(row: dict) -> bool:
    close    = row.get("close")
    bb_upper = row.get("bb_upper")
    return close is not None and bb_upper is not None and close > bb_upper


def _exit_volatility_breakout(row: dict) -> bool:
    close = row.get("close")
    sma20 = row.get("sma_20")
    return close is not None and sma20 is not None and close < sma20


# ---------------------------------------------------------------------------
# Strategy registry
# ---------------------------------------------------------------------------

STRATEGIES: dict[str, tuple[Callable, Callable]] = {
    "mean_reversion":          (_entry_mean_reversion,    _exit_mean_reversion),
    "trend_follow_lite":       (_entry_trend_follow,      _exit_trend_follow),
    "volatility_breakout_lite":(_entry_volatility_breakout, _exit_volatility_breakout),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_strategy_backtest(rows: list[dict], strategy_name: str) -> dict:
    """
    Run one named strategy on pre-enriched rows.
    Returns metrics dict with 'name' key added.
    Raises KeyError for unknown strategy_name.
    """
    entry_fn, exit_fn = STRATEGIES[strategy_name]
    result = _simulate(rows, entry_fn, exit_fn)
    result["name"] = strategy_name
    return result


def _rank(results: list[dict]) -> list[dict]:
    """
    Sort results: highest total_return, then winrate, then max_drawdown
    (less negative = better), then alphabetical name. Fully deterministic.
    """
    return sorted(
        results,
        key=lambda r: (
            -r["total_return"],
            -r["winrate"],
            -r["max_drawdown"],   # negate: less negative → smaller → ranked first
            r["name"],
        ),
    )


def run_comparison(
    db_path:   Path = DEFAULT_DB_PATH,
    exchange:  str  = DEFAULT_EXCHANGE,
    market:    str  = DEFAULT_MARKET,
    timeframe: str  = DEFAULT_TIMEFRAME,
) -> dict:
    """
    End-to-end comparison: load → enrich → run all strategies → rank.
    Returns comparison dict with 'strategies' (ranked) and 'ranking' list.
    """
    rows     = load_ohlcv_rows(db_path, exchange, market, timeframe)
    enriched = add_indicators(rows)

    results = [run_strategy_backtest(enriched, name) for name in STRATEGIES]
    ranked  = _rank(results)

    return {
        "exchange":    exchange,
        "market":      market,
        "timeframe":   timeframe,
        "rows_loaded": len(rows),
        "strategies":  ranked,
        "ranking":     [r["name"] for r in ranked],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-125: Multi-strategy comparison (research-only)."
    )
    p.add_argument("--exchange",  default=DEFAULT_EXCHANGE)
    p.add_argument("--market",    default=DEFAULT_MARKET)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--db",        default=str(DEFAULT_DB_PATH))
    return p


def main(argv: list[str] | None = None) -> int:
    args   = _build_parser().parse_args(argv)
    result = run_comparison(
        db_path   = Path(args.db),
        exchange  = args.exchange,
        market    = args.market,
        timeframe = args.timeframe,
    )

    print()
    print("=== AC-125 STRATEGY COMPARISON ===")
    print(f"{'market':<10}: {result['market']}")
    print(f"{'tf':<10}: {result['timeframe']}")
    print(f"{'rows':<10}: {result['rows_loaded']}")
    print()
    for i, s in enumerate(result["strategies"], 1):
        ret = s["total_return"]
        print(
            f"  {i}. {s['name']:<28}"
            f"  return={ret:+.4f}"
            f"  winrate={s['winrate']:.2f}"
            f"  trades={s['trades']:3d}"
            f"  max_dd={s['max_drawdown']:.4f}"
        )
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
