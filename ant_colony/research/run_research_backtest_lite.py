"""
AC-124: Research Backtest Hook (OHLCV + TA, standalone)

Reads OHLCV candles from the AC-122 SQLite database, applies AC-123 TA
indicators, runs a simple mean-reversion simulation, and returns metrics.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.

Strategy (fixed):
  ENTER LONG : rsi_14 < 30  AND  close < bb_lower
  EXIT  LONG : rsi_14 > 50
  One position at a time. No shorting. Full capital per trade.

Simulation:
  equity_start = 1.0
  pnl_frac     = (exit_price - entry_price) / entry_price
  equity      *= (1 + pnl_frac)

Metrics returned:
  {
    "trades"       : int,
    "winrate"      : float,   # [0, 1], 0.0 if no trades
    "total_return" : float,   # equity_final - 1.0
    "max_drawdown" : float,   # <= 0.0
    "equity_curve" : list[float],  # len = trades + 1
  }

Usage (CLI):
    python ant_colony/research/run_research_backtest_lite.py
    python ant_colony/research/run_research_backtest_lite.py \\
        --exchange bitvavo --market ETH-EUR --timeframe 4h
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths and import resolution
# ---------------------------------------------------------------------------

_RESEARCH_DIR = Path(__file__).resolve().parent          # ant_colony/research/
_ANT_DIR      = _RESEARCH_DIR.parent                     # ant_colony/
_REPO_ROOT    = _ANT_DIR.parent                          # repo root

# Allow both: running as standalone script and importing from repo root
for _p in (str(_REPO_ROOT), str(_RESEARCH_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from ant_colony.research.ta_indicators_lite import add_indicators
except ImportError:
    from ta_indicators_lite import add_indicators  # type: ignore[no-redef]

DEFAULT_DB_PATH = _REPO_ROOT / "data" / "ohlcv" / "ohlcv.sqlite"
DEFAULT_EXCHANGE  = "bitvavo"
DEFAULT_MARKET    = "BTC-EUR"
DEFAULT_TIMEFRAME = "1h"

# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

def load_ohlcv_rows(
    db_path: Path,
    exchange: str,
    market: str,
    timeframe: str,
) -> list[dict]:
    """
    Load OHLCV rows from SQLite, sorted ascending by ts_utc.
    Returns [] if the database does not exist or an error occurs.
    No crash on missing file or bad schema.
    """
    if not Path(db_path).exists():
        return []
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute(
            "SELECT ts_utc, open, high, low, close, volume FROM ohlcv "
            "WHERE exchange = ? AND market = ? AND timeframe = ? "
            "ORDER BY ts_utc ASC",
            (exchange, market, timeframe),
        )
        rows = [
            {
                "ts_utc":   r[0],
                "open":     r[1],
                "high":     r[2],
                "low":      r[3],
                "close":    r[4],
                "volume":   r[5],
            }
            for r in cursor.fetchall()
        ]
        conn.close()
        return rows
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

def _max_drawdown(equity_curve: list[float]) -> float:
    """
    Peak-to-trough max drawdown as a negative fraction (or 0.0).
    e.g. 12% drawdown → -0.12
    """
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    mdd  = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        if peak > 0.0:
            dd = (v - peak) / peak   # <= 0
            if dd < mdd:
                mdd = dd
    return mdd


def run_mean_reversion_backtest(enriched_rows: list[dict]) -> dict:
    """
    Simulate mean-reversion strategy on pre-enriched OHLCV rows.
    Rows must already contain indicator fields from add_indicators().

    Strategy:
      ENTER LONG : rsi_14 < 30  AND  close < bb_lower
      EXIT  LONG : rsi_14 > 50
      Force-exit at end of data if still in position.

    Returns metrics dict (see module docstring).
    Input rows are not mutated.
    """
    if not enriched_rows:
        return {
            "trades":       0,
            "winrate":      0.0,
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "equity_curve": [1.0],
        }

    equity:       float        = 1.0
    equity_curve: list[float]  = [1.0]
    trade_pnls:   list[float]  = []

    in_position:  bool          = False
    entry_price:  Optional[float] = None

    for row in enriched_rows:
        rsi      = row.get("rsi_14")
        close    = row.get("close")
        bb_lower = row.get("bb_lower")

        if close is None:
            continue

        if in_position:
            # Exit condition
            if rsi is not None and rsi > 50.0:
                pnl = (close - entry_price) / entry_price  # type: ignore[operator]
                equity *= (1.0 + pnl)
                equity_curve.append(round(equity, 10))
                trade_pnls.append(pnl)
                in_position  = False
                entry_price  = None
        else:
            # Entry condition
            if (rsi is not None and bb_lower is not None
                    and rsi < 30.0 and close < bb_lower):
                in_position = True
                entry_price = close

    # Force-exit at end of data if still holding
    if in_position and entry_price is not None:
        last_close = enriched_rows[-1].get("close")
        if last_close is not None:
            pnl = (last_close - entry_price) / entry_price
            equity *= (1.0 + pnl)
            equity_curve.append(round(equity, 10))
            trade_pnls.append(pnl)

    n_trades = len(trade_pnls)
    wins     = sum(1 for p in trade_pnls if p > 0.0)
    winrate  = wins / n_trades if n_trades > 0 else 0.0
    mdd      = _max_drawdown(equity_curve)

    return {
        "trades":       n_trades,
        "winrate":      round(winrate, 6),
        "total_return": round(equity - 1.0, 6),
        "max_drawdown": round(mdd, 6),
        "equity_curve": equity_curve,
    }


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_backtest(
    db_path:   Path  = DEFAULT_DB_PATH,
    exchange:  str   = DEFAULT_EXCHANGE,
    market:    str   = DEFAULT_MARKET,
    timeframe: str   = DEFAULT_TIMEFRAME,
) -> dict:
    """
    End-to-end: load → enrich with indicators → simulate → return metrics.
    Also injects context keys: exchange, market, timeframe, rows_loaded.
    """
    rows = load_ohlcv_rows(db_path, exchange, market, timeframe)
    enriched = add_indicators(rows)
    result = run_mean_reversion_backtest(enriched)
    result["exchange"]    = exchange
    result["market"]      = market
    result["timeframe"]   = timeframe
    result["rows_loaded"] = len(rows)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-124: Research backtest (OHLCV + TA, standalone)."
    )
    p.add_argument("--exchange",  default=DEFAULT_EXCHANGE)
    p.add_argument("--market",    default=DEFAULT_MARKET)
    p.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    p.add_argument("--db",        default=str(DEFAULT_DB_PATH))
    return p


def _fmt(label: str, value) -> str:
    return f"{label:<10}: {value}"


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = run_backtest(
        db_path   = Path(args.db),
        exchange  = args.exchange,
        market    = args.market,
        timeframe = args.timeframe,
    )

    ret = result["total_return"]
    mdd = result["max_drawdown"]

    print()
    print("=== AC-124 RESEARCH BACKTEST ===")
    print(_fmt("market",   result["market"]))
    print(_fmt("tf",       result["timeframe"]))
    print(_fmt("rows",     result["rows_loaded"]))
    print(_fmt("trades",   result["trades"]))
    print(_fmt("winrate",  f"{result['winrate']:.2f}"))
    print(_fmt("return",   f"{ret:+.4f}"))
    print(_fmt("max_dd",   f"{mdd:.4f}"))
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
