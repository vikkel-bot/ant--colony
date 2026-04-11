"""
AC-124 tests — Research Backtest Hook (OHLCV + TA, standalone)

Coverage:
  1.  no data (empty rows) → no crash, returns 0-trade metrics
  2.  small dataset → end-to-end without crash
  3.  trades > 0 on clearly signalling dataset
  4.  equity curve length == trades + 1
  5.  winrate always in [0, 1]
  6.  max_drawdown <= 0
  7.  input rows not mutated
  8.  deterministic: same input → same output
  9.  winning trade → equity > 1.0
 10.  losing trade → equity < 1.0
 11.  load_ohlcv_rows returns [] for non-existent DB
 12.  load_ohlcv_rows returns [] for corrupt DB
 13.  run_backtest orchestrator returns all required keys
 14.  equity_curve starts at 1.0
 15.  force-exit at end of data if still in position
 16.  max_drawdown == 0 when no losing trades
 17.  max_drawdown < 0 when a losing trade occurs
 18.  no-trade dataset returns total_return == 0
"""
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony" / "research"))

from run_research_backtest_lite import (
    _max_drawdown,
    load_ohlcv_rows,
    run_backtest,
    run_mean_reversion_backtest,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row(ts: int, close: float, rsi: float | None, bb_lower: float | None,
         open_: float | None = None, high: float | None = None,
         low: float | None = None) -> dict:
    """Build a pre-enriched row (indicators already set)."""
    c = close
    return {
        "ts_utc":   ts,
        "open":     open_ if open_ is not None else c - 0.1,
        "high":     high  if high  is not None else c + 0.5,
        "low":      low   if low   is not None else c - 0.5,
        "close":    c,
        "volume":   10.0,
        "rsi_14":   rsi,
        "sma_20":   c,
        "ema_20":   c,
        "atr_14":   0.5,
        "bb_upper": c + 2.0,
        "bb_lower": bb_lower,
    }


def _neutral_rows(n: int, base: float = 100.0) -> list[dict]:
    """Rows with RSI = 50 — never trigger entry or exit."""
    return [_row(i, base, rsi=50.0, bb_lower=base - 3.0) for i in range(n)]


def _signal_dataset() -> list[dict]:
    """
    Dataset with one clear entry + exit:
      Row 0-9  : neutral (rsi=50, close=100, bb_lower=97)
      Row 10   : entry signal (rsi=25 < 30, close=96 < bb_lower=97)
      Row 11-13: in position, rsi=45 (no exit yet)
      Row 14   : exit signal (rsi=60 > 50, close=103)
      Row 15-19: neutral
    """
    rows = []
    for i in range(10):
        rows.append(_row(i, 100.0, rsi=50.0, bb_lower=97.0))
    # Entry
    rows.append(_row(10, 96.0, rsi=25.0, bb_lower=97.0))
    # In position
    for i in range(11, 14):
        rows.append(_row(i, 98.0, rsi=45.0, bb_lower=97.0))
    # Exit
    rows.append(_row(14, 103.0, rsi=60.0, bb_lower=97.0))
    # Post-exit neutral
    for i in range(15, 20):
        rows.append(_row(i, 103.0, rsi=55.0, bb_lower=97.0))
    return rows


def _losing_signal_dataset() -> list[dict]:
    """Entry followed by losing exit."""
    rows = []
    for i in range(5):
        rows.append(_row(i, 100.0, rsi=50.0, bb_lower=97.0))
    # Entry at 96
    rows.append(_row(5, 96.0, rsi=25.0, bb_lower=97.0))
    # In position
    for i in range(6, 9):
        rows.append(_row(i, 94.0, rsi=45.0, bb_lower=97.0))
    # Exit at 90 (loss)
    rows.append(_row(9, 90.0, rsi=60.0, bb_lower=97.0))
    return rows


def _write_db(tmp_path: Path, rows: list[dict]) -> Path:
    """Write OHLCV rows to a temporary SQLite database."""
    db = tmp_path / "test.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE ohlcv (
            exchange TEXT, market TEXT, timeframe TEXT,
            ts_utc INTEGER, open REAL, high REAL, low REAL,
            close REAL, volume REAL,
            PRIMARY KEY (exchange, market, timeframe, ts_utc)
        )
    """)
    conn.executemany(
        "INSERT INTO ohlcv VALUES (?,?,?,?,?,?,?,?,?)",
        [("bitvavo", "BTC-EUR", "1h",
          r["ts_utc"], r["open"], r["high"], r["low"], r["close"], r["volume"])
         for r in rows],
    )
    conn.commit()
    conn.close()
    return db


# ---------------------------------------------------------------------------
# 1. No data
# ---------------------------------------------------------------------------

class TestNoData:
    def test_empty_list_no_crash(self):
        result = run_mean_reversion_backtest([])
        assert result["trades"] == 0

    def test_empty_list_returns_zero_metrics(self):
        result = run_mean_reversion_backtest([])
        assert result["winrate"]      == 0.0
        assert result["total_return"] == 0.0
        assert result["max_drawdown"] == 0.0

    def test_empty_list_equity_curve_starts_at_one(self):
        result = run_mean_reversion_backtest([])
        assert result["equity_curve"] == [1.0]

    def test_neutral_rows_no_trades(self):
        rows = _neutral_rows(50)
        result = run_mean_reversion_backtest(rows)
        assert result["trades"] == 0


# ---------------------------------------------------------------------------
# 2. Small dataset end-to-end (no crash)
# ---------------------------------------------------------------------------

class TestSmallDataset:
    def test_five_rows_no_crash(self):
        rows = _neutral_rows(5)
        run_mean_reversion_backtest(rows)

    def test_one_row_no_crash(self):
        run_mean_reversion_backtest([_row(0, 100.0, rsi=50.0, bb_lower=97.0)])

    def test_all_none_indicators_no_crash(self):
        rows = [{"ts_utc": i, "close": 100.0, "rsi_14": None, "bb_lower": None}
                for i in range(10)]
        result = run_mean_reversion_backtest(rows)
        assert result["trades"] == 0


# ---------------------------------------------------------------------------
# 3. Trades > 0 on signal dataset
# ---------------------------------------------------------------------------

class TestSignalDataset:
    def test_at_least_one_trade(self):
        result = run_mean_reversion_backtest(_signal_dataset())
        assert result["trades"] >= 1

    def test_winning_trade_positive_return(self):
        result = run_mean_reversion_backtest(_signal_dataset())
        assert result["total_return"] > 0.0

    def test_winning_trade_equity_above_one(self):
        result = run_mean_reversion_backtest(_signal_dataset())
        assert result["equity_curve"][-1] > 1.0

    def test_entry_at_signal_price(self):
        """Entry at close=96, exit at close=103 → pnl ≈ 7.29%."""
        result = run_mean_reversion_backtest(_signal_dataset())
        expected_pnl = (103.0 - 96.0) / 96.0
        assert abs(result["total_return"] - expected_pnl) < 1e-6


# ---------------------------------------------------------------------------
# 4. Equity curve length
# ---------------------------------------------------------------------------

class TestEquityCurve:
    def test_length_equals_trades_plus_one(self):
        result = run_mean_reversion_backtest(_signal_dataset())
        assert len(result["equity_curve"]) == result["trades"] + 1

    def test_length_one_when_no_trades(self):
        result = run_mean_reversion_backtest(_neutral_rows(30))
        assert len(result["equity_curve"]) == 1

    def test_starts_at_one(self):
        result = run_mean_reversion_backtest(_signal_dataset())
        assert result["equity_curve"][0] == pytest.approx(1.0)

    def test_multiple_trades_correct_length(self):
        rows = []
        # Two separate entry/exit cycles
        for cycle in range(2):
            base = cycle * 30
            rows += _neutral_rows(5, 100.0)
            rows.append(_row(base + 5, 96.0, rsi=25.0, bb_lower=97.0))
            for k in range(3):
                rows.append(_row(base + 6 + k, 98.0, rsi=45.0, bb_lower=97.0))
            rows.append(_row(base + 9, 103.0, rsi=60.0, bb_lower=97.0))
        result = run_mean_reversion_backtest(rows)
        assert len(result["equity_curve"]) == result["trades"] + 1


# ---------------------------------------------------------------------------
# 5. Winrate in [0, 1]
# ---------------------------------------------------------------------------

class TestWinrate:
    def test_winrate_zero_when_no_trades(self):
        result = run_mean_reversion_backtest(_neutral_rows(30))
        assert result["winrate"] == 0.0

    def test_winrate_one_on_all_winning(self):
        result = run_mean_reversion_backtest(_signal_dataset())
        # Single winning trade → winrate = 1.0
        assert result["winrate"] == pytest.approx(1.0)

    def test_winrate_zero_on_all_losing(self):
        result = run_mean_reversion_backtest(_losing_signal_dataset())
        assert result["winrate"] == pytest.approx(0.0)

    def test_winrate_in_range(self):
        for rows in [_neutral_rows(30), _signal_dataset(), _losing_signal_dataset()]:
            r = run_mean_reversion_backtest(rows)
            assert 0.0 <= r["winrate"] <= 1.0


# ---------------------------------------------------------------------------
# 6. Max drawdown <= 0
# ---------------------------------------------------------------------------

class TestMaxDrawdown:
    def test_max_drawdown_lte_zero_neutral(self):
        result = run_mean_reversion_backtest(_neutral_rows(30))
        assert result["max_drawdown"] <= 0.0

    def test_max_drawdown_lte_zero_winning(self):
        result = run_mean_reversion_backtest(_signal_dataset())
        assert result["max_drawdown"] <= 0.0

    def test_max_drawdown_lte_zero_losing(self):
        result = run_mean_reversion_backtest(_losing_signal_dataset())
        assert result["max_drawdown"] <= 0.0

    def test_max_drawdown_zero_no_trades(self):
        result = run_mean_reversion_backtest(_neutral_rows(30))
        assert result["max_drawdown"] == 0.0

    def test_max_drawdown_zero_only_wins(self):
        result = run_mean_reversion_backtest(_signal_dataset())
        assert result["max_drawdown"] == pytest.approx(0.0)

    def test_max_drawdown_negative_after_loss(self):
        result = run_mean_reversion_backtest(_losing_signal_dataset())
        assert result["max_drawdown"] < 0.0

    def test_max_drawdown_helper_basic(self):
        curve = [1.0, 1.1, 0.9, 1.0]
        mdd = _max_drawdown(curve)
        # peak = 1.1, trough = 0.9 → dd = (0.9-1.1)/1.1 ≈ -0.1818
        assert mdd < 0.0
        assert mdd == pytest.approx((0.9 - 1.1) / 1.1, abs=1e-6)

    def test_max_drawdown_helper_no_drawdown(self):
        assert _max_drawdown([1.0, 1.1, 1.2]) == 0.0


# ---------------------------------------------------------------------------
# 7. Input not mutated
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_rows_not_mutated(self):
        rows = _signal_dataset()
        original = [dict(r) for r in rows]
        run_mean_reversion_backtest(rows)
        for orig, after in zip(original, rows):
            assert orig == after

    def test_no_new_keys_in_input(self):
        rows = _signal_dataset()
        original_keys = {frozenset(r.keys()) for r in rows}
        run_mean_reversion_backtest(rows)
        after_keys = {frozenset(r.keys()) for r in rows}
        assert original_keys == after_keys


# ---------------------------------------------------------------------------
# 8. Deterministic
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_input_same_output(self):
        rows = _signal_dataset()
        r1 = run_mean_reversion_backtest(rows)
        r2 = run_mean_reversion_backtest(rows)
        assert r1["trades"]       == r2["trades"]
        assert r1["winrate"]      == r2["winrate"]
        assert r1["total_return"] == r2["total_return"]
        assert r1["max_drawdown"] == r2["max_drawdown"]
        assert r1["equity_curve"] == r2["equity_curve"]


# ---------------------------------------------------------------------------
# 9 & 10. Winning / losing trade equity
# ---------------------------------------------------------------------------

class TestEquityDirection:
    def test_winning_trade_equity_above_one(self):
        result = run_mean_reversion_backtest(_signal_dataset())
        assert result["equity_curve"][-1] > 1.0

    def test_losing_trade_equity_below_one(self):
        result = run_mean_reversion_backtest(_losing_signal_dataset())
        assert result["equity_curve"][-1] < 1.0

    def test_no_trade_equity_stays_at_one(self):
        result = run_mean_reversion_backtest(_neutral_rows(30))
        assert result["equity_curve"] == [1.0]


# ---------------------------------------------------------------------------
# 11 & 12. load_ohlcv_rows
# ---------------------------------------------------------------------------

class TestLoadOhlcvRows:
    def test_missing_db_returns_empty(self, tmp_path):
        result = load_ohlcv_rows(tmp_path / "nonexistent.sqlite",
                                 "bitvavo", "BTC-EUR", "1h")
        assert result == []

    def test_corrupt_db_returns_empty(self, tmp_path):
        bad = tmp_path / "bad.sqlite"
        bad.write_bytes(b"not a sqlite file")
        result = load_ohlcv_rows(bad, "bitvavo", "BTC-EUR", "1h")
        assert result == []

    def test_loads_correct_rows(self, tmp_path):
        ohlcv_rows = [{"ts_utc": i, "open": 100.0, "high": 101.0, "low": 99.0,
                       "close": 100.0 + i * 0.1, "volume": 10.0}
                      for i in range(5)]
        db = _write_db(tmp_path, ohlcv_rows)
        result = load_ohlcv_rows(db, "bitvavo", "BTC-EUR", "1h")
        assert len(result) == 5

    def test_sorted_ascending_ts(self, tmp_path):
        ohlcv_rows = [{"ts_utc": i, "open": 100.0, "high": 101.0, "low": 99.0,
                       "close": 100.0, "volume": 10.0}
                      for i in [5, 1, 3, 2, 4]]
        db = _write_db(tmp_path, ohlcv_rows)
        result = load_ohlcv_rows(db, "bitvavo", "BTC-EUR", "1h")
        ts_vals = [r["ts_utc"] for r in result]
        assert ts_vals == sorted(ts_vals)

    def test_filters_by_market(self, tmp_path):
        ohlcv_rows = [{"ts_utc": i, "open": 100.0, "high": 101.0, "low": 99.0,
                       "close": 100.0, "volume": 10.0}
                      for i in range(3)]
        db = _write_db(tmp_path, ohlcv_rows)
        result = load_ohlcv_rows(db, "bitvavo", "ETH-EUR", "1h")
        assert result == []  # wrong market


# ---------------------------------------------------------------------------
# 13. run_backtest orchestrator
# ---------------------------------------------------------------------------

class TestRunBacktest:
    def test_returns_required_keys(self, tmp_path):
        result = run_backtest(
            db_path   = tmp_path / "nonexistent.sqlite",
            exchange  = "bitvavo",
            market    = "BTC-EUR",
            timeframe = "1h",
        )
        for key in ("trades", "winrate", "total_return", "max_drawdown",
                    "equity_curve", "exchange", "market", "timeframe", "rows_loaded"):
            assert key in result

    def test_no_db_no_crash(self, tmp_path):
        result = run_backtest(
            db_path   = tmp_path / "nonexistent.sqlite",
            exchange  = "bitvavo",
            market    = "BTC-EUR",
            timeframe = "1h",
        )
        assert result["trades"] == 0
        assert result["rows_loaded"] == 0

    def test_rows_loaded_reflects_db(self, tmp_path):
        ohlcv_rows = [{"ts_utc": i, "open": 100.0, "high": 101.0, "low": 99.0,
                       "close": 100.0, "volume": 10.0}
                      for i in range(10)]
        db = _write_db(tmp_path, ohlcv_rows)
        result = run_backtest(db_path=db, exchange="bitvavo",
                              market="BTC-EUR", timeframe="1h")
        assert result["rows_loaded"] == 10


# ---------------------------------------------------------------------------
# 15. Force-exit at end of data
# ---------------------------------------------------------------------------

class TestForceExit:
    def test_open_position_at_end_is_closed(self):
        """Enter but never see rsi>50 → force-exit at last row."""
        rows = []
        for i in range(5):
            rows.append(_row(i, 100.0, rsi=50.0, bb_lower=97.0))
        # Entry signal
        rows.append(_row(5, 96.0, rsi=25.0, bb_lower=97.0))
        # Subsequent rows with rsi that never hits exit condition
        for i in range(6, 15):
            rows.append(_row(i, 98.0, rsi=40.0, bb_lower=97.0))

        result = run_mean_reversion_backtest(rows)
        assert result["trades"] == 1  # force-exit counted
        assert len(result["equity_curve"]) == 2


# ---------------------------------------------------------------------------
# 18. No-trade total_return
# ---------------------------------------------------------------------------

class TestNoTradeReturn:
    def test_total_return_zero_when_no_trades(self):
        result = run_mean_reversion_backtest(_neutral_rows(50))
        assert result["total_return"] == 0.0
