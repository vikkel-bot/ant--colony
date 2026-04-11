"""
AC-125: Tests for Multi-Strategy Comparison Layer

Covers:
  1.  No data  → no crash
  2.  All 3 strategies are executed
  3.  Output contains exactly 3 strategy results
  4.  Ranking is deterministic
  5.  Metrics exist for every strategy
  6.  Winrate in [0, 1]
  7.  max_drawdown <= 0
  8.  Same input → same output
  9.  No input mutation
  10. run_strategy_backtest raises KeyError for unknown strategy
  11. _rank ordering: highest total_return first
  12. _rank tie-break: winrate
  13. _rank tie-break: max_drawdown (less negative = better)
  14. _rank tie-break: alphabetical name
  15. Single-row dataset → no crash
  16. STRATEGIES registry contains exactly the three expected keys
  17. run_comparison returns 'ranking' key as list of names
  18. equity_curve starts at 1.0 for all strategies
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Import resolution (same pattern as production modules)
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent
_RES_DIR   = _REPO_ROOT / "ant_colony" / "research"

for _p in (str(_REPO_ROOT), str(_RES_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from ant_colony.research.run_strategy_comparison_lite import (
    STRATEGIES,
    _rank,
    _simulate,
    run_strategy_backtest,
    run_comparison,
)
from ant_colony.research.ta_indicators_lite import add_indicators


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rows(n: int, close_val: float = 50_000.0) -> list[dict]:
    """Generate n synthetic OHLCV rows with enough history for all indicators."""
    rows = []
    for i in range(n):
        c = close_val + i * 10
        rows.append({
            "ts_utc": 1_700_000_000_000 + i * 3_600_000,
            "open":   c - 50,
            "high":   c + 100,
            "low":    c - 100,
            "close":  c,
            "volume": 1.0,
        })
    return rows


def _enriched(n: int = 100, close_val: float = 50_000.0) -> list[dict]:
    return add_indicators(_make_rows(n, close_val))


def _enriched_trigger_mean_reversion() -> list[dict]:
    """
    Craft rows that trigger at least one mean_reversion trade:
    Force RSI < 30 by using a strongly declining close sequence,
    then recovering.
    Starts high, drops hard, then rebounds.
    """
    rows = []
    base_ts = 1_700_000_000_000
    # 20 stable rows (warmup)
    for i in range(20):
        rows.append({
            "ts_utc": base_ts + i * 3_600_000,
            "open": 50_000.0, "high": 50_200.0,
            "low":  49_800.0, "close": 50_000.0, "volume": 1.0,
        })
    # 30 hard-declining rows (forces RSI down)
    for i in range(30):
        c = 50_000.0 - (i + 1) * 500
        rows.append({
            "ts_utc": base_ts + (20 + i) * 3_600_000,
            "open": c + 50, "high": c + 100,
            "low":  c - 100, "close": c, "volume": 1.0,
        })
    # 40 recovery rows
    for i in range(40):
        c = 35_000.0 + (i + 1) * 300
        rows.append({
            "ts_utc": base_ts + (50 + i) * 3_600_000,
            "open": c - 50, "high": c + 200,
            "low":  c - 200, "close": c, "volume": 1.0,
        })
    return add_indicators(rows)


# ---------------------------------------------------------------------------
# 1. No data → no crash
# ---------------------------------------------------------------------------

class TestNoData:
    def test_simulate_empty_no_crash(self):
        result = _simulate([], lambda r: False, lambda r: False)
        assert isinstance(result, dict)

    def test_simulate_empty_metrics(self):
        result = _simulate([], lambda r: False, lambda r: False)
        assert result["trades"] == 0
        assert result["total_return"] == 0.0
        assert result["winrate"] == 0.0
        assert result["max_drawdown"] == 0.0
        assert result["equity_curve"] == [1.0]

    def test_run_strategy_backtest_empty_no_crash(self):
        for name in STRATEGIES:
            result = run_strategy_backtest([], name)
            assert isinstance(result, dict)

    def test_run_comparison_missing_db_no_crash(self, tmp_path):
        result = run_comparison(db_path=tmp_path / "nonexistent.sqlite")
        assert isinstance(result, dict)

    def test_run_comparison_missing_db_strategies_key(self, tmp_path):
        result = run_comparison(db_path=tmp_path / "nonexistent.sqlite")
        assert "strategies" in result

    def test_run_comparison_missing_db_three_results(self, tmp_path):
        result = run_comparison(db_path=tmp_path / "nonexistent.sqlite")
        assert len(result["strategies"]) == 3


# ---------------------------------------------------------------------------
# 2. All 3 strategies are executed
# ---------------------------------------------------------------------------

class TestAllStrategiesExecuted:
    def test_strategy_keys_present(self):
        enriched = _enriched(100)
        names = set()
        for name in STRATEGIES:
            result = run_strategy_backtest(enriched, name)
            names.add(result["name"])
        assert names == set(STRATEGIES.keys())

    def test_all_strategies_return_dict(self):
        enriched = _enriched(100)
        for name in STRATEGIES:
            result = run_strategy_backtest(enriched, name)
            assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 3. Output contains exactly 3 strategy results
# ---------------------------------------------------------------------------

class TestExactlyThreeResults:
    def test_strategies_registry_has_three(self):
        assert len(STRATEGIES) == 3

    def test_strategies_registry_exact_keys(self):
        assert set(STRATEGIES.keys()) == {
            "mean_reversion",
            "trend_follow_lite",
            "volatility_breakout_lite",
        }

    def test_run_comparison_three_strategies(self, tmp_path):
        result = run_comparison(db_path=tmp_path / "x.sqlite")
        assert len(result["strategies"]) == 3

    def test_run_comparison_three_ranking_names(self, tmp_path):
        result = run_comparison(db_path=tmp_path / "x.sqlite")
        assert len(result["ranking"]) == 3


# ---------------------------------------------------------------------------
# 4. Ranking is deterministic
# ---------------------------------------------------------------------------

class TestRankingDeterministic:
    def test_same_input_same_ranking(self, tmp_path):
        r1 = run_comparison(db_path=tmp_path / "x.sqlite")
        r2 = run_comparison(db_path=tmp_path / "x.sqlite")
        assert r1["ranking"] == r2["ranking"]

    def test_rank_function_deterministic(self):
        results = [
            {"name": "b", "total_return": 0.1, "winrate": 0.5, "max_drawdown": -0.05},
            {"name": "a", "total_return": 0.2, "winrate": 0.6, "max_drawdown": -0.10},
            {"name": "c", "total_return": 0.05, "winrate": 0.4, "max_drawdown": -0.02},
        ]
        r1 = _rank(results)
        r2 = _rank(results)
        assert [r["name"] for r in r1] == [r["name"] for r in r2]

    def test_rank_highest_return_first(self):
        results = [
            {"name": "low",  "total_return": 0.05, "winrate": 0.5, "max_drawdown": -0.01},
            {"name": "high", "total_return": 0.30, "winrate": 0.5, "max_drawdown": -0.01},
            {"name": "mid",  "total_return": 0.15, "winrate": 0.5, "max_drawdown": -0.01},
        ]
        ranked = _rank(results)
        assert ranked[0]["name"] == "high"
        assert ranked[1]["name"] == "mid"
        assert ranked[2]["name"] == "low"


# ---------------------------------------------------------------------------
# 5. Metrics exist for every strategy
# ---------------------------------------------------------------------------

class TestMetricsExist:
    REQUIRED_KEYS = {"trades", "winrate", "total_return", "max_drawdown", "equity_curve", "name"}

    def test_all_metrics_present(self):
        enriched = _enriched(100)
        for name in STRATEGIES:
            result = run_strategy_backtest(enriched, name)
            assert self.REQUIRED_KEYS.issubset(result.keys()), \
                f"{name} missing keys: {self.REQUIRED_KEYS - result.keys()}"

    def test_metrics_types(self):
        enriched = _enriched(100)
        for name in STRATEGIES:
            r = run_strategy_backtest(enriched, name)
            assert isinstance(r["trades"], int)
            assert isinstance(r["winrate"], float)
            assert isinstance(r["total_return"], float)
            assert isinstance(r["max_drawdown"], float)
            assert isinstance(r["equity_curve"], list)
            assert isinstance(r["name"], str)


# ---------------------------------------------------------------------------
# 6. Winrate in [0, 1]
# ---------------------------------------------------------------------------

class TestWinrateRange:
    def test_winrate_range_enriched(self):
        enriched = _enriched(100)
        for name in STRATEGIES:
            r = run_strategy_backtest(enriched, name)
            assert 0.0 <= r["winrate"] <= 1.0, \
                f"{name} winrate={r['winrate']} out of range"

    def test_winrate_range_empty(self):
        for name in STRATEGIES:
            r = run_strategy_backtest([], name)
            assert 0.0 <= r["winrate"] <= 1.0

    def test_winrate_zero_no_trades(self):
        for name in STRATEGIES:
            r = run_strategy_backtest([], name)
            assert r["winrate"] == 0.0


# ---------------------------------------------------------------------------
# 7. max_drawdown <= 0
# ---------------------------------------------------------------------------

class TestMaxDrawdown:
    def test_max_drawdown_non_positive(self):
        enriched = _enriched(100)
        for name in STRATEGIES:
            r = run_strategy_backtest(enriched, name)
            assert r["max_drawdown"] <= 0.0, \
                f"{name} max_drawdown={r['max_drawdown']} is positive"

    def test_max_drawdown_empty_zero(self):
        for name in STRATEGIES:
            r = run_strategy_backtest([], name)
            assert r["max_drawdown"] == 0.0

    def test_max_drawdown_no_trades_zero(self):
        # Rows that never trigger entry
        rows = _enriched(100, close_val=50_000.0)
        # mean_reversion needs rsi<30; with flat trending data likely 0 trades
        r = run_strategy_backtest(rows, "mean_reversion")
        # max_drawdown should be 0 if no trades (equity_curve=[1.0])
        if r["trades"] == 0:
            assert r["max_drawdown"] == 0.0


# ---------------------------------------------------------------------------
# 8. Same input → same output
# ---------------------------------------------------------------------------

class TestDeterministicOutput:
    def test_same_input_same_output(self):
        enriched = _enriched(150)
        for name in STRATEGIES:
            r1 = run_strategy_backtest(enriched, name)
            r2 = run_strategy_backtest(enriched, name)
            assert r1["trades"]       == r2["trades"]
            assert r1["winrate"]      == r2["winrate"]
            assert r1["total_return"] == r2["total_return"]
            assert r1["max_drawdown"] == r2["max_drawdown"]
            assert r1["equity_curve"] == r2["equity_curve"]


# ---------------------------------------------------------------------------
# 9. No input mutation
# ---------------------------------------------------------------------------

class TestNoInputMutation:
    def test_rows_not_mutated(self):
        enriched = _enriched(100)
        originals = [dict(r) for r in enriched]
        for name in STRATEGIES:
            run_strategy_backtest(enriched, name)
        assert enriched == originals

    def test_simulate_does_not_mutate(self):
        rows = _enriched(50)
        original = [dict(r) for r in rows]
        _simulate(rows, lambda r: False, lambda r: False)
        assert rows == original


# ---------------------------------------------------------------------------
# 10. Unknown strategy raises KeyError
# ---------------------------------------------------------------------------

class TestUnknownStrategy:
    def test_unknown_strategy_raises(self):
        with pytest.raises(KeyError):
            run_strategy_backtest([], "does_not_exist")

    def test_empty_string_strategy_raises(self):
        with pytest.raises(KeyError):
            run_strategy_backtest([], "")


# ---------------------------------------------------------------------------
# 11-14. Ranking tie-breaks
# ---------------------------------------------------------------------------

class TestRankTieBreaks:
    def test_tiebreak_winrate(self):
        results = [
            {"name": "a", "total_return": 0.10, "winrate": 0.4, "max_drawdown": -0.01},
            {"name": "b", "total_return": 0.10, "winrate": 0.6, "max_drawdown": -0.01},
        ]
        ranked = _rank(results)
        assert ranked[0]["name"] == "b"  # higher winrate

    def test_tiebreak_max_drawdown(self):
        results = [
            {"name": "a", "total_return": 0.10, "winrate": 0.5, "max_drawdown": -0.20},
            {"name": "b", "total_return": 0.10, "winrate": 0.5, "max_drawdown": -0.05},
        ]
        ranked = _rank(results)
        assert ranked[0]["name"] == "b"  # less negative = ranked first

    def test_tiebreak_alphabetical(self):
        results = [
            {"name": "z_strategy", "total_return": 0.10, "winrate": 0.5, "max_drawdown": -0.05},
            {"name": "a_strategy", "total_return": 0.10, "winrate": 0.5, "max_drawdown": -0.05},
        ]
        ranked = _rank(results)
        assert ranked[0]["name"] == "a_strategy"

    def test_rank_returns_new_list(self):
        results = [
            {"name": "a", "total_return": 0.1, "winrate": 0.5, "max_drawdown": -0.01},
        ]
        ranked = _rank(results)
        assert ranked is not results


# ---------------------------------------------------------------------------
# 15. Single-row dataset → no crash
# ---------------------------------------------------------------------------

class TestSingleRow:
    def test_single_row_no_crash(self):
        row = add_indicators([{
            "ts_utc": 1_700_000_000_000,
            "open": 100.0, "high": 110.0, "low": 90.0,
            "close": 105.0, "volume": 1.0,
        }])
        for name in STRATEGIES:
            result = run_strategy_backtest(row, name)
            assert isinstance(result, dict)

    def test_single_row_zero_trades(self):
        row = add_indicators([{
            "ts_utc": 1_700_000_000_000,
            "open": 100.0, "high": 110.0, "low": 90.0,
            "close": 105.0, "volume": 1.0,
        }])
        for name in STRATEGIES:
            result = run_strategy_backtest(row, name)
            assert result["trades"] == 0


# ---------------------------------------------------------------------------
# 16. STRATEGIES registry
# ---------------------------------------------------------------------------

class TestStrategiesRegistry:
    def test_all_entries_are_tuples_of_callables(self):
        for name, (entry_fn, exit_fn) in STRATEGIES.items():
            assert callable(entry_fn), f"{name} entry_fn not callable"
            assert callable(exit_fn),  f"{name} exit_fn not callable"

    def test_predicates_accept_dict(self):
        row = {"close": 100.0, "rsi_14": 25.0, "bb_lower": 110.0,
               "bb_upper": 130.0, "sma_20": 105.0, "ema_20": 104.0}
        for name, (entry_fn, exit_fn) in STRATEGIES.items():
            assert isinstance(entry_fn(row), bool), f"{name} entry_fn not bool"
            assert isinstance(exit_fn(row), bool),  f"{name} exit_fn not bool"


# ---------------------------------------------------------------------------
# 17. run_comparison 'ranking' key
# ---------------------------------------------------------------------------

class TestRunComparisonOutput:
    def test_ranking_key_is_list(self, tmp_path):
        result = run_comparison(db_path=tmp_path / "x.sqlite")
        assert isinstance(result["ranking"], list)

    def test_ranking_names_match_strategies(self, tmp_path):
        result = run_comparison(db_path=tmp_path / "x.sqlite")
        assert set(result["ranking"]) == set(STRATEGIES.keys())

    def test_strategies_key_is_list(self, tmp_path):
        result = run_comparison(db_path=tmp_path / "x.sqlite")
        assert isinstance(result["strategies"], list)

    def test_context_keys_present(self, tmp_path):
        result = run_comparison(
            db_path=tmp_path / "x.sqlite",
            exchange="test_exch",
            market="TST-EUR",
            timeframe="1h",
        )
        assert result["exchange"]  == "test_exch"
        assert result["market"]    == "TST-EUR"
        assert result["timeframe"] == "1h"
        assert "rows_loaded" in result

    def test_rows_loaded_zero_missing_db(self, tmp_path):
        result = run_comparison(db_path=tmp_path / "x.sqlite")
        assert result["rows_loaded"] == 0


# ---------------------------------------------------------------------------
# 18. equity_curve starts at 1.0
# ---------------------------------------------------------------------------

class TestEquityCurve:
    def test_equity_curve_starts_at_one(self):
        enriched = _enriched(100)
        for name in STRATEGIES:
            r = run_strategy_backtest(enriched, name)
            assert r["equity_curve"][0] == 1.0, \
                f"{name} equity_curve[0]={r['equity_curve'][0]}"

    def test_equity_curve_length_equals_trades_plus_one(self):
        enriched = _enriched(200)
        for name in STRATEGIES:
            r = run_strategy_backtest(enriched, name)
            assert len(r["equity_curve"]) == r["trades"] + 1, \
                f"{name}: len(curve)={len(r['equity_curve'])} trades={r['trades']}"

    def test_equity_curve_all_positive(self):
        enriched = _enriched(100)
        for name in STRATEGIES:
            r = run_strategy_backtest(enriched, name)
            assert all(v > 0 for v in r["equity_curve"]), \
                f"{name} has non-positive equity value"
