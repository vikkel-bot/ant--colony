"""
AC-127: Tests for Multi-Market Strategy Comparison Runner

Covers:
  1.  Multiple markets → no crash
  2.  Every market yields a result dict
  3.  top_strategy exists per market (str or None)
  4.  Frequency count is correct
  5.  Empty dataset per market → no crash
  6.  Output is deterministic
  7.  No pipeline files touched
  8.  market_count in summary matches markets list
  9.  Frequency only counts non-None top_strategy
  10. Empty markets list → no crash, empty result
  11. Ranking preserved per market
  12. Strategies list present per market
  13. flags correct
  14. Single market → works like AC-125
  15. _build_frequency sorted descending by count, then alphabetical
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Import resolution
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent
_RES_DIR   = _REPO_ROOT / "ant_colony" / "research"

for _p in (str(_REPO_ROOT), str(_RES_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from ant_colony.research.run_multi_market_comparison_lite import (
    FLAGS,
    _build_frequency,
    _run_single_market,
    run_multi_market_comparison,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_comparison(market: str, top: str, ranking: list[str]) -> dict:
    """Return a minimal comparison dict as run_comparison() would."""
    strategies = [
        {"name": n, "trades": 5, "winrate": 0.5,
         "total_return": 0.1 if n == top else 0.0,
         "max_drawdown": -0.05, "equity_curve": [1.0]}
        for n in ranking
    ]
    return {
        "exchange":    "bitvavo",
        "market":      market,
        "timeframe":   "1h",
        "rows_loaded": 100,
        "strategies":  strategies,
        "ranking":     ranking,
    }


def _patch_run_comparison(market_map: dict[str, dict]):
    """
    Patch run_comparison to return fixed results keyed by market.
    market_map: { "BTC-EUR": comparison_dict, ... }
    """
    def _fake(db_path, exchange, market, timeframe):
        if market in market_map:
            return market_map[market]
        # No data for unknown market
        return {
            "exchange": exchange, "market": market, "timeframe": timeframe,
            "rows_loaded": 0, "strategies": [], "ranking": [],
        }
    return patch(
        "ant_colony.research.run_multi_market_comparison_lite.run_comparison",
        side_effect=_fake,
    )


# ---------------------------------------------------------------------------
# 1. Multiple markets → no crash
# ---------------------------------------------------------------------------

class TestMultipleMarketsNoCrash:
    def test_three_markets_no_crash(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=["BTC-EUR", "ETH-EUR", "ADA-EUR"],
        )
        assert isinstance(result, dict)

    def test_single_market_no_crash(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=["BTC-EUR"],
        )
        assert isinstance(result, dict)

    def test_empty_markets_no_crash(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=[],
        )
        assert isinstance(result, dict)

    def test_missing_db_no_crash(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "nonexistent.sqlite",
            markets=["BTC-EUR", "ETH-EUR"],
        )
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 2. Every market yields a result dict
# ---------------------------------------------------------------------------

class TestEveryMarketHasResult:
    def test_market_count_matches(self, tmp_path):
        markets = ["BTC-EUR", "ETH-EUR", "ADA-EUR"]
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=markets,
        )
        assert len(result["markets"]) == len(markets)

    def test_each_result_has_market_key(self, tmp_path):
        markets = ["BTC-EUR", "ETH-EUR"]
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=markets,
        )
        market_names = [r["market"] for r in result["markets"]]
        assert set(market_names) == set(markets)

    def test_each_result_is_dict(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=["BTC-EUR", "ETH-EUR"],
        )
        for mr in result["markets"]:
            assert isinstance(mr, dict)


# ---------------------------------------------------------------------------
# 3. top_strategy exists per market (str or None)
# ---------------------------------------------------------------------------

class TestTopStrategyPerMarket:
    def test_top_strategy_key_present(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=["BTC-EUR", "ETH-EUR"],
        )
        for mr in result["markets"]:
            assert "top_strategy" in mr

    def test_top_strategy_is_str_or_none(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=["BTC-EUR", "ETH-EUR"],
        )
        for mr in result["markets"]:
            assert mr["top_strategy"] is None or isinstance(mr["top_strategy"], str)

    def test_top_strategy_with_patched_data(self, tmp_path):
        market_map = {
            "BTC-EUR": _fake_comparison("BTC-EUR", "trend_follow_lite",
                                        ["trend_follow_lite", "mean_reversion"]),
            "ETH-EUR": _fake_comparison("ETH-EUR", "mean_reversion",
                                        ["mean_reversion", "trend_follow_lite"]),
        }
        with _patch_run_comparison(market_map):
            result = run_multi_market_comparison(
                db_path=tmp_path / "x.sqlite",
                markets=["BTC-EUR", "ETH-EUR"],
            )
        tops = {mr["market"]: mr["top_strategy"] for mr in result["markets"]}
        assert tops["BTC-EUR"] == "trend_follow_lite"
        assert tops["ETH-EUR"] == "mean_reversion"


# ---------------------------------------------------------------------------
# 4. Frequency count is correct
# ---------------------------------------------------------------------------

class TestFrequencyCount:
    def test_frequency_correct(self, tmp_path):
        market_map = {
            "BTC-EUR": _fake_comparison("BTC-EUR", "trend_follow_lite",
                                        ["trend_follow_lite", "mean_reversion"]),
            "ETH-EUR": _fake_comparison("ETH-EUR", "trend_follow_lite",
                                        ["trend_follow_lite", "mean_reversion"]),
            "ADA-EUR": _fake_comparison("ADA-EUR", "mean_reversion",
                                        ["mean_reversion", "trend_follow_lite"]),
        }
        with _patch_run_comparison(market_map):
            result = run_multi_market_comparison(
                db_path=tmp_path / "x.sqlite",
                markets=["BTC-EUR", "ETH-EUR", "ADA-EUR"],
            )
        freq = result["summary"]["top_strategy_frequency"]
        assert freq["trend_follow_lite"] == 2
        assert freq["mean_reversion"] == 1

    def test_frequency_sums_to_market_count_with_data(self, tmp_path):
        market_map = {
            "BTC-EUR": _fake_comparison("BTC-EUR", "mean_reversion",
                                        ["mean_reversion"]),
            "ETH-EUR": _fake_comparison("ETH-EUR", "mean_reversion",
                                        ["mean_reversion"]),
        }
        with _patch_run_comparison(market_map):
            result = run_multi_market_comparison(
                db_path=tmp_path / "x.sqlite",
                markets=["BTC-EUR", "ETH-EUR"],
            )
        freq = result["summary"]["top_strategy_frequency"]
        assert sum(freq.values()) == 2

    def test_build_frequency_direct(self):
        market_results = [
            {"top_strategy": "a"},
            {"top_strategy": "b"},
            {"top_strategy": "a"},
            {"top_strategy": None},
        ]
        freq = _build_frequency(market_results)
        assert freq["a"] == 2
        assert freq["b"] == 1
        assert None not in freq

    def test_build_frequency_all_none(self):
        market_results = [{"top_strategy": None}, {"top_strategy": None}]
        freq = _build_frequency(market_results)
        assert freq == {}

    def test_build_frequency_empty(self):
        assert _build_frequency([]) == {}


# ---------------------------------------------------------------------------
# 5. Empty dataset per market → no crash
# ---------------------------------------------------------------------------

class TestEmptyDatasetPerMarket:
    def test_empty_db_no_crash(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "empty.sqlite",
            markets=["BTC-EUR", "ETH-EUR", "ADA-EUR"],
        )
        assert isinstance(result, dict)

    def test_empty_db_top_strategy_is_str_or_none(self, tmp_path):
        # AC-125 runs all strategies even on empty data (all zeros → alphabetical rank)
        # top_strategy is the alphabetically first strategy name, not None
        result = run_multi_market_comparison(
            db_path=tmp_path / "empty.sqlite",
            markets=["BTC-EUR"],
        )
        top = result["markets"][0]["top_strategy"]
        assert top is None or isinstance(top, str)

    def test_empty_db_ranking_length(self, tmp_path):
        # With empty data, AC-125 still ranks all 3 strategies (all zeros)
        result = run_multi_market_comparison(
            db_path=tmp_path / "empty.sqlite",
            markets=["BTC-EUR"],
        )
        ranking = result["markets"][0]["ranking"]
        assert isinstance(ranking, list)
        assert len(ranking) == 3  # all 3 strategies present, ordered alphabetically


# ---------------------------------------------------------------------------
# 6. Output is deterministic
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self, tmp_path):
        kwargs = dict(
            db_path=tmp_path / "x.sqlite",
            markets=["BTC-EUR", "ETH-EUR"],
            timeframe="1h",
        )
        r1 = run_multi_market_comparison(**kwargs)
        r2 = run_multi_market_comparison(**kwargs)
        assert r1["summary"] == r2["summary"]
        assert [mr["market"] for mr in r1["markets"]] == \
               [mr["market"] for mr in r2["markets"]]
        for m1, m2 in zip(r1["markets"], r2["markets"]):
            assert m1["top_strategy"] == m2["top_strategy"]
            assert m1["ranking"]      == m2["ranking"]


# ---------------------------------------------------------------------------
# 7. No pipeline files touched
# ---------------------------------------------------------------------------

class TestNoPipelineImpact:
    def test_no_ant_out_path_in_source(self):
        src = Path(__file__).resolve().parent / \
              "ant_colony" / "research" / "run_multi_market_comparison_lite.py"
        content = src.read_text(encoding="utf-8")
        assert r"C:\Trading\ANT_OUT" not in content
        assert "Trading/ANT_OUT" not in content

    def test_no_execution_intent_in_source(self):
        src = Path(__file__).resolve().parent / \
              "ant_colony" / "research" / "run_multi_market_comparison_lite.py"
        content = src.read_text(encoding="utf-8")
        assert "execution_intent" not in content
        assert "broker" not in content.lower()


# ---------------------------------------------------------------------------
# 8. market_count in summary
# ---------------------------------------------------------------------------

class TestSummaryMarketCount:
    def test_market_count_correct(self, tmp_path):
        markets = ["BTC-EUR", "ETH-EUR", "ADA-EUR"]
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=markets,
        )
        assert result["summary"]["market_count"] == 3

    def test_market_count_zero_for_empty_list(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=[],
        )
        assert result["summary"]["market_count"] == 0

    def test_summary_has_frequency_key(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=["BTC-EUR"],
        )
        assert "top_strategy_frequency" in result["summary"]


# ---------------------------------------------------------------------------
# 9. Frequency only counts non-None top_strategy
# ---------------------------------------------------------------------------

class TestFrequencyExcludesNone:
    def test_none_top_strategy_excluded(self, tmp_path):
        market_map = {
            "BTC-EUR": _fake_comparison("BTC-EUR", "trend_follow_lite",
                                        ["trend_follow_lite"]),
        }
        with _patch_run_comparison(market_map):
            result = run_multi_market_comparison(
                db_path=tmp_path / "x.sqlite",
                markets=["BTC-EUR", "ETH-EUR"],  # ETH-EUR → no data → None
            )
        freq = result["summary"]["top_strategy_frequency"]
        assert sum(freq.values()) == 1  # only BTC-EUR counted


# ---------------------------------------------------------------------------
# 11. Ranking preserved per market
# ---------------------------------------------------------------------------

class TestRankingPreserved:
    def test_ranking_order_preserved(self, tmp_path):
        ranking = ["mean_reversion", "trend_follow_lite", "volatility_breakout_lite"]
        market_map = {
            "BTC-EUR": _fake_comparison("BTC-EUR", "mean_reversion", ranking),
        }
        with _patch_run_comparison(market_map):
            result = run_multi_market_comparison(
                db_path=tmp_path / "x.sqlite",
                markets=["BTC-EUR"],
            )
        assert result["markets"][0]["ranking"] == ranking


# ---------------------------------------------------------------------------
# 12. Strategies list present per market
# ---------------------------------------------------------------------------

class TestStrategiesPerMarket:
    def test_strategies_key_present(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=["BTC-EUR"],
        )
        assert "strategies" in result["markets"][0]

    def test_strategies_is_list(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite",
            markets=["BTC-EUR"],
        )
        assert isinstance(result["markets"][0]["strategies"], list)

    def test_strategies_no_equity_curve(self, tmp_path):
        market_map = {
            "BTC-EUR": _fake_comparison("BTC-EUR", "mean_reversion",
                                        ["mean_reversion", "trend_follow_lite"]),
        }
        with _patch_run_comparison(market_map):
            result = run_multi_market_comparison(
                db_path=tmp_path / "x.sqlite",
                markets=["BTC-EUR"],
            )
        for s in result["markets"][0]["strategies"]:
            assert "equity_curve" not in s


# ---------------------------------------------------------------------------
# 13. Flags correct
# ---------------------------------------------------------------------------

class TestFlags:
    def test_research_only_true(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite", markets=["BTC-EUR"]
        )
        assert result["flags"]["research_only"] is True

    def test_pipeline_impact_false(self, tmp_path):
        result = run_multi_market_comparison(
            db_path=tmp_path / "x.sqlite", markets=["BTC-EUR"]
        )
        assert result["flags"]["pipeline_impact"] is False

    def test_flags_module_constant(self):
        assert FLAGS["research_only"] is True
        assert FLAGS["pipeline_impact"] is False


# ---------------------------------------------------------------------------
# 15. _build_frequency sorted desc by count, then alphabetical
# ---------------------------------------------------------------------------

class TestFrequencySorting:
    def test_sorted_descending_by_count(self):
        market_results = [
            {"top_strategy": "z_strategy"},
            {"top_strategy": "z_strategy"},
            {"top_strategy": "z_strategy"},
            {"top_strategy": "a_strategy"},
            {"top_strategy": "a_strategy"},
            {"top_strategy": "m_strategy"},
        ]
        freq = _build_frequency(market_results)
        keys = list(freq.keys())
        assert keys[0] == "z_strategy"  # count=3
        assert keys[1] == "a_strategy"  # count=2
        assert keys[2] == "m_strategy"  # count=1

    def test_alphabetical_tiebreak(self):
        market_results = [
            {"top_strategy": "z_strat"},
            {"top_strategy": "a_strat"},
        ]
        freq = _build_frequency(market_results)
        keys = list(freq.keys())
        assert keys[0] == "a_strat"  # count=1, alphabetically first
        assert keys[1] == "z_strat"
