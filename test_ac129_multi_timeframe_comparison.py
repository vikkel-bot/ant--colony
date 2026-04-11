"""
AC-129: Tests for Multi-Timeframe Strategy Comparison Runner

Covers:
  1.  Multiple timeframes → no crash
  2.  Every timeframe yields a result dict
  3.  top_strategy exists per timeframe (str or None)
  4.  Frequency count correct
  5.  Empty dataset per timeframe → no crash
  6.  Output deterministic
  7.  No pipeline files touched
  8.  timeframe_count in summary matches input list
  9.  Frequency only counts non-None top_strategy
  10. Empty timeframes list → no crash, empty result
  11. Ranking preserved per timeframe
  12. strategies list present per timeframe (no equity_curve)
  13. flags correct
  14. _build_frequency sorted desc by count, then alphabetical
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

from ant_colony.research.run_multi_timeframe_comparison_lite import (
    FLAGS,
    _build_frequency,
    _run_single_timeframe,
    run_multi_timeframe_comparison,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_comparison(timeframe: str, top: str, ranking: list[str]) -> dict:
    strategies = [
        {"name": n, "trades": 5, "winrate": 0.5,
         "total_return": 0.1 if n == top else 0.0,
         "max_drawdown": -0.05, "equity_curve": [1.0]}
        for n in ranking
    ]
    return {
        "exchange":    "bitvavo",
        "market":      "BTC-EUR",
        "timeframe":   timeframe,
        "rows_loaded": 100,
        "strategies":  strategies,
        "ranking":     ranking,
    }


def _patch_run_comparison(tf_map: dict[str, dict]):
    def _fake(db_path, exchange, market, timeframe):
        if timeframe in tf_map:
            return tf_map[timeframe]
        return {
            "exchange": exchange, "market": market, "timeframe": timeframe,
            "rows_loaded": 0, "strategies": [], "ranking": [],
        }
    return patch(
        "ant_colony.research.run_multi_timeframe_comparison_lite.run_comparison",
        side_effect=_fake,
    )


# ---------------------------------------------------------------------------
# 1. Multiple timeframes → no crash
# ---------------------------------------------------------------------------

class TestMultipleTimeframesNoCrash:
    def test_three_timeframes_no_crash(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h", "4h", "1d"],
        )
        assert isinstance(result, dict)

    def test_single_timeframe_no_crash(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h"],
        )
        assert isinstance(result, dict)

    def test_empty_timeframes_no_crash(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=[],
        )
        assert isinstance(result, dict)

    def test_missing_db_no_crash(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "nonexistent.sqlite",
            timeframes=["1h", "4h"],
        )
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 2. Every timeframe yields a result dict
# ---------------------------------------------------------------------------

class TestEveryTimeframeHasResult:
    def test_timeframe_count_matches(self, tmp_path):
        tfs = ["1h", "4h", "1d"]
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=tfs,
        )
        assert len(result["timeframes"]) == len(tfs)

    def test_each_result_has_timeframe_key(self, tmp_path):
        tfs = ["1h", "4h"]
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=tfs,
        )
        tf_names = [r["timeframe"] for r in result["timeframes"]]
        assert set(tf_names) == set(tfs)

    def test_each_result_is_dict(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h", "4h"],
        )
        for tr in result["timeframes"]:
            assert isinstance(tr, dict)


# ---------------------------------------------------------------------------
# 3. top_strategy per timeframe (str or None)
# ---------------------------------------------------------------------------

class TestTopStrategyPerTimeframe:
    def test_top_strategy_key_present(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h", "4h"],
        )
        for tr in result["timeframes"]:
            assert "top_strategy" in tr

    def test_top_strategy_is_str_or_none(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h", "4h"],
        )
        for tr in result["timeframes"]:
            assert tr["top_strategy"] is None or isinstance(tr["top_strategy"], str)

    def test_top_strategy_with_patched_data(self, tmp_path):
        tf_map = {
            "1h": _fake_comparison("1h", "trend_follow_lite",
                                   ["trend_follow_lite", "mean_reversion"]),
            "4h": _fake_comparison("4h", "mean_reversion",
                                   ["mean_reversion", "trend_follow_lite"]),
        }
        with _patch_run_comparison(tf_map):
            result = run_multi_timeframe_comparison(
                db_path=tmp_path / "x.sqlite",
                timeframes=["1h", "4h"],
            )
        tops = {tr["timeframe"]: tr["top_strategy"] for tr in result["timeframes"]}
        assert tops["1h"] == "trend_follow_lite"
        assert tops["4h"] == "mean_reversion"


# ---------------------------------------------------------------------------
# 4. Frequency count correct
# ---------------------------------------------------------------------------

class TestFrequencyCount:
    def test_frequency_correct(self, tmp_path):
        tf_map = {
            "1h": _fake_comparison("1h", "trend_follow_lite",
                                   ["trend_follow_lite", "mean_reversion"]),
            "4h": _fake_comparison("4h", "trend_follow_lite",
                                   ["trend_follow_lite", "mean_reversion"]),
            "1d": _fake_comparison("1d", "mean_reversion",
                                   ["mean_reversion", "trend_follow_lite"]),
        }
        with _patch_run_comparison(tf_map):
            result = run_multi_timeframe_comparison(
                db_path=tmp_path / "x.sqlite",
                timeframes=["1h", "4h", "1d"],
            )
        freq = result["summary"]["top_strategy_frequency"]
        assert freq["trend_follow_lite"] == 2
        assert freq["mean_reversion"] == 1

    def test_frequency_sums_to_count_with_data(self, tmp_path):
        tf_map = {
            "1h": _fake_comparison("1h", "mean_reversion", ["mean_reversion"]),
            "4h": _fake_comparison("4h", "mean_reversion", ["mean_reversion"]),
        }
        with _patch_run_comparison(tf_map):
            result = run_multi_timeframe_comparison(
                db_path=tmp_path / "x.sqlite",
                timeframes=["1h", "4h"],
            )
        assert sum(result["summary"]["top_strategy_frequency"].values()) == 2

    def test_build_frequency_direct(self):
        tf_results = [
            {"top_strategy": "a"},
            {"top_strategy": "b"},
            {"top_strategy": "a"},
            {"top_strategy": None},
        ]
        freq = _build_frequency(tf_results)
        assert freq["a"] == 2
        assert freq["b"] == 1
        assert None not in freq

    def test_build_frequency_all_none(self):
        assert _build_frequency([{"top_strategy": None}]) == {}

    def test_build_frequency_empty(self):
        assert _build_frequency([]) == {}


# ---------------------------------------------------------------------------
# 5. Empty dataset per timeframe → no crash
# ---------------------------------------------------------------------------

class TestEmptyDatasetPerTimeframe:
    def test_empty_db_no_crash(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "empty.sqlite",
            timeframes=["1h", "4h", "1d"],
        )
        assert isinstance(result, dict)

    def test_empty_db_ranking_length(self, tmp_path):
        # AC-125 runs all 3 strategies even on empty data → 3 entries, zeros
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "empty.sqlite",
            timeframes=["1h"],
        )
        ranking = result["timeframes"][0]["ranking"]
        assert isinstance(ranking, list)
        assert len(ranking) == 3

    def test_empty_db_top_strategy_is_str_or_none(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "empty.sqlite",
            timeframes=["1h"],
        )
        top = result["timeframes"][0]["top_strategy"]
        assert top is None or isinstance(top, str)


# ---------------------------------------------------------------------------
# 6. Output deterministic
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self, tmp_path):
        kwargs = dict(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h", "4h"],
            market="BTC-EUR",
        )
        r1 = run_multi_timeframe_comparison(**kwargs)
        r2 = run_multi_timeframe_comparison(**kwargs)
        assert r1["summary"] == r2["summary"]
        for t1, t2 in zip(r1["timeframes"], r2["timeframes"]):
            assert t1["timeframe"]    == t2["timeframe"]
            assert t1["top_strategy"] == t2["top_strategy"]
            assert t1["ranking"]      == t2["ranking"]


# ---------------------------------------------------------------------------
# 7. No pipeline files touched
# ---------------------------------------------------------------------------

class TestNoPipelineImpact:
    def test_no_ant_out_path_in_source(self):
        src = _REPO_ROOT / "ant_colony" / "research" / \
              "run_multi_timeframe_comparison_lite.py"
        content = src.read_text(encoding="utf-8")
        assert r"C:\Trading\ANT_OUT" not in content
        assert "Trading/ANT_OUT" not in content

    def test_no_execution_intent_in_source(self):
        src = _REPO_ROOT / "ant_colony" / "research" / \
              "run_multi_timeframe_comparison_lite.py"
        content = src.read_text(encoding="utf-8")
        assert "execution_intent" not in content
        assert "broker" not in content.lower()


# ---------------------------------------------------------------------------
# 8. timeframe_count in summary
# ---------------------------------------------------------------------------

class TestSummaryTimeframeCount:
    def test_timeframe_count_correct(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h", "4h", "1d"],
        )
        assert result["summary"]["timeframe_count"] == 3

    def test_timeframe_count_zero_empty_list(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=[],
        )
        assert result["summary"]["timeframe_count"] == 0

    def test_summary_has_frequency_key(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h"],
        )
        assert "top_strategy_frequency" in result["summary"]


# ---------------------------------------------------------------------------
# 9. Frequency excludes None
# ---------------------------------------------------------------------------

class TestFrequencyExcludesNone:
    def test_none_excluded_from_count(self, tmp_path):
        tf_map = {
            "1h": _fake_comparison("1h", "trend_follow_lite",
                                   ["trend_follow_lite"]),
        }
        with _patch_run_comparison(tf_map):
            result = run_multi_timeframe_comparison(
                db_path=tmp_path / "x.sqlite",
                timeframes=["1h", "4h"],  # 4h → no data → None
            )
        freq = result["summary"]["top_strategy_frequency"]
        assert sum(freq.values()) == 1


# ---------------------------------------------------------------------------
# 11. Ranking preserved per timeframe
# ---------------------------------------------------------------------------

class TestRankingPreserved:
    def test_ranking_order_preserved(self, tmp_path):
        ranking = ["mean_reversion", "trend_follow_lite", "volatility_breakout_lite"]
        tf_map = {"1h": _fake_comparison("1h", "mean_reversion", ranking)}
        with _patch_run_comparison(tf_map):
            result = run_multi_timeframe_comparison(
                db_path=tmp_path / "x.sqlite",
                timeframes=["1h"],
            )
        assert result["timeframes"][0]["ranking"] == ranking


# ---------------------------------------------------------------------------
# 12. strategies list present, no equity_curve
# ---------------------------------------------------------------------------

class TestStrategiesPerTimeframe:
    def test_strategies_key_present(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h"],
        )
        assert "strategies" in result["timeframes"][0]

    def test_strategies_is_list(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite",
            timeframes=["1h"],
        )
        assert isinstance(result["timeframes"][0]["strategies"], list)

    def test_no_equity_curve_in_strategies(self, tmp_path):
        tf_map = {
            "1h": _fake_comparison("1h", "mean_reversion",
                                   ["mean_reversion", "trend_follow_lite"]),
        }
        with _patch_run_comparison(tf_map):
            result = run_multi_timeframe_comparison(
                db_path=tmp_path / "x.sqlite",
                timeframes=["1h"],
            )
        for s in result["timeframes"][0]["strategies"]:
            assert "equity_curve" not in s


# ---------------------------------------------------------------------------
# 13. Flags correct
# ---------------------------------------------------------------------------

class TestFlags:
    def test_research_only_true(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite", timeframes=["1h"]
        )
        assert result["flags"]["research_only"] is True

    def test_pipeline_impact_false(self, tmp_path):
        result = run_multi_timeframe_comparison(
            db_path=tmp_path / "x.sqlite", timeframes=["1h"]
        )
        assert result["flags"]["pipeline_impact"] is False

    def test_flags_module_constant(self):
        assert FLAGS["research_only"] is True
        assert FLAGS["pipeline_impact"] is False


# ---------------------------------------------------------------------------
# 14. _build_frequency sorted desc by count, then alphabetical
# ---------------------------------------------------------------------------

class TestFrequencySorting:
    def test_sorted_descending_by_count(self):
        tf_results = [
            {"top_strategy": "z_strat"},
            {"top_strategy": "z_strat"},
            {"top_strategy": "z_strat"},
            {"top_strategy": "a_strat"},
            {"top_strategy": "a_strat"},
            {"top_strategy": "m_strat"},
        ]
        freq = _build_frequency(tf_results)
        keys = list(freq.keys())
        assert keys[0] == "z_strat"
        assert keys[1] == "a_strat"
        assert keys[2] == "m_strat"

    def test_alphabetical_tiebreak(self):
        tf_results = [
            {"top_strategy": "z_strat"},
            {"top_strategy": "a_strat"},
        ]
        keys = list(_build_frequency(tf_results).keys())
        assert keys[0] == "a_strat"
        assert keys[1] == "z_strat"
