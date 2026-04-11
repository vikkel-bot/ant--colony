"""
AC-128: Tests for Multi-Market Comparison Snapshot

Covers:
  1.  Valid multi-market comparison → snapshot correctly written
  2.  market_count correct
  3.  top_strategy per market correct
  4.  frequency summary correct
  5.  Empty input → no crash
  6.  Flags correct
  7.  Output deterministic
  8.  Output path created if absent
  9.  No pipeline files touched
  10. Version field correct
  11. equity_curve stripped from strategy entries
  12. Empty markets list → snapshot written with empty markets
  13. top_strategy = None when ranking empty
  14. ts_utc injection
  15. build_snapshot does not mutate input
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Import resolution
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent
_RES_DIR   = _REPO_ROOT / "ant_colony" / "research"

for _p in (str(_REPO_ROOT), str(_RES_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from ant_colony.research.build_multi_market_comparison_snapshot_lite import (
    SNAPSHOT_VERSION,
    FLAGS,
    build_snapshot,
    write_snapshot,
    build_and_write_snapshot,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = "2025-01-01T00:00:00Z"


def _make_comparison(
    markets: list[dict] | None = None,
    exchange: str = "bitvavo",
    timeframe: str = "1h",
) -> dict:
    if markets is None:
        markets = [
            {
                "market": "BTC-EUR",
                "top_strategy": "alpha",
                "ranking": ["alpha", "beta", "gamma"],
                "strategies": [
                    {"name": "alpha", "trades": 10, "winrate": 0.6,
                     "total_return": 0.20, "max_drawdown": -0.10,
                     "equity_curve": [1.0, 1.2]},
                    {"name": "beta",  "trades": 5,  "winrate": 0.4,
                     "total_return": 0.05, "max_drawdown": -0.03,
                     "equity_curve": [1.0, 1.05]},
                    {"name": "gamma", "trades": 8,  "winrate": 0.5,
                     "total_return": 0.12, "max_drawdown": -0.07,
                     "equity_curve": [1.0, 1.12]},
                ],
            },
            {
                "market": "ETH-EUR",
                "top_strategy": "beta",
                "ranking": ["beta", "alpha", "gamma"],
                "strategies": [
                    {"name": "beta",  "trades": 7, "winrate": 0.57,
                     "total_return": 0.18, "max_drawdown": -0.08,
                     "equity_curve": [1.0, 1.18]},
                    {"name": "alpha", "trades": 4, "winrate": 0.5,
                     "total_return": 0.10, "max_drawdown": -0.05,
                     "equity_curve": [1.0, 1.10]},
                    {"name": "gamma", "trades": 3, "winrate": 0.33,
                     "total_return": 0.02, "max_drawdown": -0.02,
                     "equity_curve": [1.0, 1.02]},
                ],
            },
        ]
    return {
        "exchange":  exchange,
        "timeframe": timeframe,
        "markets":   markets,
        "summary": {
            "market_count": len(markets),
            "top_strategy_frequency": {"alpha": 1, "beta": 1},
        },
        "flags": {"research_only": True, "pipeline_impact": False},
    }


# ---------------------------------------------------------------------------
# 1. Valid comparison → snapshot correctly written
# ---------------------------------------------------------------------------

class TestValidComparison:
    def test_snapshot_written(self, tmp_path):
        out = tmp_path / "snap.json"
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        write_snapshot(snap, out)
        assert out.exists()

    def test_snapshot_is_valid_json(self, tmp_path):
        out = tmp_path / "snap.json"
        write_snapshot(build_snapshot(_make_comparison(), ts_utc=_TS), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert isinstance(data, dict)

    def test_exchange_preserved(self):
        snap = build_snapshot(_make_comparison(exchange="kraken"), ts_utc=_TS)
        assert snap["exchange"] == "kraken"

    def test_timeframe_preserved(self):
        snap = build_snapshot(_make_comparison(timeframe="4h"), ts_utc=_TS)
        assert snap["timeframe"] == "4h"


# ---------------------------------------------------------------------------
# 2. market_count correct
# ---------------------------------------------------------------------------

class TestMarketCount:
    def test_market_count_matches_input(self):
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_TS)
        assert snap["summary"]["market_count"] == len(comp["markets"])

    def test_market_count_zero_empty(self):
        snap = build_snapshot(_make_comparison(markets=[]), ts_utc=_TS)
        assert snap["summary"]["market_count"] == 0

    def test_markets_list_length_matches_count(self):
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_TS)
        assert len(snap["markets"]) == snap["summary"]["market_count"]


# ---------------------------------------------------------------------------
# 3. top_strategy per market correct
# ---------------------------------------------------------------------------

class TestTopStrategyPerMarket:
    def test_top_strategy_matches_ranking_first(self):
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_TS)
        for i, mr in enumerate(snap["markets"]):
            if mr["ranking"]:
                assert mr["top_strategy"] == mr["ranking"][0]

    def test_top_strategy_btc(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        btc = next(m for m in snap["markets"] if m["market"] == "BTC-EUR")
        assert btc["top_strategy"] == "alpha"

    def test_top_strategy_eth(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        eth = next(m for m in snap["markets"] if m["market"] == "ETH-EUR")
        assert eth["top_strategy"] == "beta"


# ---------------------------------------------------------------------------
# 4. Frequency summary correct
# ---------------------------------------------------------------------------

class TestFrequencySummary:
    def test_frequency_preserved_from_comparison(self):
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_TS)
        assert snap["summary"]["top_strategy_frequency"] == {"alpha": 1, "beta": 1}

    def test_frequency_key_present(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert "top_strategy_frequency" in snap["summary"]

    def test_frequency_empty_for_empty_markets(self):
        comp = _make_comparison(markets=[])
        comp["summary"]["top_strategy_frequency"] = {}
        snap = build_snapshot(comp, ts_utc=_TS)
        assert snap["summary"]["top_strategy_frequency"] == {}


# ---------------------------------------------------------------------------
# 5. Empty input → no crash
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_empty_markets_no_crash(self):
        snap = build_snapshot(_make_comparison(markets=[]), ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_none_markets_no_crash(self):
        comp = {"exchange": "x", "timeframe": "1h",
                "markets": None, "summary": {}, "flags": {}}
        snap = build_snapshot(comp, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_empty_dict_no_crash(self):
        snap = build_snapshot({}, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_build_and_write_missing_db_no_crash(self, tmp_path):
        out = tmp_path / "snap.json"
        snap = build_and_write_snapshot(
            db_path=tmp_path / "nonexistent.sqlite",
            markets=["BTC-EUR"],
            out_path=out,
            ts_utc=_TS,
        )
        assert isinstance(snap, dict)
        assert out.exists()

    def test_empty_strategies_per_market_no_crash(self):
        comp = _make_comparison(markets=[
            {"market": "BTC-EUR", "top_strategy": None,
             "ranking": [], "strategies": []},
        ])
        snap = build_snapshot(comp, ts_utc=_TS)
        assert snap["markets"][0]["top_strategy"] is None
        assert snap["markets"][0]["ranking"] == []


# ---------------------------------------------------------------------------
# 6. Flags correct
# ---------------------------------------------------------------------------

class TestFlags:
    def test_research_only_true(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert snap["flags"]["research_only"] is True

    def test_pipeline_impact_false(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert snap["flags"]["pipeline_impact"] is False

    def test_flags_module_constant(self):
        assert FLAGS["research_only"] is True
        assert FLAGS["pipeline_impact"] is False


# ---------------------------------------------------------------------------
# 7. Output deterministic
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_snapshot(self):
        comp = _make_comparison()
        s1 = build_snapshot(comp, ts_utc=_TS)
        s2 = build_snapshot(comp, ts_utc=_TS)
        assert s1 == s2

    def test_same_input_same_json(self, tmp_path):
        comp = _make_comparison()
        out1, out2 = tmp_path / "s1.json", tmp_path / "s2.json"
        write_snapshot(build_snapshot(comp, ts_utc=_TS), out1)
        write_snapshot(build_snapshot(comp, ts_utc=_TS), out2)
        assert out1.read_text() == out2.read_text()


# ---------------------------------------------------------------------------
# 8. Output path created if absent
# ---------------------------------------------------------------------------

class TestDirectoryCreation:
    def test_nested_dir_created(self, tmp_path):
        out = tmp_path / "deep" / "nested" / "snap.json"
        write_snapshot(build_snapshot(_make_comparison(), ts_utc=_TS), out)
        assert out.exists()

    def test_overwrite_no_error(self, tmp_path):
        out = tmp_path / "snap.json"
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        write_snapshot(snap, out)
        write_snapshot(snap, out)
        assert out.exists()


# ---------------------------------------------------------------------------
# 9. No pipeline files touched
# ---------------------------------------------------------------------------

class TestNoPipelineImpact:
    def test_no_ant_out_path_in_source(self):
        src = _REPO_ROOT / "ant_colony" / "research" / \
              "build_multi_market_comparison_snapshot_lite.py"
        content = src.read_text(encoding="utf-8")
        assert r"C:\Trading\ANT_OUT" not in content
        assert "Trading/ANT_OUT" not in content

    def test_no_execution_intent_in_source(self):
        src = _REPO_ROOT / "ant_colony" / "research" / \
              "build_multi_market_comparison_snapshot_lite.py"
        content = src.read_text(encoding="utf-8")
        assert "execution_intent" not in content
        assert "broker" not in content.lower()


# ---------------------------------------------------------------------------
# 10. Version field correct
# ---------------------------------------------------------------------------

class TestVersion:
    def test_version_present(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert "version" in snap

    def test_version_value(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert snap["version"] == SNAPSHOT_VERSION

    def test_version_constant(self):
        assert SNAPSHOT_VERSION == "multi_market_comparison_snapshot_v1"


# ---------------------------------------------------------------------------
# 11. equity_curve stripped
# ---------------------------------------------------------------------------

class TestEquityCurveStripped:
    def test_no_equity_curve_in_strategies(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        for mr in snap["markets"]:
            for s in mr["strategies"]:
                assert "equity_curve" not in s

    def test_input_equity_curve_still_present(self):
        comp = _make_comparison()
        build_snapshot(comp, ts_utc=_TS)
        for mr in comp["markets"]:
            for s in mr["strategies"]:
                assert "equity_curve" in s  # input not mutated


# ---------------------------------------------------------------------------
# 12. Empty markets list → empty snapshot
# ---------------------------------------------------------------------------

class TestEmptyMarketsList:
    def test_empty_markets_in_snapshot(self):
        snap = build_snapshot(_make_comparison(markets=[]), ts_utc=_TS)
        assert snap["markets"] == []

    def test_empty_markets_count_zero(self):
        snap = build_snapshot(_make_comparison(markets=[]), ts_utc=_TS)
        assert snap["summary"]["market_count"] == 0


# ---------------------------------------------------------------------------
# 13. top_strategy = None when ranking empty
# ---------------------------------------------------------------------------

class TestTopStrategyNull:
    def test_top_strategy_none_empty_ranking(self):
        comp = _make_comparison(markets=[
            {"market": "X-EUR", "top_strategy": None,
             "ranking": [], "strategies": []},
        ])
        snap = build_snapshot(comp, ts_utc=_TS)
        assert snap["markets"][0]["top_strategy"] is None

    def test_top_strategy_null_serializes(self, tmp_path):
        out = tmp_path / "snap.json"
        comp = _make_comparison(markets=[
            {"market": "X-EUR", "top_strategy": None,
             "ranking": [], "strategies": []},
        ])
        write_snapshot(build_snapshot(comp, ts_utc=_TS), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["markets"][0]["top_strategy"] is None


# ---------------------------------------------------------------------------
# 14. ts_utc injection
# ---------------------------------------------------------------------------

class TestTsUtc:
    def test_ts_utc_injected(self):
        snap = build_snapshot(_make_comparison(), ts_utc="2025-06-01T12:00:00Z")
        assert snap["ts_utc"] == "2025-06-01T12:00:00Z"

    def test_ts_utc_auto_when_none(self):
        snap = build_snapshot(_make_comparison(), ts_utc=None)
        assert isinstance(snap["ts_utc"], str)
        assert len(snap["ts_utc"]) > 0


# ---------------------------------------------------------------------------
# 15. build_snapshot does not mutate input
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_markets_not_mutated(self):
        comp = _make_comparison()
        original = [dict(m) for m in comp["markets"]]
        build_snapshot(comp, ts_utc=_TS)
        for orig, curr in zip(original, comp["markets"]):
            assert "equity_curve" in curr["strategies"][0]  # equity_curve still present
