"""
AC-130: Tests for Multi-Timeframe Comparison Snapshot

Covers:
  1.  Build without crash
  2.  Empty data → no crash
  3.  top_per_timeframe correct
  4.  frequency exactly preserved from AC-129
  5.  timeframes_detail length matches input
  6.  Deterministic behavior
  7.  JSON structure exact (all required keys present)
  8.  Output path created if absent
  9.  No pipeline files touched
  10. top_strategy None when ranking empty
  11. timeframes list matches input order
  12. timeframes_detail metrics come from top strategy
  13. ts_utc injection
  14. build_snapshot does not mutate input
  15. version field correct
"""
from __future__ import annotations

import json
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

from ant_colony.research.build_multi_timeframe_comparison_snapshot_lite import (
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

REQUIRED_KEYS = {
    "version", "ts_utc", "market", "timeframes",
    "top_per_timeframe", "frequency", "timeframes_detail", "flags",
}


def _make_comparison(
    market: str = "BTC-EUR",
    tf_results: list[dict] | None = None,
    frequency: dict | None = None,
) -> dict:
    if tf_results is None:
        tf_results = [
            {
                "timeframe": "1h",
                "top_strategy": "alpha",
                "ranking": ["alpha", "beta"],
                "strategies": [
                    {"name": "alpha", "trades": 10, "winrate": 0.6,
                     "total_return": 0.20, "max_drawdown": -0.10,
                     "equity_curve": [1.0, 1.2]},
                    {"name": "beta", "trades": 5, "winrate": 0.4,
                     "total_return": 0.05, "max_drawdown": -0.03,
                     "equity_curve": [1.0, 1.05]},
                ],
            },
            {
                "timeframe": "4h",
                "top_strategy": "beta",
                "ranking": ["beta", "alpha"],
                "strategies": [
                    {"name": "beta", "trades": 7, "winrate": 0.57,
                     "total_return": 0.18, "max_drawdown": -0.08,
                     "equity_curve": [1.0, 1.18]},
                    {"name": "alpha", "trades": 3, "winrate": 0.33,
                     "total_return": 0.02, "max_drawdown": -0.02,
                     "equity_curve": [1.0, 1.02]},
                ],
            },
            {
                "timeframe": "1d",
                "top_strategy": "alpha",
                "ranking": ["alpha", "beta"],
                "strategies": [
                    {"name": "alpha", "trades": 2, "winrate": 0.5,
                     "total_return": 0.08, "max_drawdown": -0.04,
                     "equity_curve": [1.0, 1.08]},
                    {"name": "beta", "trades": 1, "winrate": 0.0,
                     "total_return": -0.01, "max_drawdown": -0.01,
                     "equity_curve": [1.0, 0.99]},
                ],
            },
        ]
    if frequency is None:
        frequency = {"alpha": 2, "beta": 1}
    return {
        "exchange":  "bitvavo",
        "market":    market,
        "timeframes": tf_results,
        "summary": {
            "timeframe_count":        len(tf_results),
            "top_strategy_frequency": frequency,
        },
        "flags": {"research_only": True, "pipeline_impact": False},
    }


# ---------------------------------------------------------------------------
# 1. Build without crash
# ---------------------------------------------------------------------------

class TestBuildNoCrash:
    def test_valid_input_no_crash(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_write_no_crash(self, tmp_path):
        out = tmp_path / "snap.json"
        write_snapshot(build_snapshot(_make_comparison(), ts_utc=_TS), out)
        assert out.exists()

    def test_build_and_write_no_crash(self, tmp_path):
        snap = build_and_write_snapshot(
            db_path=tmp_path / "nonexistent.sqlite",
            out_path=tmp_path / "snap.json",
            ts_utc=_TS,
        )
        assert isinstance(snap, dict)


# ---------------------------------------------------------------------------
# 2. Empty data → no crash
# ---------------------------------------------------------------------------

class TestEmptyData:
    def test_empty_tf_results_no_crash(self):
        comp = _make_comparison(tf_results=[], frequency={})
        snap = build_snapshot(comp, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_empty_dict_no_crash(self):
        snap = build_snapshot({}, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_none_timeframes_no_crash(self):
        comp = {"market": "BTC-EUR", "timeframes": None,
                "summary": {}, "flags": {}}
        snap = build_snapshot(comp, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_empty_tf_results_empty_lists(self):
        snap = build_snapshot(_make_comparison(tf_results=[], frequency={}), ts_utc=_TS)
        assert snap["timeframes"] == []
        assert snap["top_per_timeframe"] == {}
        assert snap["timeframes_detail"] == []


# ---------------------------------------------------------------------------
# 3. top_per_timeframe correct
# ---------------------------------------------------------------------------

class TestTopPerTimeframe:
    def test_keys_match_timeframes(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert set(snap["top_per_timeframe"].keys()) == {"1h", "4h", "1d"}

    def test_values_correct(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert snap["top_per_timeframe"]["1h"] == "alpha"
        assert snap["top_per_timeframe"]["4h"] == "beta"
        assert snap["top_per_timeframe"]["1d"] == "alpha"

    def test_none_when_no_top_strategy(self):
        tf_results = [
            {"timeframe": "1h", "top_strategy": None,
             "ranking": [], "strategies": []},
        ]
        comp = _make_comparison(tf_results=tf_results, frequency={})
        snap = build_snapshot(comp, ts_utc=_TS)
        assert snap["top_per_timeframe"]["1h"] is None


# ---------------------------------------------------------------------------
# 4. frequency exactly preserved
# ---------------------------------------------------------------------------

class TestFrequencyPreserved:
    def test_frequency_matches_input(self):
        comp = _make_comparison(frequency={"alpha": 2, "beta": 1})
        snap = build_snapshot(comp, ts_utc=_TS)
        assert snap["frequency"] == {"alpha": 2, "beta": 1}

    def test_frequency_empty_preserved(self):
        comp = _make_comparison(tf_results=[], frequency={})
        snap = build_snapshot(comp, ts_utc=_TS)
        assert snap["frequency"] == {}

    def test_frequency_key_present(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert "frequency" in snap


# ---------------------------------------------------------------------------
# 5. timeframes_detail length
# ---------------------------------------------------------------------------

class TestTimeframesDetailLength:
    def test_length_matches_input(self):
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_TS)
        assert len(snap["timeframes_detail"]) == 3

    def test_length_zero_empty_input(self):
        snap = build_snapshot(_make_comparison(tf_results=[], frequency={}), ts_utc=_TS)
        assert len(snap["timeframes_detail"]) == 0

    def test_detail_has_required_keys(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        expected = {"timeframe", "top_strategy", "trades",
                    "winrate", "total_return", "max_drawdown"}
        for entry in snap["timeframes_detail"]:
            assert expected.issubset(entry.keys())


# ---------------------------------------------------------------------------
# 6. Deterministic behavior
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self):
        comp = _make_comparison()
        s1 = build_snapshot(comp, ts_utc=_TS)
        s2 = build_snapshot(comp, ts_utc=_TS)
        assert s1 == s2

    def test_same_json_output(self, tmp_path):
        comp = _make_comparison()
        out1, out2 = tmp_path / "s1.json", tmp_path / "s2.json"
        write_snapshot(build_snapshot(comp, ts_utc=_TS), out1)
        write_snapshot(build_snapshot(comp, ts_utc=_TS), out2)
        assert out1.read_text() == out2.read_text()


# ---------------------------------------------------------------------------
# 7. JSON structure exact
# ---------------------------------------------------------------------------

class TestJsonStructure:
    def test_all_required_keys_present(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert REQUIRED_KEYS.issubset(snap.keys())

    def test_valid_json_on_disk(self, tmp_path):
        out = tmp_path / "snap.json"
        write_snapshot(build_snapshot(_make_comparison(), ts_utc=_TS), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert REQUIRED_KEYS.issubset(data.keys())

    def test_flags_in_json(self, tmp_path):
        out = tmp_path / "snap.json"
        write_snapshot(build_snapshot(_make_comparison(), ts_utc=_TS), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["flags"]["research_only"] is True
        assert data["flags"]["pipeline_impact"] is False

    def test_no_equity_curve_in_detail(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        for entry in snap["timeframes_detail"]:
            assert "equity_curve" not in entry


# ---------------------------------------------------------------------------
# 8. Output path created if absent
# ---------------------------------------------------------------------------

class TestDirectoryCreation:
    def test_nested_dir_created(self, tmp_path):
        out = tmp_path / "a" / "b" / "snap.json"
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
    def test_no_ant_out_path(self):
        src = _REPO_ROOT / "ant_colony" / "research" / \
              "build_multi_timeframe_comparison_snapshot_lite.py"
        content = src.read_text(encoding="utf-8")
        assert r"C:\Trading\ANT_OUT" not in content
        assert "Trading/ANT_OUT" not in content

    def test_no_execution_intent(self):
        src = _REPO_ROOT / "ant_colony" / "research" / \
              "build_multi_timeframe_comparison_snapshot_lite.py"
        content = src.read_text(encoding="utf-8")
        assert "execution_intent" not in content
        assert "broker" not in content.lower()


# ---------------------------------------------------------------------------
# 10. top_strategy None when ranking empty
# ---------------------------------------------------------------------------

class TestTopStrategyNullSerialization:
    def test_null_in_top_per_timeframe(self):
        tf_results = [
            {"timeframe": "1h", "top_strategy": None,
             "ranking": [], "strategies": []},
        ]
        snap = build_snapshot(
            _make_comparison(tf_results=tf_results, frequency={}), ts_utc=_TS
        )
        assert snap["top_per_timeframe"]["1h"] is None

    def test_null_serializes_correctly(self, tmp_path):
        tf_results = [
            {"timeframe": "1h", "top_strategy": None,
             "ranking": [], "strategies": []},
        ]
        out = tmp_path / "snap.json"
        write_snapshot(
            build_snapshot(
                _make_comparison(tf_results=tf_results, frequency={}), ts_utc=_TS
            ),
            out,
        )
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["top_per_timeframe"]["1h"] is None


# ---------------------------------------------------------------------------
# 11. timeframes list matches input order
# ---------------------------------------------------------------------------

class TestTimeframesOrder:
    def test_order_preserved(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert snap["timeframes"] == ["1h", "4h", "1d"]

    def test_market_preserved(self):
        snap = build_snapshot(_make_comparison(market="ETH-EUR"), ts_utc=_TS)
        assert snap["market"] == "ETH-EUR"


# ---------------------------------------------------------------------------
# 12. timeframes_detail metrics from top strategy
# ---------------------------------------------------------------------------

class TestDetailMetrics:
    def test_detail_1h_metrics(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        detail_1h = next(d for d in snap["timeframes_detail"]
                         if d["timeframe"] == "1h")
        # top for 1h is "alpha" with trades=10, winrate=0.6
        assert detail_1h["top_strategy"] == "alpha"
        assert detail_1h["trades"] == 10
        assert detail_1h["winrate"] == 0.6
        assert detail_1h["total_return"] == 0.20
        assert detail_1h["max_drawdown"] == -0.10

    def test_detail_4h_metrics(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        detail_4h = next(d for d in snap["timeframes_detail"]
                         if d["timeframe"] == "4h")
        assert detail_4h["top_strategy"] == "beta"
        assert detail_4h["trades"] == 7

    def test_detail_metrics_zero_when_no_top(self):
        tf_results = [
            {"timeframe": "1h", "top_strategy": None,
             "ranking": [], "strategies": []},
        ]
        snap = build_snapshot(
            _make_comparison(tf_results=tf_results, frequency={}), ts_utc=_TS
        )
        detail = snap["timeframes_detail"][0]
        assert detail["trades"] == 0
        assert detail["winrate"] == 0.0


# ---------------------------------------------------------------------------
# 13. ts_utc injection
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
# 14. build_snapshot does not mutate input
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_input_not_mutated(self):
        comp = _make_comparison()
        original_tfs = [dict(t) for t in comp["timeframes"]]
        build_snapshot(comp, ts_utc=_TS)
        for orig, curr in zip(original_tfs, comp["timeframes"]):
            assert curr["timeframe"] == orig["timeframe"]
            assert "equity_curve" in curr["strategies"][0]


# ---------------------------------------------------------------------------
# 15. version field correct
# ---------------------------------------------------------------------------

class TestVersion:
    def test_version_present(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert "version" in snap

    def test_version_value(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_TS)
        assert snap["version"] == SNAPSHOT_VERSION

    def test_version_constant(self):
        assert SNAPSHOT_VERSION == "multi_timeframe_comparison_snapshot_v1"
