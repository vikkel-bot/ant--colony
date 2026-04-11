"""
AC-131: Tests for Strategy Selection Snapshot

Covers:
  1.  Build without crash
  2.  Missing input file → clean error, no stacktrace dump
  3.  Empty / incomplete source → no crash, valid output
  4.  selected_per_timeframe exact copy of top_per_timeframe
  5.  selection_frequency exact copy of frequency
  6.  selection_detail length matches timeframes
  7.  Timeframe order is deterministic
  8.  JSON structure exact (all required keys)
  9.  flags correct
  10. Deterministic behavior across multiple runs
  11. Output path created if absent
  12. No pipeline files touched
  13. version field correct
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

from ant_colony.research.build_strategy_selection_snapshot_lite import (
    SNAPSHOT_VERSION,
    FLAGS,
    build_snapshot,
    write_snapshot,
    load_comparison_snapshot,
    build_and_write_snapshot,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = "2025-01-01T00:00:00Z"

REQUIRED_KEYS = {
    "version", "ts_utc", "market", "timeframes",
    "selected_per_timeframe", "selection_frequency",
    "selection_detail", "flags",
}


def _make_source(
    market: str = "BTC-EUR",
    timeframes: list[str] | None = None,
    top_per_timeframe: dict | None = None,
    frequency: dict | None = None,
) -> dict:
    if timeframes is None:
        timeframes = ["1h", "4h", "1d"]
    if top_per_timeframe is None:
        top_per_timeframe = {
            "1h": "volatility_breakout_lite",
            "4h": "mean_reversion",
            "1d": "mean_reversion",
        }
    if frequency is None:
        frequency = {"mean_reversion": 2, "volatility_breakout_lite": 1}
    return {
        "version":          "multi_timeframe_comparison_snapshot_v1",
        "ts_utc":           _TS,
        "market":           market,
        "timeframes":       timeframes,
        "top_per_timeframe": top_per_timeframe,
        "frequency":        frequency,
        "timeframes_detail": [],
        "flags":            {"research_only": True, "pipeline_impact": False},
    }


def _write_source(source: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(source, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. Build without crash
# ---------------------------------------------------------------------------

class TestBuildNoCrash:
    def test_valid_source_no_crash(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_write_no_crash(self, tmp_path):
        out = tmp_path / "sel.json"
        write_snapshot(build_snapshot(_make_source(), ts_utc=_TS), out)
        assert out.exists()

    def test_build_and_write_no_crash(self, tmp_path):
        src_path = tmp_path / "in.json"
        out_path = tmp_path / "out.json"
        _write_source(_make_source(), src_path)
        snap = build_and_write_snapshot(in_path=src_path, out_path=out_path, ts_utc=_TS)
        assert isinstance(snap, dict)
        assert out_path.exists()


# ---------------------------------------------------------------------------
# 2. Missing input → FileNotFoundError (not silent crash)
# ---------------------------------------------------------------------------

class TestMissingInput:
    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_comparison_snapshot(tmp_path / "nonexistent.json")

    def test_build_and_write_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            build_and_write_snapshot(
                in_path=tmp_path / "nonexistent.json",
                out_path=tmp_path / "out.json",
            )

    def test_error_message_contains_path(self, tmp_path):
        missing = tmp_path / "nonexistent.json"
        with pytest.raises(FileNotFoundError) as exc_info:
            load_comparison_snapshot(missing)
        assert str(missing) in str(exc_info.value)


# ---------------------------------------------------------------------------
# 3. Empty / incomplete source → no crash
# ---------------------------------------------------------------------------

class TestEmptyOrIncompleteSource:
    def test_empty_dict_no_crash(self):
        snap = build_snapshot({}, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_empty_timeframes_no_crash(self):
        source = _make_source(timeframes=[], top_per_timeframe={}, frequency={})
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["timeframes"] == []
        assert snap["selected_per_timeframe"] == {}
        assert snap["selection_detail"] == []

    def test_none_top_per_timeframe_no_crash(self):
        source = _make_source()
        source["top_per_timeframe"] = None
        snap = build_snapshot(source, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_invalid_json_raises_value_error(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json", encoding="utf-8")
        with pytest.raises(ValueError):
            load_comparison_snapshot(bad)


# ---------------------------------------------------------------------------
# 4. selected_per_timeframe exact copy of top_per_timeframe
# ---------------------------------------------------------------------------

class TestSelectedPerTimeframe:
    def test_exact_copy(self):
        top = {"1h": "alpha", "4h": "beta", "1d": "alpha"}
        source = _make_source(top_per_timeframe=top)
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["selected_per_timeframe"] == top

    def test_none_values_preserved(self):
        top = {"1h": None, "4h": "beta"}
        source = _make_source(
            timeframes=["1h", "4h"],
            top_per_timeframe=top,
            frequency={"beta": 1},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["selected_per_timeframe"]["1h"] is None
        assert snap["selected_per_timeframe"]["4h"] == "beta"

    def test_key_present(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert "selected_per_timeframe" in snap


# ---------------------------------------------------------------------------
# 5. selection_frequency exact copy of frequency
# ---------------------------------------------------------------------------

class TestSelectionFrequency:
    def test_exact_copy(self):
        freq = {"mean_reversion": 3, "trend_follow_lite": 1}
        source = _make_source(frequency=freq)
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["selection_frequency"] == freq

    def test_empty_frequency_preserved(self):
        source = _make_source(timeframes=[], top_per_timeframe={}, frequency={})
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["selection_frequency"] == {}

    def test_key_present(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert "selection_frequency" in snap


# ---------------------------------------------------------------------------
# 6. selection_detail length matches timeframes
# ---------------------------------------------------------------------------

class TestSelectionDetailLength:
    def test_length_matches(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert len(snap["selection_detail"]) == 3

    def test_length_zero_empty_input(self):
        source = _make_source(timeframes=[], top_per_timeframe={}, frequency={})
        snap = build_snapshot(source, ts_utc=_TS)
        assert len(snap["selection_detail"]) == 0

    def test_detail_has_required_keys(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        for entry in snap["selection_detail"]:
            assert "timeframe" in entry
            assert "selected_strategy" in entry

    def test_detail_values_match_top_per_tf(self):
        top = {"1h": "alpha", "4h": "beta", "1d": "alpha"}
        source = _make_source(top_per_timeframe=top)
        snap = build_snapshot(source, ts_utc=_TS)
        for entry in snap["selection_detail"]:
            assert entry["selected_strategy"] == top[entry["timeframe"]]


# ---------------------------------------------------------------------------
# 7. Timeframe order deterministic
# ---------------------------------------------------------------------------

class TestTimeframeOrder:
    def test_order_preserved(self):
        source = _make_source(
            timeframes=["1h", "4h", "1d"],
            top_per_timeframe={"1h": "a", "4h": "b", "1d": "a"},
            frequency={"a": 2, "b": 1},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["timeframes"] == ["1h", "4h", "1d"]

    def test_detail_order_follows_timeframes(self):
        source = _make_source(
            timeframes=["1d", "1h", "4h"],
            top_per_timeframe={"1d": "a", "1h": "b", "4h": "a"},
            frequency={"a": 2, "b": 1},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        detail_tfs = [d["timeframe"] for d in snap["selection_detail"]]
        assert detail_tfs == ["1d", "1h", "4h"]


# ---------------------------------------------------------------------------
# 8. JSON structure exact
# ---------------------------------------------------------------------------

class TestJsonStructure:
    def test_all_required_keys(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert REQUIRED_KEYS.issubset(snap.keys())

    def test_valid_json_on_disk(self, tmp_path):
        out = tmp_path / "sel.json"
        write_snapshot(build_snapshot(_make_source(), ts_utc=_TS), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert REQUIRED_KEYS.issubset(data.keys())

    def test_market_preserved(self):
        snap = build_snapshot(_make_source(market="ETH-EUR"), ts_utc=_TS)
        assert snap["market"] == "ETH-EUR"


# ---------------------------------------------------------------------------
# 9. Flags correct
# ---------------------------------------------------------------------------

class TestFlags:
    def test_research_only_true(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert snap["flags"]["research_only"] is True

    def test_pipeline_impact_false(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert snap["flags"]["pipeline_impact"] is False

    def test_flags_module_constant(self):
        assert FLAGS["research_only"] is True
        assert FLAGS["pipeline_impact"] is False


# ---------------------------------------------------------------------------
# 10. Deterministic across multiple runs
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self):
        source = _make_source()
        s1 = build_snapshot(source, ts_utc=_TS)
        s2 = build_snapshot(source, ts_utc=_TS)
        assert s1 == s2

    def test_same_json_file(self, tmp_path):
        source = _make_source()
        out1, out2 = tmp_path / "s1.json", tmp_path / "s2.json"
        write_snapshot(build_snapshot(source, ts_utc=_TS), out1)
        write_snapshot(build_snapshot(source, ts_utc=_TS), out2)
        assert out1.read_text() == out2.read_text()

    def test_from_disk_deterministic(self, tmp_path):
        src_path = tmp_path / "in.json"
        _write_source(_make_source(), src_path)
        o1 = tmp_path / "o1.json"
        o2 = tmp_path / "o2.json"
        build_and_write_snapshot(in_path=src_path, out_path=o1, ts_utc=_TS)
        build_and_write_snapshot(in_path=src_path, out_path=o2, ts_utc=_TS)
        assert o1.read_text() == o2.read_text()


# ---------------------------------------------------------------------------
# 11. Output path created if absent
# ---------------------------------------------------------------------------

class TestDirectoryCreation:
    def test_nested_dirs_created(self, tmp_path):
        out = tmp_path / "deep" / "nested" / "sel.json"
        write_snapshot(build_snapshot(_make_source(), ts_utc=_TS), out)
        assert out.exists()

    def test_overwrite_no_error(self, tmp_path):
        out = tmp_path / "sel.json"
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        write_snapshot(snap, out)
        write_snapshot(snap, out)
        assert out.exists()


# ---------------------------------------------------------------------------
# 12. No pipeline files touched
# ---------------------------------------------------------------------------

class TestNoPipelineImpact:
    def test_no_ant_out_path(self):
        src = _REPO_ROOT / "ant_colony" / "research" / \
              "build_strategy_selection_snapshot_lite.py"
        content = src.read_text(encoding="utf-8")
        assert r"C:\Trading\ANT_OUT" not in content
        assert "Trading/ANT_OUT" not in content

    def test_no_execution_intent(self):
        src = _REPO_ROOT / "ant_colony" / "research" / \
              "build_strategy_selection_snapshot_lite.py"
        content = src.read_text(encoding="utf-8")
        assert "execution_intent" not in content
        assert "broker" not in content.lower()


# ---------------------------------------------------------------------------
# 13. Version field
# ---------------------------------------------------------------------------

class TestVersion:
    def test_version_present(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert "version" in snap

    def test_version_value(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert snap["version"] == SNAPSHOT_VERSION

    def test_version_constant(self):
        assert SNAPSHOT_VERSION == "strategy_selection_snapshot_v1"


# ---------------------------------------------------------------------------
# 14. ts_utc injection
# ---------------------------------------------------------------------------

class TestTsUtc:
    def test_ts_utc_injected(self):
        snap = build_snapshot(_make_source(), ts_utc="2025-06-01T12:00:00Z")
        assert snap["ts_utc"] == "2025-06-01T12:00:00Z"

    def test_ts_utc_auto_when_none(self):
        snap = build_snapshot(_make_source(), ts_utc=None)
        assert isinstance(snap["ts_utc"], str)
        assert len(snap["ts_utc"]) > 0


# ---------------------------------------------------------------------------
# 15. build_snapshot does not mutate input
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_source_not_mutated(self):
        source = _make_source()
        original_top = dict(source["top_per_timeframe"])
        original_freq = dict(source["frequency"])
        build_snapshot(source, ts_utc=_TS)
        assert source["top_per_timeframe"] == original_top
        assert source["frequency"] == original_freq
