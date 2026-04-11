"""
AC-133: Tests for Regime-Aware Strategy Selection Summary

Covers:
  1.  Build without crash
  2.  Missing AC-131 input → FileNotFoundError
  3.  Missing AC-132 input → FileNotFoundError
  4.  Empty / incomplete input → no crash
  5.  summary_per_timeframe combines strategy + regime correctly
  6.  strategy_frequency exact from AC-131 selection_frequency
  7.  regime_frequency exact from AC-132 regime_frequency
  8.  summary_detail length matches timeframes
  9.  Timeframe order deterministic
  10. dominant_strategy correct
  11. dominant_regime correct
  12. dominant_strategy tie → alphabetical
  13. dominant_regime tie → alphabetical
  14. Missing timeframe in regime snapshot → regime = "unknown"
  15. JSON structure exact (all required keys)
  16. Flags correct
  17. Deterministic across multiple runs
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

from ant_colony.research.build_regime_aware_strategy_selection_summary_lite import (
    SNAPSHOT_VERSION,
    FLAGS,
    _dominant,
    _load_snapshot,
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
    "summary_per_timeframe", "strategy_frequency", "regime_frequency",
    "summary_detail", "summary", "flags",
}


def _make_selection(
    market: str = "BTC-EUR",
    timeframes: list[str] | None = None,
    selected: dict | None = None,
    selection_freq: dict | None = None,
) -> dict:
    if timeframes is None:
        timeframes = ["1h", "4h", "1d"]
    if selected is None:
        selected = {"1h": "volatility_breakout_lite",
                    "4h": "mean_reversion", "1d": "mean_reversion"}
    if selection_freq is None:
        selection_freq = {"mean_reversion": 2, "volatility_breakout_lite": 1}
    return {
        "version": "strategy_selection_snapshot_v1", "ts_utc": _TS,
        "market": market, "timeframes": timeframes,
        "selected_per_timeframe": selected,
        "selection_frequency": selection_freq,
        "selection_detail": [], "flags": {},
    }


def _make_regime(
    timeframes: list[str] | None = None,
    annotated: dict | None = None,
    regime_freq: dict | None = None,
) -> dict:
    if timeframes is None:
        timeframes = ["1h", "4h", "1d"]
    if annotated is None:
        annotated = {
            "1h": {"selected_strategy": "volatility_breakout_lite",
                   "regime": "volatile_trend"},
            "4h": {"selected_strategy": "mean_reversion", "regime": "range"},
            "1d": {"selected_strategy": "mean_reversion", "regime": "range"},
        }
    if regime_freq is None:
        regime_freq = {"range": 2, "volatile_trend": 1}
    return {
        "version": "regime_annotation_snapshot_v1", "ts_utc": _TS,
        "market": "BTC-EUR", "timeframes": timeframes,
        "annotated_per_timeframe": annotated,
        "regime_frequency": regime_freq,
        "regime_detail": [], "regime_summary": {"dominant_regime": "range"},
        "flags": {},
    }


def _write_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. Build without crash
# ---------------------------------------------------------------------------

class TestBuildNoCrash:
    def test_valid_inputs_no_crash(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_write_no_crash(self, tmp_path):
        out = tmp_path / "snap.json"
        write_snapshot(build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS), out)
        assert out.exists()

    def test_build_and_write_no_crash(self, tmp_path):
        sel_path = tmp_path / "sel.json"
        reg_path = tmp_path / "reg.json"
        out_path = tmp_path / "out.json"
        _write_json(_make_selection(), sel_path)
        _write_json(_make_regime(), reg_path)
        snap = build_and_write_snapshot(sel_path, reg_path, out_path, ts_utc=_TS)
        assert isinstance(snap, dict)
        assert out_path.exists()


# ---------------------------------------------------------------------------
# 2. Missing AC-131 input → FileNotFoundError
# ---------------------------------------------------------------------------

class TestMissingSelectionInput:
    def test_missing_selection_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _load_snapshot(tmp_path / "nonexistent.json", "AC-131 selection")

    def test_build_and_write_missing_selection_raises(self, tmp_path):
        reg_path = tmp_path / "reg.json"
        _write_json(_make_regime(), reg_path)
        with pytest.raises(FileNotFoundError):
            build_and_write_snapshot(
                tmp_path / "missing.json", reg_path, tmp_path / "out.json"
            )

    def test_error_message_contains_path(self, tmp_path):
        missing = tmp_path / "sel.json"
        with pytest.raises(FileNotFoundError) as exc_info:
            _load_snapshot(missing, "AC-131 selection")
        assert str(missing) in str(exc_info.value)


# ---------------------------------------------------------------------------
# 3. Missing AC-132 input → FileNotFoundError
# ---------------------------------------------------------------------------

class TestMissingRegimeInput:
    def test_missing_regime_raises(self, tmp_path):
        sel_path = tmp_path / "sel.json"
        _write_json(_make_selection(), sel_path)
        with pytest.raises(FileNotFoundError):
            build_and_write_snapshot(
                sel_path, tmp_path / "missing.json", tmp_path / "out.json"
            )


# ---------------------------------------------------------------------------
# 4. Empty / incomplete input → no crash
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_empty_dicts_no_crash(self):
        snap = build_snapshot({}, {}, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_empty_timeframes_no_crash(self):
        snap = build_snapshot(
            _make_selection(timeframes=[], selected={}, selection_freq={}),
            _make_regime(timeframes=[], annotated={}, regime_freq={}),
            ts_utc=_TS,
        )
        assert snap["timeframes"] == []
        assert snap["summary_detail"] == []

    def test_none_frequencies_no_crash(self):
        sel = _make_selection()
        sel["selection_frequency"] = None
        reg = _make_regime()
        reg["regime_frequency"] = None
        snap = build_snapshot(sel, reg, ts_utc=_TS)
        assert snap["strategy_frequency"] == {}
        assert snap["regime_frequency"] == {}


# ---------------------------------------------------------------------------
# 5. summary_per_timeframe combines strategy + regime correctly
# ---------------------------------------------------------------------------

class TestSummaryPerTimeframe:
    def test_strategy_correct(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert snap["summary_per_timeframe"]["1h"]["selected_strategy"] == \
               "volatility_breakout_lite"
        assert snap["summary_per_timeframe"]["4h"]["selected_strategy"] == \
               "mean_reversion"

    def test_regime_correct(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert snap["summary_per_timeframe"]["1h"]["regime"] == "volatile_trend"
        assert snap["summary_per_timeframe"]["4h"]["regime"] == "range"

    def test_keys_match_timeframes(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert set(snap["summary_per_timeframe"].keys()) == {"1h", "4h", "1d"}


# ---------------------------------------------------------------------------
# 6. strategy_frequency from AC-131
# ---------------------------------------------------------------------------

class TestStrategyFrequency:
    def test_exact_passthrough(self):
        sel = _make_selection(selection_freq={"alpha": 3, "beta": 1})
        snap = build_snapshot(sel, _make_regime(), ts_utc=_TS)
        assert snap["strategy_frequency"] == {"alpha": 3, "beta": 1}

    def test_empty_preserved(self):
        sel = _make_selection(selection_freq={})
        snap = build_snapshot(sel, _make_regime(), ts_utc=_TS)
        assert snap["strategy_frequency"] == {}


# ---------------------------------------------------------------------------
# 7. regime_frequency from AC-132
# ---------------------------------------------------------------------------

class TestRegimeFrequency:
    def test_exact_passthrough(self):
        reg = _make_regime(regime_freq={"range": 5, "trend": 2})
        snap = build_snapshot(_make_selection(), reg, ts_utc=_TS)
        assert snap["regime_frequency"] == {"range": 5, "trend": 2}

    def test_empty_preserved(self):
        reg = _make_regime(regime_freq={})
        snap = build_snapshot(_make_selection(), reg, ts_utc=_TS)
        assert snap["regime_frequency"] == {}


# ---------------------------------------------------------------------------
# 8. summary_detail length
# ---------------------------------------------------------------------------

class TestSummaryDetailLength:
    def test_length_matches_timeframes(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert len(snap["summary_detail"]) == 3

    def test_detail_has_required_keys(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        for entry in snap["summary_detail"]:
            assert {"timeframe", "selected_strategy", "regime"}.issubset(entry)

    def test_detail_zero_for_empty_input(self):
        snap = build_snapshot(
            _make_selection(timeframes=[], selected={}, selection_freq={}),
            _make_regime(timeframes=[], annotated={}, regime_freq={}),
            ts_utc=_TS,
        )
        assert len(snap["summary_detail"]) == 0


# ---------------------------------------------------------------------------
# 9. Timeframe order deterministic
# ---------------------------------------------------------------------------

class TestTimeframeOrder:
    def test_order_preserved(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert snap["timeframes"] == ["1h", "4h", "1d"]

    def test_detail_follows_timeframe_order(self):
        sel = _make_selection(
            timeframes=["1d", "1h"],
            selected={"1d": "mean_reversion", "1h": "trend_follow_lite"},
            selection_freq={},
        )
        reg = _make_regime(
            timeframes=["1d", "1h"],
            annotated={"1d": {"regime": "range"}, "1h": {"regime": "trend"}},
            regime_freq={},
        )
        snap = build_snapshot(sel, reg, ts_utc=_TS)
        assert [d["timeframe"] for d in snap["summary_detail"]] == ["1d", "1h"]


# ---------------------------------------------------------------------------
# 10. dominant_strategy correct
# ---------------------------------------------------------------------------

class TestDominantStrategy:
    def test_dominant_strategy_correct(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert snap["summary"]["dominant_strategy"] == "mean_reversion"

    def test_dominant_strategy_none_when_empty(self):
        sel = _make_selection(selection_freq={})
        snap = build_snapshot(sel, _make_regime(), ts_utc=_TS)
        assert snap["summary"]["dominant_strategy"] is None


# ---------------------------------------------------------------------------
# 11. dominant_regime correct
# ---------------------------------------------------------------------------

class TestDominantRegime:
    def test_dominant_regime_correct(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert snap["summary"]["dominant_regime"] == "range"

    def test_dominant_regime_none_when_empty(self):
        reg = _make_regime(regime_freq={})
        snap = build_snapshot(_make_selection(), reg, ts_utc=_TS)
        assert snap["summary"]["dominant_regime"] is None


# ---------------------------------------------------------------------------
# 12. dominant_strategy tie → alphabetical
# ---------------------------------------------------------------------------

class TestDominantStrategyTie:
    def test_tie_alphabetical(self):
        sel = _make_selection(selection_freq={"zebra": 1, "alpha": 1})
        snap = build_snapshot(sel, _make_regime(), ts_utc=_TS)
        assert snap["summary"]["dominant_strategy"] == "alpha"

    def test_dominant_helper_tie(self):
        assert _dominant({"z": 2, "a": 2}) == "a"


# ---------------------------------------------------------------------------
# 13. dominant_regime tie → alphabetical
# ---------------------------------------------------------------------------

class TestDominantRegimeTie:
    def test_tie_alphabetical(self):
        reg = _make_regime(regime_freq={"volatile_trend": 1, "range": 1})
        snap = build_snapshot(_make_selection(), reg, ts_utc=_TS)
        assert snap["summary"]["dominant_regime"] == "range"

    def test_dominant_helper_empty(self):
        assert _dominant({}) is None


# ---------------------------------------------------------------------------
# 14. Missing timeframe in regime snapshot → regime = "unknown"
# ---------------------------------------------------------------------------

class TestMissingRegimeTimeframe:
    def test_missing_tf_in_regime_falls_back_to_unknown(self):
        sel = _make_selection(timeframes=["1h", "4h"],
                              selected={"1h": "mean_reversion", "4h": "trend_follow_lite"},
                              selection_freq={})
        reg = _make_regime(timeframes=["1h"],  # 4h missing
                           annotated={"1h": {"regime": "range"}},
                           regime_freq={})
        snap = build_snapshot(sel, reg, ts_utc=_TS)
        assert snap["summary_per_timeframe"]["4h"]["regime"] == "unknown"

    def test_none_annotated_value_falls_back_to_unknown(self):
        sel = _make_selection(timeframes=["1h"],
                              selected={"1h": "mean_reversion"},
                              selection_freq={})
        reg = _make_regime(timeframes=["1h"],
                           annotated={"1h": None},  # None entry
                           regime_freq={})
        snap = build_snapshot(sel, reg, ts_utc=_TS)
        assert snap["summary_per_timeframe"]["1h"]["regime"] == "unknown"


# ---------------------------------------------------------------------------
# 15. JSON structure exact
# ---------------------------------------------------------------------------

class TestJsonStructure:
    def test_all_required_keys(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert REQUIRED_KEYS.issubset(snap.keys())

    def test_valid_json_on_disk(self, tmp_path):
        out = tmp_path / "snap.json"
        write_snapshot(build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert REQUIRED_KEYS.issubset(data.keys())

    def test_summary_has_both_dominant_keys(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert "dominant_strategy" in snap["summary"]
        assert "dominant_regime"   in snap["summary"]

    def test_version_correct(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert snap["version"] == SNAPSHOT_VERSION

    def test_version_constant(self):
        assert SNAPSHOT_VERSION == "regime_aware_strategy_selection_summary_v1"


# ---------------------------------------------------------------------------
# 16. Flags correct
# ---------------------------------------------------------------------------

class TestFlags:
    def test_research_only_true(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert snap["flags"]["research_only"] is True

    def test_pipeline_impact_false(self):
        snap = build_snapshot(_make_selection(), _make_regime(), ts_utc=_TS)
        assert snap["flags"]["pipeline_impact"] is False

    def test_flags_module_constant(self):
        assert FLAGS["research_only"] is True
        assert FLAGS["pipeline_impact"] is False


# ---------------------------------------------------------------------------
# 17. Deterministic across multiple runs
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self):
        sel, reg = _make_selection(), _make_regime()
        s1 = build_snapshot(sel, reg, ts_utc=_TS)
        s2 = build_snapshot(sel, reg, ts_utc=_TS)
        assert s1 == s2

    def test_same_json_file(self, tmp_path):
        sel, reg = _make_selection(), _make_regime()
        out1, out2 = tmp_path / "s1.json", tmp_path / "s2.json"
        write_snapshot(build_snapshot(sel, reg, ts_utc=_TS), out1)
        write_snapshot(build_snapshot(sel, reg, ts_utc=_TS), out2)
        assert out1.read_text() == out2.read_text()

    def test_from_disk_deterministic(self, tmp_path):
        sel_path = tmp_path / "sel.json"
        reg_path = tmp_path / "reg.json"
        _write_json(_make_selection(), sel_path)
        _write_json(_make_regime(), reg_path)
        o1, o2 = tmp_path / "o1.json", tmp_path / "o2.json"
        build_and_write_snapshot(sel_path, reg_path, o1, ts_utc=_TS)
        build_and_write_snapshot(sel_path, reg_path, o2, ts_utc=_TS)
        assert o1.read_text() == o2.read_text()
