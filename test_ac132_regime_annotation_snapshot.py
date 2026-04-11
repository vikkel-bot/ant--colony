"""
AC-132: Tests for Regime Annotation Snapshot

Covers:
  1.  Build without crash
  2.  Missing input file → FileNotFoundError, clean message
  3.  Empty / incomplete input → no crash
  4.  Strategy correctly mapped to regime
  5.  Unknown strategy → "unknown"
  6.  regime_frequency exact
  7.  regime_detail length matches timeframes
  8.  Timeframe order deterministic
  9.  dominant_regime correct
  10. dominant_regime tie → alphabetical
  11. JSON structure exact (all required keys)
  12. flags correct
  13. Deterministic across multiple runs
  14. version field correct
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

from ant_colony.research.build_regime_annotation_snapshot_lite import (
    SNAPSHOT_VERSION,
    STRATEGY_REGIME_MAP,
    FLAGS,
    _map_regime,
    _dominant_regime,
    build_snapshot,
    write_snapshot,
    load_selection_snapshot,
    build_and_write_snapshot,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = "2025-01-01T00:00:00Z"

REQUIRED_KEYS = {
    "version", "ts_utc", "market", "timeframes",
    "annotated_per_timeframe", "regime_frequency",
    "regime_detail", "regime_summary", "flags",
}


def _make_source(
    market: str = "BTC-EUR",
    timeframes: list[str] | None = None,
    selected: dict | None = None,
) -> dict:
    if timeframes is None:
        timeframes = ["1h", "4h", "1d"]
    if selected is None:
        selected = {
            "1h": "volatility_breakout_lite",
            "4h": "mean_reversion",
            "1d": "mean_reversion",
        }
    return {
        "version":               "strategy_selection_snapshot_v1",
        "ts_utc":                _TS,
        "market":                market,
        "timeframes":            timeframes,
        "selected_per_timeframe": selected,
        "selection_frequency":   {},
        "selection_detail":      [],
        "flags":                 {"research_only": True, "pipeline_impact": False},
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
        out = tmp_path / "snap.json"
        write_snapshot(build_snapshot(_make_source(), ts_utc=_TS), out)
        assert out.exists()

    def test_build_and_write_no_crash(self, tmp_path):
        src = tmp_path / "in.json"
        out = tmp_path / "out.json"
        _write_source(_make_source(), src)
        snap = build_and_write_snapshot(in_path=src, out_path=out, ts_utc=_TS)
        assert isinstance(snap, dict)
        assert out.exists()


# ---------------------------------------------------------------------------
# 2. Missing input → FileNotFoundError
# ---------------------------------------------------------------------------

class TestMissingInput:
    def test_missing_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_selection_snapshot(tmp_path / "nonexistent.json")

    def test_error_message_contains_path(self, tmp_path):
        missing = tmp_path / "nonexistent.json"
        with pytest.raises(FileNotFoundError) as exc_info:
            load_selection_snapshot(missing)
        assert str(missing) in str(exc_info.value)

    def test_build_and_write_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            build_and_write_snapshot(
                in_path=tmp_path / "nonexistent.json",
                out_path=tmp_path / "out.json",
            )

    def test_invalid_json_raises_value_error(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json", encoding="utf-8")
        with pytest.raises(ValueError):
            load_selection_snapshot(bad)


# ---------------------------------------------------------------------------
# 3. Empty / incomplete input → no crash
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_empty_dict_no_crash(self):
        snap = build_snapshot({}, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_empty_timeframes_no_crash(self):
        snap = build_snapshot(_make_source(timeframes=[], selected={}), ts_utc=_TS)
        assert snap["timeframes"] == []
        assert snap["regime_detail"] == []
        assert snap["annotated_per_timeframe"] == {}

    def test_none_selected_no_crash(self):
        source = _make_source()
        source["selected_per_timeframe"] = None
        snap = build_snapshot(source, ts_utc=_TS)
        assert isinstance(snap, dict)


# ---------------------------------------------------------------------------
# 4. Strategy correctly mapped to regime
# ---------------------------------------------------------------------------

class TestStrategyMapping:
    def test_mean_reversion_maps_to_range(self):
        assert _map_regime("mean_reversion") == "range"

    def test_volatility_breakout_lite_maps_to_volatile_trend(self):
        assert _map_regime("volatility_breakout_lite") == "volatile_trend"

    def test_trend_follow_lite_maps_to_trend(self):
        assert _map_regime("trend_follow_lite") == "trend"

    def test_trend_following_maps_to_trend(self):
        assert _map_regime("trend_following") == "trend"

    def test_breakout_maps_to_volatile_trend(self):
        assert _map_regime("breakout") == "volatile_trend"

    def test_momentum_maps_to_trend(self):
        assert _map_regime("momentum") == "trend"

    def test_annotated_regimes_correct(self):
        source = _make_source(
            timeframes=["1h", "4h"],
            selected={"1h": "volatility_breakout_lite", "4h": "mean_reversion"},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["annotated_per_timeframe"]["1h"]["regime"] == "volatile_trend"
        assert snap["annotated_per_timeframe"]["4h"]["regime"] == "range"

    def test_all_mapped_strategies_in_registry(self):
        for strategy in STRATEGY_REGIME_MAP:
            assert _map_regime(strategy) == STRATEGY_REGIME_MAP[strategy]


# ---------------------------------------------------------------------------
# 5. Unknown strategy → "unknown"
# ---------------------------------------------------------------------------

class TestUnknownStrategy:
    def test_unknown_name_returns_unknown(self):
        assert _map_regime("some_future_strategy") == "unknown"

    def test_none_returns_unknown(self):
        assert _map_regime(None) == "unknown"

    def test_empty_string_returns_unknown(self):
        assert _map_regime("") == "unknown"

    def test_unknown_in_snapshot_no_crash(self):
        source = _make_source(
            timeframes=["1h"],
            selected={"1h": "not_a_real_strategy"},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["annotated_per_timeframe"]["1h"]["regime"] == "unknown"


# ---------------------------------------------------------------------------
# 6. regime_frequency exact
# ---------------------------------------------------------------------------

class TestRegimeFrequency:
    def test_frequency_correct(self):
        source = _make_source(
            timeframes=["1h", "4h", "1d"],
            selected={
                "1h": "volatility_breakout_lite",  # volatile_trend
                "4h": "mean_reversion",             # range
                "1d": "mean_reversion",             # range
            },
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["regime_frequency"]["range"] == 2
        assert snap["regime_frequency"]["volatile_trend"] == 1

    def test_frequency_empty_for_empty_input(self):
        snap = build_snapshot(_make_source(timeframes=[], selected={}), ts_utc=_TS)
        assert snap["regime_frequency"] == {}

    def test_unknown_counted_in_frequency(self):
        source = _make_source(
            timeframes=["1h"],
            selected={"1h": "not_a_real_strategy"},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["regime_frequency"].get("unknown", 0) == 1


# ---------------------------------------------------------------------------
# 7. regime_detail length
# ---------------------------------------------------------------------------

class TestRegimeDetailLength:
    def test_length_matches_timeframes(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert len(snap["regime_detail"]) == 3

    def test_length_zero_empty_input(self):
        snap = build_snapshot(_make_source(timeframes=[], selected={}), ts_utc=_TS)
        assert len(snap["regime_detail"]) == 0

    def test_detail_has_required_keys(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        for entry in snap["regime_detail"]:
            assert "timeframe" in entry
            assert "selected_strategy" in entry
            assert "regime" in entry


# ---------------------------------------------------------------------------
# 8. Timeframe order deterministic
# ---------------------------------------------------------------------------

class TestTimeframeOrder:
    def test_order_preserved(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert snap["timeframes"] == ["1h", "4h", "1d"]

    def test_detail_order_follows_timeframes(self):
        source = _make_source(
            timeframes=["1d", "1h", "4h"],
            selected={"1d": "mean_reversion", "1h": "mean_reversion",
                      "4h": "trend_follow_lite"},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        detail_tfs = [d["timeframe"] for d in snap["regime_detail"]]
        assert detail_tfs == ["1d", "1h", "4h"]


# ---------------------------------------------------------------------------
# 9. dominant_regime correct
# ---------------------------------------------------------------------------

class TestDominantRegime:
    def test_dominant_is_most_frequent(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        # range: 2, volatile_trend: 1
        assert snap["regime_summary"]["dominant_regime"] == "range"

    def test_dominant_single_timeframe(self):
        source = _make_source(
            timeframes=["1h"],
            selected={"1h": "trend_follow_lite"},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["regime_summary"]["dominant_regime"] == "trend"

    def test_dominant_none_for_empty(self):
        snap = build_snapshot(_make_source(timeframes=[], selected={}), ts_utc=_TS)
        assert snap["regime_summary"]["dominant_regime"] is None


# ---------------------------------------------------------------------------
# 10. dominant_regime tie → alphabetical
# ---------------------------------------------------------------------------

class TestDominantTie:
    def test_tie_resolved_alphabetically(self):
        # range and volatile_trend each count 1
        source = _make_source(
            timeframes=["1h", "4h"],
            selected={"1h": "volatility_breakout_lite", "4h": "mean_reversion"},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        # "range" < "volatile_trend" alphabetically
        assert snap["regime_summary"]["dominant_regime"] == "range"

    def test_dominant_direct_helper(self):
        freq = {"volatile_trend": 1, "range": 1}
        assert _dominant_regime(freq) == "range"

    def test_dominant_empty_returns_none(self):
        assert _dominant_regime({}) is None


# ---------------------------------------------------------------------------
# 11. JSON structure exact
# ---------------------------------------------------------------------------

class TestJsonStructure:
    def test_all_required_keys(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert REQUIRED_KEYS.issubset(snap.keys())

    def test_valid_json_on_disk(self, tmp_path):
        out = tmp_path / "snap.json"
        write_snapshot(build_snapshot(_make_source(), ts_utc=_TS), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert REQUIRED_KEYS.issubset(data.keys())

    def test_market_preserved(self):
        snap = build_snapshot(_make_source(market="ETH-EUR"), ts_utc=_TS)
        assert snap["market"] == "ETH-EUR"

    def test_regime_summary_has_dominant(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert "dominant_regime" in snap["regime_summary"]


# ---------------------------------------------------------------------------
# 12. Flags correct
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
# 13. Deterministic across multiple runs
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
        src = tmp_path / "in.json"
        _write_source(_make_source(), src)
        o1, o2 = tmp_path / "o1.json", tmp_path / "o2.json"
        build_and_write_snapshot(in_path=src, out_path=o1, ts_utc=_TS)
        build_and_write_snapshot(in_path=src, out_path=o2, ts_utc=_TS)
        assert o1.read_text() == o2.read_text()


# ---------------------------------------------------------------------------
# 14. Version field
# ---------------------------------------------------------------------------

class TestVersion:
    def test_version_present(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert "version" in snap

    def test_version_value(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert snap["version"] == SNAPSHOT_VERSION

    def test_version_constant(self):
        assert SNAPSHOT_VERSION == "regime_annotation_snapshot_v1"


# ---------------------------------------------------------------------------
# 15. build_snapshot does not mutate input
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_source_not_mutated(self):
        source = _make_source()
        original_selected = dict(source["selected_per_timeframe"])
        original_tfs      = list(source["timeframes"])
        build_snapshot(source, ts_utc=_TS)
        assert source["selected_per_timeframe"] == original_selected
        assert source["timeframes"] == original_tfs
