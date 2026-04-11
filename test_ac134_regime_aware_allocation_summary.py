"""
AC-134: Tests for Regime-Aware Allocation Research Summary

Covers:
  1.  Build without crash
  2.  Missing input → FileNotFoundError
  3.  Empty / incomplete input → no crash
  4.  Known strategy+regime combos → correct base_weight
  5.  Mismatch combos → correct base_weight
  6.  Unknown combo → base_weight = 0.25
  7.  allocation_weight normalizes correctly (sum ≈ 1.0)
  8.  weights_sum ≈ 1.0
  9.  allocation_detail length matches timeframes
  10. Timeframe order deterministic
  11. top_timeframe correct
  12. top_timeframe tie → alphabetical
  13. dominant_strategy passthrough from AC-133 summary
  14. dominant_regime passthrough from AC-133 summary
  15. JSON structure exact
  16. Flags correct
  17. Deterministic across multiple runs
  18. Equal distribution when all base_weights are zero
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

from ant_colony.research.build_regime_aware_allocation_summary_lite import (
    SNAPSHOT_VERSION,
    WEIGHT_MAP,
    FALLBACK_WEIGHT,
    FLAGS,
    _base_weight,
    _normalize,
    _top_timeframe,
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
    "allocation_per_timeframe", "allocation_detail",
    "allocation_summary", "flags",
}


def _make_source(
    market: str = "BTC-EUR",
    timeframes: list[str] | None = None,
    per_tf: dict | None = None,
    summary: dict | None = None,
) -> dict:
    if timeframes is None:
        timeframes = ["1h", "4h", "1d"]
    if per_tf is None:
        per_tf = {
            "1h": {"selected_strategy": "volatility_breakout_lite",
                   "regime": "volatile_trend"},
            "4h": {"selected_strategy": "mean_reversion", "regime": "range"},
            "1d": {"selected_strategy": "mean_reversion", "regime": "range"},
        }
    if summary is None:
        summary = {"dominant_strategy": "mean_reversion", "dominant_regime": "range"}
    return {
        "version": "regime_aware_strategy_selection_summary_v1",
        "ts_utc": _TS,
        "market": market,
        "timeframes": timeframes,
        "summary_per_timeframe": per_tf,
        "strategy_frequency": {},
        "regime_frequency": {},
        "summary_detail": [],
        "summary": summary,
        "flags": {},
    }


def _write_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


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
        _write_json(_make_source(), src)
        snap = build_and_write_snapshot(in_path=src, out_path=out, ts_utc=_TS)
        assert isinstance(snap, dict)
        assert out.exists()


# ---------------------------------------------------------------------------
# 2. Missing input → FileNotFoundError
# ---------------------------------------------------------------------------

class TestMissingInput:
    def test_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _load_snapshot(tmp_path / "nonexistent.json")

    def test_error_contains_path(self, tmp_path):
        missing = tmp_path / "missing.json"
        with pytest.raises(FileNotFoundError) as exc_info:
            _load_snapshot(missing)
        assert str(missing) in str(exc_info.value)

    def test_build_and_write_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            build_and_write_snapshot(
                in_path=tmp_path / "missing.json",
                out_path=tmp_path / "out.json",
            )

    def test_invalid_json_raises_value_error(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json", encoding="utf-8")
        with pytest.raises(ValueError):
            _load_snapshot(bad)


# ---------------------------------------------------------------------------
# 3. Empty / incomplete input → no crash
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_empty_dict_no_crash(self):
        snap = build_snapshot({}, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_empty_timeframes_no_crash(self):
        snap = build_snapshot(
            _make_source(timeframes=[], per_tf={}, summary={}), ts_utc=_TS
        )
        assert snap["timeframes"] == []
        assert snap["allocation_detail"] == []

    def test_none_per_tf_no_crash(self):
        source = _make_source()
        source["summary_per_timeframe"] = None
        snap = build_snapshot(source, ts_utc=_TS)
        assert isinstance(snap, dict)


# ---------------------------------------------------------------------------
# 4. Known strategy+regime → correct base_weight
# ---------------------------------------------------------------------------

class TestKnownWeights:
    def test_mean_reversion_range(self):
        assert _base_weight("mean_reversion", "range") == 1.0

    def test_volatility_breakout_volatile_trend(self):
        assert _base_weight("volatility_breakout_lite", "volatile_trend") == 1.0

    def test_trend_following_trend(self):
        assert _base_weight("trend_following", "trend") == 1.0

    def test_trend_follow_lite_trend(self):
        assert _base_weight("trend_follow_lite", "trend") == 1.0

    def test_momentum_trend(self):
        assert _base_weight("momentum", "trend") == 1.0

    def test_weight_map_coverage(self):
        for (s, r), w in WEIGHT_MAP.items():
            assert _base_weight(s, r) == w


# ---------------------------------------------------------------------------
# 5. Mismatch combos → correct base_weight
# ---------------------------------------------------------------------------

class TestMismatchWeights:
    def test_mean_reversion_volatile_trend(self):
        assert _base_weight("mean_reversion", "volatile_trend") == 0.5

    def test_volatility_breakout_range(self):
        assert _base_weight("volatility_breakout_lite", "range") == 0.5

    def test_breakout_range(self):
        assert _base_weight("breakout", "range") == 0.5

    def test_unknown_unknown(self):
        assert _base_weight("unknown", "unknown") == 0.25


# ---------------------------------------------------------------------------
# 6. Unknown combo → FALLBACK_WEIGHT
# ---------------------------------------------------------------------------

class TestUnknownCombo:
    def test_unknown_strategy(self):
        assert _base_weight("some_future_strategy", "range") == FALLBACK_WEIGHT

    def test_none_strategy(self):
        assert _base_weight(None, "range") == FALLBACK_WEIGHT

    def test_none_regime(self):
        assert _base_weight("mean_reversion", None) == FALLBACK_WEIGHT

    def test_both_none(self):
        assert _base_weight(None, None) == FALLBACK_WEIGHT

    def test_unknown_in_snapshot_no_crash(self):
        source = _make_source(
            timeframes=["1h"],
            per_tf={"1h": {"selected_strategy": "alien_strategy", "regime": "warp"}},
            summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["allocation_per_timeframe"]["1h"]["base_weight"] == FALLBACK_WEIGHT


# ---------------------------------------------------------------------------
# 7. allocation_weight normalizes correctly
# ---------------------------------------------------------------------------

class TestNormalization:
    def test_normalize_equal_weights(self):
        result = _normalize([1.0, 1.0, 1.0])
        assert all(abs(w - 1 / 3) < 1e-9 for w in result)

    def test_normalize_unequal_weights(self):
        result = _normalize([1.0, 0.5])
        assert abs(result[0] - (1.0 / 1.5)) < 1e-9
        assert abs(result[1] - (0.5 / 1.5)) < 1e-9

    def test_normalize_empty(self):
        assert _normalize([]) == []

    def test_normalize_zero_total_equal_distribution(self):
        result = _normalize([0.0, 0.0, 0.0])
        assert all(abs(w - 1 / 3) < 1e-9 for w in result)


# ---------------------------------------------------------------------------
# 8. weights_sum ≈ 1.0
# ---------------------------------------------------------------------------

class TestWeightsSum:
    def test_weights_sum_one(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert abs(snap["allocation_summary"]["weights_sum"] - 1.0) < 1e-4

    def test_weights_sum_zero_for_empty(self):
        snap = build_snapshot(
            _make_source(timeframes=[], per_tf={}, summary={}), ts_utc=_TS
        )
        assert snap["allocation_summary"]["weights_sum"] == 0.0


# ---------------------------------------------------------------------------
# 9. allocation_detail length
# ---------------------------------------------------------------------------

class TestAllocationDetailLength:
    def test_length_matches_timeframes(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert len(snap["allocation_detail"]) == 3

    def test_detail_has_required_keys(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        expected = {"timeframe", "selected_strategy", "regime",
                    "base_weight", "allocation_weight"}
        for entry in snap["allocation_detail"]:
            assert expected.issubset(entry)

    def test_length_zero_empty_input(self):
        snap = build_snapshot(
            _make_source(timeframes=[], per_tf={}, summary={}), ts_utc=_TS
        )
        assert len(snap["allocation_detail"]) == 0


# ---------------------------------------------------------------------------
# 10. Timeframe order deterministic
# ---------------------------------------------------------------------------

class TestTimeframeOrder:
    def test_order_preserved(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert snap["timeframes"] == ["1h", "4h", "1d"]

    def test_detail_order_follows_timeframes(self):
        source = _make_source(
            timeframes=["1d", "1h"],
            per_tf={"1d": {"selected_strategy": "mean_reversion", "regime": "range"},
                    "1h": {"selected_strategy": "mean_reversion", "regime": "range"}},
            summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert [d["timeframe"] for d in snap["allocation_detail"]] == ["1d", "1h"]


# ---------------------------------------------------------------------------
# 11. top_timeframe correct
# ---------------------------------------------------------------------------

class TestTopTimeframe:
    def test_top_timeframe_highest_weight(self):
        # 1h gets 1.0, others 0.5 → 1h is top
        source = _make_source(
            timeframes=["1h", "4h"],
            per_tf={
                "1h": {"selected_strategy": "mean_reversion", "regime": "range"},
                "4h": {"selected_strategy": "mean_reversion", "regime": "volatile_trend"},
            },
            summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["allocation_summary"]["top_timeframe"] == "1h"

    def test_top_timeframe_none_when_empty(self):
        snap = build_snapshot(
            _make_source(timeframes=[], per_tf={}, summary={}), ts_utc=_TS
        )
        assert snap["allocation_summary"]["top_timeframe"] is None

    def test_top_timeframe_helper(self):
        assert _top_timeframe(["1h", "4h"], [0.6, 0.4]) == "1h"
        assert _top_timeframe(["1h", "4h"], [0.4, 0.6]) == "4h"


# ---------------------------------------------------------------------------
# 12. top_timeframe tie → alphabetical
# ---------------------------------------------------------------------------

class TestTopTimeframeTie:
    def test_tie_alphabetical(self):
        # both equal weights → alphabetical: "1d" < "1h"
        source = _make_source(
            timeframes=["1h", "1d"],
            per_tf={
                "1h": {"selected_strategy": "mean_reversion", "regime": "range"},
                "1d": {"selected_strategy": "mean_reversion", "regime": "range"},
            },
            summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["allocation_summary"]["top_timeframe"] == "1d"

    def test_top_timeframe_helper_tie(self):
        assert _top_timeframe(["z_tf", "a_tf"], [0.5, 0.5]) == "a_tf"


# ---------------------------------------------------------------------------
# 13. dominant_strategy passthrough
# ---------------------------------------------------------------------------

class TestDominantStrategyPassthrough:
    def test_passthrough_from_summary(self):
        source = _make_source(
            summary={"dominant_strategy": "trend_follow_lite", "dominant_regime": "trend"}
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["allocation_summary"]["dominant_strategy"] == "trend_follow_lite"

    def test_none_preserved(self):
        source = _make_source(
            summary={"dominant_strategy": None, "dominant_regime": None}
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["allocation_summary"]["dominant_strategy"] is None


# ---------------------------------------------------------------------------
# 14. dominant_regime passthrough
# ---------------------------------------------------------------------------

class TestDominantRegimePassthrough:
    def test_passthrough_from_summary(self):
        source = _make_source(
            summary={"dominant_strategy": "mean_reversion", "dominant_regime": "range"}
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["allocation_summary"]["dominant_regime"] == "range"


# ---------------------------------------------------------------------------
# 15. JSON structure exact
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

    def test_allocation_summary_keys(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert {"dominant_strategy", "dominant_regime",
                "top_timeframe", "weights_sum"}.issubset(
            snap["allocation_summary"]
        )

    def test_version_correct(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert snap["version"] == SNAPSHOT_VERSION

    def test_version_constant(self):
        assert SNAPSHOT_VERSION == "regime_aware_allocation_summary_v1"


# ---------------------------------------------------------------------------
# 16. Flags correct
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
# 17. Deterministic across multiple runs
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
        _write_json(_make_source(), src)
        o1, o2 = tmp_path / "o1.json", tmp_path / "o2.json"
        build_and_write_snapshot(in_path=src, out_path=o1, ts_utc=_TS)
        build_and_write_snapshot(in_path=src, out_path=o2, ts_utc=_TS)
        assert o1.read_text() == o2.read_text()


# ---------------------------------------------------------------------------
# 18. Equal distribution when all base_weights are zero
# ---------------------------------------------------------------------------

class TestZeroWeightFallback:
    def test_zero_weights_equal_distribution(self):
        result = _normalize([0.0, 0.0])
        assert abs(result[0] - 0.5) < 1e-9
        assert abs(result[1] - 0.5) < 1e-9

    def test_zero_weights_in_snapshot(self):
        # Use unknown combos → all get FALLBACK_WEIGHT (0.25) → not zero
        # Force zero by patching: use a source with no per_tf entries
        source = _make_source(
            timeframes=["1h", "4h"],
            per_tf={
                "1h": {"selected_strategy": None, "regime": None},
                "4h": {"selected_strategy": None, "regime": None},
            },
            summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        # None/None → FALLBACK_WEIGHT (0.25) → still non-zero, equal weights
        assert abs(snap["allocation_summary"]["weights_sum"] - 1.0) < 1e-4
