"""
AC-135: Tests for Queen Candidate Decision Snapshot

Covers:
  1.  Build without crash
  2.  Missing input → FileNotFoundError
  3.  Empty / incomplete input → no crash
  4.  Highest allocation_weight → chosen timeframe
  5.  Tie → alphabetical timeframe wins
  6.  chosen_strategy correct passthrough
  7.  chosen_regime correct passthrough
  8.  chosen_allocation_weight correct passthrough
  9.  decision_context passthrough from allocation_summary
  10. rationale_summary exact
  11. JSON structure exact (all required keys)
  12. Flags correct
  13. Deterministic across multiple runs
  14. Missing allocation_weight → treated as 0.0 (no crash)
  15. Missing strategy/regime → no crash (fallback)
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

from ant_colony.research.build_queen_candidate_decision_snapshot_lite import (
    SNAPSHOT_VERSION,
    RATIONALE_SUMMARY,
    FLAGS,
    _choose,
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
    "candidate_decision", "decision_context", "rationale_summary", "flags",
}
CANDIDATE_KEYS = {
    "chosen_timeframe", "chosen_strategy", "chosen_regime", "chosen_allocation_weight",
}
CONTEXT_KEYS = {"dominant_strategy", "dominant_regime", "weights_sum"}


def _make_source(
    market: str = "BTC-EUR",
    timeframes: list[str] | None = None,
    alloc_per_tf: dict | None = None,
    alloc_summary: dict | None = None,
) -> dict:
    if timeframes is None:
        timeframes = ["1h", "4h", "1d"]
    if alloc_per_tf is None:
        alloc_per_tf = {
            "1h": {"selected_strategy": "volatility_breakout_lite",
                   "regime": "volatile_trend",
                   "base_weight": 1.0, "allocation_weight": 0.333333},
            "4h": {"selected_strategy": "mean_reversion",
                   "regime": "range",
                   "base_weight": 1.0, "allocation_weight": 0.333333},
            "1d": {"selected_strategy": "mean_reversion",
                   "regime": "range",
                   "base_weight": 1.0, "allocation_weight": 0.333333},
        }
    if alloc_summary is None:
        alloc_summary = {
            "dominant_strategy": "mean_reversion",
            "dominant_regime":   "range",
            "top_timeframe":     "1d",
            "weights_sum":       1.0,
        }
    return {
        "version": "regime_aware_allocation_summary_v1", "ts_utc": _TS,
        "market": market, "timeframes": timeframes,
        "allocation_per_timeframe": alloc_per_tf,
        "allocation_detail": [],
        "allocation_summary": alloc_summary,
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
        src, out = tmp_path / "in.json", tmp_path / "out.json"
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

    def test_invalid_json_raises_value_error(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json", encoding="utf-8")
        with pytest.raises(ValueError):
            _load_snapshot(bad)

    def test_build_and_write_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            build_and_write_snapshot(
                in_path=tmp_path / "missing.json",
                out_path=tmp_path / "out.json",
            )


# ---------------------------------------------------------------------------
# 3. Empty / incomplete input → no crash
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_empty_dict_no_crash(self):
        snap = build_snapshot({}, ts_utc=_TS)
        assert isinstance(snap, dict)

    def test_empty_timeframes_no_crash(self):
        snap = build_snapshot(
            _make_source(timeframes=[], alloc_per_tf={}, alloc_summary={}),
            ts_utc=_TS,
        )
        assert snap["timeframes"] == []
        assert snap["candidate_decision"]["chosen_timeframe"] is None

    def test_none_alloc_per_tf_no_crash(self):
        source = _make_source()
        source["allocation_per_timeframe"] = None
        snap = build_snapshot(source, ts_utc=_TS)
        assert isinstance(snap, dict)


# ---------------------------------------------------------------------------
# 4. Highest allocation_weight → chosen timeframe
# ---------------------------------------------------------------------------

class TestHighestWeightChosen:
    def test_highest_weight_wins(self):
        source = _make_source(
            timeframes=["1h", "4h"],
            alloc_per_tf={
                "1h": {"selected_strategy": "mean_reversion", "regime": "range",
                       "base_weight": 1.0, "allocation_weight": 0.667},
                "4h": {"selected_strategy": "mean_reversion", "regime": "range",
                       "base_weight": 0.5, "allocation_weight": 0.333},
            },
            alloc_summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["candidate_decision"]["chosen_timeframe"] == "1h"

    def test_choose_helper_highest(self):
        alloc = {
            "1h": {"allocation_weight": 0.6},
            "4h": {"allocation_weight": 0.4},
        }
        assert _choose(["1h", "4h"], alloc) == "1h"

    def test_choose_helper_none_empty(self):
        assert _choose([], {}) is None


# ---------------------------------------------------------------------------
# 5. Tie → alphabetical timeframe
# ---------------------------------------------------------------------------

class TestTieBreak:
    def test_tie_alphabetical(self):
        source = _make_source(
            timeframes=["1h", "1d", "4h"],
            alloc_per_tf={
                "1h": {"selected_strategy": "a", "regime": "range",
                       "base_weight": 1.0, "allocation_weight": 0.333},
                "1d": {"selected_strategy": "a", "regime": "range",
                       "base_weight": 1.0, "allocation_weight": 0.333},
                "4h": {"selected_strategy": "a", "regime": "range",
                       "base_weight": 1.0, "allocation_weight": 0.333},
            },
            alloc_summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["candidate_decision"]["chosen_timeframe"] == "1d"

    def test_choose_helper_tie(self):
        alloc = {"z_tf": {"allocation_weight": 0.5},
                 "a_tf": {"allocation_weight": 0.5}}
        assert _choose(["z_tf", "a_tf"], alloc) == "a_tf"


# ---------------------------------------------------------------------------
# 6–8. chosen_strategy / regime / weight passthrough
# ---------------------------------------------------------------------------

class TestCandidatePassthrough:
    def test_chosen_strategy(self):
        source = _make_source(
            timeframes=["1h"],
            alloc_per_tf={"1h": {"selected_strategy": "trend_follow_lite",
                                  "regime": "trend",
                                  "base_weight": 1.0, "allocation_weight": 1.0}},
            alloc_summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["candidate_decision"]["chosen_strategy"] == "trend_follow_lite"

    def test_chosen_regime(self):
        source = _make_source(
            timeframes=["1h"],
            alloc_per_tf={"1h": {"selected_strategy": "trend_follow_lite",
                                  "regime": "trend",
                                  "base_weight": 1.0, "allocation_weight": 1.0}},
            alloc_summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["candidate_decision"]["chosen_regime"] == "trend"

    def test_chosen_allocation_weight(self):
        source = _make_source(
            timeframes=["1h"],
            alloc_per_tf={"1h": {"selected_strategy": "mean_reversion",
                                  "regime": "range",
                                  "base_weight": 1.0, "allocation_weight": 0.75}},
            alloc_summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["candidate_decision"]["chosen_allocation_weight"] == 0.75


# ---------------------------------------------------------------------------
# 9. decision_context passthrough
# ---------------------------------------------------------------------------

class TestDecisionContext:
    def test_dominant_strategy_passthrough(self):
        source = _make_source(
            alloc_summary={"dominant_strategy": "alpha", "dominant_regime": "range",
                           "top_timeframe": "1h", "weights_sum": 1.0}
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["decision_context"]["dominant_strategy"] == "alpha"

    def test_dominant_regime_passthrough(self):
        source = _make_source(
            alloc_summary={"dominant_strategy": "alpha", "dominant_regime": "volatile_trend",
                           "top_timeframe": "1h", "weights_sum": 1.0}
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["decision_context"]["dominant_regime"] == "volatile_trend"

    def test_weights_sum_passthrough(self):
        source = _make_source(
            alloc_summary={"dominant_strategy": None, "dominant_regime": None,
                           "weights_sum": 0.99}
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["decision_context"]["weights_sum"] == 0.99

    def test_context_keys_present(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert CONTEXT_KEYS.issubset(snap["decision_context"])


# ---------------------------------------------------------------------------
# 10. rationale_summary exact
# ---------------------------------------------------------------------------

class TestRationaleSummary:
    def test_rationale_exact(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert snap["rationale_summary"] == RATIONALE_SUMMARY

    def test_rationale_constant(self):
        assert RATIONALE_SUMMARY["selection_basis"] == "highest_allocation_weight"
        assert RATIONALE_SUMMARY["tie_break"] == "alphabetical_timeframe"


# ---------------------------------------------------------------------------
# 11. JSON structure exact
# ---------------------------------------------------------------------------

class TestJsonStructure:
    def test_all_required_keys(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert REQUIRED_KEYS.issubset(snap.keys())

    def test_candidate_decision_keys(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert CANDIDATE_KEYS.issubset(snap["candidate_decision"])

    def test_valid_json_on_disk(self, tmp_path):
        out = tmp_path / "snap.json"
        write_snapshot(build_snapshot(_make_source(), ts_utc=_TS), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert REQUIRED_KEYS.issubset(data.keys())

    def test_version_correct(self):
        snap = build_snapshot(_make_source(), ts_utc=_TS)
        assert snap["version"] == SNAPSHOT_VERSION

    def test_version_constant(self):
        assert SNAPSHOT_VERSION == "queen_candidate_decision_snapshot_v1"


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
# 13. Deterministic
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
# 14. Missing allocation_weight → 0.0 (no crash)
# ---------------------------------------------------------------------------

class TestMissingWeight:
    def test_missing_weight_no_crash(self):
        source = _make_source(
            timeframes=["1h"],
            alloc_per_tf={"1h": {"selected_strategy": "mean_reversion",
                                  "regime": "range"}},  # no allocation_weight
            alloc_summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["candidate_decision"]["chosen_allocation_weight"] == 0.0

    def test_choose_missing_weight_treated_as_zero(self):
        alloc = {"1h": {}, "4h": {"allocation_weight": 0.6}}
        assert _choose(["1h", "4h"], alloc) == "4h"


# ---------------------------------------------------------------------------
# 15. Missing strategy/regime → no crash
# ---------------------------------------------------------------------------

class TestMissingStrategyRegime:
    def test_missing_strategy_no_crash(self):
        source = _make_source(
            timeframes=["1h"],
            alloc_per_tf={"1h": {"regime": "range", "allocation_weight": 1.0}},
            alloc_summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["candidate_decision"]["chosen_strategy"] is None

    def test_missing_regime_fallback_unknown(self):
        source = _make_source(
            timeframes=["1h"],
            alloc_per_tf={"1h": {"selected_strategy": "mean_reversion",
                                  "allocation_weight": 1.0}},
            alloc_summary={},
        )
        snap = build_snapshot(source, ts_utc=_TS)
        assert snap["candidate_decision"]["chosen_regime"] == "unknown"
