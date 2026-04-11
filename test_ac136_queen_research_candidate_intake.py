"""
AC-136: Tests for Queen Research Candidate Intake

Covers:
  1.  Valid, fresh snapshot → CANDIDATE_ACTIVE
  2.  research_only=True always (ACTIVE, HOLD, INVALID)
  3.  Missing snapshot file → FileNotFoundError from load_candidate_snapshot
  4.  Bad JSON → ValueError from load_candidate_snapshot
  5.  load_and_consume missing file → CANDIDATE_INVALID (no raise)
  6.  load_and_consume bad JSON → CANDIDATE_INVALID (no raise)
  7.  Empty dict → CANDIDATE_INVALID
  8.  Missing required top-level key → CANDIDATE_INVALID
  9.  Version mismatch → CANDIDATE_INVALID
  10. Unparseable ts_utc → CANDIDATE_INVALID
  11. Stale snapshot → CANDIDATE_HOLD
  12. chosen_timeframe=None → CANDIDATE_HOLD
  13. All output keys present (ACTIVE)
  14. All output keys present (HOLD / INVALID)
  15. Passthrough: chosen_timeframe / strategy / regime / weight when ACTIVE
  16. Passthrough: dominant_strategy / dominant_regime / weights_sum when ACTIVE
  17. Deterministic: same snapshot → same result
  18. candidate_decision not a dict → CANDIDATE_INVALID
  19. candidate_decision missing key → CANDIDATE_INVALID
  20. Just-fresh (age = max_age_hours - 1 min) → CANDIDATE_ACTIVE
  21. Just-stale (age = max_age_hours + 1 min) → CANDIDATE_HOLD
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Import resolution
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.queen_research_candidate_intake_lite import (
    VERSION,
    EXPECTED_SNAPSHOT_VERSION,
    DEFAULT_MAX_AGE_HOURS,
    CANDIDATE_ACTIVE,
    CANDIDATE_HOLD,
    CANDIDATE_INVALID,
    REASON_CODES,
    load_candidate_snapshot,
    consume_candidate,
    load_and_consume,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
_FRESH_TS   = "2025-06-01T11:00:00Z"   # 1 h ago  — well within 24 h
_STALE_TS   = "2025-05-30T12:00:00Z"   # 48 h ago — stale


def _make_snapshot(
    ts_utc: str = _FRESH_TS,
    chosen_timeframe: str | None = "1h",
    chosen_strategy: str | None = "mean_reversion",
    chosen_regime: str = "range",
    chosen_weight: float = 0.5,
    version: str = EXPECTED_SNAPSHOT_VERSION,
    dominant_strategy: str | None = "mean_reversion",
    dominant_regime: str | None = "range",
    weights_sum: float = 1.0,
) -> dict:
    return {
        "version":   version,
        "ts_utc":    ts_utc,
        "market":    "BTC-EUR",
        "timeframes": ["1h", "4h", "1d"],
        "candidate_decision": {
            "chosen_timeframe":         chosen_timeframe,
            "chosen_strategy":          chosen_strategy,
            "chosen_regime":            chosen_regime,
            "chosen_allocation_weight": chosen_weight,
        },
        "decision_context": {
            "dominant_strategy": dominant_strategy,
            "dominant_regime":   dominant_regime,
            "weights_sum":       weights_sum,
        },
        "rationale_summary": {
            "selection_basis": "highest_allocation_weight",
            "tie_break":       "alphabetical_timeframe",
        },
        "flags": {"research_only": True, "pipeline_impact": False},
    }


def _write_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


_OUTPUT_KEYS = {
    "intake_status", "intake_valid", "intake_reason", "intake_reason_code",
    "chosen_timeframe", "chosen_strategy", "chosen_regime", "chosen_allocation_weight",
    "dominant_strategy", "dominant_regime", "weights_sum",
    "snapshot_ts_utc", "snapshot_market", "research_only",
}


# ---------------------------------------------------------------------------
# 1. Valid, fresh → CANDIDATE_ACTIVE
# ---------------------------------------------------------------------------

class TestActive:
    def test_valid_fresh_active(self):
        result = consume_candidate(_make_snapshot(), _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_ACTIVE

    def test_intake_valid_true(self):
        result = consume_candidate(_make_snapshot(), _now_utc=_NOW)
        assert result["intake_valid"] is True


# ---------------------------------------------------------------------------
# 2. research_only=True always
# ---------------------------------------------------------------------------

class TestResearchOnly:
    def test_research_only_active(self):
        result = consume_candidate(_make_snapshot(), _now_utc=_NOW)
        assert result["research_only"] is True

    def test_research_only_hold_stale(self):
        result = consume_candidate(_make_snapshot(ts_utc=_STALE_TS), _now_utc=_NOW)
        assert result["research_only"] is True

    def test_research_only_invalid(self):
        result = consume_candidate({}, _now_utc=_NOW)
        assert result["research_only"] is True

    def test_research_only_hold_no_choice(self):
        result = consume_candidate(_make_snapshot(chosen_timeframe=None), _now_utc=_NOW)
        assert result["research_only"] is True


# ---------------------------------------------------------------------------
# 3–4. load_candidate_snapshot raises on error
# ---------------------------------------------------------------------------

class TestLoadRaises:
    def test_missing_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_candidate_snapshot(tmp_path / "nonexistent.json")

    def test_error_contains_path(self, tmp_path):
        p = tmp_path / "missing.json"
        with pytest.raises(FileNotFoundError) as exc:
            load_candidate_snapshot(p)
        assert str(p) in str(exc.value)

    def test_bad_json_raises_value_error(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("not json", encoding="utf-8")
        with pytest.raises(ValueError):
            load_candidate_snapshot(p)

    def test_non_object_json_raises_value_error(self, tmp_path):
        p = tmp_path / "arr.json"
        p.write_text("[1, 2, 3]", encoding="utf-8")
        with pytest.raises(ValueError):
            load_candidate_snapshot(p)


# ---------------------------------------------------------------------------
# 5–6. load_and_consume never raises — returns INVALID
# ---------------------------------------------------------------------------

class TestLoadAndConsumeFallback:
    def test_missing_file_returns_invalid(self, tmp_path):
        result = load_and_consume(path=tmp_path / "missing.json", _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID
        assert result["intake_valid"] is False

    def test_bad_json_returns_invalid(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("oops", encoding="utf-8")
        result = load_and_consume(path=p, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID

    def test_valid_file_returns_active(self, tmp_path):
        p = tmp_path / "snap.json"
        _write_json(_make_snapshot(), p)
        result = load_and_consume(path=p, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_ACTIVE


# ---------------------------------------------------------------------------
# 7. Empty dict → CANDIDATE_INVALID
# ---------------------------------------------------------------------------

class TestEmptyDict:
    def test_empty_dict_invalid(self):
        result = consume_candidate({}, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID

    def test_not_dict_invalid(self):
        result = consume_candidate(None, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID

    def test_list_invalid(self):
        result = consume_candidate([], _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID


# ---------------------------------------------------------------------------
# 8. Missing required top-level key → CANDIDATE_INVALID
# ---------------------------------------------------------------------------

class TestMissingRequiredKey:
    @pytest.mark.parametrize("key", [
        "version", "ts_utc", "market", "timeframes",
        "candidate_decision", "decision_context", "rationale_summary", "flags",
    ])
    def test_missing_key(self, key):
        snap = _make_snapshot()
        del snap[key]
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID
        assert result["intake_reason_code"] == REASON_CODES["INVALID_MISSING"]


# ---------------------------------------------------------------------------
# 9. Version mismatch → CANDIDATE_INVALID
# ---------------------------------------------------------------------------

class TestVersionMismatch:
    def test_wrong_version(self):
        snap = _make_snapshot(version="wrong_version_v99")
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID
        assert result["intake_reason_code"] == REASON_CODES["INVALID_VERSION"]

    def test_correct_version_passes(self):
        snap = _make_snapshot(version=EXPECTED_SNAPSHOT_VERSION)
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] != CANDIDATE_INVALID or \
               result["intake_reason_code"] != REASON_CODES["INVALID_VERSION"]


# ---------------------------------------------------------------------------
# 10. Unparseable ts_utc → CANDIDATE_INVALID
# ---------------------------------------------------------------------------

class TestBadTimestamp:
    def test_bad_ts_invalid(self):
        snap = _make_snapshot()
        snap["ts_utc"] = "not-a-date"
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID
        assert result["intake_reason_code"] == REASON_CODES["INVALID_TS"]

    def test_none_ts_invalid(self):
        snap = _make_snapshot()
        snap["ts_utc"] = None
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID


# ---------------------------------------------------------------------------
# 11. Stale snapshot → CANDIDATE_HOLD
# ---------------------------------------------------------------------------

class TestStale:
    def test_stale_hold(self):
        snap = _make_snapshot(ts_utc=_STALE_TS)
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_HOLD
        assert result["intake_reason_code"] == REASON_CODES["HOLD_STALE"]

    def test_stale_intake_valid_true(self):
        snap = _make_snapshot(ts_utc=_STALE_TS)
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_valid"] is True


# ---------------------------------------------------------------------------
# 12. chosen_timeframe=None → CANDIDATE_HOLD
# ---------------------------------------------------------------------------

class TestNoChoice:
    def test_none_timeframe_hold(self):
        snap = _make_snapshot(chosen_timeframe=None)
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_HOLD
        assert result["intake_reason_code"] == REASON_CODES["HOLD_NO_CHOICE"]

    def test_none_timeframe_valid_true(self):
        snap = _make_snapshot(chosen_timeframe=None)
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_valid"] is True


# ---------------------------------------------------------------------------
# 13–14. All output keys present
# ---------------------------------------------------------------------------

class TestOutputKeys:
    def test_all_keys_active(self):
        result = consume_candidate(_make_snapshot(), _now_utc=_NOW)
        assert _OUTPUT_KEYS.issubset(result.keys())

    def test_all_keys_hold_stale(self):
        result = consume_candidate(_make_snapshot(ts_utc=_STALE_TS), _now_utc=_NOW)
        assert _OUTPUT_KEYS.issubset(result.keys())

    def test_all_keys_invalid(self):
        result = consume_candidate({}, _now_utc=_NOW)
        assert _OUTPUT_KEYS.issubset(result.keys())

    def test_all_keys_hold_no_choice(self):
        result = consume_candidate(_make_snapshot(chosen_timeframe=None), _now_utc=_NOW)
        assert _OUTPUT_KEYS.issubset(result.keys())


# ---------------------------------------------------------------------------
# 15–16. Passthrough fields when ACTIVE
# ---------------------------------------------------------------------------

class TestPassthrough:
    def test_chosen_timeframe(self):
        result = consume_candidate(_make_snapshot(chosen_timeframe="4h"), _now_utc=_NOW)
        assert result["chosen_timeframe"] == "4h"

    def test_chosen_strategy(self):
        result = consume_candidate(
            _make_snapshot(chosen_strategy="trend_follow_lite"), _now_utc=_NOW
        )
        assert result["chosen_strategy"] == "trend_follow_lite"

    def test_chosen_regime(self):
        result = consume_candidate(_make_snapshot(chosen_regime="trend"), _now_utc=_NOW)
        assert result["chosen_regime"] == "trend"

    def test_chosen_weight(self):
        result = consume_candidate(_make_snapshot(chosen_weight=0.75), _now_utc=_NOW)
        assert result["chosen_allocation_weight"] == 0.75

    def test_dominant_strategy(self):
        result = consume_candidate(
            _make_snapshot(dominant_strategy="mean_reversion"), _now_utc=_NOW
        )
        assert result["dominant_strategy"] == "mean_reversion"

    def test_dominant_regime(self):
        result = consume_candidate(
            _make_snapshot(dominant_regime="volatile_trend"), _now_utc=_NOW
        )
        assert result["dominant_regime"] == "volatile_trend"

    def test_weights_sum(self):
        result = consume_candidate(_make_snapshot(weights_sum=0.99), _now_utc=_NOW)
        assert result["weights_sum"] == 0.99

    def test_snapshot_ts_utc(self):
        result = consume_candidate(_make_snapshot(ts_utc=_FRESH_TS), _now_utc=_NOW)
        assert result["snapshot_ts_utc"] == _FRESH_TS

    def test_snapshot_market(self):
        snap = _make_snapshot()
        snap["market"] = "ETH-EUR"
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["snapshot_market"] == "ETH-EUR"


# ---------------------------------------------------------------------------
# 17. Deterministic
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self):
        snap = _make_snapshot()
        r1 = consume_candidate(snap, _now_utc=_NOW)
        r2 = consume_candidate(snap, _now_utc=_NOW)
        assert r1 == r2


# ---------------------------------------------------------------------------
# 18–19. candidate_decision schema issues
# ---------------------------------------------------------------------------

class TestCandidateDecisionSchema:
    def test_candidate_decision_not_dict(self):
        snap = _make_snapshot()
        snap["candidate_decision"] = "not a dict"
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID
        assert result["intake_reason_code"] == REASON_CODES["INVALID_SCHEMA"]

    def test_candidate_decision_none(self):
        snap = _make_snapshot()
        snap["candidate_decision"] = None
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID

    @pytest.mark.parametrize("key", [
        "chosen_timeframe", "chosen_strategy", "chosen_regime", "chosen_allocation_weight",
    ])
    def test_candidate_decision_missing_key(self, key):
        snap = _make_snapshot()
        del snap["candidate_decision"][key]
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_INVALID
        assert result["intake_reason_code"] == REASON_CODES["INVALID_SCHEMA"]


# ---------------------------------------------------------------------------
# 20–21. Boundary freshness
# ---------------------------------------------------------------------------

class TestFreshnessBoundary:
    def test_just_fresh_active(self):
        # 1 minute before expiry → still ACTIVE
        ts_dt = _NOW - timedelta(hours=DEFAULT_MAX_AGE_HOURS) + timedelta(minutes=1)
        ts_str = ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        snap = _make_snapshot(ts_utc=ts_str)
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_ACTIVE

    def test_just_stale_hold(self):
        # 1 minute after expiry → HOLD
        ts_dt = _NOW - timedelta(hours=DEFAULT_MAX_AGE_HOURS) - timedelta(minutes=1)
        ts_str = ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        snap = _make_snapshot(ts_utc=ts_str)
        result = consume_candidate(snap, _now_utc=_NOW)
        assert result["intake_status"] == CANDIDATE_HOLD

    def test_custom_max_age(self):
        # 30-minute window; 29-minute-old snapshot → ACTIVE
        ts_dt = _NOW - timedelta(minutes=29)
        ts_str = ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        snap = _make_snapshot(ts_utc=ts_str)
        result = consume_candidate(snap, max_age_hours=0, _now_utc=_NOW)
        # max_age_hours=0 means any age is stale
        assert result["intake_status"] == CANDIDATE_HOLD
