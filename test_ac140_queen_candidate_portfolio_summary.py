"""
AC-140: Tests for Queen Candidate Portfolio Summary

Covers:
  1.  Build without crash (valid inputs)
  2.  Multiple markets correctly bundled
  3.  Empty input without crash
  4.  Dominant strategy deterministic (tie-break alphabetical)
  5.  Dominant regime deterministic
  6.  Output structure stable (all required keys)
  7.  Active markets count correct
  8.  Non-active intakes included but not counted as active
  9.  Invalid/missing per-market intake -> safe fallback
  10. Non-dict intake -> safe fallback
  11. market_summaries contains all input markets
  12. research_only=True always
  13. Deterministic: same input -> same output
  14. colony_summary keys all present
  15. dominant_strategy=None when no active markets
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
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.build_queen_candidate_portfolio_summary_lite import (
    SNAPSHOT_VERSION,
    FLAGS,
    build_portfolio_summary,
    write_summary,
    build_portfolio_summary_from_paths,
    build_and_write_summary,
    _dominant,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = "2025-01-01T00:00:00Z"

REQUIRED_KEYS = {
    "version", "ts_utc", "markets", "market_summaries",
    "colony_summary", "research_only", "flags",
}
COLONY_KEYS = {
    "total_markets", "active_markets_count", "dominant_strategy",
    "dominant_regime", "total_candidate_decisions",
}
MARKET_SUMMARY_KEYS = {
    "intake_status", "chosen_timeframe", "chosen_strategy",
    "chosen_regime", "chosen_allocation_weight", "snapshot_ts_utc",
}


def _active_intake(market="BTC-EUR", strategy="mean_reversion", regime="range",
                   timeframe="1h", weight=0.5) -> dict:
    return {
        "intake_status":            "CANDIDATE_ACTIVE",
        "intake_valid":             True,
        "intake_reason":            f"CANDIDATE_ACTIVE|tf={timeframe}",
        "intake_reason_code":       "CANDIDATE_INTAKE_OK",
        "chosen_timeframe":         timeframe,
        "chosen_strategy":          strategy,
        "chosen_regime":            regime,
        "chosen_allocation_weight": weight,
        "dominant_strategy":        strategy,
        "dominant_regime":          regime,
        "weights_sum":              1.0,
        "snapshot_ts_utc":          _TS,
        "snapshot_market":          market,
        "research_only":            True,
    }


def _hold_intake(reason="stale") -> dict:
    return {
        "intake_status":            "CANDIDATE_HOLD",
        "intake_valid":             True,
        "intake_reason":            reason,
        "intake_reason_code":       "CANDIDATE_HOLD_STALE",
        "chosen_timeframe":         "1h",
        "chosen_strategy":          "mean_reversion",
        "chosen_regime":            "range",
        "chosen_allocation_weight": 0.5,
        "snapshot_ts_utc":          _TS,
        "research_only":            True,
    }


def _invalid_intake() -> dict:
    return {
        "intake_status":            "CANDIDATE_INVALID",
        "intake_valid":             False,
        "intake_reason":            "missing key",
        "intake_reason_code":       "CANDIDATE_INVALID_MISSING_FIELD",
        "chosen_timeframe":         None,
        "chosen_strategy":          None,
        "chosen_regime":            None,
        "chosen_allocation_weight": None,
        "snapshot_ts_utc":          None,
        "research_only":            True,
    }


def _write_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. Build without crash
# ---------------------------------------------------------------------------

class TestBuildNoCrash:
    def test_single_active_market(self):
        result = build_portfolio_summary({"BTC-EUR": _active_intake()}, ts_utc=_TS)
        assert isinstance(result, dict)

    def test_multiple_markets(self):
        intakes = {
            "BTC-EUR": _active_intake("BTC-EUR"),
            "ETH-EUR": _active_intake("ETH-EUR", strategy="trend_follow_lite", regime="trend"),
        }
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert isinstance(result, dict)

    def test_write_no_crash(self, tmp_path):
        out = tmp_path / "summary.json"
        summary = build_portfolio_summary({"BTC-EUR": _active_intake()}, ts_utc=_TS)
        write_summary(summary, out)
        assert out.exists()


# ---------------------------------------------------------------------------
# 2. Multiple markets correctly bundled
# ---------------------------------------------------------------------------

class TestMultipleMarkets:
    def test_all_markets_in_summaries(self):
        intakes = {"BTC-EUR": _active_intake(), "ETH-EUR": _active_intake("ETH-EUR")}
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert "BTC-EUR" in result["market_summaries"]
        assert "ETH-EUR" in result["market_summaries"]

    def test_markets_list_sorted(self):
        intakes = {"ETH-EUR": _active_intake(), "BTC-EUR": _active_intake()}
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert result["markets"] == sorted(result["markets"])

    def test_chosen_strategy_passthrough(self):
        intakes = {"BTC-EUR": _active_intake("BTC-EUR", strategy="volatility_breakout_lite")}
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert result["market_summaries"]["BTC-EUR"]["chosen_strategy"] == "volatility_breakout_lite"

    def test_chosen_regime_passthrough(self):
        intakes = {"BTC-EUR": _active_intake("BTC-EUR", regime="volatile_trend")}
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert result["market_summaries"]["BTC-EUR"]["chosen_regime"] == "volatile_trend"


# ---------------------------------------------------------------------------
# 3. Empty / incomplete input without crash
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_empty_dict_no_crash(self):
        result = build_portfolio_summary({}, ts_utc=_TS)
        assert isinstance(result, dict)

    def test_empty_markets_list(self):
        result = build_portfolio_summary({}, ts_utc=_TS)
        assert result["markets"] == []
        assert result["colony_summary"]["total_markets"] == 0
        assert result["colony_summary"]["active_markets_count"] == 0
        assert result["colony_summary"]["dominant_strategy"] is None
        assert result["colony_summary"]["dominant_regime"] is None

    def test_non_dict_input_no_crash(self):
        result = build_portfolio_summary(None, ts_utc=_TS)
        assert isinstance(result, dict)
        assert result["markets"] == []

    def test_list_input_no_crash(self):
        result = build_portfolio_summary([], ts_utc=_TS)
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 4–5. Dominant strategy/regime deterministic
# ---------------------------------------------------------------------------

class TestDominant:
    def test_dominant_strategy_highest_count(self):
        intakes = {
            "BTC-EUR": _active_intake("BTC-EUR", strategy="mean_reversion"),
            "ETH-EUR": _active_intake("ETH-EUR", strategy="mean_reversion"),
            "ADA-EUR": _active_intake("ADA-EUR", strategy="trend_follow_lite"),
        }
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert result["colony_summary"]["dominant_strategy"] == "mean_reversion"

    def test_dominant_strategy_tie_alphabetical(self):
        intakes = {
            "BTC-EUR": _active_intake("BTC-EUR", strategy="mean_reversion"),
            "ETH-EUR": _active_intake("ETH-EUR", strategy="trend_follow_lite"),
        }
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        # tie: mean_reversion vs trend_follow_lite → alphabetically "mean_reversion" wins
        assert result["colony_summary"]["dominant_strategy"] == "mean_reversion"

    def test_dominant_regime_highest_count(self):
        intakes = {
            "BTC-EUR": _active_intake("BTC-EUR", regime="range"),
            "ETH-EUR": _active_intake("ETH-EUR", regime="range"),
            "ADA-EUR": _active_intake("ADA-EUR", regime="trend"),
        }
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert result["colony_summary"]["dominant_regime"] == "range"

    def test_dominant_helper_direct(self):
        freq = {"a": 3, "b": 5, "c": 5}
        # b and c tied at 5 → alphabetically "b" wins
        assert _dominant(freq) == "b"

    def test_dominant_helper_empty(self):
        assert _dominant({}) is None


# ---------------------------------------------------------------------------
# 6. Output structure stable
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def test_required_keys(self):
        result = build_portfolio_summary({"BTC-EUR": _active_intake()}, ts_utc=_TS)
        assert REQUIRED_KEYS.issubset(result.keys())

    def test_colony_summary_keys(self):
        result = build_portfolio_summary({"BTC-EUR": _active_intake()}, ts_utc=_TS)
        assert COLONY_KEYS.issubset(result["colony_summary"].keys())

    def test_market_summary_keys(self):
        result = build_portfolio_summary({"BTC-EUR": _active_intake()}, ts_utc=_TS)
        ms = result["market_summaries"]["BTC-EUR"]
        assert MARKET_SUMMARY_KEYS.issubset(ms.keys())

    def test_version_correct(self):
        result = build_portfolio_summary({}, ts_utc=_TS)
        assert result["version"] == SNAPSHOT_VERSION

    def test_flags(self):
        result = build_portfolio_summary({}, ts_utc=_TS)
        assert result["flags"]["research_only"] is True
        assert result["flags"]["pipeline_impact"] is False

    def test_valid_json_on_disk(self, tmp_path):
        out = tmp_path / "s.json"
        write_summary(build_portfolio_summary({"BTC-EUR": _active_intake()}, ts_utc=_TS), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert REQUIRED_KEYS.issubset(data.keys())


# ---------------------------------------------------------------------------
# 7. Active markets count
# ---------------------------------------------------------------------------

class TestActiveCount:
    def test_all_active(self):
        intakes = {
            "BTC-EUR": _active_intake(),
            "ETH-EUR": _active_intake("ETH-EUR"),
        }
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert result["colony_summary"]["active_markets_count"] == 2

    def test_none_active(self):
        intakes = {
            "BTC-EUR": _hold_intake(),
            "ETH-EUR": _invalid_intake(),
        }
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert result["colony_summary"]["active_markets_count"] == 0

    def test_mixed_active_hold(self):
        intakes = {
            "BTC-EUR": _active_intake(),
            "ETH-EUR": _hold_intake(),
            "ADA-EUR": _invalid_intake(),
        }
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert result["colony_summary"]["active_markets_count"] == 1
        assert result["colony_summary"]["total_candidate_decisions"] == 1


# ---------------------------------------------------------------------------
# 8–10. Non-active / missing / non-dict intakes
# ---------------------------------------------------------------------------

class TestFallbacks:
    def test_hold_included_but_not_active(self):
        intakes = {"BTC-EUR": _hold_intake()}
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert "BTC-EUR" in result["market_summaries"]
        assert result["colony_summary"]["active_markets_count"] == 0

    def test_invalid_included_but_not_active(self):
        intakes = {"BTC-EUR": _invalid_intake()}
        result = build_portfolio_summary(intakes, ts_utc=_TS)
        assert "BTC-EUR" in result["market_summaries"]
        assert result["colony_summary"]["active_markets_count"] == 0

    def test_none_intake_safe(self):
        result = build_portfolio_summary({"BTC-EUR": None}, ts_utc=_TS)
        assert "BTC-EUR" in result["market_summaries"]
        assert result["colony_summary"]["active_markets_count"] == 0

    def test_non_dict_intake_safe(self):
        result = build_portfolio_summary({"BTC-EUR": "bad"}, ts_utc=_TS)
        assert isinstance(result, dict)
        assert result["colony_summary"]["active_markets_count"] == 0

    def test_dominant_none_when_no_active(self):
        result = build_portfolio_summary({"BTC-EUR": _invalid_intake()}, ts_utc=_TS)
        assert result["colony_summary"]["dominant_strategy"] is None
        assert result["colony_summary"]["dominant_regime"] is None


# ---------------------------------------------------------------------------
# 12. research_only=True always
# ---------------------------------------------------------------------------

class TestResearchOnly:
    def test_research_only_true(self):
        result = build_portfolio_summary({}, ts_utc=_TS)
        assert result["research_only"] is True

    def test_research_only_with_markets(self):
        result = build_portfolio_summary({"BTC-EUR": _active_intake()}, ts_utc=_TS)
        assert result["research_only"] is True


# ---------------------------------------------------------------------------
# 13. Deterministic
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self):
        intakes = {"BTC-EUR": _active_intake(), "ETH-EUR": _active_intake("ETH-EUR")}
        r1 = build_portfolio_summary(intakes, ts_utc=_TS)
        r2 = build_portfolio_summary(intakes, ts_utc=_TS)
        assert r1 == r2


# ---------------------------------------------------------------------------
# File-based: build_portfolio_summary_from_paths
# ---------------------------------------------------------------------------

class TestFromPaths:
    def _make_candidate_snapshot(self) -> dict:
        """Return a valid AC-135-compatible snapshot."""
        from ant_colony.queen_research_candidate_intake_lite import EXPECTED_SNAPSHOT_VERSION
        return {
            "version":   EXPECTED_SNAPSHOT_VERSION,
            "ts_utc":    "2025-06-01T11:00:00Z",
            "market":    "BTC-EUR",
            "timeframes": ["1h"],
            "candidate_decision": {
                "chosen_timeframe":         "1h",
                "chosen_strategy":          "mean_reversion",
                "chosen_regime":            "range",
                "chosen_allocation_weight": 0.5,
            },
            "decision_context": {
                "dominant_strategy": "mean_reversion",
                "dominant_regime":   "range",
                "weights_sum":       1.0,
            },
            "rationale_summary": {"selection_basis": "highest_allocation_weight",
                                  "tie_break": "alphabetical_timeframe"},
            "flags": {"research_only": True, "pipeline_impact": False},
        }

    def test_missing_path_returns_invalid_not_crash(self, tmp_path):
        result = build_portfolio_summary_from_paths(
            {"BTC-EUR": tmp_path / "nonexistent.json"},
        )
        assert isinstance(result, dict)
        assert result["colony_summary"]["active_markets_count"] == 0

    def test_valid_path_returns_active(self, tmp_path):
        from datetime import datetime, timezone, timedelta
        snap = self._make_candidate_snapshot()
        # ts_utc fresh = now - 1h
        now = datetime.now(timezone.utc)
        snap["ts_utc"] = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        p = tmp_path / "snap.json"
        _write_json(snap, p)
        result = build_portfolio_summary_from_paths({"BTC-EUR": p})
        assert result["colony_summary"]["active_markets_count"] == 1

    def test_build_and_write(self, tmp_path):
        out = tmp_path / "summary.json"
        result = build_and_write_summary(
            market_paths={},
            out_path=out,
            ts_utc=_TS,
        )
        assert out.exists()
        assert result["version"] == SNAPSHOT_VERSION
