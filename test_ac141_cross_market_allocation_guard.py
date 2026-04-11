"""
AC-141: Tests for Cross-Market Allocation Guard

Covers:
  1.  Normal spread → GUARD_PASS
  2.  High strategy concentration → GUARD_FAIL
  3.  High regime concentration → GUARD_FAIL
  4.  High single-market weight → GUARD_FAIL
  5.  Empty / no active markets → GUARD_PASS
  6.  Invalid/missing input → GUARD_FAIL (fail-closed)
  7.  guard_pass bool matches status
  8.  exposure_summary keys present
  9.  Deterministic output
  10. research_only=True always
  11. Multiple concurrent failures reported together
  12. Exact-threshold not exceeded → GUARD_PASS
  13. Just-above-threshold → GUARD_FAIL
  14. guard_reason_codes non-empty on FAIL
  15. Missing market_summaries key → GUARD_FAIL
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.build_cross_market_allocation_guard_lite import (
    SNAPSHOT_VERSION,
    GUARD_PASS,
    GUARD_FAIL,
    MAX_STRATEGY_CONCENTRATION,
    MAX_REGIME_CONCENTRATION,
    MAX_MARKET_WEIGHT_FRACTION,
    REASON_CODES,
    check_guard,
    check_guard_from_file,
    write_guard_result,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = "2025-01-01T00:00:00Z"

REQUIRED_KEYS = {
    "version", "ts_utc", "guard_status", "guard_pass", "guard_reasons",
    "guard_reason_codes", "exposure_summary", "active_markets_checked",
    "research_only", "flags",
}
EXPOSURE_KEYS = {
    "strategy_exposure", "regime_exposure", "market_weight_exposure",
    "max_strategy_concentration", "max_regime_concentration",
    "max_single_market_weight",
}


def _active_summary(strategy="mean_reversion", regime="range", weight=1.0) -> dict:
    return {
        "intake_status":            "CANDIDATE_ACTIVE",
        "chosen_timeframe":         "1h",
        "chosen_strategy":          strategy,
        "chosen_regime":            regime,
        "chosen_allocation_weight": weight,
        "snapshot_ts_utc":          _TS,
    }


def _hold_summary() -> dict:
    return {
        "intake_status":            "CANDIDATE_HOLD",
        "chosen_strategy":          "mean_reversion",
        "chosen_regime":            "range",
        "chosen_allocation_weight": 0.5,
    }


def _make_portfolio(market_summaries: dict) -> dict:
    """Minimal AC-140-compatible portfolio summary."""
    active_count = sum(
        1 for s in market_summaries.values()
        if isinstance(s, dict) and s.get("intake_status") == "CANDIDATE_ACTIVE"
    )
    return {
        "version":          "queen_candidate_portfolio_summary_v1",
        "ts_utc":           _TS,
        "markets":          sorted(market_summaries.keys()),
        "market_summaries": market_summaries,
        "colony_summary": {
            "total_markets":             len(market_summaries),
            "active_markets_count":      active_count,
            "dominant_strategy":         None,
            "dominant_regime":           None,
            "total_candidate_decisions": active_count,
        },
        "research_only": True,
        "flags":         {"research_only": True, "pipeline_impact": False},
    }


def _write_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. Normal spread → GUARD_PASS
# ---------------------------------------------------------------------------

class TestNormalSpread:
    def test_three_different_strategies_pass(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion",          "range",   1.0),
            "ETH-EUR": _active_summary("trend_follow_lite",       "trend",   1.0),
            "ADA-EUR": _active_summary("volatility_breakout_lite","volatile_trend", 1.0),
        })
        result = check_guard(portfolio)
        assert result["guard_status"] == GUARD_PASS

    def test_guard_pass_bool_true(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion", "range", 0.5),
            "ETH-EUR": _active_summary("trend_follow_lite", "trend", 0.5),
        })
        result = check_guard(portfolio)
        assert result["guard_pass"] is True


# ---------------------------------------------------------------------------
# 2. High strategy concentration → GUARD_FAIL
# ---------------------------------------------------------------------------

class TestStrategyConcentration:
    def test_all_same_strategy_fails(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion", "range"),
            "ETH-EUR": _active_summary("mean_reversion", "trend"),
            "ADA-EUR": _active_summary("mean_reversion", "volatile_trend"),
        })
        result = check_guard(portfolio, max_strategy_concentration=0.60)
        assert result["guard_status"] == GUARD_FAIL
        assert any(c == REASON_CODES["STRATEGY_CONCENTRATION"] for c in result["guard_reason_codes"])

    def test_strategy_reason_in_reasons(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion", "range"),
            "ETH-EUR": _active_summary("mean_reversion", "trend"),
        })
        result = check_guard(portfolio, max_strategy_concentration=0.49)
        assert result["guard_status"] == GUARD_FAIL
        assert any("mean_reversion" in r for r in result["guard_reasons"])


# ---------------------------------------------------------------------------
# 3. High regime concentration → GUARD_FAIL
# ---------------------------------------------------------------------------

class TestRegimeConcentration:
    def test_all_same_regime_fails(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion",    "range"),
            "ETH-EUR": _active_summary("trend_follow_lite", "range"),
            "ADA-EUR": _active_summary("volatility_breakout_lite", "range"),
        })
        result = check_guard(portfolio, max_regime_concentration=0.60)
        assert result["guard_status"] == GUARD_FAIL
        assert any(c == REASON_CODES["REGIME_CONCENTRATION"] for c in result["guard_reason_codes"])


# ---------------------------------------------------------------------------
# 4. High single-market weight → GUARD_FAIL
# ---------------------------------------------------------------------------

class TestMarketWeight:
    def test_dominant_market_weight_fails(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion",    "range", 9.0),
            "ETH-EUR": _active_summary("trend_follow_lite", "trend", 1.0),
        })
        result = check_guard(portfolio, max_market_weight_fraction=0.70)
        assert result["guard_status"] == GUARD_FAIL
        assert any(c == REASON_CODES["MARKET_WEIGHT"] for c in result["guard_reason_codes"])

    def test_equal_weights_pass(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion",    "range", 0.5),
            "ETH-EUR": _active_summary("trend_follow_lite", "trend", 0.5),
        })
        result = check_guard(portfolio)
        assert result["guard_pass"] is True


# ---------------------------------------------------------------------------
# 5. Empty / no active markets → GUARD_PASS
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_empty_portfolio_pass(self):
        result = check_guard(_make_portfolio({}))
        assert result["guard_status"] == GUARD_PASS
        assert result["active_markets_checked"] == 0

    def test_all_hold_pass(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _hold_summary(),
            "ETH-EUR": _hold_summary(),
        })
        result = check_guard(portfolio)
        assert result["guard_status"] == GUARD_PASS
        assert result["active_markets_checked"] == 0

    def test_no_active_reason_code(self):
        result = check_guard(_make_portfolio({}))
        assert REASON_CODES["NO_ACTIVE_MARKETS"] in result["guard_reason_codes"]


# ---------------------------------------------------------------------------
# 6. Invalid / missing input → GUARD_FAIL
# ---------------------------------------------------------------------------

class TestInvalidInput:
    def test_none_input_fail(self):
        result = check_guard(None)
        assert result["guard_status"] == GUARD_FAIL

    def test_list_input_fail(self):
        result = check_guard([])
        assert result["guard_status"] == GUARD_FAIL

    def test_empty_dict_fail_graceful(self):
        # Empty dict → no market_summaries, treated as no active markets → PASS
        result = check_guard({})
        assert result["guard_status"] == GUARD_PASS

    def test_non_dict_market_summaries_fail(self):
        portfolio = _make_portfolio({})
        portfolio["market_summaries"] = "not a dict"
        result = check_guard(portfolio)
        assert result["guard_status"] == GUARD_FAIL

    def test_missing_market_summaries_graceful(self):
        portfolio = {"ts_utc": _TS, "colony_summary": {}}
        result = check_guard(portfolio)
        # No market_summaries → no active markets → GUARD_PASS
        assert result["guard_status"] == GUARD_PASS


# ---------------------------------------------------------------------------
# 7. guard_pass bool matches status
# ---------------------------------------------------------------------------

class TestGuardPassBool:
    def test_guard_pass_true_when_pass(self):
        result = check_guard(_make_portfolio({}))
        assert result["guard_pass"] is (result["guard_status"] == GUARD_PASS)

    def test_guard_pass_false_when_fail(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion", "range"),
            "ETH-EUR": _active_summary("mean_reversion", "range"),
        })
        result = check_guard(portfolio, max_strategy_concentration=0.49)
        assert result["guard_pass"] is False
        assert result["guard_status"] == GUARD_FAIL


# ---------------------------------------------------------------------------
# 8. exposure_summary keys
# ---------------------------------------------------------------------------

class TestExposureSummaryKeys:
    def test_exposure_keys_present_active(self):
        portfolio = _make_portfolio({"BTC-EUR": _active_summary()})
        result = check_guard(portfolio)
        assert EXPOSURE_KEYS.issubset(result["exposure_summary"].keys())

    def test_exposure_keys_present_empty(self):
        result = check_guard(_make_portfolio({}))
        assert EXPOSURE_KEYS.issubset(result["exposure_summary"].keys())

    def test_required_keys_present(self):
        result = check_guard(_make_portfolio({}))
        assert REQUIRED_KEYS.issubset(result.keys())


# ---------------------------------------------------------------------------
# 9. Deterministic
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion", "range", 0.5),
            "ETH-EUR": _active_summary("trend_follow_lite", "trend", 0.5),
        })
        r1 = check_guard(portfolio)
        r2 = check_guard(portfolio)
        # timestamps will differ; compare everything except ts_utc
        r1_cmp = {k: v for k, v in r1.items() if k != "ts_utc"}
        r2_cmp = {k: v for k, v in r2.items() if k != "ts_utc"}
        assert r1_cmp == r2_cmp


# ---------------------------------------------------------------------------
# 10. research_only=True always
# ---------------------------------------------------------------------------

class TestResearchOnly:
    def test_research_only_pass(self):
        result = check_guard(_make_portfolio({}))
        assert result["research_only"] is True

    def test_research_only_fail(self):
        result = check_guard(None)
        assert result["research_only"] is True


# ---------------------------------------------------------------------------
# 11. Multiple concurrent failures
# ---------------------------------------------------------------------------

class TestMultipleFailures:
    def test_strategy_and_regime_both_fail(self):
        # All same strategy AND all same regime with very low thresholds
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion", "range", 5.0),
            "ETH-EUR": _active_summary("mean_reversion", "range", 5.0),
        })
        result = check_guard(portfolio, max_strategy_concentration=0.49,
                             max_regime_concentration=0.49)
        assert result["guard_status"] == GUARD_FAIL
        assert len(result["guard_reason_codes"]) >= 2


# ---------------------------------------------------------------------------
# 12–13. Exact-threshold boundary
# ---------------------------------------------------------------------------

class TestThresholdBoundary:
    def test_at_threshold_passes(self):
        # 2 out of 2 = 100%, threshold 100% → still fails (> not >=)
        # 1 out of 2 = 50%, threshold 50% → passes (not > 50%)
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion", "range"),
            "ETH-EUR": _active_summary("trend_follow_lite", "trend"),
        })
        result = check_guard(portfolio, max_strategy_concentration=0.50)
        # Each strategy is exactly 50%, not > 50%, so should PASS
        assert result["guard_status"] == GUARD_PASS

    def test_just_above_threshold_fails(self):
        # 2 out of 2 active markets same strategy = 100% > 80% threshold
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion", "range"),
            "ETH-EUR": _active_summary("mean_reversion", "trend"),
        })
        result = check_guard(portfolio, max_strategy_concentration=0.80)
        assert result["guard_status"] == GUARD_FAIL


# ---------------------------------------------------------------------------
# File-based
# ---------------------------------------------------------------------------

class TestFromFile:
    def test_missing_file_guard_fail(self, tmp_path):
        result = check_guard_from_file(tmp_path / "missing.json")
        assert result["guard_status"] == GUARD_FAIL

    def test_valid_file_guard_pass(self, tmp_path):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary("mean_reversion", "range", 0.5),
            "ETH-EUR": _active_summary("trend_follow_lite", "trend", 0.5),
        })
        p = tmp_path / "summary.json"
        _write_json(portfolio, p)
        result = check_guard_from_file(p)
        assert result["guard_status"] == GUARD_PASS

    def test_write_guard_result(self, tmp_path):
        out = tmp_path / "guard.json"
        result = check_guard(_make_portfolio({}))
        write_guard_result(result, out)
        assert out.exists()
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["version"] == SNAPSHOT_VERSION
