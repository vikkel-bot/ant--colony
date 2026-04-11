"""
AC-142: Tests for Execution Budget Translation

Covers:
  1.  Allocation present → budget_fraction visible and correct
  2.  budget_eur computed when total_budget_eur provided
  3.  Allocation absent (no active markets) → fallback, no crash
  4.  Invalid portfolio_summary → fallback, no crash
  5.  All-zero weights → equal split fallback
  6.  Single active market → budget_fraction=1.0
  7.  Multiple markets → fractions sum to ~1.0
  8.  Non-active markets → budget_fraction=0.0
  9.  Backward compatibility: all required output keys present
  10. research_only=True always
  11. pipeline_impact=False always
  12. Deterministic: same input → same output
  13. fallback_used=False when weights present
  14. fallback_used=True when no active markets
  15. budget_eur=None when total_budget_eur not provided
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.build_execution_budget_translation_lite import (
    SNAPSHOT_VERSION,
    FLAGS,
    translate_to_budget,
    translate_from_file,
    write_budget_translation,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = "2025-01-01T00:00:00Z"

REQUIRED_KEYS = {
    "version", "ts_utc", "budget_per_market", "budget_summary",
    "fallback_used", "research_only", "flags",
}
BUDGET_SUMMARY_KEYS = {
    "total_budget_eur", "total_budget_eur_allocated",
    "total_fraction_allocated", "active_markets", "budget_source",
}
BUDGET_ENTRY_KEYS = {
    "budget_fraction", "budget_eur", "source_weight",
    "intake_status", "budget_context",
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
            "total_markets":        len(market_summaries),
            "active_markets_count": active_count,
        },
        "research_only": True,
        "flags": {"research_only": True, "pipeline_impact": False},
    }


def _write_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. Allocation present → budget_fraction visible
# ---------------------------------------------------------------------------

class TestAllocationPresent:
    def test_single_market_fraction_one(self):
        portfolio = _make_portfolio({"BTC-EUR": _active_summary(weight=1.0)})
        result = translate_to_budget(portfolio)
        assert result["budget_per_market"]["BTC-EUR"]["budget_fraction"] == pytest.approx(1.0)

    def test_two_equal_weights_half(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary(weight=1.0),
            "ETH-EUR": _active_summary(weight=1.0),
        })
        result = translate_to_budget(portfolio)
        assert result["budget_per_market"]["BTC-EUR"]["budget_fraction"] == pytest.approx(0.5)
        assert result["budget_per_market"]["ETH-EUR"]["budget_fraction"] == pytest.approx(0.5)

    def test_unequal_weights_proportional(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary(weight=2.0),
            "ETH-EUR": _active_summary(weight=1.0),
        })
        result = translate_to_budget(portfolio)
        btc_frac = result["budget_per_market"]["BTC-EUR"]["budget_fraction"]
        eth_frac = result["budget_per_market"]["ETH-EUR"]["budget_fraction"]
        assert btc_frac == pytest.approx(2 / 3, rel=1e-4)
        assert eth_frac == pytest.approx(1 / 3, rel=1e-4)


# ---------------------------------------------------------------------------
# 2. budget_eur computed
# ---------------------------------------------------------------------------

class TestBudgetEur:
    def test_budget_eur_computed(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary(weight=0.5),
            "ETH-EUR": _active_summary(weight=0.5),
        })
        result = translate_to_budget(portfolio, total_budget_eur=10000.0)
        assert result["budget_per_market"]["BTC-EUR"]["budget_eur"] == pytest.approx(5000.0)
        assert result["budget_per_market"]["ETH-EUR"]["budget_eur"] == pytest.approx(5000.0)

    def test_total_budget_eur_in_summary(self):
        portfolio = _make_portfolio({"BTC-EUR": _active_summary()})
        result = translate_to_budget(portfolio, total_budget_eur=10000.0)
        assert result["budget_summary"]["total_budget_eur"] == 10000.0

    def test_total_budget_eur_allocated_sum(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary(weight=1.0),
            "ETH-EUR": _active_summary(weight=1.0),
        })
        result = translate_to_budget(portfolio, total_budget_eur=10000.0)
        assert result["budget_summary"]["total_budget_eur_allocated"] == pytest.approx(10000.0, abs=1.0)


# ---------------------------------------------------------------------------
# 3. No active markets → fallback
# ---------------------------------------------------------------------------

class TestNoActiveMarkets:
    def test_no_active_no_crash(self):
        portfolio = _make_portfolio({"BTC-EUR": _hold_summary()})
        result = translate_to_budget(portfolio)
        assert isinstance(result, dict)

    def test_no_active_fraction_zero(self):
        portfolio = _make_portfolio({"BTC-EUR": _hold_summary()})
        result = translate_to_budget(portfolio)
        assert result["budget_per_market"]["BTC-EUR"]["budget_fraction"] == 0.0

    def test_empty_portfolio_no_crash(self):
        portfolio = _make_portfolio({})
        result = translate_to_budget(portfolio)
        assert result["budget_per_market"] == {}

    def test_no_active_fallback_used(self):
        portfolio = _make_portfolio({"BTC-EUR": _hold_summary()})
        result = translate_to_budget(portfolio)
        assert result["fallback_used"] is True


# ---------------------------------------------------------------------------
# 4. Invalid portfolio_summary → fallback
# ---------------------------------------------------------------------------

class TestInvalidInput:
    def test_none_input_no_crash(self):
        result = translate_to_budget(None)
        assert isinstance(result, dict)
        assert result["fallback_used"] is True

    def test_list_input_no_crash(self):
        result = translate_to_budget([])
        assert isinstance(result, dict)

    def test_empty_dict_no_crash(self):
        result = translate_to_budget({})
        assert isinstance(result, dict)

    def test_non_dict_market_summaries_no_crash(self):
        portfolio = _make_portfolio({})
        portfolio["market_summaries"] = "bad"
        result = translate_to_budget(portfolio)
        assert isinstance(result, dict)
        assert result["fallback_used"] is True


# ---------------------------------------------------------------------------
# 5. All-zero weights → equal split fallback
# ---------------------------------------------------------------------------

class TestZeroWeights:
    def test_zero_weights_equal_split(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary(weight=0.0),
            "ETH-EUR": _active_summary(weight=0.0),
        })
        result = translate_to_budget(portfolio)
        assert result["budget_per_market"]["BTC-EUR"]["budget_fraction"] == pytest.approx(0.5)
        assert result["budget_per_market"]["ETH-EUR"]["budget_fraction"] == pytest.approx(0.5)
        assert result["fallback_used"] is True

    def test_zero_weights_budget_context_equal_split(self):
        portfolio = _make_portfolio({"BTC-EUR": _active_summary(weight=0.0)})
        result = translate_to_budget(portfolio)
        assert "equal_split" in result["budget_per_market"]["BTC-EUR"]["budget_context"]


# ---------------------------------------------------------------------------
# 6–8. Single / multiple / non-active market fractions
# ---------------------------------------------------------------------------

class TestFractions:
    def test_fractions_sum_to_one(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary(weight=0.333),
            "ETH-EUR": _active_summary(weight=0.333),
            "ADA-EUR": _active_summary(weight=0.334),
        })
        result = translate_to_budget(portfolio)
        total = sum(
            e["budget_fraction"]
            for e in result["budget_per_market"].values()
        )
        assert total == pytest.approx(1.0, abs=1e-4)

    def test_non_active_fraction_zero(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary(weight=1.0),
            "ETH-EUR": _hold_summary(),
        })
        result = translate_to_budget(portfolio)
        assert result["budget_per_market"]["ETH-EUR"]["budget_fraction"] == 0.0
        assert result["budget_per_market"]["BTC-EUR"]["budget_fraction"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# 9. Required output keys
# ---------------------------------------------------------------------------

class TestOutputKeys:
    def test_required_keys(self):
        result = translate_to_budget(_make_portfolio({"BTC-EUR": _active_summary()}))
        assert REQUIRED_KEYS.issubset(result.keys())

    def test_budget_summary_keys(self):
        result = translate_to_budget(_make_portfolio({"BTC-EUR": _active_summary()}))
        assert BUDGET_SUMMARY_KEYS.issubset(result["budget_summary"].keys())

    def test_budget_entry_keys(self):
        result = translate_to_budget(_make_portfolio({"BTC-EUR": _active_summary()}))
        entry = result["budget_per_market"]["BTC-EUR"]
        assert BUDGET_ENTRY_KEYS.issubset(entry.keys())

    def test_version_correct(self):
        result = translate_to_budget(_make_portfolio({}))
        assert result["version"] == SNAPSHOT_VERSION


# ---------------------------------------------------------------------------
# 10–11. research_only / pipeline_impact
# ---------------------------------------------------------------------------

class TestFlags:
    def test_research_only_true(self):
        assert translate_to_budget(_make_portfolio({}))["research_only"] is True

    def test_pipeline_impact_false(self):
        assert translate_to_budget(_make_portfolio({}))["flags"]["pipeline_impact"] is False


# ---------------------------------------------------------------------------
# 12. Deterministic
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self):
        portfolio = _make_portfolio({
            "BTC-EUR": _active_summary(weight=0.6),
            "ETH-EUR": _active_summary(weight=0.4),
        })
        r1 = translate_to_budget(portfolio)
        r2 = translate_to_budget(portfolio)
        r1c = {k: v for k, v in r1.items() if k != "ts_utc"}
        r2c = {k: v for k, v in r2.items() if k != "ts_utc"}
        assert r1c == r2c


# ---------------------------------------------------------------------------
# 13–14. fallback_used
# ---------------------------------------------------------------------------

class TestFallbackUsed:
    def test_fallback_false_with_weights(self):
        portfolio = _make_portfolio({"BTC-EUR": _active_summary(weight=1.0)})
        result = translate_to_budget(portfolio)
        assert result["fallback_used"] is False

    def test_fallback_true_no_active(self):
        result = translate_to_budget(_make_portfolio({}))
        assert result["fallback_used"] is True


# ---------------------------------------------------------------------------
# 15. budget_eur=None when not provided
# ---------------------------------------------------------------------------

class TestBudgetEurNone:
    def test_no_budget_eur_none_in_entry(self):
        portfolio = _make_portfolio({"BTC-EUR": _active_summary()})
        result = translate_to_budget(portfolio)
        assert result["budget_per_market"]["BTC-EUR"]["budget_eur"] is None

    def test_no_budget_eur_none_in_summary(self):
        portfolio = _make_portfolio({"BTC-EUR": _active_summary()})
        result = translate_to_budget(portfolio)
        assert result["budget_summary"]["total_budget_eur"] is None


# ---------------------------------------------------------------------------
# File-based
# ---------------------------------------------------------------------------

class TestFromFile:
    def test_missing_file_fallback(self, tmp_path):
        result = translate_from_file(tmp_path / "missing.json")
        assert isinstance(result, dict)
        assert result["fallback_used"] is True

    def test_valid_file_active(self, tmp_path):
        portfolio = _make_portfolio({"BTC-EUR": _active_summary(weight=1.0)})
        p = tmp_path / "summary.json"
        _write_json(portfolio, p)
        result = translate_from_file(p)
        assert result["budget_per_market"]["BTC-EUR"]["budget_fraction"] == pytest.approx(1.0)

    def test_write_budget_translation(self, tmp_path):
        out = tmp_path / "budget.json"
        result = translate_to_budget(_make_portfolio({}))
        write_budget_translation(result, out)
        assert out.exists()
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["version"] == SNAPSHOT_VERSION
