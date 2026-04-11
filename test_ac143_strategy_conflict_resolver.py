"""
AC-143: Tests for Strategy Conflict Resolver

Covers:
  1.  Two strategies same market, no conflict → ALLOW_ALL
  2.  Two strategies same market, opposing bias → BLOCK_BOTH
  3.  Two strategies same market, budget overlap → CAP_BUDGET
  4.  Opposing + budget overlap → BLOCK_BOTH (opposing takes priority)
  5.  Single strategy → no conflict
  6.  Empty intents → no conflict
  7.  Invalid input → no crash
  8.  No key collision in resolved_intents
  9.  Portfolio/execution state consistent (resolved_intents present when not blocked)
  10. Deterministic resolution
  11. research_only=True always
  12. pipeline_impact=False always
  13. conflict_detected=False when ALLOW_ALL
  14. conflict_detected=True when blocked/capped
  15. colony_conflict_summary correct counts
  16. Non-dict intent -> safe fallback
  17. budget_fraction capped to 1/n for CAP_BUDGET
  18. resolved_intents empty after BLOCK_BOTH
  19. Multiple markets processed together
  20. conflicts_detected count correct at colony level
"""
from __future__ import annotations

import sys
from pathlib import Path
import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.build_strategy_conflict_resolver_lite import (
    SNAPSHOT_VERSION,
    FLAGS,
    ACTION_ALLOW_ALL,
    ACTION_BLOCK_BOTH,
    ACTION_CAP_BUDGET,
    CONFLICT_NONE,
    CONFLICT_OPPOSING,
    CONFLICT_BUDGET,
    CONFLICT_BOTH,
    REASON_CODES,
    resolve_conflicts,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = "2025-01-01T00:00:00Z"

REQUIRED_KEYS = {
    "version", "ts_utc", "conflicts_detected", "markets_resolved",
    "colony_conflict_summary", "research_only", "flags",
}
COLONY_KEYS = {
    "total_markets", "markets_with_conflicts", "markets_clean", "total_intents",
}
MARKET_RESULT_KEYS = {
    "conflict_detected", "conflict_type", "resolution_action",
    "resolution_reason", "resolution_reason_code", "resolved_intents",
}


def _intent(strategy="mean_reversion", bias="LONG", budget=0.5) -> dict:
    return {"strategy": strategy, "bias": bias, "budget_fraction": budget}


# ---------------------------------------------------------------------------
# 1. No conflict → ALLOW_ALL
# ---------------------------------------------------------------------------

class TestNoConflict:
    def test_same_direction_no_conflict(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("mean_reversion", "LONG", 0.4),
                        _intent("trend_follow",   "LONG", 0.4)],
        })
        mr = result["markets_resolved"]["BTC-EUR"]
        assert mr["resolution_action"] == ACTION_ALLOW_ALL
        assert mr["conflict_detected"] is False

    def test_neutral_vs_long_no_conflict(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("mean_reversion", "NEUTRAL", 0.3),
                        _intent("trend_follow",   "LONG",    0.3)],
        })
        mr = result["markets_resolved"]["BTC-EUR"]
        assert mr["resolution_action"] == ACTION_ALLOW_ALL

    def test_resolved_intents_present(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.4), _intent("b", "LONG", 0.4)],
        })
        assert len(result["markets_resolved"]["BTC-EUR"]["resolved_intents"]) == 2


# ---------------------------------------------------------------------------
# 2. Opposing direction → BLOCK_BOTH
# ---------------------------------------------------------------------------

class TestOpposingDirection:
    def test_long_short_blocked(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
        })
        mr = result["markets_resolved"]["BTC-EUR"]
        assert mr["resolution_action"] == ACTION_BLOCK_BOTH
        assert mr["conflict_type"] == CONFLICT_OPPOSING

    def test_conflict_detected_true(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
        })
        assert result["markets_resolved"]["BTC-EUR"]["conflict_detected"] is True

    def test_resolved_intents_empty_after_block(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
        })
        assert result["markets_resolved"]["BTC-EUR"]["resolved_intents"] == []

    def test_reason_code_correct(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
        })
        assert result["markets_resolved"]["BTC-EUR"]["resolution_reason_code"] == \
               REASON_CODES["OPPOSING"]


# ---------------------------------------------------------------------------
# 3. Budget overlap → CAP_BUDGET
# ---------------------------------------------------------------------------

class TestBudgetOverlap:
    def test_budget_overflow_capped(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.7), _intent("b", "LONG", 0.7)],
        })
        mr = result["markets_resolved"]["BTC-EUR"]
        assert mr["resolution_action"] == ACTION_CAP_BUDGET
        assert mr["conflict_type"] == CONFLICT_BUDGET

    def test_budget_capped_to_half(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.7), _intent("b", "LONG", 0.7)],
        })
        for intent in result["markets_resolved"]["BTC-EUR"]["resolved_intents"]:
            assert intent["budget_fraction"] == pytest.approx(0.5)

    def test_budget_three_way_capped_to_third(self):
        result = resolve_conflicts({
            "BTC-EUR": [
                _intent("a", "LONG", 0.5),
                _intent("b", "LONG", 0.5),
                _intent("c", "LONG", 0.5),
            ],
        })
        for intent in result["markets_resolved"]["BTC-EUR"]["resolved_intents"]:
            assert intent["budget_fraction"] == pytest.approx(1 / 3, rel=1e-4)

    def test_resolved_intents_present_after_cap(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.7), _intent("b", "LONG", 0.7)],
        })
        assert len(result["markets_resolved"]["BTC-EUR"]["resolved_intents"]) == 2


# ---------------------------------------------------------------------------
# 4. Opposing + budget overlap → BLOCK_BOTH (opposing wins)
# ---------------------------------------------------------------------------

class TestOpposingAndBudget:
    def test_both_conflicts_block_both(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.8), _intent("b", "SHORT", 0.8)],
        })
        mr = result["markets_resolved"]["BTC-EUR"]
        assert mr["resolution_action"] == ACTION_BLOCK_BOTH
        assert mr["conflict_type"] == CONFLICT_BOTH

    def test_reason_code_combined(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.8), _intent("b", "SHORT", 0.8)],
        })
        assert result["markets_resolved"]["BTC-EUR"]["resolution_reason_code"] == \
               REASON_CODES["OPPOSING_BUDGET"]


# ---------------------------------------------------------------------------
# 5. Single strategy → no conflict
# ---------------------------------------------------------------------------

class TestSingleStrategy:
    def test_single_allow_all(self):
        result = resolve_conflicts({"BTC-EUR": [_intent("a", "LONG", 0.5)]})
        mr = result["markets_resolved"]["BTC-EUR"]
        assert mr["resolution_action"] == ACTION_ALLOW_ALL
        assert mr["conflict_detected"] is False

    def test_single_resolved_intents_contains_intent(self):
        result = resolve_conflicts({"BTC-EUR": [_intent("a", "LONG", 0.5)]})
        assert len(result["markets_resolved"]["BTC-EUR"]["resolved_intents"]) == 1


# ---------------------------------------------------------------------------
# 6. Empty intents → no conflict
# ---------------------------------------------------------------------------

class TestEmptyIntents:
    def test_empty_list_no_conflict(self):
        result = resolve_conflicts({"BTC-EUR": []})
        mr = result["markets_resolved"]["BTC-EUR"]
        assert mr["resolution_action"] == ACTION_ALLOW_ALL
        assert mr["conflict_detected"] is False

    def test_empty_list_no_crash(self):
        result = resolve_conflicts({"BTC-EUR": []})
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 7. Invalid input → no crash
# ---------------------------------------------------------------------------

class TestInvalidInput:
    def test_none_input_no_crash(self):
        result = resolve_conflicts(None)
        assert isinstance(result, dict)

    def test_list_input_no_crash(self):
        result = resolve_conflicts([])
        assert isinstance(result, dict)

    def test_empty_dict_no_crash(self):
        result = resolve_conflicts({})
        assert isinstance(result, dict)

    def test_non_list_intents_no_crash(self):
        result = resolve_conflicts({"BTC-EUR": "bad"})
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 8–9. No key collision + resolved_intents structure
# ---------------------------------------------------------------------------

class TestResolvedIntentsStructure:
    def test_no_key_collision_allow(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.4), _intent("b", "LONG", 0.4)],
        })
        intents = result["markets_resolved"]["BTC-EUR"]["resolved_intents"]
        strategies = [i["strategy"] for i in intents]
        assert len(strategies) == len(set(strategies))

    def test_resolved_intents_keys(self):
        result = resolve_conflicts({"BTC-EUR": [_intent("a", "LONG", 0.5)]})
        intent = result["markets_resolved"]["BTC-EUR"]["resolved_intents"][0]
        assert "strategy" in intent
        assert "bias" in intent
        assert "budget_fraction" in intent

    def test_budget_capped_flag(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.8), _intent("b", "LONG", 0.8)],
        })
        for i in result["markets_resolved"]["BTC-EUR"]["resolved_intents"]:
            assert i.get("budget_capped") is True


# ---------------------------------------------------------------------------
# 10. Deterministic
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_output(self):
        intents = {
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
            "ETH-EUR": [_intent("a", "LONG", 0.4)],
        }
        r1 = resolve_conflicts(intents)
        r2 = resolve_conflicts(intents)
        r1c = {k: v for k, v in r1.items() if k != "ts_utc"}
        r2c = {k: v for k, v in r2.items() if k != "ts_utc"}
        assert r1c == r2c


# ---------------------------------------------------------------------------
# 11–12. Flags
# ---------------------------------------------------------------------------

class TestFlags:
    def test_research_only_true(self):
        result = resolve_conflicts({})
        assert result["research_only"] is True

    def test_pipeline_impact_false(self):
        result = resolve_conflicts({})
        assert result["flags"]["pipeline_impact"] is False


# ---------------------------------------------------------------------------
# 13–14. conflict_detected consistency
# ---------------------------------------------------------------------------

class TestConflictDetected:
    def test_no_conflict_detected_false(self):
        result = resolve_conflicts({"BTC-EUR": [_intent("a", "LONG", 0.4)]})
        assert result["markets_resolved"]["BTC-EUR"]["conflict_detected"] is False

    def test_block_conflict_detected_true(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
        })
        assert result["markets_resolved"]["BTC-EUR"]["conflict_detected"] is True

    def test_cap_conflict_detected_true(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.8), _intent("b", "LONG", 0.8)],
        })
        assert result["markets_resolved"]["BTC-EUR"]["conflict_detected"] is True


# ---------------------------------------------------------------------------
# 15. Colony summary counts
# ---------------------------------------------------------------------------

class TestColonySummary:
    def test_colony_summary_keys(self):
        result = resolve_conflicts({})
        assert COLONY_KEYS.issubset(result["colony_conflict_summary"].keys())

    def test_with_conflict_count(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
            "ETH-EUR": [_intent("a", "LONG", 0.4)],
        })
        cs = result["colony_conflict_summary"]
        assert cs["markets_with_conflicts"] == 1
        assert cs["markets_clean"] == 1

    def test_total_markets(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5)],
            "ETH-EUR": [_intent("b", "LONG", 0.5)],
        })
        assert result["colony_conflict_summary"]["total_markets"] == 2

    def test_total_intents(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
            "ETH-EUR": [_intent("c", "LONG", 0.4)],
        })
        assert result["colony_conflict_summary"]["total_intents"] == 3


# ---------------------------------------------------------------------------
# 16. Non-dict intent → safe defaults
# ---------------------------------------------------------------------------

class TestNonDictIntent:
    def test_non_dict_intent_safe(self):
        result = resolve_conflicts({"BTC-EUR": ["bad", None, 42]})
        assert isinstance(result, dict)
        # All intents normalised to NEUTRAL bias → no opposing conflict
        assert result["markets_resolved"]["BTC-EUR"]["conflict_detected"] is False


# ---------------------------------------------------------------------------
# 17–18. Exact budget cap / empty resolved after block
# ---------------------------------------------------------------------------

class TestExactCap:
    def test_cap_value_exact(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.6), _intent("b", "LONG", 0.6)],
        })
        for i in result["markets_resolved"]["BTC-EUR"]["resolved_intents"]:
            assert i["budget_fraction"] == pytest.approx(0.5, abs=1e-5)

    def test_block_resolved_empty(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
        })
        assert result["markets_resolved"]["BTC-EUR"]["resolved_intents"] == []


# ---------------------------------------------------------------------------
# 19–20. Multiple markets + colony-level conflicts_detected
# ---------------------------------------------------------------------------

class TestMultipleMarkets:
    def test_multiple_markets_processed(self):
        result = resolve_conflicts({
            "ADA-EUR": [_intent("a", "LONG", 0.5)],
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
            "ETH-EUR": [_intent("a", "LONG", 0.8), _intent("b", "LONG", 0.8)],
        })
        assert "ADA-EUR" in result["markets_resolved"]
        assert "BTC-EUR" in result["markets_resolved"]
        assert "ETH-EUR" in result["markets_resolved"]

    def test_conflicts_detected_count(self):
        result = resolve_conflicts({
            "BTC-EUR": [_intent("a", "LONG", 0.5), _intent("b", "SHORT", 0.5)],
            "ETH-EUR": [_intent("a", "LONG", 0.8), _intent("b", "LONG", 0.8)],
            "ADA-EUR": [_intent("a", "LONG", 0.4)],
        })
        # BTC: opposing → conflict, ETH: budget → conflict, ADA: clean
        assert result["conflicts_detected"] == 2

    def test_required_keys(self):
        result = resolve_conflicts({})
        assert REQUIRED_KEYS.issubset(result.keys())

    def test_market_result_keys(self):
        result = resolve_conflicts({"BTC-EUR": [_intent("a", "LONG", 0.5)]})
        mr = result["markets_resolved"]["BTC-EUR"]
        assert MARKET_RESULT_KEYS.issubset(mr.keys())
