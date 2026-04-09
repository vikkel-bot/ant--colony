"""
Tests for AC-92: Dry-Run Execution Ledger + Replayable Audit Trace
build_dry_run_ledger_lite.py
"""
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_dry_run_ledger_lite import (
    build_dry_run_ledger,
    build_audit_trace,
    build_ledger_and_trace,
    build_ledger_trace_from_specs,
    LEDGER_COMPLETE, LEDGER_HOLD, LEDGER_REJECTED,
    LEDGER_MODE_READY, LEDGER_MODE_BASELINE, LEDGER_MODE_REJECTED,
    REASON_LEDGER_OK, REASON_LEDGER_EMPTY, REASON_LEDGER_HOLD, REASON_LEDGER_INVALID,
    TRACE_COMPLETE, TRACE_HOLD, TRACE_REJECTED,
    TRACE_MODE_READY, TRACE_MODE_BASELINE, TRACE_MODE_REJECTED,
    REASON_TRACE_OK, REASON_TRACE_EMPTY, REASON_TRACE_HOLD, REASON_TRACE_INVALID,
    CAT_CONSUMED, CAT_SKIPPED, CAT_BLOCKED,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _dry_run_complete(consumed=2, skipped=1, blocked=1):
    consumed_intents = [
        {"market": f"MKT-{i}", "intent_action": "PAPER_INCREASE_INTENT",
         "intent_status": "ALLOWED", "delta_eur": 100.0 * (i + 1), "paper_only": True}
        for i in range(consumed)
    ]
    skipped_intents = [
        {"market": f"SKP-{i}", "intent_action": "PAPER_HOLD_INTENT",
         "intent_status": "ALLOWED", "delta_eur": 0.0, "paper_only": True}
        for i in range(skipped)
    ]
    blocked_intents = [
        {"market": f"BLK-{i}", "intent_action": "PAPER_BLOCKED_INTENT",
         "intent_status": "BLOCKED", "delta_eur": 0.0, "paper_only": True}
        for i in range(blocked)
    ]
    return {
        "dry_run_status":           "DRY_RUN_COMPLETE",
        "dry_run_mode":             "DRY_RUN_READY",
        "dry_run_reason":           "dry-run complete",
        "dry_run_reason_code":      "DRY_RUN_OK",
        "dry_run_consumed_intents": consumed_intents,
        "dry_run_skipped_intents":  skipped_intents,
        "dry_run_blocked_intents":  blocked_intents,
        "dry_run_non_binding":      True,
        "dry_run_simulation_only":  True,
        "paper_only":               True,
        "live_activation_allowed":  False,
    }


def _dry_run_hold():
    return {
        "dry_run_status":           "DRY_RUN_HOLD",
        "dry_run_mode":             "DRY_RUN_BASELINE",
        "dry_run_reason":           "held",
        "dry_run_reason_code":      "DRY_RUN_HOLD_BASELINE",
        "dry_run_consumed_intents": [],
        "dry_run_skipped_intents":  [],
        "dry_run_blocked_intents":  [],
        "dry_run_non_binding":      True,
        "dry_run_simulation_only":  True,
        "paper_only":               True,
        "live_activation_allowed":  False,
    }


def _dry_run_rejected():
    return {
        "dry_run_status":           "DRY_RUN_REJECTED",
        "dry_run_mode":             "DRY_RUN_REJECTED",
        "dry_run_reason":           "rejected",
        "dry_run_reason_code":      "DRY_RUN_INVALID_INTAKE",
        "dry_run_consumed_intents": [],
        "dry_run_skipped_intents":  [],
        "dry_run_blocked_intents":  [],
        "dry_run_non_binding":      True,
        "dry_run_simulation_only":  True,
        "paper_only":               True,
        "live_activation_allowed":  False,
    }


def _complete_ledger(consumed=2, skipped=1, blocked=1):
    return build_dry_run_ledger(_dry_run_complete(consumed=consumed, skipped=skipped, blocked=blocked))


# ---------------------------------------------------------------------------
# 1. Ledger — Always-True / Always-False flags
# ---------------------------------------------------------------------------

class TestLedgerAlwaysFlags:
    def test_complete_non_binding_true(self):
        assert _complete_ledger()["ledger_non_binding"] is True

    def test_complete_simulation_only_true(self):
        assert _complete_ledger()["ledger_simulation_only"] is True

    def test_complete_paper_only_true(self):
        assert _complete_ledger()["paper_only"] is True

    def test_complete_live_activation_false(self):
        assert _complete_ledger()["live_activation_allowed"] is False

    def test_hold_non_binding_true(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert r["ledger_non_binding"] is True

    def test_hold_paper_only_true(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert r["paper_only"] is True

    def test_hold_live_activation_false(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert r["live_activation_allowed"] is False

    def test_rejected_non_binding_true(self):
        r = build_dry_run_ledger(None)
        assert r["ledger_non_binding"] is True

    def test_rejected_paper_only_true(self):
        r = build_dry_run_ledger(None)
        assert r["paper_only"] is True

    def test_rejected_live_activation_false(self):
        r = build_dry_run_ledger(None)
        assert r["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 2. Ledger — LEDGER_COMPLETE
# ---------------------------------------------------------------------------

class TestLedgerComplete:
    def test_status_complete(self):
        assert _complete_ledger()["ledger_status"] == LEDGER_COMPLETE

    def test_mode_ready(self):
        assert _complete_ledger()["ledger_mode"] == LEDGER_MODE_READY

    def test_reason_code_ok(self):
        assert _complete_ledger()["ledger_reason_code"] == REASON_LEDGER_OK

    def test_entry_count_correct(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        assert r["ledger_entry_count"] == 4

    def test_consumed_count_correct(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        assert r["consumed_count"] == 2

    def test_skipped_count_correct(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        assert r["skipped_count"] == 1

    def test_blocked_count_correct(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        assert r["blocked_count"] == 1

    def test_zero_entries_reason_code_empty(self):
        dr = _dry_run_complete(consumed=0, skipped=0, blocked=0)
        r  = build_dry_run_ledger(dr)
        assert r["ledger_reason_code"] == REASON_LEDGER_EMPTY

    def test_entries_is_list(self):
        r = _complete_ledger()
        assert isinstance(r["ledger_entries"], list)

    def test_entry_count_equals_list_length(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        assert r["ledger_entry_count"] == len(r["ledger_entries"])


# ---------------------------------------------------------------------------
# 3. Ledger — ordering convention
# ---------------------------------------------------------------------------

class TestLedgerOrdering:
    def test_consumed_entries_come_first(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        entries = r["ledger_entries"]
        assert entries[0]["ledger_category"] == CAT_CONSUMED
        assert entries[1]["ledger_category"] == CAT_CONSUMED

    def test_skipped_entries_after_consumed(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        entries = r["ledger_entries"]
        assert entries[2]["ledger_category"] == CAT_SKIPPED

    def test_blocked_entries_last(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        entries = r["ledger_entries"]
        assert entries[3]["ledger_category"] == CAT_BLOCKED

    def test_entry_indices_sequential(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        for i, entry in enumerate(r["ledger_entries"]):
            assert entry["entry_index"] == i

    def test_only_consumed_ordering(self):
        r = _complete_ledger(consumed=3, skipped=0, blocked=0)
        categories = [e["ledger_category"] for e in r["ledger_entries"]]
        assert all(c == CAT_CONSUMED for c in categories)

    def test_only_blocked_ordering(self):
        dr = _dry_run_complete(consumed=0, skipped=0, blocked=2)
        r  = build_dry_run_ledger(dr)
        categories = [e["ledger_category"] for e in r["ledger_entries"]]
        assert all(c == CAT_BLOCKED for c in categories)


# ---------------------------------------------------------------------------
# 4. Ledger — entry fields
# ---------------------------------------------------------------------------

class TestLedgerEntryFields:
    def _first_consumed_entry(self):
        return _complete_ledger(consumed=1, skipped=0, blocked=0)["ledger_entries"][0]

    def test_entry_has_index(self):
        assert "entry_index" in self._first_consumed_entry()

    def test_entry_has_category(self):
        assert "ledger_category" in self._first_consumed_entry()

    def test_entry_has_market(self):
        assert "market" in self._first_consumed_entry()

    def test_entry_has_intent_action(self):
        assert "intent_action" in self._first_consumed_entry()

    def test_entry_has_intent_status(self):
        assert "intent_status" in self._first_consumed_entry()

    def test_entry_has_delta_eur(self):
        assert "delta_eur" in self._first_consumed_entry()

    def test_entry_paper_only_true(self):
        assert self._first_consumed_entry()["paper_only"] is True

    def test_entry_market_matches_source(self):
        entry = self._first_consumed_entry()
        assert entry["market"] == "MKT-0"

    def test_entry_delta_eur_correct(self):
        entry = self._first_consumed_entry()
        assert entry["delta_eur"] == pytest.approx(100.0)

    def test_non_dict_intent_becomes_blocked_entry(self):
        dr = _dry_run_complete(consumed=0, skipped=0, blocked=0)
        dr["dry_run_consumed_intents"] = ["not-a-dict"]
        r  = build_dry_run_ledger(dr)
        assert r["ledger_entries"][0]["intent_status"] == "BLOCKED"
        assert r["ledger_entries"][0]["paper_only"] is True


# ---------------------------------------------------------------------------
# 5. Ledger — LEDGER_HOLD
# ---------------------------------------------------------------------------

class TestLedgerHold:
    def test_hold_status(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert r["ledger_status"] == LEDGER_HOLD

    def test_hold_mode(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert r["ledger_mode"] == LEDGER_MODE_BASELINE

    def test_hold_reason_code(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert r["ledger_reason_code"] == REASON_LEDGER_HOLD

    def test_hold_entries_empty(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert r["ledger_entries"] == []

    def test_hold_entry_count_zero(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert r["ledger_entry_count"] == 0

    def test_hold_counts_zero(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert r["consumed_count"] == 0
        assert r["skipped_count"] == 0
        assert r["blocked_count"] == 0


# ---------------------------------------------------------------------------
# 6. Ledger — LEDGER_REJECTED
# ---------------------------------------------------------------------------

class TestLedgerRejected:
    def test_none_gives_rejected(self):
        r = build_dry_run_ledger(None)
        assert r["ledger_status"] == LEDGER_REJECTED

    def test_list_gives_rejected(self):
        r = build_dry_run_ledger([])
        assert r["ledger_status"] == LEDGER_REJECTED

    def test_missing_status_gives_rejected(self):
        dr = _dry_run_complete()
        del dr["dry_run_status"]
        r = build_dry_run_ledger(dr)
        assert r["ledger_status"] == LEDGER_REJECTED

    def test_dry_run_rejected_gives_ledger_rejected(self):
        r = build_dry_run_ledger(_dry_run_rejected())
        assert r["ledger_status"] == LEDGER_REJECTED

    def test_rejected_mode(self):
        r = build_dry_run_ledger(None)
        assert r["ledger_mode"] == LEDGER_MODE_REJECTED

    def test_rejected_reason_code(self):
        r = build_dry_run_ledger(None)
        assert r["ledger_reason_code"] == REASON_LEDGER_INVALID

    def test_rejected_entries_empty(self):
        r = build_dry_run_ledger(None)
        assert r["ledger_entries"] == []


# ---------------------------------------------------------------------------
# 7. Ledger — Output field completeness
# ---------------------------------------------------------------------------

_LEDGER_KEYS = {
    "ledger_status", "ledger_mode",
    "ledger_entries", "ledger_entry_count",
    "ledger_reason", "ledger_reason_code",
    "consumed_count", "skipped_count", "blocked_count",
    "ledger_non_binding", "ledger_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestLedgerFieldCompleteness:
    def test_complete_has_all_keys(self):
        assert _LEDGER_KEYS.issubset(_complete_ledger().keys())

    def test_hold_has_all_keys(self):
        r = build_dry_run_ledger(_dry_run_hold())
        assert _LEDGER_KEYS.issubset(r.keys())

    def test_rejected_has_all_keys(self):
        r = build_dry_run_ledger(None)
        assert _LEDGER_KEYS.issubset(r.keys())


# ---------------------------------------------------------------------------
# 8. Trace — Always-True flags
# ---------------------------------------------------------------------------

class TestTraceAlwaysFlags:
    def test_complete_replayable_true(self):
        ledger = _complete_ledger()
        trace  = build_audit_trace(ledger)
        assert trace["replayable"] is True

    def test_complete_non_binding_true(self):
        trace = build_audit_trace(_complete_ledger())
        assert trace["trace_non_binding"] is True

    def test_complete_simulation_only_true(self):
        trace = build_audit_trace(_complete_ledger())
        assert trace["trace_simulation_only"] is True

    def test_complete_paper_only_true(self):
        trace = build_audit_trace(_complete_ledger())
        assert trace["paper_only"] is True

    def test_complete_live_activation_false(self):
        trace = build_audit_trace(_complete_ledger())
        assert trace["live_activation_allowed"] is False

    def test_hold_replayable_true(self):
        trace = build_audit_trace(build_dry_run_ledger(_dry_run_hold()))
        assert trace["replayable"] is True

    def test_hold_non_binding_true(self):
        trace = build_audit_trace(build_dry_run_ledger(_dry_run_hold()))
        assert trace["trace_non_binding"] is True

    def test_hold_live_activation_false(self):
        trace = build_audit_trace(build_dry_run_ledger(_dry_run_hold()))
        assert trace["live_activation_allowed"] is False

    def test_rejected_replayable_true(self):
        trace = build_audit_trace(None)
        assert trace["replayable"] is True

    def test_rejected_non_binding_true(self):
        trace = build_audit_trace(None)
        assert trace["trace_non_binding"] is True

    def test_rejected_live_activation_false(self):
        trace = build_audit_trace(None)
        assert trace["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 9. Trace — TRACE_COMPLETE
# ---------------------------------------------------------------------------

class TestTraceComplete:
    def test_status_complete(self):
        trace = build_audit_trace(_complete_ledger())
        assert trace["trace_status"] == TRACE_COMPLETE

    def test_mode_ready(self):
        trace = build_audit_trace(_complete_ledger())
        assert trace["trace_mode"] == TRACE_MODE_READY

    def test_reason_code_ok(self):
        trace = build_audit_trace(_complete_ledger(consumed=1))
        assert trace["trace_reason_code"] == REASON_TRACE_OK

    def test_step_count_equals_entry_count(self):
        ledger = _complete_ledger(consumed=2, skipped=1, blocked=1)
        trace  = build_audit_trace(ledger)
        assert trace["trace_step_count"] == ledger["ledger_entry_count"]

    def test_steps_is_list(self):
        trace = build_audit_trace(_complete_ledger())
        assert isinstance(trace["trace_steps"], list)

    def test_step_count_equals_list_length(self):
        trace = build_audit_trace(_complete_ledger(consumed=2, skipped=1, blocked=1))
        assert trace["trace_step_count"] == len(trace["trace_steps"])

    def test_empty_ledger_trace_reason_empty(self):
        dr     = _dry_run_complete(consumed=0, skipped=0, blocked=0)
        ledger = build_dry_run_ledger(dr)
        trace  = build_audit_trace(ledger)
        assert trace["trace_reason_code"] == REASON_TRACE_EMPTY


# ---------------------------------------------------------------------------
# 10. Trace — step fields
# ---------------------------------------------------------------------------

class TestTraceStepFields:
    def _first_step(self):
        return build_audit_trace(_complete_ledger(consumed=1, skipped=0, blocked=0))["trace_steps"][0]

    def test_step_has_index(self):
        assert "step_index" in self._first_step()

    def test_step_has_category(self):
        assert "step_category" in self._first_step()

    def test_step_has_market(self):
        assert "step_market" in self._first_step()

    def test_step_has_action(self):
        assert "step_action" in self._first_step()

    def test_step_has_delta_eur(self):
        assert "step_delta_eur" in self._first_step()

    def test_step_replay_safe_true(self):
        assert self._first_step()["replay_safe"] is True

    def test_step_paper_only_true(self):
        assert self._first_step()["paper_only"] is True

    def test_step_index_matches_entry_index(self):
        ledger = _complete_ledger(consumed=2, skipped=1, blocked=1)
        trace  = build_audit_trace(ledger)
        for step, entry in zip(trace["trace_steps"], ledger["ledger_entries"]):
            assert step["step_index"] == entry["entry_index"]

    def test_step_order_matches_ledger_order(self):
        ledger = _complete_ledger(consumed=2, skipped=1, blocked=1)
        trace  = build_audit_trace(ledger)
        for step, entry in zip(trace["trace_steps"], ledger["ledger_entries"]):
            assert step["step_category"] == entry["ledger_category"]

    def test_step_market_matches_entry(self):
        ledger = _complete_ledger(consumed=1, skipped=0, blocked=0)
        trace  = build_audit_trace(ledger)
        assert trace["trace_steps"][0]["step_market"] == ledger["ledger_entries"][0]["market"]


# ---------------------------------------------------------------------------
# 11. Trace — TRACE_HOLD
# ---------------------------------------------------------------------------

class TestTraceHold:
    def test_hold_status(self):
        trace = build_audit_trace(build_dry_run_ledger(_dry_run_hold()))
        assert trace["trace_status"] == TRACE_HOLD

    def test_hold_mode(self):
        trace = build_audit_trace(build_dry_run_ledger(_dry_run_hold()))
        assert trace["trace_mode"] == TRACE_MODE_BASELINE

    def test_hold_reason_code(self):
        trace = build_audit_trace(build_dry_run_ledger(_dry_run_hold()))
        assert trace["trace_reason_code"] == REASON_TRACE_HOLD

    def test_hold_steps_empty(self):
        trace = build_audit_trace(build_dry_run_ledger(_dry_run_hold()))
        assert trace["trace_steps"] == []

    def test_hold_step_count_zero(self):
        trace = build_audit_trace(build_dry_run_ledger(_dry_run_hold()))
        assert trace["trace_step_count"] == 0


# ---------------------------------------------------------------------------
# 12. Trace — TRACE_REJECTED
# ---------------------------------------------------------------------------

class TestTraceRejected:
    def test_none_gives_rejected(self):
        trace = build_audit_trace(None)
        assert trace["trace_status"] == TRACE_REJECTED

    def test_list_gives_rejected(self):
        trace = build_audit_trace([])
        assert trace["trace_status"] == TRACE_REJECTED

    def test_missing_ledger_status_rejected(self):
        ledger = _complete_ledger()
        del ledger["ledger_status"]
        trace = build_audit_trace(ledger)
        assert trace["trace_status"] == TRACE_REJECTED

    def test_ledger_rejected_gives_trace_rejected(self):
        trace = build_audit_trace(build_dry_run_ledger(None))
        assert trace["trace_status"] == TRACE_REJECTED

    def test_rejected_mode(self):
        trace = build_audit_trace(None)
        assert trace["trace_mode"] == TRACE_MODE_REJECTED

    def test_rejected_reason_code(self):
        trace = build_audit_trace(None)
        assert trace["trace_reason_code"] == REASON_TRACE_INVALID

    def test_rejected_steps_empty(self):
        trace = build_audit_trace(None)
        assert trace["trace_steps"] == []


# ---------------------------------------------------------------------------
# 13. Trace — Output field completeness
# ---------------------------------------------------------------------------

_TRACE_KEYS = {
    "trace_status", "trace_mode",
    "trace_steps", "trace_step_count",
    "trace_reason", "trace_reason_code",
    "replayable", "trace_non_binding", "trace_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestTraceFieldCompleteness:
    def test_complete_has_all_keys(self):
        trace = build_audit_trace(_complete_ledger())
        assert _TRACE_KEYS.issubset(trace.keys())

    def test_hold_has_all_keys(self):
        trace = build_audit_trace(build_dry_run_ledger(_dry_run_hold()))
        assert _TRACE_KEYS.issubset(trace.keys())

    def test_rejected_has_all_keys(self):
        trace = build_audit_trace(None)
        assert _TRACE_KEYS.issubset(trace.keys())


# ---------------------------------------------------------------------------
# 14. Counts consistency
# ---------------------------------------------------------------------------

class TestCountsConsistency:
    def test_entry_count_equals_sum_of_counts(self):
        r = _complete_ledger(consumed=2, skipped=1, blocked=1)
        assert r["ledger_entry_count"] == r["consumed_count"] + r["skipped_count"] + r["blocked_count"]

    def test_trace_step_count_equals_ledger_entry_count(self):
        ledger = _complete_ledger(consumed=2, skipped=1, blocked=1)
        trace  = build_audit_trace(ledger)
        assert trace["trace_step_count"] == ledger["ledger_entry_count"]

    def test_zero_consumed_count(self):
        dr = _dry_run_complete(consumed=0, skipped=2, blocked=1)
        r  = build_dry_run_ledger(dr)
        assert r["consumed_count"] == 0
        assert r["ledger_entry_count"] == 3

    def test_only_consumed_no_skipped_blocked(self):
        dr = _dry_run_complete(consumed=3, skipped=0, blocked=0)
        r  = build_dry_run_ledger(dr)
        assert r["consumed_count"] == 3
        assert r["skipped_count"] == 0
        assert r["blocked_count"] == 0
        assert r["ledger_entry_count"] == 3


# ---------------------------------------------------------------------------
# 15. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_ledger_same_inputs_same_output(self):
        dr = _dry_run_complete()
        l1 = build_dry_run_ledger(dr)
        l2 = build_dry_run_ledger(dr)
        assert l1 == l2

    def test_trace_same_inputs_same_output(self):
        ledger = _complete_ledger()
        t1 = build_audit_trace(ledger)
        t2 = build_audit_trace(ledger)
        assert t1 == t2

    def test_trace_order_stable(self):
        ledger = _complete_ledger(consumed=2, skipped=1, blocked=1)
        t1 = build_audit_trace(ledger)
        t2 = build_audit_trace(ledger)
        assert [s["step_index"] for s in t1["trace_steps"]] == [s["step_index"] for s in t2["trace_steps"]]


# ---------------------------------------------------------------------------
# 16. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_dry_run_not_mutated_by_ledger(self):
        dr = _dry_run_complete(consumed=2, skipped=1, blocked=1)
        original_len = len(dr["dry_run_consumed_intents"])
        build_dry_run_ledger(dr)
        assert len(dr["dry_run_consumed_intents"]) == original_len

    def test_ledger_not_mutated_by_trace(self):
        ledger = _complete_ledger()
        original_count = ledger["ledger_entry_count"]
        build_audit_trace(ledger)
        assert ledger["ledger_entry_count"] == original_count

    def test_ledger_entries_not_mutated_by_trace(self):
        ledger = _complete_ledger(consumed=1, skipped=0, blocked=0)
        original_category = ledger["ledger_entries"][0]["ledger_category"]
        build_audit_trace(ledger)
        assert ledger["ledger_entries"][0]["ledger_category"] == original_category


# ---------------------------------------------------------------------------
# 17. Combined: build_ledger_and_trace
# ---------------------------------------------------------------------------

class TestCombined:
    def test_has_execution_ledger_key(self):
        result = build_ledger_and_trace(_dry_run_complete())
        assert "execution_ledger" in result

    def test_has_audit_trace_key(self):
        result = build_ledger_and_trace(_dry_run_complete())
        assert "audit_trace" in result

    def test_ledger_complete_in_combined(self):
        result = build_ledger_and_trace(_dry_run_complete())
        assert result["execution_ledger"]["ledger_status"] == LEDGER_COMPLETE

    def test_trace_complete_in_combined(self):
        result = build_ledger_and_trace(_dry_run_complete(consumed=1))
        assert result["audit_trace"]["trace_status"] == TRACE_COMPLETE

    def test_combined_hold_safe(self):
        result = build_ledger_and_trace(_dry_run_hold())
        assert result["execution_ledger"]["ledger_status"] == LEDGER_HOLD
        assert result["audit_trace"]["trace_status"] == TRACE_HOLD

    def test_combined_rejected_safe(self):
        result = build_ledger_and_trace(_dry_run_rejected())
        assert result["execution_ledger"]["ledger_status"] == LEDGER_REJECTED
        assert result["audit_trace"]["trace_status"] == TRACE_REJECTED


# ---------------------------------------------------------------------------
# 18. Full chain: build_ledger_trace_from_specs
# ---------------------------------------------------------------------------

_SPECS = [
    {
        "market": "BTC-EUR",
        "strategies": [
            {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION", "weight_fraction": 0.6},
            {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT",        "weight_fraction": 0.4},
        ],
    },
    {
        "market": "ETH-EUR",
        "strategies": [
            {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION"},
        ],
    },
]

_REGIMES = {
    "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
    "ETH-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
}

class TestFullChain:
    def test_has_execution_ledger_key(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "execution_ledger" in result

    def test_has_audit_trace_key(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "audit_trace" in result

    def test_has_all_pipeline_keys(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        for key in [
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit", "queen_handoff",
            "runner_intake", "dry_run_consumption",
            "execution_ledger", "audit_trace",
        ]:
            assert key in result, f"missing key: {key}"

    def test_ledger_live_activation_false(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["execution_ledger"]["live_activation_allowed"] is False

    def test_trace_live_activation_false(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["audit_trace"]["live_activation_allowed"] is False

    def test_ledger_paper_only_true(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["execution_ledger"]["paper_only"] is True

    def test_trace_paper_only_true(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["audit_trace"]["paper_only"] is True

    def test_trace_replayable_true(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["audit_trace"]["replayable"] is True

    def test_trace_non_binding_true(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["audit_trace"]["trace_non_binding"] is True

    def test_ledger_simulation_only_true(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["execution_ledger"]["ledger_simulation_only"] is True

    def test_trace_simulation_only_true(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["audit_trace"]["trace_simulation_only"] is True

    def test_ledger_status_valid(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["execution_ledger"]["ledger_status"] in {
            LEDGER_COMPLETE, LEDGER_HOLD, LEDGER_REJECTED
        }

    def test_trace_status_valid(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["audit_trace"]["trace_status"] in {
            TRACE_COMPLETE, TRACE_HOLD, TRACE_REJECTED
        }

    def test_trace_step_count_equals_ledger_entry_count(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        ledger = result["execution_ledger"]
        trace  = result["audit_trace"]
        if ledger["ledger_status"] == LEDGER_COMPLETE:
            assert trace["trace_step_count"] == ledger["ledger_entry_count"]

    def test_all_trace_steps_replay_safe(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        for step in result["audit_trace"]["trace_steps"]:
            assert step["replay_safe"] is True

    def test_all_trace_steps_paper_only(self):
        result = build_ledger_trace_from_specs(_SPECS, 10_000.0, _REGIMES)
        for step in result["audit_trace"]["trace_steps"]:
            assert step["paper_only"] is True

    def test_empty_specs_safe(self):
        result = build_ledger_trace_from_specs([], 10_000.0)
        assert result["execution_ledger"]["live_activation_allowed"] is False
        assert result["audit_trace"]["replayable"] is True

    def test_zero_equity_safe(self):
        result = build_ledger_trace_from_specs(_SPECS, 0.0, _REGIMES)
        assert result["audit_trace"]["trace_non_binding"] is True
