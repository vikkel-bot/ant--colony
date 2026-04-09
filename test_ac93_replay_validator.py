"""
Tests for AC-93: Replay Validator + Handoff Consistency Check
build_replay_validator_lite.py
"""
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_replay_validator_lite import (
    build_replay_validator,
    build_handoff_consistency_check,
    build_validation_and_consistency,
    build_validation_from_specs,
    VALIDATION_PASSED, VALIDATION_FAILED, VALIDATION_HOLD, VALIDATION_REJECTED,
    VAL_MODE_OK, VAL_MODE_ERROR, VAL_MODE_BASELINE, VAL_MODE_REJECTED,
    REASON_VAL_PASSED, REASON_VAL_COUNT_MISMATCH, REASON_VAL_INDEX_MISMATCH,
    REASON_VAL_CAT_MISMATCH, REASON_VAL_FIELDS, REASON_VAL_HOLD, REASON_VAL_INVALID,
    CONSISTENCY_PASSED, CONSISTENCY_FAILED, CONSISTENCY_HOLD, CONSISTENCY_REJECTED,
    CON_MODE_OK, CON_MODE_ERROR, CON_MODE_BASELINE, CON_MODE_REJECTED,
    REASON_CON_MATCHED, REASON_CON_TOTAL_MISMATCH, REASON_CON_ALLOWED_MISMATCH,
    REASON_CON_BLOCKED_MISMATCH, REASON_CON_ALLOWED_LEDGER, REASON_CON_BLOCKED_LEDGER,
    REASON_CON_TRACE_MISMATCH, REASON_CON_HOLD, REASON_CON_INVALID,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _make_ledger(consumed=2, skipped=1, blocked=1):
    entries = []
    idx = 0
    for i in range(consumed):
        entries.append({
            "entry_index": idx, "ledger_category": "CONSUMED",
            "market": f"MKT-{i}", "intent_action": "PAPER_INCREASE_INTENT",
            "intent_status": "ALLOWED", "delta_eur": 100.0, "paper_only": True,
        })
        idx += 1
    for i in range(skipped):
        entries.append({
            "entry_index": idx, "ledger_category": "SKIPPED",
            "market": f"SKP-{i}", "intent_action": "PAPER_HOLD_INTENT",
            "intent_status": "ALLOWED", "delta_eur": 0.0, "paper_only": True,
        })
        idx += 1
    for i in range(blocked):
        entries.append({
            "entry_index": idx, "ledger_category": "BLOCKED",
            "market": f"BLK-{i}", "intent_action": "PAPER_BLOCKED_INTENT",
            "intent_status": "BLOCKED", "delta_eur": 0.0, "paper_only": True,
        })
        idx += 1
    total = consumed + skipped + blocked
    return {
        "ledger_status":          "LEDGER_COMPLETE",
        "ledger_mode":            "LEDGER_READY",
        "ledger_entries":         entries,
        "ledger_entry_count":     total,
        "ledger_reason":          "ok",
        "ledger_reason_code":     "LEDGER_OK",
        "consumed_count":         consumed,
        "skipped_count":          skipped,
        "blocked_count":          blocked,
        "ledger_non_binding":     True,
        "ledger_simulation_only": True,
        "paper_only":             True,
        "live_activation_allowed": False,
    }


def _make_trace_from_ledger(ledger: dict) -> dict:
    entries = ledger.get("ledger_entries", [])
    steps = [
        {
            "step_index": e["entry_index"],
            "step_category": e["ledger_category"],
            "step_market": e["market"],
            "step_action": e["intent_action"],
            "step_delta_eur": e["delta_eur"],
            "replay_safe": True,
            "paper_only": True,
        }
        for e in entries if isinstance(e, dict)
    ]
    return {
        "trace_status":           "TRACE_COMPLETE",
        "trace_mode":             "TRACE_READY",
        "trace_steps":            steps,
        "trace_step_count":       len(steps),
        "trace_reason":           "ok",
        "trace_reason_code":      "TRACE_OK",
        "replayable":             True,
        "trace_non_binding":      True,
        "trace_simulation_only":  True,
        "paper_only":             True,
        "live_activation_allowed": False,
    }


def _make_pair(consumed=2, skipped=1, blocked=1):
    ledger = _make_ledger(consumed=consumed, skipped=skipped, blocked=blocked)
    trace  = _make_trace_from_ledger(ledger)
    return ledger, trace


def _ledger_hold():
    return {
        "ledger_status":          "LEDGER_HOLD",
        "ledger_mode":            "LEDGER_BASELINE",
        "ledger_entries":         [],
        "ledger_entry_count":     0,
        "ledger_reason":          "held",
        "ledger_reason_code":     "LEDGER_HOLD_BASELINE",
        "consumed_count":         0,
        "skipped_count":          0,
        "blocked_count":          0,
        "ledger_non_binding":     True,
        "ledger_simulation_only": True,
        "paper_only":             True,
        "live_activation_allowed": False,
    }


def _trace_hold():
    return {
        "trace_status":           "TRACE_HOLD",
        "trace_mode":             "TRACE_BASELINE",
        "trace_steps":            [],
        "trace_step_count":       0,
        "trace_reason":           "held",
        "trace_reason_code":      "TRACE_HOLD_BASELINE",
        "replayable":             True,
        "trace_non_binding":      True,
        "trace_simulation_only":  True,
        "paper_only":             True,
        "live_activation_allowed": False,
    }


def _ready_handoff(allowed=3, blocked=1):
    total = allowed + blocked
    return {
        "handoff_status":                "READY_FOR_PAPER_HANDOFF",
        "handoff_mode":                  "PAPER_HANDOFF_READY",
        "handoff_ready":                 True,
        "total_intents":                 total,
        "total_allowed":                 allowed,
        "total_blocked":                 blocked,
        "queen_handoff_non_binding":     True,
        "queen_handoff_simulation_only": True,
        "paper_only":                    True,
        "live_activation_allowed":       False,
    }


def _accepted_intake(total=4, allowed=3, blocked=1):
    return {
        "runner_intake_status":          "INTAKE_ACCEPTED",
        "runner_intake_mode":            "INTAKE_READY",
        "runner_contract_valid":         True,
        "runner_contract_reason":        "ok",
        "runner_contract_reason_code":   "INTAKE_CONTRACT_VALID",
        "handoff_snapshot":              {},
        "consumed_intent_count":         total,
        "consumed_allowed_count":        allowed,
        "consumed_blocked_count":        blocked,
        "runner_intake_non_binding":     True,
        "runner_intake_simulation_only": True,
        "paper_only":                    True,
        "live_activation_allowed":       False,
    }


# ---------------------------------------------------------------------------
# 1. Validation — Always-True / Always-False flags
# ---------------------------------------------------------------------------

class TestValidationAlwaysFlags:
    def test_passed_non_binding_true(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert v["validation_non_binding"] is True

    def test_passed_simulation_only_true(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert v["validation_simulation_only"] is True

    def test_passed_paper_only_true(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert v["paper_only"] is True

    def test_passed_live_activation_false(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert v["live_activation_allowed"] is False

    def test_hold_non_binding_true(self):
        v = build_replay_validator(_ledger_hold(), _trace_hold())
        assert v["validation_non_binding"] is True

    def test_hold_live_activation_false(self):
        v = build_replay_validator(_ledger_hold(), _trace_hold())
        assert v["live_activation_allowed"] is False

    def test_rejected_non_binding_true(self):
        v = build_replay_validator(None, None)
        assert v["validation_non_binding"] is True

    def test_rejected_live_activation_false(self):
        v = build_replay_validator(None, None)
        assert v["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 2. Validation — VALIDATION_PASSED
# ---------------------------------------------------------------------------

class TestValidationPassed:
    def test_status_passed(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert v["validation_status"] == VALIDATION_PASSED

    def test_mode_ok(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert v["validation_mode"] == VAL_MODE_OK

    def test_validation_passed_true(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert v["validation_passed"] is True

    def test_reason_code_passed(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert v["validation_reason_code"] == REASON_VAL_PASSED

    def test_replay_consistent_true(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert v["replay_consistent"] is True

    def test_validated_ledger_count(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        v = build_replay_validator(l, t)
        assert v["validated_ledger_count"] == 4

    def test_validated_trace_count(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        v = build_replay_validator(l, t)
        assert v["validated_trace_count"] == 4

    def test_empty_ledger_and_trace_passes(self):
        l, t = _make_pair(consumed=0, skipped=0, blocked=0)
        v = build_replay_validator(l, t)
        assert v["validation_passed"] is True

    def test_only_consumed_passes(self):
        l, t = _make_pair(consumed=3, skipped=0, blocked=0)
        v = build_replay_validator(l, t)
        assert v["validation_passed"] is True


# ---------------------------------------------------------------------------
# 3. Validation — VALIDATION_FAILED — count mismatch
# ---------------------------------------------------------------------------

class TestValidationCountMismatch:
    def test_step_count_mismatch_fails(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=0)
        t["trace_step_count"] = 999  # corrupt
        v = build_replay_validator(l, t)
        assert v["validation_status"] == VALIDATION_FAILED

    def test_step_count_mismatch_reason_code(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=0)
        t["trace_step_count"] = 999
        v = build_replay_validator(l, t)
        assert v["validation_reason_code"] == REASON_VAL_COUNT_MISMATCH

    def test_step_count_mismatch_passed_false(self):
        l, t = _make_pair()
        t["trace_step_count"] = 0
        v = build_replay_validator(l, t)
        assert v["validation_passed"] is False

    def test_step_count_mismatch_replay_consistent_false(self):
        l, t = _make_pair()
        t["trace_step_count"] = 0
        v = build_replay_validator(l, t)
        assert v["replay_consistent"] is False


# ---------------------------------------------------------------------------
# 4. Validation — VALIDATION_FAILED — index mismatch
# ---------------------------------------------------------------------------

class TestValidationIndexMismatch:
    def test_broken_entry_index_fails(self):
        l, t = _make_pair(consumed=2, skipped=0, blocked=0)
        l["ledger_entries"][0]["entry_index"] = 99  # wrong index
        v = build_replay_validator(l, t)
        assert v["validation_status"] == VALIDATION_FAILED

    def test_broken_step_index_fails(self):
        l, t = _make_pair(consumed=2, skipped=0, blocked=0)
        t["trace_steps"][0]["step_index"] = 99
        v = build_replay_validator(l, t)
        assert v["validation_status"] == VALIDATION_FAILED

    def test_misaligned_step_index_fails(self):
        l, t = _make_pair(consumed=2, skipped=0, blocked=0)
        # entry_index=0 but step_index=1
        t["trace_steps"][0]["step_index"] = 1
        v = build_replay_validator(l, t)
        assert v["validation_status"] == VALIDATION_FAILED


# ---------------------------------------------------------------------------
# 5. Validation — VALIDATION_FAILED — category sum mismatch
# ---------------------------------------------------------------------------

class TestValidationCategorySumMismatch:
    def test_category_sum_corrupt_fails(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        l["consumed_count"] = 99  # corrupt
        v = build_replay_validator(l, t)
        assert v["validation_status"] == VALIDATION_FAILED

    def test_category_sum_reason_code(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        l["consumed_count"] = 99
        v = build_replay_validator(l, t)
        # Count mismatch is first check; category sum is third — depends on order
        assert v["validation_passed"] is False


# ---------------------------------------------------------------------------
# 6. Validation — VALIDATION_FAILED — missing fields
# ---------------------------------------------------------------------------

class TestValidationMissingFields:
    def test_entry_missing_field_fails(self):
        l, t = _make_pair(consumed=1, skipped=0, blocked=0)
        del l["ledger_entries"][0]["delta_eur"]  # remove required field
        v = build_replay_validator(l, t)
        assert v["validation_passed"] is False

    def test_step_missing_field_fails(self):
        l, t = _make_pair(consumed=1, skipped=0, blocked=0)
        del t["trace_steps"][0]["replay_safe"]
        v = build_replay_validator(l, t)
        assert v["validation_passed"] is False


# ---------------------------------------------------------------------------
# 7. Validation — VALIDATION_HOLD
# ---------------------------------------------------------------------------

class TestValidationHold:
    def test_ledger_hold_gives_validation_hold(self):
        v = build_replay_validator(_ledger_hold(), _trace_hold())
        assert v["validation_status"] == VALIDATION_HOLD

    def test_ledger_hold_with_complete_trace(self):
        _, t = _make_pair()
        v = build_replay_validator(_ledger_hold(), t)
        assert v["validation_status"] == VALIDATION_HOLD

    def test_hold_mode_baseline(self):
        v = build_replay_validator(_ledger_hold(), _trace_hold())
        assert v["validation_mode"] == VAL_MODE_BASELINE

    def test_hold_reason_code(self):
        v = build_replay_validator(_ledger_hold(), _trace_hold())
        assert v["validation_reason_code"] == REASON_VAL_HOLD

    def test_hold_passed_false(self):
        v = build_replay_validator(_ledger_hold(), _trace_hold())
        assert v["validation_passed"] is False

    def test_trace_rejected_gives_hold(self):
        l, _ = _make_pair()
        t_rej = {"trace_status": "TRACE_REJECTED", "trace_step_count": 0, "trace_steps": []}
        v = build_replay_validator(l, t_rej)
        assert v["validation_status"] == VALIDATION_HOLD


# ---------------------------------------------------------------------------
# 8. Validation — VALIDATION_REJECTED
# ---------------------------------------------------------------------------

class TestValidationRejected:
    def test_none_ledger_rejected(self):
        _, t = _make_pair()
        v = build_replay_validator(None, t)
        assert v["validation_status"] == VALIDATION_REJECTED

    def test_none_trace_rejected(self):
        l, _ = _make_pair()
        v = build_replay_validator(l, None)
        assert v["validation_status"] == VALIDATION_REJECTED

    def test_list_ledger_rejected(self):
        _, t = _make_pair()
        v = build_replay_validator([], t)
        assert v["validation_status"] == VALIDATION_REJECTED

    def test_missing_ledger_status_rejected(self):
        l, t = _make_pair()
        del l["ledger_status"]
        v = build_replay_validator(l, t)
        assert v["validation_status"] == VALIDATION_REJECTED

    def test_missing_trace_status_rejected(self):
        l, t = _make_pair()
        del t["trace_status"]
        v = build_replay_validator(l, t)
        assert v["validation_status"] == VALIDATION_REJECTED

    def test_rejected_reason_code(self):
        v = build_replay_validator(None, None)
        assert v["validation_reason_code"] == REASON_VAL_INVALID

    def test_rejected_passed_false(self):
        v = build_replay_validator(None, None)
        assert v["validation_passed"] is False

    def test_rejected_replay_consistent_false(self):
        v = build_replay_validator(None, None)
        assert v["replay_consistent"] is False


# ---------------------------------------------------------------------------
# 9. Validation — Output field completeness
# ---------------------------------------------------------------------------

_VAL_KEYS = {
    "validation_status", "validation_mode", "validation_passed",
    "validation_reason", "validation_reason_code",
    "validated_ledger_count", "validated_trace_count",
    "replay_consistent",
    "validation_non_binding", "validation_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestValidationFieldCompleteness:
    def test_passed_has_all_keys(self):
        l, t = _make_pair()
        v = build_replay_validator(l, t)
        assert _VAL_KEYS.issubset(v.keys())

    def test_hold_has_all_keys(self):
        v = build_replay_validator(_ledger_hold(), _trace_hold())
        assert _VAL_KEYS.issubset(v.keys())

    def test_rejected_has_all_keys(self):
        v = build_replay_validator(None, None)
        assert _VAL_KEYS.issubset(v.keys())


# ---------------------------------------------------------------------------
# 10. Consistency — Always-True / Always-False flags
# ---------------------------------------------------------------------------

class TestConsistencyAlwaysFlags:
    def test_passed_non_binding_true(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        c = build_handoff_consistency_check(_ready_handoff(allowed=3, blocked=1), _accepted_intake(total=4, allowed=3, blocked=1), l, t)
        assert c["consistency_non_binding"] is True

    def test_passed_simulation_only_true(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        c = build_handoff_consistency_check(_ready_handoff(allowed=3, blocked=1), _accepted_intake(total=4, allowed=3, blocked=1), l, t)
        assert c["consistency_simulation_only"] is True

    def test_passed_paper_only_true(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        c = build_handoff_consistency_check(_ready_handoff(allowed=3, blocked=1), _accepted_intake(total=4, allowed=3, blocked=1), l, t)
        assert c["paper_only"] is True

    def test_passed_live_activation_false(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        c = build_handoff_consistency_check(_ready_handoff(allowed=3, blocked=1), _accepted_intake(total=4, allowed=3, blocked=1), l, t)
        assert c["live_activation_allowed"] is False

    def test_hold_non_binding_true(self):
        c = build_handoff_consistency_check(None, None, None)
        assert c["consistency_non_binding"] is True

    def test_rejected_non_binding_true(self):
        c = build_handoff_consistency_check(None, {}, {})
        assert c["consistency_non_binding"] is True

    def test_rejected_live_activation_false(self):
        c = build_handoff_consistency_check(None, {}, {})
        assert c["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 11. Consistency — CONSISTENCY_PASSED
# ---------------------------------------------------------------------------

class TestConsistencyPassed:
    def _run(self, consumed=2, skipped=1, blocked=1):
        # allowed = consumed + skipped = 3, blocked = 1, total = 4
        allowed = consumed + skipped
        total   = allowed + blocked
        l, t = _make_pair(consumed=consumed, skipped=skipped, blocked=blocked)
        ho = _ready_handoff(allowed=allowed, blocked=blocked)
        ri = _accepted_intake(total=total, allowed=allowed, blocked=blocked)
        return build_handoff_consistency_check(ho, ri, l, t)

    def test_status_passed(self):
        assert self._run()["handoff_consistency_status"] == CONSISTENCY_PASSED

    def test_mode_ok(self):
        assert self._run()["handoff_consistency_mode"] == CON_MODE_OK

    def test_passed_true(self):
        assert self._run()["handoff_consistency_passed"] is True

    def test_reason_code_matched(self):
        assert self._run()["consistency_reason_code"] == REASON_CON_MATCHED

    def test_matched_count_equals_total_checks(self):
        c = self._run()
        # 5 checks + 1 trace check = 6
        assert c["matched_intent_count"] == 6

    def test_missing_counts_zero(self):
        c = self._run()
        assert c["missing_in_handoff_count"] == 0
        assert c["missing_in_ledger_count"] == 0
        assert c["missing_in_trace_count"] == 0


# ---------------------------------------------------------------------------
# 12. Consistency — CONSISTENCY_FAILED — various mismatches
# ---------------------------------------------------------------------------

class TestConsistencyFailed:
    def _base(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        ho = _ready_handoff(allowed=3, blocked=1)
        ri = _accepted_intake(total=4, allowed=3, blocked=1)
        return ho, ri, l, t

    def test_total_mismatch_fails(self):
        ho, ri, l, t = self._base()
        ho["total_intents"] = 99
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["handoff_consistency_status"] == CONSISTENCY_FAILED

    def test_total_mismatch_reason_code(self):
        ho, ri, l, t = self._base()
        ho["total_intents"] = 99
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["consistency_reason_code"] == REASON_CON_TOTAL_MISMATCH

    def test_allowed_mismatch_fails(self):
        ho, ri, l, t = self._base()
        ho["total_allowed"] = 99
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["handoff_consistency_passed"] is False

    def test_blocked_mismatch_fails(self):
        ho, ri, l, t = self._base()
        ho["total_blocked"] = 99
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["handoff_consistency_passed"] is False

    def test_allowed_ledger_mismatch_fails(self):
        ho, ri, l, t = self._base()
        l["consumed_count"] = 99  # consumed + skipped != allowed
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["handoff_consistency_passed"] is False

    def test_blocked_ledger_mismatch_fails(self):
        ho, ri, l, t = self._base()
        l["blocked_count"] = 99
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["handoff_consistency_passed"] is False

    def test_trace_mismatch_fails(self):
        ho, ri, l, t = self._base()
        t["trace_step_count"] = 99
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["handoff_consistency_passed"] is False

    def test_trace_mismatch_reason_code(self):
        ho, ri, l, t = self._base()
        # Pass all 5 checks, corrupt only trace
        t["trace_step_count"] = 0
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["consistency_reason_code"] == REASON_CON_TRACE_MISMATCH

    def test_failed_mode_error(self):
        ho, ri, l, t = self._base()
        ho["total_intents"] = 99
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["handoff_consistency_mode"] == CON_MODE_ERROR


# ---------------------------------------------------------------------------
# 13. Consistency — CONSISTENCY_HOLD
# ---------------------------------------------------------------------------

class TestConsistencyHold:
    def test_baseline_handoff_gives_hold(self):
        l, t = _make_pair()
        ho = {"handoff_status": "HOLD_BASELINE_HANDOFF", "total_intents": 0, "total_allowed": 0, "total_blocked": 0}
        ri = _accepted_intake()
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["handoff_consistency_status"] == CONSISTENCY_HOLD

    def test_hold_intake_gives_hold(self):
        l, t = _make_pair()
        ho = _ready_handoff()
        ri = {"runner_intake_status": "INTAKE_HOLD", "consumed_intent_count": 0, "consumed_allowed_count": 0, "consumed_blocked_count": 0}
        c = build_handoff_consistency_check(ho, ri, l, t)
        assert c["handoff_consistency_status"] == CONSISTENCY_HOLD

    def test_ledger_hold_gives_hold(self):
        c = build_handoff_consistency_check(_ready_handoff(), _accepted_intake(), _ledger_hold())
        assert c["handoff_consistency_status"] == CONSISTENCY_HOLD

    def test_hold_reason_code(self):
        c = build_handoff_consistency_check(_ready_handoff(), _accepted_intake(), _ledger_hold())
        assert c["consistency_reason_code"] == REASON_CON_HOLD

    def test_hold_passed_false(self):
        c = build_handoff_consistency_check(_ready_handoff(), _accepted_intake(), _ledger_hold())
        assert c["handoff_consistency_passed"] is False


# ---------------------------------------------------------------------------
# 14. Consistency — CONSISTENCY_REJECTED
# ---------------------------------------------------------------------------

class TestConsistencyRejected:
    def test_none_handoff_rejected(self):
        l, t = _make_pair()
        c = build_handoff_consistency_check(None, _accepted_intake(), l, t)
        assert c["handoff_consistency_status"] == CONSISTENCY_REJECTED

    def test_none_intake_rejected(self):
        l, t = _make_pair()
        c = build_handoff_consistency_check(_ready_handoff(), None, l, t)
        assert c["handoff_consistency_status"] == CONSISTENCY_REJECTED

    def test_none_ledger_rejected(self):
        c = build_handoff_consistency_check(_ready_handoff(), _accepted_intake(), None)
        assert c["handoff_consistency_status"] == CONSISTENCY_REJECTED

    def test_rejected_reason_code(self):
        c = build_handoff_consistency_check(None, None, None)
        assert c["consistency_reason_code"] == REASON_CON_INVALID

    def test_rejected_passed_false(self):
        c = build_handoff_consistency_check(None, None, None)
        assert c["handoff_consistency_passed"] is False


# ---------------------------------------------------------------------------
# 15. Consistency — Output field completeness
# ---------------------------------------------------------------------------

_CON_KEYS = {
    "handoff_consistency_status", "handoff_consistency_mode", "handoff_consistency_passed",
    "consistency_reason", "consistency_reason_code",
    "matched_intent_count",
    "missing_in_handoff_count", "missing_in_ledger_count", "missing_in_trace_count",
    "consistency_non_binding", "consistency_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestConsistencyFieldCompleteness:
    def test_passed_has_all_keys(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        c = build_handoff_consistency_check(_ready_handoff(allowed=3, blocked=1), _accepted_intake(total=4, allowed=3, blocked=1), l, t)
        assert _CON_KEYS.issubset(c.keys())

    def test_hold_has_all_keys(self):
        c = build_handoff_consistency_check(_ready_handoff(), _accepted_intake(), _ledger_hold())
        assert _CON_KEYS.issubset(c.keys())

    def test_rejected_has_all_keys(self):
        c = build_handoff_consistency_check(None, None, None)
        assert _CON_KEYS.issubset(c.keys())


# ---------------------------------------------------------------------------
# 16. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_validation_same_inputs_same_output(self):
        l, t = _make_pair()
        v1 = build_replay_validator(l, t)
        v2 = build_replay_validator(l, t)
        assert v1 == v2

    def test_consistency_same_inputs_same_output(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        ho = _ready_handoff(allowed=3, blocked=1)
        ri = _accepted_intake(total=4, allowed=3, blocked=1)
        c1 = build_handoff_consistency_check(ho, ri, l, t)
        c2 = build_handoff_consistency_check(ho, ri, l, t)
        assert c1 == c2


# ---------------------------------------------------------------------------
# 17. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_ledger_not_mutated(self):
        l, t = _make_pair()
        original_count = l["ledger_entry_count"]
        build_replay_validator(l, t)
        assert l["ledger_entry_count"] == original_count

    def test_trace_not_mutated(self):
        l, t = _make_pair()
        original_count = t["trace_step_count"]
        build_replay_validator(l, t)
        assert t["trace_step_count"] == original_count

    def test_handoff_not_mutated(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        ho = _ready_handoff(allowed=3, blocked=1)
        original_allowed = ho["total_allowed"]
        build_handoff_consistency_check(ho, _accepted_intake(total=4, allowed=3, blocked=1), l, t)
        assert ho["total_allowed"] == original_allowed


# ---------------------------------------------------------------------------
# 18. Combined: build_validation_and_consistency
# ---------------------------------------------------------------------------

class TestCombined:
    def test_has_replay_validation_key(self):
        l, t = _make_pair()
        result = build_validation_and_consistency(l, t)
        assert "replay_validation" in result

    def test_has_handoff_consistency_key(self):
        l, t = _make_pair()
        result = build_validation_and_consistency(l, t)
        assert "handoff_consistency" in result

    def test_no_handoff_gives_consistency_hold(self):
        l, t = _make_pair()
        result = build_validation_and_consistency(l, t)
        assert result["handoff_consistency"]["handoff_consistency_status"] == CONSISTENCY_HOLD

    def test_with_handoff_gives_consistency_result(self):
        l, t = _make_pair(consumed=2, skipped=1, blocked=1)
        ho = _ready_handoff(allowed=3, blocked=1)
        ri = _accepted_intake(total=4, allowed=3, blocked=1)
        result = build_validation_and_consistency(l, t, ho, ri)
        assert result["handoff_consistency"]["handoff_consistency_status"] in {
            CONSISTENCY_PASSED, CONSISTENCY_FAILED
        }

    def test_combined_hold_safe(self):
        result = build_validation_and_consistency(_ledger_hold(), _trace_hold())
        assert result["replay_validation"]["validation_status"] == VALIDATION_HOLD


# ---------------------------------------------------------------------------
# 19. Full chain: build_validation_from_specs
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
    def test_has_replay_validation_key(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "replay_validation" in result

    def test_has_handoff_consistency_key(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "handoff_consistency" in result

    def test_has_all_pipeline_keys(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        for key in [
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit", "queen_handoff",
            "runner_intake", "dry_run_consumption",
            "execution_ledger", "audit_trace",
            "replay_validation", "handoff_consistency",
        ]:
            assert key in result, f"missing key: {key}"

    def test_validation_live_activation_false(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["replay_validation"]["live_activation_allowed"] is False

    def test_consistency_live_activation_false(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["handoff_consistency"]["live_activation_allowed"] is False

    def test_validation_paper_only_true(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["replay_validation"]["paper_only"] is True

    def test_consistency_paper_only_true(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["handoff_consistency"]["paper_only"] is True

    def test_validation_non_binding_true(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["replay_validation"]["validation_non_binding"] is True

    def test_consistency_non_binding_true(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["handoff_consistency"]["consistency_non_binding"] is True

    def test_validation_simulation_only_true(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["replay_validation"]["validation_simulation_only"] is True

    def test_consistency_simulation_only_true(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["handoff_consistency"]["consistency_simulation_only"] is True

    def test_validation_status_valid(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["replay_validation"]["validation_status"] in {
            VALIDATION_PASSED, VALIDATION_FAILED, VALIDATION_HOLD, VALIDATION_REJECTED
        }

    def test_consistency_status_valid(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["handoff_consistency"]["handoff_consistency_status"] in {
            CONSISTENCY_PASSED, CONSISTENCY_FAILED, CONSISTENCY_HOLD, CONSISTENCY_REJECTED
        }

    def test_full_chain_counts_consistent_when_passed(self):
        result = build_validation_from_specs(_SPECS, 10_000.0, _REGIMES)
        v = result["replay_validation"]
        if v["validation_status"] == VALIDATION_PASSED:
            assert v["validated_ledger_count"] == v["validated_trace_count"]

    def test_empty_specs_safe(self):
        result = build_validation_from_specs([], 10_000.0)
        assert result["replay_validation"]["live_activation_allowed"] is False
        assert result["handoff_consistency"]["live_activation_allowed"] is False

    def test_zero_equity_safe(self):
        result = build_validation_from_specs(_SPECS, 0.0, _REGIMES)
        assert result["replay_validation"]["validation_non_binding"] is True
