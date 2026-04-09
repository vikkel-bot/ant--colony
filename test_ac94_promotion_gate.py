"""
Tests for AC-94: Promotion Gate for Paper-Ready Candidate
build_promotion_gate_lite.py
"""
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_promotion_gate_lite import (
    build_promotion_gate,
    build_promotion_from_specs,
    PAPER_READY, PAPER_HOLD, PAPER_REJECTED,
    MODE_READY, MODE_HOLD, MODE_REJECTED,
    REASON_ALL_CLEAR, REASON_UPSTREAM_HOLD, REASON_HANDOFF_NOT_READY,
    REASON_VALIDATION_FAILED, REASON_CONSISTENCY_FAILED,
    REASON_UPSTREAM_REJECTED, REASON_INVALID_INPUT,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _ready_handoff():
    return {
        "handoff_status":                "READY_FOR_PAPER_HANDOFF",
        "handoff_mode":                  "PAPER_HANDOFF_READY",
        "handoff_ready":                 True,
        "total_intents":                 4,
        "total_allowed":                 3,
        "total_blocked":                 1,
        "queen_handoff_non_binding":     True,
        "queen_handoff_simulation_only": True,
        "paper_only":                    True,
        "live_activation_allowed":       False,
    }


def _baseline_handoff():
    return {
        "handoff_status":                "HOLD_BASELINE_HANDOFF",
        "handoff_mode":                  "HANDOFF_BASELINE",
        "handoff_ready":                 False,
        "total_intents":                 0,
        "total_allowed":                 0,
        "total_blocked":                 0,
        "queen_handoff_non_binding":     True,
        "queen_handoff_simulation_only": True,
        "paper_only":                    True,
        "live_activation_allowed":       False,
    }


def _rejected_handoff():
    return {
        "handoff_status":                "REJECT_HANDOFF",
        "handoff_mode":                  "HANDOFF_REJECTED",
        "handoff_ready":                 False,
        "total_intents":                 0,
        "total_allowed":                 0,
        "total_blocked":                 0,
        "queen_handoff_non_binding":     True,
        "queen_handoff_simulation_only": True,
        "paper_only":                    True,
        "live_activation_allowed":       False,
    }


def _accepted_intake():
    return {
        "runner_intake_status":          "INTAKE_ACCEPTED",
        "runner_intake_mode":            "INTAKE_READY",
        "runner_contract_valid":         True,
        "runner_contract_reason":        "ok",
        "runner_contract_reason_code":   "INTAKE_CONTRACT_VALID",
        "handoff_snapshot":              {},
        "consumed_intent_count":         4,
        "consumed_allowed_count":        3,
        "consumed_blocked_count":        1,
        "runner_intake_non_binding":     True,
        "runner_intake_simulation_only": True,
        "paper_only":                    True,
        "live_activation_allowed":       False,
    }


def _hold_intake():
    return {
        "runner_intake_status":          "INTAKE_HOLD",
        "runner_intake_mode":            "INTAKE_BASELINE",
        "runner_contract_valid":         False,
        "runner_contract_reason":        "held",
        "runner_contract_reason_code":   "INTAKE_CONTRACT_HOLD_BASELINE",
        "handoff_snapshot":              {},
        "consumed_intent_count":         0,
        "consumed_allowed_count":        0,
        "consumed_blocked_count":        0,
        "runner_intake_non_binding":     True,
        "runner_intake_simulation_only": True,
        "paper_only":                    True,
        "live_activation_allowed":       False,
    }


def _rejected_intake():
    return {
        "runner_intake_status":          "INTAKE_REJECTED",
        "runner_intake_mode":            "INTAKE_REJECTED",
        "runner_contract_valid":         False,
        "runner_contract_reason":        "rejected",
        "runner_contract_reason_code":   "INTAKE_CONTRACT_INVALID_INPUT",
        "handoff_snapshot":              {},
        "consumed_intent_count":         0,
        "consumed_allowed_count":        0,
        "consumed_blocked_count":        0,
        "runner_intake_non_binding":     True,
        "runner_intake_simulation_only": True,
        "paper_only":                    True,
        "live_activation_allowed":       False,
    }


def _passed_validation():
    return {
        "validation_status":          "VALIDATION_PASSED",
        "validation_mode":            "VALIDATION_OK",
        "validation_passed":          True,
        "validation_reason":          "ok",
        "validation_reason_code":     "VALIDATION_ALL_CHECKS_PASSED",
        "validated_ledger_count":     4,
        "validated_trace_count":      4,
        "replay_consistent":          True,
        "validation_non_binding":     True,
        "validation_simulation_only": True,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _failed_validation():
    return {
        "validation_status":          "VALIDATION_FAILED",
        "validation_mode":            "VALIDATION_ERROR",
        "validation_passed":          False,
        "validation_reason":          "count mismatch",
        "validation_reason_code":     "VALIDATION_COUNT_MISMATCH",
        "validated_ledger_count":     4,
        "validated_trace_count":      3,
        "replay_consistent":          False,
        "validation_non_binding":     True,
        "validation_simulation_only": True,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _hold_validation():
    return {
        "validation_status":          "VALIDATION_HOLD",
        "validation_mode":            "VALIDATION_BASELINE",
        "validation_passed":          False,
        "validation_reason":          "held",
        "validation_reason_code":     "VALIDATION_HOLD_BASELINE",
        "validated_ledger_count":     0,
        "validated_trace_count":      0,
        "replay_consistent":          False,
        "validation_non_binding":     True,
        "validation_simulation_only": True,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _passed_consistency():
    return {
        "handoff_consistency_status":  "CONSISTENCY_PASSED",
        "handoff_consistency_mode":    "CONSISTENCY_OK",
        "handoff_consistency_passed":  True,
        "consistency_reason":          "ok",
        "consistency_reason_code":     "CONSISTENCY_ALL_MATCHED",
        "matched_intent_count":        6,
        "missing_in_handoff_count":    0,
        "missing_in_ledger_count":     0,
        "missing_in_trace_count":      0,
        "consistency_non_binding":     True,
        "consistency_simulation_only": True,
        "paper_only":                  True,
        "live_activation_allowed":     False,
    }


def _failed_consistency():
    return {
        "handoff_consistency_status":  "CONSISTENCY_FAILED",
        "handoff_consistency_mode":    "CONSISTENCY_ERROR",
        "handoff_consistency_passed":  False,
        "consistency_reason":          "total mismatch",
        "consistency_reason_code":     "CONSISTENCY_TOTAL_MISMATCH",
        "matched_intent_count":        0,
        "missing_in_handoff_count":    1,
        "missing_in_ledger_count":     0,
        "missing_in_trace_count":      0,
        "consistency_non_binding":     True,
        "consistency_simulation_only": True,
        "paper_only":                  True,
        "live_activation_allowed":     False,
    }


def _hold_consistency():
    return {
        "handoff_consistency_status":  "CONSISTENCY_HOLD",
        "handoff_consistency_mode":    "CONSISTENCY_BASELINE",
        "handoff_consistency_passed":  False,
        "consistency_reason":          "held",
        "consistency_reason_code":     "CONSISTENCY_HOLD_BASELINE",
        "matched_intent_count":        0,
        "missing_in_handoff_count":    0,
        "missing_in_ledger_count":     0,
        "missing_in_trace_count":      0,
        "consistency_non_binding":     True,
        "consistency_simulation_only": True,
        "paper_only":                  True,
        "live_activation_allowed":     False,
    }


def _all_clear():
    """Return the four inputs that produce PAPER_READY."""
    return (
        _ready_handoff(),
        _accepted_intake(),
        _passed_validation(),
        _passed_consistency(),
    )


# ---------------------------------------------------------------------------
# 1. Always-True / Always-False flags
# ---------------------------------------------------------------------------

class TestAlwaysFlags:
    def test_ready_non_binding_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["promotion_non_binding"] is True

    def test_ready_simulation_only_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["promotion_simulation_only"] is True

    def test_ready_paper_only_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["paper_only"] is True

    def test_ready_live_activation_false(self):
        g = build_promotion_gate(*_all_clear())
        assert g["live_activation_allowed"] is False

    def test_hold_non_binding_true(self):
        g = build_promotion_gate(_baseline_handoff(), _hold_intake(), _hold_validation(), _hold_consistency())
        assert g["promotion_non_binding"] is True

    def test_hold_live_activation_false(self):
        g = build_promotion_gate(_baseline_handoff(), _hold_intake(), _hold_validation(), _hold_consistency())
        assert g["live_activation_allowed"] is False

    def test_rejected_non_binding_true(self):
        g = build_promotion_gate(None, None, None, None)
        assert g["promotion_non_binding"] is True

    def test_rejected_live_activation_false(self):
        g = build_promotion_gate(None, None, None, None)
        assert g["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 2. PAPER_READY — all checks pass
# ---------------------------------------------------------------------------

class TestPaperReady:
    def test_status_paper_ready(self):
        g = build_promotion_gate(*_all_clear())
        assert g["promotion_status"] == PAPER_READY

    def test_mode_ready(self):
        g = build_promotion_gate(*_all_clear())
        assert g["promotion_mode"] == MODE_READY

    def test_promotion_ready_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["promotion_ready"] is True

    def test_paper_ready_candidate_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["paper_ready_candidate"] is True

    def test_reason_code_all_clear(self):
        g = build_promotion_gate(*_all_clear())
        assert g["promotion_reason_code"] == REASON_ALL_CLEAR

    def test_promotion_decision_contains_status(self):
        g = build_promotion_gate(*_all_clear())
        assert PAPER_READY in g["promotion_decision"]

    def test_promotion_decision_contains_reason_code(self):
        g = build_promotion_gate(*_all_clear())
        assert REASON_ALL_CLEAR in g["promotion_decision"]


# ---------------------------------------------------------------------------
# 3. PAPER_HOLD — upstream hold/baseline
# ---------------------------------------------------------------------------

class TestPaperHold:
    def test_baseline_handoff_gives_hold(self):
        g = build_promotion_gate(
            _baseline_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_status"] == PAPER_HOLD

    def test_hold_intake_gives_hold(self):
        g = build_promotion_gate(
            _ready_handoff(), _hold_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_status"] == PAPER_HOLD

    def test_hold_validation_gives_hold(self):
        # hold validation → passes steps 1-4 (not rejected), reaches step 5
        ho = _ready_handoff()
        ri = _accepted_intake()
        val = _hold_validation()   # status = VALIDATION_HOLD → in _HOLD_STATUSES
        con = _passed_consistency()
        g = build_promotion_gate(ho, ri, val, con)
        assert g["promotion_status"] == PAPER_HOLD

    def test_hold_consistency_gives_hold(self):
        ho  = _ready_handoff()
        ri  = _accepted_intake()
        val = _passed_validation()
        con = _hold_consistency()  # status = CONSISTENCY_HOLD → in _HOLD_STATUSES
        g = build_promotion_gate(ho, ri, val, con)
        assert g["promotion_status"] == PAPER_HOLD

    def test_hold_mode(self):
        g = build_promotion_gate(
            _baseline_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_mode"] == MODE_HOLD

    def test_hold_promotion_ready_false(self):
        g = build_promotion_gate(
            _baseline_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_ready"] is False

    def test_hold_paper_ready_candidate_false(self):
        g = build_promotion_gate(
            _baseline_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["paper_ready_candidate"] is False

    def test_hold_reason_code_upstream_hold(self):
        g = build_promotion_gate(
            _baseline_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_reason_code"] == REASON_UPSTREAM_HOLD

    def test_handoff_not_ready_gives_hold(self):
        ho = _ready_handoff()
        ho["handoff_ready"] = False
        g = build_promotion_gate(ho, _accepted_intake(), _passed_validation(), _passed_consistency())
        assert g["promotion_status"] == PAPER_HOLD

    def test_runner_contract_invalid_gives_hold(self):
        ri = _accepted_intake()
        ri["runner_contract_valid"] = False
        g = build_promotion_gate(_ready_handoff(), ri, _passed_validation(), _passed_consistency())
        assert g["promotion_status"] == PAPER_HOLD

    def test_handoff_not_ready_reason_code(self):
        ho = _ready_handoff()
        ho["handoff_ready"] = False
        g = build_promotion_gate(ho, _accepted_intake(), _passed_validation(), _passed_consistency())
        assert g["promotion_reason_code"] == REASON_HANDOFF_NOT_READY


# ---------------------------------------------------------------------------
# 4. PAPER_REJECTED — upstream rejected
# ---------------------------------------------------------------------------

class TestPaperRejectedUpstream:
    def test_rejected_handoff_gives_rejected(self):
        g = build_promotion_gate(
            _rejected_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_status"] == PAPER_REJECTED

    def test_rejected_intake_gives_rejected(self):
        g = build_promotion_gate(
            _ready_handoff(), _rejected_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_status"] == PAPER_REJECTED

    def test_rejected_upstream_reason_code(self):
        g = build_promotion_gate(
            _rejected_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_reason_code"] == REASON_UPSTREAM_REJECTED

    def test_rejected_promotion_ready_false(self):
        g = build_promotion_gate(
            _rejected_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_ready"] is False

    def test_rejected_paper_ready_candidate_false(self):
        g = build_promotion_gate(
            _rejected_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["paper_ready_candidate"] is False


# ---------------------------------------------------------------------------
# 5. PAPER_REJECTED — validation/consistency failed
# ---------------------------------------------------------------------------

class TestPaperRejectedFailed:
    def test_failed_validation_gives_rejected(self):
        g = build_promotion_gate(
            _ready_handoff(), _accepted_intake(),
            _failed_validation(), _passed_consistency(),
        )
        assert g["promotion_status"] == PAPER_REJECTED

    def test_failed_validation_reason_code(self):
        g = build_promotion_gate(
            _ready_handoff(), _accepted_intake(),
            _failed_validation(), _passed_consistency(),
        )
        assert g["promotion_reason_code"] == REASON_VALIDATION_FAILED

    def test_replay_not_consistent_gives_rejected(self):
        val = _passed_validation()
        val["replay_consistent"] = False
        g = build_promotion_gate(
            _ready_handoff(), _accepted_intake(),
            val, _passed_consistency(),
        )
        assert g["promotion_status"] == PAPER_REJECTED

    def test_failed_consistency_gives_rejected(self):
        g = build_promotion_gate(
            _ready_handoff(), _accepted_intake(),
            _passed_validation(), _failed_consistency(),
        )
        assert g["promotion_status"] == PAPER_REJECTED

    def test_failed_consistency_reason_code(self):
        g = build_promotion_gate(
            _ready_handoff(), _accepted_intake(),
            _passed_validation(), _failed_consistency(),
        )
        assert g["promotion_reason_code"] == REASON_CONSISTENCY_FAILED

    def test_failed_mode_rejected(self):
        g = build_promotion_gate(
            _ready_handoff(), _accepted_intake(),
            _failed_validation(), _passed_consistency(),
        )
        assert g["promotion_mode"] == MODE_REJECTED


# ---------------------------------------------------------------------------
# 6. PAPER_REJECTED — invalid input
# ---------------------------------------------------------------------------

class TestPaperRejectedInvalidInput:
    def test_none_handoff_rejected(self):
        g = build_promotion_gate(None, _accepted_intake(), _passed_validation(), _passed_consistency())
        assert g["promotion_status"] == PAPER_REJECTED

    def test_none_intake_rejected(self):
        g = build_promotion_gate(_ready_handoff(), None, _passed_validation(), _passed_consistency())
        assert g["promotion_status"] == PAPER_REJECTED

    def test_none_validation_rejected(self):
        g = build_promotion_gate(_ready_handoff(), _accepted_intake(), None, _passed_consistency())
        assert g["promotion_status"] == PAPER_REJECTED

    def test_none_consistency_rejected(self):
        g = build_promotion_gate(_ready_handoff(), _accepted_intake(), _passed_validation(), None)
        assert g["promotion_status"] == PAPER_REJECTED

    def test_all_none_rejected(self):
        g = build_promotion_gate(None, None, None, None)
        assert g["promotion_status"] == PAPER_REJECTED

    def test_invalid_input_reason_code(self):
        g = build_promotion_gate(None, None, None, None)
        assert g["promotion_reason_code"] == REASON_INVALID_INPUT

    def test_missing_handoff_status_rejected(self):
        ho = _ready_handoff()
        del ho["handoff_status"]
        g = build_promotion_gate(ho, _accepted_intake(), _passed_validation(), _passed_consistency())
        assert g["promotion_status"] == PAPER_REJECTED

    def test_missing_intake_status_rejected(self):
        ri = _accepted_intake()
        del ri["runner_intake_status"]
        g = build_promotion_gate(_ready_handoff(), ri, _passed_validation(), _passed_consistency())
        assert g["promotion_status"] == PAPER_REJECTED

    def test_list_input_rejected(self):
        g = build_promotion_gate([], {}, {}, {})
        assert g["promotion_status"] == PAPER_REJECTED

    def test_invalid_empty_snapshot(self):
        g = build_promotion_gate(None, None, None, None)
        assert g["upstream_snapshot"] == {}


# ---------------------------------------------------------------------------
# 7. Output field completeness
# ---------------------------------------------------------------------------

_GATE_KEYS = {
    "promotion_status", "promotion_mode",
    "promotion_ready", "promotion_reason", "promotion_reason_code",
    "promotion_decision", "upstream_snapshot",
    "paper_ready_candidate",
    "promotion_non_binding", "promotion_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestFieldCompleteness:
    def test_ready_has_all_keys(self):
        g = build_promotion_gate(*_all_clear())
        assert _GATE_KEYS.issubset(g.keys())

    def test_hold_has_all_keys(self):
        g = build_promotion_gate(
            _baseline_handoff(), _accepted_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert _GATE_KEYS.issubset(g.keys())

    def test_rejected_has_all_keys(self):
        g = build_promotion_gate(None, None, None, None)
        assert _GATE_KEYS.issubset(g.keys())


# ---------------------------------------------------------------------------
# 8. Upstream snapshot
# ---------------------------------------------------------------------------

class TestUpstreamSnapshot:
    def test_snapshot_has_handoff_status(self):
        g = build_promotion_gate(*_all_clear())
        assert "handoff_status" in g["upstream_snapshot"]

    def test_snapshot_handoff_ready_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["upstream_snapshot"]["handoff_ready"] is True

    def test_snapshot_validation_passed_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["upstream_snapshot"]["validation_passed"] is True

    def test_snapshot_replay_consistent_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["upstream_snapshot"]["replay_consistent"] is True

    def test_snapshot_consistency_passed_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["upstream_snapshot"]["consistency_passed"] is True

    def test_snapshot_runner_contract_valid_true(self):
        g = build_promotion_gate(*_all_clear())
        assert g["upstream_snapshot"]["runner_contract_valid"] is True

    def test_snapshot_is_copy_not_mutated(self):
        ho, ri, val, con = _all_clear()
        g = build_promotion_gate(ho, ri, val, con)
        g["upstream_snapshot"]["handoff_status"] = "MUTATED"
        assert ho["handoff_status"] == "READY_FOR_PAPER_HANDOFF"


# ---------------------------------------------------------------------------
# 9. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_inputs_same_output(self):
        ho, ri, val, con = _all_clear()
        g1 = build_promotion_gate(ho, ri, val, con)
        g2 = build_promotion_gate(ho, ri, val, con)
        assert g1 == g2

    def test_hold_deterministic(self):
        ho = _baseline_handoff()
        ri = _accepted_intake()
        val = _passed_validation()
        con = _passed_consistency()
        g1 = build_promotion_gate(ho, ri, val, con)
        g2 = build_promotion_gate(ho, ri, val, con)
        assert g1 == g2


# ---------------------------------------------------------------------------
# 10. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_handoff_not_mutated(self):
        ho, ri, val, con = _all_clear()
        original = ho["handoff_status"]
        build_promotion_gate(ho, ri, val, con)
        assert ho["handoff_status"] == original

    def test_validation_not_mutated(self):
        ho, ri, val, con = _all_clear()
        original = val["validation_passed"]
        build_promotion_gate(ho, ri, val, con)
        assert val["validation_passed"] == original


# ---------------------------------------------------------------------------
# 11. Priority ordering
# ---------------------------------------------------------------------------

class TestPriorityOrdering:
    def test_rejected_beats_hold(self):
        # Rejected handoff + hold intake → REJECTED (not HOLD)
        g = build_promotion_gate(
            _rejected_handoff(), _hold_intake(),
            _passed_validation(), _passed_consistency(),
        )
        assert g["promotion_status"] == PAPER_REJECTED

    def test_rejected_beats_failed_validation(self):
        # Rejected handoff + failed validation → REJECTED via upstream_rejected path
        g = build_promotion_gate(
            _rejected_handoff(), _accepted_intake(),
            _failed_validation(), _passed_consistency(),
        )
        assert g["promotion_status"] == PAPER_REJECTED
        assert g["promotion_reason_code"] == REASON_UPSTREAM_REJECTED

    def test_hold_beats_validation_failed(self):
        # hold consistency + failed validation → HOLD (step 3 before step 4)
        g = build_promotion_gate(
            _ready_handoff(), _accepted_intake(),
            _failed_validation(), _hold_consistency(),
        )
        assert g["promotion_status"] == PAPER_HOLD
        assert g["promotion_reason_code"] == REASON_UPSTREAM_HOLD

    def test_hold_beats_consistency_failed(self):
        # hold intake + failed consistency → HOLD (step 3 before step 5)
        g = build_promotion_gate(
            _ready_handoff(), _hold_intake(),
            _passed_validation(), _failed_consistency(),
        )
        assert g["promotion_status"] == PAPER_HOLD
        assert g["promotion_reason_code"] == REASON_UPSTREAM_HOLD


# ---------------------------------------------------------------------------
# 12. Full chain: build_promotion_from_specs
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
    def test_has_promotion_gate_key(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "promotion_gate" in result

    def test_has_all_pipeline_keys(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        for key in [
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit", "queen_handoff",
            "runner_intake", "dry_run_consumption",
            "execution_ledger", "audit_trace",
            "replay_validation", "handoff_consistency",
            "promotion_gate",
        ]:
            assert key in result, f"missing key: {key}"

    def test_gate_live_activation_false(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["promotion_gate"]["live_activation_allowed"] is False

    def test_gate_paper_only_true(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["promotion_gate"]["paper_only"] is True

    def test_gate_non_binding_true(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["promotion_gate"]["promotion_non_binding"] is True

    def test_gate_simulation_only_true(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["promotion_gate"]["promotion_simulation_only"] is True

    def test_gate_status_valid(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["promotion_gate"]["promotion_status"] in {
            PAPER_READY, PAPER_HOLD, PAPER_REJECTED
        }

    def test_gate_has_upstream_snapshot(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        snap = result["promotion_gate"]["upstream_snapshot"]
        assert isinstance(snap, dict)

    def test_paper_ready_candidate_bool(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert isinstance(result["promotion_gate"]["paper_ready_candidate"], bool)

    def test_promotion_ready_consistent_with_status(self):
        result = build_promotion_from_specs(_SPECS, 10_000.0, _REGIMES)
        g = result["promotion_gate"]
        if g["promotion_status"] == PAPER_READY:
            assert g["promotion_ready"] is True
            assert g["paper_ready_candidate"] is True
        else:
            assert g["promotion_ready"] is False
            assert g["paper_ready_candidate"] is False

    def test_empty_specs_safe(self):
        result = build_promotion_from_specs([], 10_000.0)
        assert result["promotion_gate"]["live_activation_allowed"] is False

    def test_zero_equity_safe(self):
        result = build_promotion_from_specs(_SPECS, 0.0, _REGIMES)
        assert result["promotion_gate"]["promotion_non_binding"] is True
