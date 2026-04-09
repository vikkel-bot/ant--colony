"""
Tests for AC-91: Paper Runner Intake Contract + Dry-Run Consumption
build_paper_runner_intake_lite.py
"""
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_paper_runner_intake_lite import (
    build_runner_intake,
    build_dry_run_consumption,
    build_intake_and_dry_run,
    build_dry_run_from_specs,
    INTAKE_ACCEPTED, INTAKE_HOLD, INTAKE_REJECTED,
    INTAKE_MODE_READY, INTAKE_MODE_BASELINE, INTAKE_MODE_REJECTED,
    REASON_CONTRACT_VALID, REASON_CONTRACT_HOLD_BASELINE,
    REASON_CONTRACT_NO_ALLOWED, REASON_CONTRACT_HO_REJECTED, REASON_CONTRACT_INVALID,
    DRY_RUN_COMPLETE, DRY_RUN_HOLD, DRY_RUN_REJECTED,
    DRY_RUN_MODE_READY, DRY_RUN_MODE_BASELINE, DRY_RUN_MODE_REJECTED,
    REASON_DRY_RUN_OK, REASON_DRY_RUN_ALL_SKIPPED,
    REASON_DRY_RUN_HOLD_BASELINE, REASON_DRY_RUN_INVALID,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _ready_handoff(allowed=2, blocked=1):
    total = allowed + blocked
    return {
        "handoff_status":                "READY_FOR_PAPER_HANDOFF",
        "handoff_mode":                  "PAPER_HANDOFF_READY",
        "handoff_ready":                 True,
        "handoff_reason":                "paper handoff ready",
        "handoff_reason_code":           "HANDOFF_PACK_READY",
        "total_intents":                 total,
        "total_allowed":                 allowed,
        "total_blocked":                 blocked,
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
        "handoff_reason":                "baseline hold",
        "handoff_reason_code":           "HANDOFF_PACK_BASELINE",
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
        "handoff_reason":                "rejected",
        "handoff_reason_code":           "HANDOFF_INVALID_INPUT",
        "total_intents":                 0,
        "total_allowed":                 0,
        "total_blocked":                 0,
        "queen_handoff_non_binding":     True,
        "queen_handoff_simulation_only": True,
        "paper_only":                    True,
        "live_activation_allowed":       False,
    }


def _intent_pack_with_intents(
    increase=1, decrease=1, hold=1, blocked=0,
):
    intents = []
    for i in range(increase):
        intents.append({
            "market": f"BTC-EUR",
            "asset_class": "crypto",
            "intent_action": "PAPER_INCREASE_INTENT",
            "intent_status": "ALLOWED",
            "current_capital_eur": 1000.0,
            "selected_capital_eur": 1200.0,
            "delta_eur": 200.0,
            "transition_direction": "INCREASE",
            "block_reason": "",
            "paper_only": True,
        })
    for i in range(decrease):
        intents.append({
            "market": f"ETH-EUR",
            "asset_class": "crypto",
            "intent_action": "PAPER_DECREASE_INTENT",
            "intent_status": "ALLOWED",
            "current_capital_eur": 1000.0,
            "selected_capital_eur": 800.0,
            "delta_eur": -200.0,
            "transition_direction": "DECREASE",
            "block_reason": "",
            "paper_only": True,
        })
    for i in range(hold):
        intents.append({
            "market": f"SOL-EUR",
            "asset_class": "crypto",
            "intent_action": "PAPER_HOLD_INTENT",
            "intent_status": "ALLOWED",
            "current_capital_eur": 500.0,
            "selected_capital_eur": 500.0,
            "delta_eur": 0.0,
            "transition_direction": "HOLD",
            "block_reason": "",
            "paper_only": True,
        })
    for i in range(blocked):
        intents.append({
            "market": f"XRP-EUR",
            "asset_class": "crypto",
            "intent_action": "PAPER_BLOCKED_INTENT",
            "intent_status": "BLOCKED",
            "current_capital_eur": 0.0,
            "selected_capital_eur": 0.0,
            "delta_eur": 0.0,
            "transition_direction": "HOLD",
            "block_reason": "INVALID_STEP",
            "paper_only": True,
        })

    total = increase + decrease + hold + blocked
    allowed = increase + decrease + hold
    return {
        "intent_pack_status":         "PACK_ACTIVE",
        "intent_pack_mode":           "PAPER_INTENT_PACK",
        "intents":                    intents,
        "intent_count":               total,
        "allowed_count":              allowed,
        "blocked_count":              blocked,
        "blocked_reasons":            ["INVALID_STEP"] * blocked,
        "intent_pack_non_binding":    True,
        "intent_pack_simulation_only": True,
        "paper_only":                 True,
    }


def _accepted_intake(allowed=2, blocked=1):
    return build_runner_intake(_ready_handoff(allowed=allowed, blocked=blocked))


# ---------------------------------------------------------------------------
# 1. Runner Intake — Always-True / Always-False flags
# ---------------------------------------------------------------------------

class TestIntakeAlwaysFlags:
    def test_accepted_non_binding_true(self):
        assert _accepted_intake()["runner_intake_non_binding"] is True

    def test_accepted_simulation_only_true(self):
        assert _accepted_intake()["runner_intake_simulation_only"] is True

    def test_accepted_paper_only_true(self):
        assert _accepted_intake()["paper_only"] is True

    def test_accepted_live_activation_false(self):
        assert _accepted_intake()["live_activation_allowed"] is False

    def test_hold_non_binding_true(self):
        r = build_runner_intake(_baseline_handoff())
        assert r["runner_intake_non_binding"] is True

    def test_hold_simulation_only_true(self):
        r = build_runner_intake(_baseline_handoff())
        assert r["runner_intake_simulation_only"] is True

    def test_hold_paper_only_true(self):
        r = build_runner_intake(_baseline_handoff())
        assert r["paper_only"] is True

    def test_hold_live_activation_false(self):
        r = build_runner_intake(_baseline_handoff())
        assert r["live_activation_allowed"] is False

    def test_rejected_non_binding_true(self):
        r = build_runner_intake(None)
        assert r["runner_intake_non_binding"] is True

    def test_rejected_simulation_only_true(self):
        r = build_runner_intake(None)
        assert r["runner_intake_simulation_only"] is True

    def test_rejected_paper_only_true(self):
        r = build_runner_intake(None)
        assert r["paper_only"] is True

    def test_rejected_live_activation_false(self):
        r = build_runner_intake(None)
        assert r["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 2. Runner Intake — INTAKE_ACCEPTED
# ---------------------------------------------------------------------------

class TestIntakeAccepted:
    def test_status_accepted(self):
        assert _accepted_intake()["runner_intake_status"] == INTAKE_ACCEPTED

    def test_mode_ready(self):
        assert _accepted_intake()["runner_intake_mode"] == INTAKE_MODE_READY

    def test_contract_valid_true(self):
        assert _accepted_intake()["runner_contract_valid"] is True

    def test_reason_code_valid(self):
        assert _accepted_intake()["runner_contract_reason_code"] == REASON_CONTRACT_VALID

    def test_allowed_count_correct(self):
        r = _accepted_intake(allowed=3, blocked=1)
        assert r["consumed_allowed_count"] == 3

    def test_blocked_count_correct(self):
        r = _accepted_intake(allowed=3, blocked=1)
        assert r["consumed_blocked_count"] == 1

    def test_intent_count_correct(self):
        r = _accepted_intake(allowed=3, blocked=1)
        assert r["consumed_intent_count"] == 4

    def test_single_allowed_accepted(self):
        r = build_runner_intake(_ready_handoff(allowed=1, blocked=0))
        assert r["runner_intake_status"] == INTAKE_ACCEPTED

    def test_reason_contains_allowed_count(self):
        r = _accepted_intake(allowed=3, blocked=1)
        assert "3" in r["runner_contract_reason"]


# ---------------------------------------------------------------------------
# 3. Runner Intake — INTAKE_HOLD
# ---------------------------------------------------------------------------

class TestIntakeHold:
    def test_baseline_handoff_hold(self):
        r = build_runner_intake(_baseline_handoff())
        assert r["runner_intake_status"] == INTAKE_HOLD

    def test_baseline_mode(self):
        r = build_runner_intake(_baseline_handoff())
        assert r["runner_intake_mode"] == INTAKE_MODE_BASELINE

    def test_baseline_contract_valid_false(self):
        r = build_runner_intake(_baseline_handoff())
        assert r["runner_contract_valid"] is False

    def test_baseline_reason_code(self):
        r = build_runner_intake(_baseline_handoff())
        assert r["runner_contract_reason_code"] == REASON_CONTRACT_HOLD_BASELINE

    def test_zero_allowed_active_pack_hold(self):
        ho = _ready_handoff(allowed=0, blocked=2)
        ho["total_allowed"] = 0
        r = build_runner_intake(ho)
        assert r["runner_intake_status"] == INTAKE_HOLD

    def test_zero_allowed_reason_code(self):
        ho = _ready_handoff(allowed=0, blocked=2)
        ho["total_allowed"] = 0
        r = build_runner_intake(ho)
        assert r["runner_contract_reason_code"] == REASON_CONTRACT_NO_ALLOWED

    def test_handoff_not_ready_gives_hold(self):
        ho = _ready_handoff()
        ho["handoff_ready"] = False
        ho["handoff_status"] = "HOLD_BASELINE_HANDOFF"
        r = build_runner_intake(ho)
        assert r["runner_intake_status"] == INTAKE_HOLD


# ---------------------------------------------------------------------------
# 4. Runner Intake — INTAKE_REJECTED
# ---------------------------------------------------------------------------

class TestIntakeRejected:
    def test_none_gives_rejected(self):
        r = build_runner_intake(None)
        assert r["runner_intake_status"] == INTAKE_REJECTED

    def test_list_gives_rejected(self):
        r = build_runner_intake([])
        assert r["runner_intake_status"] == INTAKE_REJECTED

    def test_missing_handoff_status_rejected(self):
        ho = _ready_handoff()
        del ho["handoff_status"]
        r = build_runner_intake(ho)
        assert r["runner_intake_status"] == INTAKE_REJECTED

    def test_missing_handoff_ready_rejected(self):
        ho = _ready_handoff()
        del ho["handoff_ready"]
        r = build_runner_intake(ho)
        assert r["runner_intake_status"] == INTAKE_REJECTED

    def test_reject_handoff_gives_rejected_intake(self):
        r = build_runner_intake(_rejected_handoff())
        assert r["runner_intake_status"] == INTAKE_REJECTED

    def test_reject_handoff_reason_code(self):
        r = build_runner_intake(_rejected_handoff())
        assert r["runner_contract_reason_code"] == REASON_CONTRACT_HO_REJECTED

    def test_rejected_contract_valid_false(self):
        r = build_runner_intake(None)
        assert r["runner_contract_valid"] is False

    def test_rejected_reason_code(self):
        r = build_runner_intake(None)
        assert r["runner_contract_reason_code"] == REASON_CONTRACT_INVALID

    def test_rejected_snapshot_empty(self):
        r = build_runner_intake(None)
        assert r["handoff_snapshot"] == {}


# ---------------------------------------------------------------------------
# 5. Runner Intake — Output field completeness
# ---------------------------------------------------------------------------

_INTAKE_KEYS = {
    "runner_intake_status", "runner_intake_mode",
    "runner_contract_valid", "runner_contract_reason", "runner_contract_reason_code",
    "handoff_snapshot",
    "consumed_intent_count", "consumed_allowed_count", "consumed_blocked_count",
    "runner_intake_non_binding", "runner_intake_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestIntakeFieldCompleteness:
    def test_accepted_has_all_keys(self):
        assert _INTAKE_KEYS.issubset(_accepted_intake().keys())

    def test_hold_has_all_keys(self):
        r = build_runner_intake(_baseline_handoff())
        assert _INTAKE_KEYS.issubset(r.keys())

    def test_rejected_has_all_keys(self):
        r = build_runner_intake(None)
        assert _INTAKE_KEYS.issubset(r.keys())


# ---------------------------------------------------------------------------
# 6. Runner Intake — Handoff snapshot
# ---------------------------------------------------------------------------

class TestHandoffSnapshot:
    def test_snapshot_status(self):
        r = _accepted_intake()
        assert r["handoff_snapshot"]["handoff_status"] == "READY_FOR_PAPER_HANDOFF"

    def test_snapshot_ready_bool(self):
        r = _accepted_intake()
        assert r["handoff_snapshot"]["handoff_ready"] is True

    def test_snapshot_total_allowed(self):
        r = _accepted_intake(allowed=3)
        assert r["handoff_snapshot"]["total_allowed"] == 3

    def test_snapshot_live_activation_always_false(self):
        r = _accepted_intake()
        assert r["handoff_snapshot"]["live_activation_allowed"] is False

    def test_snapshot_is_copy_not_reference(self):
        ho = _ready_handoff()
        r  = build_runner_intake(ho)
        r["handoff_snapshot"]["handoff_status"] = "MUTATED"
        assert ho["handoff_status"] == "READY_FOR_PAPER_HANDOFF"


# ---------------------------------------------------------------------------
# 7. Dry-Run — Always-True / Always-False flags
# ---------------------------------------------------------------------------

class TestDryRunAlwaysFlags:
    def test_complete_non_binding_true(self):
        intake  = _accepted_intake()
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_non_binding"] is True

    def test_complete_simulation_only_true(self):
        intake  = _accepted_intake()
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_simulation_only"] is True

    def test_complete_paper_only_true(self):
        intake  = _accepted_intake()
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["paper_only"] is True

    def test_complete_live_activation_false(self):
        intake  = _accepted_intake()
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["live_activation_allowed"] is False

    def test_hold_non_binding_true(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_non_binding"] is True

    def test_hold_simulation_only_true(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_simulation_only"] is True

    def test_hold_live_activation_false(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["live_activation_allowed"] is False

    def test_rejected_non_binding_true(self):
        dry_run = build_dry_run_consumption(None)
        assert dry_run["dry_run_non_binding"] is True

    def test_rejected_live_activation_false(self):
        dry_run = build_dry_run_consumption(None)
        assert dry_run["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 8. Dry-Run — DRY_RUN_COMPLETE (intake accepted)
# ---------------------------------------------------------------------------

class TestDryRunComplete:
    def test_status_complete(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=1, hold=0, blocked=0)
        intake  = _accepted_intake(allowed=2, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert dry_run["dry_run_status"] == DRY_RUN_COMPLETE

    def test_mode_ready(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=0, hold=0, blocked=0)
        intake  = _accepted_intake(allowed=1, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert dry_run["dry_run_mode"] == DRY_RUN_MODE_READY

    def test_reason_code_ok_when_consumed(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=0, hold=0, blocked=0)
        intake  = _accepted_intake(allowed=1, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert dry_run["dry_run_reason_code"] == REASON_DRY_RUN_OK

    def test_consumed_count_increase_decrease(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=1, hold=1, blocked=0)
        intake  = _accepted_intake(allowed=3, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert len(dry_run["dry_run_consumed_intents"]) == 2

    def test_skipped_count_hold(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=1, hold=1, blocked=0)
        intake  = _accepted_intake(allowed=3, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert len(dry_run["dry_run_skipped_intents"]) == 1

    def test_blocked_count(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=1, hold=0, blocked=2)
        intake  = _accepted_intake(allowed=2, blocked=2)
        dry_run = build_dry_run_consumption(intake, pack)
        assert len(dry_run["dry_run_blocked_intents"]) == 2

    def test_all_hold_reason_code_all_skipped(self):
        pack    = _intent_pack_with_intents(increase=0, decrease=0, hold=2, blocked=0)
        intake  = _accepted_intake(allowed=2, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert dry_run["dry_run_reason_code"] == REASON_DRY_RUN_ALL_SKIPPED

    def test_consumed_entry_has_market(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=0, hold=0)
        intake  = _accepted_intake(allowed=1, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert "market" in dry_run["dry_run_consumed_intents"][0]

    def test_consumed_entry_paper_only_true(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=0, hold=0)
        intake  = _accepted_intake(allowed=1, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert dry_run["dry_run_consumed_intents"][0]["paper_only"] is True

    def test_consumed_entry_has_delta_eur(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=0, hold=0)
        intake  = _accepted_intake(allowed=1, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert "delta_eur" in dry_run["dry_run_consumed_intents"][0]

    def test_skipped_entry_paper_only_true(self):
        pack    = _intent_pack_with_intents(increase=0, decrease=0, hold=1)
        intake  = _accepted_intake(allowed=1, blocked=0)
        dry_run = build_dry_run_consumption(intake, pack)
        assert dry_run["dry_run_skipped_intents"][0]["paper_only"] is True

    def test_blocked_entry_paper_only_true(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=0, hold=0, blocked=1)
        intake  = _accepted_intake(allowed=1, blocked=1)
        dry_run = build_dry_run_consumption(intake, pack)
        assert dry_run["dry_run_blocked_intents"][0]["paper_only"] is True


# ---------------------------------------------------------------------------
# 9. Dry-Run — without intent_pack (snapshot-derived)
# ---------------------------------------------------------------------------

class TestDryRunSnapshotFallback:
    def test_complete_without_pack(self):
        intake  = _accepted_intake(allowed=2, blocked=1)
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_status"] == DRY_RUN_COMPLETE

    def test_consumed_count_from_snapshot(self):
        intake  = _accepted_intake(allowed=3, blocked=0)
        dry_run = build_dry_run_consumption(intake)
        assert len(dry_run["dry_run_consumed_intents"]) == 3

    def test_blocked_count_from_snapshot(self):
        intake  = _accepted_intake(allowed=2, blocked=2)
        dry_run = build_dry_run_consumption(intake)
        assert len(dry_run["dry_run_blocked_intents"]) == 2

    def test_skipped_empty_from_snapshot(self):
        intake  = _accepted_intake(allowed=2, blocked=1)
        dry_run = build_dry_run_consumption(intake)
        assert len(dry_run["dry_run_skipped_intents"]) == 0

    def test_snapshot_entries_paper_only_true(self):
        intake  = _accepted_intake(allowed=2, blocked=0)
        dry_run = build_dry_run_consumption(intake)
        for entry in dry_run["dry_run_consumed_intents"]:
            assert entry["paper_only"] is True


# ---------------------------------------------------------------------------
# 10. Dry-Run — HOLD path
# ---------------------------------------------------------------------------

class TestDryRunHold:
    def test_baseline_intake_hold(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_status"] == DRY_RUN_HOLD

    def test_baseline_intake_mode(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_mode"] == DRY_RUN_MODE_BASELINE

    def test_baseline_reason_code(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_reason_code"] == REASON_DRY_RUN_HOLD_BASELINE

    def test_hold_consumed_empty(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_consumed_intents"] == []

    def test_hold_skipped_empty(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_skipped_intents"] == []

    def test_hold_blocked_empty(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_blocked_intents"] == []


# ---------------------------------------------------------------------------
# 11. Dry-Run — REJECTED path
# ---------------------------------------------------------------------------

class TestDryRunRejected:
    def test_none_intake_rejected(self):
        dry_run = build_dry_run_consumption(None)
        assert dry_run["dry_run_status"] == DRY_RUN_REJECTED

    def test_list_intake_rejected(self):
        dry_run = build_dry_run_consumption([])
        assert dry_run["dry_run_status"] == DRY_RUN_REJECTED

    def test_missing_intake_status_rejected(self):
        intake = _accepted_intake()
        del intake["runner_intake_status"]
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_status"] == DRY_RUN_REJECTED

    def test_rejected_intake_gives_rejected_dry_run(self):
        intake  = build_runner_intake(_rejected_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert dry_run["dry_run_status"] == DRY_RUN_REJECTED

    def test_rejected_mode(self):
        dry_run = build_dry_run_consumption(None)
        assert dry_run["dry_run_mode"] == DRY_RUN_MODE_REJECTED

    def test_rejected_reason_code(self):
        dry_run = build_dry_run_consumption(None)
        assert dry_run["dry_run_reason_code"] == REASON_DRY_RUN_INVALID

    def test_rejected_consumed_empty(self):
        dry_run = build_dry_run_consumption(None)
        assert dry_run["dry_run_consumed_intents"] == []


# ---------------------------------------------------------------------------
# 12. Dry-Run — Output field completeness
# ---------------------------------------------------------------------------

_DRY_RUN_KEYS = {
    "dry_run_status", "dry_run_mode",
    "dry_run_reason", "dry_run_reason_code",
    "dry_run_consumed_intents", "dry_run_skipped_intents", "dry_run_blocked_intents",
    "dry_run_non_binding", "dry_run_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestDryRunFieldCompleteness:
    def test_complete_has_all_keys(self):
        intake  = _accepted_intake()
        dry_run = build_dry_run_consumption(intake)
        assert _DRY_RUN_KEYS.issubset(dry_run.keys())

    def test_hold_has_all_keys(self):
        intake  = build_runner_intake(_baseline_handoff())
        dry_run = build_dry_run_consumption(intake)
        assert _DRY_RUN_KEYS.issubset(dry_run.keys())

    def test_rejected_has_all_keys(self):
        dry_run = build_dry_run_consumption(None)
        assert _DRY_RUN_KEYS.issubset(dry_run.keys())


# ---------------------------------------------------------------------------
# 13. Counts consistency
# ---------------------------------------------------------------------------

class TestCountsConsistency:
    def test_intake_counts_sum_correct(self):
        r = _accepted_intake(allowed=3, blocked=2)
        assert r["consumed_intent_count"] == r["consumed_allowed_count"] + r["consumed_blocked_count"]

    def test_dry_run_total_equals_sum(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=1, hold=1, blocked=1)
        intake  = _accepted_intake(allowed=3, blocked=1)
        dry_run = build_dry_run_consumption(intake, pack)
        total = (
            len(dry_run["dry_run_consumed_intents"])
            + len(dry_run["dry_run_skipped_intents"])
            + len(dry_run["dry_run_blocked_intents"])
        )
        assert total == 4  # increase + decrease + hold + blocked

    def test_snapshot_counts_match_intake_counts(self):
        ho = _ready_handoff(allowed=3, blocked=2)
        r  = build_runner_intake(ho)
        snap = r["handoff_snapshot"]
        assert snap["total_allowed"] == r["consumed_allowed_count"]
        assert snap["total_blocked"] == r["consumed_blocked_count"]


# ---------------------------------------------------------------------------
# 14. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_intake_same_inputs_same_output(self):
        ho = _ready_handoff()
        r1 = build_runner_intake(ho)
        r2 = build_runner_intake(ho)
        assert r1 == r2

    def test_dry_run_same_inputs_same_output(self):
        pack    = _intent_pack_with_intents(increase=1, decrease=1, hold=1, blocked=0)
        intake  = _accepted_intake(allowed=3, blocked=0)
        d1 = build_dry_run_consumption(intake, pack)
        d2 = build_dry_run_consumption(intake, pack)
        assert d1 == d2


# ---------------------------------------------------------------------------
# 15. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_handoff_not_mutated_by_intake(self):
        ho = _ready_handoff(allowed=2, blocked=1)
        original_allowed = ho["total_allowed"]
        build_runner_intake(ho)
        assert ho["total_allowed"] == original_allowed

    def test_intake_not_mutated_by_dry_run(self):
        intake = _accepted_intake()
        original_status = intake["runner_intake_status"]
        build_dry_run_consumption(intake)
        assert intake["runner_intake_status"] == original_status

    def test_intent_pack_not_mutated(self):
        pack   = _intent_pack_with_intents(increase=1, decrease=1, hold=1, blocked=0)
        intake = _accepted_intake(allowed=3, blocked=0)
        original_count = len(pack["intents"])
        build_dry_run_consumption(intake, pack)
        assert len(pack["intents"]) == original_count


# ---------------------------------------------------------------------------
# 16. Combined: build_intake_and_dry_run
# ---------------------------------------------------------------------------

class TestCombined:
    def test_has_runner_intake_key(self):
        result = build_intake_and_dry_run(_ready_handoff())
        assert "runner_intake" in result

    def test_has_dry_run_key(self):
        result = build_intake_and_dry_run(_ready_handoff())
        assert "dry_run_consumption" in result

    def test_intake_accepted_in_combined(self):
        result = build_intake_and_dry_run(_ready_handoff())
        assert result["runner_intake"]["runner_intake_status"] == INTAKE_ACCEPTED

    def test_dry_run_complete_in_combined(self):
        pack   = _intent_pack_with_intents(increase=1, decrease=1)
        result = build_intake_and_dry_run(_ready_handoff(allowed=2), pack)
        assert result["dry_run_consumption"]["dry_run_status"] == DRY_RUN_COMPLETE

    def test_combined_baseline_safe(self):
        result = build_intake_and_dry_run(_baseline_handoff())
        assert result["runner_intake"]["runner_intake_status"] == INTAKE_HOLD
        assert result["dry_run_consumption"]["dry_run_status"] == DRY_RUN_HOLD


# ---------------------------------------------------------------------------
# 17. Full chain: build_dry_run_from_specs
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
    def test_has_runner_intake_key(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "runner_intake" in result

    def test_has_dry_run_key(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "dry_run_consumption" in result

    def test_has_all_pipeline_keys(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        for key in [
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit", "queen_handoff",
            "runner_intake", "dry_run_consumption",
        ]:
            assert key in result, f"missing key: {key}"

    def test_intake_live_activation_false(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["runner_intake"]["live_activation_allowed"] is False

    def test_dry_run_live_activation_false(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["dry_run_consumption"]["live_activation_allowed"] is False

    def test_intake_paper_only_true(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["runner_intake"]["paper_only"] is True

    def test_dry_run_paper_only_true(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["dry_run_consumption"]["paper_only"] is True

    def test_intake_non_binding_true(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["runner_intake"]["runner_intake_non_binding"] is True

    def test_dry_run_non_binding_true(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["dry_run_consumption"]["dry_run_non_binding"] is True

    def test_intake_simulation_only_true(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["runner_intake"]["runner_intake_simulation_only"] is True

    def test_dry_run_simulation_only_true(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["dry_run_consumption"]["dry_run_simulation_only"] is True

    def test_intake_status_valid(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["runner_intake"]["runner_intake_status"] in {
            INTAKE_ACCEPTED, INTAKE_HOLD, INTAKE_REJECTED
        }

    def test_dry_run_status_valid(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["dry_run_consumption"]["dry_run_status"] in {
            DRY_RUN_COMPLETE, DRY_RUN_HOLD, DRY_RUN_REJECTED
        }

    def test_empty_specs_safe(self):
        result = build_dry_run_from_specs([], 10_000.0)
        assert result["runner_intake"]["live_activation_allowed"] is False
        assert result["dry_run_consumption"]["live_activation_allowed"] is False

    def test_zero_equity_safe(self):
        result = build_dry_run_from_specs(_SPECS, 0.0, _REGIMES)
        assert result["dry_run_consumption"]["dry_run_non_binding"] is True

    def test_dry_run_lists_are_lists(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        dr = result["dry_run_consumption"]
        assert isinstance(dr["dry_run_consumed_intents"], list)
        assert isinstance(dr["dry_run_skipped_intents"], list)
        assert isinstance(dr["dry_run_blocked_intents"], list)

    def test_intent_entries_paper_only_true(self):
        result = build_dry_run_from_specs(_SPECS, 10_000.0, _REGIMES)
        dr = result["dry_run_consumption"]
        for entry in dr["dry_run_consumed_intents"] + dr["dry_run_skipped_intents"] + dr["dry_run_blocked_intents"]:
            assert entry.get("paper_only") is True, f"paper_only not True in {entry}"
