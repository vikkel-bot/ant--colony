"""
Tests for AC-90: Intent Pack Consolidation + Queen Handoff Boundary
build_queen_handoff_boundary_lite.py
"""
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_queen_handoff_boundary_lite import (
    build_queen_handoff,
    build_handoff_from_pack_and_audit,
    build_handoff_from_specs,
    HANDOFF_READY,
    HANDOFF_BASELINE,
    HANDOFF_REJECTED,
    MODE_READY,
    MODE_BASELINE,
    MODE_REJECTED,
    REASON_READY,
    REASON_BASELINE_PACK,
    REASON_NO_ALLOWED,
    REASON_REJECTED_PACK,
    REASON_INVALID_INPUT,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _active_pack(allowed=2, blocked=1):
    return {
        "intent_pack_status": "PACK_ACTIVE",
        "intent_count":       allowed + blocked,
        "allowed_count":      allowed,
        "blocked_count":      blocked,
        "blocked_reasons":    ["POLICY_BLOCKED"] * blocked,
        "intents":            [],
    }

def _baseline_pack():
    return {
        "intent_pack_status": "PACK_BASELINE",
        "intent_count":       0,
        "allowed_count":      0,
        "blocked_count":      0,
        "blocked_reasons":    [],
        "intents":            [],
    }

def _rejected_pack():
    return {
        "intent_pack_status": "PACK_REJECTED",
        "intent_count":       0,
        "allowed_count":      0,
        "blocked_count":      0,
        "blocked_reasons":    [],
        "intents":            [],
    }

def _complete_audit(increase=500.0, decrease=200.0, hold=1):
    net = increase - decrease
    return {
        "audit_status":           "AUDIT_COMPLETE",
        "audit_reason_code":      "AUDIT_ALL_ALLOWED",
        "total_markets_reviewed": 3,
        "total_increase_eur":     increase,
        "total_decrease_eur":     decrease,
        "total_hold_count":       hold,
        "net_change_eur":         net,
    }

def _baseline_audit():
    return {
        "audit_status":           "AUDIT_BASELINE",
        "audit_reason_code":      "AUDIT_EMPTY",
        "total_markets_reviewed": 0,
        "total_increase_eur":     0.0,
        "total_decrease_eur":     0.0,
        "total_hold_count":       0,
        "net_change_eur":         0.0,
    }

# ---------------------------------------------------------------------------
# 1. Always-True / Always-False flags
# ---------------------------------------------------------------------------

class TestAlwaysFlags:
    def test_ready_non_binding_true(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        assert h["queen_handoff_non_binding"] is True

    def test_ready_simulation_only_true(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        assert h["queen_handoff_simulation_only"] is True

    def test_ready_paper_only_true(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        assert h["paper_only"] is True

    def test_ready_live_activation_false(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        assert h["live_activation_allowed"] is False

    def test_baseline_non_binding_true(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert h["queen_handoff_non_binding"] is True

    def test_baseline_simulation_only_true(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert h["queen_handoff_simulation_only"] is True

    def test_baseline_paper_only_true(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert h["paper_only"] is True

    def test_baseline_live_activation_false(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert h["live_activation_allowed"] is False

    def test_rejected_non_binding_true(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["queen_handoff_non_binding"] is True

    def test_rejected_simulation_only_true(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["queen_handoff_simulation_only"] is True

    def test_rejected_paper_only_true(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["paper_only"] is True

    def test_rejected_live_activation_false(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 2. READY_FOR_PAPER_HANDOFF
# ---------------------------------------------------------------------------

class TestReadyHandoff:
    def test_status_ready(self):
        h = build_queen_handoff(_active_pack(allowed=2, blocked=0), _complete_audit())
        assert h["handoff_status"] == HANDOFF_READY

    def test_mode_ready(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        assert h["handoff_mode"] == MODE_READY

    def test_handoff_ready_true(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        assert h["handoff_ready"] is True

    def test_reason_code_ready(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        assert h["handoff_reason_code"] == REASON_READY

    def test_total_allowed_correct(self):
        h = build_queen_handoff(_active_pack(allowed=3, blocked=1), _complete_audit())
        assert h["total_allowed"] == 3

    def test_total_blocked_correct(self):
        h = build_queen_handoff(_active_pack(allowed=3, blocked=1), _complete_audit())
        assert h["total_blocked"] == 1

    def test_total_intents_correct(self):
        h = build_queen_handoff(_active_pack(allowed=3, blocked=1), _complete_audit())
        assert h["total_intents"] == 4

    def test_single_allowed_intent(self):
        h = build_queen_handoff(_active_pack(allowed=1, blocked=0), _complete_audit())
        assert h["handoff_status"] == HANDOFF_READY
        assert h["handoff_ready"] is True

    def test_reason_contains_allowed_count(self):
        h = build_queen_handoff(_active_pack(allowed=2, blocked=1), _complete_audit())
        assert "2" in h["handoff_reason"]

    def test_reason_contains_blocked_count(self):
        h = build_queen_handoff(_active_pack(allowed=2, blocked=1), _complete_audit())
        assert "1" in h["handoff_reason"]


# ---------------------------------------------------------------------------
# 3. HOLD_BASELINE_HANDOFF
# ---------------------------------------------------------------------------

class TestBaselineHandoff:
    def test_baseline_pack_status(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert h["handoff_status"] == HANDOFF_BASELINE

    def test_baseline_pack_mode(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert h["handoff_mode"] == MODE_BASELINE

    def test_baseline_pack_ready_false(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert h["handoff_ready"] is False

    def test_baseline_pack_reason_code(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert h["handoff_reason_code"] == REASON_BASELINE_PACK

    def test_empty_pack_status_baseline(self):
        pack = {
            "intent_pack_status": "",
            "intent_count": 0,
            "allowed_count": 0,
            "blocked_count": 0,
            "blocked_reasons": [],
        }
        h = build_queen_handoff(pack, _baseline_audit())
        assert h["handoff_status"] == HANDOFF_BASELINE

    def test_active_pack_no_allowed_hold(self):
        pack = _active_pack(allowed=0, blocked=2)
        h = build_queen_handoff(pack, _complete_audit())
        assert h["handoff_status"] == HANDOFF_BASELINE

    def test_active_pack_no_allowed_mode(self):
        pack = _active_pack(allowed=0, blocked=2)
        h = build_queen_handoff(pack, _complete_audit())
        assert h["handoff_mode"] == MODE_BASELINE

    def test_active_pack_no_allowed_ready_false(self):
        pack = _active_pack(allowed=0, blocked=2)
        h = build_queen_handoff(pack, _complete_audit())
        assert h["handoff_ready"] is False

    def test_active_pack_no_allowed_reason_code(self):
        pack = _active_pack(allowed=0, blocked=2)
        h = build_queen_handoff(pack, _complete_audit())
        assert h["handoff_reason_code"] == REASON_NO_ALLOWED

    def test_baseline_reason_contains_status(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert "PACK_BASELINE" in h["handoff_reason"]


# ---------------------------------------------------------------------------
# 4. REJECT_HANDOFF — invalid inputs
# ---------------------------------------------------------------------------

class TestRejectedHandoff:
    def test_none_pack_rejected(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["handoff_status"] == HANDOFF_REJECTED

    def test_none_audit_rejected(self):
        h = build_queen_handoff(_active_pack(), None)
        assert h["handoff_status"] == HANDOFF_REJECTED

    def test_both_none_rejected(self):
        h = build_queen_handoff(None, None)
        assert h["handoff_status"] == HANDOFF_REJECTED

    def test_list_pack_rejected(self):
        h = build_queen_handoff([], _complete_audit())
        assert h["handoff_status"] == HANDOFF_REJECTED

    def test_string_pack_rejected(self):
        h = build_queen_handoff("pack", _complete_audit())
        assert h["handoff_status"] == HANDOFF_REJECTED

    def test_missing_intent_pack_status_rejected(self):
        pack = {"allowed_count": 2, "blocked_count": 0, "intent_count": 2}
        h = build_queen_handoff(pack, _complete_audit())
        assert h["handoff_status"] == HANDOFF_REJECTED

    def test_missing_audit_status_rejected(self):
        audit = {"total_markets_reviewed": 2}
        h = build_queen_handoff(_active_pack(), audit)
        assert h["handoff_status"] == HANDOFF_REJECTED

    def test_pack_rejected_status_gives_reject(self):
        h = build_queen_handoff(_rejected_pack(), _complete_audit())
        assert h["handoff_status"] == HANDOFF_REJECTED

    def test_pack_rejected_reason_code(self):
        h = build_queen_handoff(_rejected_pack(), _complete_audit())
        assert h["handoff_reason_code"] == REASON_INVALID_INPUT

    def test_rejected_mode(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["handoff_mode"] == MODE_REJECTED

    def test_rejected_ready_false(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["handoff_ready"] is False

    def test_rejected_reason_code(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["handoff_reason_code"] == REASON_INVALID_INPUT

    def test_rejected_totals_zero(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["total_intents"] == 0
        assert h["total_allowed"] == 0
        assert h["total_blocked"] == 0

    def test_rejected_snapshots_empty(self):
        h = build_queen_handoff(None, _complete_audit())
        assert h["intent_pack_snapshot"] == {}
        assert h["audit_snapshot"] == {}


# ---------------------------------------------------------------------------
# 5. Output field completeness
# ---------------------------------------------------------------------------

_REQUIRED_KEYS = {
    "handoff_status", "handoff_mode", "handoff_ready",
    "handoff_reason", "handoff_reason_code",
    "intent_pack_snapshot", "audit_snapshot",
    "total_intents", "total_allowed", "total_blocked",
    "queen_handoff_non_binding", "queen_handoff_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestOutputFieldCompleteness:
    def test_ready_has_all_keys(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        assert _REQUIRED_KEYS.issubset(h.keys())

    def test_baseline_has_all_keys(self):
        h = build_queen_handoff(_baseline_pack(), _baseline_audit())
        assert _REQUIRED_KEYS.issubset(h.keys())

    def test_rejected_has_all_keys(self):
        h = build_queen_handoff(None, _complete_audit())
        assert _REQUIRED_KEYS.issubset(h.keys())


# ---------------------------------------------------------------------------
# 6. Snapshot fields
# ---------------------------------------------------------------------------

class TestPackSnapshot:
    def test_snapshot_status(self):
        h = build_queen_handoff(_active_pack(allowed=2, blocked=1), _complete_audit())
        snap = h["intent_pack_snapshot"]
        assert snap["intent_pack_status"] == "PACK_ACTIVE"

    def test_snapshot_intent_count(self):
        h = build_queen_handoff(_active_pack(allowed=2, blocked=1), _complete_audit())
        snap = h["intent_pack_snapshot"]
        assert snap["intent_count"] == 3

    def test_snapshot_allowed_count(self):
        h = build_queen_handoff(_active_pack(allowed=2, blocked=1), _complete_audit())
        snap = h["intent_pack_snapshot"]
        assert snap["allowed_count"] == 2

    def test_snapshot_blocked_count(self):
        h = build_queen_handoff(_active_pack(allowed=2, blocked=1), _complete_audit())
        snap = h["intent_pack_snapshot"]
        assert snap["blocked_count"] == 1

    def test_snapshot_blocked_reasons_list(self):
        h = build_queen_handoff(_active_pack(allowed=2, blocked=1), _complete_audit())
        snap = h["intent_pack_snapshot"]
        assert isinstance(snap["blocked_reasons"], list)

    def test_snapshot_is_copy(self):
        pack = _active_pack(allowed=2, blocked=1)
        h = build_queen_handoff(pack, _complete_audit())
        h["intent_pack_snapshot"]["intent_pack_status"] = "MUTATED"
        assert pack["intent_pack_status"] == "PACK_ACTIVE"


class TestAuditSnapshot:
    def test_audit_snap_status(self):
        h = build_queen_handoff(_active_pack(), _complete_audit(increase=600.0, decrease=100.0))
        snap = h["audit_snapshot"]
        assert snap["audit_status"] == "AUDIT_COMPLETE"

    def test_audit_snap_reason_code(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        snap = h["audit_snapshot"]
        assert snap["audit_reason_code"] == "AUDIT_ALL_ALLOWED"

    def test_audit_snap_total_markets(self):
        h = build_queen_handoff(_active_pack(), _complete_audit())
        snap = h["audit_snapshot"]
        assert snap["total_markets_reviewed"] == 3

    def test_audit_snap_increase(self):
        h = build_queen_handoff(_active_pack(), _complete_audit(increase=800.0))
        snap = h["audit_snapshot"]
        assert snap["total_increase_eur"] == pytest.approx(800.0)

    def test_audit_snap_decrease(self):
        h = build_queen_handoff(_active_pack(), _complete_audit(decrease=300.0))
        snap = h["audit_snapshot"]
        assert snap["total_decrease_eur"] == pytest.approx(300.0)

    def test_audit_snap_hold_count(self):
        h = build_queen_handoff(_active_pack(), _complete_audit(hold=2))
        snap = h["audit_snapshot"]
        assert snap["total_hold_count"] == 2

    def test_audit_snap_net_change(self):
        h = build_queen_handoff(_active_pack(), _complete_audit(increase=500.0, decrease=200.0))
        snap = h["audit_snapshot"]
        assert snap["net_change_eur"] == pytest.approx(300.0)


# ---------------------------------------------------------------------------
# 7. Counts consistency
# ---------------------------------------------------------------------------

class TestCountsConsistency:
    def test_intents_equals_allowed_plus_blocked(self):
        h = build_queen_handoff(_active_pack(allowed=3, blocked=2), _complete_audit())
        assert h["total_intents"] == h["total_allowed"] + h["total_blocked"]

    def test_snapshot_counts_match_top_level(self):
        h = build_queen_handoff(_active_pack(allowed=3, blocked=2), _complete_audit())
        snap = h["intent_pack_snapshot"]
        assert snap["allowed_count"] == h["total_allowed"]
        assert snap["blocked_count"] == h["total_blocked"]
        assert snap["intent_count"]  == h["total_intents"]

    def test_zero_blocked_consistent(self):
        h = build_queen_handoff(_active_pack(allowed=4, blocked=0), _complete_audit())
        assert h["total_blocked"] == 0
        assert h["total_intents"] == 4

    def test_zero_intents_baseline_active_pack(self):
        pack = _active_pack(allowed=0, blocked=0)
        h = build_queen_handoff(pack, _complete_audit())
        assert h["total_intents"] == 0
        assert h["total_allowed"] == 0
        assert h["total_blocked"] == 0


# ---------------------------------------------------------------------------
# 8. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_inputs_same_output(self):
        pack  = _active_pack(allowed=2, blocked=1)
        audit = _complete_audit()
        h1 = build_queen_handoff(pack, audit)
        h2 = build_queen_handoff(pack, audit)
        assert h1 == h2

    def test_baseline_deterministic(self):
        pack  = _baseline_pack()
        audit = _baseline_audit()
        h1 = build_queen_handoff(pack, audit)
        h2 = build_queen_handoff(pack, audit)
        assert h1 == h2

    def test_rejected_deterministic(self):
        h1 = build_queen_handoff(None, _complete_audit())
        h2 = build_queen_handoff(None, _complete_audit())
        assert h1["handoff_status"] == h2["handoff_status"]
        assert h1["handoff_reason_code"] == h2["handoff_reason_code"]


# ---------------------------------------------------------------------------
# 9. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_pack_not_mutated(self):
        pack = _active_pack(allowed=2, blocked=1)
        original_allowed = pack["allowed_count"]
        build_queen_handoff(pack, _complete_audit())
        assert pack["allowed_count"] == original_allowed

    def test_audit_not_mutated(self):
        audit = _complete_audit(increase=500.0)
        original_increase = audit["total_increase_eur"]
        build_queen_handoff(_active_pack(), audit)
        assert audit["total_increase_eur"] == original_increase

    def test_blocked_reasons_not_mutated(self):
        pack = _active_pack(allowed=1, blocked=2)
        original_len = len(pack["blocked_reasons"])
        build_queen_handoff(pack, _complete_audit())
        assert len(pack["blocked_reasons"]) == original_len


# ---------------------------------------------------------------------------
# 10. Alias: build_handoff_from_pack_and_audit
# ---------------------------------------------------------------------------

class TestAlias:
    def test_alias_ready(self):
        pack  = _active_pack()
        audit = _complete_audit()
        h1 = build_queen_handoff(pack, audit)
        h2 = build_handoff_from_pack_and_audit(pack, audit)
        assert h1 == h2

    def test_alias_baseline(self):
        pack  = _baseline_pack()
        audit = _baseline_audit()
        h1 = build_queen_handoff(pack, audit)
        h2 = build_handoff_from_pack_and_audit(pack, audit)
        assert h1 == h2

    def test_alias_rejected(self):
        h1 = build_queen_handoff(None, _complete_audit())
        h2 = build_handoff_from_pack_and_audit(None, _complete_audit())
        assert h1["handoff_status"] == h2["handoff_status"]


# ---------------------------------------------------------------------------
# 11. Full chain: build_handoff_from_specs
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
    def test_chain_has_queen_handoff_key(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "queen_handoff" in result

    def test_chain_has_intent_pack_key(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "intent_pack" in result

    def test_chain_has_transition_audit_key(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "transition_audit" in result

    def test_chain_handoff_live_activation_false(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["queen_handoff"]["live_activation_allowed"] is False

    def test_chain_handoff_paper_only_true(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["queen_handoff"]["paper_only"] is True

    def test_chain_handoff_non_binding_true(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["queen_handoff"]["queen_handoff_non_binding"] is True

    def test_chain_handoff_simulation_only_true(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["queen_handoff"]["queen_handoff_simulation_only"] is True

    def test_chain_has_all_pipeline_keys(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        for key in [
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit", "queen_handoff",
        ]:
            assert key in result, f"missing key: {key}"

    def test_chain_handoff_status_valid(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["queen_handoff"]["handoff_status"] in {
            HANDOFF_READY, HANDOFF_BASELINE, HANDOFF_REJECTED
        }

    def test_chain_no_regimes_baseline_or_hold(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, market_regimes={})
        # Without regime data, handoff should not crash and should be valid
        h = result["queen_handoff"]
        assert h["handoff_status"] in {HANDOFF_READY, HANDOFF_BASELINE, HANDOFF_REJECTED}
        assert h["live_activation_allowed"] is False

    def test_chain_empty_specs_safe(self):
        result = build_handoff_from_specs([], 10_000.0)
        h = result["queen_handoff"]
        assert h["handoff_status"] in {HANDOFF_READY, HANDOFF_BASELINE, HANDOFF_REJECTED}
        assert h["live_activation_allowed"] is False

    def test_chain_zero_equity_safe(self):
        result = build_handoff_from_specs(_SPECS, 0.0, _REGIMES)
        h = result["queen_handoff"]
        assert h["live_activation_allowed"] is False

    def test_chain_handoff_counts_consistent(self):
        result = build_handoff_from_specs(_SPECS, 10_000.0, _REGIMES)
        h = result["queen_handoff"]
        assert h["total_intents"] == h["total_allowed"] + h["total_blocked"]
