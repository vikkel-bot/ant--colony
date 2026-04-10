"""
Tests for AC-96: Review Packet — Unified Human Decision Object
build_review_packet_lite.py
"""
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_review_packet_lite import (
    build_review_packet,
    build_review_packet_from_specs,
    PKT_READY, PKT_HOLD, PKT_REJECTED,
    PKT_MODE,
    HINT_ALLOW, HINT_HOLD, HINT_REJECT,
    VERSION, COMPONENT,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _promo_gate(status="PAPER_READY"):
    return {
        "promotion_status":          status,
        "promotion_mode":            "PROMOTION_READY",
        "promotion_ready":           status == "PAPER_READY",
        "promotion_reason":          "ok",
        "promotion_reason_code":     "PROMOTION_ALL_CLEAR",
        "promotion_decision":        f"{status}: PROMOTION_ALL_CLEAR",
        "paper_ready_candidate":     status == "PAPER_READY",
        "upstream_snapshot":         {},
        "promotion_non_binding":     True,
        "promotion_simulation_only": True,
        "paper_only":                True,
        "live_activation_allowed":   False,
    }


def _dossier(status="DOSSIER_READY"):
    return {
        "dossier_status":          status,
        "dossier_mode":            "DOSSIER_PAPER_READY",
        "dossier_ready_for_review": True,
        "dossier_reason":          "ok",
        "dossier_reason_code":     "DOSSIER_PROMOTION_READY",
        "promotion_snapshot":      {},
        "validation_snapshot":     {},
        "consistency_snapshot":    {},
        "handoff_snapshot":        {},
        "runner_snapshot":         {},
        "readiness_counts":        {"total_intents": 4, "total_allowed": 3, "total_blocked": 1,
                                    "ledger_entry_count": 4, "trace_step_count": 4, "matched_checks": 6},
        "dossier_non_binding":     True,
        "dossier_simulation_only": True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


def _review(status="REVIEW_READY", priority="LOW",
            hint="Candidate is PAPER_READY. All checks passed.",
            reason="ok", reason_code="REVIEW_PAPER_READY_OK",
            key_findings=None, blocking_findings=None):
    return {
        "review_status":           status,
        "review_mode":             "REVIEW_PAPER_READY",
        "review_decision_hint":    hint,
        "review_reason":           reason,
        "review_reason_code":      reason_code,
        "key_findings":            key_findings or ["promotion_status=PAPER_READY (PROMOTION_ALL_CLEAR)"],
        "blocking_findings":       blocking_findings or [],
        "review_priority":         priority,
        "review_non_binding":      True,
        "review_simulation_only":  True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


def _hold_review():
    return _review(
        status="REVIEW_HOLD", priority="MEDIUM",
        hint="Candidate is PAPER_HOLD. Upstream layers in baseline/hold state.",
        reason="hold", reason_code="REVIEW_PAPER_HOLD_OK",
    )


def _rejected_review(findings=None):
    return _review(
        status="REVIEW_REJECTED", priority="HIGH",
        hint="Candidate is PAPER_REJECTED. One or more checks failed.",
        reason="rejected", reason_code="REVIEW_PAPER_REJECTED_OK",
        blocking_findings=findings or ["validation failed: count mismatch"],
    )


def _all_ready():
    return (_promo_gate("PAPER_READY"), _dossier("DOSSIER_READY"), _review())


def _all_hold():
    return (_promo_gate("PAPER_HOLD"), _dossier("DOSSIER_HOLD"), _hold_review())


def _all_rejected():
    return (_promo_gate("PAPER_REJECTED"), _dossier("DOSSIER_REJECTED"), _rejected_review())


# ---------------------------------------------------------------------------
# 1. Always-correct flags
# ---------------------------------------------------------------------------

class TestFlags:
    def test_ready_non_binding_true(self):
        p = build_review_packet(*_all_ready())
        assert p["flags"]["non_binding"] is True

    def test_ready_simulation_only_true(self):
        p = build_review_packet(*_all_ready())
        assert p["flags"]["simulation_only"] is True

    def test_ready_paper_only_true(self):
        p = build_review_packet(*_all_ready())
        assert p["flags"]["paper_only"] is True

    def test_ready_live_activation_false(self):
        p = build_review_packet(*_all_ready())
        assert p["flags"]["live_activation_allowed"] is False

    def test_hold_non_binding_true(self):
        assert build_review_packet(*_all_hold())["flags"]["non_binding"] is True

    def test_hold_live_activation_false(self):
        assert build_review_packet(*_all_hold())["flags"]["live_activation_allowed"] is False

    def test_rejected_non_binding_true(self):
        assert build_review_packet(*_all_rejected())["flags"]["non_binding"] is True

    def test_rejected_live_activation_false(self):
        assert build_review_packet(*_all_rejected())["flags"]["live_activation_allowed"] is False

    def test_invalid_input_non_binding_true(self):
        p = build_review_packet(None, None, None)
        assert p["flags"]["non_binding"] is True

    def test_invalid_input_live_activation_false(self):
        p = build_review_packet(None, None, None)
        assert p["flags"]["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 2. Status mapping
# ---------------------------------------------------------------------------

class TestStatusMapping:
    def test_review_ready_maps_to_ready(self):
        p = build_review_packet(*_all_ready())
        assert p["review_packet_status"] == PKT_READY

    def test_review_hold_maps_to_hold(self):
        p = build_review_packet(*_all_hold())
        assert p["review_packet_status"] == PKT_HOLD

    def test_review_rejected_maps_to_rejected(self):
        p = build_review_packet(*_all_rejected())
        assert p["review_packet_status"] == PKT_REJECTED

    def test_unknown_review_status_maps_to_rejected(self):
        rev = _review(status="SOMETHING_UNKNOWN")
        p = build_review_packet(_promo_gate(), _dossier(), rev)
        assert p["review_packet_status"] == PKT_REJECTED

    def test_packet_mode_always_simulation_only(self):
        for fn in [_all_ready, _all_hold, _all_rejected]:
            p = build_review_packet(*fn())
            assert p["review_packet_mode"] == PKT_MODE


# ---------------------------------------------------------------------------
# 3. Decision mapping
# ---------------------------------------------------------------------------

class TestDecisionMapping:
    def test_ready_decision_hint_allow(self):
        p = build_review_packet(*_all_ready())
        assert p["decision"]["decision_hint"] == HINT_ALLOW

    def test_hold_decision_hint_hold(self):
        p = build_review_packet(*_all_hold())
        assert p["decision"]["decision_hint"] == HINT_HOLD

    def test_rejected_decision_hint_do_not_promote(self):
        p = build_review_packet(*_all_rejected())
        assert p["decision"]["decision_hint"] == HINT_REJECT

    def test_priority_taken_from_review(self):
        p = build_review_packet(*_all_ready())
        assert p["decision"]["priority"] == "LOW"

    def test_hold_priority_medium(self):
        p = build_review_packet(*_all_hold())
        assert p["decision"]["priority"] == "MEDIUM"

    def test_rejected_priority_high(self):
        p = build_review_packet(*_all_rejected())
        assert p["decision"]["priority"] == "HIGH"

    def test_reason_taken_from_review(self):
        p = build_review_packet(*_all_ready())
        assert p["decision"]["reason"] == "ok"

    def test_reason_code_taken_from_review(self):
        p = build_review_packet(*_all_ready())
        assert p["decision"]["reason_code"] == "REVIEW_PAPER_READY_OK"


# ---------------------------------------------------------------------------
# 4. Findings passthrough
# ---------------------------------------------------------------------------

class TestFindings:
    def test_key_findings_passed_through(self):
        kf = ["finding A", "finding B"]
        rev = _review(key_findings=kf)
        p = build_review_packet(_promo_gate(), _dossier(), rev)
        assert p["findings"]["key_findings"] == kf

    def test_blocking_findings_passed_through(self):
        bf = ["blocker X"]
        rev = _rejected_review(findings=bf)
        p = build_review_packet(_promo_gate("PAPER_REJECTED"), _dossier("DOSSIER_REJECTED"), rev)
        assert p["findings"]["blocking_findings"] == bf

    def test_blocking_findings_empty_when_ready(self):
        p = build_review_packet(*_all_ready())
        assert p["findings"]["blocking_findings"] == []

    def test_findings_is_copy_not_reference(self):
        kf = ["original"]
        rev = _review(key_findings=kf)
        p = build_review_packet(_promo_gate(), _dossier(), rev)
        p["findings"]["key_findings"].append("mutated")
        assert kf == ["original"]


# ---------------------------------------------------------------------------
# 5. Summary
# ---------------------------------------------------------------------------

class TestSummary:
    def test_summary_promotion_status(self):
        p = build_review_packet(*_all_ready())
        assert p["summary"]["promotion_status"] == "PAPER_READY"

    def test_summary_dossier_status(self):
        p = build_review_packet(*_all_ready())
        assert p["summary"]["dossier_status"] == "DOSSIER_READY"

    def test_summary_review_status(self):
        p = build_review_packet(*_all_ready())
        assert p["summary"]["review_status"] == "REVIEW_READY"

    def test_summary_hold(self):
        p = build_review_packet(*_all_hold())
        assert p["summary"]["promotion_status"] == "PAPER_HOLD"
        assert p["summary"]["dossier_status"] == "DOSSIER_HOLD"
        assert p["summary"]["review_status"] == "REVIEW_HOLD"

    def test_summary_none_gate_gives_empty_promotion_status(self):
        p = build_review_packet(None, _dossier(), _review())
        assert p["summary"]["promotion_status"] == ""


# ---------------------------------------------------------------------------
# 6. Snapshots
# ---------------------------------------------------------------------------

class TestSnapshots:
    def test_snapshots_has_promotion_key(self):
        p = build_review_packet(*_all_ready())
        assert "promotion" in p["snapshots"]

    def test_snapshots_has_dossier_key(self):
        p = build_review_packet(*_all_ready())
        assert "dossier" in p["snapshots"]

    def test_snapshots_has_review_key(self):
        p = build_review_packet(*_all_ready())
        assert "review" in p["snapshots"]

    def test_promotion_snapshot_status(self):
        p = build_review_packet(*_all_ready())
        assert p["snapshots"]["promotion"]["promotion_status"] == "PAPER_READY"

    def test_dossier_snapshot_status(self):
        p = build_review_packet(*_all_ready())
        assert p["snapshots"]["dossier"]["dossier_status"] == "DOSSIER_READY"

    def test_review_snapshot_status(self):
        p = build_review_packet(*_all_ready())
        assert p["snapshots"]["review"]["review_status"] == "REVIEW_READY"

    def test_review_snapshot_priority(self):
        p = build_review_packet(*_all_ready())
        assert p["snapshots"]["review"]["review_priority"] == "LOW"

    def test_dossier_snapshot_has_readiness_counts(self):
        p = build_review_packet(*_all_ready())
        assert "readiness_counts" in p["snapshots"]["dossier"]

    def test_none_gate_gives_empty_promotion_snapshot(self):
        p = build_review_packet(None, _dossier(), _review())
        assert p["snapshots"]["promotion"] == {}


# ---------------------------------------------------------------------------
# 7. Structural fields
# ---------------------------------------------------------------------------

class TestStructuralFields:
    def test_version(self):
        p = build_review_packet(*_all_ready())
        assert p["version"] == VERSION

    def test_component(self):
        p = build_review_packet(*_all_ready())
        assert p["component"] == COMPONENT

    def test_ts_utc_present(self):
        p = build_review_packet(*_all_ready())
        assert "ts_utc" in p
        assert isinstance(p["ts_utc"], str)
        assert p["ts_utc"].endswith("Z")

    def test_review_packet_status_present(self):
        p = build_review_packet(*_all_ready())
        assert "review_packet_status" in p

    def test_review_packet_mode_present(self):
        p = build_review_packet(*_all_ready())
        assert "review_packet_mode" in p


# ---------------------------------------------------------------------------
# 8. Output field completeness
# ---------------------------------------------------------------------------

_PKT_TOP_KEYS = {
    "version", "component", "ts_utc",
    "review_packet_status", "review_packet_mode",
    "decision", "findings", "summary", "snapshots", "flags",
}
_DECISION_KEYS = {"decision_hint", "priority", "reason", "reason_code"}
_FINDINGS_KEYS = {"key_findings", "blocking_findings"}
_SUMMARY_KEYS  = {"promotion_status", "dossier_status", "review_status"}
_SNAPSHOT_KEYS = {"promotion", "dossier", "review"}
_FLAGS_KEYS    = {"non_binding", "simulation_only", "paper_only", "live_activation_allowed"}

class TestFieldCompleteness:
    def _check(self, fn):
        p = build_review_packet(*fn())
        assert _PKT_TOP_KEYS.issubset(p.keys())
        assert _DECISION_KEYS.issubset(p["decision"].keys())
        assert _FINDINGS_KEYS.issubset(p["findings"].keys())
        assert _SUMMARY_KEYS.issubset(p["summary"].keys())
        assert _SNAPSHOT_KEYS.issubset(p["snapshots"].keys())
        assert _FLAGS_KEYS.issubset(p["flags"].keys())

    def test_ready_has_all_keys(self):
        self._check(_all_ready)

    def test_hold_has_all_keys(self):
        self._check(_all_hold)

    def test_rejected_has_all_keys(self):
        self._check(_all_rejected)

    def test_invalid_has_all_keys(self):
        p = build_review_packet(None, None, None)
        assert _PKT_TOP_KEYS.issubset(p.keys())
        assert _FLAGS_KEYS.issubset(p["flags"].keys())


# ---------------------------------------------------------------------------
# 9. Invalid input (fail-closed)
# ---------------------------------------------------------------------------

class TestInvalidInput:
    def test_none_review_gives_rejected(self):
        p = build_review_packet(_promo_gate(), _dossier(), None)
        assert p["review_packet_status"] == PKT_REJECTED

    def test_none_dossier_gives_rejected(self):
        p = build_review_packet(_promo_gate(), None, _review())
        assert p["review_packet_status"] == PKT_REJECTED

    def test_all_none_gives_rejected(self):
        p = build_review_packet(None, None, None)
        assert p["review_packet_status"] == PKT_REJECTED

    def test_list_review_gives_rejected(self):
        p = build_review_packet(_promo_gate(), _dossier(), [])
        assert p["review_packet_status"] == PKT_REJECTED

    def test_missing_review_status_gives_rejected(self):
        rev = _review()
        del rev["review_status"]
        p = build_review_packet(_promo_gate(), _dossier(), rev)
        assert p["review_packet_status"] == PKT_REJECTED

    def test_missing_dossier_status_gives_rejected(self):
        dos = _dossier()
        del dos["dossier_status"]
        p = build_review_packet(_promo_gate(), dos, _review())
        assert p["review_packet_status"] == PKT_REJECTED

    def test_rejected_decision_hint_do_not_promote(self):
        p = build_review_packet(None, None, None)
        assert p["decision"]["decision_hint"] == HINT_REJECT

    def test_rejected_priority_high(self):
        p = build_review_packet(None, None, None)
        assert p["decision"]["priority"] == "HIGH"

    def test_rejected_key_findings_non_empty(self):
        p = build_review_packet(None, None, None)
        assert len(p["findings"]["key_findings"]) >= 1


# ---------------------------------------------------------------------------
# 10. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_inputs_same_output_except_ts(self):
        g, d, r = _all_ready()
        p1 = build_review_packet(g, d, r)
        p2 = build_review_packet(g, d, r)
        # timestamps may differ by a second in slow CI; compare without ts_utc
        p1c = {k: v for k, v in p1.items() if k != "ts_utc"}
        p2c = {k: v for k, v in p2.items() if k != "ts_utc"}
        assert p1c == p2c

    def test_hold_deterministic(self):
        g, d, r = _all_hold()
        p1 = build_review_packet(g, d, r)
        p2 = build_review_packet(g, d, r)
        assert p1["review_packet_status"] == p2["review_packet_status"]
        assert p1["decision"] == p2["decision"]


# ---------------------------------------------------------------------------
# 11. No side effects on input objects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_review_not_mutated(self):
        rev = _review(key_findings=["A", "B"])
        original_len = len(rev["key_findings"])
        build_review_packet(_promo_gate(), _dossier(), rev)
        assert len(rev["key_findings"]) == original_len

    def test_dossier_not_mutated(self):
        dos = _dossier()
        original_status = dos["dossier_status"]
        build_review_packet(_promo_gate(), dos, _review())
        assert dos["dossier_status"] == original_status

    def test_gate_not_mutated(self):
        gate = _promo_gate()
        original = gate["promotion_status"]
        build_review_packet(gate, _dossier(), _review())
        assert gate["promotion_status"] == original


# ---------------------------------------------------------------------------
# 12. Full chain: build_review_packet_from_specs
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
    def test_has_review_packet_key(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "review_packet" in result

    def test_has_all_pipeline_keys(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        for key in [
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit", "queen_handoff",
            "runner_intake", "dry_run_consumption",
            "execution_ledger", "audit_trace",
            "replay_validation", "handoff_consistency",
            "promotion_gate", "paper_readiness_dossier", "human_review_summary",
            "review_packet",
        ]:
            assert key in result, f"missing key: {key}"

    def test_packet_flags_correct(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        flags = result["review_packet"]["flags"]
        assert flags["non_binding"] is True
        assert flags["simulation_only"] is True
        assert flags["paper_only"] is True
        assert flags["live_activation_allowed"] is False

    def test_packet_mode_simulation_only(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["review_packet"]["review_packet_mode"] == PKT_MODE

    def test_packet_status_valid(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["review_packet"]["review_packet_status"] in {PKT_READY, PKT_HOLD, PKT_REJECTED}

    def test_packet_decision_hint_valid(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["review_packet"]["decision"]["decision_hint"] in {HINT_ALLOW, HINT_HOLD, HINT_REJECT}

    def test_packet_has_ts_utc(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        ts = result["review_packet"]["ts_utc"]
        assert isinstance(ts, str) and ts.endswith("Z")

    def test_packet_version_correct(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["review_packet"]["version"] == VERSION

    def test_packet_component_correct(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["review_packet"]["component"] == COMPONENT

    def test_packet_key_findings_non_empty(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert len(result["review_packet"]["findings"]["key_findings"]) >= 1

    def test_packet_status_consistent_with_review(self):
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES)
        rev_status = result["human_review_summary"]["review_status"]
        pkt_status = result["review_packet"]["review_packet_status"]
        mapping = {"REVIEW_READY": PKT_READY, "REVIEW_HOLD": PKT_HOLD, "REVIEW_REJECTED": PKT_REJECTED}
        assert mapping.get(rev_status, PKT_REJECTED) == pkt_status

    def test_empty_specs_safe(self):
        result = build_review_packet_from_specs([], 10_000.0)
        assert result["review_packet"]["flags"]["live_activation_allowed"] is False

    def test_zero_equity_safe(self):
        result = build_review_packet_from_specs(_SPECS, 0.0, _REGIMES)
        assert result["review_packet"]["flags"]["non_binding"] is True

    def test_no_file_write_by_default(self, tmp_path):
        # write_output=False → no file written (we can't check C:\Trading\ANT_OUT
        # in CI, but we can verify write_output=False doesn't crash)
        result = build_review_packet_from_specs(_SPECS, 10_000.0, _REGIMES, write_output=False)
        assert "review_packet" in result
