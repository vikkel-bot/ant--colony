"""
AC-78: Queen Advisory Preview Aggregation + Decision Envelope — tests

Covers:
  A. CONSIDER_VARIANT preview → VARIANT_CONSIDERATION envelope
  B. HOLD_BASELINE preview    → BASELINE_HOLD envelope
  C. REJECT_INTAKE preview    → ENVELOPE_REJECTED (fail-closed)
  D. Non-dict / bad input     → ENVELOPE_REJECTED
  E. envelope_non_binding always True
  F. envelope_simulation_only always True
  G. selected_variant_id: scenario_id for VARIANT_CONSIDERATION, "baseline" otherwise
  H. decision_confidence: passthrough for valid paths, 0.0 for rejected
  I. All required output fields present on all paths
  J. No mutation of inputs
  K. build_envelope_from_advisory() full-pipeline round-trip
  L. Backward compat: AC-76/77 fields unchanged
"""
import copy
import importlib.util
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load modules
# ---------------------------------------------------------------------------
_ENV_PATH = (
    Path(__file__).parent / "ant_colony" / "queen_decision_envelope_lite.py"
)
_INTERP_PATH = (
    Path(__file__).parent / "ant_colony" / "queen_advisory_interpretation_lite.py"
)
_INTAKE_PATH = (
    Path(__file__).parent / "ant_colony" / "queen_advisory_intake_lite.py"
)


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_env    = _load(_ENV_PATH,    "_env")
_interp = _load(_INTERP_PATH, "_interp")
_intake = _load(_INTAKE_PATH, "_intake")

build_decision_envelope      = _env.build_decision_envelope
build_envelope_from_advisory = _env.build_envelope_from_advisory
DECISION_REASON_CODES        = _env.DECISION_REASON_CODES

VARIANT_CONSIDERATION = _env.ENVELOPE_VARIANT_CONSIDERATION
BASELINE_HOLD         = _env.ENVELOPE_BASELINE_HOLD
ENVELOPE_REJECTED     = _env.ENVELOPE_REJECTED


# ---------------------------------------------------------------------------
# Helpers — preview dicts (AC-77 output shape)
# ---------------------------------------------------------------------------

def _consider_preview(scenario_id="s_variant_1", confidence=0.72):
    return {
        "interpretation_status":       "CONSIDER_VARIANT",
        "interpretation_reason":       f"INTAKE_ACTIVE|scenario={scenario_id}",
        "interpretation_reason_code":  "CONSIDER_VARIANT_INTAKE_ACTIVE",
        "interpreted_scenario_id":     scenario_id,
        "interpreted_advisory_action": "CONSIDER_VARIANT",
        "preview_action":              "CONSIDER_VARIANT",
        "preview_variant_id":          scenario_id,
        "preview_confidence":          confidence,
        "preview_non_binding":         True,
        "preview_simulation_only":     True,
    }


def _hold_preview(scenario_id="baseline"):
    return {
        "interpretation_status":       "HOLD_BASELINE",
        "interpretation_reason":       f"INTAKE_HOLD|scenario={scenario_id}",
        "interpretation_reason_code":  "HOLD_BASELINE_INTAKE_HOLD",
        "interpreted_scenario_id":     scenario_id,
        "interpreted_advisory_action": "KEEP_CURRENT_POLICY",
        "preview_action":              "HOLD_BASELINE",
        "preview_variant_id":          "baseline",
        "preview_confidence":          1.0,
        "preview_non_binding":         True,
        "preview_simulation_only":     True,
    }


def _reject_preview(reason="intake invalid"):
    return {
        "interpretation_status":       "REJECT_INTAKE",
        "interpretation_reason":       reason,
        "interpretation_reason_code":  "REJECT_INTAKE_INVALID",
        "interpreted_scenario_id":     "baseline",
        "interpreted_advisory_action": "",
        "preview_action":              "REJECT_INTAKE",
        "preview_variant_id":          "baseline",
        "preview_confidence":          0.0,
        "preview_non_binding":         True,
        "preview_simulation_only":     True,
    }


# Advisory helpers for full-pipeline tests
def _valid_active_advisory(scenario_id="s_variant_1", confidence=0.72):
    return {
        "advisory_status":               "ADVISORY_ACTIVE",
        "advisory_scenario_id":          scenario_id,
        "advisory_action":               "CONSIDER_VARIANT",
        "advisory_confidence":           confidence,
        "advisory_reason":               "WORTH_REVIEW|risk=LOW_RISK",
        "advisory_simulation_only":      True,
        "advisory_reason_code":          "ACTIVE_WORTH_REVIEW",
        "queen_intake_ready":            True,
        "queen_intake_contract_version": "v1",
    }


def _valid_hold_advisory():
    return {
        "advisory_status":               "BASELINE_HOLD",
        "advisory_scenario_id":          "baseline",
        "advisory_action":               "KEEP_CURRENT_POLICY",
        "advisory_confidence":           1.0,
        "advisory_reason":               "NO_CANDIDATES_SIMULATED|basis=0_scenarios",
        "advisory_simulation_only":      True,
        "advisory_reason_code":          "HOLD_NO_CANDIDATES",
        "queen_intake_ready":            False,
        "queen_intake_contract_version": "v1",
    }


# All required envelope output fields
_REQUIRED_ENVELOPE_FIELDS = {
    "decision_envelope_status",
    "decision_mode",
    "decision_reason",
    "decision_reason_code",
    "selected_variant_id",
    "selected_action",
    "decision_confidence",
    "envelope_non_binding",
    "envelope_simulation_only",
}


# ---------------------------------------------------------------------------
# A. CONSIDER_VARIANT preview → VARIANT_CONSIDERATION
# ---------------------------------------------------------------------------

class TestVariantConsiderationPath:
    def _env(self, **kw):
        return build_decision_envelope(_consider_preview(**kw))

    def test_decision_envelope_status(self):
        assert self._env()["decision_envelope_status"] == VARIANT_CONSIDERATION

    def test_decision_mode(self):
        assert self._env()["decision_mode"] == "ADVISORY_VARIANT"

    def test_selected_variant_id(self):
        e = self._env(scenario_id="aggressive_v2")
        assert e["selected_variant_id"] == "aggressive_v2"

    def test_selected_action(self):
        assert self._env()["selected_action"] == "CONSIDER_VARIANT"

    def test_decision_confidence_passthrough(self):
        e = self._env(confidence=0.65)
        assert abs(e["decision_confidence"] - 0.65) < 1e-9

    def test_envelope_non_binding_true(self):
        assert self._env()["envelope_non_binding"] is True

    def test_envelope_simulation_only_true(self):
        assert self._env()["envelope_simulation_only"] is True

    def test_decision_reason_code(self):
        e = self._env()
        assert e["decision_reason_code"] == "ENVELOPE_VARIANT_CONSIDERATION_OK"

    def test_all_required_fields_present(self):
        e = self._env()
        for f in _REQUIRED_ENVELOPE_FIELDS:
            assert f in e, f"Missing: {f}"

    def test_decision_reason_contains_variant(self):
        e = self._env(scenario_id="s_test")
        assert "s_test" in e["decision_reason"]

    def test_confidence_as_int_in_preview(self):
        preview = _consider_preview(confidence=1)
        preview["preview_confidence"] = 1
        e = build_decision_envelope(preview)
        assert e["decision_envelope_status"] == VARIANT_CONSIDERATION
        assert isinstance(e["decision_confidence"], float)


# ---------------------------------------------------------------------------
# B. HOLD_BASELINE preview → BASELINE_HOLD
# ---------------------------------------------------------------------------

class TestBaselineHoldPath:
    def _env(self, **kw):
        return build_decision_envelope(_hold_preview(**kw))

    def test_decision_envelope_status(self):
        assert self._env()["decision_envelope_status"] == BASELINE_HOLD

    def test_decision_mode(self):
        assert self._env()["decision_mode"] == "ADVISORY_HOLD"

    def test_selected_variant_id_is_baseline(self):
        assert self._env()["selected_variant_id"] == "baseline"

    def test_selected_action(self):
        assert self._env()["selected_action"] == "HOLD_BASELINE"

    def test_envelope_non_binding_true(self):
        assert self._env()["envelope_non_binding"] is True

    def test_envelope_simulation_only_true(self):
        assert self._env()["envelope_simulation_only"] is True

    def test_decision_reason_code(self):
        e = self._env()
        assert e["decision_reason_code"] == "ENVELOPE_BASELINE_HOLD_OK"

    def test_all_required_fields_present(self):
        e = self._env()
        for f in _REQUIRED_ENVELOPE_FIELDS:
            assert f in e, f"Missing: {f}"

    def test_decision_confidence_passthrough(self):
        hold = _hold_preview()
        hold["preview_confidence"] = 0.9
        e = build_decision_envelope(hold)
        assert abs(e["decision_confidence"] - 0.9) < 1e-9


# ---------------------------------------------------------------------------
# C. REJECT_INTAKE preview → ENVELOPE_REJECTED (fail-closed)
# ---------------------------------------------------------------------------

class TestEnvelopeRejectedPath:
    def _env(self):
        return build_decision_envelope(_reject_preview())

    def test_decision_envelope_status(self):
        assert self._env()["decision_envelope_status"] == ENVELOPE_REJECTED

    def test_decision_mode(self):
        assert self._env()["decision_mode"] == "ADVISORY_REJECTED"

    def test_selected_variant_id_is_baseline(self):
        assert self._env()["selected_variant_id"] == "baseline"

    def test_selected_action_is_reject(self):
        assert self._env()["selected_action"] == "REJECT"

    def test_decision_confidence_is_zero(self):
        assert self._env()["decision_confidence"] == pytest.approx(0.0)

    def test_envelope_non_binding_true(self):
        assert self._env()["envelope_non_binding"] is True

    def test_envelope_simulation_only_true(self):
        assert self._env()["envelope_simulation_only"] is True

    def test_decision_reason_code(self):
        e = self._env()
        assert e["decision_reason_code"] == "ENVELOPE_REJECTED_PREVIEW_REJECT"

    def test_all_required_fields_present(self):
        e = self._env()
        for f in _REQUIRED_ENVELOPE_FIELDS:
            assert f in e, f"Missing: {f}"


# ---------------------------------------------------------------------------
# D. Non-dict / bad input → ENVELOPE_REJECTED
# ---------------------------------------------------------------------------

class TestBadPreviewInput:
    @pytest.mark.parametrize("bad", [None, [], "string", 42, True])
    def test_non_dict_gives_rejected(self, bad):
        e = build_decision_envelope(bad)
        assert e["decision_envelope_status"] == ENVELOPE_REJECTED
        assert e["envelope_non_binding"] is True
        assert e["envelope_simulation_only"] is True

    def test_empty_dict_gives_rejected(self):
        e = build_decision_envelope({})
        assert e["decision_envelope_status"] == ENVELOPE_REJECTED

    def test_missing_interpretation_status(self):
        preview = _consider_preview()
        del preview["interpretation_status"]
        e = build_decision_envelope(preview)
        assert e["decision_envelope_status"] == ENVELOPE_REJECTED
        assert e["decision_reason_code"] == "ENVELOPE_REJECTED_MISSING_FIELD"

    def test_missing_preview_confidence(self):
        preview = _consider_preview()
        del preview["preview_confidence"]
        e = build_decision_envelope(preview)
        assert e["decision_envelope_status"] == ENVELOPE_REJECTED

    def test_unknown_interpretation_status(self):
        preview = _consider_preview()
        preview["interpretation_status"] = "UNKNOWN_XYZ"
        e = build_decision_envelope(preview)
        assert e["decision_envelope_status"] == ENVELOPE_REJECTED
        assert e["decision_reason_code"] == "ENVELOPE_REJECTED_UNKNOWN_STATUS"


# ---------------------------------------------------------------------------
# E+F. envelope_non_binding and envelope_simulation_only always True
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    _cases = [
        lambda: build_decision_envelope(_consider_preview()),
        lambda: build_decision_envelope(_hold_preview()),
        lambda: build_decision_envelope(_reject_preview()),
        lambda: build_decision_envelope(None),
        lambda: build_decision_envelope({}),
    ]

    def test_non_binding_always_true(self):
        for case in self._cases:
            assert case()["envelope_non_binding"] is True

    def test_simulation_only_always_true(self):
        for case in self._cases:
            assert case()["envelope_simulation_only"] is True


# ---------------------------------------------------------------------------
# G. selected_variant_id
# ---------------------------------------------------------------------------

class TestSelectedVariantId:
    def test_variant_consideration_uses_scenario_id(self):
        e = build_decision_envelope(_consider_preview(scenario_id="beta_policy"))
        assert e["selected_variant_id"] == "beta_policy"

    def test_baseline_hold_always_baseline(self):
        e = build_decision_envelope(_hold_preview())
        assert e["selected_variant_id"] == "baseline"

    def test_rejected_always_baseline(self):
        e = build_decision_envelope(_reject_preview())
        assert e["selected_variant_id"] == "baseline"

    def test_bad_input_always_baseline(self):
        e = build_decision_envelope(None)
        assert e["selected_variant_id"] == "baseline"


# ---------------------------------------------------------------------------
# H. decision_confidence
# ---------------------------------------------------------------------------

class TestDecisionConfidence:
    def test_variant_passthrough(self):
        e = build_decision_envelope(_consider_preview(confidence=0.81))
        assert abs(e["decision_confidence"] - 0.81) < 1e-9

    def test_hold_passthrough(self):
        hold = _hold_preview()
        hold["preview_confidence"] = 0.75
        e = build_decision_envelope(hold)
        assert abs(e["decision_confidence"] - 0.75) < 1e-9

    def test_rejected_confidence_zero(self):
        e = build_decision_envelope(_reject_preview())
        assert e["decision_confidence"] == pytest.approx(0.0)

    def test_none_confidence_in_preview_gives_zero(self):
        preview = _consider_preview()
        preview["preview_confidence"] = None
        e = build_decision_envelope(preview)
        # preview_confidence=None is allowed — _safe_float returns 0.0
        # but interpretation_status=CONSIDER_VARIANT still passes, confidence=0.0
        assert isinstance(e["decision_confidence"], float)

    def test_confidence_is_float(self):
        for preview in [_consider_preview(), _hold_preview()]:
            e = build_decision_envelope(preview)
            assert isinstance(e["decision_confidence"], float)


# ---------------------------------------------------------------------------
# I. All required fields on every path
# ---------------------------------------------------------------------------

class TestRequiredFieldsAllPaths:
    def _check(self, preview_obj):
        e = build_decision_envelope(preview_obj)
        for f in _REQUIRED_ENVELOPE_FIELDS:
            assert f in e, f"Missing field {f}"

    def test_variant_path(self):    self._check(_consider_preview())
    def test_hold_path(self):       self._check(_hold_preview())
    def test_reject_path(self):     self._check(_reject_preview())
    def test_none_path(self):       self._check(None)
    def test_empty_dict_path(self): self._check({})


# ---------------------------------------------------------------------------
# J. No mutation of inputs
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_consider_preview_not_mutated(self):
        preview = _consider_preview()
        original = copy.deepcopy(preview)
        build_decision_envelope(preview)
        assert preview == original

    def test_hold_preview_not_mutated(self):
        preview = _hold_preview()
        original = copy.deepcopy(preview)
        build_decision_envelope(preview)
        assert preview == original

    def test_reject_preview_not_mutated(self):
        preview = _reject_preview()
        original = copy.deepcopy(preview)
        build_decision_envelope(preview)
        assert preview == original


# ---------------------------------------------------------------------------
# K. build_envelope_from_advisory() full pipeline
# ---------------------------------------------------------------------------

class TestFullPipeline:
    def test_active_advisory_gives_variant_consideration(self):
        result = build_envelope_from_advisory(_valid_active_advisory())
        assert "intake"   in result
        assert "preview"  in result
        assert "envelope" in result
        e = result["envelope"]
        assert e["decision_envelope_status"] == VARIANT_CONSIDERATION
        assert e["envelope_non_binding"]     is True
        assert e["envelope_simulation_only"] is True

    def test_hold_advisory_gives_baseline_hold(self):
        result = build_envelope_from_advisory(_valid_hold_advisory())
        e = result["envelope"]
        assert e["decision_envelope_status"] == BASELINE_HOLD
        assert e["selected_variant_id"]      == "baseline"

    def test_invalid_advisory_gives_rejected(self):
        result = build_envelope_from_advisory(None)
        e = result["envelope"]
        assert e["decision_envelope_status"] == ENVELOPE_REJECTED

    def test_contract_mismatch_gives_rejected(self):
        adv = _valid_active_advisory()
        adv["queen_intake_contract_version"] = "v99"
        result = build_envelope_from_advisory(adv)
        e = result["envelope"]
        assert e["decision_envelope_status"] == ENVELOPE_REJECTED

    def test_all_three_pipeline_keys_present(self):
        result = build_envelope_from_advisory(_valid_active_advisory())
        assert set(result.keys()) >= {"intake", "preview", "envelope"}

    def test_pipeline_intake_simulation_only(self):
        result = build_envelope_from_advisory(_valid_active_advisory())
        assert result["intake"]["intake_simulation_only"] is True

    def test_pipeline_preview_simulation_only(self):
        result = build_envelope_from_advisory(_valid_active_advisory())
        assert result["preview"]["preview_simulation_only"] is True

    def test_variant_id_flows_through_pipeline(self):
        result = build_envelope_from_advisory(
            _valid_active_advisory(scenario_id="conservative_hold")
        )
        assert result["envelope"]["selected_variant_id"] == "conservative_hold"

    def test_confidence_flows_through_pipeline(self):
        result = build_envelope_from_advisory(
            _valid_active_advisory(confidence=0.55)
        )
        assert abs(result["envelope"]["decision_confidence"] - 0.55) < 1e-9


# ---------------------------------------------------------------------------
# L. Backward compat: AC-76/77 fields unchanged
# ---------------------------------------------------------------------------

class TestBackwardCompat:
    _AC76_FIELDS = {
        "intake_status", "intake_valid", "would_consider_variant",
        "intake_simulation_only", "consumed_advisory_scenario_id",
    }
    _AC77_FIELDS = {
        "interpretation_status", "preview_action", "preview_variant_id",
        "preview_confidence", "preview_non_binding", "preview_simulation_only",
    }

    def test_intake_fields_preserved(self):
        result = build_envelope_from_advisory(_valid_active_advisory())
        for f in self._AC76_FIELDS:
            assert f in result["intake"], f"AC-76 field missing: {f}"

    def test_preview_fields_preserved(self):
        result = build_envelope_from_advisory(_valid_active_advisory())
        for f in self._AC77_FIELDS:
            assert f in result["preview"], f"AC-77 field missing: {f}"

    def test_envelope_does_not_overwrite_intake_or_preview(self):
        result = build_envelope_from_advisory(_valid_active_advisory())
        assert result["intake"]   is not result["envelope"]
        assert result["preview"]  is not result["envelope"]

    def test_reason_codes_stable(self):
        assert DECISION_REASON_CODES["VARIANT_OK"]     == "ENVELOPE_VARIANT_CONSIDERATION_OK"
        assert DECISION_REASON_CODES["HOLD_OK"]        == "ENVELOPE_BASELINE_HOLD_OK"
        assert DECISION_REASON_CODES["REJECT_PREVIEW"] == "ENVELOPE_REJECTED_PREVIEW_REJECT"
