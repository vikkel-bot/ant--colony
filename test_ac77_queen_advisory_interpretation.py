"""
AC-77: Queen Advisory Interpretation + Non-Binding Allocation Preview — tests

Covers:
  A. INTAKE_ACTIVE → CONSIDER_VARIANT preview
  B. INTAKE_HOLD   → HOLD_BASELINE preview
  C. INTAKE_INVALID → REJECT_INTAKE preview (fail-closed)
  D. Non-dict / bad input → REJECT_INTAKE
  E. preview_non_binding always True
  F. preview_simulation_only always True
  G. preview_variant_id: scenario_id for CONSIDER_VARIANT, "baseline" otherwise
  H. preview_confidence: passthrough for valid, 0.0 for invalid
  I. All required output fields present on all paths
  J. No mutation of inputs
  K. interpret_advisory() pipeline round-trip
  L. Backward compat: existing AC-76 intake fields unchanged
"""
import copy
import importlib.util
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load modules under test
# ---------------------------------------------------------------------------
_INTERP_PATH = (
    Path(__file__).parent
    / "ant_colony"
    / "queen_advisory_interpretation_lite.py"
)
_INTAKE_PATH = (
    Path(__file__).parent
    / "ant_colony"
    / "queen_advisory_intake_lite.py"
)


def _load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_interp = _load_module(_INTERP_PATH, "_interp")
_intake = _load_module(_INTAKE_PATH, "_intake")

interpret_intake        = _interp.interpret_intake
interpret_advisory      = _interp.interpret_advisory
consume_advisory        = _intake.consume_advisory
INTERPRETATION_REASON_CODES = _interp.INTERPRETATION_REASON_CODES

CONSIDER_VARIANT = _interp.INTERPRETATION_CONSIDER_VARIANT
HOLD_BASELINE    = _interp.INTERPRETATION_HOLD_BASELINE
REJECT_INTAKE    = _interp.INTERPRETATION_REJECT_INTAKE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _valid_hold_advisory(reason_code="HOLD_NO_CANDIDATES"):
    return {
        "advisory_status":               "BASELINE_HOLD",
        "advisory_scenario_id":          "baseline",
        "advisory_action":               "KEEP_CURRENT_POLICY",
        "advisory_confidence":           1.0,
        "advisory_reason":               "NO_CANDIDATES_SIMULATED|basis=0_scenarios",
        "advisory_simulation_only":      True,
        "advisory_reason_code":          reason_code,
        "queen_intake_ready":            False,
        "queen_intake_contract_version": "v1",
    }


def _active_intake(scenario_id="s_variant_1", confidence=0.72):
    """Build a valid INTAKE_ACTIVE result directly (bypassing consume_advisory)."""
    return {
        "intake_status":                       "INTAKE_ACTIVE",
        "intake_valid":                        True,
        "intake_reason":                       "ADVISORY_ACTIVE|...",
        "intake_reason_code":                  "ACTIVE_INTAKE_OK",
        "consumed_advisory_scenario_id":       scenario_id,
        "would_consider_variant":              True,
        "intake_simulation_only":              True,
        "consumed_advisory_status":            "ADVISORY_ACTIVE",
        "consumed_advisory_action":            "CONSIDER_VARIANT",
        "consumed_advisory_confidence":        confidence,
        "consumed_advisory_reason_code":       "ACTIVE_WORTH_REVIEW",
        "consumed_queen_intake_contract_version": "v1",
    }


def _hold_intake(scenario_id="baseline"):
    return {
        "intake_status":                       "INTAKE_HOLD",
        "intake_valid":                        True,
        "intake_reason":                       "BASELINE_HOLD|...",
        "intake_reason_code":                  "HOLD_INTAKE_OK",
        "consumed_advisory_scenario_id":       scenario_id,
        "would_consider_variant":              False,
        "intake_simulation_only":              True,
        "consumed_advisory_status":            "BASELINE_HOLD",
        "consumed_advisory_action":            "KEEP_CURRENT_POLICY",
        "consumed_advisory_confidence":        1.0,
        "consumed_advisory_reason_code":       "HOLD_NO_CANDIDATES",
        "consumed_queen_intake_contract_version": "v1",
    }


def _invalid_intake(reason="advisory is not a dict"):
    return {
        "intake_status":                       "INTAKE_INVALID",
        "intake_valid":                        False,
        "intake_reason":                       reason,
        "intake_reason_code":                  "HOLD_INVALID_INPUT",
        "consumed_advisory_scenario_id":       "baseline",
        "would_consider_variant":              False,
        "intake_simulation_only":              True,
        "consumed_advisory_status":            "",
        "consumed_advisory_action":            "",
        "consumed_advisory_confidence":        None,
        "consumed_advisory_reason_code":       "",
        "consumed_queen_intake_contract_version": "",
    }


# All required output fields for interpretation preview
_REQUIRED_PREVIEW_FIELDS = {
    "interpretation_status",
    "interpretation_reason",
    "interpretation_reason_code",
    "interpreted_scenario_id",
    "interpreted_advisory_action",
    "preview_action",
    "preview_variant_id",
    "preview_confidence",
    "preview_non_binding",
    "preview_simulation_only",
}


# ---------------------------------------------------------------------------
# A. INTAKE_ACTIVE → CONSIDER_VARIANT
# ---------------------------------------------------------------------------

class TestConsiderVariantPath:
    def _preview(self, **kw):
        return interpret_intake(_active_intake(**kw))

    def test_interpretation_status(self):
        assert self._preview()["interpretation_status"] == CONSIDER_VARIANT

    def test_preview_action(self):
        assert self._preview()["preview_action"] == CONSIDER_VARIANT

    def test_preview_variant_id_matches_scenario(self):
        p = self._preview(scenario_id="my_scenario")
        assert p["preview_variant_id"] == "my_scenario"

    def test_interpreted_scenario_id(self):
        p = self._preview(scenario_id="s_abc")
        assert p["interpreted_scenario_id"] == "s_abc"

    def test_interpreted_advisory_action(self):
        p = self._preview()
        assert p["interpreted_advisory_action"] == "CONSIDER_VARIANT"

    def test_preview_confidence_passthrough(self):
        p = self._preview(confidence=0.65)
        assert abs(p["preview_confidence"] - 0.65) < 1e-9

    def test_preview_non_binding_true(self):
        assert self._preview()["preview_non_binding"] is True

    def test_preview_simulation_only_true(self):
        assert self._preview()["preview_simulation_only"] is True

    def test_interpretation_reason_code(self):
        p = self._preview()
        assert p["interpretation_reason_code"] == "CONSIDER_VARIANT_INTAKE_ACTIVE"

    def test_all_required_fields_present(self):
        p = self._preview()
        for f in _REQUIRED_PREVIEW_FIELDS:
            assert f in p, f"Missing: {f}"


# ---------------------------------------------------------------------------
# B. INTAKE_HOLD → HOLD_BASELINE
# ---------------------------------------------------------------------------

class TestHoldBaselinePath:
    def _preview(self, **kw):
        return interpret_intake(_hold_intake(**kw))

    def test_interpretation_status(self):
        assert self._preview()["interpretation_status"] == HOLD_BASELINE

    def test_preview_action(self):
        assert self._preview()["preview_action"] == HOLD_BASELINE

    def test_preview_variant_id_is_baseline(self):
        assert self._preview()["preview_variant_id"] == "baseline"

    def test_interpreted_scenario_id(self):
        p = self._preview(scenario_id="baseline")
        assert p["interpreted_scenario_id"] == "baseline"

    def test_interpreted_advisory_action(self):
        p = self._preview()
        assert p["interpreted_advisory_action"] == "KEEP_CURRENT_POLICY"

    def test_preview_non_binding_true(self):
        assert self._preview()["preview_non_binding"] is True

    def test_preview_simulation_only_true(self):
        assert self._preview()["preview_simulation_only"] is True

    def test_interpretation_reason_code(self):
        p = self._preview()
        assert p["interpretation_reason_code"] == "HOLD_BASELINE_INTAKE_HOLD"

    def test_all_required_fields_present(self):
        p = self._preview()
        for f in _REQUIRED_PREVIEW_FIELDS:
            assert f in p, f"Missing: {f}"


# ---------------------------------------------------------------------------
# C. INTAKE_INVALID → REJECT_INTAKE (fail-closed)
# ---------------------------------------------------------------------------

class TestRejectIntakePath:
    def _preview(self):
        return interpret_intake(_invalid_intake())

    def test_interpretation_status(self):
        assert self._preview()["interpretation_status"] == REJECT_INTAKE

    def test_preview_action(self):
        assert self._preview()["preview_action"] == REJECT_INTAKE

    def test_preview_variant_id_is_baseline(self):
        assert self._preview()["preview_variant_id"] == "baseline"

    def test_preview_confidence_is_zero(self):
        assert self._preview()["preview_confidence"] == pytest.approx(0.0)

    def test_preview_non_binding_true(self):
        assert self._preview()["preview_non_binding"] is True

    def test_preview_simulation_only_true(self):
        assert self._preview()["preview_simulation_only"] is True

    def test_interpretation_reason_code(self):
        p = self._preview()
        assert p["interpretation_reason_code"] == "REJECT_INTAKE_INVALID"

    def test_all_required_fields_present(self):
        p = self._preview()
        for f in _REQUIRED_PREVIEW_FIELDS:
            assert f in p, f"Missing: {f}"

    def test_invalid_intake_with_contract_mismatch(self):
        # Simulate an intake that came from a contract-version-mismatch advisory
        intake = {
            **_invalid_intake("contract version mismatch"),
            "intake_reason_code": "HOLD_CONTRACT_VERSION_MISMATCH",
        }
        p = interpret_intake(intake)
        assert p["interpretation_status"] == REJECT_INTAKE
        assert p["preview_non_binding"] is True


# ---------------------------------------------------------------------------
# D. Non-dict / bad input → REJECT_INTAKE
# ---------------------------------------------------------------------------

class TestBadIntakeInput:
    @pytest.mark.parametrize("bad", [None, [], "string", 42, True])
    def test_non_dict_gives_reject(self, bad):
        p = interpret_intake(bad)
        assert p["interpretation_status"] == REJECT_INTAKE
        assert p["preview_non_binding"] is True
        assert p["preview_simulation_only"] is True

    def test_empty_dict_gives_reject(self):
        p = interpret_intake({})
        assert p["interpretation_status"] == REJECT_INTAKE

    def test_missing_intake_status_field(self):
        intake = _active_intake()
        del intake["intake_status"]
        p = interpret_intake(intake)
        assert p["interpretation_status"] == REJECT_INTAKE

    def test_missing_would_consider_variant_field(self):
        intake = _active_intake()
        del intake["would_consider_variant"]
        p = interpret_intake(intake)
        assert p["interpretation_status"] == REJECT_INTAKE

    def test_unknown_intake_status(self):
        intake = _active_intake()
        intake["intake_status"] = "UNKNOWN_STATUS"
        p = interpret_intake(intake)
        assert p["interpretation_status"] == REJECT_INTAKE
        assert p["interpretation_reason_code"] == "REJECT_UNKNOWN_INTAKE_STATUS"


# ---------------------------------------------------------------------------
# E+F. preview_non_binding and preview_simulation_only always True
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    _cases = [
        lambda: interpret_intake(_active_intake()),
        lambda: interpret_intake(_hold_intake()),
        lambda: interpret_intake(_invalid_intake()),
        lambda: interpret_intake(None),
        lambda: interpret_intake({}),
    ]

    def test_non_binding_always_true(self):
        for case in self._cases:
            p = case()
            assert p["preview_non_binding"] is True

    def test_simulation_only_always_true(self):
        for case in self._cases:
            p = case()
            assert p["preview_simulation_only"] is True


# ---------------------------------------------------------------------------
# G. preview_variant_id
# ---------------------------------------------------------------------------

class TestPreviewVariantId:
    def test_consider_variant_uses_scenario_id(self):
        p = interpret_intake(_active_intake(scenario_id="aggressive_v1"))
        assert p["preview_variant_id"] == "aggressive_v1"

    def test_hold_baseline_always_baseline(self):
        p = interpret_intake(_hold_intake())
        assert p["preview_variant_id"] == "baseline"

    def test_reject_always_baseline(self):
        p = interpret_intake(_invalid_intake())
        assert p["preview_variant_id"] == "baseline"

    def test_bad_input_always_baseline(self):
        p = interpret_intake(None)
        assert p["preview_variant_id"] == "baseline"


# ---------------------------------------------------------------------------
# H. preview_confidence
# ---------------------------------------------------------------------------

class TestPreviewConfidence:
    def test_consider_variant_passthrough(self):
        p = interpret_intake(_active_intake(confidence=0.88))
        assert abs(p["preview_confidence"] - 0.88) < 1e-9

    def test_hold_baseline_passthrough(self):
        hold = _hold_intake()
        hold["consumed_advisory_confidence"] = 0.95
        p = interpret_intake(hold)
        assert abs(p["preview_confidence"] - 0.95) < 1e-9

    def test_invalid_intake_confidence_is_zero(self):
        p = interpret_intake(_invalid_intake())
        assert p["preview_confidence"] == pytest.approx(0.0)

    def test_none_confidence_in_intake_gives_zero(self):
        intake = _active_intake()
        intake["consumed_advisory_confidence"] = None
        p = interpret_intake(intake)
        assert p["preview_confidence"] == pytest.approx(0.0)

    def test_confidence_is_float(self):
        for intake in [_active_intake(), _hold_intake()]:
            p = interpret_intake(intake)
            assert isinstance(p["preview_confidence"], float)


# ---------------------------------------------------------------------------
# I. All required fields on every path
# ---------------------------------------------------------------------------

class TestRequiredFieldsAllPaths:
    def _check(self, intake_obj):
        p = interpret_intake(intake_obj)
        for f in _REQUIRED_PREVIEW_FIELDS:
            assert f in p, f"Missing field {f}"

    def test_active_path(self):  self._check(_active_intake())
    def test_hold_path(self):    self._check(_hold_intake())
    def test_invalid_path(self): self._check(_invalid_intake())
    def test_none_path(self):    self._check(None)


# ---------------------------------------------------------------------------
# J. No mutation of inputs
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_active_intake_not_mutated(self):
        intake = _active_intake()
        original = copy.deepcopy(intake)
        interpret_intake(intake)
        assert intake == original

    def test_hold_intake_not_mutated(self):
        intake = _hold_intake()
        original = copy.deepcopy(intake)
        interpret_intake(intake)
        assert intake == original

    def test_invalid_intake_not_mutated(self):
        intake = _invalid_intake()
        original = copy.deepcopy(intake)
        interpret_intake(intake)
        assert intake == original


# ---------------------------------------------------------------------------
# K. interpret_advisory() pipeline round-trip
# ---------------------------------------------------------------------------

class TestInterpretAdvisoryPipeline:
    def test_active_advisory_gives_consider_variant(self):
        result = interpret_advisory(_valid_active_advisory())
        assert "intake" in result
        assert "preview" in result
        assert result["preview"]["interpretation_status"] == CONSIDER_VARIANT
        assert result["preview"]["preview_non_binding"] is True
        assert result["preview"]["preview_simulation_only"] is True

    def test_hold_advisory_gives_hold_baseline(self):
        result = interpret_advisory(_valid_hold_advisory())
        assert result["preview"]["interpretation_status"] == HOLD_BASELINE
        assert result["preview"]["preview_variant_id"] == "baseline"

    def test_invalid_advisory_gives_reject(self):
        result = interpret_advisory(None)
        assert result["preview"]["interpretation_status"] == REJECT_INTAKE

    def test_contract_mismatch_gives_reject(self):
        adv = _valid_active_advisory()
        adv["queen_intake_contract_version"] = "v99"
        result = interpret_advisory(adv)
        assert result["preview"]["interpretation_status"] == REJECT_INTAKE

    def test_pipeline_both_keys_present(self):
        result = interpret_advisory(_valid_active_advisory())
        assert "intake" in result
        assert "preview" in result

    def test_pipeline_intake_has_simulation_only(self):
        result = interpret_advisory(_valid_active_advisory())
        assert result["intake"]["intake_simulation_only"] is True


# ---------------------------------------------------------------------------
# L. Backward compat: existing AC-76 intake fields unchanged
# ---------------------------------------------------------------------------

class TestBackwardCompat:
    """confirm existing AC-76 intake fields are not altered by the interpretation layer."""

    _AC76_INTAKE_FIELDS = {
        "intake_status", "intake_valid", "intake_reason", "intake_reason_code",
        "consumed_advisory_scenario_id", "would_consider_variant", "intake_simulation_only",
        "consumed_advisory_status", "consumed_advisory_action", "consumed_advisory_confidence",
        "consumed_advisory_reason_code", "consumed_queen_intake_contract_version",
    }

    def test_intake_fields_preserved_in_pipeline_output(self):
        result = interpret_advisory(_valid_active_advisory())
        for f in self._AC76_INTAKE_FIELDS:
            assert f in result["intake"], f"AC-76 intake field missing: {f}"

    def test_preview_does_not_overwrite_intake(self):
        result = interpret_advisory(_valid_active_advisory())
        # intake and preview are separate dicts
        assert result["intake"] is not result["preview"]

    def test_consume_advisory_still_works_standalone(self):
        adv = _valid_active_advisory()
        intake = consume_advisory(adv)
        assert intake["intake_status"] == "INTAKE_ACTIVE"
        assert intake["intake_simulation_only"] is True

    def test_reason_codes_constant_values_stable(self):
        assert INTERPRETATION_REASON_CODES["CONSIDER_VARIANT_OK"] == "CONSIDER_VARIANT_INTAKE_ACTIVE"
        assert INTERPRETATION_REASON_CODES["HOLD_BASELINE_OK"]    == "HOLD_BASELINE_INTAKE_HOLD"
        assert INTERPRETATION_REASON_CODES["REJECT_INTAKE_INVALID"] == "REJECT_INTAKE_INVALID"
