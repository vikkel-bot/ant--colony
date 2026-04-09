"""
AC-79: Queen Decision Envelope Observability + Portfolio Preview Hook — tests

Covers:
  A. VARIANT_CONSIDERATION envelope → CONSIDER_VARIANT_ALLOCATION preview
  B. BASELINE_HOLD envelope         → HOLD_BASELINE_ALLOCATION preview
  C. ENVELOPE_REJECTED envelope     → REJECT_ENVELOPE preview (fail-closed)
  D. Non-dict / bad input           → REJECT_ENVELOPE
  E. portfolio_preview_non_binding always True
  F. portfolio_preview_simulation_only always True
  G. preview_portfolio_target: variant_id or "baseline"
  H. preview_portfolio_confidence: passthrough or 0.0 for rejected
  I. All required output fields present on all paths
  J. No mutation of inputs
  K. build_portfolio_preview_from_advisory() full pipeline
  L. Backward compat: AC-76/77/78 fields unchanged
"""
import copy
import importlib.util
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load modules
# ---------------------------------------------------------------------------
_PP_PATH = (
    Path(__file__).parent / "ant_colony" / "queen_portfolio_preview_lite.py"
)
_ENV_PATH = (
    Path(__file__).parent / "ant_colony" / "queen_decision_envelope_lite.py"
)


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_pp  = _load(_PP_PATH,  "_pp")
_env = _load(_ENV_PATH, "_env")

build_portfolio_preview              = _pp.build_portfolio_preview
build_portfolio_preview_from_advisory = _pp.build_portfolio_preview_from_advisory
PORTFOLIO_PREVIEW_REASON_CODES       = _pp.PORTFOLIO_PREVIEW_REASON_CODES

CONSIDER_VARIANT = _pp.PORTFOLIO_ACTION_CONSIDER_VARIANT
HOLD_BASELINE    = _pp.PORTFOLIO_ACTION_HOLD_BASELINE
REJECT_ENVELOPE  = _pp.PORTFOLIO_ACTION_REJECT_ENVELOPE


# ---------------------------------------------------------------------------
# Helpers — envelope dicts (AC-78 output shape)
# ---------------------------------------------------------------------------

def _variant_envelope(scenario_id="s_variant_1", confidence=0.72):
    return {
        "decision_envelope_status": "VARIANT_CONSIDERATION",
        "decision_mode":            "ADVISORY_VARIANT",
        "decision_reason":          f"CONSIDER_VARIANT|variant={scenario_id}",
        "decision_reason_code":     "ENVELOPE_VARIANT_CONSIDERATION_OK",
        "selected_variant_id":      scenario_id,
        "selected_action":          "CONSIDER_VARIANT",
        "decision_confidence":      confidence,
        "envelope_non_binding":     True,
        "envelope_simulation_only": True,
    }


def _hold_envelope(confidence=1.0):
    return {
        "decision_envelope_status": "BASELINE_HOLD",
        "decision_mode":            "ADVISORY_HOLD",
        "decision_reason":          "HOLD_BASELINE|variant=baseline",
        "decision_reason_code":     "ENVELOPE_BASELINE_HOLD_OK",
        "selected_variant_id":      "baseline",
        "selected_action":          "HOLD_BASELINE",
        "decision_confidence":      confidence,
        "envelope_non_binding":     True,
        "envelope_simulation_only": True,
    }


def _rejected_envelope(reason="preview rejected"):
    return {
        "decision_envelope_status": "ENVELOPE_REJECTED",
        "decision_mode":            "ADVISORY_REJECTED",
        "decision_reason":          reason,
        "decision_reason_code":     "ENVELOPE_REJECTED_PREVIEW_REJECT",
        "selected_variant_id":      "baseline",
        "selected_action":          "REJECT",
        "decision_confidence":      0.0,
        "envelope_non_binding":     True,
        "envelope_simulation_only": True,
    }


# Advisory helpers for full-pipeline tests
def _active_advisory(scenario_id="s_variant_1", confidence=0.72):
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


def _hold_advisory():
    return {
        "advisory_status":               "BASELINE_HOLD",
        "advisory_scenario_id":          "baseline",
        "advisory_action":               "KEEP_CURRENT_POLICY",
        "advisory_confidence":           1.0,
        "advisory_reason":               "NO_CANDIDATES_SIMULATED",
        "advisory_simulation_only":      True,
        "advisory_reason_code":          "HOLD_NO_CANDIDATES",
        "queen_intake_ready":            False,
        "queen_intake_contract_version": "v1",
    }


# All required portfolio preview output fields
_REQUIRED_PP_FIELDS = {
    "envelope_observed",
    "envelope_status",
    "preview_portfolio_action",
    "preview_portfolio_target",
    "preview_portfolio_reason",
    "preview_portfolio_reason_code",
    "preview_portfolio_confidence",
    "portfolio_preview_non_binding",
    "portfolio_preview_simulation_only",
}


# ---------------------------------------------------------------------------
# A. VARIANT_CONSIDERATION envelope → CONSIDER_VARIANT_ALLOCATION
# ---------------------------------------------------------------------------

class TestConsiderVariantAllocation:
    def _pp(self, **kw):
        return build_portfolio_preview(_variant_envelope(**kw))

    def test_preview_portfolio_action(self):
        assert self._pp()["preview_portfolio_action"] == CONSIDER_VARIANT

    def test_envelope_observed(self):
        assert self._pp()["envelope_observed"] == "VARIANT_CONSIDERATION"

    def test_envelope_status(self):
        assert self._pp()["envelope_status"] == "VARIANT_CONSIDERATION"

    def test_preview_portfolio_target_is_scenario_id(self):
        pp = self._pp(scenario_id="aggressive_v2")
        assert pp["preview_portfolio_target"] == "aggressive_v2"

    def test_preview_portfolio_confidence_passthrough(self):
        pp = self._pp(confidence=0.65)
        assert abs(pp["preview_portfolio_confidence"] - 0.65) < 1e-9

    def test_portfolio_preview_non_binding_true(self):
        assert self._pp()["portfolio_preview_non_binding"] is True

    def test_portfolio_preview_simulation_only_true(self):
        assert self._pp()["portfolio_preview_simulation_only"] is True

    def test_reason_code(self):
        pp = self._pp()
        assert pp["preview_portfolio_reason_code"] == "PORTFOLIO_PREVIEW_VARIANT_CONSIDERATION_OK"

    def test_all_required_fields(self):
        pp = self._pp()
        for f in _REQUIRED_PP_FIELDS:
            assert f in pp, f"Missing: {f}"

    def test_reason_contains_variant_id(self):
        pp = self._pp(scenario_id="conservative_v1")
        assert "conservative_v1" in pp["preview_portfolio_reason"]

    def test_confidence_is_float(self):
        assert isinstance(self._pp()["preview_portfolio_confidence"], float)


# ---------------------------------------------------------------------------
# B. BASELINE_HOLD envelope → HOLD_BASELINE_ALLOCATION
# ---------------------------------------------------------------------------

class TestHoldBaselineAllocation:
    def _pp(self, **kw):
        return build_portfolio_preview(_hold_envelope(**kw))

    def test_preview_portfolio_action(self):
        assert self._pp()["preview_portfolio_action"] == HOLD_BASELINE

    def test_envelope_observed(self):
        assert self._pp()["envelope_observed"] == "BASELINE_HOLD"

    def test_envelope_status(self):
        assert self._pp()["envelope_status"] == "BASELINE_HOLD"

    def test_preview_portfolio_target_is_baseline(self):
        assert self._pp()["preview_portfolio_target"] == "baseline"

    def test_portfolio_preview_non_binding_true(self):
        assert self._pp()["portfolio_preview_non_binding"] is True

    def test_portfolio_preview_simulation_only_true(self):
        assert self._pp()["portfolio_preview_simulation_only"] is True

    def test_reason_code(self):
        pp = self._pp()
        assert pp["preview_portfolio_reason_code"] == "PORTFOLIO_PREVIEW_BASELINE_HOLD_OK"

    def test_all_required_fields(self):
        pp = self._pp()
        for f in _REQUIRED_PP_FIELDS:
            assert f in pp, f"Missing: {f}"

    def test_confidence_passthrough(self):
        pp = self._pp(confidence=0.88)
        assert abs(pp["preview_portfolio_confidence"] - 0.88) < 1e-9


# ---------------------------------------------------------------------------
# C. ENVELOPE_REJECTED → REJECT_ENVELOPE (fail-closed)
# ---------------------------------------------------------------------------

class TestRejectEnvelopePath:
    def _pp(self):
        return build_portfolio_preview(_rejected_envelope())

    def test_preview_portfolio_action(self):
        assert self._pp()["preview_portfolio_action"] == REJECT_ENVELOPE

    def test_preview_portfolio_target_is_baseline(self):
        assert self._pp()["preview_portfolio_target"] == "baseline"

    def test_preview_portfolio_confidence_zero(self):
        assert self._pp()["preview_portfolio_confidence"] == pytest.approx(0.0)

    def test_portfolio_preview_non_binding_true(self):
        assert self._pp()["portfolio_preview_non_binding"] is True

    def test_portfolio_preview_simulation_only_true(self):
        assert self._pp()["portfolio_preview_simulation_only"] is True

    def test_reason_code(self):
        pp = self._pp()
        assert pp["preview_portfolio_reason_code"] == "PORTFOLIO_PREVIEW_REJECTED_ENVELOPE"

    def test_all_required_fields(self):
        pp = self._pp()
        for f in _REQUIRED_PP_FIELDS:
            assert f in pp, f"Missing: {f}"

    def test_envelope_observed_preserved(self):
        pp = self._pp()
        assert pp["envelope_observed"] == "ENVELOPE_REJECTED"


# ---------------------------------------------------------------------------
# D. Non-dict / bad input → REJECT_ENVELOPE
# ---------------------------------------------------------------------------

class TestBadEnvelopeInput:
    @pytest.mark.parametrize("bad", [None, [], "string", 42, True])
    def test_non_dict_gives_reject(self, bad):
        pp = build_portfolio_preview(bad)
        assert pp["preview_portfolio_action"] == REJECT_ENVELOPE
        assert pp["portfolio_preview_non_binding"] is True
        assert pp["portfolio_preview_simulation_only"] is True

    def test_empty_dict_gives_reject(self):
        pp = build_portfolio_preview({})
        assert pp["preview_portfolio_action"] == REJECT_ENVELOPE

    def test_missing_decision_envelope_status(self):
        env = _variant_envelope()
        del env["decision_envelope_status"]
        pp = build_portfolio_preview(env)
        assert pp["preview_portfolio_action"] == REJECT_ENVELOPE
        assert pp["preview_portfolio_reason_code"] == "PORTFOLIO_PREVIEW_REJECTED_MISSING_FIELD"

    def test_missing_selected_variant_id(self):
        env = _variant_envelope()
        del env["selected_variant_id"]
        pp = build_portfolio_preview(env)
        assert pp["preview_portfolio_action"] == REJECT_ENVELOPE

    def test_unknown_envelope_status(self):
        env = _variant_envelope()
        env["decision_envelope_status"] = "UNKNOWN_XYZ"
        pp = build_portfolio_preview(env)
        assert pp["preview_portfolio_action"] == REJECT_ENVELOPE
        assert pp["preview_portfolio_reason_code"] == "PORTFOLIO_PREVIEW_REJECTED_UNKNOWN_STATUS"


# ---------------------------------------------------------------------------
# E+F. portfolio_preview_non_binding and portfolio_preview_simulation_only always True
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    _cases = [
        lambda: build_portfolio_preview(_variant_envelope()),
        lambda: build_portfolio_preview(_hold_envelope()),
        lambda: build_portfolio_preview(_rejected_envelope()),
        lambda: build_portfolio_preview(None),
        lambda: build_portfolio_preview({}),
    ]

    def test_non_binding_always_true(self):
        for case in self._cases:
            assert case()["portfolio_preview_non_binding"] is True

    def test_simulation_only_always_true(self):
        for case in self._cases:
            assert case()["portfolio_preview_simulation_only"] is True


# ---------------------------------------------------------------------------
# G. preview_portfolio_target
# ---------------------------------------------------------------------------

class TestPreviewPortfolioTarget:
    def test_variant_uses_scenario_id(self):
        pp = build_portfolio_preview(_variant_envelope(scenario_id="beta_policy"))
        assert pp["preview_portfolio_target"] == "beta_policy"

    def test_hold_always_baseline(self):
        pp = build_portfolio_preview(_hold_envelope())
        assert pp["preview_portfolio_target"] == "baseline"

    def test_rejected_always_baseline(self):
        pp = build_portfolio_preview(_rejected_envelope())
        assert pp["preview_portfolio_target"] == "baseline"

    def test_bad_input_always_baseline(self):
        pp = build_portfolio_preview(None)
        assert pp["preview_portfolio_target"] == "baseline"


# ---------------------------------------------------------------------------
# H. preview_portfolio_confidence
# ---------------------------------------------------------------------------

class TestPreviewPortfolioConfidence:
    def test_variant_passthrough(self):
        pp = build_portfolio_preview(_variant_envelope(confidence=0.81))
        assert abs(pp["preview_portfolio_confidence"] - 0.81) < 1e-9

    def test_hold_passthrough(self):
        pp = build_portfolio_preview(_hold_envelope(confidence=0.95))
        assert abs(pp["preview_portfolio_confidence"] - 0.95) < 1e-9

    def test_rejected_is_zero(self):
        pp = build_portfolio_preview(_rejected_envelope())
        assert pp["preview_portfolio_confidence"] == pytest.approx(0.0)

    def test_none_confidence_gives_zero(self):
        env = _variant_envelope()
        env["decision_confidence"] = None
        pp = build_portfolio_preview(env)
        assert isinstance(pp["preview_portfolio_confidence"], float)

    def test_confidence_is_float(self):
        for env in [_variant_envelope(), _hold_envelope()]:
            pp = build_portfolio_preview(env)
            assert isinstance(pp["preview_portfolio_confidence"], float)


# ---------------------------------------------------------------------------
# I. All required fields on every path
# ---------------------------------------------------------------------------

class TestRequiredFieldsAllPaths:
    def _check(self, env_obj):
        pp = build_portfolio_preview(env_obj)
        for f in _REQUIRED_PP_FIELDS:
            assert f in pp, f"Missing field {f}"

    def test_variant_path(self):    self._check(_variant_envelope())
    def test_hold_path(self):       self._check(_hold_envelope())
    def test_reject_path(self):     self._check(_rejected_envelope())
    def test_none_path(self):       self._check(None)
    def test_empty_dict_path(self): self._check({})


# ---------------------------------------------------------------------------
# J. No mutation of inputs
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_variant_envelope_not_mutated(self):
        env = _variant_envelope()
        original = copy.deepcopy(env)
        build_portfolio_preview(env)
        assert env == original

    def test_hold_envelope_not_mutated(self):
        env = _hold_envelope()
        original = copy.deepcopy(env)
        build_portfolio_preview(env)
        assert env == original

    def test_rejected_envelope_not_mutated(self):
        env = _rejected_envelope()
        original = copy.deepcopy(env)
        build_portfolio_preview(env)
        assert env == original


# ---------------------------------------------------------------------------
# K. build_portfolio_preview_from_advisory() full pipeline
# ---------------------------------------------------------------------------

class TestFullPipeline:
    def test_active_advisory_gives_consider_variant(self):
        result = build_portfolio_preview_from_advisory(_active_advisory())
        assert set(result.keys()) >= {"intake", "preview", "envelope", "portfolio_preview"}
        pp = result["portfolio_preview"]
        assert pp["preview_portfolio_action"]          == CONSIDER_VARIANT
        assert pp["portfolio_preview_non_binding"]     is True
        assert pp["portfolio_preview_simulation_only"] is True

    def test_hold_advisory_gives_hold_baseline(self):
        result = build_portfolio_preview_from_advisory(_hold_advisory())
        pp = result["portfolio_preview"]
        assert pp["preview_portfolio_action"] == HOLD_BASELINE
        assert pp["preview_portfolio_target"] == "baseline"

    def test_invalid_advisory_gives_reject(self):
        result = build_portfolio_preview_from_advisory(None)
        pp = result["portfolio_preview"]
        assert pp["preview_portfolio_action"] == REJECT_ENVELOPE

    def test_contract_mismatch_gives_reject(self):
        adv = _active_advisory()
        adv["queen_intake_contract_version"] = "v99"
        result = build_portfolio_preview_from_advisory(adv)
        pp = result["portfolio_preview"]
        assert pp["preview_portfolio_action"] == REJECT_ENVELOPE

    def test_variant_id_flows_through(self):
        result = build_portfolio_preview_from_advisory(
            _active_advisory(scenario_id="tight_gates")
        )
        assert result["portfolio_preview"]["preview_portfolio_target"] == "tight_gates"

    def test_confidence_flows_through(self):
        result = build_portfolio_preview_from_advisory(
            _active_advisory(confidence=0.58)
        )
        assert abs(result["portfolio_preview"]["preview_portfolio_confidence"] - 0.58) < 1e-9

    def test_all_four_pipeline_keys_present(self):
        result = build_portfolio_preview_from_advisory(_active_advisory())
        assert "intake"            in result
        assert "preview"           in result
        assert "envelope"          in result
        assert "portfolio_preview" in result

    def test_pipeline_all_simulation_only_flags(self):
        result = build_portfolio_preview_from_advisory(_active_advisory())
        assert result["intake"]["intake_simulation_only"]          is True
        assert result["preview"]["preview_simulation_only"]        is True
        assert result["envelope"]["envelope_simulation_only"]      is True
        assert result["portfolio_preview"]["portfolio_preview_simulation_only"] is True


# ---------------------------------------------------------------------------
# L. Backward compat: AC-76/77/78 fields unchanged
# ---------------------------------------------------------------------------

class TestBackwardCompat:
    _AC76_FIELDS = {"intake_status", "intake_valid", "intake_simulation_only"}
    _AC77_FIELDS = {"interpretation_status", "preview_action", "preview_non_binding"}
    _AC78_FIELDS = {"decision_envelope_status", "envelope_non_binding", "selected_variant_id"}

    def test_intake_fields_preserved(self):
        result = build_portfolio_preview_from_advisory(_active_advisory())
        for f in self._AC76_FIELDS:
            assert f in result["intake"], f"AC-76 field missing: {f}"

    def test_preview_fields_preserved(self):
        result = build_portfolio_preview_from_advisory(_active_advisory())
        for f in self._AC77_FIELDS:
            assert f in result["preview"], f"AC-77 field missing: {f}"

    def test_envelope_fields_preserved(self):
        result = build_portfolio_preview_from_advisory(_active_advisory())
        for f in self._AC78_FIELDS:
            assert f in result["envelope"], f"AC-78 field missing: {f}"

    def test_portfolio_preview_is_separate_dict(self):
        result = build_portfolio_preview_from_advisory(_active_advisory())
        for key in ("intake", "preview", "envelope"):
            assert result[key] is not result["portfolio_preview"]

    def test_reason_codes_stable(self):
        assert PORTFOLIO_PREVIEW_REASON_CODES["CONSIDER_VARIANT_OK"] == "PORTFOLIO_PREVIEW_VARIANT_CONSIDERATION_OK"
        assert PORTFOLIO_PREVIEW_REASON_CODES["HOLD_BASELINE_OK"]    == "PORTFOLIO_PREVIEW_BASELINE_HOLD_OK"
        assert PORTFOLIO_PREVIEW_REASON_CODES["REJECT_ENVELOPE"]     == "PORTFOLIO_PREVIEW_REJECTED_ENVELOPE"
