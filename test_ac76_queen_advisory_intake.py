"""
AC-76: Queen Advisory Intake (Passive) — tests

Covers:
  A. Valid ADVISORY_ACTIVE correctly consumed → INTAKE_ACTIVE
  B. Valid BASELINE_HOLD correctly consumed → INTAKE_HOLD
  C. Contract version mismatch → INTAKE_INVALID (passive hold)
  D. Missing required fields → INTAKE_INVALID
  E. Invalid advisory_status / advisory_action → INTAKE_INVALID
  F. Invalid advisory_confidence (non-numeric, out of range) → INTAKE_INVALID
  G. queen_intake_ready=False → INTAKE_INVALID when ADVISORY_ACTIVE
  H. Non-dict input → INTAKE_INVALID
  I. intake_simulation_only always True
  J. would_consider_variant: True only for INTAKE_ACTIVE
  K. Backward compat: all required output fields present
  L. No mutation of inputs
  M. Contract constant EXPECTED_CONTRACT_VERSION == "v1"
"""
import copy
import importlib.util
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load module under test
# ---------------------------------------------------------------------------
_INTAKE_PATH = (
    Path(__file__).parent
    / "ant_colony"
    / "queen_advisory_intake_lite.py"
)

def _load_intake():
    spec = importlib.util.spec_from_file_location("_intake", _INTAKE_PATH)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_intake = _load_intake()

consume_advisory          = _intake.consume_advisory
EXPECTED_CONTRACT_VERSION = _intake.EXPECTED_CONTRACT_VERSION
INTAKE_REASON_CODES       = _intake.INTAKE_REASON_CODES
_REQUIRED_FIELDS          = _intake._REQUIRED_FIELDS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_active_advisory(
    scenario_id="s_variant_1",
    confidence=0.72,
    action="CONSIDER_VARIANT",
    reason_code="ACTIVE_WORTH_REVIEW",
) -> dict:
    return {
        "advisory_status":               "ADVISORY_ACTIVE",
        "advisory_scenario_id":          scenario_id,
        "advisory_action":               action,
        "advisory_confidence":           confidence,
        "advisory_reason":               f"WORTH_REVIEW|risk=LOW_RISK|basis=7_scenarios",
        "advisory_simulation_only":      True,
        "advisory_reason_code":          reason_code,
        "queen_intake_ready":            True,
        "queen_intake_contract_version": EXPECTED_CONTRACT_VERSION,
    }


def _valid_hold_advisory(
    scenario_id="baseline",
    reason_code="HOLD_NO_CANDIDATES",
) -> dict:
    return {
        "advisory_status":               "BASELINE_HOLD",
        "advisory_scenario_id":          scenario_id,
        "advisory_action":               "KEEP_CURRENT_POLICY",
        "advisory_confidence":           1.0,
        "advisory_reason":               "NO_CANDIDATES_SIMULATED|basis=0_scenarios",
        "advisory_simulation_only":      True,
        "advisory_reason_code":          reason_code,
        "queen_intake_ready":            False,
        "queen_intake_contract_version": EXPECTED_CONTRACT_VERSION,
    }


# All required output fields for intake result
_REQUIRED_OUTPUT_FIELDS = {
    "intake_status",
    "intake_valid",
    "intake_reason",
    "intake_reason_code",
    "consumed_advisory_scenario_id",
    "would_consider_variant",
    "intake_simulation_only",
    "consumed_advisory_status",
    "consumed_advisory_action",
    "consumed_advisory_confidence",
    "consumed_advisory_reason_code",
    "consumed_queen_intake_contract_version",
}


# ---------------------------------------------------------------------------
# M. Contract constant
# ---------------------------------------------------------------------------

class TestContractConstant:
    def test_expected_contract_version_is_v1(self):
        assert EXPECTED_CONTRACT_VERSION == "v1"

    def test_intake_reason_codes_active_ok_prefix(self):
        assert INTAKE_REASON_CODES["ACTIVE_OK"].startswith("ACTIVE_")

    def test_intake_reason_codes_hold_paths_prefix(self):
        hold_keys = [k for k in INTAKE_REASON_CODES if k != "ACTIVE_OK"]
        for k in hold_keys:
            assert INTAKE_REASON_CODES[k].startswith("HOLD_"), f"Bad prefix: {k}"


# ---------------------------------------------------------------------------
# A. Valid ADVISORY_ACTIVE → INTAKE_ACTIVE
# ---------------------------------------------------------------------------

class TestValidAdvisoryActive:
    def _result(self, **kw):
        return consume_advisory(_valid_active_advisory(**kw))

    def test_intake_status_active(self):
        assert self._result()["intake_status"] == "INTAKE_ACTIVE"

    def test_intake_valid_true(self):
        assert self._result()["intake_valid"] is True

    def test_would_consider_variant_true(self):
        assert self._result()["would_consider_variant"] is True

    def test_intake_simulation_only_true(self):
        assert self._result()["intake_simulation_only"] is True

    def test_consumed_scenario_id(self):
        r = self._result(scenario_id="my_variant")
        assert r["consumed_advisory_scenario_id"] == "my_variant"

    def test_consumed_advisory_status(self):
        assert self._result()["consumed_advisory_status"] == "ADVISORY_ACTIVE"

    def test_consumed_advisory_action(self):
        assert self._result()["consumed_advisory_action"] == "CONSIDER_VARIANT"

    def test_consumed_advisory_confidence(self):
        r = self._result(confidence=0.72)
        assert r["consumed_advisory_confidence"] == pytest.approx(0.72)

    def test_consumed_reason_code_passthrough(self):
        r = self._result(reason_code="ACTIVE_WORTH_REVIEW")
        assert r["consumed_advisory_reason_code"] == "ACTIVE_WORTH_REVIEW"

    def test_consumed_contract_version_passthrough(self):
        r = self._result()
        assert r["consumed_queen_intake_contract_version"] == EXPECTED_CONTRACT_VERSION

    def test_intake_reason_code_active_ok(self):
        assert self._result()["intake_reason_code"] == "ACTIVE_INTAKE_OK"

    def test_all_required_output_fields_present(self):
        r = self._result()
        for f in _REQUIRED_OUTPUT_FIELDS:
            assert f in r, f"Missing output field: {f}"

    def test_candidate_for_trial_also_active(self):
        adv = _valid_active_advisory(
            action="CONSIDER_VARIANT",
            reason_code="ACTIVE_CANDIDATE_FOR_TRIAL",
        )
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_ACTIVE"
        assert r["would_consider_variant"] is True


# ---------------------------------------------------------------------------
# B. Valid BASELINE_HOLD → INTAKE_HOLD
# ---------------------------------------------------------------------------

class TestValidBaselineHold:
    def _result(self, **kw):
        return consume_advisory(_valid_hold_advisory(**kw))

    def test_intake_status_hold(self):
        assert self._result()["intake_status"] == "INTAKE_HOLD"

    def test_intake_valid_true(self):
        assert self._result()["intake_valid"] is True

    def test_would_consider_variant_false(self):
        assert self._result()["would_consider_variant"] is False

    def test_intake_simulation_only_true(self):
        assert self._result()["intake_simulation_only"] is True

    def test_consumed_scenario_id_baseline(self):
        assert self._result()["consumed_advisory_scenario_id"] == "baseline"

    def test_consumed_advisory_status(self):
        assert self._result()["consumed_advisory_status"] == "BASELINE_HOLD"

    def test_consumed_advisory_action(self):
        assert self._result()["consumed_advisory_action"] == "KEEP_CURRENT_POLICY"

    def test_intake_reason_code_hold_ok(self):
        assert self._result()["intake_reason_code"] == "HOLD_INTAKE_OK"

    def test_all_required_output_fields_present(self):
        r = self._result()
        for f in _REQUIRED_OUTPUT_FIELDS:
            assert f in r, f"Missing output field: {f}"

    def test_hold_with_different_reason_codes(self):
        for code in ("HOLD_ALL_NO_CHANGE", "HOLD_ALL_REJECTED_OR_INSUFFICIENT",
                     "HOLD_NO_SUITABLE_CANDIDATE"):
            adv = _valid_hold_advisory(reason_code=code)
            r = consume_advisory(adv)
            assert r["intake_status"] == "INTAKE_HOLD"
            assert r["consumed_advisory_reason_code"] == code


# ---------------------------------------------------------------------------
# C. Contract version mismatch → INTAKE_INVALID
# ---------------------------------------------------------------------------

class TestContractVersionMismatch:
    def _mismatch_advisory(self, version):
        adv = _valid_active_advisory()
        adv["queen_intake_contract_version"] = version
        return adv

    def test_wrong_version_gives_invalid(self):
        r = consume_advisory(self._mismatch_advisory("v2"))
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_wrong_version_intake_valid_false(self):
        r = consume_advisory(self._mismatch_advisory("v2"))
        assert r["intake_valid"] is False

    def test_wrong_version_would_consider_false(self):
        r = consume_advisory(self._mismatch_advisory("v2"))
        assert r["would_consider_variant"] is False

    def test_wrong_version_reason_code(self):
        r = consume_advisory(self._mismatch_advisory("v99"))
        assert r["intake_reason_code"] == "HOLD_CONTRACT_VERSION_MISMATCH"

    def test_empty_version_gives_invalid(self):
        r = consume_advisory(self._mismatch_advisory(""))
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_none_version_gives_invalid(self):
        adv = _valid_active_advisory()
        adv["queen_intake_contract_version"] = None
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"


# ---------------------------------------------------------------------------
# D. Missing required fields → INTAKE_INVALID
# ---------------------------------------------------------------------------

class TestMissingRequiredFields:
    def test_missing_advisory_status(self):
        adv = _valid_active_advisory()
        del adv["advisory_status"]
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"
        assert r["intake_reason_code"] == "HOLD_MISSING_FIELD"

    def test_missing_queen_intake_ready(self):
        adv = _valid_active_advisory()
        del adv["queen_intake_ready"]
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_missing_advisory_confidence(self):
        adv = _valid_active_advisory()
        del adv["advisory_confidence"]
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_missing_queen_intake_contract_version(self):
        adv = _valid_active_advisory()
        del adv["queen_intake_contract_version"]
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_missing_advisory_reason_code(self):
        adv = _valid_active_advisory()
        del adv["advisory_reason_code"]
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_missing_advisory_action(self):
        adv = _valid_active_advisory()
        del adv["advisory_action"]
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_missing_advisory_scenario_id(self):
        adv = _valid_active_advisory()
        del adv["advisory_scenario_id"]
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_all_missing_fields_give_invalid(self):
        for field in _REQUIRED_FIELDS:
            adv = _valid_active_advisory()
            del adv[field]
            r = consume_advisory(adv)
            assert r["intake_status"] == "INTAKE_INVALID", f"Expected INVALID when {field} missing"


# ---------------------------------------------------------------------------
# E. Invalid advisory_status / advisory_action
# ---------------------------------------------------------------------------

class TestInvalidFieldValues:
    def test_unknown_advisory_status(self):
        adv = _valid_active_advisory()
        adv["advisory_status"] = "UNKNOWN_STATUS"
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"
        assert r["intake_reason_code"] == "HOLD_INVALID_STATUS"

    def test_empty_advisory_status(self):
        adv = _valid_active_advisory()
        adv["advisory_status"] = ""
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_unknown_advisory_action(self):
        adv = _valid_active_advisory()
        adv["advisory_action"] = "DO_SOMETHING"
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"
        assert r["intake_reason_code"] == "HOLD_INVALID_ACTION"

    def test_empty_advisory_action(self):
        adv = _valid_active_advisory()
        adv["advisory_action"] = ""
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"


# ---------------------------------------------------------------------------
# F. Invalid advisory_confidence
# ---------------------------------------------------------------------------

class TestInvalidConfidence:
    def test_confidence_string_nonnumeric(self):
        adv = _valid_active_advisory()
        adv["advisory_confidence"] = "high"
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"
        assert r["intake_reason_code"] == "HOLD_INVALID_CONFIDENCE"

    def test_confidence_none(self):
        adv = _valid_active_advisory()
        adv["advisory_confidence"] = None
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_confidence_above_1(self):
        adv = _valid_active_advisory()
        adv["advisory_confidence"] = 1.01
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"
        assert r["intake_reason_code"] == "HOLD_INVALID_CONFIDENCE"

    def test_confidence_negative(self):
        adv = _valid_active_advisory()
        adv["advisory_confidence"] = -0.01
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"

    def test_confidence_exactly_0_valid(self):
        adv = _valid_active_advisory(confidence=0.0)
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_ACTIVE"

    def test_confidence_exactly_1_valid(self):
        adv = _valid_hold_advisory()
        adv["advisory_confidence"] = 1.0
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_HOLD"

    def test_confidence_as_int_is_valid(self):
        adv = _valid_active_advisory(confidence=1)
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_ACTIVE"


# ---------------------------------------------------------------------------
# G. queen_intake_ready=False when ADVISORY_ACTIVE → INTAKE_INVALID
# ---------------------------------------------------------------------------

class TestIntakeReadyValidation:
    def test_active_with_ready_false_is_invalid(self):
        adv = _valid_active_advisory()
        adv["queen_intake_ready"] = False
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"
        assert r["intake_reason_code"] == "HOLD_ACTIVE_BUT_NOT_READY"
        assert r["would_consider_variant"] is False

    def test_hold_with_ready_false_is_valid(self):
        adv = _valid_hold_advisory()
        adv["queen_intake_ready"] = False
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_HOLD"
        assert r["intake_valid"] is True

    def test_hold_with_ready_true_is_valid(self):
        adv = _valid_hold_advisory()
        adv["queen_intake_ready"] = True
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_HOLD"
        assert r["intake_valid"] is True

    def test_active_with_ready_none_is_invalid(self):
        adv = _valid_active_advisory()
        adv["queen_intake_ready"] = None
        r = consume_advisory(adv)
        assert r["intake_status"] == "INTAKE_INVALID"


# ---------------------------------------------------------------------------
# H. Non-dict input → INTAKE_INVALID
# ---------------------------------------------------------------------------

class TestNonDictInput:
    @pytest.mark.parametrize("bad_input", [None, [], "string", 42, 3.14, True])
    def test_non_dict_gives_invalid(self, bad_input):
        r = consume_advisory(bad_input)
        assert r["intake_status"] == "INTAKE_INVALID"
        assert r["intake_valid"] is False
        assert r["intake_reason_code"] == "HOLD_INVALID_INPUT"
        assert r["would_consider_variant"] is False

    def test_empty_dict_gives_invalid(self):
        r = consume_advisory({})
        assert r["intake_status"] == "INTAKE_INVALID"


# ---------------------------------------------------------------------------
# I. intake_simulation_only always True
# ---------------------------------------------------------------------------

class TestSimulationOnlyFlag:
    def test_simulation_only_on_active(self):
        r = consume_advisory(_valid_active_advisory())
        assert r["intake_simulation_only"] is True

    def test_simulation_only_on_hold(self):
        r = consume_advisory(_valid_hold_advisory())
        assert r["intake_simulation_only"] is True

    def test_simulation_only_on_invalid(self):
        r = consume_advisory(None)
        assert r["intake_simulation_only"] is True

    def test_simulation_only_on_contract_mismatch(self):
        adv = _valid_active_advisory()
        adv["queen_intake_contract_version"] = "v99"
        r = consume_advisory(adv)
        assert r["intake_simulation_only"] is True


# ---------------------------------------------------------------------------
# J. would_consider_variant
# ---------------------------------------------------------------------------

class TestWouldConsiderVariant:
    def test_true_only_for_intake_active(self):
        r = consume_advisory(_valid_active_advisory())
        assert r["would_consider_variant"] is True

    def test_false_for_intake_hold(self):
        r = consume_advisory(_valid_hold_advisory())
        assert r["would_consider_variant"] is False

    def test_false_for_intake_invalid(self):
        for bad in [None, {}, _valid_active_advisory()]:
            if bad == {}:
                pass
            elif bad is None:
                r = consume_advisory(bad)
                assert r["would_consider_variant"] is False
            else:
                bad["advisory_status"] = "BROKEN"
                r = consume_advisory(bad)
                assert r["would_consider_variant"] is False

    def test_is_bool(self):
        for adv in [_valid_active_advisory(), _valid_hold_advisory()]:
            r = consume_advisory(adv)
            assert isinstance(r["would_consider_variant"], bool)


# ---------------------------------------------------------------------------
# K. All required output fields present in all paths
# ---------------------------------------------------------------------------

class TestOutputFieldsPresent:
    def _check(self, adv):
        r = consume_advisory(adv)
        for f in _REQUIRED_OUTPUT_FIELDS:
            assert f in r, f"Missing: {f}"

    def test_active_path_complete(self):
        self._check(_valid_active_advisory())

    def test_hold_path_complete(self):
        self._check(_valid_hold_advisory())

    def test_invalid_path_complete(self):
        self._check(None)

    def test_mismatch_path_complete(self):
        adv = _valid_active_advisory()
        adv["queen_intake_contract_version"] = "v99"
        self._check(adv)


# ---------------------------------------------------------------------------
# L. No mutation of inputs
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_active_advisory_not_mutated(self):
        adv = _valid_active_advisory()
        original = copy.deepcopy(adv)
        consume_advisory(adv)
        assert adv == original

    def test_hold_advisory_not_mutated(self):
        adv = _valid_hold_advisory()
        original = copy.deepcopy(adv)
        consume_advisory(adv)
        assert adv == original


# ---------------------------------------------------------------------------
# Integration: feed advisory from simulate → intake round-trip
# ---------------------------------------------------------------------------

class TestRoundTrip:
    """Feed output from build_allocation_advisory into consume_advisory."""

    def _load_sim(self):
        sim_path = (
            Path(__file__).parent
            / "ant_colony"
            / "build_allocation_memory_policy_simulation_lite.py"
        )
        spec = importlib.util.spec_from_file_location("_sim", sim_path)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _make_advisory_active(self, sim):
        from ant_colony.policy.load_scenario_registry_lite import _DEFAULT_REGISTRY  # noqa
        summary = {
            "comparison_count":              1,
            "best_candidate_scenario_id":    "s1",
            "best_candidate_recommendation": "WORTH_REVIEW",
            "best_candidate_risk_class":     "LOW_RISK",
            "rejected_count":                0,
            "insufficient_data_count":       0,
            "no_change_count":               0,
        }
        comps = [{"scenario_id": "s1", "changed_parameters_count": 1}]
        return sim.build_allocation_advisory(summary, comps)

    def _make_advisory_hold(self, sim):
        summary = {
            "comparison_count":              0,
            "best_candidate_scenario_id":    None,
            "best_candidate_recommendation": None,
            "best_candidate_risk_class":     None,
            "rejected_count":                0,
            "insufficient_data_count":       0,
            "no_change_count":               0,
        }
        return sim.build_allocation_advisory(summary, [])

    def test_active_advisory_from_sim_gives_intake_active(self):
        sim = self._load_sim()
        advisory = self._make_advisory_active(sim)
        r = consume_advisory(advisory)
        assert r["intake_status"] == "INTAKE_ACTIVE"
        assert r["would_consider_variant"] is True
        assert r["intake_simulation_only"] is True

    def test_hold_advisory_from_sim_gives_intake_hold(self):
        sim = self._load_sim()
        advisory = self._make_advisory_hold(sim)
        r = consume_advisory(advisory)
        assert r["intake_status"] == "INTAKE_HOLD"
        assert r["would_consider_variant"] is False
        assert r["intake_simulation_only"] is True

    def test_sim_advisory_contract_version_matches_expected(self):
        sim = self._load_sim()
        assert sim.QUEEN_INTAKE_CONTRACT_VERSION == EXPECTED_CONTRACT_VERSION
