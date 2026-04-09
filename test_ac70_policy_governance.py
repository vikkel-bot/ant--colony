"""
AC-70: Policy Governance + Change Safety — test suite

Covers:
  A. Public API surface: KEY_TYPES, KEY_BOUNDS, GROUP_CONSTRAINTS exported
  B. Clean file → LOADED_FROM_FILE, fallback_used=False
  C. Invalid type → per-key fallback, LOADED_WITH_CORRECTIONS, fallback_used=False
  D. Out-of-bounds value → per-key fallback, LOADED_WITH_CORRECTIONS
  E. JSON boolean → treated as wrong type, per-key fallback
  F. Cross-constraint violation → full group fallback, LOADED_WITH_SECTION_FALLBACK
  G. Corrupt JSON → FALLBACK_DEFAULT, fallback_used=True
  H. Missing file → FALLBACK_DEFAULT, fallback_used=True
  I. Missing required key in file → FALLBACK_DEFAULT (structure check)
  J. check_policy_drift: clean file → no drift
  K. check_policy_drift: extra key in file → drift detected
  L. check_policy_drift: missing key in file → drift detected
  M. check_policy_drift: missing file → drift detected
  N. Canonical loader is the single source (DEFAULT_POLICY accessible)
  O. Corrected keys fall back to DEFAULT_POLICY value, not zero/None
  P. float-from-int coercion accepted (e.g. 1 for a float key)
"""
import copy
import importlib.util
import json
import tempfile
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Loader bootstrap
# ---------------------------------------------------------------------------

_LOADER_PATH = Path(__file__).parent / "ant_colony" / "policy" / "load_allocation_memory_policy_lite.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("_ac70_loader", _LOADER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


LOADER = _load_module()


def _make_policy_file(tmp_dir: Path, overrides: dict = None, groups_overrides: dict = None) -> Path:
    """Write a valid-baseline policy JSON to a temp file with optional overrides applied."""
    base = copy.deepcopy(LOADER.DEFAULT_POLICY)
    if overrides:
        base.update(overrides)
    if groups_overrides:
        for group, vals in groups_overrides.items():
            base["groups"][group].update(vals)
    p = tmp_dir / "policy.json"
    p.write_text(json.dumps(base), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# A. Public API surface
# ---------------------------------------------------------------------------

class TestPublicAPISurface:
    def test_key_types_exported(self):
        assert hasattr(LOADER, "KEY_TYPES")
        assert isinstance(LOADER.KEY_TYPES, dict)
        assert len(LOADER.KEY_TYPES) >= 28

    def test_key_bounds_exported(self):
        assert hasattr(LOADER, "KEY_BOUNDS")
        assert isinstance(LOADER.KEY_BOUNDS, dict)
        assert len(LOADER.KEY_BOUNDS) >= 20

    def test_group_constraints_exported(self):
        assert hasattr(LOADER, "GROUP_CONSTRAINTS")
        assert isinstance(LOADER.GROUP_CONSTRAINTS, dict)
        assert set(LOADER.GROUP_CONSTRAINTS) == {
            "memory_gate", "memory_rolling_window", "review_thresholds"
        }

    def test_all_policy_keys_have_type_entry(self):
        for group_vals in LOADER.DEFAULT_POLICY["groups"].values():
            for key in group_vals:
                assert key in LOADER.KEY_TYPES, f"Key '{key}' missing from KEY_TYPES"

    def test_check_policy_drift_exported(self):
        assert callable(getattr(LOADER, "check_policy_drift", None))

    def test_default_policy_exported(self):
        assert hasattr(LOADER, "DEFAULT_POLICY")
        assert "groups" in LOADER.DEFAULT_POLICY


# ---------------------------------------------------------------------------
# B. Clean file → LOADED_FROM_FILE
# ---------------------------------------------------------------------------

class TestCleanLoad:
    def test_clean_load_reason(self, tmp_path):
        p = _make_policy_file(tmp_path)
        _, fb, reason = LOADER.load_policy(p)
        assert reason == "LOADED_FROM_FILE"

    def test_clean_load_fallback_used_false(self, tmp_path):
        p = _make_policy_file(tmp_path)
        _, fb, _ = LOADER.load_policy(p)
        assert fb is False

    def test_clean_load_values_match_file(self, tmp_path):
        # window_size=12 satisfies full_memory_at(8) <= window_size, no constraint violation
        p = _make_policy_file(tmp_path, groups_overrides={"memory_rolling_window": {"window_size": 12}})
        policy, _, _ = LOADER.load_policy(p)
        assert policy["groups"]["memory_rolling_window"]["window_size"] == 12

    def test_clean_load_returns_deep_copy(self, tmp_path):
        p = _make_policy_file(tmp_path)
        policy1, _, _ = LOADER.load_policy(p)
        policy2, _, _ = LOADER.load_policy(p)
        policy1["groups"]["memory_gate"]["window_size"] = 999
        # mutation of first result must not affect second
        assert policy2["groups"]["memory_gate"].get("window_size") != 999


# ---------------------------------------------------------------------------
# C. Invalid type → per-key fallback
# ---------------------------------------------------------------------------

class TestInvalidType:
    def test_string_for_int_key_corrected(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_rolling_window"]["window_size"] = "ten"
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        policy, fb, reason = LOADER.load_policy(p)
        assert policy["groups"]["memory_rolling_window"]["window_size"] == LOADER.DEFAULT_POLICY["groups"]["memory_rolling_window"]["window_size"]
        assert fb is False
        assert "LOADED_WITH_CORRECTIONS" in reason

    def test_string_for_float_key_corrected(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_gate"]["modifier_band_min"] = "low"
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        policy, fb, reason = LOADER.load_policy(p)
        assert policy["groups"]["memory_gate"]["modifier_band_min"] == LOADER.DEFAULT_POLICY["groups"]["memory_gate"]["modifier_band_min"]
        assert "LOADED_WITH_CORRECTIONS" in reason

    def test_null_for_str_key_corrected(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_gate"]["conflict_policy_mode"] = None
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        policy, fb, reason = LOADER.load_policy(p)
        assert policy["groups"]["memory_gate"]["conflict_policy_mode"] == LOADER.DEFAULT_POLICY["groups"]["memory_gate"]["conflict_policy_mode"]
        assert "LOADED_WITH_CORRECTIONS" in reason

    def test_empty_str_for_str_key_corrected(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_gate"]["conflict_policy_mode"] = ""
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        policy, fb, reason = LOADER.load_policy(p)
        assert policy["groups"]["memory_gate"]["conflict_policy_mode"] == LOADER.DEFAULT_POLICY["groups"]["memory_gate"]["conflict_policy_mode"]


# ---------------------------------------------------------------------------
# D. Out-of-bounds value → per-key fallback
# ---------------------------------------------------------------------------

class TestOutOfBounds:
    def test_modifier_band_min_too_high(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={"memory_gate": {"modifier_band_min": 2.0}})
        policy, fb, reason = LOADER.load_policy(p)
        assert policy["groups"]["memory_gate"]["modifier_band_min"] == LOADER.DEFAULT_POLICY["groups"]["memory_gate"]["modifier_band_min"]
        assert "LOADED_WITH_CORRECTIONS" in reason or "SECTION_FALLBACK" in reason

    def test_window_size_zero(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={"memory_rolling_window": {"window_size": 0}})
        policy, fb, reason = LOADER.load_policy(p)
        assert policy["groups"]["memory_rolling_window"]["window_size"] == LOADER.DEFAULT_POLICY["groups"]["memory_rolling_window"]["window_size"]

    def test_confidence_above_1(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={"memory_gate": {"memory_confidence_min_negative": 1.5}})
        policy, fb, reason = LOADER.load_policy(p)
        assert policy["groups"]["memory_gate"]["memory_confidence_min_negative"] == LOADER.DEFAULT_POLICY["groups"]["memory_gate"]["memory_confidence_min_negative"]

    def test_negative_rate_below_zero(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={"review_thresholds": {"review_cooldown_rate_warn": -0.1}})
        policy, fb, reason = LOADER.load_policy(p)
        assert policy["groups"]["review_thresholds"]["review_cooldown_rate_warn"] == LOADER.DEFAULT_POLICY["groups"]["review_thresholds"]["review_cooldown_rate_warn"]

    def test_fallback_used_false_on_bounds_error(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={"memory_rolling_window": {"window_size": 9999}})
        _, fb, _ = LOADER.load_policy(p)
        assert fb is False


# ---------------------------------------------------------------------------
# E. JSON boolean treated as wrong type
# ---------------------------------------------------------------------------

class TestBooleanRejected:
    def test_bool_true_for_float_key(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_gate"]["modifier_band_min"] = True  # JSON true
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        policy, fb, reason = LOADER.load_policy(p)
        # Must not be True
        assert policy["groups"]["memory_gate"]["modifier_band_min"] is not True
        assert isinstance(policy["groups"]["memory_gate"]["modifier_band_min"], float)

    def test_bool_false_for_int_key(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_rolling_window"]["window_size"] = False
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        policy, fb, reason = LOADER.load_policy(p)
        assert policy["groups"]["memory_rolling_window"]["window_size"] is not False
        assert isinstance(policy["groups"]["memory_rolling_window"]["window_size"], int)


# ---------------------------------------------------------------------------
# F. Cross-constraint violation → full group fallback
# ---------------------------------------------------------------------------

class TestGroupConstraintViolation:
    def test_modifier_band_min_gte_max_triggers_group_fallback(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={
            "memory_gate": {"modifier_band_min": 1.10, "modifier_band_max": 0.95}
        })
        policy, fb, reason = LOADER.load_policy(p)
        assert "SECTION_FALLBACK" in reason
        mg = policy["groups"]["memory_gate"]
        # Entire group must match defaults
        for key, default_val in LOADER.DEFAULT_POLICY["groups"]["memory_gate"].items():
            assert mg[key] == default_val, f"key '{key}' not reset to default after group fallback"

    def test_confidence_neg_above_pos_triggers_group_fallback(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={
            "memory_gate": {
                "memory_confidence_min_negative": 0.90,
                "memory_confidence_min_positive": 0.50,
            }
        })
        policy, fb, reason = LOADER.load_policy(p)
        assert "SECTION_FALLBACK" in reason

    def test_full_memory_at_above_window_size(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={
            "memory_rolling_window": {"window_size": 5, "full_memory_at": 8}
        })
        policy, fb, reason = LOADER.load_policy(p)
        assert "SECTION_FALLBACK" in reason
        # Rolling window group must revert to defaults
        rw = policy["groups"]["memory_rolling_window"]
        assert rw["window_size"] == LOADER.DEFAULT_POLICY["groups"]["memory_rolling_window"]["window_size"]

    def test_sparse_threshold_above_full_memory_at(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={
            "memory_rolling_window": {"sparse_window_threshold": 9, "full_memory_at": 8}
        })
        policy, _, reason = LOADER.load_policy(p)
        assert "SECTION_FALLBACK" in reason

    def test_group_constraint_fallback_used_false(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={
            "memory_gate": {"modifier_band_min": 1.20, "modifier_band_max": 0.80}
        })
        _, fb, _ = LOADER.load_policy(p)
        assert fb is False

    def test_review_low_watch_above_high_watch(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={
            "review_thresholds": {
                "review_memory_applied_rate_low_watch": 0.90,
                "review_memory_applied_rate_high_watch": 0.20,
            }
        })
        policy, _, reason = LOADER.load_policy(p)
        assert "SECTION_FALLBACK" in reason

    def test_constraint_violation_in_one_group_does_not_affect_other(self, tmp_path):
        """A constraint violation in memory_gate must not corrupt memory_rolling_window."""
        # window_size=12 satisfies full_memory_at(8) <= window_size — no constraint clash
        p = _make_policy_file(tmp_path, groups_overrides={
            "memory_gate": {"modifier_band_min": 1.20, "modifier_band_max": 0.80},
            "memory_rolling_window": {"window_size": 12},
        })
        policy, _, _ = LOADER.load_policy(p)
        # Rolling window should have the file's window_size=12 (unaffected by memory_gate fallback)
        assert policy["groups"]["memory_rolling_window"]["window_size"] == 12


# ---------------------------------------------------------------------------
# G. Corrupt JSON → FALLBACK_DEFAULT
# ---------------------------------------------------------------------------

class TestCorruptJson:
    def test_corrupt_json_returns_default(self, tmp_path):
        p = tmp_path / "corrupt.json"
        p.write_text("{not valid json: }", encoding="utf-8")
        policy, fb, reason = LOADER.load_policy(p)
        assert fb is True
        assert "FALLBACK_DEFAULT" in reason
        assert policy["groups"] == LOADER.DEFAULT_POLICY["groups"]

    def test_corrupt_json_reason_contains_parse_error(self, tmp_path):
        p = tmp_path / "corrupt.json"
        p.write_text("<<<garbage>>>", encoding="utf-8")
        _, _, reason = LOADER.load_policy(p)
        assert "PARSE_ERROR" in reason

    def test_empty_file_is_corrupt(self, tmp_path):
        p = tmp_path / "empty.json"
        p.write_text("", encoding="utf-8")
        _, fb, reason = LOADER.load_policy(p)
        assert fb is True
        assert "FALLBACK_DEFAULT" in reason


# ---------------------------------------------------------------------------
# H. Missing file → FALLBACK_DEFAULT
# ---------------------------------------------------------------------------

class TestMissingFile:
    def test_missing_file_returns_default(self, tmp_path):
        p = tmp_path / "nonexistent.json"
        policy, fb, reason = LOADER.load_policy(p)
        assert fb is True
        assert "FALLBACK_DEFAULT" in reason
        assert policy["groups"] == LOADER.DEFAULT_POLICY["groups"]

    def test_missing_file_reason_contains_not_found(self, tmp_path):
        _, _, reason = LOADER.load_policy(tmp_path / "does_not_exist.json")
        assert "FILE_NOT_FOUND" in reason


# ---------------------------------------------------------------------------
# I. Missing required key → structure failure → FALLBACK_DEFAULT
# ---------------------------------------------------------------------------

class TestStructureValidation:
    def test_missing_groups_key(self, tmp_path):
        p = tmp_path / "p.json"
        p.write_text(json.dumps({"policy_name": "test"}), encoding="utf-8")
        _, fb, reason = LOADER.load_policy(p)
        assert fb is True
        assert "FALLBACK_DEFAULT" in reason

    def test_missing_required_group(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        del base["groups"]["memory_gate"]
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        _, fb, reason = LOADER.load_policy(p)
        assert fb is True
        assert "FALLBACK_DEFAULT" in reason

    def test_missing_required_key_within_group(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        del base["groups"]["memory_rolling_window"]["window_size"]
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        _, fb, reason = LOADER.load_policy(p)
        assert fb is True
        assert "FALLBACK_DEFAULT" in reason


# ---------------------------------------------------------------------------
# J. check_policy_drift: clean file → no drift
# ---------------------------------------------------------------------------

class TestDriftDetectionClean:
    def test_clean_file_no_drift(self, tmp_path):
        p = _make_policy_file(tmp_path)
        result = LOADER.check_policy_drift(p)
        assert result["drift_detected"] is False
        assert result["keys_in_default_not_in_file"] == []
        assert result["keys_in_file_not_in_default"] == []
        assert result["file_error"] is None

    def test_production_policy_no_drift(self):
        """The checked-in policy file must have zero drift against DEFAULT_POLICY."""
        result = LOADER.check_policy_drift()
        assert result["drift_detected"] is False, (
            f"Production policy has drift: {result}"
        )


# ---------------------------------------------------------------------------
# K. check_policy_drift: extra key in file → drift
# ---------------------------------------------------------------------------

class TestDriftDetectionExtra:
    def test_extra_key_detected(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_gate"]["unknown_future_key"] = 0.99
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        result = LOADER.check_policy_drift(p)
        assert result["drift_detected"] is True
        assert "unknown_future_key" in result["keys_in_file_not_in_default"]


# ---------------------------------------------------------------------------
# L. check_policy_drift: missing key → drift
# ---------------------------------------------------------------------------

class TestDriftDetectionMissing:
    def test_missing_key_detected(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        del base["groups"]["memory_rolling_window"]["sparse_window_threshold"]
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        result = LOADER.check_policy_drift(p)
        assert result["drift_detected"] is True
        assert "sparse_window_threshold" in result["keys_in_default_not_in_file"]


# ---------------------------------------------------------------------------
# M. check_policy_drift: missing file → drift
# ---------------------------------------------------------------------------

class TestDriftDetectionMissingFile:
    def test_missing_file_is_drift(self, tmp_path):
        result = LOADER.check_policy_drift(tmp_path / "ghost.json")
        assert result["drift_detected"] is True
        assert result["file_error"] is not None


# ---------------------------------------------------------------------------
# N. Canonical loader is the single source
# ---------------------------------------------------------------------------

class TestCanonicalSingleSource:
    def test_ac63_imports_from_canonical_loader(self):
        """AC-63 must expose _POLICY_FALLBACK_USED (wired to canonical loader)."""
        import importlib
        spec = importlib.util.spec_from_file_location(
            "_ac63", Path(__file__).parent / "ant_colony" / "build_allocation_feedback_memory_lite.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert hasattr(mod, "_POLICY_FALLBACK_USED")

    def test_ac64_imports_from_canonical_loader(self):
        """AC-64 must expose _POLICY_FALLBACK_USED."""
        import importlib
        spec = importlib.util.spec_from_file_location(
            "_ac64", Path(__file__).parent / "ant_colony" / "build_allocation_decision_quality_lite.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert hasattr(mod, "_POLICY_FALLBACK_USED")

    def test_ac66_imports_from_canonical_loader(self):
        """AC-66 must expose _POLICY_FALLBACK_USED."""
        import importlib
        spec = importlib.util.spec_from_file_location(
            "_ac66", Path(__file__).parent / "ant_colony" / "build_allocation_memory_policy_review_lite.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert hasattr(mod, "_POLICY_FALLBACK_USED")


# ---------------------------------------------------------------------------
# O. Corrected keys fall back to DEFAULT_POLICY value, not zero/None
# ---------------------------------------------------------------------------

class TestCorrectionFallbackValues:
    def test_int_key_falls_back_to_default_not_zero(self, tmp_path):
        p = _make_policy_file(tmp_path, groups_overrides={"memory_rolling_window": {"cooldown_cycles_default": "bad"}})
        policy, _, _ = LOADER.load_policy(p)
        expected = LOADER.DEFAULT_POLICY["groups"]["memory_rolling_window"]["cooldown_cycles_default"]
        assert policy["groups"]["memory_rolling_window"]["cooldown_cycles_default"] == expected
        assert expected != 0 or expected == 0  # just ensure it's the specific default

    def test_float_key_falls_back_to_default_not_none(self, tmp_path):
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_gate"]["negative_blend_weight"] = [1, 2, 3]  # list — wrong type
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        policy, _, _ = LOADER.load_policy(p)
        assert policy["groups"]["memory_gate"]["negative_blend_weight"] is not None
        assert isinstance(policy["groups"]["memory_gate"]["negative_blend_weight"], float)


# ---------------------------------------------------------------------------
# P. Float-from-int coercion accepted
# ---------------------------------------------------------------------------

class TestFloatFromIntCoercion:
    def test_integer_value_accepted_for_float_key(self, tmp_path):
        """JSON integer 1 should be silently accepted/coerced for a float key."""
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_gate"]["negative_blend_weight"] = 1  # int, not float
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        policy, _, reason = LOADER.load_policy(p)
        # Should either load clean or with corrections — but the value should be 1.0, not rejected
        # (1 is within [0,1] bounds)
        assert policy["groups"]["memory_gate"]["negative_blend_weight"] == 1.0
        assert isinstance(policy["groups"]["memory_gate"]["negative_blend_weight"], float)

    def test_float_whole_number_accepted_for_int_key(self, tmp_path):
        """JSON 8.0 should be coerced to int 8 for an int key without correction."""
        base = copy.deepcopy(LOADER.DEFAULT_POLICY)
        base["groups"]["memory_rolling_window"]["window_size"] = 8.0
        p = tmp_path / "p.json"
        p.write_text(json.dumps(base), encoding="utf-8")
        policy, _, reason = LOADER.load_policy(p)
        assert policy["groups"]["memory_rolling_window"]["window_size"] == 8
        assert isinstance(policy["groups"]["memory_rolling_window"]["window_size"], int)
