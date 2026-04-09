"""
AC-72: Policy Scenario Registry + Controlled Comparison — test suite

Covers:
  A. Registry loader — clean load, scenario_ids stable, required fields present
  B. Registry loader — fail-closed: missing file, corrupt JSON, empty scenarios
  C. apply_overlay() — correct diff application, no mutation, unknown keys ignored
  D. Registry helpers — get_scenario_ids, get_baseline_scenario, get_variant_scenarios
  E. Simulator enrichment — scenario_id, scenario_type, fingerprint in comparison entries
  F. Scenario fingerprints — baseline unchanged, variants differ correctly
  G. Canonical loader remains single source — registry stores no policy values
  H. Controlled comparison — changed_parameters isolated to overlay keys only
  I. Scenario IDs stable — builtin registry IDs match expected set
  J. Baseline scenario intact — overlay={} produces unchanged policy
"""
import copy
import importlib.util
import json
import tempfile
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module imports
# ---------------------------------------------------------------------------

_ROOT       = Path(__file__).parent
_LOADER_PATH    = _ROOT / "ant_colony" / "policy" / "load_allocation_memory_policy_lite.py"
_REGISTRY_PATH  = _ROOT / "ant_colony" / "policy" / "load_scenario_registry_lite.py"
_SIM_PATH       = _ROOT / "ant_colony" / "build_allocation_memory_policy_simulation_lite.py"
_REG_JSON_PATH  = _ROOT / "ant_colony" / "policy" / "allocation_memory_scenario_registry.json"


def _load_mod(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


LOADER   = _load_mod(_LOADER_PATH, "_ac72_loader")
REGISTRY = _load_mod(_REGISTRY_PATH, "_ac72_registry")
SIM      = _load_mod(_SIM_PATH, "_ac72_sim")

_BASELINE_POLICY = copy.deepcopy(LOADER.DEFAULT_POLICY)

# Expected stable scenario IDs in the production registry
_EXPECTED_IDS = {
    "baseline",
    "stricter_positive_gate",
    "looser_positive_gate",
    "stronger_negative_dampening",
    "weaker_negative_dampening",
    "higher_memory_confidence_threshold",
    "conflict_allow_on_positive",
    "unsafe_extreme",
}


# ---------------------------------------------------------------------------
# A. Registry loader — clean load
# ---------------------------------------------------------------------------

class TestRegistryLoad:
    def test_registry_file_exists(self):
        assert _REG_JSON_PATH.exists()

    def test_load_registry_returns_tuple(self):
        scenarios, fb, reason = REGISTRY.load_registry()
        assert isinstance(scenarios, list)
        assert isinstance(fb, bool)
        assert isinstance(reason, str)

    def test_clean_load_fallback_false(self):
        _, fb, _ = REGISTRY.load_registry()
        assert fb is False

    def test_clean_load_reason_contains_loaded(self):
        _, _, reason = REGISTRY.load_registry()
        assert "LOADED" in reason

    def test_all_expected_scenario_ids_present(self):
        scenarios, _, _ = REGISTRY.load_registry()
        ids = {s["scenario_id"] for s in scenarios}
        assert _EXPECTED_IDS.issubset(ids)

    def test_required_fields_on_every_scenario(self):
        scenarios, _, _ = REGISTRY.load_registry()
        for s in scenarios:
            for key in ("scenario_id", "scenario_type", "overlay"):
                assert key in s, f"Scenario {s.get('scenario_id')} missing key {key}"

    def test_scenario_types_valid(self):
        scenarios, _, _ = REGISTRY.load_registry()
        for s in scenarios:
            assert s["scenario_type"] in ("baseline", "variant"), \
                f"Invalid type for {s['scenario_id']}: {s['scenario_type']}"

    def test_exactly_one_baseline_scenario(self):
        scenarios, _, _ = REGISTRY.load_registry()
        baselines = [s for s in scenarios if s["scenario_type"] == "baseline"]
        assert len(baselines) == 1

    def test_overlay_is_dict_per_scenario(self):
        scenarios, _, _ = REGISTRY.load_registry()
        for s in scenarios:
            assert isinstance(s["overlay"], dict), \
                f"Overlay is not dict for {s['scenario_id']}"

    def test_baseline_overlay_is_empty(self):
        scenarios, _, _ = REGISTRY.load_registry()
        baseline = next(s for s in scenarios if s["scenario_id"] == "baseline")
        assert baseline["overlay"] == {}

    def test_scenario_count_matches_expected(self):
        scenarios, _, _ = REGISTRY.load_registry()
        assert len(scenarios) == len(_EXPECTED_IDS)


# ---------------------------------------------------------------------------
# B. Registry loader — fail-closed
# ---------------------------------------------------------------------------

class TestRegistryFailClosed:
    def test_missing_file_returns_default(self, tmp_path):
        scenarios, fb, reason = REGISTRY.load_registry(tmp_path / "nonexistent.json")
        assert fb is True
        assert "FALLBACK_DEFAULT" in reason
        assert len(scenarios) >= 1
        assert scenarios[0]["scenario_id"] == "baseline"

    def test_corrupt_json_returns_default(self, tmp_path):
        p = tmp_path / "corrupt.json"
        p.write_text("{bad json: }", encoding="utf-8")
        scenarios, fb, reason = REGISTRY.load_registry(p)
        assert fb is True
        assert "FALLBACK_DEFAULT" in reason

    def test_missing_scenarios_key_returns_default(self, tmp_path):
        p = tmp_path / "no_scenarios.json"
        p.write_text(json.dumps({"schema_version": "1"}), encoding="utf-8")
        scenarios, fb, reason = REGISTRY.load_registry(p)
        assert fb is True

    def test_empty_scenarios_list_returns_default(self, tmp_path):
        p = tmp_path / "empty.json"
        p.write_text(json.dumps({"scenarios": []}), encoding="utf-8")
        scenarios, fb, reason = REGISTRY.load_registry(p)
        assert fb is True

    def test_all_invalid_scenarios_returns_default(self, tmp_path):
        p = tmp_path / "invalid.json"
        p.write_text(json.dumps({"scenarios": [{"no_id": True}]}), encoding="utf-8")
        scenarios, fb, reason = REGISTRY.load_registry(p)
        assert fb is True

    def test_fallback_always_includes_baseline(self, tmp_path):
        scenarios, _, _ = REGISTRY.load_registry(tmp_path / "ghost.json")
        ids = [s["scenario_id"] for s in scenarios]
        assert "baseline" in ids


# ---------------------------------------------------------------------------
# C. apply_overlay() — correctness, immutability, safety
# ---------------------------------------------------------------------------

class TestApplyOverlay:
    def test_empty_overlay_returns_same_values(self):
        result = REGISTRY.apply_overlay(_BASELINE_POLICY, {})
        assert result["groups"] == _BASELINE_POLICY["groups"]

    def test_overlay_value_applied(self):
        overlay = {"memory_gate": {"memory_confidence_min_positive": 0.85}}
        result = REGISTRY.apply_overlay(_BASELINE_POLICY, overlay)
        assert result["groups"]["memory_gate"]["memory_confidence_min_positive"] == 0.85

    def test_unmodified_keys_unchanged(self):
        overlay = {"memory_gate": {"memory_confidence_min_positive": 0.85}}
        result = REGISTRY.apply_overlay(_BASELINE_POLICY, overlay)
        assert result["groups"]["memory_gate"]["modifier_band_min"] == \
               _BASELINE_POLICY["groups"]["memory_gate"]["modifier_band_min"]

    def test_no_mutation_of_baseline(self):
        original_val = _BASELINE_POLICY["groups"]["memory_gate"]["memory_confidence_min_positive"]
        REGISTRY.apply_overlay(_BASELINE_POLICY, {"memory_gate": {"memory_confidence_min_positive": 0.99}})
        assert _BASELINE_POLICY["groups"]["memory_gate"]["memory_confidence_min_positive"] == original_val

    def test_unknown_group_ignored(self):
        overlay = {"nonexistent_group": {"some_key": 999}}
        result = REGISTRY.apply_overlay(_BASELINE_POLICY, overlay)
        assert "nonexistent_group" not in result.get("groups", {})

    def test_unknown_key_in_valid_group_ignored(self):
        overlay = {"memory_gate": {"unknown_future_key": 0.5}}
        result = REGISTRY.apply_overlay(_BASELINE_POLICY, overlay)
        assert "unknown_future_key" not in result["groups"]["memory_gate"]

    def test_multiple_keys_in_overlay(self):
        overlay = {"memory_gate": {"negative_blend_weight": 0.70, "negative_correction_cap": 0.07}}
        result = REGISTRY.apply_overlay(_BASELINE_POLICY, overlay)
        assert result["groups"]["memory_gate"]["negative_blend_weight"] == 0.70
        assert result["groups"]["memory_gate"]["negative_correction_cap"] == 0.07

    def test_multiple_groups_in_overlay(self):
        overlay = {
            "memory_gate": {"modifier_band_max": 1.03},
            "memory_rolling_window": {"window_size": 12},
        }
        result = REGISTRY.apply_overlay(_BASELINE_POLICY, overlay)
        assert result["groups"]["memory_gate"]["modifier_band_max"] == 1.03
        assert result["groups"]["memory_rolling_window"]["window_size"] == 12

    def test_none_overlay_treated_as_empty(self):
        result = REGISTRY.apply_overlay(_BASELINE_POLICY, None)
        assert result["groups"] == _BASELINE_POLICY["groups"]


# ---------------------------------------------------------------------------
# D. Registry helpers
# ---------------------------------------------------------------------------

class TestRegistryHelpers:
    def test_get_scenario_ids_sorted(self):
        scenarios, _, _ = REGISTRY.load_registry()
        ids = REGISTRY.get_scenario_ids(scenarios)
        assert ids == sorted(ids)

    def test_get_scenario_ids_contains_expected(self):
        scenarios, _, _ = REGISTRY.load_registry()
        ids = set(REGISTRY.get_scenario_ids(scenarios))
        assert _EXPECTED_IDS.issubset(ids)

    def test_get_baseline_scenario_returns_baseline_type(self):
        scenarios, _, _ = REGISTRY.load_registry()
        baseline = REGISTRY.get_baseline_scenario(scenarios)
        assert baseline["scenario_type"] == "baseline"
        assert baseline["scenario_id"] == "baseline"

    def test_get_baseline_scenario_fallback_when_none(self):
        result = REGISTRY.get_baseline_scenario([{"scenario_id": "v1", "scenario_type": "variant", "overlay": {}}])
        assert result["scenario_type"] == "baseline"

    def test_get_variant_scenarios_excludes_baseline(self):
        scenarios, _, _ = REGISTRY.load_registry()
        variants = REGISTRY.get_variant_scenarios(scenarios)
        for v in variants:
            assert v["scenario_type"] == "variant"
        baseline_ids = [v["scenario_id"] for v in variants if v["scenario_id"] == "baseline"]
        assert baseline_ids == []

    def test_get_variant_scenarios_count(self):
        scenarios, _, _ = REGISTRY.load_registry()
        variants = REGISTRY.get_variant_scenarios(scenarios)
        # All scenarios except baseline are variants
        assert len(variants) == len(scenarios) - 1


# ---------------------------------------------------------------------------
# E. Simulator enrichment — scenario_id, scenario_type, fingerprint
# ---------------------------------------------------------------------------

def _make_obs_rec(memory_available=True, mem_conf=0.80, mem_modifier=0.95,
                  cycle_mod=1.00, cycle_bias="NEUTRAL", cooldown=False):
    return {
        "memory_available": memory_available,
        "memory_confidence": mem_conf,
        "memory_modifier": mem_modifier,
        "memory_bias_class": "NEGATIVE",
        "cycle_modifier": cycle_mod,
        "cycle_bias_class": cycle_bias,
        "cooldown_flag": cooldown,
        "memory_influence_gate": "NEGATIVE_GATE_OPEN",
        "base_feedback_confidence": 0.60,
    }


class TestSimulatorEnrichment:
    def test_comparison_has_scenario_id(self):
        spec = {
            "scenario_id": "test_scenario",
            "policy_name": "test_scenario",
            "policy_description": "test",
            "scenario_type": "variant",
            "overlay": {"memory_gate": {"memory_confidence_min_positive": 0.85}},
        }
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, [spec])
        comp = result["policy_comparisons"][0]
        assert comp["scenario_id"] == "test_scenario"

    def test_comparison_has_scenario_type(self):
        spec = {
            "scenario_id": "s1",
            "policy_name": "s1",
            "scenario_type": "variant",
            "overlay": {},
        }
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, [spec])
        comp = result["policy_comparisons"][0]
        assert comp["scenario_type"] == "variant"

    def test_comparison_has_fingerprint(self):
        spec = {
            "scenario_id": "s1",
            "policy_name": "s1",
            "scenario_type": "variant",
            "overlay": {},
        }
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, [spec])
        comp = result["policy_comparisons"][0]
        assert "fingerprint" in comp
        fp = comp["fingerprint"]
        assert isinstance(fp, str) and len(fp) == 16

    def test_scenario_id_defaults_to_policy_name(self):
        """Backward compat: specs without scenario_id use policy_name as scenario_id."""
        spec = {
            "policy_name": "legacy_candidate",
            "policy_description": "old-style spec",
            "overlay": {},
        }
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, [spec])
        comp = result["policy_comparisons"][0]
        assert comp["scenario_id"] == "legacy_candidate"

    def test_scenario_type_defaults_to_variant(self):
        """Backward compat: specs without scenario_type default to 'variant'."""
        spec = {"policy_name": "x", "overlay": {}}
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, [spec])
        assert result["policy_comparisons"][0]["scenario_type"] == "variant"


# ---------------------------------------------------------------------------
# F. Scenario fingerprints — baseline unchanged, variants differ
# ---------------------------------------------------------------------------

class TestScenarioFingerprints:
    def test_empty_overlay_fingerprint_equals_baseline(self):
        """Applying empty overlay produces identical fingerprint to baseline."""
        applied = REGISTRY.apply_overlay(_BASELINE_POLICY, {})
        assert LOADER.policy_fingerprint(applied) == LOADER.policy_fingerprint(_BASELINE_POLICY)

    def test_variant_overlay_changes_fingerprint(self):
        overlay = {"memory_gate": {"memory_confidence_min_positive": 0.85}}
        variant = REGISTRY.apply_overlay(_BASELINE_POLICY, overlay)
        assert LOADER.policy_fingerprint(variant) != LOADER.policy_fingerprint(_BASELINE_POLICY)

    def test_different_variants_have_different_fingerprints(self):
        scenarios, _, _ = REGISTRY.load_registry()
        variants = REGISTRY.get_variant_scenarios(scenarios)
        fps = set()
        for s in variants:
            p = REGISTRY.apply_overlay(_BASELINE_POLICY, s["overlay"])
            fps.add(LOADER.policy_fingerprint(p))
        # All variants with distinct overlays should have distinct fingerprints
        # (at minimum, most should differ from baseline)
        unique_fps = len(fps)
        assert unique_fps >= len(variants) - 1  # allow at most 1 collision (conflict_allow shares values)

    def test_same_overlay_always_same_fingerprint(self):
        overlay = {"memory_gate": {"negative_blend_weight": 0.70}}
        p1 = REGISTRY.apply_overlay(_BASELINE_POLICY, overlay)
        p2 = REGISTRY.apply_overlay(_BASELINE_POLICY, overlay)
        assert LOADER.policy_fingerprint(p1) == LOADER.policy_fingerprint(p2)


# ---------------------------------------------------------------------------
# G. Canonical loader is single source — no policy values in registry
# ---------------------------------------------------------------------------

class TestCanonicalLoaderSingleSource:
    def test_registry_json_contains_no_full_policy_values(self):
        """Registry JSON should have no groups-level policy keys as top-level entries."""
        data = json.loads(_REG_JSON_PATH.read_text(encoding="utf-8"))
        policy_keys = set()
        for gv in LOADER.DEFAULT_POLICY["groups"].values():
            policy_keys.update(gv.keys())
        # Top-level keys in registry should not be policy parameter keys
        for k in data.keys():
            if k.startswith("_"):
                continue
            assert k not in policy_keys, f"Policy key '{k}' found at registry top level"

    def test_overlay_only_contains_subset_of_policy_keys(self):
        """Every key in every overlay must be a known policy key."""
        data = json.loads(_REG_JSON_PATH.read_text(encoding="utf-8"))
        policy_keys = set()
        for gv in LOADER.DEFAULT_POLICY["groups"].values():
            policy_keys.update(gv.keys())
        for scenario in data.get("scenarios", []):
            for group_vals in scenario.get("overlay", {}).values():
                for k in group_vals:
                    assert k in policy_keys, \
                        f"Overlay key '{k}' in scenario '{scenario['scenario_id']}' is not a known policy key"

    def test_simulator_re_exports_canonical_default_policy(self):
        """sim.DEFAULT_POLICY must equal canonical loader's DEFAULT_POLICY."""
        assert SIM.DEFAULT_POLICY["groups"] == LOADER.DEFAULT_POLICY["groups"]

    def test_registry_loader_does_not_embed_policy_values(self):
        """load_scenario_registry_lite has no _DEFAULT_POLICY group values."""
        # The registry's _DEFAULT_REGISTRY only has overlay={}; no actual numeric values
        for s in REGISTRY._DEFAULT_REGISTRY:
            assert s["overlay"] == {}, "Default registry entry must have empty overlay"


# ---------------------------------------------------------------------------
# H. Controlled comparison — changed_parameters isolated to overlay keys
# ---------------------------------------------------------------------------

class TestControlledComparison:
    def test_changed_parameters_contains_only_overlay_keys(self):
        overlay = {"memory_gate": {"memory_confidence_min_positive": 0.85}}
        spec = {
            "scenario_id": "test",
            "policy_name": "test",
            "overlay": overlay,
        }
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, [spec])
        comp = result["policy_comparisons"][0]
        changed = comp["changed_parameters"]
        assert set(changed.keys()) == {"memory_confidence_min_positive"}

    def test_baseline_scenario_zero_changed_parameters(self):
        spec = {
            "scenario_id": "baseline_check",
            "policy_name": "baseline_check",
            "scenario_type": "baseline",
            "overlay": {},
        }
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, [spec])
        comp = result["policy_comparisons"][0]
        assert comp["changed_parameters_count"] == 0

    def test_changed_parameters_show_before_after(self):
        overlay = {"memory_gate": {"modifier_band_min": 0.88}}
        spec = {"policy_name": "modified", "overlay": overlay}
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, [spec])
        changed = result["policy_comparisons"][0]["changed_parameters"]
        assert "modifier_band_min" in changed
        entry = changed["modifier_band_min"]
        assert "baseline" in entry and "candidate" in entry
        assert entry["baseline"] == 0.90
        assert entry["candidate"] == 0.88

    def test_delta_vs_baseline_present(self):
        spec = {
            "policy_name": "test",
            "overlay": {"memory_gate": {"memory_confidence_min_positive": 0.85}},
        }
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, [spec])
        comp = result["policy_comparisons"][0]
        assert "delta_vs_baseline" in comp
        assert isinstance(comp["delta_vs_baseline"], dict)


# ---------------------------------------------------------------------------
# I. Scenario IDs stable — builtin registry IDs match expected set exactly
# ---------------------------------------------------------------------------

class TestScenarioIDStability:
    def test_expected_ids_all_present(self):
        scenarios, _, _ = REGISTRY.load_registry()
        ids = {s["scenario_id"] for s in scenarios}
        assert ids == _EXPECTED_IDS

    def test_scenario_ids_are_strings(self):
        scenarios, _, _ = REGISTRY.load_registry()
        for s in scenarios:
            assert isinstance(s["scenario_id"], str)
            assert s["scenario_id"].strip()

    def test_scenario_ids_unique(self):
        scenarios, _, _ = REGISTRY.load_registry()
        ids = [s["scenario_id"] for s in scenarios]
        assert len(ids) == len(set(ids)), "Duplicate scenario_ids found"

    def test_registry_exports_version(self):
        assert hasattr(REGISTRY, "VERSION")
        assert "scenario_registry" in REGISTRY.VERSION


# ---------------------------------------------------------------------------
# J. Baseline scenario — overlay={} produces unchanged policy
# ---------------------------------------------------------------------------

class TestBaselineScenario:
    def test_baseline_produces_identical_policy(self):
        baseline_scenario = REGISTRY.get_baseline_scenario(REGISTRY.load_registry()[0])
        result = REGISTRY.apply_overlay(_BASELINE_POLICY, baseline_scenario["overlay"])
        assert result["groups"] == _BASELINE_POLICY["groups"]

    def test_baseline_fingerprint_matches_canonical(self):
        baseline_scenario = REGISTRY.get_baseline_scenario(REGISTRY.load_registry()[0])
        applied = REGISTRY.apply_overlay(_BASELINE_POLICY, baseline_scenario["overlay"])
        assert LOADER.policy_fingerprint(applied) == LOADER.policy_fingerprint(_BASELINE_POLICY)

    def test_simulation_with_all_registry_scenarios(self):
        """Smoke test: all registry variant scenarios simulate without error."""
        scenarios, _, _ = REGISTRY.load_registry()
        variants = REGISTRY.get_variant_scenarios(scenarios)
        candidate_specs = [
            {**s, "policy_name": s["scenario_id"]}
            for s in variants
        ]
        result = SIM.build_simulation([_make_obs_rec()], _BASELINE_POLICY, candidate_specs)
        assert result["summary"]["simulations_completed"] == len(variants)
        assert result["summary"]["simulations_failed"] == 0
