"""
AC-67 tests: Memory Policy Simulation / What-if Engine
Imports build_simulation and core functions from
build_allocation_memory_policy_simulation_lite.py (AC-67).

Test scenarios:
  A. Baseline parity — default policy produces consistent baseline metrics
  B. Stricter positive gate — positive_applied_rate falls
  C. Looser positive gate — positive_applied_rate rises (risk grows with extreme)
  D. Stronger negative dampening — negative/caution impact increases
  E. Shorter / higher confidence threshold — fewer memory-applied cases
  F. Higher memory confidence threshold — more neutral fallback
  G. Unsafe candidate — TOO_RISKY / HIGH_RISK
  H. Missing policy file — safe default fallback
  I. Summary consistency — counts, deltas, recommendations consistent
"""
import importlib.util
import json
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Load production module
# ---------------------------------------------------------------------------

_MODULE_PATH = (
    Path(__file__).parent
    / "ant_colony"
    / "build_allocation_memory_policy_simulation_lite.py"
)
spec = importlib.util.spec_from_file_location("sim_mod", _MODULE_PATH)
sim = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sim)


# ---------------------------------------------------------------------------
# Helpers — build synthetic AC-65 observability records
# ---------------------------------------------------------------------------

def make_sim_rec(
    strategy_key="BTC-EUR__EDGE4",
    impact_class="NO_EFFECT",
    memory_available=True,
    memory_modifier_applied=False,
    memory_confidence=0.80,
    memory_modifier=1.00,
    memory_bias_class="NEUTRAL",
    cycle_modifier=1.00,
    cycle_bias_class="NEUTRAL",
    cooldown_flag=False,
    memory_influence_gate="MEMORY_NEUTRAL",
    base_feedback_confidence=0.70,
    modifier_delta=0.0,
    confidence_delta=0.0,
    dq_score_delta=0.0,
    safe_band_ok=True,
    dq_gate_changed=False,
):
    return {
        "strategy_key":            strategy_key,
        "impact_class":            impact_class,
        "memory_available":        memory_available,
        "memory_modifier_applied": memory_modifier_applied,
        "memory_confidence":       memory_confidence,
        "memory_modifier":         memory_modifier,
        "memory_bias_class":       memory_bias_class,
        "cycle_modifier":          cycle_modifier,
        "cycle_bias_class":        cycle_bias_class,
        "cooldown_flag":           cooldown_flag,
        "memory_influence_gate":   memory_influence_gate,
        "base_feedback_confidence": base_feedback_confidence,
        "modifier_delta":          modifier_delta,
        "confidence_delta":        confidence_delta,
        "dq_score_delta":          dq_score_delta,
        "safe_band_ok":            safe_band_ok,
        "dq_gate_changed":         dq_gate_changed,
    }


BASELINE_POLICY = sim.DEFAULT_POLICY


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

_PASS = []
_FAIL = []


def check(label, condition, detail=""):
    if condition:
        _PASS.append(label)
    else:
        _FAIL.append(f"FAIL: {label}" + (f" — {detail}" if detail else ""))


# ===========================================================================
# A. Baseline parity
# ===========================================================================

def test_a_baseline_parity():
    """
    Under the default policy, simulate_record should produce results
    consistent with the same logic as AC-64 apply_memory_gate.
    """
    flat = sim.flatten_policy(BASELINE_POLICY)

    # A1: no memory → NO_MEMORY
    rec = make_sim_rec(memory_available=False)
    r = sim.simulate_record(rec, flat)
    check("A: no memory → NO_MEMORY impact", r["sim_impact_class"] == "NO_MEMORY")
    check("A: no memory → applied=False", r["sim_memory_modifier_applied"] is False)
    check("A: no memory → delta=0.0", r["sim_modifier_delta"] == 0.0)

    # A2: low memory confidence → BLOCKED_LOW_CONFIDENCE
    rec = make_sim_rec(memory_available=True, memory_confidence=0.30, memory_modifier=0.95,
                       memory_bias_class="NEGATIVE", memory_influence_gate="MEMORY_CONF_TOO_LOW")
    r = sim.simulate_record(rec, flat)
    check("A: low conf → BLOCKED_LOW_CONFIDENCE", r["sim_impact_class"] == "BLOCKED_LOW_CONFIDENCE")
    check("A: low conf → applied=False", r["sim_memory_modifier_applied"] is False)

    # A3: negative memory → NEGATIVE_DAMPENING, correction applied
    rec = make_sim_rec(
        memory_available=True, memory_confidence=0.80,
        memory_modifier=0.95, memory_bias_class="NEGATIVE",
        cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
        memory_influence_gate="NEGATIVE_GATE_OPEN",
        base_feedback_confidence=0.70,
    )
    r = sim.simulate_record(rec, flat)
    # expected: correction = (0.95 - 1.00) * 0.50 = -0.025, capped -0.05 → -0.025
    # final = clamp(1.00 + (-0.025), 0.90, 1.05) = 0.9750
    check("A: negative → NEGATIVE_DAMPENING", r["sim_impact_class"] == "NEGATIVE_DAMPENING")
    check("A: negative → applied=True", r["sim_memory_modifier_applied"] is True)
    check("A: negative → delta < 0", r["sim_modifier_delta"] < 0.0, f"got {r['sim_modifier_delta']}")
    check("A: negative → delta = -0.025", r["sim_modifier_delta"] == -0.025, f"got {r['sim_modifier_delta']}")

    # A4: caution (cooldown) → CAUTION_DAMPENING
    rec = make_sim_rec(
        memory_available=True, memory_confidence=0.70,
        memory_modifier=0.90, memory_bias_class="NEGATIVE_CAUTION",
        cycle_modifier=1.00, cooldown_flag=True,
        memory_influence_gate="COOLDOWN_GATE_OPEN",
        base_feedback_confidence=0.70,
    )
    r = sim.simulate_record(rec, flat)
    check("A: cooldown → CAUTION_DAMPENING", r["sim_impact_class"] == "CAUTION_DAMPENING")
    check("A: cooldown → applied=True", r["sim_memory_modifier_applied"] is True)

    # A5: positive memory, sufficient confidence → POSITIVE_REINFORCEMENT
    rec = make_sim_rec(
        memory_available=True, memory_confidence=0.85,
        memory_modifier=1.05, memory_bias_class="POSITIVE",
        cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
        cooldown_flag=False, memory_influence_gate="POSITIVE_GATE_OPEN",
        base_feedback_confidence=0.70,
    )
    r = sim.simulate_record(rec, flat)
    # expected: correction = (1.05 - 1.00) * 0.30 = 0.015, cap 0.03 → 0.015
    check("A: positive → POSITIVE_REINFORCEMENT", r["sim_impact_class"] == "POSITIVE_REINFORCEMENT")
    check("A: positive → applied=True", r["sim_memory_modifier_applied"] is True)
    check("A: positive → delta > 0", r["sim_modifier_delta"] > 0.0, f"got {r['sim_modifier_delta']}")
    check("A: positive → delta = 0.015", r["sim_modifier_delta"] == 0.015, f"got {r['sim_modifier_delta']}")

    # A6: positive memory, conflict → BLOCKED_BY_CONFLICT
    rec = make_sim_rec(
        memory_available=True, memory_confidence=0.85,
        memory_modifier=1.05, memory_bias_class="POSITIVE",
        cycle_modifier=1.00, cycle_bias_class="NEGATIVE",
        cooldown_flag=False, memory_influence_gate="CONFLICT_BLOCKED",
    )
    r = sim.simulate_record(rec, flat)
    check("A: conflict → BLOCKED_BY_CONFLICT", r["sim_impact_class"] == "BLOCKED_BY_CONFLICT")
    check("A: conflict → applied=False", r["sim_memory_modifier_applied"] is False)

    # A7: positive memory, insufficient positive confidence → BLOCKED_LOW_CONFIDENCE
    rec = make_sim_rec(
        memory_available=True, memory_confidence=0.60,  # above neg gate (0.50), below pos gate (0.75)
        memory_modifier=1.05, memory_bias_class="POSITIVE",
        cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
        cooldown_flag=False, memory_influence_gate="POSITIVE_CONF_TOO_LOW",
    )
    r = sim.simulate_record(rec, flat)
    check("A: pos conf too low → BLOCKED_LOW_CONFIDENCE", r["sim_impact_class"] == "BLOCKED_LOW_CONFIDENCE")

    # A8: neutral memory → NO_EFFECT
    rec = make_sim_rec(
        memory_available=True, memory_confidence=0.80,
        memory_modifier=1.00, memory_bias_class="NEUTRAL",
        memory_influence_gate="MEMORY_NEUTRAL",
    )
    r = sim.simulate_record(rec, flat)
    check("A: neutral → NO_EFFECT", r["sim_impact_class"] == "NO_EFFECT")


# ===========================================================================
# B. Stricter positive gate — confidence 0.75→0.85
# ===========================================================================

def test_b_stricter_positive_gate():
    # 5 records: 2 with positive memory at confidence 0.78 (between 0.75 and 0.85),
    # 3 neutral
    recs = (
        [make_sim_rec(memory_available=True, memory_confidence=0.78,
                      memory_modifier=1.05, memory_bias_class="POSITIVE",
                      cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
                      memory_influence_gate="POSITIVE_GATE_OPEN",
                      base_feedback_confidence=0.70)] * 2 +
        [make_sim_rec(memory_available=True, memory_confidence=0.80,
                      memory_modifier=1.00, memory_bias_class="NEUTRAL",
                      memory_influence_gate="MEMORY_NEUTRAL")] * 3
    )

    # Under baseline: 2 positive reinforcement records
    baseline_sim = sim.build_simulation(recs, BASELINE_POLICY, [])
    base_metrics = baseline_sim["baseline_metrics"]
    check("B: baseline positive rate = 0.40", base_metrics["simulated_positive_applied_rate"] == 0.40,
          f"got {base_metrics['simulated_positive_applied_rate']}")

    # Under stricter positive gate: those 2 records (conf=0.78) now blocked
    cand = [{
        "policy_name": "stricter_positive_gate",
        "policy_description": "Test: raise pos gate 0.75→0.85",
        "overlay": {"memory_gate": {"memory_confidence_min_positive": 0.85}},
    }]
    result = sim.build_simulation(recs, BASELINE_POLICY, cand)
    comp = result["policy_comparisons"][0]

    check("B: stricter → positive_rate falls to 0.0",
          comp["simulated_positive_applied_rate"] == 0.0,
          f"got {comp['simulated_positive_applied_rate']}")
    check("B: stricter → delta_positive negative",
          comp["delta_vs_baseline"]["positive_applied_rate"] < 0.0)
    check("B: stricter → LOW_RISK or MEDIUM_RISK",
          comp["policy_risk_class"] in ("LOW_RISK", "MEDIUM_RISK"),
          f"got {comp['policy_risk_class']}")
    check("B: stricter → policy_safe=True", comp["policy_safe"] is True)
    check("B: stricter → changed_parameters has positive conf gate",
          "memory_confidence_min_positive" in comp["changed_parameters"])


# ===========================================================================
# C. Looser positive gate — confidence 0.75→0.60
# ===========================================================================

def test_c_looser_positive_gate():
    # Records with confidence 0.62–0.72 (between 0.60 and 0.75) that are currently blocked
    recs = (
        [make_sim_rec(memory_available=True, memory_confidence=0.65,
                      memory_modifier=1.05, memory_bias_class="POSITIVE",
                      cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
                      memory_influence_gate="POSITIVE_CONF_TOO_LOW",
                      base_feedback_confidence=0.70)] * 4 +
        [make_sim_rec(memory_available=True, memory_confidence=0.80,
                      memory_modifier=1.00, memory_bias_class="NEUTRAL",
                      memory_influence_gate="MEMORY_NEUTRAL")] * 6
    )

    cand = [{
        "policy_name": "looser_positive_gate",
        "policy_description": "Test: lower pos gate 0.75→0.60",
        "overlay": {"memory_gate": {"memory_confidence_min_positive": 0.60}},
    }]
    result = sim.build_simulation(recs, BASELINE_POLICY, cand)
    comp = result["policy_comparisons"][0]

    check("C: looser → positive_rate increases",
          comp["simulated_positive_applied_rate"] > result["baseline_metrics"]["simulated_positive_applied_rate"],
          f"sim={comp['simulated_positive_applied_rate']} base={result['baseline_metrics']['simulated_positive_applied_rate']}")
    check("C: looser → delta_positive positive",
          comp["delta_vs_baseline"]["positive_applied_rate"] > 0.0)
    check("C: looser → policy_safe=True", comp["policy_safe"] is True)

    # With extreme loosening driving rate above warn threshold (0.30)
    # 4/10 = 0.40 which is > 0.30 → MEDIUM_RISK (or HIGH_RISK if > 0.45)
    check("C: looser with 40% pos rate → MEDIUM_RISK or HIGH_RISK",
          comp["policy_risk_class"] in ("MEDIUM_RISK", "HIGH_RISK"),
          f"got {comp['policy_risk_class']}")


# ===========================================================================
# D. Stronger negative dampening
# ===========================================================================

def test_d_stronger_negative_dampening():
    # 10 records: 5 with negative memory (moderate), 5 neutral
    recs = (
        [make_sim_rec(memory_available=True, memory_confidence=0.80,
                      memory_modifier=0.95, memory_bias_class="NEGATIVE",
                      cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
                      memory_influence_gate="NEGATIVE_GATE_OPEN",
                      base_feedback_confidence=0.70)] * 5 +
        [make_sim_rec(memory_available=True, memory_confidence=0.80,
                      memory_modifier=1.00, memory_bias_class="NEUTRAL",
                      memory_influence_gate="MEMORY_NEUTRAL")] * 5
    )

    cand = [{
        "policy_name": "stronger_negative_dampening",
        "policy_description": "Test: neg blend 0.50→0.70, cap 0.05→0.07",
        "overlay": {"memory_gate": {"negative_blend_weight": 0.70, "negative_correction_cap": 0.07}},
    }]
    result = sim.build_simulation(recs, BASELINE_POLICY, cand)
    comp = result["policy_comparisons"][0]

    # Baseline negative delta: (0.95 - 1.00) * 0.50 = -0.025
    # Candidate negative delta: (0.95 - 1.00) * 0.70 = -0.035 (capped at -0.07 → -0.035)
    check("D: stronger → avg_modifier_delta more negative under candidate",
          comp["simulated_avg_modifier_delta"] < result["baseline_metrics"]["simulated_avg_modifier_delta"],
          f"cand={comp['simulated_avg_modifier_delta']} base={result['baseline_metrics']['simulated_avg_modifier_delta']}")
    check("D: stronger → policy_safe=True", comp["policy_safe"] is True)
    check("D: stronger → no safe band violations", comp["simulated_safe_band_violation_count"] == 0)
    # Negative rate unchanged (same records go through negative gate)
    check("D: stronger → negative_rate unchanged",
          comp["simulated_negative_applied_rate"] == result["baseline_metrics"]["simulated_negative_applied_rate"],
          f"cand={comp['simulated_negative_applied_rate']} base={result['baseline_metrics']['simulated_negative_applied_rate']}")


# ===========================================================================
# E. Shorter cooldown — APPROXIMATE simulation
# (cooldown flag state can't be re-simulated from AC-65 records alone;
# tested here via explicit cooldown records)
# ===========================================================================

def test_e_shorter_cooldown_scope():
    # Test that BUILTIN_CANDIDATES includes stricter_positive_gate (proxy for E —
    # parameter that reduces applied rate). Cooldown itself is not re-simulated
    # at gate level (flag is preserved from obs record).
    #
    # Instead test: higher memory confidence threshold reduces memory-applied rate

    recs = (
        [make_sim_rec(memory_available=True, memory_confidence=0.55,
                      memory_modifier=0.95, memory_bias_class="NEGATIVE",
                      cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
                      memory_influence_gate="NEGATIVE_GATE_OPEN",
                      base_feedback_confidence=0.70)] * 4 +
        [make_sim_rec(memory_available=True, memory_confidence=0.80,
                      memory_modifier=1.00, memory_bias_class="NEUTRAL",
                      memory_influence_gate="MEMORY_NEUTRAL")] * 6
    )

    # Higher threshold: 0.50→0.65 blocks the 4 records with confidence=0.55
    cand = [{
        "policy_name": "higher_confidence_threshold",
        "policy_description": "Test: raise neg gate 0.50→0.65",
        "overlay": {"memory_gate": {"memory_confidence_min_negative": 0.65}},
    }]
    result = sim.build_simulation(recs, BASELINE_POLICY, cand)
    comp = result["policy_comparisons"][0]

    check("E: higher threshold → negative_rate falls to 0.0",
          comp["simulated_negative_applied_rate"] == 0.0,
          f"got {comp['simulated_negative_applied_rate']}")
    check("E: higher threshold → memory_applied_rate falls",
          comp["simulated_memory_applied_rate"] < result["baseline_metrics"]["simulated_memory_applied_rate"])
    # Blocked records go to BLOCKED_LOW_CONFIDENCE, not NO_EFFECT — so
    # neutral_fallback_rate doesn't change; check delta_negative instead.
    check("E: higher threshold → delta_negative_applied_rate negative",
          comp["delta_vs_baseline"]["negative_applied_rate"] < 0.0)


# ===========================================================================
# F. Higher memory confidence threshold → more neutral fallback
# ===========================================================================

def test_f_higher_confidence_threshold():
    # 10 records: 6 with confidence=0.58 (just above default 0.50 gate,
    # just below raised 0.65 gate), 4 high confidence
    recs = (
        [make_sim_rec(memory_available=True, memory_confidence=0.58,
                      memory_modifier=0.95, memory_bias_class="NEGATIVE",
                      cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
                      memory_influence_gate="NEGATIVE_GATE_OPEN",
                      base_feedback_confidence=0.70)] * 6 +
        [make_sim_rec(memory_available=True, memory_confidence=0.90,
                      memory_modifier=1.05, memory_bias_class="POSITIVE",
                      cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
                      memory_influence_gate="POSITIVE_GATE_OPEN",
                      base_feedback_confidence=0.70)] * 4
    )

    # Baseline: 6 negative + 4 positive applied
    cand = [{
        "policy_name": "higher_memory_confidence_threshold",
        "policy_description": "Test: raise negative gate 0.50→0.65",
        "overlay": {"memory_gate": {"memory_confidence_min_negative": 0.65}},
    }]
    result = sim.build_simulation(recs, BASELINE_POLICY, cand)
    comp = result["policy_comparisons"][0]

    check("F: threshold raised → negative_rate = 0.0",
          comp["simulated_negative_applied_rate"] == 0.0,
          f"got {comp['simulated_negative_applied_rate']}")
    check("F: threshold raised → memory_applied_rate falls",
          comp["simulated_memory_applied_rate"] < result["baseline_metrics"]["simulated_memory_applied_rate"])
    check("F: threshold raised → delta_negative_applied_rate < 0",
          comp["delta_vs_baseline"]["negative_applied_rate"] < 0.0)
    check("F: threshold raised → policy_safe=True", comp["policy_safe"] is True)


# ===========================================================================
# G. Unsafe candidate → HIGH_RISK / TOO_RISKY
# ===========================================================================

def test_g_unsafe_candidate():
    recs = [make_sim_rec() for _ in range(10)]

    # Extreme parameters: positive_correction_cap=0.15 (limit is 0.06)
    unsafe_cand = [{
        "policy_name": "unsafe_extreme",
        "policy_description": "Extreme params beyond safe limits",
        "overlay": {"memory_gate": {
            "positive_correction_cap": 0.15,
            "negative_blend_weight":   0.95,
            "negative_correction_cap": 0.15,
        }},
    }]
    result = sim.build_simulation(recs, BASELINE_POLICY, unsafe_cand)
    comp = result["policy_comparisons"][0]

    check("G: unsafe → policy_safe=False", comp["policy_safe"] is False)
    check("G: unsafe → HIGH_RISK", comp["policy_risk_class"] == "HIGH_RISK",
          f"got {comp['policy_risk_class']}")
    check("G: unsafe → TOO_RISKY recommendation",
          comp["policy_recommendation"] == "TOO_RISKY",
          f"got {comp['policy_recommendation']}")
    check("G: unsafe → violations listed",
          len(comp["policy_safe_violations"]) > 0)

    # Validate_policy directly
    flat = sim.flatten_policy(sim.apply_overlay(sim.DEFAULT_POLICY, unsafe_cand[0]["overlay"]))
    is_safe, reasons = sim.validate_policy(flat)
    check("G: validate_policy returns False", is_safe is False)
    check("G: validate_policy returns violation reasons", len(reasons) > 0)


# ===========================================================================
# H. Missing policy file → safe default fallback
# ===========================================================================

def test_h_missing_policy_file():
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "does_not_exist.json"
        policy, fallback_used, reason = sim.load_policy(missing)
        check("H: missing file → fallback_used=True", fallback_used is True)
        check("H: missing file → returns valid policy dict", isinstance(policy, dict))
        check("H: missing file → groups present", "groups" in policy)
        check("H: missing file → policy_name correct",
              policy.get("policy_name") == "baseline_default",
              f"got {policy.get('policy_name')}")
        check("H: missing file → reason contains FALLBACK", "FALLBACK" in reason)

    # Malformed JSON
    with tempfile.TemporaryDirectory() as tmp:
        bad = Path(tmp) / "bad.json"
        bad.write_text("NOT_JSON{{{", encoding="utf-8")
        policy, fallback_used, reason = sim.load_policy(bad)
        check("H: bad JSON → fallback_used=True", fallback_used is True)
        check("H: bad JSON → reason contains FALLBACK", "FALLBACK" in reason)

    # Valid file — should NOT use fallback
    policy_path = Path(__file__).parent / "ant_colony" / "policy" / "allocation_memory_policy.json"
    if policy_path.exists():
        policy, fallback_used, reason = sim.load_policy(policy_path)
        check("H: valid file → fallback_used=False", fallback_used is False,
              f"reason: {reason}")
        check("H: valid file → groups present", "groups" in policy)


# ===========================================================================
# I. Summary consistency
# ===========================================================================

def test_i_summary_consistency():
    # 10 records with a mix of impact types
    recs = (
        [make_sim_rec(memory_available=True, memory_confidence=0.85,
                      memory_modifier=1.05, memory_bias_class="POSITIVE",
                      cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
                      memory_influence_gate="POSITIVE_GATE_OPEN",
                      base_feedback_confidence=0.70)] * 3 +
        [make_sim_rec(memory_available=True, memory_confidence=0.80,
                      memory_modifier=0.95, memory_bias_class="NEGATIVE",
                      cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
                      memory_influence_gate="NEGATIVE_GATE_OPEN",
                      base_feedback_confidence=0.70)] * 3 +
        [make_sim_rec(memory_available=True, memory_confidence=0.80,
                      memory_modifier=1.00, memory_bias_class="NEUTRAL",
                      memory_influence_gate="MEMORY_NEUTRAL")] * 4
    )

    cands = [
        {"policy_name": "cand_a", "policy_description": "Test A",
         "overlay": {"memory_gate": {"memory_confidence_min_positive": 0.85}}},
        {"policy_name": "cand_b", "policy_description": "Test B",
         "overlay": {"memory_gate": {"negative_blend_weight": 0.70}}},
    ]
    result = sim.build_simulation(recs, BASELINE_POLICY, cands)

    summary = result["summary"]
    check("I: summary.candidate_policies_total=2",
          summary["candidate_policies_total"] == 2)
    check("I: summary.records_total=10",
          summary["records_total"] == 10)
    check("I: summary.simulations_completed=2",
          summary["simulations_completed"] == 2)
    check("I: summary.simulations_failed=0",
          summary["simulations_failed"] == 0)
    check("I: policy_comparisons count matches candidates",
          len(result["policy_comparisons"]) == 2)
    check("I: baseline_metrics present",
          "baseline_metrics" in result)
    check("I: baseline_policy present",
          "baseline_policy" in result)

    # Each comparison has required fields
    required_fields = [
        "policy_name", "policy_description", "changed_parameters",
        "policy_safe", "policy_safe_violations", "simulated_records_total",
        "simulated_memory_applied_rate", "simulated_positive_applied_rate",
        "simulated_negative_applied_rate", "simulated_caution_applied_rate",
        "simulated_conflict_block_rate", "simulated_neutral_fallback_rate",
        "simulated_avg_modifier_delta", "simulated_avg_confidence_delta",
        "simulated_safe_band_violation_count",
        "delta_vs_baseline", "policy_risk_class", "policy_recommendation",
        "simulation_reasons",
    ]
    for comp in result["policy_comparisons"]:
        for field in required_fields:
            check(f"I: comparison {comp['policy_name']} has field {field}",
                  field in comp, f"missing {field}")

    # Delta consistency: positive_applied_rate delta = sim_rate - baseline_rate
    for comp in result["policy_comparisons"]:
        base_pos = result["baseline_metrics"]["simulated_positive_applied_rate"]
        sim_pos  = comp["simulated_positive_applied_rate"]
        delta_pos = comp["delta_vs_baseline"]["positive_applied_rate"]
        check(f"I: {comp['policy_name']} delta_positive consistent",
              abs(delta_pos - round(sim_pos - base_pos, 4)) < 1e-6,
              f"delta={delta_pos} sim={sim_pos} base={base_pos}")

    # Summary fields consistency
    check("I: summary.reviewable_records_count = records_total",
          summary["reviewable_records_count"] == summary["records_total"])
    check("I: summary.recommendations_generated_count = simulations_completed",
          summary["recommendations_generated_count"] == summary["simulations_completed"])


# ===========================================================================
# J. Additional invariants and policy abstraction checks
# ===========================================================================

def test_j_invariants():
    # J1. flatten_policy produces flat dict
    flat = sim.flatten_policy(BASELINE_POLICY)
    for expected_key in [
        "memory_confidence_min_negative", "memory_confidence_min_positive",
        "negative_blend_weight", "positive_blend_weight",
        "negative_correction_cap", "positive_correction_cap",
        "modifier_band_min", "modifier_band_max",
        "cooldown_cycles_default", "window_size",
        "review_min_records", "review_positive_applied_rate_warn",
    ]:
        check(f"J: flatten_policy has key {expected_key}", expected_key in flat)

    # J2. Default policy values match AC-63/64/65/66 hardcoded constants
    check("J: default memory_confidence_min_negative=0.50",
          flat["memory_confidence_min_negative"] == 0.50)
    check("J: default memory_confidence_min_positive=0.75",
          flat["memory_confidence_min_positive"] == 0.75)
    check("J: default negative_blend_weight=0.50",
          flat["negative_blend_weight"] == 0.50)
    check("J: default positive_blend_weight=0.30",
          flat["positive_blend_weight"] == 0.30)
    check("J: default negative_correction_cap=0.05",
          flat["negative_correction_cap"] == 0.05)
    check("J: default positive_correction_cap=0.03",
          flat["positive_correction_cap"] == 0.03)
    check("J: default modifier_band_min=0.90",
          flat["modifier_band_min"] == 0.90)
    check("J: default modifier_band_max=1.05",
          flat["modifier_band_max"] == 1.05)
    check("J: default cooldown_cycles_default=3",
          flat["cooldown_cycles_default"] == 3)
    check("J: default review_positive_applied_rate_warn=0.30",
          flat["review_positive_applied_rate_warn"] == 0.30)

    # J3. apply_overlay does not mutate baseline
    import copy
    original = copy.deepcopy(BASELINE_POLICY)
    sim.apply_overlay(BASELINE_POLICY, {"memory_gate": {"memory_confidence_min_positive": 0.99}})
    check("J: apply_overlay does not mutate baseline",
          BASELINE_POLICY["groups"]["memory_gate"]["memory_confidence_min_positive"] == 0.75)

    # J4. validate_policy — safe params → safe
    is_safe, _ = sim.validate_policy(flat)
    check("J: default policy is safe", is_safe is True)

    # J5. validate_policy — inverted positive/negative gate → unsafe
    bad_flat = dict(flat)
    bad_flat["memory_confidence_min_positive"] = 0.30  # less than neg gate 0.50
    bad_flat["memory_confidence_min_negative"] = 0.50
    is_safe, reasons = sim.validate_policy(bad_flat)
    check("J: inverted gates → unsafe", is_safe is False)
    check("J: inverted gates → reason mentions positive < negative",
          any("positive" in r.lower() for r in reasons))

    # J6. simulate_record — RECENT_HARMFUL_BLOCKED preserved
    rec = make_sim_rec(
        memory_available=True, memory_confidence=0.85,
        memory_modifier=1.05, memory_bias_class="POSITIVE",
        cycle_modifier=1.00, cycle_bias_class="NEUTRAL",
        memory_influence_gate="RECENT_HARMFUL_BLOCKED",
    )
    r = sim.simulate_record(rec, flat)
    check("J: RECENT_HARMFUL_BLOCKED preserved", r["sim_gate_name"] == "RECENT_HARMFUL_BLOCKED")
    check("J: RECENT_HARMFUL_BLOCKED → applied=False", r["sim_memory_modifier_applied"] is False)

    # J7. build_simulation with 0 records — no crash, minimal safe output
    result = sim.build_simulation([], BASELINE_POLICY, [])
    check("J: empty records → summary present", "summary" in result)
    check("J: empty records → records_total=0", result["summary"]["records_total"] == 0)

    # J8. build_simulation with 0 candidates — comparisons empty
    result = sim.build_simulation([make_sim_rec()], BASELINE_POLICY, [])
    check("J: no candidates → policy_comparisons empty", result["policy_comparisons"] == [])

    # J9. BUILTIN_CANDIDATES count matches expected
    check("J: BUILTIN_CANDIDATES count >= 5", len(sim.BUILTIN_CANDIDATES) >= 5)

    # J10. VERSION is memory_policy_simulation_v1
    check("J: VERSION", sim.VERSION == "memory_policy_simulation_v1", f"got {sim.VERSION}")

    # J11. Policy file exists and loads correctly
    policy_path = Path(__file__).parent / "ant_colony" / "policy" / "allocation_memory_policy.json"
    check("J: policy file exists", policy_path.exists())
    if policy_path.exists():
        content = json.loads(policy_path.read_text(encoding="utf-8"))
        check("J: policy file has groups", "groups" in content)
        check("J: policy file has memory_gate group", "memory_gate" in content.get("groups", {}))
        check("J: policy file has review_thresholds group", "review_thresholds" in content.get("groups", {}))
        check("J: policy file memory_confidence_min_negative=0.50",
              content["groups"]["memory_gate"]["memory_confidence_min_negative"] == 0.50)

    # J12. compute_changed_params — identifies differences correctly
    flat_base = sim.flatten_policy(BASELINE_POLICY)
    overlay = {"memory_gate": {"memory_confidence_min_positive": 0.85}}
    cand_policy = sim.apply_overlay(BASELINE_POLICY, overlay)
    flat_cand = sim.flatten_policy(cand_policy)
    changed = sim.compute_changed_params(flat_base, flat_cand)
    check("J: changed_params detects positive gate change",
          "memory_confidence_min_positive" in changed)
    check("J: changed_params baseline value correct",
          changed["memory_confidence_min_positive"]["baseline"] == 0.75)
    check("J: changed_params candidate value correct",
          changed["memory_confidence_min_positive"]["candidate"] == 0.85)

    # J13. CONFLICT_ALLOW_ON_CONFLICT — allows positive when cycle is negative
    rec = make_sim_rec(
        memory_available=True, memory_confidence=0.85,
        memory_modifier=1.05, memory_bias_class="POSITIVE",
        cycle_modifier=1.00, cycle_bias_class="NEGATIVE",
        memory_influence_gate="CONFLICT_BLOCKED",
        base_feedback_confidence=0.70,
    )
    allow_flat = dict(flat)
    allow_flat["conflict_policy_mode"] = "ALLOW_ON_CONFLICT"
    r = sim.simulate_record(rec, allow_flat)
    check("J: ALLOW_ON_CONFLICT → not blocked by conflict",
          r["sim_impact_class"] != "BLOCKED_BY_CONFLICT",
          f"got {r['sim_impact_class']}")
    check("J: ALLOW_ON_CONFLICT → POSITIVE_REINFORCEMENT",
          r["sim_impact_class"] == "POSITIVE_REINFORCEMENT",
          f"got {r['sim_impact_class']}")


# ===========================================================================
# Run all tests
# ===========================================================================

def main():
    test_a_baseline_parity()
    test_b_stricter_positive_gate()
    test_c_looser_positive_gate()
    test_d_stronger_negative_dampening()
    test_e_shorter_cooldown_scope()
    test_f_higher_confidence_threshold()
    test_g_unsafe_candidate()
    test_h_missing_policy_file()
    test_i_summary_consistency()
    test_j_invariants()

    total = len(_PASS) + len(_FAIL)
    print(f"\nAC-67 results: {len(_PASS)}/{total} PASS")

    if _FAIL:
        for f in _FAIL:
            print(f"  {f}")
        sys.exit(1)
    else:
        print("  All tests PASS — AC67 memory policy simulation validated.")
        sys.exit(0)


if __name__ == "__main__":
    main()
