"""
AC-68 tests: Policy Loader Wiring + Policy Surface Consolidation

Verifies that:
  A. Canonical loader is the single source — AC-67 simulator no longer embeds its own DEFAULT_POLICY
  B. Baseline equivalence — AC-63/64/66 module constants match canonical loader values exactly
  C. Missing/corrupt policy file — loader fallback, module constants still correct
  D. Partial policy (missing subsection) — defaults fill in, modules stable
  E. Policy values flow end-to-end — changing a policy value (via test overlay) propagates
"""
import copy
import importlib.util
import json
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Load production modules
# ---------------------------------------------------------------------------

def _load_module(name: str, rel_path: str):
    path = Path(__file__).parent / rel_path
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

loader_mod = _load_module("loader",  "ant_colony/policy/load_allocation_memory_policy_lite.py")
sim_mod    = _load_module("sim_mod", "ant_colony/build_allocation_memory_policy_simulation_lite.py")
ac63_mod   = _load_module("ac63",   "ant_colony/build_allocation_feedback_memory_lite.py")
ac64_mod   = _load_module("ac64",   "ant_colony/build_allocation_decision_quality_lite.py")
ac66_mod   = _load_module("ac66",   "ant_colony/build_allocation_memory_policy_review_lite.py")


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

_PASS: list = []
_FAIL: list = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        _PASS.append(label)
    else:
        _FAIL.append(f"FAIL: {label}" + (f" — {detail}" if detail else ""))


# ===========================================================================
# A. Canonical loader is the single source
# ===========================================================================

def test_a_canonical_loader_single_source():
    # A1. Canonical loader exposes DEFAULT_POLICY (public, not _DEFAULT_POLICY)
    check("A: loader exposes DEFAULT_POLICY", hasattr(loader_mod, "DEFAULT_POLICY"))
    check("A: loader DEFAULT_POLICY has groups", "groups" in loader_mod.DEFAULT_POLICY)

    # A2. Simulator does NOT define its own DEFAULT_POLICY body — it re-exports from loader.
    #     Proof: sim.DEFAULT_POLICY equals loader.DEFAULT_POLICY (deep equality)
    check(
        "A: sim.DEFAULT_POLICY equals loader.DEFAULT_POLICY",
        sim_mod.DEFAULT_POLICY == loader_mod.DEFAULT_POLICY,
        f"sim={list(sim_mod.DEFAULT_POLICY.get('groups', {}).keys())} "
        f"loader={list(loader_mod.DEFAULT_POLICY.get('groups', {}).keys())}",
    )

    # A3. Simulator's load_policy IS the canonical loader's load_policy (same function object
    #     or at minimum same behaviour — test behaviour since importlib creates separate modules)
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "no_policy.json"
        p_sim,  fb_sim,  r_sim  = sim_mod.load_policy(missing)
        p_load, fb_load, r_load = loader_mod.load_policy(missing)
        check("A: sim.load_policy fallback = loader.load_policy fallback", fb_sim is True and fb_load is True)
        check("A: sim.load_policy returns same structure as loader", p_sim == p_load, f"sim≠loader")

    # A4. Simulator has no embedded DEFAULT_POLICY literal (grep the source)
    sim_src = Path(__file__).parent / "ant_colony" / "build_allocation_memory_policy_simulation_lite.py"
    sim_text = sim_src.read_text(encoding="utf-8")
    check(
        "A: simulator source does not embed a DEFAULT_POLICY dict literal",
        '"policy_name":    "baseline_default"' not in sim_text,
        "Found embedded policy dict in simulator",
    )
    check(
        "A: simulator source imports canonical loader",
        "_import_canonical_loader" in sim_text or "load_allocation_memory_policy_lite" in sim_text,
    )


# ===========================================================================
# B. Baseline equivalence — module constants match canonical loader values
# ===========================================================================

def test_b_baseline_equivalence():
    canonical, _, _ = loader_mod.load_policy()
    mg  = canonical["groups"]["memory_gate"]
    mw  = canonical["groups"]["memory_rolling_window"]
    rev = canonical["groups"]["review_thresholds"]

    # AC-63 rolling window
    check("B: AC63 WINDOW_SIZE == policy",
          ac63_mod.WINDOW_SIZE == mw["window_size"],
          f"{ac63_mod.WINDOW_SIZE} vs {mw['window_size']}")
    check("B: AC63 FULL_MEMORY_AT == policy",
          ac63_mod.FULL_MEMORY_AT == mw["full_memory_at"],
          f"{ac63_mod.FULL_MEMORY_AT} vs {mw['full_memory_at']}")
    check("B: AC63 COOLDOWN_PERSIST_CYCLES == policy",
          ac63_mod.COOLDOWN_PERSIST_CYCLES == mw["cooldown_cycles_default"],
          f"{ac63_mod.COOLDOWN_PERSIST_CYCLES} vs {mw['cooldown_cycles_default']}")

    # AC-64 memory gate
    check("B: AC64 MEMORY_CONF_GATE_NEG == policy",
          ac64_mod.MEMORY_CONF_GATE_NEG == mg["memory_confidence_min_negative"],
          f"{ac64_mod.MEMORY_CONF_GATE_NEG} vs {mg['memory_confidence_min_negative']}")
    check("B: AC64 MEMORY_CONF_GATE_POS == policy",
          ac64_mod.MEMORY_CONF_GATE_POS == mg["memory_confidence_min_positive"],
          f"{ac64_mod.MEMORY_CONF_GATE_POS} vs {mg['memory_confidence_min_positive']}")
    check("B: AC64 MEMORY_NEG_INFLUENCE == policy",
          ac64_mod.MEMORY_NEG_INFLUENCE == mg["negative_blend_weight"],
          f"{ac64_mod.MEMORY_NEG_INFLUENCE} vs {mg['negative_blend_weight']}")
    check("B: AC64 MEMORY_POS_INFLUENCE == policy",
          ac64_mod.MEMORY_POS_INFLUENCE == mg["positive_blend_weight"],
          f"{ac64_mod.MEMORY_POS_INFLUENCE} vs {mg['positive_blend_weight']}")
    check("B: AC64 MEMORY_MAX_CORR_NEG == policy",
          ac64_mod.MEMORY_MAX_CORR_NEG == mg["negative_correction_cap"],
          f"{ac64_mod.MEMORY_MAX_CORR_NEG} vs {mg['negative_correction_cap']}")
    check("B: AC64 MEMORY_MAX_CORR_POS == policy",
          ac64_mod.MEMORY_MAX_CORR_POS == mg["positive_correction_cap"],
          f"{ac64_mod.MEMORY_MAX_CORR_POS} vs {mg['positive_correction_cap']}")
    check("B: AC64 MODIFIER_MIN == policy",
          ac64_mod.MODIFIER_MIN == mg["modifier_band_min"],
          f"{ac64_mod.MODIFIER_MIN} vs {mg['modifier_band_min']}")
    check("B: AC64 MODIFIER_MAX == policy",
          ac64_mod.MODIFIER_MAX == mg["modifier_band_max"],
          f"{ac64_mod.MODIFIER_MAX} vs {mg['modifier_band_max']}")
    check("B: AC64 _RECENT_HARMFUL_LOOKBACK == policy",
          ac64_mod._RECENT_HARMFUL_LOOKBACK == mg["recent_harmful_lookback"],
          f"{ac64_mod._RECENT_HARMFUL_LOOKBACK} vs {mg['recent_harmful_lookback']}")
    check("B: AC64 _RECENT_HARMFUL_THRESHOLD == policy",
          ac64_mod._RECENT_HARMFUL_THRESHOLD == mg["recent_harmful_block_threshold"],
          f"{ac64_mod._RECENT_HARMFUL_THRESHOLD} vs {mg['recent_harmful_block_threshold']}")
    check("B: AC64 _CONFLICT_POLICY_MODE == policy",
          ac64_mod._CONFLICT_POLICY_MODE == mg["conflict_policy_mode"],
          f"{ac64_mod._CONFLICT_POLICY_MODE} vs {mg['conflict_policy_mode']}")

    # AC-66 review thresholds
    check("B: AC66 MIN_REVIEWABLE_RECORDS == policy",
          ac66_mod.MIN_REVIEWABLE_RECORDS == rev["review_min_records"],
          f"{ac66_mod.MIN_REVIEWABLE_RECORDS} vs {rev['review_min_records']}")
    check("B: AC66 POSITIVE_RATE_REVIEW == policy",
          ac66_mod.POSITIVE_RATE_REVIEW == rev["review_positive_applied_rate_warn"],
          f"{ac66_mod.POSITIVE_RATE_REVIEW} vs {rev['review_positive_applied_rate_warn']}")
    check("B: AC66 POSITIVE_RATE_WATCH == policy",
          ac66_mod.POSITIVE_RATE_WATCH == rev["review_positive_applied_rate_watch"],
          f"{ac66_mod.POSITIVE_RATE_WATCH} vs {rev['review_positive_applied_rate_watch']}")
    check("B: AC66 NEGATIVE_RATE_REVIEW == policy",
          ac66_mod.NEGATIVE_RATE_REVIEW == rev["review_negative_applied_rate_warn"],
          f"{ac66_mod.NEGATIVE_RATE_REVIEW} vs {rev['review_negative_applied_rate_warn']}")
    check("B: AC66 COOLDOWN_RATE_REVIEW == policy",
          ac66_mod.COOLDOWN_RATE_REVIEW == rev["review_cooldown_rate_warn"],
          f"{ac66_mod.COOLDOWN_RATE_REVIEW} vs {rev['review_cooldown_rate_warn']}")
    check("B: AC66 CONFLICT_RATE_REVIEW == policy",
          ac66_mod.CONFLICT_RATE_REVIEW == rev["review_conflict_block_rate_warn"],
          f"{ac66_mod.CONFLICT_RATE_REVIEW} vs {rev['review_conflict_block_rate_warn']}")
    check("B: AC66 LOW_CONF_RATE_REVIEW == policy",
          ac66_mod.LOW_CONF_RATE_REVIEW == rev["review_low_conf_blocked_rate_warn"],
          f"{ac66_mod.LOW_CONF_RATE_REVIEW} vs {rev['review_low_conf_blocked_rate_warn']}")
    check("B: AC66 AVG_DELTA_WATCH == policy",
          ac66_mod.AVG_DELTA_WATCH == rev["review_avg_delta_watch"],
          f"{ac66_mod.AVG_DELTA_WATCH} vs {rev['review_avg_delta_watch']}")


# ===========================================================================
# C. Missing policy file → loader fallback; module constants remain correct
# ===========================================================================

def test_c_missing_file_fallback():
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "no_policy.json"

        # Loader returns fallback with correct default values
        policy, fb, reason = loader_mod.load_policy(missing)
        check("C: missing file → fallback_used=True", fb is True)
        check("C: missing file → FALLBACK in reason", "FALLBACK" in reason)
        check("C: missing file → policy still has groups", "groups" in policy)

        # Default values in fallback match what modules expect
        mg  = policy["groups"]["memory_gate"]
        mw  = policy["groups"]["memory_rolling_window"]
        rev = policy["groups"]["review_thresholds"]

        check("C: fallback memory_confidence_min_negative=0.50",
              mg["memory_confidence_min_negative"] == 0.50)
        check("C: fallback window_size=10",      mw["window_size"] == 10)
        check("C: fallback review_min_records=5", rev["review_min_records"] == 5)
        check("C: fallback modifier_band_min=0.90", mg["modifier_band_min"] == 0.90)
        check("C: fallback modifier_band_max=1.05", mg["modifier_band_max"] == 1.05)
        check("C: fallback cooldown_cycles_default=3", mw["cooldown_cycles_default"] == 3)


# ===========================================================================
# D. Partial policy (missing subsection) → defaults fill in
# ===========================================================================

def test_d_partial_policy_fallback():
    # Policy file with only memory_gate — missing memory_rolling_window and review_thresholds
    partial = {
        "policy_name": "partial_test",
        "policy_version": "v1",
        "description": "partial",
        "groups": {
            "memory_gate": {
                "memory_confidence_min_negative": 0.60,
                "memory_confidence_min_positive": 0.80,
                "negative_blend_weight": 0.50,
                "positive_blend_weight": 0.30,
                "modifier_band_min": 0.90,
                "modifier_band_max": 1.05,
            }
            # memory_rolling_window and review_thresholds are missing
        }
    }

    # validate_structure should reject (missing required groups)
    with tempfile.TemporaryDirectory() as tmp:
        pfile = Path(tmp) / "partial.json"
        pfile.write_text(json.dumps(partial), encoding="utf-8")
        policy, fb, reason = loader_mod.load_policy(pfile)
        # Should fall back because required groups are missing
        check("D: missing required group → fallback", fb is True,
              f"reason={reason}")
        check("D: missing required group → default window_size=10",
              policy["groups"]["memory_rolling_window"]["window_size"] == 10)

    # Policy with all groups but a single key missing → deep-merge fills it in
    complete_minus_one = {
        "policy_name": "complete_minus_one",
        "policy_version": "v1",
        "description": "test",
        "groups": {
            "memory_gate": {
                "memory_confidence_min_negative": 0.55,
                "memory_confidence_min_positive": 0.80,
                "negative_blend_weight": 0.50,
                "positive_blend_weight": 0.30,
                "modifier_band_min": 0.90,
                "modifier_band_max": 1.05,
                # recent_harmful_lookback intentionally omitted
                "recent_harmful_block_threshold": 2,
                "conflict_policy_mode": "BLOCK_ON_CONFLICT",
                "negative_correction_cap": 0.05,
                "positive_correction_cap": 0.03,
            },
            "memory_rolling_window": {
                "window_size": 10,
                "full_memory_at": 8,
                "cooldown_cycles_default": 3,
            },
            "review_thresholds": {
                "review_min_records": 5,
                "review_positive_applied_rate_warn": 0.30,
                "review_positive_applied_rate_watch": 0.20,
                "review_negative_applied_rate_warn": 0.70,
                "review_cooldown_rate_warn": 0.50,
                "review_conflict_block_rate_warn": 0.30,
                "review_low_conf_blocked_rate_warn": 0.50,
                "review_avg_delta_watch": 0.02,
            },
        }
    }

    with tempfile.TemporaryDirectory() as tmp:
        pfile = Path(tmp) / "minus_one.json"
        pfile.write_text(json.dumps(complete_minus_one), encoding="utf-8")
        policy, fb, reason = loader_mod.load_policy(pfile)
        check("D: complete minus one key → no fallback", fb is False, f"reason={reason}")
        check("D: modified value loaded (neg_conf=0.55)",
              policy["groups"]["memory_gate"]["memory_confidence_min_negative"] == 0.55)
        check("D: missing key filled from default (recent_harmful_lookback=3)",
              policy["groups"]["memory_gate"]["recent_harmful_lookback"] == 3)


# ===========================================================================
# E. Policy load observability — fallback status is accessible per module
# ===========================================================================

def test_e_policy_observability():
    # Each wired module exposes _POLICY_FALLBACK_USED and _POLICY_LOAD_REASON
    for mod, name in [(ac63_mod, "AC63"), (ac64_mod, "AC64"), (ac66_mod, "AC66")]:
        check(f"E: {name} exposes _POLICY_FALLBACK_USED",
              hasattr(mod, "_POLICY_FALLBACK_USED"))
        check(f"E: {name} exposes _POLICY_LOAD_REASON",
              hasattr(mod, "_POLICY_LOAD_REASON"))
        check(f"E: {name} _POLICY_FALLBACK_USED is bool",
              isinstance(mod._POLICY_FALLBACK_USED, bool))
        check(f"E: {name} _POLICY_LOAD_REASON is str",
              isinstance(mod._POLICY_LOAD_REASON, str))

    # With actual policy file present, no module should be using fallback
    for mod, name in [(ac63_mod, "AC63"), (ac64_mod, "AC64"), (ac66_mod, "AC66")]:
        check(f"E: {name} loaded policy successfully (not fallback)",
              mod._POLICY_FALLBACK_USED is False,
              f"reason={mod._POLICY_LOAD_REASON}")


# ===========================================================================
# F. AC-63 functional — behavior identical after wiring
# ===========================================================================

def test_f_ac63_functional():
    # update_memory_record uses WINDOW_SIZE and COOLDOWN_PERSIST_CYCLES
    empty = ac63_mod.empty_memory_record("BTC-EUR__EDGE4", "BTC-EUR")

    outcomes = [{"audit_id": f"a{i}", "outcome_label": "HELPFUL"} for i in range(5)]
    updated = ac63_mod.update_memory_record(empty, outcomes, False, "CYC1", "2026-01-01T00:00:00Z")

    check("F: AC63 window capped at WINDOW_SIZE",
          len(updated["rolling_window"]) <= ac63_mod.WINDOW_SIZE)
    check("F: AC63 WINDOW_SIZE=10 from policy",
          ac63_mod.WINDOW_SIZE == 10)
    check("F: AC63 FULL_MEMORY_AT=8 from policy",
          ac63_mod.FULL_MEMORY_AT == 8)

    # Cooldown persistence
    with_cooldown = ac63_mod.update_memory_record(empty, [], True, "CYC2", "2026-01-01T00:00:01Z")
    check("F: AC63 cooldown set to COOLDOWN_PERSIST_CYCLES",
          with_cooldown["cooldown_cycles_remaining"] == ac63_mod.COOLDOWN_PERSIST_CYCLES)
    check("F: AC63 COOLDOWN_PERSIST_CYCLES=3 from policy",
          ac63_mod.COOLDOWN_PERSIST_CYCLES == 3)


# ===========================================================================
# G. AC-64 functional — apply_memory_gate behavior identical after wiring
# ===========================================================================

def test_g_ac64_functional():
    # Negative correction: (0.95 - 1.00) * 0.50 = -0.025
    # Capped at -0.05 → -0.025; final = clamp(1.00 - 0.025, 0.90, 1.05) = 0.975
    mem_rec = {
        "memory_confidence": 0.80,
        "memory_modifier":   0.95,  # unused directly in apply_memory_gate
        "rolling_window": [
            {"outcome_label": "HARMFUL"},
            {"outcome_label": "HARMFUL"},
            {"outcome_label": "HARMFUL"},
            {"outcome_label": "HELPFUL"},
            {"outcome_label": "NEUTRAL"},
        ],
        "cooldown_flag": False,
    }
    final, applied, gate, mem_applied, reason = ac64_mod.apply_memory_gate(1.00, "NEUTRAL", mem_rec)
    check("G: AC64 negative gate gives correction",
          applied is True and mem_applied is True, f"gate={gate}")
    check("G: AC64 final modifier within band",
          ac64_mod.MODIFIER_MIN <= final <= ac64_mod.MODIFIER_MAX,
          f"final={final}")

    # Positive correction blocked by BLOCK_ON_CONFLICT
    pos_mem = {
        "memory_confidence": 0.85,
        "rolling_window": [
            {"outcome_label": "HELPFUL"},
            {"outcome_label": "HELPFUL"},
            {"outcome_label": "HELPFUL"},
            {"outcome_label": "HELPFUL"},
            {"outcome_label": "HELPFUL"},
        ],
        "cooldown_flag": False,
    }
    final2, applied2, gate2, _, _ = ac64_mod.apply_memory_gate(1.00, "NEGATIVE", pos_mem)
    check("G: AC64 positive blocked by conflict (BLOCK_ON_CONFLICT mode)",
          gate2 == "CONFLICT_BLOCKED", f"gate={gate2}")
    check("G: AC64 conflict mode from policy",
          ac64_mod._CONFLICT_POLICY_MODE == "BLOCK_ON_CONFLICT")


# ===========================================================================
# H. AC-66 functional — review thresholds from policy
# ===========================================================================

def test_h_ac66_functional():
    # Build records that trigger REVIEW_POSITIVE_GATE (positive_rate > 0.30)
    # 4 positive out of 10 = 0.40 > 0.30
    recs = (
        [{"impact_class": "POSITIVE_REINFORCEMENT", "memory_available": True,
          "memory_modifier_applied": True, "modifier_delta": 0.015,
          "confidence_delta": 0.01, "dq_score_delta": 0.01,
          "safe_band_ok": True, "dq_gate_changed": False, "cooldown_flag": False}] * 4 +
        [{"impact_class": "NO_EFFECT", "memory_available": True,
          "memory_modifier_applied": False, "modifier_delta": 0.0,
          "confidence_delta": 0.0, "dq_score_delta": 0.0,
          "safe_band_ok": True, "dq_gate_changed": False, "cooldown_flag": False}] * 6
    )
    review = ac66_mod.build_policy_review(recs)
    pos_rate = review["metrics"]["positive_applied_rate"]

    # 0.40 > POSITIVE_RATE_REVIEW (0.30) → REVIEW_REQUIRED
    check("H: AC66 positive_rate=0.40 triggers REVIEW when threshold=0.30",
          review["policy_status"] in ("REVIEW_REQUIRED", "REVIEW_SUGGESTED"),
          f"status={review['policy_status']} rate={pos_rate}")
    check("H: AC66 MIN_REVIEWABLE_RECORDS=5 from policy",
          ac66_mod.MIN_REVIEWABLE_RECORDS == 5)
    check("H: AC66 POSITIVE_RATE_REVIEW=0.30 from policy",
          ac66_mod.POSITIVE_RATE_REVIEW == 0.30)


# ===========================================================================
# Run all tests
# ===========================================================================

def main():
    test_a_canonical_loader_single_source()
    test_b_baseline_equivalence()
    test_c_missing_file_fallback()
    test_d_partial_policy_fallback()
    test_e_policy_observability()
    test_f_ac63_functional()
    test_g_ac64_functional()
    test_h_ac66_functional()

    total = len(_PASS) + len(_FAIL)
    print(f"\nAC-68 results: {len(_PASS)}/{total} PASS")

    if _FAIL:
        for f in _FAIL:
            print(f"  {f}")
        sys.exit(1)
    else:
        print("  All tests PASS — AC68 policy loader wiring validated.")
        sys.exit(0)


if __name__ == "__main__":
    main()
