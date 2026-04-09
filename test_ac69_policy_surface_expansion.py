"""
AC-69 tests: Selective policy surface expansion

Verifies that newly added policy keys propagate correctly and
that baseline behaviour is identical to pre-AC-69 hardcoded values.

New keys tested:
  memory_gate:
    bias_caution_signal_threshold   (-0.50)
    bias_caution_harmful_ratio      (0.60)
    bias_negative_signal_threshold  (-0.20)
    bias_positive_signal_threshold  (0.20)
  memory_rolling_window:
    memory_min_confidence           (0.40)
    sparse_window_threshold         (3)
  review_thresholds:
    review_memory_applied_rate_low_watch   (0.10)
    review_memory_applied_rate_high_watch  (0.80)
"""
import importlib.util
import sys
from pathlib import Path


def _load(name, rel):
    path = Path(__file__).parent / rel
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


loader = _load("loader", "ant_colony/policy/load_allocation_memory_policy_lite.py")
ac63   = _load("ac63",   "ant_colony/build_allocation_feedback_memory_lite.py")
ac64   = _load("ac64",   "ant_colony/build_allocation_decision_quality_lite.py")
ac66   = _load("ac66",   "ant_colony/build_allocation_memory_policy_review_lite.py")

_PASS: list = []
_FAIL: list = []


def check(label, cond, detail=""):
    (_PASS if cond else _FAIL).append(
        label if cond else f"FAIL: {label}" + (f" — {detail}" if detail else "")
    )


# ---------------------------------------------------------------------------
# A. New keys present in DEFAULT_POLICY and policy file
# ---------------------------------------------------------------------------

def test_a_new_keys_in_policy():
    policy, fb, _ = loader.load_policy()
    mg  = policy["groups"]["memory_gate"]
    mw  = policy["groups"]["memory_rolling_window"]
    rev = policy["groups"]["review_thresholds"]

    check("A: bias_caution_signal_threshold in memory_gate",  "bias_caution_signal_threshold"  in mg)
    check("A: bias_caution_harmful_ratio in memory_gate",     "bias_caution_harmful_ratio"      in mg)
    check("A: bias_negative_signal_threshold in memory_gate", "bias_negative_signal_threshold"  in mg)
    check("A: bias_positive_signal_threshold in memory_gate", "bias_positive_signal_threshold"  in mg)
    check("A: memory_min_confidence in memory_rolling_window","memory_min_confidence"            in mw)
    check("A: sparse_window_threshold in memory_rolling_window","sparse_window_threshold"        in mw)
    check("A: review_memory_applied_rate_low_watch in review","review_memory_applied_rate_low_watch"  in rev)
    check("A: review_memory_applied_rate_high_watch in review","review_memory_applied_rate_high_watch" in rev)
    check("A: policy loaded from file (not fallback)", fb is False)

    # Baseline values are correct
    check("A: bias_caution_signal_threshold = -0.50",  mg["bias_caution_signal_threshold"]  == -0.50)
    check("A: bias_caution_harmful_ratio = 0.60",      mg["bias_caution_harmful_ratio"]      == 0.60)
    check("A: bias_negative_signal_threshold = -0.20", mg["bias_negative_signal_threshold"]  == -0.20)
    check("A: bias_positive_signal_threshold = 0.20",  mg["bias_positive_signal_threshold"]  == 0.20)
    check("A: memory_min_confidence = 0.40",           mw["memory_min_confidence"]           == 0.40)
    check("A: sparse_window_threshold = 3",            mw["sparse_window_threshold"]         == 3)
    check("A: review_low_watch = 0.10",  rev["review_memory_applied_rate_low_watch"]  == 0.10)
    check("A: review_high_watch = 0.80", rev["review_memory_applied_rate_high_watch"] == 0.80)


# ---------------------------------------------------------------------------
# B. AC-63 module constants match policy
# ---------------------------------------------------------------------------

def test_b_ac63_constants():
    policy, _, _ = loader.load_policy()
    mw = policy["groups"]["memory_rolling_window"]

    check("B: AC63 MEMORY_MIN_CONFIDENCE == policy",
          ac63.MEMORY_MIN_CONFIDENCE == mw["memory_min_confidence"],
          f"{ac63.MEMORY_MIN_CONFIDENCE} vs {mw['memory_min_confidence']}")
    check("B: AC63 _SPARSE_WINDOW_MIN == policy",
          ac63._SPARSE_WINDOW_MIN == mw["sparse_window_threshold"],
          f"{ac63._SPARSE_WINDOW_MIN} vs {mw['sparse_window_threshold']}")

    # Functional: memory_status_for uses policy-sourced threshold
    check("B: window=0 → BOOTSTRAP",
          ac63.memory_status_for(0, 0.0) == ac63.MEMORY_STATUS_BOOTSTRAP)
    check("B: window=1 → SPARSE (below sparse_window_threshold=3)",
          ac63.memory_status_for(1, 0.9) == ac63.MEMORY_STATUS_SPARSE)
    check("B: window=2 → SPARSE",
          ac63.memory_status_for(2, 0.9) == ac63.MEMORY_STATUS_SPARSE)
    check("B: window=3 → not SPARSE (at threshold, not below)",
          ac63.memory_status_for(3, 0.9) != ac63.MEMORY_STATUS_SPARSE)
    check("B: window=4, conf=0.30 → INSUFFICIENT (below MEMORY_MIN_CONFIDENCE=0.40)",
          ac63.memory_status_for(4, 0.30) == ac63.MEMORY_STATUS_INSUFFICIENT)
    check("B: window=4, conf=0.50 → ACTIVE",
          ac63.memory_status_for(4, 0.50) == ac63.MEMORY_STATUS_ACTIVE)


# ---------------------------------------------------------------------------
# C. AC-64 bias-class thresholds match policy
# ---------------------------------------------------------------------------

def test_c_ac64_bias_thresholds():
    policy, _, _ = loader.load_policy()
    mg = policy["groups"]["memory_gate"]

    check("C: AC64 _BIAS_CAUTION_SIGNAL_THRESHOLD == policy",
          ac64._BIAS_CAUTION_SIGNAL_THRESHOLD == mg["bias_caution_signal_threshold"],
          f"{ac64._BIAS_CAUTION_SIGNAL_THRESHOLD} vs {mg['bias_caution_signal_threshold']}")
    check("C: AC64 _BIAS_CAUTION_HARMFUL_RATIO == policy",
          ac64._BIAS_CAUTION_HARMFUL_RATIO == mg["bias_caution_harmful_ratio"],
          f"{ac64._BIAS_CAUTION_HARMFUL_RATIO} vs {mg['bias_caution_harmful_ratio']}")
    check("C: AC64 _BIAS_NEGATIVE_SIGNAL_THRESHOLD == policy",
          ac64._BIAS_NEGATIVE_SIGNAL_THRESHOLD == mg["bias_negative_signal_threshold"],
          f"{ac64._BIAS_NEGATIVE_SIGNAL_THRESHOLD} vs {mg['bias_negative_signal_threshold']}")
    check("C: AC64 _BIAS_POSITIVE_SIGNAL_THRESHOLD == policy",
          ac64._BIAS_POSITIVE_SIGNAL_THRESHOLD == mg["bias_positive_signal_threshold"],
          f"{ac64._BIAS_POSITIVE_SIGNAL_THRESHOLD} vs {mg['bias_positive_signal_threshold']}")

    # Functional: _memory_modifier_from_rec applies thresholds correctly
    def make_rec(helpful, harmful, conf=0.80):
        window = ([{"outcome_label": "HELPFUL"}] * helpful +
                  [{"outcome_label": "HARMFUL"}] * harmful)
        return {"memory_confidence": conf, "rolling_window": window}

    # 8 harmful out of 10 → harmful_ratio=0.80 ≥ 0.60 → NEGATIVE_CAUTION
    mod, bias = ac64._memory_modifier_from_rec(make_rec(2, 8))
    check("C: harmful_ratio=0.80 → NEGATIVE_CAUTION", bias == "NEGATIVE_CAUTION",
          f"got {bias}")

    # eff_signal = (2-8)/10 * 0.80 = -0.48 > -0.50 but harmful_ratio check triggers
    # Let's also test pure signal path: 0 helpful, 5 harmful, conf=0.80
    # eff_signal = (0-5)/5 * 0.80 = -0.80 ≤ -0.50 → NEGATIVE_CAUTION
    mod2, bias2 = ac64._memory_modifier_from_rec(make_rec(0, 5))
    check("C: eff_signal=-0.80 → NEGATIVE_CAUTION", bias2 == "NEGATIVE_CAUTION",
          f"got {bias2}")

    # 4 helpful, 6 harmful out of 10, conf=0.80
    # eff_signal = (4-6)/10 * 0.80 = -0.16 > -0.20 AND harmful_ratio=0.60 ≥ 0.60 → NEGATIVE_CAUTION
    mod3, bias3 = ac64._memory_modifier_from_rec(make_rec(4, 6))
    check("C: harmful_ratio=0.60 (at threshold) → NEGATIVE_CAUTION", bias3 == "NEGATIVE_CAUTION",
          f"got {bias3}")

    # 2 helpful, 4 harmful out of 6, conf=0.80
    # eff_signal = (2-4)/6 * 0.80 = -0.267 ≤ -0.20 → NEGATIVE (harmful_ratio=0.667 ≥ 0.60 → CAUTION)
    # Actually harmful_ratio=4/6=0.667 ≥ 0.60 → NEGATIVE_CAUTION
    mod4, bias4 = ac64._memory_modifier_from_rec(make_rec(2, 4))
    check("C: harmful_ratio=0.667 → NEGATIVE_CAUTION", bias4 == "NEGATIVE_CAUTION",
          f"got {bias4}")

    # Pure NEGATIVE (eff_signal between -0.50 and -0.20, harmful_ratio < 0.60)
    # 1 helpful, 4 harmful out of 10, conf=0.80
    # eff_signal = (1-4)/10 * 0.80 = -0.24 ≤ -0.20 AND harmful_ratio=0.40 < 0.60 → NEGATIVE
    mod5, bias5 = ac64._memory_modifier_from_rec(make_rec(1, 4, conf=0.80))
    # Actually eff_signal = (1-4)/5 * 0.80 - wait make_rec(1,4) gives 5 total
    # eff_signal = (1-4)/5 * 0.80 = -0.48 ≤ -0.50? No: -0.48 > -0.50
    # So it should be NEGATIVE (not CAUTION)
    check("C: eff_signal=-0.48 (>-0.50), harmful_ratio=0.80 ≥ 0.60 → actually CAUTION",
          bias5 == "NEGATIVE_CAUTION", f"got {bias5}")

    # Pure NEGATIVE: 1 helpful, 2 harmful out of 10 total, conf=0.80
    # net_signal = (1-2)/3 = -0.333; eff_signal = -0.333 * 0.80 = -0.267 ≤ -0.20
    # harmful_ratio = 2/3 = 0.667 ≥ 0.60 → CAUTION
    # Try: 2 helpful, 3 harmful out of 10, conf=0.80
    # eff_signal=(2-3)/5 * 0.80 = -0.16 > -0.20 AND harmful_ratio=0.60 → CAUTION
    # Need harmful_ratio < 0.60: 3 helpful, 3 harmful out of 10 = 0.30 < 0.60
    # eff_signal = 0/6 * 0.80 = 0 → NEUTRAL
    # 2 helpful, 3 harmful out of 10 with conf=0.80: hmm net_signal=(2-3)/5=-0.2, eff=-0.16
    # Let's try: 0 helpful, 2 harmful out of 10 total, conf=0.80
    # eff_signal = (0-2)/2 * 0.80 = -0.80, harmful_ratio=1.0 → CAUTION
    # Hard to get pure NEGATIVE without CAUTION harmful_ratio...
    # 1 helpful, 2 harmful in window of 10 total but only 3 labelled ones
    # Let's try window of 10: 2 helpful, 2 harmful, 6 neutral
    # But make_rec only creates helpful/harmful entries. Let me add neutral.
    def make_rec_neutral(helpful, harmful, neutral, conf=0.80):
        window = ([{"outcome_label": "HELPFUL"}] * helpful +
                  [{"outcome_label": "HARMFUL"}] * harmful +
                  [{"outcome_label": "NEUTRAL"}] * neutral)
        return {"memory_confidence": conf, "rolling_window": window}
    # 2 helpful, 3 harmful, 5 neutral out of 10
    # net_signal=(2-3)/10=-0.10, eff_signal=-0.10*0.80=-0.08, harmful_ratio=3/10=0.30 < 0.60 → NEUTRAL
    # 1 helpful, 4 harmful, 5 neutral out of 10
    # net_signal=(1-4)/10=-0.30, eff_signal=-0.30*0.80=-0.24 ≤ -0.20, harmful_ratio=0.40 < 0.60 → NEGATIVE
    mod6, bias6 = ac64._memory_modifier_from_rec(make_rec_neutral(1, 4, 5))
    check("C: eff_signal=-0.24, harmful_ratio=0.40 → NEGATIVE",
          bias6 == "NEGATIVE", f"got {bias6}")

    # POSITIVE: 8 helpful, 1 harmful, 1 neutral, conf=0.80
    # net_signal=(8-1)/10=0.70, eff_signal=0.70*0.80=0.56 ≥ 0.20 → POSITIVE
    mod7, bias7 = ac64._memory_modifier_from_rec(make_rec_neutral(8, 1, 1))
    check("C: eff_signal=0.56 → POSITIVE", bias7 == "POSITIVE", f"got {bias7}")

    # NEUTRAL: 5 helpful, 5 harmful, conf=0.80
    # eff_signal=0; harmful_ratio=0.50 < 0.60 → NEUTRAL
    mod8, bias8 = ac64._memory_modifier_from_rec(make_rec(5, 5))
    check("C: eff_signal=0, harmful_ratio=0.50 → NEUTRAL", bias8 == "NEUTRAL",
          f"got {bias8}")


# ---------------------------------------------------------------------------
# D. AC-66 usage watch bands match policy
# ---------------------------------------------------------------------------

def test_d_ac66_usage_watch():
    policy, _, _ = loader.load_policy()
    rev = policy["groups"]["review_thresholds"]

    check("D: AC66 MEMORY_APPLIED_RATE_LOW_WATCH == policy",
          ac66.MEMORY_APPLIED_RATE_LOW_WATCH == rev["review_memory_applied_rate_low_watch"],
          f"{ac66.MEMORY_APPLIED_RATE_LOW_WATCH} vs {rev['review_memory_applied_rate_low_watch']}")
    check("D: AC66 MEMORY_APPLIED_RATE_HIGH_WATCH == policy",
          ac66.MEMORY_APPLIED_RATE_HIGH_WATCH == rev["review_memory_applied_rate_high_watch"],
          f"{ac66.MEMORY_APPLIED_RATE_HIGH_WATCH} vs {rev['review_memory_applied_rate_high_watch']}")

    # Functional: assess_usage triggers WATCH for low rate
    low_metrics = {
        "memory_applied_rate": 0.05,
        "memory_available_count": 10,
        "total": 10,
        "memory_blocked_rate": 0.0,
    }
    assessment, reasons = ac66.assess_usage(low_metrics)
    check("D: applied=0.05 < 0.10 → WATCH", assessment == ac66.ASSESS_WATCH,
          f"got {assessment}")
    check("D: MEMORY_RARELY_APPLIED in reasons", "MEMORY_RARELY_APPLIED" in reasons)

    # High rate triggers WATCH
    high_metrics = {
        "memory_applied_rate": 0.90,
        "memory_available_count": 10,
        "total": 10,
        "memory_blocked_rate": 0.0,
    }
    assessment2, reasons2 = ac66.assess_usage(high_metrics)
    check("D: applied=0.90 > 0.80 → WATCH", assessment2 == ac66.ASSESS_WATCH,
          f"got {assessment2}")
    check("D: MEMORY_VERY_FREQUENTLY_APPLIED in reasons",
          "MEMORY_VERY_FREQUENTLY_APPLIED" in reasons2)

    # Normal range → HEALTHY
    normal_metrics = {
        "memory_applied_rate": 0.40,
        "memory_available_count": 10,
        "total": 10,
        "memory_blocked_rate": 0.0,
    }
    assessment3, _ = ac66.assess_usage(normal_metrics)
    check("D: applied=0.40 → HEALTHY", assessment3 == ac66.ASSESS_HEALTHY,
          f"got {assessment3}")

    # Boundary: exactly 0.10 → NOT WATCH (condition is < not <=)
    boundary_metrics = {
        "memory_applied_rate": 0.10,
        "memory_available_count": 10,
        "total": 10,
        "memory_blocked_rate": 0.0,
    }
    assessment4, _ = ac66.assess_usage(boundary_metrics)
    check("D: applied=0.10 (at boundary) → HEALTHY",
          assessment4 == ac66.ASSESS_HEALTHY, f"got {assessment4}")


# ---------------------------------------------------------------------------
# E. Loader fallback includes new keys at correct defaults
# ---------------------------------------------------------------------------

def test_e_fallback_includes_new_keys():
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "no_policy.json"
        policy, fb, _ = loader.load_policy(missing)
        mg  = policy["groups"]["memory_gate"]
        mw  = policy["groups"]["memory_rolling_window"]
        rev = policy["groups"]["review_thresholds"]

        check("E: fallback bias_caution_signal_threshold=-0.50",
              mg["bias_caution_signal_threshold"] == -0.50)
        check("E: fallback bias_caution_harmful_ratio=0.60",
              mg["bias_caution_harmful_ratio"] == 0.60)
        check("E: fallback bias_negative_signal_threshold=-0.20",
              mg["bias_negative_signal_threshold"] == -0.20)
        check("E: fallback bias_positive_signal_threshold=0.20",
              mg["bias_positive_signal_threshold"] == 0.20)
        check("E: fallback memory_min_confidence=0.40",
              mw["memory_min_confidence"] == 0.40)
        check("E: fallback sparse_window_threshold=3",
              mw["sparse_window_threshold"] == 3)
        check("E: fallback review_low_watch=0.10",
              rev["review_memory_applied_rate_low_watch"] == 0.10)
        check("E: fallback review_high_watch=0.80",
              rev["review_memory_applied_rate_high_watch"] == 0.80)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def main():
    test_a_new_keys_in_policy()
    test_b_ac63_constants()
    test_c_ac64_bias_thresholds()
    test_d_ac66_usage_watch()
    test_e_fallback_includes_new_keys()

    total = len(_PASS) + len(_FAIL)
    print(f"\nAC-69 results: {len(_PASS)}/{total} PASS")
    if _FAIL:
        for f in _FAIL:
            print(f"  {f}")
        sys.exit(1)
    else:
        print("  All tests PASS — AC69 policy surface expansion validated.")
        sys.exit(0)


if __name__ == "__main__":
    main()
