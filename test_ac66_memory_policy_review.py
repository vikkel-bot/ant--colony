"""
AC-66 tests: Memory Impact Review & Policy Tuning
Imports build_policy_review and core functions from
build_allocation_memory_policy_review_lite.py (AC-66).

Test scenarios:
  A. Insufficient review data — too few records → INSUFFICIENT_REVIEW_DATA
  B. Healthy policy — metrics all within bounds → KEEP_POLICY
  C. Positive policy too permissive → REVIEW_POSITIVE_GATE
  D. Negative policy too aggressive → REVIEW_NEGATIVE_SENSITIVITY
  E. Cooldown suspicious → REVIEW_COOLDOWN_LENGTH
  F. Conflict blocks too frequent → REVIEW_CONFLICT_POLICY
  G. Safe band violation → REVIEW_SAFE_BAND
  H. Confidence threshold issue → REVIEW_MEMORY_CONFIDENCE_THRESHOLD
  I. Summary consistency — counts, rates, status consistency
"""
import importlib.util
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Load production module
# ---------------------------------------------------------------------------

_MODULE_PATH = (
    Path(__file__).parent
    / "ant_colony"
    / "build_allocation_memory_policy_review_lite.py"
)
spec = importlib.util.spec_from_file_location("pr_mod", _MODULE_PATH)
pr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pr)


# ---------------------------------------------------------------------------
# Helpers — build synthetic AC-65 observability records
# ---------------------------------------------------------------------------

def make_obs_rec(
    strategy_key="BTC-EUR__EDGE4",
    impact_class="NO_EFFECT",
    memory_available=True,
    memory_modifier_applied=False,
    modifier_delta=0.0,
    confidence_delta=0.0,
    dq_score_delta=0.0,
    safe_band_ok=True,
    dq_gate_changed=False,
    cooldown_flag=False,
):
    return {
        "strategy_key":           strategy_key,
        "impact_class":           impact_class,
        "memory_available":       memory_available,
        "memory_modifier_applied": memory_modifier_applied,
        "modifier_delta":         modifier_delta,
        "confidence_delta":       confidence_delta,
        "dq_score_delta":         dq_score_delta,
        "safe_band_ok":           safe_band_ok,
        "dq_gate_changed":        dq_gate_changed,
        "cooldown_flag":          cooldown_flag,
    }


def _records(specs):
    """Build a list of obs records from a list of (impact_class, kwargs) tuples."""
    return [make_obs_rec(**kw) for kw in specs]


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
# A. Insufficient review data
# ===========================================================================

def test_a_insufficient_data():
    # 0 records
    review = pr.build_policy_review([])
    check("A: empty → INSUFFICIENT_REVIEW_DATA status",
          review["policy_status"] == "INSUFFICIENT_REVIEW_DATA",
          f"got {review['policy_status']}")
    rec_labels = [r["recommendation_label"] for r in review["recommendations"]]
    check("A: empty → INSUFFICIENT_REVIEW_DATA label",
          "INSUFFICIENT_REVIEW_DATA" in rec_labels)
    check("A: empty → only INSUFFICIENT rec",
          len(review["recommendations"]) == 1,
          f"got {rec_labels}")

    # 4 records (below MIN_REVIEWABLE_RECORDS=5)
    recs = [make_obs_rec(strategy_key=f"K{i}") for i in range(4)]
    review = pr.build_policy_review(recs)
    check("A: 4 recs → INSUFFICIENT_REVIEW_DATA",
          review["policy_status"] == "INSUFFICIENT_REVIEW_DATA",
          f"got {review['policy_status']}")
    check("A: 4 recs summary.records_total=4",
          review["summary"]["records_total"] == 4)
    check("A: 4 recs insufficient_count > 0",
          review["summary"]["insufficient_review_data_count"] > 0)

    # Exactly 5 records — should NOT be INSUFFICIENT
    recs = [make_obs_rec(strategy_key=f"K{i}") for i in range(5)]
    review = pr.build_policy_review(recs)
    check("A: 5 recs → not INSUFFICIENT",
          review["policy_status"] != "INSUFFICIENT_REVIEW_DATA",
          f"got {review['policy_status']}")


# ===========================================================================
# B. Healthy policy — all within bounds → KEEP_POLICY
# ===========================================================================

def test_b_healthy_policy():
    # 10 records: mix of neutral and small negative — no threshold crossed
    recs = (
        [make_obs_rec(impact_class="NO_EFFECT",           memory_available=True,  memory_modifier_applied=False, modifier_delta=0.0)] * 4 +
        [make_obs_rec(impact_class="NEGATIVE_DAMPENING",  memory_available=True,  memory_modifier_applied=True,  modifier_delta=-0.01, confidence_delta=-0.007, dq_score_delta=-0.003)] * 2 +
        [make_obs_rec(impact_class="NO_MEMORY",           memory_available=False, memory_modifier_applied=False, modifier_delta=0.0)] * 4
    )
    review = pr.build_policy_review(recs)

    check("B: KEEP_POLICY status",
          review["policy_status"] == "KEEP_POLICY",
          f"got {review['policy_status']}")
    rec_labels = [r["recommendation_label"] for r in review["recommendations"]]
    check("B: only KEEP_POLICY label",
          rec_labels == ["KEEP_POLICY"],
          f"got {rec_labels}")
    check("B: metrics total=10", review["metrics"]["total"] == 10)
    check("B: no safe band violations", review["metrics"]["safe_band_violations"] == 0)
    check("B: summary policy_status matches", review["summary"]["policy_status"] == "KEEP_POLICY")


# ===========================================================================
# C. Positive policy too permissive → REVIEW_POSITIVE_GATE
# ===========================================================================

def test_c_positive_policy_review():
    # positive_applied_rate > 0.30 triggers REVIEW_POSITIVE_GATE
    # 10 records: 4 POSITIVE_REINFORCEMENT (40%) + 6 NO_EFFECT
    recs = (
        [make_obs_rec(impact_class="POSITIVE_REINFORCEMENT", memory_available=True, memory_modifier_applied=True,  modifier_delta=0.02, confidence_delta=0.014)] * 4 +
        [make_obs_rec(impact_class="NO_EFFECT",              memory_available=True, memory_modifier_applied=False, modifier_delta=0.0)] * 6
    )
    review = pr.build_policy_review(recs)

    labels = [r["recommendation_label"] for r in review["recommendations"]]
    check("C: REVIEW_POSITIVE_GATE in recommendations",
          "REVIEW_POSITIVE_GATE" in labels,
          f"got {labels}")
    check("C: status REVIEW_REQUIRED or REVIEW_SUGGESTED",
          review["policy_status"] in ("REVIEW_REQUIRED", "REVIEW_SUGGESTED"),
          f"got {review['policy_status']}")
    check("C: positive_applied_rate > 0.30",
          review["metrics"]["positive_applied_rate"] > 0.30)

    # positive policy assessment should be REVIEW
    check("C: positive_policy assessment=REVIEW",
          review["assessments"]["positive_policy"] == "REVIEW",
          f"got {review['assessments']['positive_policy']}")


# ===========================================================================
# D. Negative policy too aggressive → REVIEW_NEGATIVE_SENSITIVITY
# ===========================================================================

def test_d_negative_policy_review():
    # neg+caution combined > 0.70
    # 10 records: 5 NEGATIVE_DAMPENING + 3 CAUTION_DAMPENING + 2 NO_EFFECT = 80%
    recs = (
        [make_obs_rec(impact_class="NEGATIVE_DAMPENING", memory_available=True, memory_modifier_applied=True, modifier_delta=-0.01)] * 5 +
        [make_obs_rec(impact_class="CAUTION_DAMPENING",  memory_available=True, memory_modifier_applied=True, modifier_delta=-0.02, cooldown_flag=True)] * 3 +
        [make_obs_rec(impact_class="NO_EFFECT",          memory_available=True, memory_modifier_applied=False, modifier_delta=0.0)] * 2
    )
    review = pr.build_policy_review(recs)

    labels = [r["recommendation_label"] for r in review["recommendations"]]
    check("D: REVIEW_NEGATIVE_SENSITIVITY in recommendations",
          "REVIEW_NEGATIVE_SENSITIVITY" in labels,
          f"got {labels}")
    combined = review["metrics"]["negative_applied_rate"] + review["metrics"]["caution_applied_rate"]
    check("D: combined neg+caution rate > 0.70", combined > 0.70, f"got {combined}")
    check("D: negative_policy assessment=REVIEW",
          review["assessments"]["negative_policy"] == "REVIEW",
          f"got {review['assessments']['negative_policy']}")


# ===========================================================================
# E. Cooldown suspicious → REVIEW_COOLDOWN_LENGTH
# ===========================================================================

def test_e_cooldown_review():
    # cooldown_seen_rate > 0.50
    # 10 records: 6 with cooldown_flag=True
    recs = (
        [make_obs_rec(impact_class="CAUTION_DAMPENING", memory_available=True, memory_modifier_applied=True, modifier_delta=-0.02, cooldown_flag=True)] * 6 +
        [make_obs_rec(impact_class="NO_EFFECT",         memory_available=True, memory_modifier_applied=False, modifier_delta=0.0)] * 4
    )
    review = pr.build_policy_review(recs)

    labels = [r["recommendation_label"] for r in review["recommendations"]]
    check("E: REVIEW_COOLDOWN_LENGTH in recommendations",
          "REVIEW_COOLDOWN_LENGTH" in labels,
          f"got {labels}")
    check("E: cooldown_rate > 0.50",
          review["metrics"]["cooldown_rate"] > 0.50,
          f"got {review['metrics']['cooldown_rate']}")
    check("E: cooldown_policy assessment=REVIEW",
          review["assessments"]["cooldown_policy"] == "REVIEW",
          f"got {review['assessments']['cooldown_policy']}")


# ===========================================================================
# F. Conflict blocks too frequent → REVIEW_CONFLICT_POLICY
# ===========================================================================

def test_f_conflict_review():
    # conflict_block_rate > 0.30
    # 10 records: 4 BLOCKED_BY_CONFLICT (40%) + 6 NO_EFFECT
    recs = (
        [make_obs_rec(impact_class="BLOCKED_BY_CONFLICT", memory_available=True, memory_modifier_applied=False, modifier_delta=0.0)] * 4 +
        [make_obs_rec(impact_class="NO_EFFECT",           memory_available=True, memory_modifier_applied=False, modifier_delta=0.0)] * 6
    )
    review = pr.build_policy_review(recs)

    labels = [r["recommendation_label"] for r in review["recommendations"]]
    check("F: REVIEW_CONFLICT_POLICY in recommendations",
          "REVIEW_CONFLICT_POLICY" in labels,
          f"got {labels}")
    check("F: conflict_block_rate > 0.30",
          review["metrics"]["conflict_block_rate"] > 0.30,
          f"got {review['metrics']['conflict_block_rate']}")


# ===========================================================================
# G. Safe band violation → REVIEW_SAFE_BAND (HIGH priority)
# ===========================================================================

def test_g_safe_band_violation():
    # One safe_band_ok=False record is enough
    recs = (
        [make_obs_rec(impact_class="NO_EFFECT", memory_available=True, memory_modifier_applied=False, modifier_delta=0.0)] * 7 +
        [make_obs_rec(impact_class="NEGATIVE_DAMPENING", memory_available=True, memory_modifier_applied=True,
                      modifier_delta=-0.01, safe_band_ok=False)] * 1 +
        [make_obs_rec(impact_class="NO_EFFECT")] * 2
    )
    review = pr.build_policy_review(recs)

    labels = [r["recommendation_label"] for r in review["recommendations"]]
    check("G: REVIEW_SAFE_BAND in recommendations",
          "REVIEW_SAFE_BAND" in labels,
          f"got {labels}")
    check("G: safe_band violations count >= 1",
          review["metrics"]["safe_band_violations"] >= 1)
    check("G: REVIEW_SAFE_BAND priority HIGH",
          any(r["priority"] == "HIGH" for r in review["recommendations"]
              if r["recommendation_label"] == "REVIEW_SAFE_BAND"))
    check("G: status REVIEW_REQUIRED",
          review["policy_status"] == "REVIEW_REQUIRED",
          f"got {review['policy_status']}")
    check("G: safety assessment=REVIEW",
          review["assessments"]["safety"] == "REVIEW",
          f"got {review['assessments']['safety']}")


# ===========================================================================
# H. Confidence threshold issue → REVIEW_MEMORY_CONFIDENCE_THRESHOLD
# ===========================================================================

def test_h_confidence_threshold_issue():
    # low_conf_blocked_rate_of_available > 0.50
    # 10 records: 8 memory_available, 5 BLOCKED_LOW_CONFIDENCE (5/8 = 0.625 > 0.50)
    recs = (
        [make_obs_rec(impact_class="BLOCKED_LOW_CONFIDENCE", memory_available=True, memory_modifier_applied=False, modifier_delta=0.0)] * 5 +
        [make_obs_rec(impact_class="NO_EFFECT",              memory_available=True, memory_modifier_applied=False, modifier_delta=0.0)] * 3 +
        [make_obs_rec(impact_class="NO_MEMORY",              memory_available=False, memory_modifier_applied=False, modifier_delta=0.0)] * 2
    )
    review = pr.build_policy_review(recs)

    labels = [r["recommendation_label"] for r in review["recommendations"]]
    check("H: REVIEW_MEMORY_CONFIDENCE_THRESHOLD in recommendations",
          "REVIEW_MEMORY_CONFIDENCE_THRESHOLD" in labels,
          f"got {labels}")
    lc_rate = review["metrics"]["low_conf_blocked_rate_of_available"]
    check("H: low_conf_blocked_rate_of_available > 0.50",
          lc_rate > 0.50,
          f"got {lc_rate}")
    check("H: low_conf_blocked_count=5",
          review["metrics"]["low_conf_blocked_count"] == 5,
          f"got {review['metrics']['low_conf_blocked_count']}")


# ===========================================================================
# I. Summary consistency
# ===========================================================================

def test_i_summary_consistency():
    # Build a controlled set to verify rates and counts are internally consistent
    recs = (
        [make_obs_rec(impact_class="POSITIVE_REINFORCEMENT", memory_available=True,  memory_modifier_applied=True,  modifier_delta=0.015, confidence_delta=0.01)] * 2 +
        [make_obs_rec(impact_class="NEGATIVE_DAMPENING",     memory_available=True,  memory_modifier_applied=True,  modifier_delta=-0.01, confidence_delta=-0.007)] * 3 +
        [make_obs_rec(impact_class="NO_EFFECT",              memory_available=True,  memory_modifier_applied=False, modifier_delta=0.0)] * 3 +
        [make_obs_rec(impact_class="NO_MEMORY",              memory_available=False, memory_modifier_applied=False, modifier_delta=0.0)] * 2
    )
    review = pr.build_policy_review(recs)
    m = review["metrics"]

    # Count consistency
    check("I: total=10", m["total"] == 10)
    check("I: memory_available_count=8", m["memory_available_count"] == 8, f"got {m['memory_available_count']}")
    check("I: memory_applied_count=5", m["memory_applied_count"] == 5, f"got {m['memory_applied_count']}")
    check("I: positive_applied_count=2", m["positive_applied_count"] == 2, f"got {m['positive_applied_count']}")
    check("I: negative_applied_count=3", m["negative_applied_count"] == 3, f"got {m['negative_applied_count']}")

    # Rate consistency
    check("I: memory_applied_rate=0.50", m["memory_applied_rate"] == 0.5, f"got {m['memory_applied_rate']}")
    check("I: positive_applied_rate=0.20", m["positive_applied_rate"] == 0.20, f"got {m['positive_applied_rate']}")
    check("I: negative_applied_rate=0.30", m["negative_applied_rate"] == 0.30, f"got {m['negative_applied_rate']}")

    # Summary mirrors metrics
    s = review["summary"]
    check("I: summary.records_total=10", s["records_total"] == 10)
    check("I: summary.memory_applied_rate consistent",
          s["memory_applied_rate"] == m["memory_applied_rate"])
    check("I: summary.positive_applied_rate consistent",
          s["positive_applied_rate"] == m["positive_applied_rate"])
    check("I: summary.policy_status == review.policy_status",
          s["policy_status"] == review["policy_status"])
    check("I: summary.policy_recommendation_count == len(recommendations)",
          s["policy_recommendation_count"] == len(review["recommendations"]))

    # policy_review sections present
    for section in ("usage_review", "safety_review", "positive_policy_review",
                    "negative_policy_review", "cooldown_review", "data_sufficiency_review"):
        check(f"I: policy_review has section {section}",
              section in review["policy_review"],
              f"missing {section}")

    # strategy_key_reviews
    skr = review["strategy_key_reviews"]
    check("I: strategy_key_reviews non-empty", len(skr) > 0)
    for entry in skr:
        for field in ("strategy_key", "records_count", "memory_applied_rate",
                      "avg_modifier_delta", "avg_confidence_delta",
                      "cooldown_seen", "dominant_impact_class", "strategy_policy_note"):
            check(f"I: strategy_key_reviews entry has {field}", field in entry, f"missing {field}")


# ===========================================================================
# J. Additional invariants
# ===========================================================================

def test_j_invariants():
    # J1. KEEP_POLICY when only one triggering threshold borderline (watch-only)
    # positive_applied_rate between 0.20 and 0.30 → WATCH, but no REVIEW label
    recs = (
        [make_obs_rec(impact_class="POSITIVE_REINFORCEMENT", memory_available=True, memory_modifier_applied=True, modifier_delta=0.02)] * 2 +
        [make_obs_rec(impact_class="NO_EFFECT",              memory_available=True, memory_modifier_applied=False, modifier_delta=0.0)] * 8
    )
    review = pr.build_policy_review(recs)
    pos_rate = review["metrics"]["positive_applied_rate"]
    check("J: positive_rate=0.20 (WATCH threshold)",
          pos_rate == 0.20, f"got {pos_rate}")
    check("J: WATCH-only → no REVIEW_POSITIVE_GATE label",
          "REVIEW_POSITIVE_GATE" not in [r["recommendation_label"] for r in review["recommendations"]])

    # J2. Multiple review triggers simultaneously → both labels present, status REVIEW_REQUIRED
    recs = (
        [make_obs_rec(impact_class="POSITIVE_REINFORCEMENT", memory_available=True, memory_modifier_applied=True,  modifier_delta=0.02)] * 4 +  # 40% > 30%
        [make_obs_rec(impact_class="NO_EFFECT", memory_available=True, memory_modifier_applied=False, modifier_delta=0.0, safe_band_ok=False)] * 6   # violations
    )
    review = pr.build_policy_review(recs)
    labels = [r["recommendation_label"] for r in review["recommendations"]]
    check("J: multiple triggers → REVIEW_POSITIVE_GATE present", "REVIEW_POSITIVE_GATE" in labels, f"got {labels}")
    check("J: multiple triggers → REVIEW_SAFE_BAND present", "REVIEW_SAFE_BAND" in labels, f"got {labels}")
    check("J: multiple triggers → REVIEW_REQUIRED", review["policy_status"] == "REVIEW_REQUIRED", f"got {review['policy_status']}")

    # J3. build_policy_review on empty list — no crash, returns dict with expected keys
    review = pr.build_policy_review([])
    for key in ("metrics", "assessments", "summary", "policy_review", "recommendations", "strategy_key_reviews", "policy_status"):
        check(f"J: empty list result has key {key}", key in review)

    # J4. compute_policy_status: KEEP_POLICY when all recommendations are KEEP_POLICY
    recs_keep = [{"recommendation_label": "KEEP_POLICY", "priority": "LOW"}]
    check("J: compute_policy_status KEEP_POLICY",
          pr.compute_policy_status(recs_keep) == "KEEP_POLICY")

    # J5. compute_policy_status: REVIEW_REQUIRED for HIGH priority
    recs_high = [{"recommendation_label": "REVIEW_SAFE_BAND", "priority": "HIGH"}]
    check("J: compute_policy_status REVIEW_REQUIRED",
          pr.compute_policy_status(recs_high) == "REVIEW_REQUIRED")

    # J6. compute_policy_status: REVIEW_SUGGESTED for LOW priority
    recs_low = [{"recommendation_label": "REVIEW_CONFLICT_POLICY", "priority": "LOW"}]
    check("J: compute_policy_status REVIEW_SUGGESTED",
          pr.compute_policy_status(recs_low) == "REVIEW_SUGGESTED")

    # J7. VERSION is memory_policy_review_v1
    check("J: VERSION is memory_policy_review_v1",
          pr.VERSION == "memory_policy_review_v1",
          f"got {pr.VERSION}")

    # J8. load_json missing file → default value, no crash
    from pathlib import Path
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "does_not_exist.json"
        result = pr.load_json(missing, {"records": []})
        check("J: load_json missing file → default", result == {"records": []})

    # J9. assess_data_sufficiency with exactly MIN_REVIEWABLE_RECORDS
    metrics = pr.compute_review_metrics([make_obs_rec() for _ in range(pr.MIN_REVIEWABLE_RECORDS)])
    label, _ = pr.assess_data_sufficiency(metrics)
    check("J: MIN_REVIEWABLE_RECORDS → HEALTHY sufficiency",
          label == pr.ASSESS_HEALTHY, f"got {label}")

    # J10. compute_review_metrics empty list → all-zero dict, no crash
    m = pr.compute_review_metrics([])
    check("J: empty metrics total=0", m["total"] == 0)
    check("J: empty metrics memory_applied_rate=0.0", m["memory_applied_rate"] == 0.0)


# ===========================================================================
# Run all tests
# ===========================================================================

def main():
    test_a_insufficient_data()
    test_b_healthy_policy()
    test_c_positive_policy_review()
    test_d_negative_policy_review()
    test_e_cooldown_review()
    test_f_conflict_review()
    test_g_safe_band_violation()
    test_h_confidence_threshold_issue()
    test_i_summary_consistency()
    test_j_invariants()

    total = len(_PASS) + len(_FAIL)
    print(f"\nAC-66 results: {len(_PASS)}/{total} PASS")

    if _FAIL:
        for f in _FAIL:
            print(f"  {f}")
        sys.exit(1)
    else:
        print("  All tests PASS — AC66 memory policy review validated.")
        sys.exit(0)


if __name__ == "__main__":
    main()
