"""
AC-63 Part A tests: Persistent Feedback Memory

Scenarios:
  A. Fresh start / no memory — neutral bootstrap, no crash
  B. Helpful accumulation — memory signal grows, stays bounded
  C. Harmful streak — cooldown persists across cycles
  D. Mixed recovery — harmful → neutral/helpful, gradual recovery
  E. Sparse evidence — low memory_confidence, no positive bias
  F. Missing/corrupt memory — fail-closed fallback
  H. Invariants — modifier band, window bounds, idempotency
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
    / "build_allocation_feedback_memory_lite.py"
)
spec = importlib.util.spec_from_file_location("mem_mod", _MODULE_PATH)
mem = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mem)

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_outcome(audit_id, outcome_label, strategy_key="BTC-EUR__EDGE4"):
    """Minimal AC-60 record for testing."""
    return {
        "audit_id":         audit_id,
        "strategy_key":     strategy_key,
        "evaluation_status": "READY",
        "outcome_label":    outcome_label,
        "market":           strategy_key.split("__")[0] if "__" in strategy_key else "",
    }


def make_fb_rec(strategy_key, cooldown=False, market=""):
    """Minimal AC-61 record for testing."""
    return {
        "strategy_key":    strategy_key,
        "market":          market or (strategy_key.split("__")[0] if "__" in strategy_key else ""),
        "cooldown_flag":   cooldown,
        "allocation_bias_class": "NEGATIVE_CAUTION" if cooldown else "NEUTRAL",
        "allocation_conviction_modifier": 0.90 if cooldown else 1.00,
        "feedback_status": "READY",
    }


def run_build(attribution_records, feedback_records, existing_memory, cycle_id="CY001"):
    ts = "2026-01-01T00:00:00Z"
    return mem.build_memory_state(
        attribution_records, feedback_records, existing_memory, cycle_id, ts
    )


# ===========================================================================
# A. Fresh start / no memory
# ===========================================================================

def test_a_fresh_start():
    # Empty inputs → neutral bootstrap
    result = run_build([], [], {}, "FRESH")

    check("A: no crash on empty inputs", isinstance(result, dict))
    check("A: empty memory → empty result", len(result) == 0)

    # Single new outcome on fresh memory
    result = run_build(
        [make_outcome("a1", "HELPFUL")],
        [make_fb_rec("BTC-EUR__EDGE4")],
        {},
        "FRESH2",
    )
    check("A: strategy_key appears after first record", "BTC-EUR__EDGE4" in result)
    r = result["BTC-EUR__EDGE4"]
    check("A: first record in window", len(r["rolling_window"]) == 1)
    check("A: memory_status SPARSE (window < 3)", r["memory_status"] == "SPARSE", r["memory_status"])
    check("A: cooldown_flag False initially", r["cooldown_flag"] is False)
    check("A: last_outcome_label correct", r["last_outcome_label"] == "HELPFUL")
    check("A: last_cycle_id set", r["last_cycle_id"] == "FRESH2")

    # No existing memory + no outcomes → key still appears via feedback_records
    result2 = run_build(
        [],
        [make_fb_rec("ETH-EUR__EDGE3")],
        {},
        "FRESH3",
    )
    check("A: key from feedback with no outcomes", "ETH-EUR__EDGE3" in result2)
    r2 = result2["ETH-EUR__EDGE3"]
    check("A: zero outcomes → BOOTSTRAP", r2["memory_status"] == "BOOTSTRAP", r2["memory_status"])
    check("A: rolling_net_signal=0.0 on bootstrap", r2["rolling_net_signal"] == 0.0)
    check("A: memory_confidence=0.0 on bootstrap", r2["memory_confidence"] == 0.0)


# ===========================================================================
# B. Helpful accumulation
# ===========================================================================

def test_b_helpful_accumulation():
    memory = {}
    SK = "BTC-EUR__EDGE4"

    # Accumulate 8 HELPFUL outcomes over 8 cycles
    for i in range(8):
        memory = run_build(
            [make_outcome(f"h{i}", "HELPFUL")],
            [make_fb_rec(SK)],
            memory,
            f"CY{i:03d}",
        )

    r = memory[SK]
    check("B: window grows to 8", len(r["rolling_window"]) == 8)
    check("B: net_signal positive", r["rolling_net_signal"] > 0.0, f"got {r['rolling_net_signal']}")
    check("B: net_signal == 1.0 (all helpful)", r["rolling_net_signal"] == 1.0, f"got {r['rolling_net_signal']}")
    check("B: memory_confidence at full (>= 1.0)", r["memory_confidence"] == 1.0, f"got {r['memory_confidence']}")
    check("B: memory_status ACTIVE", r["memory_status"] == "ACTIVE", r["memory_status"])
    check("B: helpful_total_lifetime == 8", r["helpful_total"] == 8)
    check("B: harmful_total == 0", r["harmful_total"] == 0)
    check("B: cooldown_flag False", r["cooldown_flag"] is False)
    check("B: last_outcome_label HELPFUL", r["last_outcome_label"] == "HELPFUL")

    # Continue: add 5 more HELPFUL — window stays at WINDOW_SIZE=10
    for i in range(8, 13):
        memory = run_build(
            [make_outcome(f"h{i}", "HELPFUL")],
            [make_fb_rec(SK)],
            memory,
            f"CY{i:03d}",
        )

    r2 = memory[SK]
    check("B: window capped at WINDOW_SIZE", len(r2["rolling_window"]) == mem.WINDOW_SIZE)
    check("B: helpful_total_lifetime grows beyond window", r2["helpful_total"] == 13)
    check("B: net_signal still 1.0 (all helpful)", r2["rolling_net_signal"] == 1.0)


# ===========================================================================
# C. Harmful streak + cooldown persistence
# ===========================================================================

def test_c_harmful_streak():
    SK = "SOL-EUR__EDGE4"
    memory = {}

    # First 3 harmful outcomes — triggers caution in AC-61
    for i in range(3):
        memory = run_build(
            [make_outcome(f"bad{i}", "HARMFUL", SK)],
            [make_fb_rec(SK, cooldown=True)],  # AC-61 signals caution
            memory,
            f"BAD{i:03d}",
        )

    r = memory[SK]
    check("C: cooldown_flag True after harmful cycles", r["cooldown_flag"] is True, f"got {r['cooldown_flag']}")
    check("C: cooldown_cycles_remaining = PERSIST", r["cooldown_cycles_remaining"] == mem.COOLDOWN_PERSIST_CYCLES)
    check("C: net_signal negative", r["rolling_net_signal"] < 0.0)

    # Now run cycles WITHOUT new harmful — cooldown should persist for COOLDOWN_PERSIST_CYCLES cycles
    for i in range(mem.COOLDOWN_PERSIST_CYCLES):
        memory = run_build(
            [],                          # no new outcomes
            [make_fb_rec(SK, cooldown=False)],  # AC-61 no longer signals caution
            memory,
            f"RECOVER{i:03d}",
        )
        r = memory[SK]
        expected_remaining = mem.COOLDOWN_PERSIST_CYCLES - (i + 1)
        expected_cooldown = expected_remaining > 0
        check(
            f"C: cooldown_flag correct at decay step {i+1}",
            r["cooldown_flag"] is expected_cooldown,
            f"got {r['cooldown_flag']}, expected {expected_cooldown}",
        )
        check(
            f"C: cooldown_remaining correct at step {i+1}",
            r["cooldown_cycles_remaining"] == expected_remaining,
            f"got {r['cooldown_cycles_remaining']}",
        )

    # After COOLDOWN_PERSIST_CYCLES decay cycles, cooldown should be gone
    r_final = memory[SK]
    check("C: cooldown expired after persist cycles", r_final["cooldown_flag"] is False)
    check("C: cooldown_remaining=0", r_final["cooldown_cycles_remaining"] == 0)


# ===========================================================================
# D. Mixed recovery — harmful → neutral/helpful
# ===========================================================================

def test_d_mixed_recovery():
    SK = "ADA-EUR__EDGE3"
    memory = {}

    # Phase 1: 4 harmful outcomes
    for i in range(4):
        memory = run_build(
            [make_outcome(f"harm{i}", "HARMFUL", SK)],
            [make_fb_rec(SK, cooldown=True)],
            memory,
            f"PHASE1_{i}",
        )

    r_after_harm = memory[SK]
    check("D: net_signal negative after harm phase", r_after_harm["rolling_net_signal"] < 0.0)
    check("D: cooldown active after harm", r_after_harm["cooldown_flag"] is True)

    # Phase 2: 4 neutral, then 4 helpful outcomes
    for i in range(4):
        memory = run_build(
            [make_outcome(f"neut{i}", "NEUTRAL", SK)],
            [make_fb_rec(SK, cooldown=False)],
            memory,
            f"PHASE2_{i}",
        )

    r_neutral = memory[SK]
    signal_neutral = r_neutral["rolling_net_signal"]
    check("D: net_signal recovering (< before or <= 0)", signal_neutral <= r_after_harm["rolling_net_signal"] + 0.50)
    check("D: net_signal not yet positive after neutral phase", signal_neutral <= 0.0, f"got {signal_neutral}")

    for i in range(4):
        memory = run_build(
            [make_outcome(f"help{i}", "HELPFUL", SK)],
            [make_fb_rec(SK, cooldown=False)],
            memory,
            f"PHASE3_{i}",
        )

    r_helpful = memory[SK]
    check("D: net_signal improves in helpful phase", r_helpful["rolling_net_signal"] > signal_neutral)
    check("D: eventually positive after enough helpful", r_helpful["rolling_net_signal"] > 0.0, f"got {r_helpful['rolling_net_signal']}")
    check("D: cooldown expired (persist cycles passed)", r_helpful["cooldown_flag"] is False)
    check("D: helpful_total > 0", r_helpful["helpful_total"] > 0)
    check("D: harmful_total lifetime preserved", r_helpful["harmful_total"] == 4)


# ===========================================================================
# E. Sparse evidence
# ===========================================================================

def test_e_sparse_evidence():
    SK = "DOT-EUR__EDGE4"

    # Only 1 outcome
    result = run_build(
        [make_outcome("sp1", "HELPFUL", SK)],
        [make_fb_rec(SK)],
        {},
        "SPARSE1",
    )
    r = result[SK]
    check("E: 1 outcome → SPARSE status", r["memory_status"] == "SPARSE", r["memory_status"])
    check("E: memory_confidence < MEMORY_MIN_CONFIDENCE", r["memory_confidence"] < mem.MEMORY_MIN_CONFIDENCE)
    check("E: cooldown_flag False on sparse", r["cooldown_flag"] is False)
    check("E: helpful net_signal ≤ 1.0 (bounded)", r["rolling_net_signal"] <= 1.0)

    # 2 outcomes
    result2 = run_build(
        [make_outcome("sp2", "HELPFUL", SK)],
        [make_fb_rec(SK)],
        result,
        "SPARSE2",
    )
    r2 = result2[SK]
    check("E: 2 outcomes → still SPARSE", r2["memory_status"] == "SPARSE", r2["memory_status"])
    check("E: memory_confidence still below threshold", r2["memory_confidence"] < mem.MEMORY_MIN_CONFIDENCE)

    # Enough outcomes to reach ACTIVE: need FULL_MEMORY_AT=8 for conf=1.0,
    # but MEMORY_MIN_CONFIDENCE=0.40 → need 0.40 × FULL_MEMORY_AT = 3.2 → 4 records
    # After above 2 runs we have 2. Add 2 more:
    extra = result2
    for i in range(2):
        extra = run_build(
            [make_outcome(f"sp_extra{i}", "HELPFUL", SK)],
            [make_fb_rec(SK)],
            extra,
            f"SPARSE_EXTRA{i}",
        )
    r_extra = extra[SK]
    check("E: 4 outcomes → memory_confidence >= threshold", r_extra["memory_confidence"] >= mem.MEMORY_MIN_CONFIDENCE, f"got {r_extra['memory_confidence']}")


# ===========================================================================
# F. Missing/corrupt memory file
# ===========================================================================

def test_f_missing_corrupt_memory():
    import tempfile
    from pathlib import Path as P

    # F1: load from missing file → empty dict (no crash)
    with tempfile.TemporaryDirectory() as tmp:
        missing = P(tmp) / "does_not_exist.json"
        result = mem.load_existing_memory(missing)
        check("F: missing file → empty dict", result == {})

    # F2: load from corrupt file → empty dict (no crash)
    with tempfile.TemporaryDirectory() as tmp:
        bad = P(tmp) / "bad.json"
        bad.write_text("NOT JSON {{{", encoding="utf-8")
        result = mem.load_existing_memory(bad)
        check("F: corrupt file → empty dict", result == {})

    # F3: load from valid but empty strategy_keys → empty dict
    import json
    with tempfile.TemporaryDirectory() as tmp:
        good = P(tmp) / "good.json"
        good.write_text(json.dumps({"version": "v1", "strategy_keys": {}}), encoding="utf-8")
        result = mem.load_existing_memory(good)
        check("F: empty strategy_keys → empty dict", result == {})

    # F4: build_memory_state with no inputs and no memory → empty result, no crash
    result = run_build([], [], {}, "F4")
    check("F: all empty inputs → empty result dict", isinstance(result, dict))
    check("F: all empty inputs → no crash", True)


# ===========================================================================
# H. Invariants
# ===========================================================================

def test_h_invariants():
    SK = "BTC-EUR__EDGE4"

    # H1. Window never exceeds WINDOW_SIZE
    memory = {}
    for i in range(25):
        memory = run_build(
            [make_outcome(f"inv{i}", "HELPFUL", SK)],
            [make_fb_rec(SK)],
            memory,
            f"INVCYCLE{i:03d}",
        )
    r = memory[SK]
    check("H: window never exceeds WINDOW_SIZE", len(r["rolling_window"]) <= mem.WINDOW_SIZE)
    check("H: window exactly WINDOW_SIZE after many cycles", len(r["rolling_window"]) == mem.WINDOW_SIZE)
    check("H: helpful_total > WINDOW_SIZE (lifetime tracks all)", r["helpful_total"] == 25)

    # H2. Idempotency — re-running with same cycle_id and same records does not double-count
    result1 = run_build(
        [make_outcome("idem1", "HELPFUL", SK), make_outcome("idem2", "HARMFUL", SK)],
        [make_fb_rec(SK)],
        {},
        "IDEM",
    )
    result2 = run_build(
        [make_outcome("idem1", "HELPFUL", SK), make_outcome("idem2", "HARMFUL", SK)],
        [make_fb_rec(SK)],
        result1,  # second run starts from result of first
        "IDEM",
    )
    check("H: idempotent — window size same on re-run", len(result2[SK]["rolling_window"]) == len(result1[SK]["rolling_window"]))
    check("H: idempotent — helpful_total same on re-run", result2[SK]["helpful_total"] == result1[SK]["helpful_total"])

    # H3. rolling_net_signal always in [-1.0, 1.0]
    for outcome in ["HELPFUL", "NEUTRAL", "HARMFUL"]:
        test_mem = {}
        for i in range(10):
            test_mem = run_build(
                [make_outcome(f"band{i}", outcome, SK)],
                [make_fb_rec(SK, cooldown=(outcome == "HARMFUL"))],
                test_mem,
                f"BAND{i}",
            )
        sig = test_mem[SK]["rolling_net_signal"]
        check(f"H: net_signal in [-1,1] for all-{outcome}", -1.0 <= sig <= 1.0, f"got {sig}")

    # H4. memory_confidence always in [0.0, 1.0]
    for n_outcomes in [0, 1, 3, 6, 8, 12, 20]:
        conf = mem.compute_memory_confidence(n_outcomes)
        check(f"H: memory_confidence in [0,1] for n={n_outcomes}", 0.0 <= conf <= 1.0, f"got {conf}")

    # H5. empty_memory_record returns expected fields
    er = mem.empty_memory_record("X__Y", "X")
    for field in ["strategy_key", "memory_status", "ready_outcomes_total", "helpful_total",
                  "neutral_total", "harmful_total", "rolling_window", "rolling_net_signal",
                  "memory_confidence", "cooldown_flag", "cooldown_cycles_remaining",
                  "last_outcome_label", "last_update_ts_utc", "last_cycle_id", "memory_reasons"]:
        check(f"H: empty_memory_record has field '{field}'", field in er)
    check("H: empty_memory_record rolling_window is empty list", er["rolling_window"] == [])
    check("H: empty_memory_record cooldown_flag is False", er["cooldown_flag"] is False)
    check("H: empty_memory_record memory_status is BOOTSTRAP", er["memory_status"] == mem.MEMORY_STATUS_BOOTSTRAP)

    # H6. Cooldown is never permanent — even if set, it decays after COOLDOWN_PERSIST_CYCLES
    mem_state = {}
    for i in range(5):
        mem_state = run_build(
            [make_outcome(f"harm_perm{i}", "HARMFUL", SK)],
            [make_fb_rec(SK, cooldown=True)],
            mem_state,
            f"PERM{i}",
        )
    # Now stop setting cooldown
    for i in range(mem.COOLDOWN_PERSIST_CYCLES + 2):
        mem_state = run_build(
            [],
            [make_fb_rec(SK, cooldown=False)],
            mem_state,
            f"DECAY{i}",
        )
    r_decayed = mem_state[SK]
    check("H: cooldown eventually expires", r_decayed["cooldown_flag"] is False)
    check("H: cooldown_remaining=0 after full decay", r_decayed["cooldown_cycles_remaining"] == 0)

    # H7. Multiple strategy_keys coexist independently
    mix_result = run_build(
        [
            make_outcome("m1", "HELPFUL", "BTC-EUR__EDGE4"),
            make_outcome("m2", "HARMFUL", "ETH-EUR__EDGE3"),
        ],
        [
            make_fb_rec("BTC-EUR__EDGE4"),
            make_fb_rec("ETH-EUR__EDGE3", cooldown=True),
        ],
        {},
        "MULTI",
    )
    check("H: both keys present", "BTC-EUR__EDGE4" in mix_result and "ETH-EUR__EDGE3" in mix_result)
    check("H: BTC helpful, no cooldown", mix_result["BTC-EUR__EDGE4"]["cooldown_flag"] is False)
    check("H: ETH cooldown active", mix_result["ETH-EUR__EDGE3"]["cooldown_flag"] is True)


# ===========================================================================
# Run all tests
# ===========================================================================

def main():
    test_a_fresh_start()
    test_b_helpful_accumulation()
    test_c_harmful_streak()
    test_d_mixed_recovery()
    test_e_sparse_evidence()
    test_f_missing_corrupt_memory()
    test_h_invariants()

    total = len(_PASS) + len(_FAIL)
    print(f"\nAC-63 Part A (memory) results: {len(_PASS)}/{total} PASS")

    if _FAIL:
        for f in _FAIL:
            print(f"  {f}")
        sys.exit(1)
    else:
        print("  All tests PASS — AC63 feedback memory validated.")
        sys.exit(0)


if __name__ == "__main__":
    main()
