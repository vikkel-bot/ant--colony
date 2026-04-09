"""
AC-64 tests: Memory-Aware Conviction Integration

Scenarios:
  A. No memory — exact cycle modifier, no memory effect
  B. Low confidence memory — gate blocked, no memory effect
  C. Negative caution memory — cooldown/caution gates open, extra demping visible
  D. Positive aligned memory — all positive gates pass, small positive correction
  E. Positive but insufficient evidence — gate blocks positive effect
  F. Cycle-memory conflict — cycle positive, memory negative → no positive escalation
  G. Recovery case — harmful streak fades, straf dooft geleidelijk uit
  H. Invariants — final modifier in band, fallback deterministic, AC-62 intact
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
    / "build_allocation_decision_quality_lite.py"
)
spec = importlib.util.spec_from_file_location("dq_mod", _MODULE_PATH)
dq = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dq)

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

def make_memory_rec(
    strategy_key,
    window_labels,
    cooldown_flag=False,
    cooldown_remaining=0,
    override_confidence=None,
):
    """Build minimal AC-63 memory record."""
    n = len(window_labels)
    conf = round(min(1.0, n / 8), 4) if override_confidence is None else override_confidence
    window = [
        {"audit_id": f"t{i}", "cycle_id": "CY0", "outcome_label": lbl}
        for i, lbl in enumerate(window_labels)
    ]
    return {
        "strategy_key":              strategy_key,
        "memory_confidence":         conf,
        "rolling_window":            window,
        "cooldown_flag":             cooldown_flag,
        "cooldown_cycles_remaining": cooldown_remaining,
        "memory_status":             "ACTIVE" if conf >= 0.40 else "SPARSE",
    }


def gate(cycle_mod, cycle_bias, mem_rec):
    return dq.apply_memory_gate(cycle_mod, cycle_bias, mem_rec)


def mem_mod(mem_rec):
    return dq._memory_modifier_from_rec(mem_rec)


# ===========================================================================
# A. No memory
# ===========================================================================

def test_a_no_memory():
    """No memory record → exact cycle modifier, gate blocked."""
    for cycle_mod in [1.00, 0.95, 1.05, 0.90]:
        final, passed, gate_name, applied, reason = gate(cycle_mod, "NEUTRAL", None)
        check(f"A: no memory → final == cycle_mod ({cycle_mod})", final == cycle_mod, f"got {final}")
        check(f"A: no memory → gate_passed=False ({cycle_mod})", passed is False)
        check(f"A: no memory → applied=False ({cycle_mod})", applied is False)
        check(f"A: no memory → gate=MEMORY_ABSENT ({cycle_mod})", gate_name == "MEMORY_ABSENT", gate_name)

    # load_memory_index with no records → empty dict (fail-closed)
    import tempfile, json
    from pathlib import Path as P
    with tempfile.TemporaryDirectory() as tmp:
        missing = P(tmp) / "no_file.json"
        idx = dq.load_memory_index(missing)
        check("A: missing memory file → empty dict", idx == {})

    with tempfile.TemporaryDirectory() as tmp:
        bad = P(tmp) / "bad.json"
        bad.write_text("{NOT JSON", encoding="utf-8")
        idx = dq.load_memory_index(bad)
        check("A: corrupt memory file → empty dict", idx == {})


# ===========================================================================
# B. Low confidence memory
# ===========================================================================

def test_b_low_confidence():
    """Memory present but confidence < MEMORY_CONF_GATE_NEG → no effect."""
    SK = "BTC-EUR__EDGE4"

    # Confidence below MEMORY_CONF_GATE_NEG = 0.50
    low_conf_cases = [0.0, 0.10, 0.30, 0.49]
    for conf in low_conf_cases:
        # Even harmful window, but confidence too low
        rec = make_memory_rec(SK, ["HARMFUL"] * 4, override_confidence=conf)
        cycle_mod = 1.00
        final, passed, gate_name, applied, reason = gate(cycle_mod, "NEUTRAL", rec)
        check(f"B: low conf={conf} → gate_passed=False", passed is False, gate_name)
        check(f"B: low conf={conf} → final==cycle_mod", final == cycle_mod, f"got {final}")
        check(f"B: low conf={conf} → gate=MEMORY_CONF_TOO_LOW", gate_name == "MEMORY_CONF_TOO_LOW", gate_name)

    # Confidence exactly at threshold → should pass (>= MEMORY_CONF_GATE_NEG=0.50)
    rec_at = make_memory_rec(SK, ["HARMFUL"] * 8, override_confidence=0.50)
    final_at, passed_at, gate_at, _, _ = gate(1.00, "NEUTRAL", rec_at)
    check("B: confidence exactly 0.50 → gate passes (negative)", passed_at is True, gate_at)
    check("B: negative memory at threshold → dampens", final_at <= 1.00, f"got {final_at}")


# ===========================================================================
# C. Negative caution memory — cooldown and caution demping
# ===========================================================================

def test_c_negative_caution():
    SK = "SOL-EUR__EDGE4"

    # C1. Cooldown active — gate opens even if memory bias is borderline
    rec_cooldown = make_memory_rec(
        SK, ["HARMFUL"] * 8,
        cooldown_flag=True, cooldown_remaining=2,
        override_confidence=0.60,
    )
    cycle_mod = 1.00
    final_cd, passed_cd, gate_cd, applied_cd, reason_cd = gate(cycle_mod, "NEUTRAL", rec_cooldown)
    check("C1: cooldown → gate COOLDOWN_GATE_OPEN", gate_cd == "COOLDOWN_GATE_OPEN", gate_cd)
    check("C1: cooldown → gate_passed=True", passed_cd is True)
    check("C1: cooldown → applied=True", applied_cd is True)
    check("C1: cooldown → final < cycle_mod (damped)", final_cd < cycle_mod, f"final={final_cd}")
    check("C1: cooldown → final in [0.90, 1.05]", dq.MODIFIER_MIN <= final_cd <= dq.MODIFIER_MAX)

    # C2. Strong harmful memory — NEGATIVE_CAUTION bias, no cooldown flag but signal is caution
    rec_caution = make_memory_rec(SK, ["HARMFUL"] * 8, override_confidence=0.75)
    mem_val, mem_bias = mem_mod(rec_caution)
    check("C2: all-harmful window → memory bias NEGATIVE_CAUTION", mem_bias == "NEGATIVE_CAUTION", mem_bias)
    check("C2: all-harmful window → memory modifier=0.90", mem_val == dq.MODIFIER_CAUTION, f"got {mem_val}")

    final_c2, passed_c2, gate_c2, _, _ = gate(1.00, "NEUTRAL", rec_caution)
    check("C2: caution signal → gate NEGATIVE_GATE_OPEN", gate_c2 == "NEGATIVE_GATE_OPEN", gate_c2)
    check("C2: caution signal → damped", final_c2 < 1.00, f"got {final_c2}")

    # Correction should be bounded by MEMORY_MAX_CORR_NEG
    max_negative_final = dq.MODIFIER_NEUTRAL - dq.MEMORY_MAX_CORR_NEG
    check("C2: correction capped at MEMORY_MAX_CORR_NEG", final_c2 >= max_negative_final, f"got {final_c2}")

    # C3. Negative memory on top of already-negative cycle modifier
    # cycle_modifier=0.90, memory also negative → final stays at MODIFIER_MIN
    rec_neg = make_memory_rec(SK, ["HARMFUL"] * 8, override_confidence=0.70)
    final_c3, passed_c3, _, _, _ = gate(0.90, "NEGATIVE_CAUTION", rec_neg)
    check("C3: already at floor → final stays at MODIFIER_MIN", final_c3 == dq.MODIFIER_MIN, f"got {final_c3}")

    # C4. Cycle positive, memory caution → memory dampens (not cycle's positive wins)
    final_c4, passed_c4, gate_c4, _, _ = gate(1.05, "POSITIVE", rec_caution)
    check("C4: cycle positive + memory caution → damped below cycle", final_c4 < 1.05, f"got {final_c4}")
    check("C4: final still in band", dq.MODIFIER_MIN <= final_c4 <= dq.MODIFIER_MAX)


# ===========================================================================
# D. Positive aligned memory
# ===========================================================================

def test_d_positive_aligned():
    SK = "BTC-EUR__EDGE4"

    # Build a high-confidence positive memory record with no recent harmful
    rec = make_memory_rec(SK, ["HELPFUL"] * 8, override_confidence=0.80)
    mem_val, mem_bias = mem_mod(rec)
    check("D: all-helpful → POSITIVE bias", mem_bias == "POSITIVE", mem_bias)
    check("D: all-helpful → POSITIVE modifier", mem_val == dq.MODIFIER_POSITIVE, f"got {mem_val}")

    # Aligned cycle (NEUTRAL or POSITIVE) + positive memory → should pass
    for cycle_mod, cycle_bias in [(1.00, "NEUTRAL"), (1.05, "POSITIVE")]:
        final, passed, gate_name, applied, reason = gate(cycle_mod, cycle_bias, rec)
        check(f"D: aligned ({cycle_bias}) + positive memory → POSITIVE_GATE_OPEN", gate_name == "POSITIVE_GATE_OPEN", gate_name)
        check(f"D: aligned ({cycle_bias}) + positive memory → gate_passed=True", passed is True)
        check(f"D: aligned ({cycle_bias}) + positive memory → final >= cycle_mod", final >= cycle_mod, f"got {final}")
        check(f"D: aligned ({cycle_bias}) + positive memory → within band", dq.MODIFIER_MIN <= final <= dq.MODIFIER_MAX)
        # Correction should be small (bounded by MEMORY_MAX_CORR_POS)
        max_positive_correction = dq.MEMORY_MAX_CORR_POS
        check(
            f"D: positive correction bounded by MEMORY_MAX_CORR_POS ({cycle_bias})",
            final <= cycle_mod + max_positive_correction + 1e-9,
            f"final={final} cycle={cycle_mod} max_corr={max_positive_correction}",
        )

    # Correction is smaller than negative correction (asymmetry)
    # For same memory signal: positive correction ≤ negative correction in abs
    rec_neg = make_memory_rec("K", ["HARMFUL"] * 8, override_confidence=0.80)
    pos_final, _, _, _, _ = gate(1.00, "NEUTRAL", rec)
    neg_final, _, _, _, _ = gate(1.00, "NEUTRAL", rec_neg)
    pos_corr = pos_final - 1.00
    neg_corr = abs(1.00 - neg_final)
    check("D: positive correction <= negative correction in abs (asymmetry)", pos_corr <= neg_corr, f"pos={pos_corr:.4f} neg={neg_corr:.4f}")


# ===========================================================================
# E. Positive but insufficient evidence
# ===========================================================================

def test_e_positive_insufficient():
    SK = "ADA-EUR__EDGE3"

    # E1. Positive memory but confidence < MEMORY_CONF_GATE_POS = 0.75
    low_conf_cases = [0.50, 0.60, 0.74]
    for conf in low_conf_cases:
        rec = make_memory_rec(SK, ["HELPFUL"] * 8, override_confidence=conf)
        final, passed, gate_name, applied, reason = gate(1.00, "NEUTRAL", rec)
        check(f"E: pos but conf={conf} < 0.75 → POSITIVE_CONF_TOO_LOW", gate_name == "POSITIVE_CONF_TOO_LOW", gate_name)
        check(f"E: pos but conf={conf} → gate_passed=False", passed is False)
        check(f"E: pos but conf={conf} → no positive effect", final <= 1.00, f"got {final}")

    # E2. Positive memory with recent harmful streak (last 3 have 2+ harmful)
    # window: 5 helpful + 3 harmful at the end
    rec_streak = make_memory_rec(
        SK, ["HELPFUL"] * 5 + ["HARMFUL", "HARMFUL", "HARMFUL"],
        override_confidence=0.80,
    )
    final_s, passed_s, gate_s, _, _ = gate(1.00, "NEUTRAL", rec_streak)
    check("E: recent harmful streak → RECENT_HARMFUL_BLOCKED", gate_s == "RECENT_HARMFUL_BLOCKED", gate_s)
    check("E: recent harmful streak → gate_passed=False", passed_s is False)
    check("E: recent harmful streak → no positive effect", final_s <= 1.00, f"got {final_s}")


# ===========================================================================
# F. Cycle-memory conflict
# ===========================================================================

def test_f_conflict():
    SK = "ETH-EUR__EDGE3"

    rec_positive_mem = make_memory_rec(SK, ["HELPFUL"] * 8, override_confidence=0.80)

    # F1. Cycle negative, memory positive → CONFLICT_BLOCKED
    for cycle_bias in ("NEGATIVE", "NEGATIVE_CAUTION"):
        final, passed, gate_name, applied, reason = gate(0.95, cycle_bias, rec_positive_mem)
        check(f"F1: conflict ({cycle_bias}+MEM_POS) → CONFLICT_BLOCKED", gate_name == "CONFLICT_BLOCKED", gate_name)
        check(f"F1: conflict ({cycle_bias}+MEM_POS) → gate_passed=False", passed is False)
        check(f"F1: conflict ({cycle_bias}+MEM_POS) → no positive escalation", final <= 1.00, f"got {final}")
        check(f"F1: conflict ({cycle_bias}+MEM_POS) → final==cycle_mod", final == 0.95, f"got {final}")

    # F2. Cycle positive, memory negative → memory dampens (no positive conflict)
    rec_neg_mem = make_memory_rec(SK, ["HARMFUL"] * 8, override_confidence=0.70)
    final_f2, passed_f2, gate_f2, _, _ = gate(1.05, "POSITIVE", rec_neg_mem)
    check("F2: cycle positive + memory negative → damped", final_f2 < 1.05, f"got {final_f2}")
    check("F2: cycle positive + memory negative → NEGATIVE_GATE_OPEN", gate_f2 == "NEGATIVE_GATE_OPEN", gate_f2)
    check("F2: no escalation beyond cycle_mod (dampened)", final_f2 <= 1.05)

    # F3. INSUFFICIENT_EVIDENCE cycle bias + positive memory → no conflict gate, but positive gate may still apply
    # (INSUFFICIENT_EVIDENCE is not in the conflict blacklist)
    rec_pos = make_memory_rec(SK, ["HELPFUL"] * 8, override_confidence=0.80)
    final_f3, passed_f3, gate_f3, _, _ = gate(1.00, "INSUFFICIENT_EVIDENCE", rec_pos)
    # INSUFFICIENT_EVIDENCE is not "NEGATIVE" or "NEGATIVE_CAUTION" → conflict gate doesn't block
    check("F3: INSUFFICIENT_EVIDENCE not blocked by conflict gate", gate_f3 != "CONFLICT_BLOCKED", gate_f3)


# ===========================================================================
# G. Recovery case
# ===========================================================================

def test_g_recovery():
    """
    Harmful streak → caution state → neutral outcomes → gradual improvement.
    Key checks: straf dooft geleidelijk uit, niet abrupt.
    """
    import importlib.util as ilu
    from pathlib import Path as P

    mem_path = P(__file__).parent / "ant_colony" / "build_allocation_feedback_memory_lite.py"
    spec2 = ilu.spec_from_file_location("mem_mod", mem_path)
    mem = ilu.module_from_spec(spec2)
    spec2.loader.exec_module(mem)

    SK = "BTC-EUR__EDGE4"

    _seq = [0]

    def make_fb(sk, cooldown=False):
        return {"strategy_key": sk, "cooldown_flag": cooldown, "market": "BTC-EUR"}

    def make_attr(sk, label):
        _seq[0] += 1
        return {
            "audit_id": f"{sk}_{label}_{_seq[0]}",
            "strategy_key": sk,
            "evaluation_status": "READY",
            "outcome_label": label,
        }

    # Phase 1: 5 harmful cycles — build caution state
    memory_state = {}
    for i in range(5):
        memory_state = mem.build_memory_state(
            [make_attr(SK, "HARMFUL")],
            [make_fb(SK, cooldown=True)],
            memory_state, f"HARM{i}", "2026-01-01T00:00:00Z",
        )

    mem_rec_phase1 = memory_state[SK]
    _, bias_phase1 = dq._memory_modifier_from_rec(mem_rec_phase1)
    check("G: after 5 harmful → NEGATIVE_CAUTION bias", bias_phase1 == "NEGATIVE_CAUTION", bias_phase1)

    final_p1, _, gate_p1, _, _ = dq.apply_memory_gate(1.00, "NEUTRAL", mem_rec_phase1)
    check("G: caution state → memory dampens", final_p1 < 1.00, f"got {final_p1}")

    # Phase 2: 5 neutral cycles — gradual recovery, no more cooldown signal
    for i in range(5):
        memory_state = mem.build_memory_state(
            [make_attr(SK, "NEUTRAL")],
            [make_fb(SK, cooldown=False)],
            memory_state, f"NEUT{i}", "2026-01-02T00:00:00Z",
        )

    mem_rec_phase2 = memory_state[SK]
    _, bias_phase2 = dq._memory_modifier_from_rec(mem_rec_phase2)
    final_p2, _, gate_p2, _, _ = dq.apply_memory_gate(1.00, "NEUTRAL", mem_rec_phase2)

    # Net signal should be improving (less negative or neutral)
    net_p1 = mem_rec_phase1["rolling_net_signal"]
    net_p2 = mem_rec_phase2["rolling_net_signal"]
    check("G: net_signal improves after neutral phase", net_p2 > net_p1, f"p1={net_p1} p2={net_p2}")

    # Cooldown should have decayed (COOLDOWN_PERSIST_CYCLES=3, we ran 5 neutral cycles)
    check("G: cooldown decayed after neutral cycles", mem_rec_phase2["cooldown_flag"] is False)

    # Phase 3: 5 helpful cycles — further recovery
    for i in range(5):
        memory_state = mem.build_memory_state(
            [make_attr(SK, "HELPFUL")],
            [make_fb(SK, cooldown=False)],
            memory_state, f"HELP{i}", "2026-01-03T00:00:00Z",
        )

    mem_rec_phase3 = memory_state[SK]
    net_p3 = mem_rec_phase3["rolling_net_signal"]
    _, bias_phase3 = dq._memory_modifier_from_rec(mem_rec_phase3)

    check("G: net_signal improves in helpful phase", net_p3 > net_p2, f"p2={net_p2} p3={net_p3}")
    check("G: eventually NEUTRAL or POSITIVE bias after recovery", bias_phase3 in ("NEUTRAL", "POSITIVE", "INSUFFICIENT_EVIDENCE"), bias_phase3)

    # Verify straf is not permanent — after full recovery, memory is no longer blocking positive
    final_p3, _, gate_p3, _, _ = dq.apply_memory_gate(1.00, "NEUTRAL", mem_rec_phase3)
    # Gate should be NEUTRAL or POSITIVE_GATE_OPEN, not COOLDOWN/NEGATIVE anymore
    check("G: after recovery, gate is not caution", gate_p3 not in ("COOLDOWN_GATE_OPEN", "NEGATIVE_GATE_OPEN"), gate_p3)


# ===========================================================================
# H. Invariants
# ===========================================================================

def test_h_invariants():
    # H1. Final modifier always in [MODIFIER_MIN, MODIFIER_MAX]
    cases = [
        (0.90, "NEGATIVE_CAUTION", make_memory_rec("K", ["HARMFUL"] * 8, override_confidence=0.80)),
        (1.05, "POSITIVE",          make_memory_rec("K", ["HELPFUL"] * 8, override_confidence=0.80)),
        (1.00, "NEUTRAL",           make_memory_rec("K", ["NEUTRAL"] * 8, override_confidence=0.80)),
        (0.90, "NEGATIVE",          None),
        (1.05, "POSITIVE",          None),
        (1.00, "NEUTRAL",           make_memory_rec("K", ["HELPFUL"], override_confidence=0.10)),
    ]
    for cycle_mod, cycle_bias, mem_rec in cases:
        final, _, _, _, _ = dq.apply_memory_gate(cycle_mod, cycle_bias, mem_rec)
        check(
            f"H: final in [0.90,1.05] for cycle={cycle_mod} bias={cycle_bias}",
            dq.MODIFIER_MIN <= final <= dq.MODIFIER_MAX,
            f"got {final}",
        )

    # H2. Memory correction never exceeds MEMORY_MAX_CORR_NEG on negative side
    rec_neg = make_memory_rec("K", ["HARMFUL"] * 8, override_confidence=1.0)
    for cycle_mod in [0.90, 0.95, 1.00, 1.05]:
        final, _, _, _, _ = dq.apply_memory_gate(cycle_mod, "NEUTRAL", rec_neg)
        # The correction applied is cycle_mod - final (negative)
        correction = cycle_mod - final
        check(
            f"H: negative correction <= MEMORY_MAX_CORR_NEG for cycle={cycle_mod}",
            correction <= dq.MEMORY_MAX_CORR_NEG + 1e-9,
            f"correction={correction:.4f}",
        )

    # H3. Positive correction never exceeds MEMORY_MAX_CORR_POS
    rec_pos = make_memory_rec("K", ["HELPFUL"] * 8, override_confidence=1.0)
    for cycle_mod in [0.90, 0.95, 1.00]:
        final, _, _, _, _ = dq.apply_memory_gate(cycle_mod, "NEUTRAL", rec_pos)
        correction = final - cycle_mod
        check(
            f"H: positive correction <= MEMORY_MAX_CORR_POS for cycle={cycle_mod}",
            correction <= dq.MEMORY_MAX_CORR_POS + 1e-9,
            f"correction={correction:.4f}",
        )

    # H4. _memory_modifier_from_rec returns valid modifier values
    cases_mod = [
        (["HELPFUL"] * 8, dq.MODIFIER_POSITIVE, "POSITIVE"),
        (["HARMFUL"] * 8, dq.MODIFIER_CAUTION,  "NEGATIVE_CAUTION"),
        (["NEUTRAL"] * 8, dq.MODIFIER_NEUTRAL,  "NEUTRAL"),
        (["HELPFUL"] * 4 + ["HARMFUL"] * 4, dq.MODIFIER_NEUTRAL, None),  # mixed → any
    ]
    for labels, exp_mod, exp_bias in cases_mod:
        if exp_bias is None:
            continue  # skip mixed (result depends on effective_signal calculation)
        rec = make_memory_rec("K", labels, override_confidence=1.0)
        mod_val, bias_val = dq._memory_modifier_from_rec(rec)
        check(f"H: modifier correct for all-{labels[0]}", mod_val == exp_mod, f"got {mod_val}")
        check(f"H: bias correct for all-{labels[0]}", bias_val == exp_bias, f"got {bias_val}")

    # H5. _memory_modifier_from_rec with None → MODIFIER_NEUTRAL, NO_MEMORY
    mod_n, bias_n = dq._memory_modifier_from_rec(None)
    check("H: None memory → MODIFIER_NEUTRAL", mod_n == dq.MODIFIER_NEUTRAL)
    check("H: None memory → NO_MEMORY bias", bias_n == "NO_MEMORY")

    # H6. apply_conviction_modifier (AC62) still works unchanged
    for base_conf in [0.0, 0.50, 1.0]:
        eff, mod, bias, status, cooldown, applied, reason = dq.apply_conviction_modifier(
            base_conf, "BTC-EUR__EDGE4", {}
        )
        check(f"H: AC62 apply_conviction_modifier still works (base={base_conf})", isinstance(eff, float))
        check(f"H: AC62 modifier=1.00 on empty index", mod == 1.00)

    # H7. VERSION is decision_quality_v3
    check("H: VERSION is decision_quality_v3", dq.VERSION == "decision_quality_v3", dq.VERSION)

    # H8. load_memory_index returns dict keyed by strategy_key
    import tempfile, json
    from pathlib import Path as P
    with tempfile.TemporaryDirectory() as tmp:
        mem_data = {
            "strategy_keys": {
                "BTC-EUR__EDGE4": make_memory_rec("BTC-EUR__EDGE4", ["HELPFUL"] * 5, override_confidence=0.70),
                "ETH-EUR__EDGE3": make_memory_rec("ETH-EUR__EDGE3", ["HARMFUL"] * 3, override_confidence=0.55),
            }
        }
        fpath = P(tmp) / "mem.json"
        fpath.write_text(json.dumps(mem_data), encoding="utf-8")
        idx = dq.load_memory_index(fpath)
        check("H: load_memory_index returns 2 keys", len(idx) == 2)
        check("H: BTC-EUR__EDGE4 in index", "BTC-EUR__EDGE4" in idx)
        check("H: ETH-EUR__EDGE3 in index", "ETH-EUR__EDGE3" in idx)

    # H9. Determinism — same inputs always produce same output
    rec_det = make_memory_rec("K", ["HARMFUL"] * 6, override_confidence=0.75)
    results = [dq.apply_memory_gate(1.00, "NEUTRAL", rec_det) for _ in range(5)]
    check("H: deterministic — same inputs same output", len(set(r[0] for r in results)) == 1)

    # H10. compute_quality_score still in [0,1]
    for d, c, b, r, ch in [(1.0, 1.0, 1.0, 1.0, 0.0), (0.0, 0.0, 0.0, 0.0, 0.6)]:
        qs = dq.compute_quality_score(d, c, b, r, ch)
        check(f"H: quality_score in [0,1]", 0.0 <= qs <= 1.0, f"got {qs}")


# ===========================================================================
# Run all tests
# ===========================================================================

def main():
    test_a_no_memory()
    test_b_low_confidence()
    test_c_negative_caution()
    test_d_positive_aligned()
    test_e_positive_insufficient()
    test_f_conflict()
    test_g_recovery()
    test_h_invariants()

    total = len(_PASS) + len(_FAIL)
    print(f"\nAC-64 results: {len(_PASS)}/{total} PASS")

    if _FAIL:
        for f in _FAIL:
            print(f"  {f}")
        sys.exit(1)
    else:
        print("  All tests PASS — AC64 memory-aware conviction integration validated.")
        sys.exit(0)


if __name__ == "__main__":
    main()
