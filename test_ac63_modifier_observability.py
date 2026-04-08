"""
AC-63 Part B tests: Modifier Observability Dashboarding

Scenarios:
  G. Observability consistency — base vs effective conviction visible,
     cycle vs memory contribution distinguishable
  H. Invariants — modifier band safe, cooldown readable, neutral fallback present
"""
import importlib.util
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Load production modules
# ---------------------------------------------------------------------------
_OBS_PATH = (
    Path(__file__).parent
    / "ant_colony"
    / "build_allocation_modifier_observability_lite.py"
)
spec = importlib.util.spec_from_file_location("obs_mod", _OBS_PATH)
obs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(obs)

_MEM_PATH = (
    Path(__file__).parent
    / "ant_colony"
    / "build_allocation_feedback_memory_lite.py"
)
spec2 = importlib.util.spec_from_file_location("mem_mod", _MEM_PATH)
mem = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(mem)

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

def make_fb_rec(sk, modifier=1.00, bias="NEUTRAL", cooldown=False, feedback_status="READY", market=""):
    return {
        "strategy_key":                   sk,
        "market":                         market or sk.split("__")[0],
        "allocation_conviction_modifier": modifier,
        "allocation_bias_class":          bias,
        "cooldown_flag":                  cooldown,
        "feedback_status":                feedback_status,
    }


def make_dq_rec(sk, base_conf=0.70, effective_conf=None):
    """Minimal AC-62 decision quality record."""
    eff = effective_conf if effective_conf is not None else base_conf
    parts = sk.split("__")
    return {
        "position_key":              sk,
        "market":                    parts[0] if len(parts) > 1 else "",
        "strategy":                  parts[1] if len(parts) > 1 else sk,
        "base_feedback_confidence":  base_conf,
        "effective_feedback_confidence": eff,
    }


def make_memory_rec(sk, window_labels, cooldown_flag=False, cooldown_remaining=0):
    """Build a minimal memory record with specified window labels."""
    n = len(window_labels)
    helpful = window_labels.count("HELPFUL")
    harmful = window_labels.count("HARMFUL")
    net_signal = round((helpful - harmful) / max(n, 1), 4) if n > 0 else 0.0
    conf = round(min(1.0, n / mem.FULL_MEMORY_AT), 4)

    status = "BOOTSTRAP"
    if n == 0:
        status = "BOOTSTRAP"
    elif n < 3:
        status = "SPARSE"
    elif conf < mem.MEMORY_MIN_CONFIDENCE:
        status = "INSUFFICIENT"
    else:
        status = "ACTIVE"

    window = [{"audit_id": f"t{i}", "cycle_id": "CY0", "outcome_label": lbl}
              for i, lbl in enumerate(window_labels)]

    return {
        "strategy_key":               sk,
        "market":                     sk.split("__")[0] if "__" in sk else "",
        "memory_status":              status,
        "rolling_window":             window,
        "rolling_net_signal":         net_signal,
        "memory_confidence":          conf,
        "cooldown_flag":              cooldown_flag,
        "cooldown_cycles_remaining":  cooldown_remaining,
        "last_outcome_label":         window_labels[-1] if window_labels else None,
        "last_cycle_id":              "CY0",
        "last_update_ts_utc":         "2026-01-01T00:00:00Z",
    }


# ===========================================================================
# G. Observability consistency
# ===========================================================================

def test_g_observability_consistency():
    SK = "BTC-EUR__EDGE4"
    BASE = 0.70

    # G1. Cycle modifier 1.05 (positive), memory also positive (8 HELPFULs)
    fb_recs  = [make_fb_rec(SK, modifier=1.05, bias="POSITIVE")]
    dq_recs  = [make_dq_rec(SK, base_conf=BASE, effective_conf=round(BASE * 1.05, 4))]
    mem_state = {SK: make_memory_rec(SK, ["HELPFUL"] * 8)}

    recs = obs.build_observability_report(fb_recs, dq_recs, mem_state)
    check("G1: one record returned", len(recs) == 1)
    r = recs[0]

    check("G1: strategy_key correct", r["strategy_key"] == SK)
    check("G1: base_feedback_confidence = 0.70", r["base_feedback_confidence"] == BASE)
    check("G1: cycle_modifier = 1.05", r["cycle_modifier"] == 1.05)
    check("G1: ac62_effective_confidence visible", r["ac62_effective_confidence"] == round(BASE * 1.05, 4))
    check("G1: memory_modifier positive (8 helpfuls)", r["memory_modifier"] == obs.MODIFIER_POSITIVE)
    check("G1: memory_bias_class POSITIVE", r["memory_bias_class"] == "POSITIVE")
    check("G1: effective_modifier in [0.90, 1.05]", obs.MODIFIER_MIN <= r["effective_modifier"] <= obs.MODIFIER_MAX)
    check("G1: effective_feedback_confidence in [0, 1]", 0.0 <= r["effective_feedback_confidence"] <= 1.0)
    check("G1: memory_blended True (conf >= threshold)", r["memory_blended"] is True)
    check("G1: modifier_applied True", r["modifier_applied"] is True)

    # G2. Cycle modifier 1.00 (neutral), memory negative (8 HARMFULs)
    SK2 = "ETH-EUR__EDGE3"
    fb_recs2  = [make_fb_rec(SK2, modifier=1.00, bias="NEUTRAL")]
    dq_recs2  = [make_dq_rec(SK2, base_conf=0.60)]
    mem_state2 = {SK2: make_memory_rec(SK2, ["HARMFUL"] * 8)}

    recs2 = obs.build_observability_report(fb_recs2, dq_recs2, mem_state2)
    r2 = recs2[0]

    check("G2: cycle_modifier = 1.00 (neutral cycle)", r2["cycle_modifier"] == 1.00)
    check("G2: memory_modifier < 1.00 (negative memory)", r2["memory_modifier"] < 1.00)
    check("G2: effective_modifier < 1.00 (memory dampens)", r2["effective_modifier"] < 1.00)
    check("G2: memory_blended True", r2["memory_blended"] is True)
    check("G2: MEMORY_DAMPENS_CYCLE in flags", "MEMORY_DAMPENS_CYCLE" in r2["observability_flags"])

    # G3. Memory confidence below threshold → no blend, cycle modifier used as-is
    SK3 = "SOL-EUR__EDGE4"
    fb_recs3  = [make_fb_rec(SK3, modifier=0.95, bias="NEGATIVE")]
    dq_recs3  = [make_dq_rec(SK3, base_conf=0.80)]
    mem_state3 = {SK3: make_memory_rec(SK3, ["HARMFUL"])}  # only 1 record → low confidence

    recs3 = obs.build_observability_report(fb_recs3, dq_recs3, mem_state3)
    r3 = recs3[0]

    check("G3: memory_confidence < MEMORY_MIN_CONFIDENCE", r3["memory_confidence"] < obs.MEMORY_MIN_CONFIDENCE)
    check("G3: memory_blended False (confidence too low)", r3["memory_blended"] is False)
    check("G3: effective_modifier == cycle_modifier when no blend", r3["effective_modifier"] == r3["cycle_modifier"])
    check("G3: MEMORY_CONFIDENCE_INSUFFICIENT in flags", "MEMORY_CONFIDENCE_INSUFFICIENT" in r3["observability_flags"])

    # G4. No memory record at all → neutral fallback
    SK4 = "ADA-EUR__EDGE4"
    fb_recs4  = [make_fb_rec(SK4, modifier=1.00, bias="NEUTRAL")]
    dq_recs4  = [make_dq_rec(SK4, base_conf=0.60)]

    recs4 = obs.build_observability_report(fb_recs4, dq_recs4, {})
    r4 = recs4[0]
    check("G4: no memory → memory_modifier=1.00", r4["memory_modifier"] == 1.00)
    check("G4: no memory → memory_blended=False", r4["memory_blended"] is False)
    check("G4: no memory → memory_status=NO_MEMORY", r4["memory_status"] == "NO_MEMORY")
    check("G4: no memory → effective=cycle_modifier", r4["effective_modifier"] == r4["cycle_modifier"])

    # G5. Cooldown visible in observability
    SK5 = "DOT-EUR__EDGE3"
    fb_recs5   = [make_fb_rec(SK5, modifier=0.90, bias="NEGATIVE_CAUTION", cooldown=True)]
    dq_recs5   = [make_dq_rec(SK5, base_conf=0.75)]
    mem_state5 = {SK5: make_memory_rec(SK5, ["HARMFUL"] * 8, cooldown_flag=True, cooldown_remaining=2)}

    recs5 = obs.build_observability_report(fb_recs5, dq_recs5, mem_state5)
    r5 = recs5[0]
    check("G5: cooldown_flag True", r5["cooldown_flag"] is True)
    check("G5: cooldown_cycles_remaining=2", r5["cooldown_cycles_remaining"] == 2)
    check("G5: MEMORY_COOLDOWN_ACTIVE in flags", "MEMORY_COOLDOWN_ACTIVE" in r5["observability_flags"])
    check("G5: CYCLE_CAUTION in flags", "CYCLE_CAUTION" in r5["observability_flags"])

    # G6. Cycle and memory bias disagree → flag visible
    SK6 = "XRP-EUR__EDGE4"
    fb_recs6   = [make_fb_rec(SK6, modifier=1.05, bias="POSITIVE")]  # cycle: positive
    dq_recs6   = [make_dq_rec(SK6, base_conf=0.70)]
    mem_state6 = {SK6: make_memory_rec(SK6, ["HARMFUL"] * 8)}        # memory: negative

    recs6 = obs.build_observability_report(fb_recs6, dq_recs6, mem_state6)
    r6 = recs6[0]
    check("G6: CYCLE_MEMORY_BIAS_DISAGREE in flags", "CYCLE_MEMORY_BIAS_DISAGREE" in r6["observability_flags"])

    # G7. Multiple strategy_keys → all appear
    all_fb = fb_recs + fb_recs2 + fb_recs3 + fb_recs4
    all_dq = dq_recs + dq_recs2 + dq_recs3 + dq_recs4
    all_mem = {**mem_state, **mem_state2, **mem_state3}

    all_recs = obs.build_observability_report(all_fb, all_dq, all_mem)
    check("G7: all strategy_keys present", len(all_recs) == 4)
    found_keys = {r["strategy_key"] for r in all_recs}
    for sk in [SK, SK2, SK3, SK4]:
        check(f"G7: {sk} in report", sk in found_keys)


# ===========================================================================
# H. Invariants
# ===========================================================================

def test_h_invariants():
    # H1. compute_memory_modifier with None → neutral
    modifier, bias, reasons = obs.compute_memory_modifier(None)
    check("H: None memory_rec → modifier=1.00", modifier == obs.MODIFIER_NEUTRAL)
    check("H: None memory_rec → bias=NO_MEMORY", bias == obs.BIAS_NO_MEMORY)
    check("H: None memory_rec → reasons non-empty", len(reasons) > 0)

    # H2. compute_memory_modifier — all helpful → POSITIVE
    rec = make_memory_rec("K", ["HELPFUL"] * 8)
    modifier, bias, _ = obs.compute_memory_modifier(rec)
    check("H: all helpful → POSITIVE modifier", modifier == obs.MODIFIER_POSITIVE)
    check("H: all helpful → POSITIVE bias", bias == "POSITIVE")

    # H3. compute_memory_modifier — all harmful → CAUTION
    rec2 = make_memory_rec("K", ["HARMFUL"] * 8)
    modifier2, bias2, _ = obs.compute_memory_modifier(rec2)
    check("H: all harmful → CAUTION modifier", modifier2 == obs.MODIFIER_CAUTION)
    check("H: all harmful → CAUTION bias", bias2 == "NEGATIVE_CAUTION")

    # H4. compute_memory_modifier — low confidence → INSUFFICIENT
    rec3 = make_memory_rec("K", ["HELPFUL"])  # only 1, conf < threshold
    modifier3, bias3, _ = obs.compute_memory_modifier(rec3)
    check("H: low confidence → INSUFFICIENT bias", bias3 == "INSUFFICIENT_EVIDENCE")
    check("H: low confidence → neutral modifier", modifier3 == obs.MODIFIER_NEUTRAL)

    # H5. blend_modifiers — modifier always in [MODIFIER_MIN, MODIFIER_MAX]
    test_cases = [
        (1.05, 0.90, 1.0),   # positive cycle, caution memory, high confidence
        (0.90, 1.05, 1.0),   # caution cycle, positive memory, high confidence
        (1.00, 1.00, 0.5),   # both neutral
        (0.95, 0.90, 0.8),   # both negative/caution
    ]
    for cyc, mem_mod, conf in test_cases:
        eff, blended, reason = obs.blend_modifiers(cyc, mem_mod, conf)
        check(
            f"H: blend({cyc},{mem_mod},{conf}) in [0.90,1.05]",
            obs.MODIFIER_MIN <= eff <= obs.MODIFIER_MAX,
            f"got {eff}",
        )
        check(f"H: blend({cyc},{mem_mod},{conf}) reason non-empty", len(reason) > 0)

    # H6. blend_modifiers — below threshold → no blend
    eff_low, blended_low, reason_low = obs.blend_modifiers(0.95, 1.05, 0.10)
    check("H: low confidence → no blend (blended=False)", blended_low is False)
    check("H: low confidence → cycle modifier used as-is", eff_low == 0.95)

    # H7. blend_modifiers — memory weight never exceeds MEMORY_WEIGHT_MAX
    # At full confidence (1.0), memory gets MEMORY_WEIGHT_MAX=0.40 → cycle gets 0.60
    eff_full, _, _ = obs.blend_modifiers(1.00, 1.05, 1.0)
    expected_full = round(1.00 * 0.60 + 1.05 * 0.40, 4)
    # Might be clamped; check it's <= MODIFIER_MAX
    check("H: full-confidence blend clamped to band", obs.MODIFIER_MIN <= eff_full <= obs.MODIFIER_MAX)
    # Memory cannot dominate: even at max weight (40%), result stays moderate
    check("H: memory weight at most 40% influence", eff_full <= 1.02 + 0.001)

    # H8. effective_feedback_confidence always in [0, 1]
    edge_cases = [
        (0.0,  "HELPFUL",  8),
        (1.0,  "HELPFUL",  8),
        (0.99, "HARMFUL",  8),
        (0.50, "NEUTRAL",  1),
    ]
    for base, lbl, n in edge_cases:
        fb = [make_fb_rec("T__K", modifier=1.05 if lbl=="HELPFUL" else 0.90)]
        dq = [make_dq_rec("T__K", base_conf=base)]
        mem_state = {"T__K": make_memory_rec("T__K", [lbl] * n)}
        recs = obs.build_observability_report(fb, dq, mem_state)
        r = recs[0]
        check(
            f"H: eff_conf in [0,1] for base={base} lbl={lbl} n={n}",
            0.0 <= r["effective_feedback_confidence"] <= 1.0,
            f"got {r['effective_feedback_confidence']}",
        )

    # H9. build_observability_report with all empty inputs → no crash
    recs_empty = obs.build_observability_report([], [], {})
    check("H: empty inputs → empty list (no crash)", recs_empty == [])

    # H10. TSV_HEADERS are a subset of keys in every record
    fb = [make_fb_rec("BTC-EUR__EDGE4", modifier=1.05, bias="POSITIVE")]
    dq = [make_dq_rec("BTC-EUR__EDGE4", base_conf=0.70)]
    mem_s = {"BTC-EUR__EDGE4": make_memory_rec("BTC-EUR__EDGE4", ["HELPFUL"] * 6)}
    sample = obs.build_observability_report(fb, dq, mem_s)[0]
    for h in obs.TSV_HEADERS:
        check(f"H: TSV header '{h}' present in record", h in sample, f"missing from {list(sample.keys())}")

    # H11. VERSION is correct
    check("H: VERSION = modifier_observability_v1", obs.VERSION == "modifier_observability_v1", obs.VERSION)


# ===========================================================================
# Run all tests
# ===========================================================================

def main():
    test_g_observability_consistency()
    test_h_invariants()

    total = len(_PASS) + len(_FAIL)
    print(f"\nAC-63 Part B (observability) results: {len(_PASS)}/{total} PASS")

    if _FAIL:
        for f in _FAIL:
            print(f"  {f}")
        sys.exit(1)
    else:
        print("  All tests PASS — AC63 modifier observability validated.")
        sys.exit(0)


if __name__ == "__main__":
    main()
