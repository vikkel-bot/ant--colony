"""
AC-62 tests: Conviction Modifier Wiring
Imports load_feedback_index and apply_conviction_modifier from the modified
build_allocation_decision_quality_lite.py (AC56+AC62).

Test scenarios:
  A. Neutral fallback — key absent from index → modifier 1.00, no change
  B. Positive modifier — modifier 1.05 → effective_confidence rises
  C. Negative modifier — modifier 0.95 → effective_confidence falls
  D. Negative caution — modifier 0.90 + cooldown_flag True
  E. Insufficient evidence → modifier 1.00 (neutral, fail-closed)
  F. Mixed batch — multiple strategy_keys, different modifiers
  G. Invariants — modifier band [0.90, 1.05], no crash on missing AC-61 artefact
"""
import importlib.util
import json
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Load production module via importlib (no __init__ required)
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
# Helpers
# ---------------------------------------------------------------------------

def make_fb_record(
    strategy_key,
    bias_class="NEUTRAL",
    modifier=1.00,
    feedback_status="READY",
    cooldown_flag=False,
):
    return {
        "strategy_key": strategy_key,
        "allocation_bias_class": bias_class,
        "allocation_conviction_modifier": modifier,
        "feedback_status": feedback_status,
        "cooldown_flag": cooldown_flag,
    }


def make_feedback_json(records):
    """Build minimal allocation_feedback_integration.json content."""
    return {"records": records}


def write_feedback_file(tmp_dir, records):
    path = Path(tmp_dir) / "allocation_feedback_integration.json"
    path.write_text(json.dumps(make_feedback_json(records)), encoding="utf-8")
    return path


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
# A. Neutral fallback — strategy_key not in index
# ===========================================================================

def test_a_neutral_fallback():
    index = {}  # empty — no AC-61 data

    for base_conf in [0.0, 0.50, 0.80, 1.0]:
        eff, mod, bias, status, cooldown, applied, reason = dq.apply_conviction_modifier(
            base_conf, "BTC-EUR__EDGE4", index
        )
        check(
            f"A: modifier=1.00 for missing key (base={base_conf})",
            mod == 1.00,
            f"got {mod}",
        )
        check(
            f"A: effective_conf == base for missing key (base={base_conf})",
            eff == round(max(0.0, min(1.0, base_conf)), 4),
            f"got eff={eff}",
        )
        check(
            f"A: modifier_applied=False for missing key (base={base_conf})",
            applied is False,
        )
        check(
            f"A: status=NO_FEEDBACK_DATA for missing key (base={base_conf})",
            status == "NO_FEEDBACK_DATA",
            f"got {status}",
        )
        check(
            f"A: cooldown=False for missing key (base={base_conf})",
            cooldown is False,
        )


# ===========================================================================
# B. Positive modifier — 1.05 → effective confidence rises
# ===========================================================================

def test_b_positive_modifier():
    index = {
        "ETH-EUR__EDGE3": make_fb_record(
            "ETH-EUR__EDGE3",
            bias_class="POSITIVE",
            modifier=1.05,
            feedback_status="READY",
            cooldown_flag=False,
        )
    }

    base_conf = 0.70
    eff, mod, bias, status, cooldown, applied, reason = dq.apply_conviction_modifier(
        base_conf, "ETH-EUR__EDGE3", index
    )
    expected_eff = round(min(1.0, 0.70 * 1.05), 4)

    check("B: modifier=1.05", mod == 1.05, f"got {mod}")
    check("B: bias_class=POSITIVE", bias == "POSITIVE", f"got {bias}")
    check("B: effective_conf > base_conf", eff > base_conf, f"eff={eff} base={base_conf}")
    check("B: effective_conf correct", eff == expected_eff, f"got {eff}, want {expected_eff}")
    check("B: modifier_applied=True", applied is True)
    check("B: cooldown=False", cooldown is False)
    check("B: status=READY", status == "READY", f"got {status}")
    check("B: AC61 in reason", "AC61" in reason)


# ===========================================================================
# C. Negative modifier — 0.95 → effective confidence falls
# ===========================================================================

def test_c_negative_modifier():
    index = {
        "SOL-EUR__EDGE4": make_fb_record(
            "SOL-EUR__EDGE4",
            bias_class="NEGATIVE",
            modifier=0.95,
            feedback_status="READY",
            cooldown_flag=False,
        )
    }

    base_conf = 0.80
    eff, mod, bias, status, cooldown, applied, reason = dq.apply_conviction_modifier(
        base_conf, "SOL-EUR__EDGE4", index
    )
    expected_eff = round(0.80 * 0.95, 4)

    check("C: modifier=0.95", mod == 0.95, f"got {mod}")
    check("C: bias_class=NEGATIVE", bias == "NEGATIVE", f"got {bias}")
    check("C: effective_conf < base_conf", eff < base_conf, f"eff={eff} base={base_conf}")
    check("C: effective_conf correct", eff == expected_eff, f"got {eff}, want {expected_eff}")
    check("C: modifier_applied=True", applied is True)
    check("C: cooldown=False", cooldown is False)


# ===========================================================================
# D. Negative caution — modifier 0.90, cooldown_flag True
# ===========================================================================

def test_d_negative_caution():
    index = {
        "BTC-EUR__EDGE4": make_fb_record(
            "BTC-EUR__EDGE4",
            bias_class="NEGATIVE_CAUTION",
            modifier=0.90,
            feedback_status="READY",
            cooldown_flag=True,
        )
    }

    base_conf = 0.75
    eff, mod, bias, status, cooldown, applied, reason = dq.apply_conviction_modifier(
        base_conf, "BTC-EUR__EDGE4", index
    )
    expected_eff = round(0.75 * 0.90, 4)

    check("D: modifier=0.90 (caution floor)", mod == 0.90, f"got {mod}")
    check("D: bias_class=NEGATIVE_CAUTION", bias == "NEGATIVE_CAUTION", f"got {bias}")
    check("D: effective_conf correct", eff == expected_eff, f"got {eff}, want {expected_eff}")
    check("D: effective_conf notably lower than base", eff < base_conf - 0.05, f"eff={eff}")
    check("D: modifier_applied=True", applied is True)
    check("D: cooldown=True", cooldown is True, f"got {cooldown}")


# ===========================================================================
# E. Insufficient evidence — modifier stays 1.00 (fail-closed)
# ===========================================================================

def test_e_insufficient_evidence():
    # AC-61 emits modifier=1.00 for INSUFFICIENT_EVIDENCE — test that wiring
    # preserves this (no positive bias on thin evidence)
    index = {
        "ADA-EUR__EDGE3": make_fb_record(
            "ADA-EUR__EDGE3",
            bias_class="INSUFFICIENT_EVIDENCE",
            modifier=1.00,
            feedback_status="INSUFFICIENT_EVIDENCE",
            cooldown_flag=False,
        )
    }

    base_conf = 0.60
    eff, mod, bias, status, cooldown, applied, reason = dq.apply_conviction_modifier(
        base_conf, "ADA-EUR__EDGE3", index
    )

    check("E: modifier=1.00 for insufficient evidence", mod == 1.00, f"got {mod}")
    check("E: effective_conf == base_conf", eff == base_conf, f"eff={eff}")
    check("E: modifier_applied=False", applied is False)
    check("E: bias_class=INSUFFICIENT_EVIDENCE", bias == "INSUFFICIENT_EVIDENCE", f"got {bias}")
    check("E: status=INSUFFICIENT_EVIDENCE", status == "INSUFFICIENT_EVIDENCE", f"got {status}")


# ===========================================================================
# F. Mixed batch — multiple strategy_keys, validate each independently
# ===========================================================================

def test_f_mixed_batch():
    records = [
        make_fb_record("BTC-EUR__EDGE4",  bias_class="POSITIVE",          modifier=1.05, feedback_status="READY",                cooldown_flag=False),
        make_fb_record("ETH-EUR__EDGE3",  bias_class="NEUTRAL",           modifier=1.00, feedback_status="READY",                cooldown_flag=False),
        make_fb_record("SOL-EUR__EDGE4",  bias_class="NEGATIVE",          modifier=0.95, feedback_status="READY",                cooldown_flag=False),
        make_fb_record("ADA-EUR__EDGE4",  bias_class="NEGATIVE_CAUTION",  modifier=0.90, feedback_status="READY",                cooldown_flag=True),
        make_fb_record("DOT-EUR__EDGE3",  bias_class="INSUFFICIENT_EVIDENCE", modifier=1.00, feedback_status="INSUFFICIENT_EVIDENCE", cooldown_flag=False),
    ]
    index = {r["strategy_key"]: r for r in records}

    base = 0.70
    cases = [
        ("BTC-EUR__EDGE4",  1.05, round(base * 1.05, 4), False),
        ("ETH-EUR__EDGE3",  1.00, base,                  False),
        ("SOL-EUR__EDGE4",  0.95, round(base * 0.95, 4), False),
        ("ADA-EUR__EDGE4",  0.90, round(base * 0.90, 4), True),
        ("DOT-EUR__EDGE3",  1.00, base,                  False),
    ]

    for sk, exp_mod, exp_eff, exp_cooldown in cases:
        eff, mod, _, _, cooldown, _, _ = dq.apply_conviction_modifier(base, sk, index)
        check(f"F: {sk} modifier={exp_mod}", mod == exp_mod, f"got {mod}")
        check(f"F: {sk} eff={exp_eff}", eff == exp_eff, f"got {eff}")
        check(f"F: {sk} cooldown={exp_cooldown}", cooldown == exp_cooldown, f"got {cooldown}")

    # Unknown key → neutral fallback
    eff, mod, _, status, cooldown, applied, _ = dq.apply_conviction_modifier(
        base, "UNKNOWN__EDGE99", index
    )
    check("F: unknown key → modifier=1.00", mod == 1.00)
    check("F: unknown key → status=NO_FEEDBACK_DATA", status == "NO_FEEDBACK_DATA")
    check("F: unknown key → applied=False", applied is False)


# ===========================================================================
# G. Invariants — modifier band, score clamping, no crash without artefact
# ===========================================================================

def test_g_invariants():
    # G1. load_feedback_index from missing file → empty dict (no crash)
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "does_not_exist.json"
        idx = dq.load_feedback_index(missing)
        check("G: missing file → empty dict", idx == {}, f"got {idx}")

    # G2. load_feedback_index from valid file → correct index
    with tempfile.TemporaryDirectory() as tmp:
        records = [
            make_fb_record("BTC-EUR__EDGE4", bias_class="POSITIVE", modifier=1.05),
            make_fb_record("ETH-EUR__EDGE3", bias_class="NEGATIVE", modifier=0.95),
        ]
        fpath = write_feedback_file(tmp, records)
        idx = dq.load_feedback_index(fpath)
        check("G: load_feedback_index key count", len(idx) == 2, f"got {len(idx)}")
        check("G: BTC-EUR__EDGE4 in index", "BTC-EUR__EDGE4" in idx)
        check("G: ETH-EUR__EDGE3 in index", "ETH-EUR__EDGE3" in idx)

    # G3. Modifier band — any raw modifier is clamped to [0.90, 1.05]
    # Inject out-of-band modifier values directly to test clamping in apply_conviction_modifier
    for raw_mod, exp_clamped in [(1.50, 1.05), (0.50, 0.90), (1.05, 1.05), (0.90, 0.90)]:
        idx = {
            "TEST__KEY": make_fb_record(
                "TEST__KEY", modifier=raw_mod, bias_class="POSITIVE" if raw_mod > 1 else "NEGATIVE"
            )
        }
        eff, mod, _, _, _, _, _ = dq.apply_conviction_modifier(0.70, "TEST__KEY", idx)
        check(
            f"G: raw_modifier={raw_mod} clamped to {exp_clamped}",
            mod == exp_clamped,
            f"got {mod}",
        )

    # G4. effective_confidence always in [0.0, 1.0]
    edge_cases = [
        (0.0,  1.05, 0.0),
        (1.0,  1.05, 1.0),
        (0.99, 1.05, round(min(1.0, 0.99 * 1.05), 4)),
        (0.0,  0.90, 0.0),
    ]
    for base, mod_val, exp_eff in edge_cases:
        idx = {"K": make_fb_record("K", modifier=mod_val)}
        eff, _, _, _, _, _, _ = dq.apply_conviction_modifier(base, "K", idx)
        check(
            f"G: eff in [0,1] for base={base} mod={mod_val}",
            0.0 <= eff <= 1.0,
            f"got {eff}",
        )
        check(
            f"G: eff correct for base={base} mod={mod_val}",
            eff == exp_eff,
            f"got {eff}, want {exp_eff}",
        )

    # G5. score_conviction still works (AC56 unchanged)
    sc, reason = dq.score_conviction(0.80, "BULL")
    check("G: score_conviction returns float", isinstance(sc, float))
    check("G: score_conviction BULL high", sc >= 0.70, f"got {sc}")
    check("G: score_conviction reason non-empty", len(reason) > 0)

    # G6. compute_quality_score still within [0, 1]
    for d, c, b, r, ch in [
        (1.0, 1.0, 1.0, 1.0, 0.0),
        (0.0, 0.0, 0.0, 0.0, 0.6),
        (0.5, 0.5, 0.5, 0.5, 0.2),
    ]:
        qs = dq.compute_quality_score(d, c, b, r, ch)
        check(
            f"G: quality_score in [0,1] for ({d},{c},{b},{r},{ch})",
            0.0 <= qs <= 1.0,
            f"got {qs}",
        )

    # G7. VERSION bumped (v2 → v3 with AC64 memory integration)
    check("G: VERSION is decision_quality_v3", dq.VERSION == "decision_quality_v3", f"got {dq.VERSION}")

    # G8. load_feedback_index handles malformed JSON → empty dict
    with tempfile.TemporaryDirectory() as tmp:
        bad = Path(tmp) / "bad.json"
        bad.write_text("NOT JSON{{{", encoding="utf-8")
        idx = dq.load_feedback_index(bad)
        check("G: malformed JSON → empty dict", idx == {}, f"got {idx}")

    # G9. load_feedback_index handles empty records list → empty dict
    with tempfile.TemporaryDirectory() as tmp:
        fpath = Path(tmp) / "empty.json"
        fpath.write_text(json.dumps({"records": []}), encoding="utf-8")
        idx = dq.load_feedback_index(fpath)
        check("G: empty records list → empty dict", idx == {}, f"got {idx}")


# ===========================================================================
# Run all tests
# ===========================================================================

def main():
    test_a_neutral_fallback()
    test_b_positive_modifier()
    test_c_negative_modifier()
    test_d_negative_caution()
    test_e_insufficient_evidence()
    test_f_mixed_batch()
    test_g_invariants()

    total = len(_PASS) + len(_FAIL)
    print(f"\nAC-62 results: {len(_PASS)}/{total} PASS")

    if _FAIL:
        for f in _FAIL:
            print(f"  {f}")
        sys.exit(1)
    else:
        print("  All tests PASS — AC62 conviction modifier wiring validated.")
        sys.exit(0)


if __name__ == "__main__":
    main()
