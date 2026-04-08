"""
AC61: Allocation Feedback Integration — synthetic validation
Tests outcome aggregation, bias classification, conviction modifier, cooldown
logic, and fail-closed behaviour with controlled inputs. No file I/O.

Scenarios:
  A: Positive evidence   — 4×HELPFUL, 1×NEUTRAL             → POSITIVE, modifier=1.05
  B: Negative evidence   — 3×HARMFUL, 1×NEUTRAL             → NEGATIVE / CAUTION
  C: Mixed evidence      — equal HELPFUL and HARMFUL         → NEUTRAL
  D: Sparse evidence     — 2×HELPFUL (< MIN_READY=3)         → INSUFFICIENT_EVIDENCE
  E: No ready outcomes   — only PENDING/NOT_EXECUTED         → NO_READY_OUTCOMES
  F: Cooldown case       — 5×HARMFUL (>= 60% harmful)       → NEGATIVE_CAUTION, cooldown=True
  G: Batch multi-key     — two strategy keys, different signals
  H: Invariants          — modifier in band, bias valid, determinism

Usage: python test_ac61_feedback_integration.py
"""
import importlib.util
from pathlib import Path

# --- Load feedback integration production module ---
_mod_path = Path(__file__).parent / "ant_colony" / "build_allocation_feedback_integration_lite.py"
_spec = importlib.util.spec_from_file_location("fi", _mod_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

build_feedback_record      = _mod.build_feedback_record
build_feedback_integration = _mod.build_feedback_integration
compute_confidence_weight  = _mod.compute_confidence_weight
determine_bias             = _mod.determine_bias
modifier_for_bias          = _mod.modifier_for_bias

MIN_READY_OUTCOMES    = _mod.MIN_READY_OUTCOMES
FULL_CONFIDENCE_AT    = _mod.FULL_CONFIDENCE_AT
POSITIVE_THRESHOLD    = _mod.POSITIVE_THRESHOLD
NEGATIVE_THRESHOLD    = _mod.NEGATIVE_THRESHOLD
CAUTION_THRESHOLD     = _mod.CAUTION_THRESHOLD
CAUTION_HARMFUL_RATIO = _mod.CAUTION_HARMFUL_RATIO

MODIFIER_POSITIVE = _mod.MODIFIER_POSITIVE
MODIFIER_NEUTRAL  = _mod.MODIFIER_NEUTRAL
MODIFIER_NEGATIVE = _mod.MODIFIER_NEGATIVE
MODIFIER_CAUTION  = _mod.MODIFIER_CAUTION
MODIFIER_BAND_MIN = _mod.MODIFIER_BAND_MIN
MODIFIER_BAND_MAX = _mod.MODIFIER_BAND_MAX

BIAS_POSITIVE     = _mod.BIAS_POSITIVE
BIAS_NEUTRAL      = _mod.BIAS_NEUTRAL
BIAS_NEGATIVE     = _mod.BIAS_NEGATIVE
BIAS_CAUTION      = _mod.BIAS_CAUTION
BIAS_INSUFFICIENT = _mod.BIAS_INSUFFICIENT

FEEDBACK_READY         = _mod.FEEDBACK_READY
FEEDBACK_INSUFFICIENT  = _mod.FEEDBACK_INSUFFICIENT
FEEDBACK_NO_READY      = _mod.FEEDBACK_NO_READY_OUTCOMES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


def make_attr(pk, outcome_label, eval_status="READY",
              strategy_key=None, market="TST"):
    """Create a minimal AC60-style attribution record."""
    sk = strategy_key or (pk.split("__")[1] if "__" in pk else pk)
    return {
        "audit_id":          f"CYC__{pk}__001",
        "position_key":      pk,
        "market":            market,
        "strategy_key":      sk,
        "evaluation_status": eval_status,
        "outcome_label":     outcome_label,
        "outcome_score":     {"HELPFUL": 0.4, "NEUTRAL": 0.0, "HARMFUL": -0.4}.get(outcome_label, None),
    }


def make_ready(n_helpful=0, n_neutral=0, n_harmful=0,
               sk="S1", market="TST", prefix="TST__S1"):
    recs = []
    for i in range(n_helpful):
        recs.append(make_attr(f"{prefix}_{i}", "HELPFUL", strategy_key=sk, market=market))
    for i in range(n_neutral):
        recs.append(make_attr(f"{prefix}_N{i}", "NEUTRAL", strategy_key=sk, market=market))
    for i in range(n_harmful):
        recs.append(make_attr(f"{prefix}_H{i}", "HARMFUL", strategy_key=sk, market=market))
    return recs


def make_non_ready(n=3, sk="S1", market="TST", prefix="TST__S1"):
    recs = []
    for i in range(n):
        label = ["PENDING", "NOT_EXECUTED", "INSUFFICIENT_DATA"][i % 3]
        status = label  # non-READY eval status
        recs.append(make_attr(f"{prefix}_NR{i}", label, eval_status=status,
                              strategy_key=sk, market=market))
    return recs


def run_integration(recs, sk=None):
    """Run build_feedback_integration and return the record for sk."""
    all_recs = build_feedback_integration(recs)
    if sk:
        return next((r for r in all_recs if r["strategy_key"] == sk), None)
    return all_recs[0] if all_recs else None


# ---------------------------------------------------------------------------
print("\n" + "#"*72)
print("  AC61 ALLOCATION FEEDBACK INTEGRATION — SYNTHETIC VALIDATION")
print(f"  MIN_READY={MIN_READY_OUTCOMES}  FULL_CONF_AT={FULL_CONFIDENCE_AT}")
print(f"  POS_THRESH={POSITIVE_THRESHOLD}  NEG_THRESH={NEGATIVE_THRESHOLD}"
      f"  CAUTION_THRESH={CAUTION_THRESHOLD}")
print(f"  modifier band=[{MODIFIER_BAND_MIN}, {MODIFIER_BAND_MAX}]")
print("#"*72)


# ---------------------------------------------------------------------------
# SCENARIO A — Positive evidence: 4×HELPFUL, 1×NEUTRAL, 0×HARMFUL
# net_signal = (4-0)/5 = 0.80
# confidence_weight = min(1.0, 5/6) ≈ 0.833
# effective_signal = 0.80 × 0.833 ≈ 0.667 >= POSITIVE_THRESHOLD(0.20) → POSITIVE
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO A — Positive evidence (4×HELPFUL, 1×NEUTRAL)")
print(f"{'='*72}")
recs_a = make_ready(n_helpful=4, n_neutral=1, n_harmful=0, sk="EDGE4", market="BTC-EUR", prefix="BTC_A")
r_a = run_integration(recs_a, "EDGE4")
print(f"  bias={r_a['allocation_bias_class']}  modifier={r_a['allocation_conviction_modifier']}")
print(f"  net_signal={r_a['net_outcome_signal']}  eff_signal={r_a['effective_signal']}")

check("A: allocation_bias_class == POSITIVE",          r_a["allocation_bias_class"] == BIAS_POSITIVE)
check("A: allocation_conviction_modifier == 1.05",     r_a["allocation_conviction_modifier"] == MODIFIER_POSITIVE)
check("A: cooldown_flag == False",                     r_a["cooldown_flag"] is False)
check("A: feedback_status == READY",                   r_a["feedback_status"] == FEEDBACK_READY)
check("A: helpful_count == 4",                         r_a["helpful_count"] == 4)
check("A: net_outcome_signal > 0",                     r_a["net_outcome_signal"] > 0)
check("A: POSITIVE in integration_reasons",
      "POSITIVE_SIGNAL" in r_a["integration_reasons"])


# ---------------------------------------------------------------------------
# SCENARIO B — Negative evidence: 1×HELPFUL, 1×NEUTRAL, 3×HARMFUL
# net_signal = (1-3)/5 = -0.40
# confidence_weight = 5/6 ≈ 0.833
# effective_signal = -0.40 × 0.833 ≈ -0.333 < NEGATIVE_THRESHOLD(-0.20) → NEGATIVE
# not quite CAUTION: -0.333 > CAUTION_THRESHOLD(-0.50), harmful_ratio=3/5=0.60 → CAUTION
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO B — Negative evidence (1×HELPFUL, 1×NEUTRAL, 3×HARMFUL)")
print(f"{'='*72}")
recs_b = make_ready(n_helpful=1, n_neutral=1, n_harmful=3, sk="EDGE3", market="ETH-EUR", prefix="ETH_B")
r_b = run_integration(recs_b, "EDGE3")
print(f"  bias={r_b['allocation_bias_class']}  modifier={r_b['allocation_conviction_modifier']}")
print(f"  harmful_ratio={r_b['harmful_ratio']}  eff_signal={r_b['effective_signal']}")

# harmful_ratio = 3/5 = 0.60 >= CAUTION_HARMFUL_RATIO(0.60) → CAUTION fires
check("B: allocation_bias_class is NEGATIVE or NEGATIVE_CAUTION",
      r_b["allocation_bias_class"] in (BIAS_NEGATIVE, BIAS_CAUTION))
check("B: allocation_conviction_modifier <= 0.95",
      r_b["allocation_conviction_modifier"] <= MODIFIER_NEGATIVE,
      f"modifier={r_b['allocation_conviction_modifier']}")
check("B: feedback_status == READY",                   r_b["feedback_status"] == FEEDBACK_READY)
check("B: harmful_count == 3",                         r_b["harmful_count"] == 3)
check("B: net_outcome_signal < 0",                     r_b["net_outcome_signal"] < 0)


# ---------------------------------------------------------------------------
# SCENARIO C — Mixed evidence: 2×HELPFUL, 1×NEUTRAL, 2×HARMFUL
# net_signal = (2-2)/5 = 0.0 → NEUTRAL
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO C — Mixed evidence (2×HELPFUL, 1×NEUTRAL, 2×HARMFUL)")
print(f"{'='*72}")
recs_c = make_ready(n_helpful=2, n_neutral=1, n_harmful=2, sk="S_MIX", prefix="MIX_C")
r_c = run_integration(recs_c, "S_MIX")
print(f"  bias={r_c['allocation_bias_class']}  net_signal={r_c['net_outcome_signal']}")

check("C: allocation_bias_class == NEUTRAL",           r_c["allocation_bias_class"] == BIAS_NEUTRAL)
check("C: allocation_conviction_modifier == 1.00",     r_c["allocation_conviction_modifier"] == MODIFIER_NEUTRAL)
check("C: net_outcome_signal == 0.0",                  r_c["net_outcome_signal"] == 0.0)
check("C: cooldown_flag == False",                     r_c["cooldown_flag"] is False)
check("C: NEUTRAL in integration_reasons",
      "NEUTRAL_SIGNAL" in r_c["integration_reasons"])


# ---------------------------------------------------------------------------
# SCENARIO D — Sparse evidence: 2×HELPFUL (< MIN_READY=3)
# → INSUFFICIENT_EVIDENCE, modifier = NEUTRAL (1.00), no positive bias
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO D — Sparse evidence (2×HELPFUL, < MIN_READY=3)")
print(f"{'='*72}")
recs_d = make_ready(n_helpful=2, n_neutral=0, n_harmful=0, sk="S_SPARSE", prefix="SPARSE_D")
r_d = run_integration(recs_d, "S_SPARSE")
print(f"  bias={r_d['allocation_bias_class']}  modifier={r_d['allocation_conviction_modifier']}")
print(f"  ready_n={r_d['ready_outcomes_count']}  feedback_status={r_d['feedback_status']}")

check("D: allocation_bias_class == INSUFFICIENT_EVIDENCE",
      r_d["allocation_bias_class"] == BIAS_INSUFFICIENT)
check("D: modifier == 1.00 (no positive bias on thin evidence)",
      r_d["allocation_conviction_modifier"] == MODIFIER_NEUTRAL)
check("D: feedback_status == INSUFFICIENT_EVIDENCE",
      r_d["feedback_status"] == FEEDBACK_INSUFFICIENT)
check("D: ready_outcomes_count == 2",                  r_d["ready_outcomes_count"] == 2)
check("D: cooldown_flag == False",                     r_d["cooldown_flag"] is False)
check("D: INSUFFICIENT_EVIDENCE in reasons",
      "INSUFFICIENT_EVIDENCE" in r_d["integration_reasons"])


# ---------------------------------------------------------------------------
# SCENARIO E — No ready outcomes: only PENDING/NOT_EXECUTED/INSUFFICIENT_DATA
# → NO_READY_OUTCOMES, modifier = neutral
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO E — No ready outcomes (only non-READY records)")
print(f"{'='*72}")
recs_e = make_non_ready(n=4, sk="S_NOREADY", prefix="NOREADY_E")
r_e = run_integration(recs_e, "S_NOREADY")
print(f"  bias={r_e['allocation_bias_class']}  ready_n={r_e['ready_outcomes_count']}")
print(f"  feedback_status={r_e['feedback_status']}")

check("E: allocation_bias_class == INSUFFICIENT_EVIDENCE",
      r_e["allocation_bias_class"] == BIAS_INSUFFICIENT)
check("E: modifier == 1.00 (no positive bias)",
      r_e["allocation_conviction_modifier"] == MODIFIER_NEUTRAL)
check("E: feedback_status == NO_READY_OUTCOMES",
      r_e["feedback_status"] == FEEDBACK_NO_READY)
check("E: ready_outcomes_count == 0",                  r_e["ready_outcomes_count"] == 0)
check("E: helpful_count == 0 (non-ready not counted)", r_e["helpful_count"] == 0)
check("E: cooldown_flag == False",                     r_e["cooldown_flag"] is False)


# ---------------------------------------------------------------------------
# SCENARIO F — Cooldown case: 5×HARMFUL, 1×HELPFUL
# harmful_ratio = 5/6 ≈ 0.833 >= CAUTION_HARMFUL_RATIO(0.60) → NEGATIVE_CAUTION
# effective_signal = (1-5)/6 × min(1, 6/6) = -0.667 × 1.0 = -0.667 <= CAUTION_THRESHOLD
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO F — Cooldown case (5×HARMFUL, 1×HELPFUL)")
print(f"{'='*72}")
recs_f = make_ready(n_helpful=1, n_neutral=0, n_harmful=5, sk="S_COOL", prefix="COOL_F")
r_f = run_integration(recs_f, "S_COOL")
print(f"  bias={r_f['allocation_bias_class']}  modifier={r_f['allocation_conviction_modifier']}")
print(f"  cooldown={r_f['cooldown_flag']}  eff_signal={r_f['effective_signal']}")

check("F: allocation_bias_class == NEGATIVE_CAUTION",  r_f["allocation_bias_class"] == BIAS_CAUTION)
check("F: allocation_conviction_modifier == 0.90",     r_f["allocation_conviction_modifier"] == MODIFIER_CAUTION)
check("F: cooldown_flag == True",                      r_f["cooldown_flag"] is True)
check("F: feedback_status == READY",                   r_f["feedback_status"] == FEEDBACK_READY)
check("F: COOLDOWN_ACTIVE in integration_reasons",
      "COOLDOWN_ACTIVE" in r_f["integration_reasons"])
check("F: harmful_count == 5",                         r_f["harmful_count"] == 5)


# ---------------------------------------------------------------------------
# SCENARIO G — Batch multi-key: two strategy keys with different signals
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO G — Batch attribution (two strategy keys)")
print(f"{'='*72}")
recs_g = (
    make_ready(n_helpful=4, n_neutral=1, n_harmful=0, sk="S_GOOD", prefix="TST__S_GOOD") +
    make_ready(n_helpful=0, n_neutral=0, n_harmful=4, sk="S_BAD",  prefix="TST__S_BAD")
)
batch_g = build_feedback_integration(recs_g)

r_good = next(r for r in batch_g if r["strategy_key"] == "S_GOOD")
r_bad  = next(r for r in batch_g if r["strategy_key"] == "S_BAD")

print(f"  S_GOOD: bias={r_good['allocation_bias_class']}  modifier={r_good['allocation_conviction_modifier']}")
print(f"  S_BAD:  bias={r_bad['allocation_bias_class']}  modifier={r_bad['allocation_conviction_modifier']}")

check("G: S_GOOD is POSITIVE",                         r_good["allocation_bias_class"] == BIAS_POSITIVE)
check("G: S_BAD is NEGATIVE_CAUTION",                  r_bad["allocation_bias_class"] == BIAS_CAUTION)
check("G: S_GOOD modifier > S_BAD modifier",
      r_good["allocation_conviction_modifier"] > r_bad["allocation_conviction_modifier"])
check("G: S_BAD cooldown == True",                     r_bad["cooldown_flag"] is True)
check("G: batch returns 2 records",                    len(batch_g) == 2)


# ---------------------------------------------------------------------------
# SCENARIO H — Confidence weight computation
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO H — Confidence weight scaling")
print(f"{'='*72}")
weights = {n: compute_confidence_weight(n) for n in range(8)}
print(f"  weights: {weights}")

check("H: weight(0) == 0.0",                           weights[0] == 0.0)
check("H: weight(3) == 0.5",                           weights[3] == 0.5,
      f"got={weights[3]}")
check(f"H: weight({FULL_CONFIDENCE_AT}) == 1.0",       weights[FULL_CONFIDENCE_AT] == 1.0)
check("H: weight(1) < weight(3) < weight(6)",
      weights[1] < weights[3] < weights[6])
check("H: weight always in [0,1]",
      all(0.0 <= w <= 1.0 for w in weights.values()))


# ---------------------------------------------------------------------------
# MATHEMATICAL INVARIANTS
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  MATHEMATICAL INVARIANTS")
print(f"{'='*72}")

VALID_BIASES   = {BIAS_POSITIVE, BIAS_NEUTRAL, BIAS_NEGATIVE, BIAS_CAUTION, BIAS_INSUFFICIENT}
VALID_STATUSES = {FEEDBACK_READY, FEEDBACK_INSUFFICIENT, FEEDBACK_NO_READY}

# Test various combinations for modifier bounds and valid classification
test_combos = [
    (0, 0, 0),   # empty
    (1, 0, 0),   # too sparse
    (3, 0, 0),   # just enough, all helpful → POSITIVE
    (0, 3, 0),   # all neutral → NEUTRAL
    (0, 0, 3),   # all harmful → CAUTION or NEGATIVE
    (3, 3, 3),   # balanced → NEUTRAL
    (6, 0, 0),   # large positive → POSITIVE
    (0, 0, 6),   # large negative → CAUTION
    (2, 1, 0),   # sparse → INSUFFICIENT
]

for n_h, n_n, n_hm in test_combos:
    recs = make_ready(n_helpful=n_h, n_neutral=n_n, n_harmful=n_hm,
                      sk="INV", prefix=f"INV_{n_h}_{n_n}_{n_hm}")
    # Always include one non-ready record so the key appears in the batch
    recs = recs + [make_attr(f"INV_{n_h}_{n_n}_{n_hm}_NR", "NOT_EXECUTED",
                             eval_status="NOT_EXECUTED", strategy_key="INV")]
    r = run_integration(recs, "INV")

    check(f"INV({n_h},{n_n},{n_hm}): bias in valid set",
          r["allocation_bias_class"] in VALID_BIASES,
          f"bias={r['allocation_bias_class']}")
    check(f"INV({n_h},{n_n},{n_hm}): modifier in band [{MODIFIER_BAND_MIN},{MODIFIER_BAND_MAX}]",
          MODIFIER_BAND_MIN <= r["allocation_conviction_modifier"] <= MODIFIER_BAND_MAX,
          f"mod={r['allocation_conviction_modifier']}")
    check(f"INV({n_h},{n_n},{n_hm}): feedback_status in valid set",
          r["feedback_status"] in VALID_STATUSES,
          f"status={r['feedback_status']}")

# Fail-closed: no POSITIVE bias without MIN_READY records
for n in range(MIN_READY_OUTCOMES):
    r = build_feedback_record("TEST", "MKT", make_ready(n_helpful=n, sk="TEST"))
    check(f"FAIL_CLOSED: no POSITIVE bias with {n} ready records",
          r["allocation_bias_class"] != BIAS_POSITIVE,
          f"bias={r['allocation_bias_class']}")

# Determinism: same input → same output
recs_det = make_ready(n_helpful=3, n_neutral=2, n_harmful=1, sk="DET", prefix="DET")
r1 = run_integration(recs_det, "DET")
r2 = run_integration(recs_det, "DET")
check("DETERMINISM: same input → same modifier",
      r1["allocation_conviction_modifier"] == r2["allocation_conviction_modifier"])
check("DETERMINISM: same input → same bias",
      r1["allocation_bias_class"] == r2["allocation_bias_class"])

# Modifier for INSUFFICIENT → always 1.00 (no positive creep)
r_insuf = build_feedback_record("INSUF", "MKT", [])  # empty list
check("INSUFFICIENT_EVIDENCE: modifier == 1.00",
      r_insuf["allocation_conviction_modifier"] == MODIFIER_NEUTRAL,
      f"mod={r_insuf['allocation_conviction_modifier']}")
check("INSUFFICIENT_EVIDENCE: no cooldown",
      r_insuf["cooldown_flag"] is False)


# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print(f"\n{'#'*72}")
print("  AC61 VALIDATION RESULTS")
print(f"{'#'*72}")

passed = sum(1 for _, ok in results if ok)
total  = len(results)
for label, ok in results:
    print(f"  [{'PASS' if ok else 'FAIL'}]  {label}")

print(f"\n  {'='*50}")
print(f"  TOTAL: {passed}/{total} PASSED {'✓ ALL OK' if passed == total else '✗ FAILURES'}")

print(f"\n{'#'*72}")
print("  INTERPRETATIE")
print(f"{'#'*72}")

pos_ok   = all(ok for lbl, ok in results if lbl.startswith("A:"))
neg_ok   = all(ok for lbl, ok in results if lbl.startswith("B:"))
mix_ok   = all(ok for lbl, ok in results if lbl.startswith("C:"))
sparse_ok = all(ok for lbl, ok in results if lbl.startswith("D:"))
noready_ok = all(ok for lbl, ok in results if lbl.startswith("E:"))
cool_ok  = all(ok for lbl, ok in results if lbl.startswith("F:"))
batch_ok = all(ok for lbl, ok in results if lbl.startswith("G:"))
conf_ok  = all(ok for lbl, ok in results if lbl.startswith("H:"))
inv_ok   = all(ok for lbl, ok in results if lbl.startswith("INV") or
               lbl.startswith("FAIL_CLOSED") or lbl.startswith("DETERMINISM") or
               lbl.startswith("INSUFFICIENT_EVIDENCE"))

print(f"  Positief signaal correct (POSITIVE, mod=1.05):  {'JA' if pos_ok else 'NEE'}")
print(f"  Negatief signaal correct (NEGATIVE/CAUTION):    {'JA' if neg_ok else 'NEE'}")
print(f"  Gemengd signaal correct (NEUTRAL, mod=1.00):    {'JA' if mix_ok else 'NEE'}")
print(f"  Sparse evidence fail-closed (INSUFFICIENT):     {'JA' if sparse_ok else 'NEE'}")
print(f"  No ready outcomes fail-closed:                  {'JA' if noready_ok else 'NEE'}")
print(f"  Cooldown correct (NEGATIVE_CAUTION, flag=True): {'JA' if cool_ok else 'NEE'}")
print(f"  Batch multi-key correct:                        {'JA' if batch_ok else 'NEE'}")
print(f"  Confidence weight scaling correct:              {'JA' if conf_ok else 'NEE'}")
print(f"  Mathematische invarianten OK:                   {'JA' if inv_ok else 'NEE'}")
print()

if passed == total:
    print("  >>> AC-61 GESLAAGD — feedback integration volledig gevalideerd <<<")
else:
    print(f"  >>> {total - passed} FAILURES — pipeline niet volledig gevalideerd <<<")
print()
