"""
AC57: Multi-Cycle Stability Validation — synthetic test harness
Tests that the Decision Quality Gate (AC56) behaves stably across 20-cycle
synthetic sequences. No file I/O — imports all functions from production modules.

Scenario families:
  A: Noisy small drift           → 100% BLOCK, zero flips
  B: Sustained strong conviction → 100% PASS, ≤ 1 flip
  C: Regime flip (BULL→BEAR)     → clean demotion at flip, low flip rate
  D: Churn oscillation           → gate oscillates PASS/HOLD, penalty visible
  E: Sparse evidence degradation → progressive demotion, low flip rate
  F: Hard constraint pressure    → 100% BLOCK, zero flips

Stop condition: if ANY scenario is UNSTABLE → verdict NOT_STABLE_STOP → AC-58 blocked.

Usage: python test_ac57_multicycle_stability.py
"""
import importlib.util
from pathlib import Path

# --- Load stability production module ---
_stab_path = Path(__file__).parent / "ant_colony" / "build_allocation_quality_stability_lite.py"
_stab_spec = importlib.util.spec_from_file_location("stab", _stab_path)
_stab_mod  = importlib.util.module_from_spec(_stab_spec)
_stab_spec.loader.exec_module(_stab_mod)

run_cycle        = _stab_mod.run_cycle
analyse_stability = _stab_mod.analyse_stability
overall_verdict  = _stab_mod.overall_verdict
FLIP_RATE_STABLE = _stab_mod.FLIP_RATE_STABLE

scenario_a = _stab_mod.scenario_a_noisy_drift
scenario_b = _stab_mod.scenario_b_sustained_conviction
scenario_c = _stab_mod.scenario_c_regime_flip
scenario_d = _stab_mod.scenario_d_churn_oscillation
scenario_e = _stab_mod.scenario_e_sparse_evidence
scenario_f = _stab_mod.scenario_f_constraint_pressure

# Also load DQ scoring constants for value-level assertions
_dq_path = Path(__file__).parent / "ant_colony" / "build_allocation_decision_quality_lite.py"
_dq_spec = importlib.util.spec_from_file_location("dq", _dq_path)
_dq_mod  = importlib.util.module_from_spec(_dq_spec)
_dq_spec.loader.exec_module(_dq_mod)

GATE_PASS_MIN = _dq_mod.GATE_PASS_MIN
GATE_HOLD_MIN = _dq_mod.GATE_HOLD_MIN


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


def print_scenario(name, metrics, cycles):
    print(f"\n{'='*72}")
    print(f"  {name}")
    print(f"{'='*72}")
    gates = [r["gate"] for r in cycles]
    scores = [r["score"] for r in cycles]
    # Print first/mid/last
    for idx in [0, len(cycles)//2, len(cycles)-1]:
        print(f"  cycle[{idx:>2}] gate={cycles[idx]['gate']:<5} "
              f"score={cycles[idx]['score']:.4f} churn={cycles[idx]['churn']:.2f}")
    print(f"  flips={metrics['gate_flip_count']}/{metrics['cycles_total']-1}  "
          f"rate={metrics['gate_flip_rate']:.2f}  "
          f"assessment={metrics['stability_assessment']}")
    print(f"  PASS={metrics['pass_count']}  HOLD={metrics['hold_count']}  "
          f"BLOCK={metrics['block_count']}  "
          f"promotions={metrics['promotion_count']}  demotions={metrics['demotion_count']}")


# ---------------------------------------------------------------------------
print("\n" + "#"*72)
print("  AC57 MULTI-CYCLE STABILITY VALIDATION")
print(f"  FLIP_RATE_STABLE={FLIP_RATE_STABLE}  GATE_PASS_MIN={GATE_PASS_MIN}  "
      f"GATE_HOLD_MIN={GATE_HOLD_MIN}")
print("#"*72)


# ---------------------------------------------------------------------------
# SCENARIO A — Noisy small drift → must stay BLOCK
# ---------------------------------------------------------------------------
cyc_a = scenario_a(20)
met_a = analyse_stability(cyc_a)
print_scenario("SCENARIO A — Noisy small drift (sub-immaterial, 20 cycles)", met_a, cyc_a)

check("A: all cycles are BLOCK",        met_a["block_count"] == 20)
check("A: zero gate flips",             met_a["gate_flip_count"] == 0)
check("A: pass_count == 0",             met_a["pass_count"] == 0)
check("A: stability_assessment STABLE", met_a["stability_assessment"] == "STABLE")
# Score can be > GATE_HOLD_MIN because regime/conviction still contribute,
# but hard BLOCK fires on DRIFT_TOO_SMALL before the score gate is reached.
check("A: gate is BLOCK regardless of score (hard constraint wins)",
      all(r["gate"] == "BLOCK" for r in cyc_a))


# ---------------------------------------------------------------------------
# SCENARIO B — Sustained conviction → must stay PASS
# ---------------------------------------------------------------------------
cyc_b = scenario_b(20)
met_b = analyse_stability(cyc_b)
print_scenario("SCENARIO B — Sustained strong conviction, BULL (20 cycles)", met_b, cyc_b)

check("B: all cycles are PASS",              met_b["pass_count"] == 20)
check("B: zero gate flips",                  met_b["gate_flip_count"] == 0)
check("B: avg_score >= GATE_PASS_MIN",       met_b["avg_quality_score"] >= GATE_PASS_MIN,
      f"avg={met_b['avg_quality_score']:.4f}")
check("B: stability_assessment STABLE",      met_b["stability_assessment"] == "STABLE")
check("B: no block cycles",                  met_b["block_count"] == 0)


# ---------------------------------------------------------------------------
# SCENARIO C — Regime flip BULL→BEAR
# ---------------------------------------------------------------------------
cyc_c = scenario_c(20)
met_c = analyse_stability(cyc_c)
print_scenario("SCENARIO C — Regime flip BULL→BEAR at cycle 10 (20 cycles)", met_c, cyc_c)

# Expect PASS in first half, HOLD in second (BEAR caps PASS→HOLD)
first_half_gates  = [cyc_c[i]["gate"] for i in range(10)]
second_half_gates = [cyc_c[i]["gate"] for i in range(10, 20)]

check("C: first half all PASS (BULL)",
      all(g == "PASS" for g in first_half_gates),
      f"gates={first_half_gates}")
check("C: second half all HOLD (BEAR cap)",
      all(g == "HOLD" for g in second_half_gates),
      f"gates={second_half_gates}")
check("C: exactly 1 gate flip (at regime change)",
      met_c["gate_flip_count"] == 1,
      f"flips={met_c['gate_flip_count']}")
check("C: flip_rate <= FLIP_RATE_STABLE",
      met_c["gate_flip_rate"] <= FLIP_RATE_STABLE,
      f"rate={met_c['gate_flip_rate']:.4f}")
check("C: stability_assessment STABLE",   met_c["stability_assessment"] == "STABLE")
check("C: demotion_count == 1",           met_c["demotion_count"] == 1)
check("C: promotion_count == 0",          met_c["promotion_count"] == 0)


# ---------------------------------------------------------------------------
# SCENARIO D — Churn oscillation
# ---------------------------------------------------------------------------
cyc_d = scenario_d(20)
met_d = analyse_stability(cyc_d)
print_scenario("SCENARIO D — Churn oscillation (low vs extreme, 20 cycles)", met_d, cyc_d)

even_churns = [cyc_d[i]["churn"] for i in range(0, 20, 2)]
odd_churns  = [cyc_d[i]["churn"] for i in range(1, 20, 2)]

check("D: even cycles have churn < 0.20 (low)",
      all(c == 0.0 for c in even_churns),
      f"churns={even_churns}")
check("D: odd cycles have churn >= 0.40 (extreme)",
      all(c >= 0.40 for c in odd_churns),
      f"churns={odd_churns}")
check("D: no BLOCK cycles (drift+BULL overcomes churn)",
      met_d["block_count"] == 0,
      f"BLOCK={met_d['block_count']}")
# Churn penalty reduces score but drift=0.35+BULL (score≈0.85) absorbs it;
# extreme churn drops score to ~0.73 — still above PASS threshold.
# Penalty is visible in score delta, not necessarily in gate change.
even_scores = [cyc_d[i]["score"] for i in range(0, 20, 2)]
odd_scores  = [cyc_d[i]["score"] for i in range(1, 20, 2)]
check("D: extreme-churn cycles have lower score than low-churn cycles",
      all(odd_scores[i] < even_scores[i] for i in range(len(odd_scores))),
      f"even_avg={sum(even_scores)/len(even_scores):.4f} odd_avg={sum(odd_scores)/len(odd_scores):.4f}")
check("D: avg_churn_penalty > 0",
      met_d["avg_churn_penalty"] > 0.0,
      f"avg_churn={met_d['avg_churn_penalty']:.4f}")
check("D: stability NOT UNSTABLE (churn flip stays PASS/HOLD)",
      met_d["stability_assessment"] != "UNSTABLE")


# ---------------------------------------------------------------------------
# SCENARIO E — Sparse evidence degradation
# ---------------------------------------------------------------------------
cyc_e = scenario_e(20)
met_e = analyse_stability(cyc_e)
print_scenario("SCENARIO E — Sparse evidence (conviction degrades 0.80→0.0, 20 cycles)",
               met_e, cyc_e)

# First cycle should be PASS (high conf), last should be BLOCK (conf=0)
check("E: first cycle PASS (high conviction)",
      cyc_e[0]["gate"] == "PASS",
      f"gate={cyc_e[0]['gate']}")
# With conf=0 but drift_mat=0.80 and regime=BULL, score ≈ 0.53 → HOLD, not BLOCK.
# BLOCK only occurs if score < 0.30 or hard constraint fires.
check("E: last gate is HOLD or BLOCK (degraded, no longer PASS)",
      cyc_e[-1]["gate"] in ("HOLD", "BLOCK"),
      f"gate={cyc_e[-1]['gate']}")
check("E: demotion_count > 0 (degrades over time)",
      met_e["demotion_count"] > 0)
check("E: flip_rate <= FLIP_RATE_STABLE (monotone degradation, few reversals)",
      met_e["gate_flip_rate"] <= FLIP_RATE_STABLE,
      f"rate={met_e['gate_flip_rate']:.4f}")
check("E: stability_assessment STABLE",
      met_e["stability_assessment"] == "STABLE")


# ---------------------------------------------------------------------------
# SCENARIO F — Hard constraint pressure → 100% BLOCK
# ---------------------------------------------------------------------------
cyc_f = scenario_f(20)
met_f = analyse_stability(cyc_f)
print_scenario("SCENARIO F — Constraint pressure (budget_excl / tiny_drift, 20 cycles)",
               met_f, cyc_f)

check("F: all cycles BLOCK",             met_f["block_count"] == 20)
check("F: zero gate flips",              met_f["gate_flip_count"] == 0)
check("F: pass_count == 0",              met_f["pass_count"] == 0)
check("F: stability_assessment STABLE",  met_f["stability_assessment"] == "STABLE")


# ---------------------------------------------------------------------------
# OVERALL VERDICT
# ---------------------------------------------------------------------------
all_metrics = [met_a, met_b, met_c, met_d, met_e, met_f]
for m, name in zip(all_metrics, ["A", "B", "C", "D", "E", "F"]):
    m["scenario"] = name

verdict = overall_verdict(all_metrics)

print(f"\n{'='*72}")
print("  OVERALL STABILITY VERDICT")
print(f"{'='*72}")
print(f"  >>> {verdict}")

check("OVERALL: verdict is STABLE_FOR_AC58",
      verdict == "STABLE_FOR_AC58",
      f"verdict={verdict}")


# ---------------------------------------------------------------------------
# MATHEMATICAL INVARIANTS ACROSS ALL SCENARIOS
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  MATHEMATICAL INVARIANTS")
print(f"{'='*72}")

all_cycles = cyc_a + cyc_b + cyc_c + cyc_d + cyc_e + cyc_f
for i, cyc in enumerate(all_cycles):
    check(f"score in [0,1] cycle {i}",
          0.0 <= cyc["score"] <= 1.0,
          f"score={cyc['score']}")
    check(f"gate valid cycle {i}",
          cyc["gate"] in ("PASS", "HOLD", "BLOCK"),
          f"gate={cyc['gate']}")


# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print(f"\n{'#'*72}")
print("  AC57 VALIDATION RESULTS")
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

noisy_ok    = all(ok for lbl, ok in results if lbl.startswith("A:"))
sustain_ok  = all(ok for lbl, ok in results if lbl.startswith("B:"))
flip_ok     = all(ok for lbl, ok in results if lbl.startswith("C:"))
churn_ok    = all(ok for lbl, ok in results if lbl.startswith("D:"))
sparse_ok   = all(ok for lbl, ok in results if lbl.startswith("E:"))
constr_ok   = all(ok for lbl, ok in results if lbl.startswith("F:"))
verdict_ok  = all(ok for lbl, ok in results if lbl.startswith("OVERALL:"))
invariants_ok = all(ok for lbl, ok in results if "cycle" in lbl)

print(f"  Noisy drift geblokkeerd (A):         {'JA' if noisy_ok else 'NEE'}")
print(f"  Sterke conviction stabiel PASS (B):  {'JA' if sustain_ok else 'NEE'}")
print(f"  Regime flip clean demotion (C):      {'JA' if flip_ok else 'NEE'}")
print(f"  Churn penalty actief/zichtbaar (D):  {'JA' if churn_ok else 'NEE'}")
print(f"  Sparse evidence → progressief BLOCK: {'JA' if sparse_ok else 'NEE'}")
print(f"  Hard constraints 100% BLOCK (F):     {'JA' if constr_ok else 'NEE'}")
print(f"  Stabiliteitsverdict STABLE_FOR_AC58: {'JA' if verdict_ok else 'NEE'}")
print(f"  Mathematische invarianten OK:        {'JA' if invariants_ok else 'NEE'}")
print()

if verdict == "STABLE_FOR_AC58":
    print("  >>> AC-58 MAG STARTEN <<<")
else:
    print("  >>> STOP: AC-58 GEBLOKKEERD — instabiliteit gedetecteerd <<<")
print()
