"""
AC56: Decision Quality Gate — synthetic validation
Tests scoring, gate logic and fail-closed behaviour with controlled inputs.
No file I/O — imports all scoring functions from production module.

Scenarios:
  A: Small noisy drift           → HOLD or BLOCK (drift too marginal)
  B: Material drift + conviction → PASS
  C: High drift but budget excl  → BLOCK (hard constraint)
  D: Churn-prone oscillation     → churn penalty visible, no easy PASS
  E: Missing / zero data         → fail-closed, no optimistic PASS

Usage: python test_ac56_decision_quality.py
"""
import importlib.util
from pathlib import Path

# --- Load production module ---
_mod_path = Path(__file__).parent / "ant_colony" / "build_allocation_decision_quality_lite.py"
_spec = importlib.util.spec_from_file_location("dq", _mod_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

score_drift_materiality  = _mod.score_drift_materiality
score_conviction         = _mod.score_conviction
score_regime_compat      = _mod.score_regime_compat
score_churn              = _mod.score_churn
compute_quality_score    = _mod.compute_quality_score
determine_gate           = _mod.determine_gate
to_float                 = _mod.to_float

GATE_PASS_MIN = _mod.GATE_PASS_MIN
GATE_HOLD_MIN = _mod.GATE_HOLD_MIN
W_DRIFT       = _mod.W_DRIFT
W_CONVICTION  = _mod.W_CONVICTION
W_BUDGET      = _mod.W_BUDGET
W_REGIME      = _mod.W_REGIME
W_CHURN_PEN   = _mod.W_CHURN_PEN


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


def run_gate(drift_pct, feedback_conf, regime_type, rebal_selected,
             delta_eur=50.0, actual_eur=100.0, target_eur=150.0) -> dict:
    """Run full scoring + gate pipeline, return result dict."""
    drift_mat, drift_reason    = score_drift_materiality(drift_pct)
    conviction, conv_reason    = score_conviction(feedback_conf, regime_type)
    regime_sc, regime_reason   = score_regime_compat(regime_type)
    churn, churn_reason        = score_churn(delta_eur, actual_eur, target_eur)
    budget_ok                  = 1.0 if rebal_selected else 0.0

    score = compute_quality_score(drift_mat, conviction, budget_ok, regime_sc, churn)

    component_reasons = [drift_reason, conv_reason, regime_reason, churn_reason]
    gate, gate_reasons = determine_gate(
        score, rebal_selected, drift_pct, drift_mat, component_reasons,
        regime_score=regime_sc,
    )
    return {
        "drift_mat": drift_mat, "conviction": conviction, "regime_sc": regime_sc,
        "churn": churn, "budget_ok": budget_ok, "score": score,
        "gate": gate, "reasons": gate_reasons,
    }


def print_scenario(title, res):
    print(f"\n{'='*68}")
    print(f"  {title}")
    print(f"{'='*68}")
    print(f"  drift_mat={res['drift_mat']:.2f}  conviction={res['conviction']:.4f}"
          f"  regime={res['regime_sc']:.2f}  churn_pen={res['churn']:.2f}"
          f"  budget_ok={res['budget_ok']:.1f}")
    print(f"  quality_score={res['score']:.4f}  gate={res['gate']}")
    print(f"  reasons: {' | '.join(res['reasons'])}")


# ---------------------------------------------------------------------------
print("\n" + "#"*68)
print("  AC56 DECISION QUALITY GATE — SYNTHETIC VALIDATION")
print(f"  GATE_PASS_MIN={GATE_PASS_MIN}  GATE_HOLD_MIN={GATE_HOLD_MIN}")
print(f"  weights: drift={W_DRIFT} conv={W_CONVICTION} budget={W_BUDGET}"
      f" regime={W_REGIME} churn_pen={W_CHURN_PEN}")
print("#"*68)

# ---------------------------------------------------------------------------
# SCENARIO A — Small noisy drift, moderate conviction
# ---------------------------------------------------------------------------
# drift_pct=0.03 → below DRIFT_IMMATERIAL (0.05) → score 0.0 → hard BLOCK
res_a1 = run_gate(drift_pct=0.03, feedback_conf=0.6, regime_type="BULL",
                  rebal_selected=True, delta_eur=30.0, actual_eur=100.0, target_eur=103.0)
print_scenario("SCENARIO A1 — tiny drift (3%), budget ok, BULL", res_a1)
check("A1: drift_mat == 0.0 (immaterial)", res_a1["drift_mat"] == 0.0)
check("A1: gate == BLOCK (DRIFT_TOO_SMALL)", res_a1["gate"] == "BLOCK")
check("A1: DRIFT_TOO_SMALL in reasons",
      any("DRIFT_TOO_SMALL" in r for r in res_a1["reasons"]))

# drift_pct=0.07 → marginal (0.05–0.10) → score 0.25, low total → HOLD or BLOCK
res_a2 = run_gate(drift_pct=0.07, feedback_conf=0.35, regime_type="SIDEWAYS",
                  rebal_selected=True, delta_eur=70.0, actual_eur=500.0, target_eur=570.0)
print_scenario("SCENARIO A2 — marginal drift (7%), low conviction, SIDEWAYS", res_a2)
check("A2: drift_mat == 0.25 (marginal)", res_a2["drift_mat"] == 0.25)
check("A2: gate != PASS (weak overall)", res_a2["gate"] != "PASS")
check("A2: score < GATE_PASS_MIN", res_a2["score"] < GATE_PASS_MIN)

# ---------------------------------------------------------------------------
# SCENARIO B — Material drift, strong conviction, BULL, budget ok
# ---------------------------------------------------------------------------
res_b = run_gate(drift_pct=0.45, feedback_conf=0.90, regime_type="BULL",
                 rebal_selected=True, delta_eur=50.0, actual_eur=200.0, target_eur=250.0)
print_scenario("SCENARIO B — extreme drift (45%), strong conviction, BULL", res_b)
check("B: drift_mat == 1.0 (extreme)", res_b["drift_mat"] == 1.00)
check("B: conviction high (feedback=0.90, BULL)", res_b["conviction"] >= 0.85)
check("B: budget_ok == 1.0", res_b["budget_ok"] == 1.0)
check("B: score >= GATE_PASS_MIN", res_b["score"] >= GATE_PASS_MIN)
check("B: gate == PASS", res_b["gate"] == "PASS")
check("B: CONVICTION_STRONG in reasons",
      any("CONVICTION_STRONG" in r for r in res_b["reasons"]))

# Material drift, medium confidence, TREND
res_b2 = run_gate(drift_pct=0.25, feedback_conf=0.75, regime_type="TREND",
                  rebal_selected=True, delta_eur=80.0, actual_eur=200.0, target_eur=280.0)
print_scenario("SCENARIO B2 — large drift (25%), medium conviction, TREND", res_b2)
check("B2: drift_mat == 0.80 (large)", res_b2["drift_mat"] == 0.80)
check("B2: gate is PASS or HOLD (not BLOCK)", res_b2["gate"] in ("PASS", "HOLD"))
check("B2: score >= GATE_HOLD_MIN", res_b2["score"] >= GATE_HOLD_MIN)

# ---------------------------------------------------------------------------
# SCENARIO C — High drift but budget excluded → hard BLOCK
# ---------------------------------------------------------------------------
res_c = run_gate(drift_pct=0.60, feedback_conf=0.95, regime_type="BULL",
                 rebal_selected=False, delta_eur=300.0, actual_eur=100.0, target_eur=400.0)
print_scenario("SCENARIO C — extreme drift, strong conviction, BUDGET EXCLUDED", res_c)
check("C: drift_mat == 1.0", res_c["drift_mat"] == 1.00)
check("C: conviction high", res_c["conviction"] >= 0.85)
check("C: budget_ok == 0.0 (excluded)", res_c["budget_ok"] == 0.0)
check("C: gate == BLOCK (hard constraint)", res_c["gate"] == "BLOCK")
check("C: BUDGET_EXCLUDED in reasons",
      any("BUDGET_EXCLUDED" in r for r in res_c["reasons"]))
check("C: score NOT used as gate (hard BLOCK wins)",
      res_c["gate"] == "BLOCK")   # score might be high but gate is still BLOCK

# ---------------------------------------------------------------------------
# SCENARIO D — Churn-prone / oscillating case
# ---------------------------------------------------------------------------
# Large delta relative to actual position → high churn penalty
res_d1 = run_gate(drift_pct=0.30, feedback_conf=0.70, regime_type="BULL",
                  rebal_selected=True,
                  delta_eur=500.0, actual_eur=50.0, target_eur=550.0)
print_scenario("SCENARIO D1 — large drift, HIGH churn (delta 10× actual)", res_d1)
check("D1: churn == 0.60 (extreme ratio: 500/550=0.909 ≥ CHURN_HIGH 0.90)", res_d1["churn"] == 0.60)
check("D1: churn penalty reduces score vs no-churn baseline",
      res_d1["score"] < compute_quality_score(
          res_d1["drift_mat"], res_d1["conviction"],
          res_d1["budget_ok"], res_d1["regime_sc"], 0.0))
check("D1: gate is PASS or HOLD (churn penalizes but strong drift+BULL survive)",
      res_d1["gate"] in ("PASS", "HOLD"))

# Medium churn — visible but not blocking
res_d2 = run_gate(drift_pct=0.25, feedback_conf=0.80, regime_type="BULL",
                  rebal_selected=True,
                  delta_eur=120.0, actual_eur=200.0, target_eur=320.0)
print_scenario("SCENARIO D2 — large drift, MEDIUM churn (delta 40% of ref)", res_d2)
check("D2: churn == 0.20 (medium ratio ~37%)", res_d2["churn"] == 0.20)
check("D2: gate is PASS or HOLD (churn manageable)", res_d2["gate"] in ("PASS", "HOLD"))

# ---------------------------------------------------------------------------
# SCENARIO E — Missing / zero data → fail-closed
# ---------------------------------------------------------------------------
# All zeros — no conviction, no drift, budget excluded
res_e1 = run_gate(drift_pct=0.0, feedback_conf=0.0, regime_type="UNKNOWN",
                  rebal_selected=False, delta_eur=0.0, actual_eur=0.0, target_eur=0.0)
print_scenario("SCENARIO E1 — all zeros, budget excluded", res_e1)
check("E1: gate == BLOCK", res_e1["gate"] == "BLOCK")
check("E1: score < 0.15 (regime_compat residual only, all else zero)",
      res_e1["score"] < 0.15, f"score={res_e1['score']}")

# Zero conviction but some drift and budget ok — no optimistic PASS
res_e2 = run_gate(drift_pct=0.35, feedback_conf=0.0, regime_type="UNKNOWN",
                  rebal_selected=True, delta_eur=100.0, actual_eur=100.0, target_eur=200.0)
print_scenario("SCENARIO E2 — large drift but conviction=0, regime=UNKNOWN", res_e2)
check("E2: gate != PASS (no conviction)", res_e2["gate"] != "PASS")
check("E2: conviction == 0.0 (feedback 0, UNKNOWN → 0.5 * 0 = 0)", res_e2["conviction"] == 0.0)

# Regime BEAR with strong conviction — gating by regime
res_e3 = run_gate(drift_pct=0.40, feedback_conf=0.90, regime_type="BEAR",
                  rebal_selected=True, delta_eur=100.0, actual_eur=200.0, target_eur=300.0)
print_scenario("SCENARIO E3 — extreme drift, strong conviction, BEAR regime", res_e3)
check("E3: regime_sc == 0.3 (BEAR constrained)", res_e3["regime_sc"] == 0.30)
check("E3: conviction scaled down by BEAR",
      res_e3["conviction"] < 0.50,
      f"conviction={res_e3['conviction']:.4f} (0.9 * 0.3 = 0.27)")
check("E3: gate != PASS (BEAR constrains)", res_e3["gate"] != "PASS")
check("E3: REGIME_ADVERSE in reasons",
      any("REGIME_ADVERSE" in r for r in res_e3["reasons"]))

# ---------------------------------------------------------------------------
# MATHEMATICAL INVARIANTS
# ---------------------------------------------------------------------------
print(f"\n{'='*68}")
print("  MATHEMATICAL INVARIANTS")
print(f"{'='*68}")

# Score is always in [0.0, 1.0]
test_cases = [
    (0.0, 0.0, "UNKNOWN", False, 0.0, 0.0, 0.0),
    (1.0, 1.0, "BULL",    True,  1.0, 0.0, 1.0),
    (0.5, 0.5, "TREND",   True,  0.5, 200.0, 250.0),
    (-0.9, 0.9, "BULL",   True,  900.0, 100.0, 1000.0),  # negative drift (over-allocated)
]
for drift, conf, reg, sel, delta, actual, target in test_cases:
    r = run_gate(drift, conf, reg, sel, delta, actual, target)
    check(f"score in [0,1] for drift={drift} conf={conf}",
          0.0 <= r["score"] <= 1.0, f"score={r['score']}")

# All gate values are valid
for drift, conf, reg, sel, delta, actual, target in test_cases:
    r = run_gate(drift, conf, reg, sel, delta, actual, target)
    check(f"gate is PASS/HOLD/BLOCK for drift={drift}",
          r["gate"] in ("PASS", "HOLD", "BLOCK"), f"gate={r['gate']}")

# Budget excluded always → BLOCK
for drift_v in [0.01, 0.10, 0.50, 0.90]:
    r = run_gate(drift_v, 0.95, "BULL", False, 100.0, 100.0, 200.0)
    check(f"budget excluded always BLOCK (drift={drift_v})",
          r["gate"] == "BLOCK", f"gate={r['gate']}")

# Drift < DRIFT_IMMATERIAL always → BLOCK (even with budget ok)
for drift_v in [0.0, 0.01, 0.03, 0.049]:
    r = run_gate(drift_v, 0.95, "BULL", True, 5.0, 100.0, 105.0)
    check(f"tiny drift always BLOCK (drift={drift_v})",
          r["gate"] == "BLOCK", f"gate={r['gate']}")

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print(f"\n{'#'*68}")
print("  AC56 VALIDATION RESULTS")
print(f"{'#'*68}")

passed = sum(1 for _, ok in results if ok)
total  = len(results)
for label, ok in results:
    print(f"  [{'PASS' if ok else 'FAIL'}]  {label}")

print(f"\n  {'='*46}")
print(f"  TOTAL: {passed}/{total} PASSED {'✓ ALL OK' if passed == total else '✗ FAILURES'}")

print(f"\n{'#'*68}")
print("  INTERPRETATIE")
print(f"{'#'*68}")
noisy_ok    = all(ok for lbl, ok in results if lbl.startswith("A"))
material_ok = all(ok for lbl, ok in results if lbl.startswith("B"))
block_ok    = all(ok for lbl, ok in results if lbl.startswith("C"))
churn_ok    = all(ok for lbl, ok in results if lbl.startswith("D"))
failcl_ok   = all(ok for lbl, ok in results if lbl.startswith("E"))
math_ok     = all(ok for lbl, ok in results if "score in [0,1]" in lbl
                  or "gate is PASS/HOLD/BLOCK" in lbl
                  or "budget excluded" in lbl
                  or "tiny drift" in lbl)
print(f"  Kleine noisy drift geblokkeerd:    {'JA' if noisy_ok else 'NEE'}")
print(f"  Sterke cases doorkomen (PASS):     {'JA' if material_ok else 'NEE'}")
print(f"  Budget constraint blokkeert:       {'JA' if block_ok else 'NEE'}")
print(f"  Churn penalty zichtbaar/actief:    {'JA' if churn_ok else 'NEE'}")
print(f"  Fail-closed bij incomplete data:   {'JA' if failcl_ok else 'NEE'}")
print(f"  Mathematische invarianten OK:      {'JA' if math_ok else 'NEE'}")
print()
