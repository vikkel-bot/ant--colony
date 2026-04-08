"""
AC60: Paper Rebalance Outcome Attribution — synthetic validation
Tests attribution logic, outcome labels, fail-closed behaviour, and
all evaluation statuses with controlled inputs. No file I/O.

Scenarios:
  A: Helpful rebalance       — drift improves materially, churn low     → HELPFUL
  B: Neutral rebalance       — marginal improvement, moderate churn     → NEUTRAL
  C: Harmful (drift worse)   — delta in wrong direction, drift worsens  → HARMFUL
  D: Harmful (extreme churn) — extreme churn, no drift improvement      → HARMFUL
  E: Pending window          — not applicable here; instead tests that
                               INSUFFICIENT_DATA fires for missing data  → INSUFFICIENT_DATA
  F: Not executed candidate  — execution_status == SKIPPED              → NOT_EXECUTED
  G: Boundary thresholds     — exactly at improvement thresholds        → NEUTRAL
  H: Batch attribution       — mixed batch via build_attribution_records
  I: Mathematical invariants — score ∈ [-1,1], label always valid

Usage: python test_ac60_rebalance_outcome_attribution.py
"""
import importlib.util
from pathlib import Path

# --- Load attribution production module ---
_mod_path = Path(__file__).parent / "ant_colony" / "build_paper_rebalance_outcome_attribution_lite.py"
_spec = importlib.util.spec_from_file_location("attr", _mod_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

attribute_outcome           = _mod.attribute_outcome
build_attribution_records   = _mod.build_attribution_records
alloc_fit                   = _mod.alloc_fit

LABEL_HELPFUL           = _mod.LABEL_HELPFUL
LABEL_NEUTRAL           = _mod.LABEL_NEUTRAL
LABEL_HARMFUL           = _mod.LABEL_HARMFUL
LABEL_PENDING           = _mod.LABEL_PENDING
LABEL_INSUFFICIENT_DATA = _mod.LABEL_INSUFFICIENT_DATA
LABEL_NOT_EXECUTED      = _mod.LABEL_NOT_EXECUTED

STATUS_READY             = _mod.STATUS_READY
STATUS_INSUFFICIENT_DATA = _mod.STATUS_INSUFFICIENT_DATA
STATUS_NOT_EXECUTED      = _mod.STATUS_NOT_EXECUTED

DRIFT_IMPROVEMENT_MEANINGFUL     = _mod.DRIFT_IMPROVEMENT_MEANINGFUL
ALLOC_FIT_IMPROVEMENT_MEANINGFUL = _mod.ALLOC_FIT_IMPROVEMENT_MEANINGFUL
CHURN_HARMFUL_THRESHOLD          = _mod.CHURN_HARMFUL_THRESHOLD
CHURN_EXTREME_THRESHOLD          = _mod.CHURN_EXTREME_THRESHOLD

EQUITY = 1000.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


def make_exec(pk, exec_status="EXECUTED", delta=50.0, gate="PASS",
              rebal_action="REBALANCE_INCREASE", skip_reason="",
              ts="2026-01-01T12:00:00Z", market="TST", strategy="S1"):
    return {
        "execution_id":            f"20260101T120000Z__{pk}__001",
        "position_key":            pk,
        "market":                  market,
        "strategy_key":            strategy,
        "execution_status":        exec_status,
        "executed_delta_eur":      float(delta),
        "decision_quality_gate":   gate,
        "rebalance_action":        rebal_action,
        "simulation_action":       "SIMULATE_TRANSITION" if exec_status == "EXECUTED" else "NO_ACTION",
        "skip_reason":             skip_reason,
        "ts_utc":                  ts,
    }


def run_attr(pk, exec_status="EXECUTED", delta=50.0, gate="PASS",
             drift_before=None, budget_before=None, budget_after=None,
             churn=None, rebal_action="REBALANCE_INCREASE",
             skip_reason="", equity=EQUITY):
    exec_rec = make_exec(pk, exec_status, delta, gate, rebal_action, skip_reason)
    sim_action = "SIMULATE_TRANSITION" if exec_status == "EXECUTED" else "NO_ACTION"
    return attribute_outcome(
        exec_record=exec_rec,
        drift_before=drift_before,
        budget_before=budget_before,
        budget_after=budget_after,
        churn_cost_proxy=churn,
        equity=equity,
        sim_action=sim_action,
    )


# ---------------------------------------------------------------------------
print("\n" + "#"*72)
print("  AC60 PAPER REBALANCE OUTCOME ATTRIBUTION — SYNTHETIC VALIDATION")
print(f"  equity={EQUITY:.0f}  DRIFT_MEANINGFUL={DRIFT_IMPROVEMENT_MEANINGFUL}"
      f"  CHURN_HARMFUL={CHURN_HARMFUL_THRESHOLD}  CHURN_EXTREME={CHURN_EXTREME_THRESHOLD}")
print("#"*72)


# ---------------------------------------------------------------------------
# SCENARIO A — Helpful rebalance
# Drift = -0.15 (under-allocated 15%), delta = +150 EUR
# drift_after_est = -0.15 + 150/1000 = +0.00 → perfect correction
# churn = 0.10 (low), allocation fit improves from 0.85 → 1.0
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO A — Helpful rebalance (drift corrected, churn low)")
print(f"{'='*72}")
r_a = run_attr(
    "A1", drift_before=-0.15, delta=150.0, churn=0.10,
    budget_before=400.0, budget_after=250.0,
)
print(f"  label={r_a['outcome_label']}  score={r_a['outcome_score']}")
print(f"  drift_before={r_a['drift_before']}  drift_after={r_a['drift_after']}")
print(f"  drift_improvement={r_a['drift_improvement']}  alloc_fit_improvement={r_a['allocation_fit_improvement']}")

check("A: outcome_label == HELPFUL",               r_a["outcome_label"] == LABEL_HELPFUL)
check("A: evaluation_status == READY",             r_a["evaluation_status"] == STATUS_READY)
check("A: drift_improvement > MEANINGFUL",
      r_a["drift_improvement"] >= DRIFT_IMPROVEMENT_MEANINGFUL,
      f"drift_imp={r_a['drift_improvement']:.4f}")
check("A: alloc_fit_after > alloc_fit_before",
      r_a["allocation_fit_after"] > r_a["allocation_fit_before"])
check("A: outcome_score > 0",                      r_a["outcome_score"] > 0)
check("A: BETTER_THAN_BASELINE in attribution",
      "BETTER_THAN_BASELINE" in r_a["attribution_reasons"])
check("A: budget_pressure_change < 0 (consumed)",
      r_a["budget_pressure_change"] < 0)
check("A: paper_only == True",                     r_a["paper_only"] is True)


# ---------------------------------------------------------------------------
# SCENARIO B — Neutral rebalance
# Drift = -0.04 (slightly under), delta = +10 EUR
# drift_after_est = -0.04 + 0.01 = -0.03 → improvement = 0.01 < MEANINGFUL (0.02)
# Improvement below threshold → NEUTRAL
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO B — Neutral rebalance (marginal improvement below threshold)")
print(f"{'='*72}")
r_b = run_attr(
    "B1", drift_before=-0.04, delta=10.0, churn=0.10,
    budget_before=400.0, budget_after=390.0,
)
print(f"  label={r_b['outcome_label']}  score={r_b['outcome_score']}")
print(f"  drift_improvement={r_b['drift_improvement']:.4f}")

check("B: outcome_label == NEUTRAL",               r_b["outcome_label"] == LABEL_NEUTRAL)
check("B: evaluation_status == READY",             r_b["evaluation_status"] == STATUS_READY)
check("B: drift_improvement >= 0 (positive)",      r_b["drift_improvement"] >= 0,
      f"drift_imp={r_b['drift_improvement']:.4f}")
check("B: drift_improvement < MEANINGFUL",
      r_b["drift_improvement"] < DRIFT_IMPROVEMENT_MEANINGFUL,
      f"drift_imp={r_b['drift_improvement']:.4f}")


# ---------------------------------------------------------------------------
# SCENARIO C — Harmful rebalance (drift worsens)
# Drift = -0.10 (under-allocated), delta = -80 EUR (wrong direction, reducing further)
# drift_after_est = -0.10 - 0.08 = -0.18 → drift gets worse
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO C — Harmful rebalance (wrong direction, drift worsens)")
print(f"{'='*72}")
r_c = run_attr(
    "C1", drift_before=-0.10, delta=-80.0, churn=0.20,
    budget_before=400.0, budget_after=320.0,
)
print(f"  label={r_c['outcome_label']}  score={r_c['outcome_score']}")
print(f"  drift_before={r_c['drift_before']}  drift_after={r_c['drift_after']}")
print(f"  drift_improvement={r_c['drift_improvement']:.4f}")

check("C: outcome_label == HARMFUL",               r_c["outcome_label"] == LABEL_HARMFUL)
check("C: evaluation_status == READY",             r_c["evaluation_status"] == STATUS_READY)
check("C: drift_improvement < 0 (drift worsened)", r_c["drift_improvement"] < 0,
      f"drift_imp={r_c['drift_improvement']:.4f}")
check("C: abs(drift_after) > abs(drift_before)",
      abs(r_c["drift_after"]) > abs(r_c["drift_before"]))
check("C: WORSE_THAN_BASELINE in attribution",
      "WORSE_THAN_BASELINE" in r_c["attribution_reasons"])
check("C: outcome_score < 0",                      r_c["outcome_score"] < 0)


# ---------------------------------------------------------------------------
# SCENARIO D — Harmful rebalance (extreme churn, negligible improvement)
# Drift = -0.025 (tiny drift), delta = +500 EUR (massively oversized)
# drift_after_est = -0.025 + 0.50 = +0.475 → huge overshoot
# churn = 0.60 (EXTREME)
# Note: in this case drift actually worsens due to overshoot → HARMFUL
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO D — Harmful rebalance (extreme churn, overshoot)")
print(f"{'='*72}")
r_d = run_attr(
    "D1", drift_before=-0.025, delta=500.0, churn=0.60,
    budget_before=400.0, budget_after=-100.0,
)
print(f"  label={r_d['outcome_label']}  score={r_d['outcome_score']}")
print(f"  drift_before={r_d['drift_before']}  drift_after={r_d['drift_after']}")
print(f"  drift_improvement={r_d['drift_improvement']:.4f}  churn={r_d['churn_cost_proxy']}")

check("D: outcome_label == HARMFUL",               r_d["outcome_label"] == LABEL_HARMFUL)
check("D: churn_cost_proxy == 0.60",               r_d["churn_cost_proxy"] == 0.60)
check("D: drift_improvement < MEANINGFUL (overshoot)",
      r_d["drift_improvement"] < DRIFT_IMPROVEMENT_MEANINGFUL,
      f"drift_imp={r_d['drift_improvement']:.4f}")


# ---------------------------------------------------------------------------
# SCENARIO E — Insufficient data (missing drift_before)
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO E — Insufficient data (drift_before missing)")
print(f"{'='*72}")
r_e1 = run_attr(
    "E1", drift_before=None, delta=50.0,
    budget_before=400.0, budget_after=350.0, churn=0.10,
)
print(f"  label={r_e1['outcome_label']}  eval_status={r_e1['evaluation_status']}")
check("E1: outcome_label == INSUFFICIENT_DATA",    r_e1["outcome_label"] == LABEL_INSUFFICIENT_DATA)
check("E1: evaluation_status == INSUFFICIENT_DATA", r_e1["evaluation_status"] == STATUS_INSUFFICIENT_DATA)
check("E1: outcome_score is None",                 r_e1["outcome_score"] is None)
check("E1: drift_improvement is None",             r_e1["drift_improvement"] is None)

# Missing equity (equity=0)
r_e2 = run_attr(
    "E2", drift_before=-0.10, delta=50.0,
    budget_before=400.0, budget_after=350.0, churn=0.10,
    equity=0.0,
)
check("E2: INSUFFICIENT_DATA when equity=0",
      r_e2["outcome_label"] == LABEL_INSUFFICIENT_DATA)

# Missing delta (delta=0 → too small to evaluate)
r_e3 = run_attr(
    "E3", drift_before=-0.10, delta=0.0,
    budget_before=400.0, budget_after=400.0, churn=0.0,
)
check("E3: INSUFFICIENT_DATA when delta=0",
      r_e3["outcome_label"] == LABEL_INSUFFICIENT_DATA)


# ---------------------------------------------------------------------------
# SCENARIO F — Not executed (SKIPPED in AC59)
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO F — Not executed candidate (SKIPPED in AC59)")
print(f"{'='*72}")
r_f = run_attr(
    "F1", exec_status="SKIPPED", delta=0.0, gate="HOLD",
    drift_before=-0.10, churn=0.0,
    skip_reason="GATE_NOT_PASS",
)
print(f"  label={r_f['outcome_label']}  eval_status={r_f['evaluation_status']}")
check("F: outcome_label == NOT_EXECUTED",           r_f["outcome_label"] == LABEL_NOT_EXECUTED)
check("F: evaluation_status == NOT_EXECUTED",       r_f["evaluation_status"] == STATUS_NOT_EXECUTED)
check("F: outcome_score is None",                   r_f["outcome_score"] is None)
check("F: drift_improvement is None",               r_f["drift_improvement"] is None)
check("F: attribution_reasons contains NOT_EXECUTED",
      "NOT_EXECUTED" in r_f["attribution_reasons"])
check("F: attribution_reasons contains skip_reason",
      "GATE_NOT_PASS" in r_f["attribution_reasons"])


# ---------------------------------------------------------------------------
# SCENARIO G — Boundary thresholds
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO G — Boundary threshold cases")
print(f"{'='*72}")

# G1: improvement exactly at DRIFT_IMPROVEMENT_MEANINGFUL → HELPFUL boundary
# drift_before=-0.05, delta=+20 → drift_after=-0.03 → improvement=0.02 = MEANINGFUL
r_g1 = run_attr("G1", drift_before=-0.05, delta=20.0, churn=0.0,
                budget_before=400.0, budget_after=380.0)
check("G1: drift_improvement == MEANINGFUL (boundary)",
      abs(r_g1["drift_improvement"] - DRIFT_IMPROVEMENT_MEANINGFUL) < 1e-9,
      f"drift_imp={r_g1['drift_improvement']}")
check("G1: outcome_label HELPFUL (at boundary, no churn)",
      r_g1["outcome_label"] == LABEL_HELPFUL,
      f"label={r_g1['outcome_label']}")

# G2: improvement just below MEANINGFUL → NEUTRAL
# drift_before=-0.05, delta=+15 → drift_after=-0.035 → improvement=0.015 < 0.02
r_g2 = run_attr("G2", drift_before=-0.05, delta=15.0, churn=0.0,
                budget_before=400.0, budget_after=385.0)
check("G2: drift_improvement < MEANINGFUL",
      r_g2["drift_improvement"] < DRIFT_IMPROVEMENT_MEANINGFUL,
      f"drift_imp={r_g2['drift_improvement']}")
check("G2: outcome_label NEUTRAL (below boundary)",
      r_g2["outcome_label"] == LABEL_NEUTRAL,
      f"label={r_g2['outcome_label']}")

# G3: CHURN at HARMFUL boundary with meaningful drift improvement → NEUTRAL
# (meaningful improvement but high churn → not fully HELPFUL)
r_g3 = run_attr("G3", drift_before=-0.15, delta=150.0, churn=CHURN_HARMFUL_THRESHOLD,
                budget_before=400.0, budget_after=250.0)
check("G3: high churn with meaningful improvement → NEUTRAL",
      r_g3["outcome_label"] == LABEL_NEUTRAL,
      f"label={r_g3['outcome_label']} churn={r_g3['churn_cost_proxy']}")


# ---------------------------------------------------------------------------
# SCENARIO H — Batch attribution via build_attribution_records
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO H — Batch attribution (mixed candidates)")
print(f"{'='*72}")
exec_records = [
    make_exec("H_EXEC1", "EXECUTED", delta=100.0, gate="PASS"),
    make_exec("H_EXEC2", "EXECUTED", delta=50.0,  gate="PASS"),
    make_exec("H_SKIP",  "SKIPPED",  delta=0.0,   gate="HOLD",
              skip_reason="GATE_NOT_PASS"),
]
drift_idx = {
    "H_EXEC1": {"drift_pct": -0.15},
    "H_EXEC2": {"drift_pct": -0.03},  # small drift, marginal improvement
    "H_SKIP":  {"drift_pct": -0.10},
}
sim_idx = {
    "H_EXEC1": {"budget_pressure_before_eur": 400.0, "budget_pressure_after_eur": 300.0,
                "simulation_action": "SIMULATE_TRANSITION"},
    "H_EXEC2": {"budget_pressure_before_eur": 300.0, "budget_pressure_after_eur": 250.0,
                "simulation_action": "SIMULATE_TRANSITION"},
    "H_SKIP":  {"budget_pressure_before_eur": 400.0, "budget_pressure_after_eur": 400.0,
                "simulation_action": "NO_ACTION"},
}
dq_idx = {
    "H_EXEC1": {"churn_penalty": 0.10},
    "H_EXEC2": {"churn_penalty": 0.10},
    "H_SKIP":  {"churn_penalty": 0.20},
}

batch = build_attribution_records(exec_records, drift_idx, sim_idx, dq_idx, EQUITY)

def get_batch(pk): return next(r for r in batch if r["position_key"] == pk)

r_h1 = get_batch("H_EXEC1")
r_h2 = get_batch("H_EXEC2")
r_hs = get_batch("H_SKIP")

print(f"  H_EXEC1: label={r_h1['outcome_label']}  drift_imp={r_h1['drift_improvement']}")
print(f"  H_EXEC2: label={r_h2['outcome_label']}  drift_imp={r_h2['drift_improvement']}")
print(f"  H_SKIP:  label={r_hs['outcome_label']}  eval={r_hs['evaluation_status']}")

check("H: H_EXEC1 → HELPFUL (large drift improvement, low churn)",
      r_h1["outcome_label"] == LABEL_HELPFUL)
check("H: H_EXEC2 → NEUTRAL (small drift, below meaningful)",
      r_h2["outcome_label"] == LABEL_NEUTRAL)
check("H: H_SKIP → NOT_EXECUTED",
      r_hs["outcome_label"] == LABEL_NOT_EXECUTED)
check("H: batch returns 3 records",  len(batch) == 3)
check("H: H_EXEC1 eval_status READY", r_h1["evaluation_status"] == STATUS_READY)


# ---------------------------------------------------------------------------
# SCENARIO I — Mathematical invariants
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  MATHEMATICAL INVARIANTS")
print(f"{'='*72}")

VALID_LABELS   = {LABEL_HELPFUL, LABEL_NEUTRAL, LABEL_HARMFUL,
                  LABEL_PENDING, LABEL_INSUFFICIENT_DATA, LABEL_NOT_EXECUTED}
VALID_STATUSES = {STATUS_READY, STATUS_INSUFFICIENT_DATA, STATUS_NOT_EXECUTED,
                  _mod.STATUS_PENDING_WINDOW}

test_cases = [
    # (drift_before, delta, churn, exec_status, equity)
    (-0.20, 200.0,  0.10,  "EXECUTED",  1000.0),   # helpful
    (-0.02, 10.0,   0.20,  "EXECUTED",  1000.0),   # neutral (small)
    (-0.10, -100.0, 0.20,  "EXECUTED",  1000.0),   # harmful (drift worse)
    (0.15,  -150.0, 0.10,  "EXECUTED",  1000.0),   # helpful (over-alloc, reduce)
    (None,  50.0,   0.10,  "EXECUTED",  1000.0),   # insufficient data
    (-0.10, 0.0,    0.0,   "SKIPPED",   1000.0),   # not executed
    (-0.10, 50.0,   0.60,  "EXECUTED",  1000.0),   # potentially harmful (extreme churn)
    (-0.30, 300.0,  0.0,   "EXECUTED",  1000.0),   # helpful (large correction, no churn)
]

for i, (drift_b, delta, churn, status, eq) in enumerate(test_cases):
    r = run_attr(f"INV_{i}", exec_status=status, delta=delta,
                 drift_before=drift_b, churn=churn, equity=eq)
    check(f"INV_{i}: label in valid set",
          r["outcome_label"] in VALID_LABELS,
          f"label={r['outcome_label']}")
    check(f"INV_{i}: eval_status in valid set",
          r["evaluation_status"] in VALID_STATUSES,
          f"status={r['evaluation_status']}")
    if r["outcome_score"] is not None:
        check(f"INV_{i}: score in [-1,1]",
              -1.0 <= r["outcome_score"] <= 1.0,
              f"score={r['outcome_score']}")

# alloc_fit always in [0, 1]
for drift_v in [-1.0, -0.5, 0.0, 0.5, 1.0, 2.0]:
    af = alloc_fit(drift_v)
    check(f"alloc_fit({drift_v}) in [0,1]",
          0.0 <= af <= 1.0, f"af={af}")

# SKIPPED records never get a non-None score
for skip_reason in ["GATE_NOT_PASS", "SIMULATION_NO_ACTION", "REBALANCE_ACTION_HOLD"]:
    r = run_attr("SKIP_INV", exec_status="SKIPPED", skip_reason=skip_reason,
                 drift_before=-0.10, delta=0.0, churn=0.0)
    check(f"SKIPPED with {skip_reason} has no score",
          r["outcome_score"] is None)

# Fail-closed: no HELPFUL/NEUTRAL/HARMFUL label without READY status
all_test_records = [r_a, r_b, r_c, r_d, r_e1, r_e2, r_e3, r_f] + batch
for r in all_test_records:
    if r["outcome_label"] in (LABEL_HELPFUL, LABEL_NEUTRAL, LABEL_HARMFUL):
        check(f"READY status required for {r['position_key']} label {r['outcome_label']}",
              r["evaluation_status"] == STATUS_READY,
              f"status={r['evaluation_status']}")


# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print(f"\n{'#'*72}")
print("  AC60 VALIDATION RESULTS")
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

helpful_ok   = all(ok for lbl, ok in results if lbl.startswith("A:"))
neutral_ok   = all(ok for lbl, ok in results if lbl.startswith("B:"))
harmful_ok   = all(ok for lbl, ok in results if lbl.startswith(("C:", "D:")))
insuf_ok     = all(ok for lbl, ok in results if lbl.startswith("E"))
not_exec_ok  = all(ok for lbl, ok in results if lbl.startswith("F:"))
boundary_ok  = all(ok for lbl, ok in results if lbl.startswith("G"))
batch_ok     = all(ok for lbl, ok in results if lbl.startswith("H:"))
invariant_ok = all(ok for lbl, ok in results if lbl.startswith("INV") or "alloc_fit" in lbl
                   or "SKIPPED with" in lbl or "READY status" in lbl)

print(f"  Helpful rebalance correct gelabeld:          {'JA' if helpful_ok else 'NEE'}")
print(f"  Neutral rebalance correct gelabeld:          {'JA' if neutral_ok else 'NEE'}")
print(f"  Harmful rebalance correct gelabeld:          {'JA' if harmful_ok else 'NEE'}")
print(f"  Fail-closed (insufficient data):             {'JA' if insuf_ok else 'NEE'}")
print(f"  NOT_EXECUTED correct behandeld:              {'JA' if not_exec_ok else 'NEE'}")
print(f"  Grenswaarden correct:                        {'JA' if boundary_ok else 'NEE'}")
print(f"  Batch attributie correct:                    {'JA' if batch_ok else 'NEE'}")
print(f"  Mathematische invarianten OK:                {'JA' if invariant_ok else 'NEE'}")
print()

if passed == total:
    print("  >>> AC-60 GESLAAGD — outcome attribution pipeline volledig gevalideerd <<<")
else:
    print(f"  >>> {total - passed} FAILURES — attributie pipeline niet volledig gevalideerd <<<")
print()
