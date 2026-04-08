"""
AC58: Portfolio Transition Simulation — synthetic validation
Tests simulation logic, conflict detection, budget pressure, and turnover
classification with controlled inputs. No file I/O.

Scenarios:
  A: PASS candidate, no conflicts              → SIMULATE_TRANSITION
  B: HOLD candidate                            → NO_ACTION (GATE_NOT_PASS)
  C: BLOCK candidate                           → NO_ACTION (GATE_NOT_PASS)
  D: PASS but budget_excluded                  → NO_ACTION (BUDGET_NOT_SELECTED)
  E: PASS but delta=0                          → NO_ACTION (DELTA_ZERO)
  F: PASS but direction conflict               → NO_ACTION (CONFLICT_DIRECTION)
  G: Multiple PASS, second exceeds sim budget  → first SIMULATE, second NO_ACTION
  H: Turnover classification across thresholds
  I: Budget pressure decreases correctly after each simulation

Stop condition: if hard constraints leak PASS → AC-59 blocked.

Usage: python test_ac58_transition_simulation.py
"""
import importlib.util
from pathlib import Path

# --- Load simulation production module ---
_sim_path = Path(__file__).parent / "ant_colony" / "build_portfolio_transition_simulation_lite.py"
_sim_spec = importlib.util.spec_from_file_location("sim", _sim_path)
_sim_mod  = importlib.util.module_from_spec(_sim_spec)
_sim_spec.loader.exec_module(_sim_mod)

simulate_transitions  = _sim_mod.simulate_transitions
detect_conflicts      = _sim_mod.detect_conflicts
classify_turnover     = _sim_mod.classify_turnover
MAX_SIMULATION_NOTIONAL_PCT = _sim_mod.MAX_SIMULATION_NOTIONAL_PCT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EQUITY = 1000.0
SIM_BUDGET = round(EQUITY * MAX_SIMULATION_NOTIONAL_PCT, 2)  # 400.0

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


def make_dq(pk, gate, score, market="TST", strategy="S1"):
    return {
        "position_key": pk, "market": market, "strategy": strategy,
        "decision_quality_gate": gate, "decision_quality_score": score,
    }


def make_drift(pk, actual, target, alloc_pct=0.10, drift_pct=None):
    if drift_pct is None:
        drift_pct = (actual - target) / max(EQUITY, 1.0)
    return {
        "position_key": pk,
        "actual_notional_eur": actual,
        "target_notional_eur": target,
        "allocation_pct": alloc_pct,
        "drift_pct": drift_pct,
    }


def make_rebal(pk, delta, selected=True):
    return {
        "position_key": pk,
        "rebalance_selected": selected,
        "rebalance_capped_delta_eur": float(delta),
    }


def run_sim(dq_list, drift_list, rebal_list, equity=EQUITY):
    d_idx = {r["position_key"]: r for r in drift_list}
    r_idx = {r["position_key"]: r for r in rebal_list}
    return simulate_transitions(dq_list, d_idx, r_idx, equity)


def get_row(rows, pk):
    for r in rows:
        if r["position_key"] == pk:
            return r
    return None


# ---------------------------------------------------------------------------
print("\n" + "#"*72)
print("  AC58 PORTFOLIO TRANSITION SIMULATION — SYNTHETIC VALIDATION")
print(f"  equity={EQUITY:.0f}  sim_budget={SIM_BUDGET:.0f} EUR"
      f"  (MAX_SIMULATION_NOTIONAL_PCT={MAX_SIMULATION_NOTIONAL_PCT})")
print("#"*72)


# ---------------------------------------------------------------------------
# SCENARIO A — Clean PASS candidate
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO A — PASS candidate, no conflicts")
print(f"{'='*72}")
rows_a = run_sim(
    [make_dq("A1", "PASS", 0.75)],
    [make_drift("A1", actual=80.0, target=100.0, alloc_pct=0.10, drift_pct=-0.02)],
    [make_rebal("A1", delta=20.0, selected=True)],
)
r = get_row(rows_a, "A1")
print(f"  action={r['simulation_action']}  conflicts={r['conflict_flags']}")
print(f"  transition_est={r['transition_notional_estimate']}  turnover={r['turnover_class']}")

check("A: simulation_action == SIMULATE_TRANSITION", r["simulation_action"] == "SIMULATE_TRANSITION")
check("A: no conflict_flags",                        r["conflict_flags"] == "")
check("A: budget_pressure_after < budget_pressure_before",
      r["budget_pressure_after_eur"] < r["budget_pressure_before_eur"])
check("A: transition_notional_estimate == 20.0",     r["transition_notional_estimate"] == 20.0)


# ---------------------------------------------------------------------------
# SCENARIO B — HOLD candidate → blocked
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO B — HOLD candidate → NO_ACTION")
print(f"{'='*72}")
rows_b = run_sim(
    [make_dq("B1", "HOLD", 0.45)],
    [make_drift("B1", actual=80.0, target=100.0, alloc_pct=0.10)],
    [make_rebal("B1", delta=20.0)],
)
r = get_row(rows_b, "B1")
print(f"  action={r['simulation_action']}  conflicts={r['conflict_flags']}")

check("B: simulation_action == NO_ACTION",    r["simulation_action"] == "NO_ACTION")
check("B: GATE_NOT_PASS in conflict_flags",   "GATE_NOT_PASS" in r["conflict_flags"])
check("B: budget unchanged (no sim)",
      r["budget_pressure_after_eur"] == r["budget_pressure_before_eur"])


# ---------------------------------------------------------------------------
# SCENARIO C — BLOCK candidate → blocked
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO C — BLOCK candidate → NO_ACTION")
print(f"{'='*72}")
rows_c = run_sim(
    [make_dq("C1", "BLOCK", 0.15)],
    [make_drift("C1", actual=50.0, target=100.0, alloc_pct=0.10)],
    [make_rebal("C1", delta=50.0)],
)
r = get_row(rows_c, "C1")
check("C: simulation_action == NO_ACTION",   r["simulation_action"] == "NO_ACTION")
check("C: GATE_NOT_PASS in conflict_flags",  "GATE_NOT_PASS" in r["conflict_flags"])


# ---------------------------------------------------------------------------
# SCENARIO D — PASS but budget_excluded
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO D — PASS but rebalance_selected=False → NO_ACTION")
print(f"{'='*72}")
rows_d = run_sim(
    [make_dq("D1", "PASS", 0.70)],
    [make_drift("D1", actual=80.0, target=100.0, alloc_pct=0.10)],
    [make_rebal("D1", delta=20.0, selected=False)],
)
r = get_row(rows_d, "D1")
check("D: simulation_action == NO_ACTION",         r["simulation_action"] == "NO_ACTION")
check("D: BUDGET_NOT_SELECTED in conflict_flags",  "BUDGET_NOT_SELECTED" in r["conflict_flags"])
check("D: GATE_NOT_PASS not in conflicts",         "GATE_NOT_PASS" not in r["conflict_flags"])


# ---------------------------------------------------------------------------
# SCENARIO E — PASS but delta=0
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO E — PASS but delta=0 → NO_ACTION")
print(f"{'='*72}")
rows_e = run_sim(
    [make_dq("E1", "PASS", 0.60)],
    [make_drift("E1", actual=100.0, target=100.0, alloc_pct=0.10)],
    [make_rebal("E1", delta=0.0)],
)
r = get_row(rows_e, "E1")
check("E: simulation_action == NO_ACTION",  r["simulation_action"] == "NO_ACTION")
check("E: DELTA_ZERO in conflict_flags",    "DELTA_ZERO" in r["conflict_flags"])


# ---------------------------------------------------------------------------
# SCENARIO F — PASS but direction conflict (drift says increase, delta says decrease)
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO F — PASS but direction conflict → NO_ACTION")
print(f"{'='*72}")
# drift_pct = +0.05 (actual=150 > target=100 → over-allocated, reduce needed)
# but delta = +30 (increasing) → same sign → CONFLICT_DIRECTION
rows_f = run_sim(
    [make_dq("F1", "PASS", 0.65)],
    [make_drift("F1", actual=150.0, target=100.0, alloc_pct=0.10, drift_pct=0.05)],
    [make_rebal("F1", delta=30.0)],  # wrong direction: should be negative to reduce
)
r = get_row(rows_f, "F1")
print(f"  action={r['simulation_action']}  conflicts={r['conflict_flags']}")
check("F: simulation_action == NO_ACTION",       r["simulation_action"] == "NO_ACTION")
check("F: CONFLICT_DIRECTION in conflict_flags", "CONFLICT_DIRECTION" in r["conflict_flags"])


# ---------------------------------------------------------------------------
# SCENARIO G — Two PASS candidates; second exceeds sim budget
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO G — two PASS candidates; G2 exceeds remaining sim budget")
print(f"{'='*72}")
# G1: score=0.90, delta=350 → fits in 400 budget
# G2: score=0.80, delta=100 → remaining = 50, 100 > 50 → over budget
rows_g = run_sim(
    [make_dq("G1", "PASS", 0.90), make_dq("G2", "PASS", 0.80)],
    # actual < target → under-allocated → drift_pct negative → delta positive (increase)
    [make_drift("G1", 50.0, 400.0, 0.40, drift_pct=-0.35),
     make_drift("G2", 50.0, 150.0, 0.15, drift_pct=-0.10)],
    [make_rebal("G1", 350.0), make_rebal("G2", 100.0)],
)
r1 = get_row(rows_g, "G1")
r2 = get_row(rows_g, "G2")
print(f"  G1: action={r1['simulation_action']} budget_before={r1['budget_pressure_before_eur']}"
      f" budget_after={r1['budget_pressure_after_eur']}")
print(f"  G2: action={r2['simulation_action']} budget_before={r2['budget_pressure_before_eur']}"
      f" conflicts={r2['conflict_flags']}")

check("G: G1 SIMULATE_TRANSITION (higher score, fits budget)",
      r1["simulation_action"] == "SIMULATE_TRANSITION")
check("G: G2 NO_ACTION (over budget after G1 consumed 350)",
      r2["simulation_action"] == "NO_ACTION")
check("G: G2 CONFLICT_OVER_BUDGET",
      "CONFLICT_OVER_BUDGET" in r2["conflict_flags"])
check("G: G1 budget_after == SIM_BUDGET - 350",
      r1["budget_pressure_after_eur"] == SIM_BUDGET - 350.0,
      f"expected={SIM_BUDGET-350.0} got={r1['budget_pressure_after_eur']}")
check("G: G2 budget_before == G1 budget_after",
      r2["budget_pressure_before_eur"] == r1["budget_pressure_after_eur"])


# ---------------------------------------------------------------------------
# SCENARIO H — Turnover classification
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO H — Turnover classification across thresholds")
print(f"{'='*72}")

# reference = max(actual, target, 1)
# ratio = abs(delta) / reference
turnover_cases = [
    # (actual, target, delta, expected_class)
    (200.0, 250.0, 10.0,  "LOW"),      # ratio = 10/250 = 0.04  < 0.20
    (200.0, 250.0, 60.0,  "MEDIUM"),   # ratio = 60/250 = 0.24  < 0.50
    (200.0, 250.0, 160.0, "HIGH"),     # ratio = 160/250 = 0.64 < 0.90
    (200.0, 250.0, 240.0, "EXTREME"),  # ratio = 240/250 = 0.96 >= 0.90
]
for actual, target, delta, expected in turnover_cases:
    reference = max(abs(actual), abs(target), 1.0)
    tc = classify_turnover(abs(delta), reference)
    check(f"H: classify_turnover({delta}/{reference:.0f}) == {expected}",
          tc == expected, f"got={tc}")


# ---------------------------------------------------------------------------
# SCENARIO I — Budget pressure decreases correctly
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO I — Budget pressure decreases correctly across 3 PASS candidates")
print(f"{'='*72}")
rows_i = run_sim(
    [make_dq("I1", "PASS", 0.90),
     make_dq("I2", "PASS", 0.80),
     make_dq("I3", "PASS", 0.70)],
    # actual < target → under-allocated → drift_pct negative → delta positive (increase)
    [make_drift("I1", 50.0, 150.0, 0.15, drift_pct=-0.10),
     make_drift("I2", 50.0, 150.0, 0.15, drift_pct=-0.10),
     make_drift("I3", 50.0, 150.0, 0.15, drift_pct=-0.10)],
    [make_rebal("I1", 80.0),
     make_rebal("I2", 80.0),
     make_rebal("I3", 80.0)],
)
ri1 = get_row(rows_i, "I1")
ri2 = get_row(rows_i, "I2")
ri3 = get_row(rows_i, "I3")

print(f"  I1: before={ri1['budget_pressure_before_eur']} after={ri1['budget_pressure_after_eur']}")
print(f"  I2: before={ri2['budget_pressure_before_eur']} after={ri2['budget_pressure_after_eur']}")
print(f"  I3: before={ri3['budget_pressure_before_eur']} after={ri3['budget_pressure_after_eur']}")

check("I: I1 starts with full sim budget",
      ri1["budget_pressure_before_eur"] == SIM_BUDGET)
check("I: I1 budget_after = SIM_BUDGET - 80",
      ri1["budget_pressure_after_eur"] == SIM_BUDGET - 80.0)
check("I: I2 budget_before = I1 budget_after",
      ri2["budget_pressure_before_eur"] == ri1["budget_pressure_after_eur"])
check("I: I3 budget_before = I2 budget_after",
      ri3["budget_pressure_before_eur"] == ri2["budget_pressure_after_eur"])
check("I: all 3 SIMULATE_TRANSITION (total 240 <= 400 budget)",
      all(get_row(rows_i, pk)["simulation_action"] == "SIMULATE_TRANSITION"
          for pk in ["I1", "I2", "I3"]))


# ---------------------------------------------------------------------------
# MATHEMATICAL INVARIANTS
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  MATHEMATICAL INVARIANTS")
print(f"{'='*72}")

# simulation_action always SIMULATE_TRANSITION or NO_ACTION
all_rows = rows_a + rows_b + rows_c + rows_d + rows_e + rows_f + rows_g + rows_i
for r in all_rows:
    check(f"action valid for {r['position_key']}",
          r["simulation_action"] in ("SIMULATE_TRANSITION", "NO_ACTION"),
          f"action={r['simulation_action']}")

# budget_pressure_after <= budget_pressure_before for all rows
for r in all_rows:
    check(f"budget_after <= budget_before for {r['position_key']}",
          r["budget_pressure_after_eur"] <= r["budget_pressure_before_eur"] + 0.01,
          f"before={r['budget_pressure_before_eur']} after={r['budget_pressure_after_eur']}")

# NO_ACTION never consumes budget
for r in all_rows:
    if r["simulation_action"] == "NO_ACTION":
        check(f"NO_ACTION budget unchanged for {r['position_key']}",
              r["budget_pressure_after_eur"] == r["budget_pressure_before_eur"],
              f"before={r['budget_pressure_before_eur']} after={r['budget_pressure_after_eur']}")


# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print(f"\n{'#'*72}")
print("  AC58 VALIDATION RESULTS")
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

gate_ok      = all(ok for lbl, ok in results if lbl.startswith(("A:", "B:", "C:")))
conflict_ok  = all(ok for lbl, ok in results if lbl.startswith(("D:", "E:", "F:")))
budget_ok    = all(ok for lbl, ok in results if lbl.startswith(("G:", "I:")))
turnover_ok  = all(ok for lbl, ok in results if lbl.startswith("H:"))
invars_ok    = all(ok for lbl, ok in results if "valid for" in lbl
                   or "budget_after <= budget_before" in lbl
                   or "NO_ACTION budget unchanged" in lbl)

print(f"  Gate filter correct (PASS→sim, HOLD/BLOCK→block): {'JA' if gate_ok else 'NEE'}")
print(f"  Conflict detectie actief:                          {'JA' if conflict_ok else 'NEE'}")
print(f"  Budget pressure correct bijgehouden:               {'JA' if budget_ok else 'NEE'}")
print(f"  Turnover classificatie correct:                    {'JA' if turnover_ok else 'NEE'}")
print(f"  Mathematische invarianten OK:                      {'JA' if invars_ok else 'NEE'}")

all_scenarios_pass = passed == total
if all_scenarios_pass:
    print(f"\n  >>> AC-59 MAG STARTEN <<<")
else:
    print(f"\n  >>> STOP: AC-59 GEBLOKKEERD — simulatie failures <<<")
print()
