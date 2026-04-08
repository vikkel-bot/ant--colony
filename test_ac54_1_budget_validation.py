"""
AC54.1 Synthetic portfolio budget validation
Validates AC54 rebalance budget selection logic with controlled synthetic intents:
  - Priority ordering (HIGH > MEDIUM, then largest abs drift_pct first)
  - Portfolio-level budget cap (30% of equity)
  - Greedy selection (no partial fills)
  - Edge cases: exact fit, all-small, mixed, determinism

Approach: imports constants from production module, wraps selection logic in
run_budget_selection() — no file I/O, fully reproducible.

Usage: python test_ac54_1_budget_validation.py
"""
import importlib.util
import sys
from pathlib import Path
from copy import deepcopy

# --- Load production module (no package __init__ needed) ---
_mod_path = Path(__file__).parent / "ant_colony" / "build_rebalance_intents_lite.py"
_spec = importlib.util.spec_from_file_location("build_rebalance_intents_lite", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

MAX_PORTFOLIO_REBALANCE_PCT = _mod.MAX_PORTFOLIO_REBALANCE_PCT   # 0.30
_SEVERITY_ORDER             = _mod._SEVERITY_ORDER               # {"HIGH": 0, "MEDIUM": 1}
to_float                    = _mod.to_float

EQUITY = 1000.0
PORTFOLIO_BUDGET = round(EQUITY * MAX_PORTFOLIO_REBALANCE_PCT, 2)  # 300.00


# ---------------------------------------------------------------------------
# Helper: mirrors AC54 sort + greedy selection from main()
# ---------------------------------------------------------------------------

def run_budget_selection(intents: list, equity: float = EQUITY) -> dict:
    """
    Sort intents (HIGH > MEDIUM, then abs drift_pct descending),
    then greedily select within portfolio_budget.
    Returns dict with sorted_intents, selected, excluded, used_eur, budget_eur.
    """
    budget = round(equity * MAX_PORTFOLIO_REBALANCE_PCT, 2)
    items  = deepcopy(intents)

    items.sort(key=lambda i: (
        _SEVERITY_ORDER.get(i.get("drift_severity", ""), 99),
        -abs(to_float(i.get("drift_pct", 0.0))),
    ))

    running = 0.0
    selected, excluded = [], []

    for item in items:
        abs_delta = abs(to_float(item.get("rebalance_capped_delta_eur", 0.0)))
        if running + abs_delta <= budget:
            item["rebalance_selected"]      = True
            item["rebalance_budget_reason"] = "SELECTED_WITHIN_BUDGET"
            running = round(running + abs_delta, 2)
            selected.append(item)
        else:
            item["rebalance_selected"]      = False
            item["rebalance_budget_reason"] = "EXCLUDED_BUDGET_LIMIT"
            excluded.append(item)

    return {
        "sorted_intents": items,
        "selected":       selected,
        "excluded":       excluded,
        "used_eur":       round(running, 2),
        "budget_eur":     budget,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_intent(label, severity, delta, drift_pct):
    return {
        "position_key":              label,
        "drift_severity":            severity,
        "drift_pct":                 drift_pct,
        "rebalance_capped_delta_eur": float(delta),
    }


results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


def run_scenario(title, intents, show_sorted=True):
    res = run_budget_selection(intents)
    print(f"\n{'='*68}")
    print(f"  {title}")
    print(f"  budget={res['budget_eur']:.2f} EUR   used={res['used_eur']:.2f} EUR")
    print(f"{'='*68}")
    print("  Sorted order:")
    for i in res["sorted_intents"]:
        sel = "✓" if i["rebalance_selected"] else "✗"
        print(f"    {sel} {i['position_key']:<16} {i['drift_severity']:<8}"
              f" delta={i['rebalance_capped_delta_eur']:>7.2f}"
              f" drift={i['drift_pct']:>6.3f}  → {i['rebalance_budget_reason']}")
    print(f"  selected={len(res['selected'])}  excluded={len(res['excluded'])}")
    return res


# ---------------------------------------------------------------------------
# SCENARIO A — multiple HIGH, only first fits
# ---------------------------------------------------------------------------
print("\n" + "#"*68)
print("  AC54.1 SYNTHETIC PORTFOLIO BUDGET VALIDATION")
print(f"  equity={EQUITY:.0f} EUR   budget={PORTFOLIO_BUDGET:.0f} EUR   "
      f"(MAX_PORTFOLIO_REBALANCE_PCT={MAX_PORTFOLIO_REBALANCE_PCT})")
print("#"*68)

res_a = run_scenario("SCENARIO A — 3×HIGH, only first fits (delta=250 each)",
    [make_intent("A1", "HIGH", 250, 0.9),
     make_intent("A2", "HIGH", 250, 0.6),
     make_intent("A3", "HIGH", 250, 0.4)])
check("A: only 1 selected",    len(res_a["selected"]) == 1)
check("A: A1 selected first",  res_a["selected"][0]["position_key"] == "A1")
check("A: used ≤ budget",      res_a["used_eur"] <= res_a["budget_eur"])
check("A: used == 250",        res_a["used_eur"] == 250.0)
check("A: A2 excluded",        res_a["excluded"][0]["position_key"] == "A2")

# ---------------------------------------------------------------------------
# SCENARIO B — HIGH + MEDIUM mix, two fit
# ---------------------------------------------------------------------------
res_b = run_scenario("SCENARIO B — HIGH 200 + MEDIUM 100 + MEDIUM 100, budget=300",
    [make_intent("B_M1", "MEDIUM", 100, 0.7),
     make_intent("B_H",  "HIGH",   200, 0.8),
     make_intent("B_M2", "MEDIUM", 100, 0.5)])
check("B: 2 selected",         len(res_b["selected"]) == 2)
check("B: HIGH selected first", res_b["sorted_intents"][0]["drift_severity"] == "HIGH")
check("B: B_H selected",       res_b["selected"][0]["position_key"] == "B_H")
check("B: B_M1 selected",      res_b["selected"][1]["position_key"] == "B_M1")
check("B: B_M2 excluded",      res_b["excluded"][0]["position_key"] == "B_M2")
check("B: used == 300",        res_b["used_eur"] == 300.0)

# ---------------------------------------------------------------------------
# SCENARIO C — MEDIUM only, two of three fit
# ---------------------------------------------------------------------------
res_c = run_scenario("SCENARIO C — 3×MEDIUM (150 each), budget=300",
    [make_intent("C3", "MEDIUM", 150, 0.4),
     make_intent("C1", "MEDIUM", 150, 0.6),
     make_intent("C2", "MEDIUM", 150, 0.5)])
check("C: 2 selected",         len(res_c["selected"]) == 2)
check("C: C1 first (drift=0.6)", res_c["sorted_intents"][0]["position_key"] == "C1")
check("C: C3 excluded",        res_c["excluded"][0]["position_key"] == "C3")
check("C: used == 300",        res_c["used_eur"] == 300.0)

# ---------------------------------------------------------------------------
# SCENARIO D — small deltas, all fit
# ---------------------------------------------------------------------------
res_d = run_scenario("SCENARIO D — small deltas (50 each), all 4 fit",
    [make_intent("D1", "HIGH",   50, 0.9),
     make_intent("D2", "HIGH",   50, 0.8),
     make_intent("D3", "MEDIUM", 50, 0.7),
     make_intent("D4", "MEDIUM", 50, 0.6)])
check("D: all 4 selected",     len(res_d["selected"]) == 4)
check("D: used == 200",        res_d["used_eur"] == 200.0)
check("D: used ≤ budget",      res_d["used_eur"] <= res_d["budget_eur"])
check("D: HIGH precede MEDIUM",
      res_d["sorted_intents"][0]["drift_severity"] == "HIGH" and
      res_d["sorted_intents"][2]["drift_severity"] == "MEDIUM")

# ---------------------------------------------------------------------------
# SCENARIO E — exact budget fit
# ---------------------------------------------------------------------------
res_e = run_scenario("SCENARIO E — exact budget fit (150+150=300)",
    [make_intent("E1", "HIGH", 150, 0.9),
     make_intent("E2", "HIGH", 150, 0.8)])
check("E: both selected",      len(res_e["selected"]) == 2)
check("E: used == 300 exact",  res_e["used_eur"] == 300.0)
check("E: no excluded",        len(res_e["excluded"]) == 0)

# ---------------------------------------------------------------------------
# SCENARIO F — determinism (identical intents, stable order)
# ---------------------------------------------------------------------------
base = [make_intent(f"F{i}", "HIGH", 100, 0.5) for i in range(3)]
r1 = run_budget_selection(base)
r2 = run_budget_selection(base)
r3 = run_budget_selection(deepcopy(base))

print(f"\n{'='*68}")
print("  SCENARIO F — determinism (3 identical intents)")
print(f"{'='*68}")
order1 = [i["position_key"] for i in r1["sorted_intents"]]
order2 = [i["position_key"] for i in r2["sorted_intents"]]
order3 = [i["position_key"] for i in r3["sorted_intents"]]
print(f"  run1: {order1}  used={r1['used_eur']}")
print(f"  run2: {order2}  used={r2['used_eur']}")
print(f"  run3: {order3}  used={r3['used_eur']}")
check("F: order stable run1==run2",  order1 == order2)
check("F: order stable run1==run3",  order1 == order3)
check("F: used identical",           r1["used_eur"] == r2["used_eur"] == r3["used_eur"])
check("F: 3 selected (300 total)",   r1["used_eur"] == 300.0)

# ---------------------------------------------------------------------------
# SCENARIO G — large + small intents; small still fit after large
# ---------------------------------------------------------------------------
res_g = run_scenario("SCENARIO G — large+small (250+10+10=270, all fit)",
    [make_intent("G2", "HIGH",  10, 0.8),
     make_intent("G1", "HIGH", 250, 0.9),
     make_intent("G3", "HIGH",  10, 0.7)])
check("G: all 3 selected",          len(res_g["selected"]) == 3)
check("G: G1 first (largest drift)", res_g["sorted_intents"][0]["position_key"] == "G1")
check("G: used == 270",             res_g["used_eur"] == 270.0)
check("G: used ≤ budget",           res_g["used_eur"] <= res_g["budget_eur"])

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print(f"\n{'#'*68}")
print("  AC54.1 VALIDATION RESULTS")
print(f"{'#'*68}")

passed = sum(1 for _, ok in results if ok)
total  = len(results)
for label, ok in results:
    print(f"  [{'PASS' if ok else 'FAIL'}]  {label}")

print(f"\n  {'='*46}")
print(f"  TOTAL: {passed}/{total} PASSED {'✓ ALL OK' if passed == total else '✗ FAILURES'}")

# ---------------------------------------------------------------------------
# INTERPRETATIE
# ---------------------------------------------------------------------------
print(f"\n{'#'*68}")
print("  INTERPRETATIE")
print(f"{'#'*68}")
priority_ok   = all(ok for lbl, ok in results if lbl.startswith(("B: HIGH", "D: HIGH")))
budget_ok     = all(ok for lbl, ok in results if "used ≤ budget" in lbl or "used ==" in lbl)
selection_ok  = all(ok for lbl, ok in results if "selected" in lbl or "excluded" in lbl)
determinism_ok = all(ok for lbl, ok in results if lbl.startswith("F:"))

print(f"  Prioritering HIGH > MEDIUM:    {'JA' if priority_ok else 'NEE'}")
print(f"  Budget wordt gerespecteerd:    {'JA' if budget_ok else 'NEE'}")
print(f"  Selectie correct:              {'JA' if selection_ok else 'NEE'}")
print(f"  Determinisme stabiel:          {'JA' if determinism_ok else 'NEE'}")
print()
