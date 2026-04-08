"""
AC56: Rebalance audit validation (test only)
Validates AC55 audit trail internal consistency with synthetic intents:
  - priority_rank matches sort order
  - selection_order matches actual selection sequence
  - budget_running_before/after is mathematically correct
  - rebalance_selected matches budget rules
  - audit_summary_reason matches cycle outcome
  - determinism across identical runs

Approach: imports helpers and constants from production module, mirrors
selection+audit flow in run_full_audit() — no file I/O.

Usage: python test_ac56_rebalance_audit_validation.py
"""
import importlib.util
from copy import deepcopy
from pathlib import Path

# --- Load production module ---
_mod_path = Path(__file__).parent / "ant_colony" / "build_rebalance_intents_lite.py"
_spec = importlib.util.spec_from_file_location("build_rebalance_intents_lite", _mod_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

MAX_PORTFOLIO_REBALANCE_PCT = _mod.MAX_PORTFOLIO_REBALANCE_PCT   # 0.30
_SEVERITY_ORDER             = _mod._SEVERITY_ORDER               # {"HIGH": 0, "MEDIUM": 1}
to_float                    = _mod.to_float
build_audit_decision_reason = _mod.build_audit_decision_reason
build_audit_summary_reason  = _mod.build_audit_summary_reason

EQUITY  = 1000.0
BUDGET  = round(EQUITY * MAX_PORTFOLIO_REBALANCE_PCT, 2)   # 300.00


# ---------------------------------------------------------------------------
# Mirror of production sort + budget + audit flow (no file I/O)
# ---------------------------------------------------------------------------

def run_full_audit(raw_intents: list, equity: float = EQUITY) -> dict:
    """
    Mirrors the AC54+AC55 flow from build_rebalance_intents_lite.main():
      1. Sort by severity then abs(drift_pct) descending
      2. Assign priority_rank (1-based)
      3. Greedy budget selection
      4. Track before/after/remaining per intent
      5. Assign selection_order (None if excluded)
      6. Build audit_decision_reason per intent
      7. Build summary

    Returns dict with intents, selected, excluded, used_eur, budget_eur, summary.
    """
    budget  = round(equity * MAX_PORTFOLIO_REBALANCE_PCT, 2)
    intents = deepcopy(raw_intents)

    # Step 1+2: sort and rank
    intents.sort(key=lambda i: (
        _SEVERITY_ORDER.get(i.get("drift_severity", ""), 99),
        -abs(to_float(i.get("drift_pct", 0.0))),
    ))
    for rank, intent in enumerate(intents, 1):
        intent["priority_rank"] = rank

    # Step 3+4+5: greedy selection with budget tracking
    running       = 0.0
    selection_seq = 0

    for intent in intents:
        abs_delta = abs(to_float(intent.get("rebalance_capped_delta_eur", 0.0)))
        before    = round(running, 2)

        if budget <= 0.0 or running + abs_delta > budget:
            intent["rebalance_selected"]      = False
            intent["rebalance_budget_reason"] = "EXCLUDED_BUDGET_LIMIT"
            intent["selection_order"]         = None
        else:
            selection_seq += 1
            intent["rebalance_selected"]      = True
            intent["rebalance_budget_reason"] = "SELECTED_WITHIN_BUDGET"
            intent["selection_order"]         = selection_seq
            running = round(running + abs_delta, 2)

        after = round(running, 2)
        intent["budget_running_before_eur"]  = before
        intent["budget_running_after_eur"]   = after
        intent["budget_remaining_after_eur"] = round(budget - after, 2)

    # Step 6: audit decision reason
    for intent in intents:
        intent["audit_decision_reason"] = build_audit_decision_reason(intent)

    selected = [i for i in intents if i.get("rebalance_selected")]
    excluded = [i for i in intents if not i.get("rebalance_selected")]

    summary_reason = build_audit_summary_reason(len(selected), len(excluded), intents)

    return {
        "intents":        intents,
        "selected":       selected,
        "excluded":       excluded,
        "used_eur":       round(running, 2),
        "budget_eur":     budget,
        "summary_reason": summary_reason,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_intent(label, severity, delta, drift_pct):
    return {
        "position_key":              label,
        "market":                    label.split("__")[0] if "__" in label else label,
        "strategy":                  label.split("__")[1] if "__" in label else "EDGE",
        "drift_severity":            severity,
        "drift_pct":                 drift_pct,
        "rebalance_action":          "REBALANCE_INCREASE",
        "rebalance_capped_delta_eur": float(delta),
    }


results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


def print_trace(res: dict):
    print(f"  budget={res['budget_eur']:.2f} EUR   used={res['used_eur']:.2f} EUR   "
          f"summary_reason={res['summary_reason']}")
    print(f"  {'pos_key':<18} {'sev':<8} {'drift':>7} {'delta':>8} "
          f"{'rank':>5} {'ord':>4} {'before':>8} {'after':>8} {'remain':>8} sel")
    print(f"  {'-'*96}")
    for i in res["intents"]:
        sel = "✓" if i["rebalance_selected"] else "✗"
        print(f"  {i['position_key']:<18} {i['drift_severity']:<8} "
              f"{i['drift_pct']:>7.3f} {i['rebalance_capped_delta_eur']:>8.2f} "
              f"{i['priority_rank']:>5} {str(i['selection_order']):>4} "
              f"{i['budget_running_before_eur']:>8.2f} {i['budget_running_after_eur']:>8.2f} "
              f"{i['budget_remaining_after_eur']:>8.2f} {sel}")
        print(f"    {i['audit_decision_reason']}")


def validate_math(res: dict, label_prefix: str):
    """Core mathematical invariants that must hold for any scenario."""
    budget = res["budget_eur"]
    for intent in res["intents"]:
        pk    = intent["position_key"]
        delta = abs(to_float(intent.get("rebalance_capped_delta_eur", 0.0)))
        before = to_float(intent["budget_running_before_eur"])
        after  = to_float(intent["budget_running_after_eur"])
        remain = to_float(intent["budget_remaining_after_eur"])
        sel    = intent["rebalance_selected"]

        if sel:
            check(f"{label_prefix} {pk}: after == before+delta",
                  abs(after - (before + delta)) < 0.01,
                  f"before={before} delta={delta} after={after}")
        else:
            check(f"{label_prefix} {pk}: after == before (excluded)",
                  abs(after - before) < 0.01,
                  f"before={before} after={after}")

        check(f"{label_prefix} {pk}: remaining == budget-after",
              abs(remain - (budget - after)) < 0.01,
              f"remain={remain} budget-after={budget-after}")

    check(f"{label_prefix}: used_eur <= budget",
          res["used_eur"] <= budget,
          f"used={res['used_eur']} budget={budget}")


def validate_ranks(res: dict, label_prefix: str):
    ranks = [i["priority_rank"] for i in res["intents"]]
    check(f"{label_prefix}: ranks are 1..N",
          ranks == list(range(1, len(ranks) + 1)),
          f"ranks={ranks}")
    # Verify HIGH before MEDIUM
    sevs = [i["drift_severity"] for i in res["intents"]]
    if "HIGH" in sevs and "MEDIUM" in sevs:
        last_high  = max(idx for idx, s in enumerate(sevs) if s == "HIGH")
        first_med  = min(idx for idx, s in enumerate(sevs) if s == "MEDIUM")
        check(f"{label_prefix}: all HIGH before MEDIUM",
              last_high < first_med,
              f"last_HIGH_idx={last_high} first_MEDIUM_idx={first_med}")
    # Within same severity, verify descending abs(drift_pct)
    for sev in ("HIGH", "MEDIUM"):
        grp = [i for i in res["intents"] if i["drift_severity"] == sev]
        drifts = [abs(to_float(i["drift_pct"])) for i in grp]
        check(f"{label_prefix}: {sev} sorted by abs(drift_pct) desc",
              drifts == sorted(drifts, reverse=True),
              f"drifts={drifts}")


def validate_selection_order(res: dict, label_prefix: str):
    orders = [i["selection_order"] for i in res["intents"] if i["rebalance_selected"]]
    check(f"{label_prefix}: selection_order is 1..selected_count",
          orders == list(range(1, len(orders) + 1)),
          f"orders={orders}")
    none_orders = [i["selection_order"] for i in res["intents"] if not i["rebalance_selected"]]
    check(f"{label_prefix}: excluded intents have selection_order=None",
          all(o is None for o in none_orders),
          f"none_orders={none_orders}")


# ---------------------------------------------------------------------------
print("\n" + "#"*68)
print("  AC56 REBALANCE AUDIT VALIDATION")
print(f"  equity={EQUITY:.0f} EUR   budget={BUDGET:.0f} EUR   "
      f"(MAX_PORTFOLIO_REBALANCE_PCT={MAX_PORTFOLIO_REBALANCE_PCT})")
print("#"*68)

# ---------------------------------------------------------------------------
# SCENARIO A — 3×HIGH, only first fits
# ---------------------------------------------------------------------------
print(f"\n{'='*68}")
print("  SCENARIO A — 3×HIGH delta=250 each, only A fits")
print(f"{'='*68}")
res_a = run_full_audit([
    make_intent("A1", "HIGH", 250, 0.90),
    make_intent("A2", "HIGH", 250, 0.60),
    make_intent("A3", "HIGH", 250, 0.40),
])
print_trace(res_a)
validate_ranks(res_a, "A")
validate_selection_order(res_a, "A")
validate_math(res_a, "A")
check("A: 1 selected", len(res_a["selected"]) == 1)
check("A: A1 selected (highest drift)", res_a["selected"][0]["position_key"] == "A1")
check("A: A2/A3 excluded",
      [i["position_key"] for i in res_a["excluded"]] == ["A2", "A3"])
check("A: running A: 0→250", res_a["intents"][0]["budget_running_after_eur"] == 250.0)
check("A: running B: stays 250", res_a["intents"][1]["budget_running_after_eur"] == 250.0)
check("A: summary reason",
      res_a["summary_reason"] == "ONLY_HIGHEST_PRIORITY_INTENT_FIT_WITHIN_BUDGET",
      res_a["summary_reason"])

# ---------------------------------------------------------------------------
# SCENARIO B — 2×HIGH + 1×MEDIUM, exact fill, MEDIUM excluded
# ---------------------------------------------------------------------------
print(f"\n{'='*68}")
print("  SCENARIO B — HIGH 150 + HIGH 150 + MEDIUM 100, budget=300")
print(f"{'='*68}")
res_b = run_full_audit([
    make_intent("B_M", "MEDIUM", 100, 0.60),
    make_intent("B_H2", "HIGH",  150, 0.70),
    make_intent("B_H1", "HIGH",  150, 0.80),
])
print_trace(res_b)
validate_ranks(res_b, "B")
validate_selection_order(res_b, "B")
validate_math(res_b, "B")
check("B: rank order is B_H1, B_H2, B_M",
      [i["position_key"] for i in res_b["intents"]] == ["B_H1", "B_H2", "B_M"])
check("B: 2 selected (both HIGH)", len(res_b["selected"]) == 2)
check("B: B_H1 rank=1 sel=1", res_b["intents"][0]["priority_rank"] == 1 and
      res_b["intents"][0]["selection_order"] == 1)
check("B: B_H2 rank=2 sel=2", res_b["intents"][1]["priority_rank"] == 2 and
      res_b["intents"][1]["selection_order"] == 2)
check("B: B_M rank=3 excluded", res_b["intents"][2]["priority_rank"] == 3 and
      res_b["intents"][2]["selection_order"] is None)
check("B: running B_H1: 0→150", res_b["intents"][0]["budget_running_after_eur"] == 150.0)
check("B: running B_H2: 150→300", res_b["intents"][1]["budget_running_after_eur"] == 300.0)
check("B: running B_M: stays 300", res_b["intents"][2]["budget_running_after_eur"] == 300.0)
check("B: remaining after B_M = 0", res_b["intents"][2]["budget_remaining_after_eur"] == 0.0)
check("B: used == 300", res_b["used_eur"] == 300.0)
check("B: summary reason HIGH_PRIORITY_CONSUMED_BUDGET",
      res_b["summary_reason"] == "HIGH_PRIORITY_CONSUMED_BUDGET",
      res_b["summary_reason"])

# ---------------------------------------------------------------------------
# SCENARIO C — all fit
# ---------------------------------------------------------------------------
print(f"\n{'='*68}")
print("  SCENARIO C — 2×HIGH + 2×MEDIUM, all fit (total=200)")
print(f"{'='*68}")
res_c = run_full_audit([
    make_intent("C3", "MEDIUM", 50, 0.70),
    make_intent("C4", "MEDIUM", 50, 0.60),
    make_intent("C1", "HIGH",   50, 0.90),
    make_intent("C2", "HIGH",   50, 0.80),
])
print_trace(res_c)
validate_ranks(res_c, "C")
validate_selection_order(res_c, "C")
validate_math(res_c, "C")
check("C: all 4 selected", len(res_c["selected"]) == 4)
check("C: selection_order 1..4",
      [i["selection_order"] for i in res_c["intents"]] == [1, 2, 3, 4])
check("C: used == 200", res_c["used_eur"] == 200.0)
check("C: remaining after last == 100",
      res_c["intents"][-1]["budget_remaining_after_eur"] == 100.0)
check("C: summary reason ALL_CANDIDATES_FIT_WITHIN_BUDGET",
      res_c["summary_reason"] == "ALL_CANDIDATES_FIT_WITHIN_BUDGET",
      res_c["summary_reason"])

# ---------------------------------------------------------------------------
# SCENARIO D — empty input
# ---------------------------------------------------------------------------
print(f"\n{'='*68}")
print("  SCENARIO D — empty input")
print(f"{'='*68}")
res_d = run_full_audit([])
print(f"  intents=0  selected=0  excluded=0  used={res_d['used_eur']}")
print(f"  summary_reason={res_d['summary_reason']}")
check("D: no intents", len(res_d["intents"]) == 0)
check("D: selected=0", len(res_d["selected"]) == 0)
check("D: used=0", res_d["used_eur"] == 0.0)
check("D: summary reason NO_REBALANCE_CANDIDATES",
      res_d["summary_reason"] == "NO_REBALANCE_CANDIDATES",
      res_d["summary_reason"])

# ---------------------------------------------------------------------------
# SCENARIO E — determinism with identical inputs
# ---------------------------------------------------------------------------
print(f"\n{'='*68}")
print("  SCENARIO E — determinism (3 identical HIGH intents × 3 runs)")
print(f"{'='*68}")
base_e = [make_intent(f"E{i}", "HIGH", 100, 0.50) for i in range(1, 4)]
runs   = [run_full_audit(base_e) for _ in range(3)]
orders = [[i["position_key"] for i in r["intents"]] for r in runs]
sels   = [[i["selection_order"] for i in r["intents"]] for r in runs]
useds  = [r["used_eur"] for r in runs]
print(f"  run1 order: {orders[0]}  used={useds[0]}")
print(f"  run2 order: {orders[1]}  used={useds[1]}")
print(f"  run3 order: {orders[2]}  used={useds[2]}")
check("E: rank order stable run1==run2", orders[0] == orders[1])
check("E: rank order stable run1==run3", orders[0] == orders[2])
check("E: selection_order stable",       sels[0] == sels[1] == sels[2])
check("E: used identical",               useds[0] == useds[1] == useds[2])
check("E: 3 selected (3×100=300)",       runs[0]["used_eur"] == 300.0)
validate_math(runs[0], "E")

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
math_ok  = all(ok for lbl, ok in results if "after ==" in lbl or "remaining ==" in lbl or "used_eur <=" in lbl)
rank_ok  = all(ok for lbl, ok in results if "rank" in lbl or "sorted" in lbl)
sel_ok   = all(ok for lbl, ok in results if "selection_order" in lbl or "selected" in lbl)
summ_ok  = all(ok for lbl, ok in results if "summary reason" in lbl)
det_ok   = all(ok for lbl, ok in results if lbl.startswith("E:"))
print(f"  Budget math consistent:        {'JA' if math_ok else 'NEE'}")
print(f"  Priority ranking correct:      {'JA' if rank_ok else 'NEE'}")
print(f"  Selection ordering correct:    {'JA' if sel_ok else 'NEE'}")
print(f"  Summary reason correct:        {'JA' if summ_ok else 'NEE'}")
print(f"  Determinisme stabiel:          {'JA' if det_ok else 'NEE'}")
print()
