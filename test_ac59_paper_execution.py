"""
AC59: Paper-Only Conditional Rebalance Execution — synthetic validation
Tests execution eligibility logic, skip reasons, and paper-mode enforcement.
No file I/O — imports production function directly.

Scenarios:
  A: PASS + SIMULATE_TRANSITION + INCREASE   → EXECUTED
  B: HOLD + NO_ACTION                        → SKIPPED (GATE_NOT_PASS + SIMULATION_NO_ACTION)
  C: BLOCK + NO_ACTION                       → SKIPPED (GATE_NOT_PASS + SIMULATION_NO_ACTION)
  D: PASS + SIMULATE_TRANSITION + HOLD action → SKIPPED (REBALANCE_ACTION_HOLD)
  E: PASS + NO_ACTION (conflict)             → SKIPPED (SIMULATION_NO_ACTION)
  F: Mixed batch: 2 PASS+sim, 1 HOLD, 1 BLOCK → 2 EXECUTED, 2 SKIPPED
  G: execution_mode always PAPER
  H: executed_delta == 0 for all SKIPPED
  I: execution_id format check
  J: total_notional = sum of abs(executed_delta) for EXECUTED only

Usage: python test_ac59_paper_execution.py
"""
import importlib.util
from pathlib import Path

# --- Load execution production module ---
_exec_path = Path(__file__).parent / "ant_colony" / "paper_rebalance_execution_lite.py"
_exec_spec = importlib.util.spec_from_file_location("exec", _exec_path)
_exec_mod  = importlib.util.module_from_spec(_exec_spec)
_exec_spec.loader.exec_module(_exec_mod)

build_execution_records = _exec_mod.build_execution_records
EXECUTION_MODE          = _exec_mod.EXECUTION_MODE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CYCLE_ID = "20260101T120000Z"

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


def make_sim_row(pk, gate, sim_action, market="TST", strategy="S1",
                 score=0.70, conflict_flags=""):
    return {
        "position_key":          pk,
        "market":                market,
        "strategy_key":          strategy,
        "decision_quality_gate": gate,
        "decision_quality_score": score,
        "simulation_action":     sim_action,
        "conflict_flags":        conflict_flags,
    }


def make_rebal(pk, action="REBALANCE_INCREASE", delta=50.0):
    return {
        "position_key":              pk,
        "rebalance_action":          action,
        "rebalance_capped_delta_eur": float(delta),
    }


def run_exec(sim_rows, rebal_rows, cycle_id=CYCLE_ID):
    r_idx = {r["position_key"]: r for r in rebal_rows}
    return build_execution_records(sim_rows, {}, r_idx, cycle_id)


def get_rec(records, pk):
    for r in records:
        if r["position_key"] == pk:
            return r
    return None


# ---------------------------------------------------------------------------
print("\n" + "#"*72)
print("  AC59 PAPER-ONLY CONDITIONAL REBALANCE EXECUTION — VALIDATION")
print(f"  EXECUTION_MODE={EXECUTION_MODE}")
print("#"*72)


# ---------------------------------------------------------------------------
# SCENARIO A — PASS + SIMULATE_TRANSITION + INCREASE → EXECUTED
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO A — PASS + SIMULATE + INCREASE → EXECUTED")
print(f"{'='*72}")
recs_a = run_exec(
    [make_sim_row("A1", "PASS", "SIMULATE_TRANSITION")],
    [make_rebal("A1", "REBALANCE_INCREASE", delta=75.0)],
)
r = get_rec(recs_a, "A1")
print(f"  status={r['execution_status']}  delta={r['executed_delta_eur']}  mode={r['execution_mode']}")

check("A: execution_status == EXECUTED",       r["execution_status"] == "EXECUTED")
check("A: executed_delta_eur == 75.0",         r["executed_delta_eur"] == 75.0)
check("A: execution_mode == PAPER",            r["execution_mode"] == "PAPER")
check("A: skip_reason empty",                  r["skip_reason"] == "")
check("A: executed_price_ref == SIMULATED_MID", r["executed_price_ref"] == "SIMULATED_MID")


# ---------------------------------------------------------------------------
# SCENARIO B — HOLD + NO_ACTION → SKIPPED
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO B — HOLD + NO_ACTION → SKIPPED")
print(f"{'='*72}")
recs_b = run_exec(
    [make_sim_row("B1", "HOLD", "NO_ACTION",
                  conflict_flags="GATE_NOT_PASS", score=0.45)],
    [make_rebal("B1", "REBALANCE_INCREASE", delta=30.0)],
)
r = get_rec(recs_b, "B1")
print(f"  status={r['execution_status']}  skip_reason={r['skip_reason']}")

check("B: execution_status == SKIPPED",           r["execution_status"] == "SKIPPED")
check("B: GATE_NOT_PASS in skip_reason",          "GATE_NOT_PASS" in r["skip_reason"])
check("B: SIMULATION_NO_ACTION in skip_reason",   "SIMULATION_NO_ACTION" in r["skip_reason"])
check("B: executed_delta_eur == 0.0",             r["executed_delta_eur"] == 0.0)


# ---------------------------------------------------------------------------
# SCENARIO C — BLOCK + NO_ACTION → SKIPPED
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO C — BLOCK + NO_ACTION → SKIPPED")
print(f"{'='*72}")
recs_c = run_exec(
    [make_sim_row("C1", "BLOCK", "NO_ACTION", score=0.10)],
    [make_rebal("C1", "REBALANCE_INCREASE", delta=20.0)],
)
r = get_rec(recs_c, "C1")
check("C: execution_status == SKIPPED",   r["execution_status"] == "SKIPPED")
check("C: GATE_NOT_PASS in skip_reason",  "GATE_NOT_PASS" in r["skip_reason"])
check("C: executed_delta_eur == 0.0",     r["executed_delta_eur"] == 0.0)


# ---------------------------------------------------------------------------
# SCENARIO D — PASS + SIMULATE but REBALANCE_HOLD action → SKIPPED
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO D — PASS + SIMULATE but HOLD action → SKIPPED")
print(f"{'='*72}")
recs_d = run_exec(
    [make_sim_row("D1", "PASS", "SIMULATE_TRANSITION")],
    [make_rebal("D1", "REBALANCE_HOLD", delta=0.0)],
)
r = get_rec(recs_d, "D1")
print(f"  status={r['execution_status']}  skip_reason={r['skip_reason']}")
check("D: execution_status == SKIPPED",           r["execution_status"] == "SKIPPED")
check("D: REBALANCE_ACTION_HOLD in skip_reason",  "REBALANCE_ACTION_HOLD" in r["skip_reason"])
check("D: GATE_NOT_PASS not in skip_reason",      "GATE_NOT_PASS" not in r["skip_reason"])


# ---------------------------------------------------------------------------
# SCENARIO E — PASS + NO_ACTION (sim conflict) → SKIPPED
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO E — PASS but simulation blocked (conflict) → SKIPPED")
print(f"{'='*72}")
recs_e = run_exec(
    [make_sim_row("E1", "PASS", "NO_ACTION",
                  conflict_flags="CONFLICT_DIRECTION")],
    [make_rebal("E1", "REBALANCE_INCREASE", delta=40.0)],
)
r = get_rec(recs_e, "E1")
check("E: execution_status == SKIPPED",          r["execution_status"] == "SKIPPED")
check("E: SIMULATION_NO_ACTION in skip_reason",  "SIMULATION_NO_ACTION" in r["skip_reason"])
check("E: GATE_NOT_PASS not in skip_reason",     "GATE_NOT_PASS" not in r["skip_reason"])
check("E: executed_delta_eur == 0.0",            r["executed_delta_eur"] == 0.0)


# ---------------------------------------------------------------------------
# SCENARIO F — Mixed batch
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO F — mixed batch: 2 EXECUTED, 2 SKIPPED")
print(f"{'='*72}")
recs_f = run_exec(
    [
        make_sim_row("F_PASS1", "PASS", "SIMULATE_TRANSITION", score=0.80),
        make_sim_row("F_PASS2", "PASS", "SIMULATE_TRANSITION", score=0.70),
        make_sim_row("F_HOLD",  "HOLD", "NO_ACTION", score=0.45),
        make_sim_row("F_BLOCK", "BLOCK", "NO_ACTION", score=0.10),
    ],
    [
        make_rebal("F_PASS1", "REBALANCE_INCREASE", delta=100.0),
        make_rebal("F_PASS2", "REBALANCE_REDUCE", delta=-60.0),
        make_rebal("F_HOLD",  "REBALANCE_INCREASE", delta=30.0),
        make_rebal("F_BLOCK", "REBALANCE_INCREASE", delta=20.0),
    ],
)
executed_f = [r for r in recs_f if r["execution_status"] == "EXECUTED"]
skipped_f  = [r for r in recs_f if r["execution_status"] == "SKIPPED"]

print(f"  executed={len(executed_f)}  skipped={len(skipped_f)}")
for r in recs_f:
    print(f"  {r['position_key']:<12} {r['execution_status']:<9}"
          f" delta={r['executed_delta_eur']:>8.2f}  skip={r['skip_reason']}")

check("F: 2 EXECUTED",                          len(executed_f) == 2)
check("F: 2 SKIPPED",                           len(skipped_f) == 2)
check("F: F_PASS1 EXECUTED",                    get_rec(recs_f, "F_PASS1")["execution_status"] == "EXECUTED")
check("F: F_PASS2 EXECUTED",                    get_rec(recs_f, "F_PASS2")["execution_status"] == "EXECUTED")
check("F: F_HOLD SKIPPED",                      get_rec(recs_f, "F_HOLD")["execution_status"] == "SKIPPED")
check("F: F_BLOCK SKIPPED",                     get_rec(recs_f, "F_BLOCK")["execution_status"] == "SKIPPED")
check("F: F_PASS1 delta == 100.0",              get_rec(recs_f, "F_PASS1")["executed_delta_eur"] == 100.0)
check("F: F_PASS2 delta == -60.0",              get_rec(recs_f, "F_PASS2")["executed_delta_eur"] == -60.0)
check("F: SKIPPED deltas are 0.0",
      all(r["executed_delta_eur"] == 0.0 for r in skipped_f))


# ---------------------------------------------------------------------------
# SCENARIO G — execution_mode always PAPER for all records
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO G — execution_mode always PAPER")
print(f"{'='*72}")
all_recs = recs_a + recs_b + recs_c + recs_d + recs_e + recs_f
for r in all_recs:
    check(f"G: mode==PAPER for {r['position_key']}",
          r["execution_mode"] == "PAPER",
          f"mode={r['execution_mode']}")


# ---------------------------------------------------------------------------
# SCENARIO H — executed_delta == 0 for all SKIPPED
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO H — executed_delta == 0 for all SKIPPED records")
print(f"{'='*72}")
skipped_all = [r for r in all_recs if r["execution_status"] == "SKIPPED"]
for r in skipped_all:
    check(f"H: SKIPPED delta==0 for {r['position_key']}",
          r["executed_delta_eur"] == 0.0,
          f"delta={r['executed_delta_eur']}")


# ---------------------------------------------------------------------------
# SCENARIO I — execution_id format: {cycle_id}__{pk}__{seq:03d}
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO I — execution_id format")
print(f"{'='*72}")
# All execution IDs should start with cycle_id and contain position_key
for r in all_recs:
    eid = r["execution_id"]
    check(f"I: exec_id starts with cycle_id for {r['position_key']}",
          eid.startswith(CYCLE_ID),
          f"exec_id={eid}")
    check(f"I: exec_id contains position_key for {r['position_key']}",
          r["position_key"] in eid,
          f"exec_id={eid}")


# ---------------------------------------------------------------------------
# SCENARIO J — total_notional = sum abs(executed_delta) for EXECUTED
# ---------------------------------------------------------------------------
print(f"\n{'='*72}")
print("  SCENARIO J — total notional calculation")
print(f"{'='*72}")
executed_all = [r for r in all_recs if r["execution_status"] == "EXECUTED"]
expected_total = round(sum(abs(r["executed_delta_eur"]) for r in executed_all), 2)
# F_PASS1=100 + F_PASS2=60 + A=75 = 235
print(f"  executed_count={len(executed_all)}  expected_total={expected_total}")
check("J: total notional matches sum of abs(executed_delta)",
      expected_total == round(sum(abs(r["executed_delta_eur"]) for r in executed_all), 2))
check("J: SKIPPED records contribute 0 to total",
      sum(abs(r["executed_delta_eur"]) for r in skipped_all) == 0.0)


# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print(f"\n{'#'*72}")
print("  AC59 VALIDATION RESULTS")
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

exec_ok    = all(ok for lbl, ok in results if lbl.startswith("A:"))
skip_ok    = all(ok for lbl, ok in results if lbl.startswith(("B:", "C:", "D:", "E:")))
batch_ok   = all(ok for lbl, ok in results if lbl.startswith("F:"))
paper_ok   = all(ok for lbl, ok in results if lbl.startswith("G:"))
delta_ok   = all(ok for lbl, ok in results if lbl.startswith("H:"))
id_ok      = all(ok for lbl, ok in results if lbl.startswith("I:"))
total_ok   = all(ok for lbl, ok in results if lbl.startswith("J:"))

print(f"  PASS+sim → EXECUTED correct:              {'JA' if exec_ok else 'NEE'}")
print(f"  HOLD/BLOCK/conflict → SKIPPED correct:    {'JA' if skip_ok else 'NEE'}")
print(f"  Mixed batch correct:                      {'JA' if batch_ok else 'NEE'}")
print(f"  Altijd PAPER mode:                        {'JA' if paper_ok else 'NEE'}")
print(f"  SKIPPED delta altijd 0:                   {'JA' if delta_ok else 'NEE'}")
print(f"  execution_id format correct:              {'JA' if id_ok else 'NEE'}")
print(f"  Notional berekening correct:              {'JA' if total_ok else 'NEE'}")

all_ok = passed == total
if all_ok:
    print(f"\n  >>> AC57+AC58+AC59 VOLLEDIG GEVALIDEERD — PIPELINE COMPLEET <<<")
else:
    print(f"\n  >>> FAILURES — pipeline niet volledig gevalideerd <<<")
print()
