"""
AC59: Paper-Only Conditional Rebalance Execution
Strictly paper mode. No real orders. Appends to execution log and writes summary.

Sits after AC58 (Portfolio Transition Simulation). Executes only those candidates
that cleared ALL prior gates:
  1. decision_quality_gate == PASS          (AC56)
  2. simulation_action == SIMULATE_TRANSITION (AC58, no hard conflicts)
  3. paper mode enforced (never real orders)

Reads:
  portfolio_transition_simulation.json  — simulation result per candidate (AC58)
  allocation_decision_quality.json      — quality score, gate, reasons (AC56)
  allocation_portfolio_drift.json       — actual/target notionals (AC52)
  rebalance_intents.json                — delta, action, cap (AC53–55)

Writes:
  paper_rebalance_execution_summary.json   — summary + all records
  paper_rebalance_execution_log.jsonl      — one JSON line per executed record

Execution record fields:
  execution_id       — {cycle_id}__{position_key}__{seq:03d}
  market, strategy_key, position_key
  cycle_id
  execution_mode     — always "PAPER"
  rebalance_action   — REBALANCE_INCREASE / REDUCE / OPEN / CLOSE / HOLD
  executed_delta_eur — notional delta applied in paper (0 if skipped)
  executed_price_ref — "SIMULATED_MID" (no live prices in paper mode)
  decision_quality_gate, decision_quality_score
  simulation_action
  execution_status   — EXECUTED / SKIPPED
  skip_reason        — reason if SKIPPED (empty if EXECUTED)
  ts_utc

Skip reasons (any non-PASS/non-SIMULATE candidate):
  GATE_NOT_PASS            — gate was HOLD or BLOCK
  SIMULATION_NO_ACTION     — simulation blocked this candidate
  REBALANCE_ACTION_HOLD    — action is REBALANCE_HOLD (no movement intended)
  PAPER_MODE_ONLY          — live execution is disabled (always set in paper runs)

Usage: python ant_colony/paper_rebalance_execution_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

SIM_PATH      = OUT_DIR / "portfolio_transition_simulation.json"
DQ_PATH       = OUT_DIR / "allocation_decision_quality.json"
DRIFT_PATH    = OUT_DIR / "allocation_portfolio_drift.json"
REBALANCE_PATH = OUT_DIR / "rebalance_intents.json"

EXEC_SUMMARY_OUT = OUT_DIR / "paper_rebalance_execution_summary.json"
EXEC_LOG_OUT     = OUT_DIR / "paper_rebalance_execution_log.jsonl"

VERSION       = "paper_exec_v1"
EXECUTION_MODE = "PAPER"   # never changes; real execution requires separate module

# Actions that require no actual movement
_HOLD_ACTIONS = frozenset({"REBALANCE_HOLD"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def to_float(v, default=0.0):
    try:
        f = float(v)
        return f if f == f else float(default)
    except Exception:
        return float(default)


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def append_jsonl(path: Path, obj):
    line = json.dumps(obj, separators=(",", ":"))
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


# ---------------------------------------------------------------------------
# Core execution logic (importable for tests)
# ---------------------------------------------------------------------------

def build_execution_records(
    sim_rows: list,
    dq_index: dict,
    rebal_index: dict,
    cycle_id: str,
) -> list:
    """
    Build execution records from simulation rows.
    Returns list of dicts — one per candidate.
    """
    records = []
    seq = 0

    for row in sim_rows:
        pk           = row.get("position_key", "")
        market       = row.get("market", "")
        strategy     = row.get("strategy_key", "")
        gate         = row.get("decision_quality_gate", "BLOCK")
        score        = to_float(row.get("decision_quality_score", 0.0))
        sim_action   = row.get("simulation_action", "NO_ACTION")
        conflict_flags = row.get("conflict_flags", "")

        dq_rec    = dq_index.get(pk) or {}
        rebal_rec = rebal_index.get(pk) or {}

        rebal_action = str(rebal_rec.get("rebalance_action") or "REBALANCE_HOLD")
        delta_eur    = to_float(rebal_rec.get("rebalance_capped_delta_eur", 0.0))

        ts = utc_now_ts()
        seq += 1
        exec_id = f"{cycle_id}__{pk}__{seq:03d}"

        # Determine execution eligibility
        skip_reasons = []

        if gate != "PASS":
            skip_reasons.append("GATE_NOT_PASS")

        if sim_action != "SIMULATE_TRANSITION":
            skip_reasons.append("SIMULATION_NO_ACTION")

        if rebal_action in _HOLD_ACTIONS:
            skip_reasons.append("REBALANCE_ACTION_HOLD")

        if skip_reasons:
            exec_status    = "SKIPPED"
            executed_delta = 0.0
            skip_reason    = "|".join(skip_reasons)
        else:
            exec_status    = "EXECUTED"
            executed_delta = delta_eur
            skip_reason    = ""

        records.append({
            "execution_id":          exec_id,
            "market":                market,
            "strategy_key":          strategy,
            "position_key":          pk,
            "cycle_id":              cycle_id,
            "execution_mode":        EXECUTION_MODE,
            "rebalance_action":      rebal_action,
            "executed_delta_eur":    executed_delta,
            "executed_price_ref":    "SIMULATED_MID",
            "decision_quality_gate": gate,
            "decision_quality_score": score,
            "simulation_action":     sim_action,
            "conflict_flags":        conflict_flags,
            "execution_status":      exec_status,
            "skip_reason":           skip_reason,
            "ts_utc":                ts,
        })

    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    sim_data  = load_json(SIM_PATH, {})  or {}
    dq_data   = load_json(DQ_PATH, {})   or {}
    drift_data = load_json(DRIFT_PATH, {}) or {}
    rebal_data = load_json(REBALANCE_PATH, {}) or {}

    cycle_id = (dq_data.get("cycle_id")
                or sim_data.get("cycle_id")
                or drift_data.get("cycle_id")
                or "UNKNOWN")
    equity   = to_float(dq_data.get("equity") or drift_data.get("equity", 0.0))

    sim_rows = sim_data.get("rows") or []

    dq_index    = {r["position_key"]: r for r in (dq_data.get("records") or [])
                   if r.get("position_key")}
    rebal_index = {i["position_key"]: i for i in (rebal_data.get("intents") or [])
                   if i.get("position_key")}

    records = build_execution_records(sim_rows, dq_index, rebal_index, cycle_id)

    executed = [r for r in records if r["execution_status"] == "EXECUTED"]
    skipped  = [r for r in records if r["execution_status"] == "SKIPPED"]

    total_notional = round(sum(abs(r["executed_delta_eur"]) for r in executed), 2)

    summary = {
        "component":       "paper_rebalance_execution_lite",
        "version":         VERSION,
        "ts_utc":          ts,
        "cycle_id":        cycle_id,
        "equity":          equity,
        "execution_mode":  EXECUTION_MODE,
        "records_total":   len(records),
        "executed_count":  len(executed),
        "skipped_count":   len(skipped),
        "total_notional_executed_eur": total_notional,
        "source_sim_rows":     len(sim_rows),
        "source_dq_records":   len(dq_index),
        "source_rebal_intents": len(rebal_index),
        "records": records,
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(EXEC_SUMMARY_OUT, summary)
        for rec in executed:
            append_jsonl(EXEC_LOG_OUT, rec)
    except Exception as e:
        print(f"[WARN] Could not write output: {e}")

    print(json.dumps({k: v for k, v in summary.items() if k != "records"}, indent=2))

    for r in records:
        status = r["execution_status"]
        delta  = r["executed_delta_eur"]
        reason = r.get("skip_reason") or r.get("simulation_action", "")
        print(f"  {r['position_key']:<30} {status:<9} delta={delta:>8.2f} EUR"
              f"  gate={r['decision_quality_gate']:<5} {reason}")


if __name__ == "__main__":
    main()
