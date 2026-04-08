"""
AC53/AC54: Controlled rebalance intents with portfolio-level risk budget.
Translates AC52 drift rows into explicit, structured rebalance intents.
GEEN orders, GEEN execution — pure intent generation + observability.

Input:
  allocation_portfolio_drift.json  — drift + interpretation per position_key (AC51/AC52)
  paper_portfolio_state.json       — equity (authoritative)

Output:
  rebalance_intents.json
  rebalance_intents.tsv

Filtering:
  Only MEDIUM/HIGH severity rows produce intents.
  LOW severity → skipped.
  allocation_pct == 0 and actual == 0 → skipped (no exposure on either side).

Per-intent cap (AC53):
  MAX_REBALANCE_PCT_PER_CYCLE = 0.25 (25% of equity per intent)
  rebalance_capped_delta_eur  = clamp(delta, ±max_rebalance_eur)

Portfolio-level budget (AC54):
  MAX_PORTFOLIO_REBALANCE_PCT = 0.30 (30% of equity total across all intents)
  Intents sorted HIGH → MEDIUM, then by abs(drift_pct) descending.
  Selected greedily until portfolio_rebalance_budget_eur is exhausted.
  rebalance_selected = True / False per intent.

Usage: python ant_colony/build_rebalance_intents_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

DRIFT_PATH       = OUT_DIR / "allocation_portfolio_drift.json"
PORTFOLIO_PATH   = OUT_DIR / "paper_portfolio_state.json"

OUT_PATH       = OUT_DIR / "rebalance_intents.json"
OUT_TSV_PATH   = OUT_DIR / "rebalance_intents.tsv"
AUDIT_PATH     = OUT_DIR / "rebalance_budget_audit.json"
AUDIT_TSV_PATH = OUT_DIR / "rebalance_budget_audit.tsv"

TSV_HEADERS = [
    "market", "strategy", "position_key",
    "rebalance_action", "drift_severity", "drift_cause",
    "target_notional_eur", "actual_notional_eur",
    "rebalance_delta_eur", "rebalance_capped_delta_eur",
    "rebalance_cap_applied", "rebalance_reason",
    "rebalance_selected", "rebalance_budget_reason",
]

AUDIT_TSV_HEADERS = [
    "market", "strategy", "position_key",
    "drift_severity", "drift_pct", "rebalance_action",
    "rebalance_capped_delta_eur", "priority_rank",
    "rebalance_selected", "rebalance_budget_reason", "audit_decision_reason",
]

MAX_REBALANCE_PCT_PER_CYCLE  = 0.25  # per-intent cap (AC53)
MAX_PORTFOLIO_REBALANCE_PCT  = 0.30  # portfolio-level total cap (AC54)

_SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1}  # lower = higher priority

# Drift status → rebalance action
_ACTION_MAP = {
    "UNDER_ALLOCATED":    "REBALANCE_INCREASE",
    "OVER_ALLOCATED":     "REBALANCE_REDUCE",
    "NO_POSITION":        "REBALANCE_OPEN",
    "UNEXPECTED_POSITION": "REBALANCE_CLOSE",
    "DRIFT_OK":           "REBALANCE_HOLD",
}

_ACTIVE_ACTIONS = frozenset({"REBALANCE_INCREASE", "REBALANCE_REDUCE",
                              "REBALANCE_OPEN", "REBALANCE_CLOSE"})


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


def build_audit_decision_reason(intent: dict) -> str:
    """AC55: compact human-readable explanation of budget decision per intent."""
    sel   = intent.get("rebalance_selected", False)
    sev   = intent.get("drift_severity", "?")
    pct   = to_float(intent.get("drift_pct", 0.0))
    delta = to_float(intent.get("rebalance_capped_delta_eur", 0.0))
    if sel:
        return f"SELECTED | {sev} | DRIFT_{pct:.6f} | DELTA_{delta:.2f} | WITHIN_BUDGET"
    return f"EXCLUDED | {sev} | DRIFT_{pct:.6f} | DELTA_{delta:.2f} | BUDGET_LIMIT"


def build_audit_summary_reason(selected_count: int, excluded_count: int, intents: list) -> str:
    """AC55: single-string cycle-level explanation of budget outcome."""
    if not intents:
        return "NO_REBALANCE_CANDIDATES"
    if excluded_count == 0:
        return "ALL_CANDIDATES_FIT_WITHIN_BUDGET"
    if selected_count == 1:
        return "ONLY_HIGHEST_PRIORITY_INTENT_FIT_WITHIN_BUDGET"
    med_total    = sum(1 for i in intents if i.get("drift_severity") == "MEDIUM")
    med_selected = sum(1 for i in intents
                       if i.get("rebalance_selected") and i.get("drift_severity") == "MEDIUM")
    high_selected = sum(1 for i in intents
                        if i.get("rebalance_selected") and i.get("drift_severity") == "HIGH")
    if med_total > 0 and med_selected == 0 and high_selected > 0:
        return "HIGH_PRIORITY_CONSUMED_BUDGET"
    return "PARTIAL_BUDGET_UTILIZATION"


def write_tsv(path: Path, headers: list, rows: list):
    lines = ["\t".join(headers)]
    for row in rows:
        lines.append("\t".join(
            "" if row.get(h) is None else str(row.get(h))
            for h in headers
        ))
    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    ts = utc_now_ts()

    drift_data = load_json(DRIFT_PATH, {}) or {}
    portfolio  = load_json(PORTFOLIO_PATH, {}) or {}

    cycle_id = drift_data.get("cycle_id")
    drift_rows = drift_data.get("rows") or []

    # Equity: prefer portfolio_state (live), fallback to drift file
    equity = to_float(portfolio.get("equity") or drift_data.get("equity") or 0.0)
    max_rebalance_eur = round(equity * MAX_REBALANCE_PCT_PER_CYCLE, 2)

    intents = []
    action_counts = {
        "REBALANCE_INCREASE": 0,
        "REBALANCE_REDUCE":   0,
        "REBALANCE_OPEN":     0,
        "REBALANCE_CLOSE":    0,
        "REBALANCE_HOLD":     0,
    }
    capped_count = 0
    skipped_low  = 0
    skipped_zero = 0

    for row in drift_rows:
        drift_severity = str(row.get("drift_severity") or "LOW")
        drift_status   = str(row.get("drift_status") or "DRIFT_OK")
        allocation_pct = to_float(row.get("allocation_pct", 0.0))
        actual         = to_float(row.get("actual_notional_eur", 0.0))

        # Skip LOW severity — not worth acting on
        if drift_severity == "LOW":
            skipped_low += 1
            continue

        # Skip when there is nothing on either side (e.g. intentionally blocked with 0 alloc)
        if allocation_pct <= 0.0 and actual <= 0.0:
            skipped_zero += 1
            continue

        rebalance_action = _ACTION_MAP.get(drift_status, "REBALANCE_HOLD")

        # For REBALANCE_HOLD (DRIFT_OK) at MEDIUM/HIGH — still skip, not actionable
        if rebalance_action == "REBALANCE_HOLD":
            action_counts["REBALANCE_HOLD"] += 1
            continue

        target = to_float(row.get("target_notional_eur", 0.0))
        delta  = round(target - actual, 2)   # positive = need more, negative = need less

        # Cap: clamp magnitude to max_rebalance_eur, preserve sign
        if abs(delta) > max_rebalance_eur:
            sign = 1 if delta >= 0 else -1
            capped_delta = round(sign * max_rebalance_eur, 2)
            cap_applied  = True
            capped_count += 1
        else:
            capped_delta = delta
            cap_applied  = False

        rebalance_reason = f"{drift_status}_{drift_severity}"

        action_counts[rebalance_action] = action_counts.get(rebalance_action, 0) + 1

        intents.append({
            # Identity
            "market":       row.get("market"),
            "strategy":     row.get("strategy"),
            "position_key": row.get("position_key"),
            "cycle_id":     cycle_id,
            # Rebalance
            "rebalance_action":          rebalance_action,
            "rebalance_reason":          rebalance_reason,
            "drift_pct":                 row.get("drift_pct"),
            "drift_eur":                 row.get("drift_eur"),
            "drift_severity":            drift_severity,
            "drift_cause":               row.get("drift_cause"),
            "drift_status":              drift_status,
            # Sizing
            "equity":                    equity,
            "max_rebalance_eur":         max_rebalance_eur,
            "target_notional_eur":       target,
            "actual_notional_eur":       actual,
            "rebalance_delta_eur":       delta,
            "rebalance_capped_delta_eur": capped_delta,
            "rebalance_cap_applied":     cap_applied,
            # Passthrough context
            "allocation_pct":            allocation_pct,
            "reconciliation_status":     row.get("reconciliation_status"),
            "effective_action":          row.get("effective_action"),
        })

    total_abs_rebalance = round(sum(abs(i["rebalance_capped_delta_eur"]) for i in intents), 2)

    # === AC54: portfolio-level risk budget ===
    portfolio_budget = round(equity * MAX_PORTFOLIO_REBALANCE_PCT, 2)

    # Sort: HIGH before MEDIUM, then largest abs(drift_pct) first within same severity
    intents.sort(key=lambda i: (
        _SEVERITY_ORDER.get(i["drift_severity"], 99),
        -abs(to_float(i.get("drift_pct", 0.0))),
    ))

    # AC55: assign priority_rank after sort
    for rank, intent in enumerate(intents, 1):
        intent["priority_rank"] = rank

    running_sum    = 0.0
    selected_count = 0
    excluded_count = 0
    selection_seq  = 0

    for intent in intents:
        abs_delta = abs(to_float(intent.get("rebalance_capped_delta_eur", 0.0)))
        before = round(running_sum, 2)

        if portfolio_budget <= 0.0:
            intent["rebalance_selected"]      = False
            intent["rebalance_budget_reason"] = "EXCLUDED_BUDGET_LIMIT"
            intent["selection_order"]         = None
            excluded_count += 1
        elif running_sum + abs_delta <= portfolio_budget:
            selection_seq += 1
            intent["rebalance_selected"]      = True
            intent["rebalance_budget_reason"] = "SELECTED_WITHIN_BUDGET"
            intent["selection_order"]         = selection_seq
            running_sum = round(running_sum + abs_delta, 2)
            selected_count += 1
        else:
            intent["rebalance_selected"]      = False
            intent["rebalance_budget_reason"] = "EXCLUDED_BUDGET_LIMIT"
            intent["selection_order"]         = None
            excluded_count += 1

        after = round(running_sum, 2)
        intent["budget_running_before_eur"]   = before
        intent["budget_running_after_eur"]    = after
        intent["budget_remaining_after_eur"]  = round(portfolio_budget - after, 2)

    # AC55: build audit_decision_reason for every intent
    for intent in intents:
        intent["audit_decision_reason"] = build_audit_decision_reason(intent)

    utilization_pct = round(running_sum / max(portfolio_budget, 1.0), 4)

    out = {
        "component": "build_rebalance_intents_lite",
        "ts_utc": ts,
        "cycle_id": cycle_id,
        "equity": equity,
        # Per-intent cap (AC53)
        "max_rebalance_pct_per_cycle": MAX_REBALANCE_PCT_PER_CYCLE,
        "max_rebalance_eur": max_rebalance_eur,
        "capped_count": capped_count,
        # Portfolio budget (AC54)
        "max_portfolio_rebalance_pct": MAX_PORTFOLIO_REBALANCE_PCT,
        "portfolio_rebalance_budget_eur": portfolio_budget,
        "portfolio_rebalance_used_eur": round(running_sum, 2),
        "portfolio_rebalance_utilization_pct": utilization_pct,
        "selected_count": selected_count,
        "excluded_count": excluded_count,
        # Intent counts
        "intents_total": len(intents),
        "increase_count": action_counts["REBALANCE_INCREASE"],
        "reduce_count":   action_counts["REBALANCE_REDUCE"],
        "open_count":     action_counts["REBALANCE_OPEN"],
        "close_count":    action_counts["REBALANCE_CLOSE"],
        "hold_count":     action_counts["REBALANCE_HOLD"],
        "skipped_low_severity": skipped_low,
        "skipped_zero_exposure": skipped_zero,
        "total_abs_rebalance_eur": total_abs_rebalance,
        "intents": intents,
    }

    write_json(OUT_PATH, out)
    write_tsv(OUT_TSV_PATH, TSV_HEADERS, intents)

    # === AC55: rebalance budget audit trail ===
    try:
        high_cand  = sum(1 for i in intents if i.get("drift_severity") == "HIGH")
        med_cand   = sum(1 for i in intents if i.get("drift_severity") == "MEDIUM")
        high_sel   = sum(1 for i in intents if i.get("rebalance_selected") and i.get("drift_severity") == "HIGH")
        med_sel    = sum(1 for i in intents if i.get("rebalance_selected") and i.get("drift_severity") == "MEDIUM")
        sel_eur    = round(sum(abs(to_float(i.get("rebalance_capped_delta_eur", 0))) for i in intents if i.get("rebalance_selected")), 2)
        excl_eur   = round(sum(abs(to_float(i.get("rebalance_capped_delta_eur", 0))) for i in intents if not i.get("rebalance_selected")), 2)
        lg_sel     = max((i for i in intents if i.get("rebalance_selected")),
                         key=lambda i: abs(to_float(i.get("rebalance_capped_delta_eur", 0))), default=None)
        lg_excl    = max((i for i in intents if not i.get("rebalance_selected")),
                         key=lambda i: abs(to_float(i.get("rebalance_capped_delta_eur", 0))), default=None)

        audit_rows = [
            {k: i.get(k) for k in (
                "market", "strategy", "position_key", "drift_status", "drift_severity",
                "drift_pct", "rebalance_action", "rebalance_delta_eur",
                "rebalance_capped_delta_eur", "priority_rank", "selection_order",
                "rebalance_selected", "rebalance_budget_reason",
                "budget_running_before_eur", "budget_running_after_eur",
                "budget_remaining_after_eur", "audit_decision_reason",
            )}
            for i in intents
        ]

        audit = {
            "component": "rebalance_budget_audit",
            "ts_utc": ts,
            "cycle_id": cycle_id,
            "equity": equity,
            "portfolio_rebalance_budget_eur": portfolio_budget,
            "portfolio_rebalance_used_eur": round(running_sum, 2),
            "portfolio_rebalance_utilization_pct": utilization_pct,
            "candidate_count": len(intents),
            "selected_count": selected_count,
            "excluded_count": excluded_count,
            "selection_mode": "GREEDY_SEVERITY_THEN_DRIFT",
            "summary": {
                "high_candidates": high_cand,
                "medium_candidates": med_cand,
                "high_selected": high_sel,
                "medium_selected": med_sel,
                "total_selected_abs_eur": sel_eur,
                "total_excluded_abs_eur": excl_eur,
                "largest_selected_position_key": lg_sel["position_key"] if lg_sel else None,
                "largest_excluded_position_key": lg_excl["position_key"] if lg_excl else None,
                "audit_summary_reason": build_audit_summary_reason(selected_count, excluded_count, intents),
            },
            "rows": audit_rows,
        }
        write_json(AUDIT_PATH, audit)
        write_tsv(AUDIT_TSV_PATH, AUDIT_TSV_HEADERS, audit_rows)
    except Exception:
        pass  # audit must not crash main rebalance write

    print(json.dumps({k: v for k, v in out.items() if k != "intents"}, indent=2))


if __name__ == "__main__":
    main()
