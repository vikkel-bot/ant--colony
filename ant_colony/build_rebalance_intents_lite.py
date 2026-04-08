"""
AC53: Controlled rebalance intents (no execution)
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

Cap:
  max_rebalance_pct_per_cycle = 0.25 (25% of equity per intent, hardcoded)
  rebalance_capped_delta_eur  = clamp(delta, -max_eur, +max_eur)

Usage: python ant_colony/build_rebalance_intents_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

DRIFT_PATH       = OUT_DIR / "allocation_portfolio_drift.json"
PORTFOLIO_PATH   = OUT_DIR / "paper_portfolio_state.json"

OUT_PATH     = OUT_DIR / "rebalance_intents.json"
OUT_TSV_PATH = OUT_DIR / "rebalance_intents.tsv"

TSV_HEADERS = [
    "market", "strategy", "position_key",
    "rebalance_action", "drift_severity", "drift_cause",
    "target_notional_eur", "actual_notional_eur",
    "rebalance_delta_eur", "rebalance_capped_delta_eur",
    "rebalance_cap_applied", "rebalance_reason",
]

MAX_REBALANCE_PCT_PER_CYCLE = 0.25

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

    out = {
        "component": "build_rebalance_intents_lite",
        "ts_utc": ts,
        "cycle_id": cycle_id,
        "equity": equity,
        "max_rebalance_pct_per_cycle": MAX_REBALANCE_PCT_PER_CYCLE,
        "max_rebalance_eur": max_rebalance_eur,
        "intents_total": len(intents),
        "increase_count": action_counts["REBALANCE_INCREASE"],
        "reduce_count":   action_counts["REBALANCE_REDUCE"],
        "open_count":     action_counts["REBALANCE_OPEN"],
        "close_count":    action_counts["REBALANCE_CLOSE"],
        "hold_count":     action_counts["REBALANCE_HOLD"],
        "capped_count":   capped_count,
        "skipped_low_severity": skipped_low,
        "skipped_zero_exposure": skipped_zero,
        "total_abs_rebalance_eur": total_abs_rebalance,
        "intents": intents,
    }

    write_json(OUT_PATH, out)
    write_tsv(OUT_TSV_PATH, TSV_HEADERS, intents)

    print(json.dumps({k: v for k, v in out.items() if k != "intents"}, indent=2))


if __name__ == "__main__":
    main()
