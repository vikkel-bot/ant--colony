"""
AC51: Allocation vs portfolio drift control
Pure observability — measures drift between queen allocation targets and actual portfolio
exposure per position_key. No rebalancing, no orders, no execution changes.

Inputs:
  execution_summary.json                    — allocation targets per market/strategy
  paper_portfolio_state.json                — actual positions and equity
  allocation_execution_reconciliation.json  — reconciliation_status per position_key (AC50)

Outputs:
  allocation_portfolio_drift.json
  allocation_portfolio_drift.tsv

Drift:
  target_notional_eur = allocation_pct * equity
  actual_notional_eur = position.notional_eur (entry notional) or 0
  drift_eur           = actual - target
  drift_pct           = drift_eur / max(equity, 1)

Classification (priority order):
  UNEXPECTED_POSITION  actual > 0 and target == 0
  NO_POSITION          actual == 0 and target > 0
  OVER_ALLOCATED       drift_pct > 0.05
  UNDER_ALLOCATED      drift_pct < -0.05
  DRIFT_OK             abs(drift_pct) < 0.05

Usage: python ant_colony/allocation_portfolio_drift_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

EXECUTION_SUMMARY_PATH = OUT_DIR / "execution_summary.json"
PORTFOLIO_STATE_PATH   = OUT_DIR / "paper_portfolio_state.json"
RECONCILIATION_PATH    = OUT_DIR / "allocation_execution_reconciliation.json"

OUT_PATH     = OUT_DIR / "allocation_portfolio_drift.json"
OUT_TSV_PATH = OUT_DIR / "allocation_portfolio_drift.tsv"

TSV_HEADERS = [
    "market", "strategy", "position_key",
    "allocation_pct", "actual_notional_eur", "target_notional_eur",
    "drift_pct", "drift_status",
]

DRIFT_THRESHOLD = 0.05  # 5% of equity

DRIFT_OK              = "DRIFT_OK"
DRIFT_OVER            = "OVER_ALLOCATED"
DRIFT_UNDER           = "UNDER_ALLOCATED"
DRIFT_NO_POSITION     = "NO_POSITION"
DRIFT_UNEXPECTED      = "UNEXPECTED_POSITION"


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
        return f if f == f else float(default)  # NaN guard
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


def classify_drift(actual: float, target: float, drift_pct: float) -> tuple:
    """
    Returns (drift_status, drift_reason).
    Priority: UNEXPECTED_POSITION > NO_POSITION > OVER/UNDER > OK.
    """
    if actual > 0 and target <= 0:
        return DRIFT_UNEXPECTED, "POSITION_WITHOUT_TARGET"
    if actual <= 0 and target > 0:
        return DRIFT_NO_POSITION, "NO_POSITION_BUT_TARGET"
    if drift_pct > DRIFT_THRESHOLD:
        return DRIFT_OVER, "ACTUAL_GT_TARGET"
    if drift_pct < -DRIFT_THRESHOLD:
        return DRIFT_UNDER, "TARGET_GT_ACTUAL"
    return DRIFT_OK, "WITHIN_TOLERANCE"


def main():
    ts = utc_now_ts()

    exec_summary   = load_json(EXECUTION_SUMMARY_PATH, {}) or {}
    portfolio      = load_json(PORTFOLIO_STATE_PATH, {}) or {}
    reconciliation = load_json(RECONCILIATION_PATH, {}) or {}

    cycle_id     = exec_summary.get("cycle_id")
    markets_data = exec_summary.get("markets") or {}

    # Equity: authoritative from portfolio state; fallback 0 (fail-closed: drift_pct = 0)
    equity = to_float(portfolio.get("equity", 0.0))

    # Positions keyed by position_key
    raw_positions = portfolio.get("positions") or {}
    if not isinstance(raw_positions, dict):
        raw_positions = {}

    # Reconciliation index: position_key → reconciliation_status
    recon_index = {}
    for row in (reconciliation.get("rows") or []):
        pk = row.get("position_key")
        if pk:
            recon_index[pk] = row.get("reconciliation_status")

    rows = []
    seen_position_keys = set()
    status_counts = {}

    for market in sorted(markets_data.keys()):
        strategies_data = (markets_data[market].get("strategies") or {})

        for strategy in sorted(strategies_data.keys()):
            sr           = strategies_data[strategy] or {}
            position_key = f"{market}__{strategy}"
            seen_position_keys.add(position_key)

            # Load intent for allocation_pct and execution fields
            intent = {}
            intent_file = sr.get("intent_file")
            if intent_file:
                intent = load_json(Path(intent_file), {}) or {}
            if not intent:
                intent = load_json(OUT_DIR / f"{position_key}_execution_intent.json", {}) or {}

            def _get(key, default=None):
                return intent.get(key) if intent.get(key) is not None else sr.get(key, default)

            allocation_pct    = to_float(_get("allocation_pct", 0.0))
            effective_action  = str(_get("effective_action") or _get("action") or "NO_ACTION")
            execution_allowed = bool(_get("execution_allowed", _get("allowed", False)))

            # Target: what queen wants
            target_notional = round(allocation_pct * equity, 2)

            # Actual: current position notional (at entry price)
            pos = raw_positions.get(position_key) or {}
            if str((pos.get("position") or "FLAT")).upper() == "LONG":
                actual_notional = to_float(pos.get("notional_eur", 0.0))
            else:
                actual_notional = 0.0
            actual_notional = max(actual_notional, 0.0)

            drift_eur = round(actual_notional - target_notional, 2)
            drift_pct = round(drift_eur / max(equity, 1.0), 6)

            drift_status, drift_reason = classify_drift(actual_notional, target_notional, drift_pct)
            status_counts[drift_status] = status_counts.get(drift_status, 0) + 1

            rows.append({
                "market": market,
                "strategy": strategy,
                "position_key": position_key,
                "cycle_id": cycle_id,
                "allocation_pct": allocation_pct,
                "target_notional_eur": target_notional,
                "actual_notional_eur": actual_notional,
                "drift_eur": drift_eur,
                "drift_pct": drift_pct,
                "drift_status": drift_status,
                "drift_reason": drift_reason,
                "effective_action": effective_action,
                "execution_allowed": execution_allowed,
                "reconciliation_status": recon_index.get(position_key),
                "equity_used": equity,
            })

    # Unexpected positions: in portfolio but not in execution_summary
    for position_key, pos in sorted(raw_positions.items()):
        if position_key in seen_position_keys:
            continue
        if not isinstance(pos, dict):
            continue
        if str((pos.get("position") or "FLAT")).upper() != "LONG":
            continue

        actual_notional = max(to_float(pos.get("notional_eur", 0.0)), 0.0)
        parts = position_key.split("__", 1)
        market   = parts[0] if len(parts) > 1 else position_key
        strategy = parts[1] if len(parts) > 1 else "UNKNOWN"

        drift_eur = round(actual_notional, 2)  # target=0, drift=actual
        drift_pct = round(drift_eur / max(equity, 1.0), 6)

        status_counts[DRIFT_UNEXPECTED] = status_counts.get(DRIFT_UNEXPECTED, 0) + 1

        rows.append({
            "market": market,
            "strategy": strategy,
            "position_key": position_key,
            "cycle_id": cycle_id,
            "allocation_pct": 0.0,
            "target_notional_eur": 0.0,
            "actual_notional_eur": actual_notional,
            "drift_eur": drift_eur,
            "drift_pct": drift_pct,
            "drift_status": DRIFT_UNEXPECTED,
            "drift_reason": "POSITION_WITHOUT_TARGET",
            "effective_action": None,
            "execution_allowed": None,
            "reconciliation_status": recon_index.get(position_key),
            "equity_used": equity,
        })

    total_abs_drift_eur = round(sum(abs(r["drift_eur"]) for r in rows), 2)
    total_abs_drift_pct = round(sum(abs(r["drift_pct"]) for r in rows), 6)

    out = {
        "component": "allocation_portfolio_drift_lite",
        "ts_utc": ts,
        "cycle_id": cycle_id,
        "equity": equity,
        "rows_total": len(rows),
        "drift_ok_count": status_counts.get(DRIFT_OK, 0),
        "over_allocated_count": status_counts.get(DRIFT_OVER, 0),
        "under_allocated_count": status_counts.get(DRIFT_UNDER, 0),
        "no_position_count": status_counts.get(DRIFT_NO_POSITION, 0),
        "unexpected_position_count": status_counts.get(DRIFT_UNEXPECTED, 0),
        "status_counts": status_counts,
        "total_abs_drift_eur": total_abs_drift_eur,
        "total_abs_drift_pct": total_abs_drift_pct,
        "rows": rows,
    }

    write_json(OUT_PATH, out)
    write_tsv(OUT_TSV_PATH, TSV_HEADERS, rows)

    print(json.dumps({k: v for k, v in out.items() if k != "rows"}, indent=2))


if __name__ == "__main__":
    main()
