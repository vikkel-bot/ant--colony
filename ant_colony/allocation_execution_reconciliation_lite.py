"""
AC50: Allocation-to-execution reconciliation
Matches queen allocation intents against paper_execution_log to explain per position_key:
  signal → allocation_pct → requested_notional_eur → granted_notional_eur → executed / skipped

Input:
  execution_summary.json          — intent paths, allocation state per strategy
  paper_execution_log.jsonl       — what the runner actually executed or skipped
  {position_key}_execution_intent.json — detailed intent per strategy (decision_id, extra fields)

Output:
  allocation_execution_reconciliation.json
  allocation_execution_reconciliation.tsv

Matching strategy: primary on decision_id (exact match, no fuzzy logic).
Fail-closed: missing files → no crash, status = MISSING_EXECUTION_EVIDENCE or SKIPPED_BLOCKED.

Usage: python ant_colony/allocation_execution_reconciliation_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

EXECUTION_SUMMARY_PATH  = OUT_DIR / "execution_summary.json"
PAPER_EXEC_SUMMARY_PATH = OUT_DIR / "paper_execution_summary.json"
EXECUTION_LOG_PATH      = OUT_DIR / "paper_execution_log.jsonl"

OUT_PATH     = OUT_DIR / "allocation_execution_reconciliation.json"
OUT_TSV_PATH = OUT_DIR / "allocation_execution_reconciliation.tsv"

TSV_HEADERS = [
    "market", "strategy", "position_key",
    "effective_action", "allocation_pct",
    "requested_notional_eur", "granted_notional_eur",
    "reconciliation_status", "execution_skip_reason",
    "request_minus_granted_eur",
]

# Statussen (deterministisch, geen overlap)
STATUS_FULLY_EXECUTED              = "FULLY_EXECUTED"
STATUS_PARTIALLY_GRANTED           = "PARTIALLY_GRANTED"
STATUS_SKIPPED_ALREADY_IN_POSITION = "SKIPPED_ALREADY_IN_POSITION"
STATUS_SKIPPED_DUST_FILTERED       = "SKIPPED_DUST_FILTERED"
STATUS_SKIPPED_NO_ACTION           = "SKIPPED_NO_ACTION"
STATUS_SKIPPED_BLOCKED             = "SKIPPED_BLOCKED"
STATUS_SKIPPED_OTHER               = "SKIPPED_OTHER"
STATUS_NOT_REQUESTED               = "NOT_REQUESTED"
STATUS_MISSING_EVIDENCE            = "MISSING_EXECUTION_EVIDENCE"


def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def load_jsonl(path: Path) -> list:
    rows = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    except Exception:
        pass
    return rows


def to_float(v, default=0.0):
    try:
        return float(v)
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


def build_log_index(log_rows: list) -> dict:
    """
    Index execution log by decision_id (last entry wins per id).
    Handles both current runner format and legacy format.
    """
    idx = {}
    for row in log_rows:
        did = str(row.get("decision_id", "") or "").strip()
        if did:
            idx[did] = row
    return idx


def read_granted(log_entry: dict) -> float:
    """Read granted_notional_eur, with fallback to legacy notional_eur field."""
    v = log_entry.get("granted_notional_eur")
    if v is None:
        v = log_entry.get("notional_eur")
    return to_float(v)


def determine_status(
    execution_allowed: bool,
    effective_action: str,
    requested_notional: float,
    block_reason,
    log_entry,
) -> tuple:
    """
    Returns (status, granted_notional, skip_reason, already_in_pos, dust_filtered, budget_limited).
    Deterministic. No fuzzy matching.
    """
    if not execution_allowed:
        return (STATUS_SKIPPED_BLOCKED, 0.0,
                str(block_reason or "BLOCKED"), False, False, False)

    if effective_action != "ENTER_LONG":
        return (STATUS_SKIPPED_NO_ACTION, 0.0, effective_action, False, False, False)

    if requested_notional <= 0.0:
        return (STATUS_NOT_REQUESTED, 0.0, "ZERO_REQUESTED_NOTIONAL", False, False, False)

    if log_entry is None:
        return (STATUS_MISSING_EVIDENCE, 0.0, None, False, False, False)

    log_action = str(log_entry.get("action", "") or "").upper()
    log_reason = str(log_entry.get("reason", "") or "")

    if log_action == "ENTER_LONG":
        granted = read_granted(log_entry)
        budget_limited = bool(log_entry.get("skipped_due_to_budget", False))
        if budget_limited:
            return (STATUS_PARTIALLY_GRANTED, granted, "BUDGET_SCALED", False, False, True)
        return (STATUS_FULLY_EXECUTED, granted, None, False, False, False)

    if log_action == "SKIP":
        if log_reason == "ALREADY_IN_POSITION":
            return (STATUS_SKIPPED_ALREADY_IN_POSITION, 0.0, log_reason, True, False, False)
        if log_reason == "DUST_FILTERED":
            granted = read_granted(log_entry)
            return (STATUS_SKIPPED_DUST_FILTERED, granted, log_reason, False, True, False)
        return (STATUS_SKIPPED_OTHER, 0.0, log_reason, False, False, False)

    return (STATUS_MISSING_EVIDENCE, 0.0, None, False, False, False)


def main():
    ts = utc_now_ts()

    exec_summary      = load_json(EXECUTION_SUMMARY_PATH, {}) or {}
    paper_exec_summary = load_json(PAPER_EXEC_SUMMARY_PATH, {}) or {}
    log_rows          = load_jsonl(EXECUTION_LOG_PATH)
    log_index         = build_log_index(log_rows)

    # cycle_id: added to summary in AC50; fallback from first intent encountered
    cycle_id    = exec_summary.get("cycle_id")
    markets_data = exec_summary.get("markets") or {}

    rows = []
    total_requested = 0.0
    total_granted   = 0.0
    status_counts: dict = {}

    for market in sorted(markets_data.keys()):
        strategies_data = (markets_data[market].get("strategies") or {})

        for strategy in sorted(strategies_data.keys()):
            sr           = strategies_data[strategy] or {}
            position_key = f"{market}__{strategy}"

            # Load intent file for decision_id and extra fields
            intent = {}
            intent_file = sr.get("intent_file")
            if intent_file:
                intent = load_json(Path(intent_file), {}) or {}
            if not intent:
                intent = load_json(OUT_DIR / f"{position_key}_execution_intent.json", {}) or {}

            # Infer cycle_id from intent if still unknown
            if not cycle_id and intent.get("cycle_id"):
                cycle_id = intent["cycle_id"]

            # decision_id: from intent file; construct as fallback
            decision_id = str(intent.get("decision_id") or "").strip()
            if not decision_id:
                eff = str(sr.get("effective_action") or sr.get("action") or "NO_ACTION")
                decision_id = f"{position_key}_{cycle_id or 'UNKNOWN'}_{eff}"

            # Allocation fields — intent takes priority over strategy_results
            def _get(key, default=None):
                return intent.get(key) if intent.get(key) is not None else sr.get(key, default)

            execution_allowed   = bool(_get("execution_allowed", _get("allowed", False)))
            effective_action    = str(_get("effective_action") or _get("action") or "NO_ACTION")
            requested_notional  = to_float(_get("requested_notional_eur", 0.0))
            allocation_pct      = to_float(_get("allocation_pct", 0.0))
            block_reason        = _get("block_reason") or _get("reason") or _get("effective_reason")

            # Match against execution log
            log_entry = log_index.get(decision_id)

            status, granted_notional, skip_reason, already_in_pos, dust_filtered, budget_limited = (
                determine_status(
                    execution_allowed, effective_action,
                    requested_notional, block_reason, log_entry
                )
            )

            delta = round(requested_notional - granted_notional, 2)
            ratio = (round(granted_notional / requested_notional, 4)
                     if requested_notional > 0 else None)

            total_requested = round(total_requested + requested_notional, 2)
            total_granted   = round(total_granted + granted_notional, 2)
            status_counts[status] = status_counts.get(status, 0) + 1

            rows.append({
                # Identity
                "market": market,
                "strategy": strategy,
                "position_key": position_key,
                "cycle_id": cycle_id,
                "decision_id": decision_id,
                # Queen intent / allocation
                "signal_action": _get("signal_action"),
                "effective_action": effective_action,
                "execution_allowed": execution_allowed,
                "allocation_pct": allocation_pct,
                "requested_notional_eur": requested_notional,
                "allocation_reason": _get("allocation_reason"),
                "allocation_bias_reason": _get("allocation_bias_reason"),
                "smoothing_reason": _get("smoothing_reason"),
                "guardrail_reason": _get("guardrail_reason"),
                "regime_type": _get("regime_type"),
                # Execution outcome
                "granted_notional_eur": granted_notional,
                "executed_action": log_entry.get("action") if log_entry else None,
                "executed": status == STATUS_FULLY_EXECUTED,
                "execution_skip_reason": skip_reason,
                "execution_log_found": log_entry is not None,
                "already_in_position": already_in_pos,
                "dust_filtered": dust_filtered,
                "budget_limited": budget_limited,
                # Delta / reconciliation
                "request_minus_granted_eur": delta,
                "requested_vs_granted_ratio": ratio,
                "reconciliation_status": status,
            })

    fully_executed = status_counts.get(STATUS_FULLY_EXECUTED, 0)
    partial        = status_counts.get(STATUS_PARTIALLY_GRANTED, 0)
    skipped        = sum(v for k, v in status_counts.items() if k.startswith("SKIPPED"))
    missing        = status_counts.get(STATUS_MISSING_EVIDENCE, 0)
    not_req        = status_counts.get(STATUS_NOT_REQUESTED, 0)

    out = {
        "component": "allocation_execution_reconciliation_lite",
        "ts_utc": ts,
        "cycle_id": cycle_id,
        "rows_total": len(rows),
        "fully_executed_count": fully_executed,
        "partially_granted_count": partial,
        "skipped_count": skipped,
        "missing_execution_evidence_count": missing,
        "not_requested_count": not_req,
        "status_counts": status_counts,
        "total_requested_eur": total_requested,
        "total_granted_eur": total_granted,
        "total_request_minus_granted_eur": round(total_requested - total_granted, 2),
        "source_log_rows": len(log_rows),
        "source_log_indexed": len(log_index),
        "rows": rows,
    }

    write_json(OUT_PATH, out)
    write_tsv(OUT_TSV_PATH, TSV_HEADERS, rows)

    # Print compact summary (no rows)
    print(json.dumps({k: v for k, v in out.items() if k != "rows"}, indent=2))


if __name__ == "__main__":
    main()
