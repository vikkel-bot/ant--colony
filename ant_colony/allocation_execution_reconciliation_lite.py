"""
AC50.1: Allocation-to-execution reconciliation (strict cycle-scoped)
Matches queen allocation intents against paper_execution_log to explain per position_key:
  signal → allocation_pct → requested_notional_eur → granted_notional_eur → executed / skipped

Cycle scoping (AC50.1):
  Reconciliation only uses execution log entries from the SAME cycle as the active intent.
  Cycle is extracted from decision_id via timestamp pattern (\\d{8}T\\d{6}Z).
  Cross-cycle matches → CROSS_CYCLE_EVIDENCE (not counted in totals).
  No match at all   → NO_SAME_CYCLE_EXECUTION.

Input:
  execution_summary.json          — intent paths, allocation state per strategy, active cycle_id
  paper_execution_log.jsonl       — what the runner actually executed or skipped (all cycles)
  {position_key}_execution_intent.json — detailed intent per strategy (decision_id, extra fields)

Output:
  allocation_execution_reconciliation.json
  allocation_execution_reconciliation.tsv

Matching strategy: primary on decision_id (exact), same-cycle only.
Fail-closed: missing files → no crash, status = NO_SAME_CYCLE_EXECUTION or SKIPPED_BLOCKED.

Usage: python ant_colony/allocation_execution_reconciliation_lite.py
"""
import json
import re
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
    "cycle_match_status",
]

# Cycle match classifications
CYCLE_MATCH_SAME    = "SAME_CYCLE"
CYCLE_MATCH_CROSS   = "CROSS_CYCLE"
CYCLE_MATCH_UNKNOWN = "UNKNOWN_CYCLE"

# Regex to extract embedded cycle timestamp from decision_id
_CYCLE_ID_RE = re.compile(r"\d{8}T\d{6}Z")

# Reconciliation statuses (deterministic, no overlap)
STATUS_FULLY_EXECUTED              = "FULLY_EXECUTED"
STATUS_PARTIALLY_GRANTED           = "PARTIALLY_GRANTED"
STATUS_SKIPPED_ALREADY_IN_POSITION = "SKIPPED_ALREADY_IN_POSITION"
STATUS_SKIPPED_DUST_FILTERED       = "SKIPPED_DUST_FILTERED"
STATUS_SKIPPED_NO_ACTION           = "SKIPPED_NO_ACTION"
STATUS_SKIPPED_BLOCKED             = "SKIPPED_BLOCKED"
STATUS_SKIPPED_OTHER               = "SKIPPED_OTHER"
STATUS_NOT_REQUESTED               = "NOT_REQUESTED"
STATUS_CROSS_CYCLE_EVIDENCE        = "CROSS_CYCLE_EVIDENCE"
STATUS_NO_SAME_CYCLE_EXECUTION     = "NO_SAME_CYCLE_EXECUTION"

# Intent-side-only statuses (no execution log entry needed)
_INTENT_SIDE_STATUSES = frozenset({
    STATUS_SKIPPED_BLOCKED,
    STATUS_SKIPPED_NO_ACTION,
    STATUS_NOT_REQUESTED,
})


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


def extract_cycle_id_from_str(s: str) -> str:
    """Extract embedded cycle timestamp from a string (e.g. decision_id). Returns 'UNKNOWN' if not parseable."""
    m = _CYCLE_ID_RE.search(s or "")
    return m.group(0) if m else "UNKNOWN"


def build_log_indices(log_rows: list, active_cycle_id: str) -> tuple:
    """
    Build two indices keyed by decision_id (last entry wins per id):
      same_cycle_index — only rows whose embedded cycle_id matches active_cycle_id
      all_index        — all rows regardless of cycle

    Also returns log_cycle_map: decision_id → extracted cycle_id from log entry.
    """
    all_index = {}
    same_cycle_index = {}
    log_cycle_map = {}

    for row in log_rows:
        did = str(row.get("decision_id", "") or "").strip()
        if not did:
            continue
        log_cycle_id = extract_cycle_id_from_str(did)
        all_index[did] = row
        log_cycle_map[did] = log_cycle_id
        if active_cycle_id and active_cycle_id != "UNKNOWN" and log_cycle_id == active_cycle_id:
            same_cycle_index[did] = row

    return same_cycle_index, all_index, log_cycle_map


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
    Deterministic. log_entry must be a same-cycle entry or None.
    When log_entry is None for an ENTER_LONG intent, returns a placeholder status
    (caller will override to CROSS_CYCLE_EVIDENCE or NO_SAME_CYCLE_EXECUTION).
    """
    if not execution_allowed:
        return (STATUS_SKIPPED_BLOCKED, 0.0,
                str(block_reason or "BLOCKED"), False, False, False)

    if effective_action != "ENTER_LONG":
        return (STATUS_SKIPPED_NO_ACTION, 0.0, effective_action, False, False, False)

    if requested_notional <= 0.0:
        return (STATUS_NOT_REQUESTED, 0.0, "ZERO_REQUESTED_NOTIONAL", False, False, False)

    if log_entry is None:
        # Placeholder — caller will override based on cycle classification
        return ("_NEEDS_CYCLE_CLASSIFICATION", 0.0, None, False, False, False)

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

    return ("_NEEDS_CYCLE_CLASSIFICATION", 0.0, None, False, False, False)


def main():
    ts = utc_now_ts()

    exec_summary      = load_json(EXECUTION_SUMMARY_PATH, {}) or {}
    log_rows          = load_jsonl(EXECUTION_LOG_PATH)

    # Active cycle_id: authoritative source is execution_summary (added in AC50)
    active_cycle_id = str(exec_summary.get("cycle_id") or "UNKNOWN").strip() or "UNKNOWN"
    markets_data    = exec_summary.get("markets") or {}

    # Build cycle-scoped log indices
    same_cycle_index, all_index, log_cycle_map = build_log_indices(log_rows, active_cycle_id)

    # Log-level cycle stats
    same_cycle_log_count = len(same_cycle_index)
    cross_cycle_log_count = sum(
        1 for did, cid in log_cycle_map.items()
        if cid != "UNKNOWN" and cid != active_cycle_id
    )
    unknown_cycle_log_count = sum(1 for cid in log_cycle_map.values() if cid == "UNKNOWN")

    rows = []
    total_requested = 0.0
    total_granted   = 0.0
    status_counts: dict = {}

    # Row-level cycle counts (by reconciliation row, not log row)
    row_same_cycle_count    = 0
    row_cross_cycle_count   = 0
    row_unknown_cycle_count = 0

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

            # intent_cycle_id: from intent file; fallback to active_cycle_id
            intent_cycle_id = str(intent.get("cycle_id") or active_cycle_id or "UNKNOWN").strip()

            # decision_id: from intent file; construct as fallback
            decision_id = str(intent.get("decision_id") or "").strip()
            if not decision_id:
                eff = str(sr.get("effective_action") or sr.get("action") or "NO_ACTION")
                decision_id = f"{position_key}_{active_cycle_id}_{eff}"

            # Allocation fields — intent takes priority over strategy_results
            def _get(key, default=None):
                return intent.get(key) if intent.get(key) is not None else sr.get(key, default)

            execution_allowed  = bool(_get("execution_allowed", _get("allowed", False)))
            effective_action   = str(_get("effective_action") or _get("action") or "NO_ACTION")
            requested_notional = to_float(_get("requested_notional_eur", 0.0))
            allocation_pct     = to_float(_get("allocation_pct", 0.0))
            block_reason       = _get("block_reason") or _get("reason") or _get("effective_reason")

            # === AC50.1: cycle-scoped log lookup ===
            same_cycle_entry = same_cycle_index.get(decision_id)
            any_entry        = all_index.get(decision_id)

            if same_cycle_entry is not None:
                cycle_match_status = CYCLE_MATCH_SAME
                exec_cycle_id      = active_cycle_id
                log_entry_for_status = same_cycle_entry
            elif any_entry is not None:
                # Found in log but from a different cycle
                raw_log_cycle = log_cycle_map.get(decision_id, "UNKNOWN")
                cycle_match_status   = CYCLE_MATCH_UNKNOWN if raw_log_cycle == "UNKNOWN" else CYCLE_MATCH_CROSS
                exec_cycle_id        = raw_log_cycle
                log_entry_for_status = None  # Do NOT use cross/unknown-cycle evidence
            else:
                cycle_match_status   = CYCLE_MATCH_UNKNOWN
                exec_cycle_id        = None
                log_entry_for_status = None

            status, granted_notional, skip_reason, already_in_pos, dust_filtered, budget_limited = (
                determine_status(
                    execution_allowed, effective_action,
                    requested_notional, block_reason, log_entry_for_status
                )
            )

            # Resolve placeholder status based on cycle classification
            if status == "_NEEDS_CYCLE_CLASSIFICATION":
                if cycle_match_status == CYCLE_MATCH_CROSS:
                    status = STATUS_CROSS_CYCLE_EVIDENCE
                else:
                    status = STATUS_NO_SAME_CYCLE_EXECUTION
                granted_notional = 0.0

            # Intent-side-only statuses: always SAME_CYCLE (no execution log needed)
            if status in _INTENT_SIDE_STATUSES:
                cycle_match_status = CYCLE_MATCH_SAME
                exec_cycle_id      = None

            # Row-level cycle counters
            if cycle_match_status == CYCLE_MATCH_SAME:
                row_same_cycle_count += 1
            elif cycle_match_status == CYCLE_MATCH_CROSS:
                row_cross_cycle_count += 1
            else:
                row_unknown_cycle_count += 1

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
                "cycle_id": active_cycle_id,
                "decision_id": decision_id,
                # Cycle scoping (AC50.1)
                "intent_cycle_id": intent_cycle_id,
                "execution_cycle_id": exec_cycle_id,
                "cycle_match_status": cycle_match_status,
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
                "executed_action": log_entry_for_status.get("action") if log_entry_for_status else None,
                "executed": status == STATUS_FULLY_EXECUTED,
                "execution_skip_reason": skip_reason,
                "execution_log_found": log_entry_for_status is not None,
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
    cross_cycle    = status_counts.get(STATUS_CROSS_CYCLE_EVIDENCE, 0)
    no_same_cycle  = status_counts.get(STATUS_NO_SAME_CYCLE_EXECUTION, 0)

    out = {
        "component": "allocation_execution_reconciliation_lite",
        "version": "reconciliation_v2",
        "ts_utc": ts,
        # Active cycle
        "active_cycle_id": active_cycle_id,
        # Row-level summary
        "rows_total": len(rows),
        "fully_executed_count": fully_executed,
        "partially_granted_count": partial,
        "skipped_count": skipped,
        "cross_cycle_evidence_count": cross_cycle,
        "no_same_cycle_execution_count": no_same_cycle,
        "status_counts": status_counts,
        # Cycle-scoped totals (granted only from SAME_CYCLE matches)
        "total_requested_eur": total_requested,
        "total_granted_eur": total_granted,
        "total_request_minus_granted_eur": round(total_requested - total_granted, 2),
        # Row-level cycle classification counts
        "same_cycle_rows_count": row_same_cycle_count,
        "cross_cycle_rows_count": row_cross_cycle_count,
        "unknown_cycle_rows_count": row_unknown_cycle_count,
        # Log-level cycle classification counts
        "source_log_rows": len(log_rows),
        "source_log_indexed": len(all_index),
        "same_cycle_log_count": same_cycle_log_count,
        "cross_cycle_log_count": cross_cycle_log_count,
        "unknown_cycle_log_count": unknown_cycle_log_count,
        "rows": rows,
    }

    write_json(OUT_PATH, out)
    write_tsv(OUT_TSV_PATH, TSV_HEADERS, rows)

    # Print compact summary (no rows)
    print(json.dumps({k: v for k, v in out.items() if k != "rows"}, indent=2))


if __name__ == "__main__":
    main()
