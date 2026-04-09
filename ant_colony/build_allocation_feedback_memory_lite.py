"""
AC63 Part A: Persistent Feedback Memory
Pure observability + state-persistence layer. No execution, no orders.

Sits after AC-61 (Allocation Feedback Integration) and AC-60 (Outcome Attribution).
Maintains a conservative rolling-window memory of READY outcome records per
strategy_key across multiple cycles.

Central question:
  What does the cumulative history of paper rebalance outcomes tell us about
  each strategy_key — across cycles, not just the most recent one?

Memory model — rolling window with cooldown persistence:
  - Rolling window of last WINDOW_SIZE READY outcome labels per strategy_key.
  - Window is stored as a list of {audit_id, cycle_id, outcome_label} entries.
  - New records are appended; oldest are dropped when window exceeds WINDOW_SIZE.
  - Idempotent: records already in window (by audit_id) are never double-counted.
  - Cooldown persistence: cooldown_cycles_remaining counter decrements each cycle
    until it reaches 0; reset to COOLDOWN_PERSIST_CYCLES when AC-61 signals caution.

This layer does NOT:
  - influence actual modifier execution (AC-62 still uses AC-61 cycle modifier only)
  - learn autonomously or optimise for returns
  - create positive bias on thin evidence

This layer DOES:
  - maintain rolling window state per strategy_key
  - persist cooldown across cycles
  - track lifetime totals alongside window
  - write an auditable memory artefact for AC-63 observability

Reads:
  paper_rebalance_outcome_attribution.json   — per-record outcome labels (AC60)
  allocation_feedback_integration.json       — per-strategy cooldown signal (AC61)
  allocation_feedback_memory.json            — existing memory state (AC63, previous run)

Writes:
  allocation_feedback_memory.json            — updated memory state

Usage: python ant_colony/build_allocation_feedback_memory_lite.py
"""
import importlib.util as _ilu
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

ATTRIBUTION_PATH   = OUT_DIR / "paper_rebalance_outcome_attribution.json"
FEEDBACK_PATH      = OUT_DIR / "allocation_feedback_integration.json"
MEMORY_PATH        = OUT_DIR / "allocation_feedback_memory.json"

VERSION = "feedback_memory_v1"

# ---------------------------------------------------------------------------
# AC-68: load rolling-window constants from canonical policy loader.
# Fail-closed: if loader unavailable, inline defaults (same values) are used.
# Not-policy values (MEMORY_MIN_CONFIDENCE, sparse threshold) remain hardcoded.
# ---------------------------------------------------------------------------
def _load_ac63_policy():
    try:
        _path = Path(__file__).parent / "policy" / "load_allocation_memory_policy_lite.py"
        _spec = _ilu.spec_from_file_location("_policy_loader", _path)
        _mod = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        _policy, _fb, _reason = _mod.load_policy()
        return _policy["groups"].get("memory_rolling_window", {}), _fb, _reason
    except Exception as _exc:
        return {}, True, f"LOADER_UNAVAILABLE:{_exc}"

_ac63_window, _POLICY_FALLBACK_USED, _POLICY_LOAD_REASON = _load_ac63_policy()

# Rolling window discipline — sourced from policy (fail-closed to inline defaults)
WINDOW_SIZE             = _ac63_window.get("window_size",             10)
FULL_MEMORY_AT          = _ac63_window.get("full_memory_at",          8)
COOLDOWN_PERSIST_CYCLES = _ac63_window.get("cooldown_cycles_default", 3)

# AC69: status-classification thresholds — sourced from policy (fail-closed to inline defaults)
MEMORY_MIN_CONFIDENCE  = _ac63_window.get("memory_min_confidence",   0.40)
_SPARSE_WINDOW_MIN     = _ac63_window.get("sparse_window_threshold",  3)

# AC-60 labels
_READY_OUTCOME_LABELS = {"HELPFUL", "NEUTRAL", "HARMFUL"}
_READY_EVAL_STATUS    = "READY"

# Memory status values
MEMORY_STATUS_ACTIVE       = "ACTIVE"
MEMORY_STATUS_SPARSE       = "SPARSE"
MEMORY_STATUS_BOOTSTRAP    = "BOOTSTRAP"
MEMORY_STATUS_INSUFFICIENT = "INSUFFICIENT"


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


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Memory record helpers (importable for tests)
# ---------------------------------------------------------------------------

def compute_memory_confidence(window_size: int) -> float:
    """Linearly scales from 0.0 (0 records) to 1.0 (FULL_MEMORY_AT records)."""
    if window_size <= 0:
        return 0.0
    return round(min(1.0, window_size / FULL_MEMORY_AT), 4)


def memory_status_for(window_size: int, memory_confidence: float) -> str:
    if window_size == 0:
        return MEMORY_STATUS_BOOTSTRAP
    if window_size < _SPARSE_WINDOW_MIN:
        return MEMORY_STATUS_SPARSE
    if memory_confidence < MEMORY_MIN_CONFIDENCE:
        return MEMORY_STATUS_INSUFFICIENT
    return MEMORY_STATUS_ACTIVE


def empty_memory_record(strategy_key: str, market: str = "") -> dict:
    """Return a fresh, neutral memory record (bootstrap state)."""
    return {
        "strategy_key":            strategy_key,
        "market":                  market,
        "memory_status":           MEMORY_STATUS_BOOTSTRAP,
        "ready_outcomes_total":    0,
        "helpful_total":           0,
        "neutral_total":           0,
        "harmful_total":           0,
        "rolling_window":          [],
        "rolling_net_signal":      0.0,
        "memory_confidence":       0.0,
        "cooldown_flag":           False,
        "cooldown_cycles_remaining": 0,
        "last_outcome_label":      None,
        "last_update_ts_utc":      None,
        "last_cycle_id":           None,
        "memory_reasons":          "BOOTSTRAP_NO_HISTORY",
    }


def update_memory_record(
    rec: dict,
    new_outcomes: list,
    cycle_cooldown: bool,
    cycle_id: str,
    ts: str,
) -> dict:
    """
    Update a single strategy_key memory record with new READY outcomes.

    rec:            existing memory dict (or fresh from empty_memory_record)
    new_outcomes:   list of {audit_id, outcome_label} from current cycle (READY only)
    cycle_cooldown: bool — whether AC-61 flagged cooldown for this strategy_key
    cycle_id:       current cycle identifier
    ts:             current UTC timestamp string
    """
    # Idempotency: skip already-seen audit_ids
    existing_ids = {e["audit_id"] for e in rec.get("rolling_window") or [] if "audit_id" in e}

    added = []
    for o in new_outcomes:
        aid = str(o.get("audit_id") or "")
        label = str(o.get("outcome_label") or "")
        if aid and label in _READY_OUTCOME_LABELS and aid not in existing_ids:
            added.append({
                "audit_id":      aid,
                "cycle_id":      cycle_id,
                "outcome_label": label,
            })

    # Append new records and trim to rolling window
    window = list(rec.get("rolling_window") or []) + added
    window = window[-WINDOW_SIZE:]

    # Recompute from window
    labels        = [e["outcome_label"] for e in window]
    n             = len(labels)
    helpful_win   = labels.count("HELPFUL")
    neutral_win   = labels.count("NEUTRAL")
    harmful_win   = labels.count("HARMFUL")

    net_signal         = round((helpful_win - harmful_win) / max(n, 1), 4) if n > 0 else 0.0
    memory_confidence  = compute_memory_confidence(n)
    mem_status         = memory_status_for(n, memory_confidence)

    # Cooldown persistence
    cooldown_remaining = int(rec.get("cooldown_cycles_remaining") or 0)
    if cycle_cooldown:
        cooldown_remaining = COOLDOWN_PERSIST_CYCLES
    elif cooldown_remaining > 0:
        cooldown_remaining -= 1
    cooldown_flag = cooldown_remaining > 0

    # Lifetime running totals (sum over all cycles ever processed)
    helpful_total = int(rec.get("helpful_total") or 0) + sum(
        1 for o in added if o["outcome_label"] == "HELPFUL"
    )
    neutral_total = int(rec.get("neutral_total") or 0) + sum(
        1 for o in added if o["outcome_label"] == "NEUTRAL"
    )
    harmful_total = int(rec.get("harmful_total") or 0) + sum(
        1 for o in added if o["outcome_label"] == "HARMFUL"
    )
    ready_total   = int(rec.get("ready_outcomes_total") or 0) + len(added)

    # Reason tags
    reasons = [
        f"WINDOW_N={n}",
        f"NET_SIGNAL={net_signal}",
        f"CONF={memory_confidence}",
        f"STATUS={mem_status}",
    ]
    if added:
        reasons.append(f"NEW_ADDED={len(added)}")
    if cycle_cooldown:
        reasons.append("COOLDOWN_SET_FROM_CYCLE")
    elif cooldown_flag:
        reasons.append(f"COOLDOWN_PERSIST_REMAINING={cooldown_remaining}")

    last_label = labels[-1] if labels else None

    return {
        "strategy_key":               rec.get("strategy_key", ""),
        "market":                     rec.get("market", ""),
        "memory_status":              mem_status,
        "ready_outcomes_total":       ready_total,
        "helpful_total":              helpful_total,
        "neutral_total":              neutral_total,
        "harmful_total":              harmful_total,
        "rolling_window":             window,
        "rolling_net_signal":         net_signal,
        "memory_confidence":          memory_confidence,
        "cooldown_flag":              cooldown_flag,
        "cooldown_cycles_remaining":  cooldown_remaining,
        "last_outcome_label":         last_label,
        "last_update_ts_utc":         ts,
        "last_cycle_id":              cycle_id,
        "memory_reasons":             "|".join(reasons),
    }


# ---------------------------------------------------------------------------
# Core build function (importable for tests)
# ---------------------------------------------------------------------------

def build_memory_state(
    attribution_records: list,
    feedback_records: list,
    existing_memory: dict,
    cycle_id: str,
    ts: str,
) -> dict:
    """
    Build updated memory state from current cycle's attribution + feedback records.

    attribution_records: list of AC-60 records (may include non-READY)
    feedback_records:    list of AC-61 records (per strategy_key)
    existing_memory:     dict keyed by strategy_key (from previous memory file)
    cycle_id:            current cycle identifier
    ts:                  current UTC timestamp

    Returns: dict keyed by strategy_key with updated memory records.
    """
    # Index AC-61 feedback by strategy_key for cooldown lookup
    feedback_index = {}
    for r in (feedback_records or []):
        sk = str(r.get("strategy_key") or "").strip()
        if sk:
            feedback_index[sk] = r

    # Group new READY attribution records by strategy_key
    grouped_new: dict[str, list] = {}
    for rec in (attribution_records or []):
        sk = str(rec.get("strategy_key") or "").strip()
        if not sk:
            pk = str(rec.get("position_key") or "")
            sk = pk.split("__", 1)[1] if "__" in pk else (pk or "UNKNOWN")

        eval_status   = str(rec.get("evaluation_status") or "")
        outcome_label = str(rec.get("outcome_label") or "")

        if eval_status == _READY_EVAL_STATUS and outcome_label in _READY_OUTCOME_LABELS:
            if sk not in grouped_new:
                grouped_new[sk] = []
            # Use audit_id or execution_id as unique identifier
            audit_id = str(
                rec.get("audit_id") or rec.get("execution_id") or
                f"{cycle_id}__{sk}__unknown"
            )
            grouped_new[sk].append({
                "audit_id":      audit_id,
                "outcome_label": outcome_label,
            })

    # Merge: update all known strategy_keys
    all_keys = (
        set(existing_memory.keys()) |
        set(grouped_new.keys()) |
        set(feedback_index.keys())
    )

    updated: dict[str, dict] = {}
    for sk in sorted(all_keys):
        base_rec     = dict(existing_memory.get(sk) or empty_memory_record(sk))
        new_outcomes = grouped_new.get(sk, [])
        fb_rec       = feedback_index.get(sk) or {}

        # Market: prefer AC-61, fallback to existing
        market = (
            str(fb_rec.get("market") or "").strip() or
            str(base_rec.get("market") or "").strip()
        )
        base_rec["market"] = market

        cycle_cooldown = bool(fb_rec.get("cooldown_flag", False))

        updated[sk] = update_memory_record(
            base_rec, new_outcomes, cycle_cooldown, cycle_id, ts
        )

    return updated


def load_existing_memory(path: Path) -> dict:
    """Load strategy_keys dict from existing memory file. Fail-closed → {}."""
    data = load_json(path, {})
    if not data or not isinstance(data, dict):
        return {}
    return data.get("strategy_keys") or {}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    # Load inputs
    attr_data   = load_json(ATTRIBUTION_PATH, {}) or {}
    fb_data     = load_json(FEEDBACK_PATH, {}) or {}
    cycle_id    = (
        attr_data.get("cycle_id") or
        fb_data.get("cycle_id") or
        "UNKNOWN"
    )

    attribution_records = attr_data.get("records") or []
    feedback_records    = fb_data.get("records") or []

    # Load existing memory
    existing_memory = load_existing_memory(MEMORY_PATH)

    # Build updated memory state
    updated = build_memory_state(
        attribution_records, feedback_records, existing_memory, cycle_id, ts
    )

    # Summary
    total              = len(updated)
    active_count       = sum(1 for r in updated.values() if r["memory_status"] == MEMORY_STATUS_ACTIVE)
    sparse_count       = sum(1 for r in updated.values() if r["memory_status"] == MEMORY_STATUS_SPARSE)
    bootstrap_count    = sum(1 for r in updated.values() if r["memory_status"] == MEMORY_STATUS_BOOTSTRAP)
    cooldown_count     = sum(1 for r in updated.values() if r["cooldown_flag"])
    helpful_total      = sum(r["helpful_total"] for r in updated.values())
    harmful_total      = sum(r["harmful_total"] for r in updated.values())
    avg_confidence     = (
        round(sum(r["memory_confidence"] for r in updated.values()) / total, 4)
        if total > 0 else 0.0
    )

    summary = {
        "strategy_keys_total":   total,
        "active_count":          active_count,
        "sparse_count":          sparse_count,
        "bootstrap_count":       bootstrap_count,
        "cooldown_count":        cooldown_count,
        "helpful_total_lifetime": helpful_total,
        "harmful_total_lifetime": harmful_total,
        "avg_memory_confidence": avg_confidence,
    }

    out = {
        "component":   "build_allocation_feedback_memory_lite",
        "version":     VERSION,
        "ts_utc":      ts,
        "cycle_id":    cycle_id,
        "paper_only":  True,
        "source_files": {
            "paper_rebalance_outcome_attribution": str(ATTRIBUTION_PATH),
            "allocation_feedback_integration":     str(FEEDBACK_PATH),
        },
        "memory_constants": {
            "window_size":             WINDOW_SIZE,
            "full_memory_at":          FULL_MEMORY_AT,
            "memory_min_confidence":   MEMORY_MIN_CONFIDENCE,
            "cooldown_persist_cycles": COOLDOWN_PERSIST_CYCLES,
        },
        "summary":       summary,
        "strategy_keys": updated,
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(MEMORY_PATH, out)
    except Exception as e:
        print(f"[WARN] Could not write memory output: {e}")

    print(json.dumps({k: v for k, v in out.items() if k != "strategy_keys"}, indent=2))
    for sk, r in sorted(updated.items()):
        print(
            f"  {sk:<24} status={r['memory_status']:<12}"
            f" conf={r['memory_confidence']:.2f}"
            f" net={r['rolling_net_signal']:+.4f}"
            f" cooldown={r['cooldown_flag']}"
            f" window={len(r['rolling_window'])}"
        )


if __name__ == "__main__":
    main()
