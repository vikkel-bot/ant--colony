"""
AC-136: Queen Research Candidate Intake (Read-Only)

Loads and validates the AC-135 queen candidate decision snapshot as an
advisory input signal for the Queen layer.

Design principles:
  - Strictly passive and read-only: no allocation, execution, or pipeline changes.
  - research_only=True is always set on every output.
  - Fail-closed: any validation error -> CANDIDATE_INVALID -> passive hold.
  - Version mismatch -> CANDIDATE_INVALID.
  - Stale snapshot (> max_age_hours) -> CANDIDATE_HOLD.
  - Pure core function (consume_candidate) -- no I/O, no side effects.
  - Optional CLI for file-based observability.

Input:  data/research/queen_candidate_decision_snapshot.json (AC-135 output)
Output: in-memory intake result dict (no file writes from this module)

Intake statuses:
  CANDIDATE_ACTIVE  -- snapshot valid, fresh, chosen_timeframe present
  CANDIDATE_HOLD    -- snapshot valid but stale, or no chosen_timeframe
  CANDIDATE_INVALID -- snapshot missing, malformed, or schema failure

Usage (importable):
    from ant_colony.queen_research_candidate_intake_lite import (
        load_candidate_snapshot, consume_candidate, load_and_consume,
    )
    result = load_and_consume()

Usage (CLI):
    python ant_colony/queen_research_candidate_intake_lite.py
    python ant_colony/queen_research_candidate_intake_lite.py \\
        --snapshot data/research/queen_candidate_decision_snapshot.json
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_MODULE_DIR = Path(__file__).resolve().parent
_REPO_ROOT  = _MODULE_DIR.parent

DEFAULT_SNAPSHOT_PATH = _REPO_ROOT / "data" / "research" / \
                        "queen_candidate_decision_snapshot.json"

VERSION = "queen_research_candidate_intake_v1"
EXPECTED_SNAPSHOT_VERSION = "queen_candidate_decision_snapshot_v1"

# Default freshness window: 24 hours
DEFAULT_MAX_AGE_HOURS: int = 24

# Required top-level keys from the AC-135 snapshot schema
_REQUIRED_KEYS: tuple = (
    "version", "ts_utc", "market", "timeframes",
    "candidate_decision", "decision_context", "rationale_summary", "flags",
)

# Required keys inside candidate_decision
_REQUIRED_CANDIDATE_KEYS: tuple = (
    "chosen_timeframe", "chosen_strategy", "chosen_regime", "chosen_allocation_weight",
)

# Intake status values
CANDIDATE_ACTIVE  = "CANDIDATE_ACTIVE"
CANDIDATE_HOLD    = "CANDIDATE_HOLD"
CANDIDATE_INVALID = "CANDIDATE_INVALID"

# Machine-stable reason codes
REASON_CODES: dict = {
    "ACTIVE_OK":       "CANDIDATE_INTAKE_OK",
    "HOLD_STALE":      "CANDIDATE_HOLD_STALE",
    "HOLD_NO_CHOICE":  "CANDIDATE_HOLD_NO_TIMEFRAME",
    "INVALID_INPUT":   "CANDIDATE_INVALID_INPUT",
    "INVALID_MISSING": "CANDIDATE_INVALID_MISSING_FIELD",
    "INVALID_VERSION": "CANDIDATE_INVALID_VERSION",
    "INVALID_SCHEMA":  "CANDIDATE_INVALID_SCHEMA",
    "INVALID_TS":      "CANDIDATE_INVALID_TIMESTAMP",
}


# ---------------------------------------------------------------------------
# Loader (raises on error — caller decides how to handle)
# ---------------------------------------------------------------------------

def load_candidate_snapshot(path: Path) -> dict:
    """
    Load AC-135 snapshot from disk.

    Raises:
        FileNotFoundError: if path does not exist.
        ValueError: if file is not valid JSON or not a JSON object.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"[AC-136] Candidate snapshot not found: {path}\n"
            "Run AC-135 first to generate the queen candidate decision snapshot."
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"[AC-136] Candidate snapshot is not valid JSON: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ValueError("[AC-136] Candidate snapshot must be a JSON object.")
    return data


# ---------------------------------------------------------------------------
# Core intake function (pure, no I/O)
# ---------------------------------------------------------------------------

def consume_candidate(
    snapshot: object,
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    _now_utc: Optional[datetime] = None,
) -> dict:
    """
    Validate and consume an AC-135 candidate snapshot dict.

    Returns an intake result dict. research_only=True is always set.
    Fail-closed: any validation problem produces CANDIDATE_INVALID.

    Args:
        snapshot:      dict from load_candidate_snapshot() (or raw dict for testing).
        max_age_hours: freshness window; snapshots older than this become HOLD.
        _now_utc:      injectable clock for testing (default: datetime.now(utc)).
    """
    # 1. Must be a non-None dict
    if not isinstance(snapshot, dict):
        return _invalid("INVALID_INPUT", "snapshot is not a dict", snapshot)

    # 2. All required top-level keys present
    for key in _REQUIRED_KEYS:
        if key not in snapshot:
            return _invalid("INVALID_MISSING", f"missing required key: {key}", snapshot)

    # 3. Version must match
    got_version = snapshot["version"]
    if got_version != EXPECTED_SNAPSHOT_VERSION:
        return _invalid(
            "INVALID_VERSION",
            f"version mismatch: expected={EXPECTED_SNAPSHOT_VERSION} got={got_version}",
            snapshot,
        )

    # 4. ts_utc must be parseable
    ts_str = snapshot["ts_utc"]
    try:
        ts_dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return _invalid("INVALID_TS", f"ts_utc unparseable: {ts_str!r}", snapshot)

    # 5. candidate_decision must be a dict with required keys
    cd = snapshot.get("candidate_decision")
    if not isinstance(cd, dict):
        return _invalid("INVALID_SCHEMA", "candidate_decision is not a dict", snapshot)
    for key in _REQUIRED_CANDIDATE_KEYS:
        if key not in cd:
            return _invalid("INVALID_SCHEMA", f"candidate_decision missing key: {key}", snapshot)

    # 6. Freshness check — stale → HOLD (not INVALID; snapshot itself is well-formed)
    now = _now_utc or datetime.now(timezone.utc)
    age = now - ts_dt
    if age > timedelta(hours=max_age_hours):
        age_h = age.total_seconds() / 3600
        return _hold(
            "HOLD_STALE",
            f"snapshot stale: age={age_h:.1f}h max={max_age_hours}h",
            snapshot,
        )

    # 7. No chosen timeframe → HOLD (valid snapshot, no actionable candidate)
    chosen_tf = cd.get("chosen_timeframe")
    if chosen_tf is None:
        return _hold("HOLD_NO_CHOICE", "chosen_timeframe is None", snapshot)

    # All checks passed — ACTIVE
    ctx = snapshot.get("decision_context") or {}
    return {
        "intake_status":            CANDIDATE_ACTIVE,
        "intake_valid":             True,
        "intake_reason":            (
            f"CANDIDATE_ACTIVE|tf={chosen_tf}"
            f"|strategy={cd.get('chosen_strategy')}"
            f"|regime={cd.get('chosen_regime')}"
            f"|weight={cd.get('chosen_allocation_weight')}"
        ),
        "intake_reason_code":       REASON_CODES["ACTIVE_OK"],
        "chosen_timeframe":         chosen_tf,
        "chosen_strategy":          cd.get("chosen_strategy"),
        "chosen_regime":            cd.get("chosen_regime", "unknown"),
        "chosen_allocation_weight": cd.get("chosen_allocation_weight", 0.0),
        "dominant_strategy":        ctx.get("dominant_strategy"),
        "dominant_regime":          ctx.get("dominant_regime"),
        "weights_sum":              ctx.get("weights_sum", 0.0),
        "snapshot_ts_utc":          ts_str,
        "snapshot_market":          snapshot.get("market", ""),
        "research_only":            True,
    }


# ---------------------------------------------------------------------------
# Convenience end-to-end function
# ---------------------------------------------------------------------------

def load_and_consume(
    path: Path = DEFAULT_SNAPSHOT_PATH,
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    _now_utc: Optional[datetime] = None,
) -> dict:
    """
    Load AC-135 snapshot from disk and run consume_candidate().

    On FileNotFoundError or ValueError: returns CANDIDATE_INVALID result
    (does not re-raise — caller gets a structured result in all cases).
    """
    try:
        snapshot = load_candidate_snapshot(path)
    except FileNotFoundError as exc:
        return _invalid("INVALID_INPUT", f"snapshot file not found: {exc}", None)
    except ValueError as exc:
        return _invalid("INVALID_INPUT", f"snapshot load error: {exc}", None)
    return consume_candidate(snapshot, max_age_hours=max_age_hours, _now_utc=_now_utc)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _invalid(code_key: str, reason: str, snapshot: object) -> dict:
    """Fail-closed CANDIDATE_INVALID result. research_only=True always."""
    _cd_raw  = snapshot.get("candidate_decision")  if isinstance(snapshot, dict) else None
    _ctx_raw = snapshot.get("decision_context")    if isinstance(snapshot, dict) else None
    cd  = _cd_raw  if isinstance(_cd_raw,  dict) else {}
    ctx = _ctx_raw if isinstance(_ctx_raw, dict) else {}
    return {
        "intake_status":            CANDIDATE_INVALID,
        "intake_valid":             False,
        "intake_reason":            reason,
        "intake_reason_code":       REASON_CODES.get(code_key, "CANDIDATE_INVALID_UNKNOWN"),
        "chosen_timeframe":         cd.get("chosen_timeframe"),
        "chosen_strategy":          cd.get("chosen_strategy"),
        "chosen_regime":            cd.get("chosen_regime"),
        "chosen_allocation_weight": cd.get("chosen_allocation_weight"),
        "dominant_strategy":        ctx.get("dominant_strategy"),
        "dominant_regime":          ctx.get("dominant_regime"),
        "weights_sum":              ctx.get("weights_sum"),
        "snapshot_ts_utc":          (snapshot.get("ts_utc") if isinstance(snapshot, dict) else None),
        "snapshot_market":          (snapshot.get("market", "") if isinstance(snapshot, dict) else ""),
        "research_only":            True,
    }


def _hold(code_key: str, reason: str, snapshot: dict) -> dict:
    """CANDIDATE_HOLD result (valid snapshot, no actionable candidate). research_only=True always."""
    cd  = snapshot.get("candidate_decision") or {}
    ctx = snapshot.get("decision_context") or {}
    return {
        "intake_status":            CANDIDATE_HOLD,
        "intake_valid":             True,
        "intake_reason":            reason,
        "intake_reason_code":       REASON_CODES.get(code_key, "CANDIDATE_HOLD_UNKNOWN"),
        "chosen_timeframe":         cd.get("chosen_timeframe"),
        "chosen_strategy":          cd.get("chosen_strategy"),
        "chosen_regime":            cd.get("chosen_regime"),
        "chosen_allocation_weight": cd.get("chosen_allocation_weight"),
        "dominant_strategy":        ctx.get("dominant_strategy"),
        "dominant_regime":          ctx.get("dominant_regime"),
        "weights_sum":              ctx.get("weights_sum", 0.0),
        "snapshot_ts_utc":          snapshot.get("ts_utc"),
        "snapshot_market":          snapshot.get("market", ""),
        "research_only":            True,
    }


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# CLI (observability only — no file writes)
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-136: Queen research candidate intake (read-only, observability)."
    )
    p.add_argument(
        "--snapshot",
        dest="snapshot",
        default=str(DEFAULT_SNAPSHOT_PATH),
        help="Path to AC-135 queen_candidate_decision_snapshot.json",
    )
    p.add_argument(
        "--max-age-hours",
        dest="max_age_hours",
        type=int,
        default=DEFAULT_MAX_AGE_HOURS,
        help=f"Max snapshot age in hours before HOLD (default: {DEFAULT_MAX_AGE_HOURS})",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = load_and_consume(
        path=Path(args.snapshot),
        max_age_hours=args.max_age_hours,
    )
    out = {
        "component": "queen_research_candidate_intake_lite",
        "version":   VERSION,
        "ts_utc":    _now_utc_iso(),
        "source":    args.snapshot,
        "intake":    result,
    }
    print(json.dumps(out, indent=2))
    return 0 if result["intake_status"] != CANDIDATE_INVALID else 1


if __name__ == "__main__":
    raise SystemExit(main())
