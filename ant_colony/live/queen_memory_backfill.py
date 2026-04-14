"""
AC-186 v2: Queen Memory Backfill (Corrected Architecture)

Converts historical trade artifacts into Queen memory format without corrupting
learning integrity. Preserves strict lane separation, avoids fake causal data,
and marks every record with data_source="historical_backfill" so the Queen
pipeline can distinguish reconstructed data from real execution.

One sentence: Maps historical paper trade records to Queen memory schema with
full data-source tagging and sentinel-only causal fields — non-binding, no
execution/allocation/gate changes.

IMPORTANT — this module is strictly non-binding:
  - no execution changes
  - no allocation changes
  - no live gate changes
  - output is written to a SEPARATE lane (default "historical_backfill")
  - data_source="historical_backfill" on every record (MANDATORY)

Source artifacts (read-only, in priority order):
    {source_dir}/paper_trade_reconstruction.json   — preferred (most complete)
    {source_dir}/paper_trade_feedback.json         — fallback
    {source_dir}/*.json                            — last resort: any JSON file
                                                     containing a "rows" or
                                                     "trades" list

Output:
    {base_output_dir}/{lane}/memory/{safe_trade_id}.json

Schema — every field:

  Identity / timing:
    trade_id              ← source trade_id
    market                ← source market
    strategy_key          ← source strategy ("UNKNOWN" if absent)
    entry_ts_utc          ← source entry_ts  (normalised to Z suffix)
    exit_ts_utc           ← source exit_ts   (normalised to Z suffix)
    hold_duration_minutes ← computed from entry/exit timestamps
    exit_reason           ← mapped to valid enum; else "UNKNOWN"

  Outcome:
    realized_pnl_eur  ← source realized_pnl (rounded 8dp)
    win_loss_label    ← WIN / LOSS / FLAT derived from pnl

  Execution quality — sentinels only (NO fake data):
    slippage_vs_expected_eur ← 0.0  (unavailable in paper source)
    entry_latency_ms         ← 0    (unavailable in paper source)
    execution_quality_flag   ← "OK" (paper has no anomaly concept)
    anomaly_flag             ← False

  Causal context — sentinels only:
    market_regime_at_entry ← "UNKNOWN" (foundation for future grouping)
    volatility_at_entry    ← "UNKNOWN"
    signal_strength        ← -1.0
    signal_key             ← "UNKNOWN"

  Metadata:
    lane              ← caller-supplied (default "historical_backfill")
    memory_version    ← "1"
    record_type       ← "closed_trade_memory"
    feedback_ts_utc   ← exit_ts_utc (best available proxy)
    memory_ts_utc     ← UTC timestamp at conversion time
    data_source       ← "historical_backfill"  ← NEW MANDATORY FIELD
    queen_action_required ← True (regime/volatility UNKNOWN)

Only CLOSED trades are converted; OPEN trades are counted and skipped.
Fail-closed: unreadable source → ok=False with clear reason. Never raises.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_LANE = "historical_backfill"
DATA_SOURCE  = "historical_backfill"

_VALID_EXIT_REASONS = frozenset({"SL", "TP", "SIGNAL", "OPERATOR_KILL", "MANUAL", "UNKNOWN"})
_SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_\-]")

# Candidate filenames searched inside source_dir, in priority order
_SOURCE_CANDIDATES = [
    "paper_trade_reconstruction.json",
    "paper_trade_feedback.json",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalise_ts(ts: Any) -> str | None:
    """
    Normalise a timestamp string to YYYY-MM-DDTHH:MM:SSZ.

    Accepts ISO 8601 strings with or without timezone suffix. Returns None if
    the value cannot be parsed. Never raises.
    """
    if not isinstance(ts, str) or not ts.strip():
        return None
    s = ts.strip()
    if s.endswith("Z") and len(s) == 20:
        return s
    for suffix in ("+00:00", "+0000"):
        if s.endswith(suffix):
            s = s[: -len(suffix)] + "Z"
            break
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            dt = datetime.strptime(s.rstrip("Z") + "Z", fmt.rstrip("Z") + "Z")
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:  # noqa: BLE001
        return None


def _hold_duration_minutes(entry_ts: str | None, exit_ts: str | None) -> float:
    """Compute hold duration in minutes; returns 0.0 if either timestamp is absent. Never raises."""
    if not entry_ts or not exit_ts:
        return 0.0
    try:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        t0  = datetime.strptime(entry_ts, fmt)
        t1  = datetime.strptime(exit_ts,  fmt)
        return max(0.0, round((t1 - t0).total_seconds() / 60.0, 4))
    except Exception:  # noqa: BLE001
        return 0.0


def _safe_filename(name: str) -> str:
    """Replace characters unsafe for filenames with underscores."""
    return _SAFE_NAME_RE.sub("_", name)


def _map_exit_reason(raw: Any) -> str:
    """Map a paper exit_reason to a valid Queen memory exit_reason enum value."""
    if not isinstance(raw, str):
        return "UNKNOWN"
    upper = raw.strip().upper()
    return upper if upper in _VALID_EXIT_REASONS else "UNKNOWN"


def _extract_trade_list(data: Any) -> list[dict[str, Any]] | None:
    """
    Extract a list of trade/row dicts from a parsed JSON value.

    Accepts: direct list, dict with "rows", "trades", or "closed_trades" key.
    Returns None if no recognisable list is found.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("rows", "trades", "closed_trades"):
            candidate = data.get(key)
            if isinstance(candidate, list):
                return candidate
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_source_records(path: str | Path) -> list[dict[str, Any]] | None:
    """
    Read historical trade records from a file or directory.

    If path is a file, reads it directly. If path is a directory, searches
    for candidate filenames (paper_trade_reconstruction.json first, then
    paper_trade_feedback.json, then any *.json file containing a trade list).
    Returns a list of record dicts, or None if nothing readable is found.
    Never raises.
    """
    try:
        p = Path(path)
        if not p.exists():
            return None

        # File path supplied directly
        if p.is_file():
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                return _extract_trade_list(data)
            except Exception:  # noqa: BLE001
                return None

        # Directory — try candidates in priority order
        for candidate in _SOURCE_CANDIDATES:
            fp = p / candidate
            if fp.exists():
                try:
                    data = json.loads(fp.read_text(encoding="utf-8"))
                    records = _extract_trade_list(data)
                    if records is not None:
                        return records
                except Exception:  # noqa: BLE001
                    pass

        # Last resort: any *.json file in the directory
        for fp in sorted(p.glob("*.json")):
            try:
                data = json.loads(fp.read_text(encoding="utf-8"))
                records = _extract_trade_list(data)
                if records is not None:
                    return records
            except Exception:  # noqa: BLE001
                pass

        return None
    except Exception:  # noqa: BLE001
        return None


def map_to_memory_schema(
    record: dict[str, Any],
    lane: str = DEFAULT_LANE,
    now_utc: str | None = None,
) -> dict[str, Any] | None:
    """
    Convert one closed historical trade record to Queen memory schema.

    Returns the memory entry dict (with data_source="historical_backfill"), or
    None if the record is OPEN or missing minimum required fields
    (trade_id, market, entry_ts, exit_ts). Never raises.
    """
    try:
        state = str(record.get("state") or record.get("holding_state") or "").upper()
        if state != "CLOSED":
            return None

        trade_id = str(record.get("trade_id") or "").strip()
        market   = str(record.get("market")   or "").strip()
        if not trade_id or not market:
            return None

        entry_ts = _normalise_ts(record.get("entry_ts"))
        exit_ts  = _normalise_ts(record.get("exit_ts"))
        if not entry_ts or not exit_ts:
            return None

        strategy_key = str(record.get("strategy") or record.get("strategy_key") or "UNKNOWN").strip() or "UNKNOWN"

        try:
            pnl = float(record.get("realized_pnl") or record.get("realized_pnl_eur") or 0.0)
        except (TypeError, ValueError):
            pnl = 0.0

        win_loss_label = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "FLAT")
        hold_dur       = _hold_duration_minutes(entry_ts, exit_ts)
        exit_reason    = _map_exit_reason(record.get("exit_reason"))
        memory_ts      = now_utc or _now_utc()

        return {
            "memory_version":           "1",
            "record_type":              "closed_trade_memory",
            "lane":                     lane,
            "data_source":              DATA_SOURCE,          # MANDATORY new field
            "market":                   market,
            "strategy_key":             strategy_key,
            "trade_id":                 trade_id,
            "entry_ts_utc":             entry_ts,
            "exit_ts_utc":              exit_ts,
            "hold_duration_minutes":    hold_dur,
            "realized_pnl_eur":         round(pnl, 8),
            "win_loss_label":           win_loss_label,
            "exit_reason":              exit_reason,
            "anomaly_flag":             False,
            "execution_quality_flag":   "OK",
            # Causal sentinels — NO fake data; Queen uses data_source to distinguish
            "market_regime_at_entry":   "UNKNOWN",
            "volatility_at_entry":      "UNKNOWN",
            "signal_strength":          -1.0,
            "signal_key":               "UNKNOWN",
            "slippage_vs_expected_eur": 0.0,
            "entry_latency_ms":         0,
            # Metadata
            "feedback_ts_utc":          exit_ts,
            "memory_ts_utc":            memory_ts,
            "queen_action_required":    True,
        }
    except Exception:  # noqa: BLE001
        return None


def write_memory_record(record: dict[str, Any], output_dir: str | Path) -> None:
    """
    Write one memory record to output_dir/{safe_trade_id}.json atomically.

    Uses a .tmp sibling + os.replace() for crash safety. Never raises.
    """
    try:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = _safe_filename(str(record.get("trade_id") or "unknown")) + ".json"
        path     = out_dir / filename
        tmp      = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
    except Exception:  # noqa: BLE001
        pass


def run(
    source_dir: str | Path,
    base_output_dir: str | Path,
    lane: str = DEFAULT_LANE,
) -> dict[str, Any]:
    """
    Orchestrate the full historical memory backfill for one source directory.

    Reads trade records from source_dir, converts each CLOSED trade to Queen
    memory schema (with data_source="historical_backfill"), and writes files
    to {base_output_dir}/{lane}/memory/.

    Parameters:
        source_dir      — directory (or file) containing source artifacts
        base_output_dir — root output directory (e.g. C:\\Trading\\ANT_LIVE)
        lane            — lane name for the backfill (default "historical_backfill")

    Returns:
        {
            "ok": bool,
            "reason": str,
            "lane": str,
            "output_dir": str,
            "total_source_records": int,
            "converted": int,
            "skipped_open": int,
            "skipped_invalid": int,
        }

    Never raises.
    """
    mem_dir = Path(base_output_dir) / lane / "memory"

    try:
        records = read_source_records(source_dir)
        if records is None:
            return {
                "ok":                   False,
                "reason":               f"source not found or unreadable: {source_dir}",
                "lane":                 lane,
                "output_dir":           str(mem_dir),
                "total_source_records": 0,
                "converted":            0,
                "skipped_open":         0,
                "skipped_invalid":      0,
            }

        now      = _now_utc()
        converted    = 0
        skipped_open = 0
        skipped_inv  = 0

        for rec in records:
            if not isinstance(rec, dict):
                skipped_inv += 1
                continue

            state = str(rec.get("state") or rec.get("holding_state") or "").upper()
            if state != "CLOSED":
                skipped_open += 1
                continue

            entry = map_to_memory_schema(rec, lane=lane, now_utc=now)
            if entry is None:
                skipped_inv += 1
                continue

            write_memory_record(entry, mem_dir)
            converted += 1

        return {
            "ok":                   True,
            "reason":               "BACKFILL_COMPLETE",
            "lane":                 lane,
            "output_dir":           str(mem_dir),
            "total_source_records": len(records),
            "converted":            converted,
            "skipped_open":         skipped_open,
            "skipped_invalid":      skipped_inv,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok":                   False,
            "reason":               f"unexpected backfill error: {exc}",
            "lane":                 lane,
            "output_dir":           str(mem_dir),
            "total_source_records": 0,
            "converted":            0,
            "skipped_open":         0,
            "skipped_invalid":      0,
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    if len(sys.argv) < 3:
        print(
            "Usage: python -m ant_colony.live.queen_memory_backfill "
            "<source_dir> <base_output_dir> [lane]"
        )
        sys.exit(1)

    source  = sys.argv[1]
    out_dir = sys.argv[2]
    lane    = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_LANE

    result = run(source, out_dir, lane)
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
