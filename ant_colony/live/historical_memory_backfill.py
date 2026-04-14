"""
AC-186: Historical Memory Backfill

Reads paper_trade_reconstruction.json (the existing paper trade source) and
converts every CLOSED trade into a Queen memory artifact with the same schema
that the live pipeline produces, so all downstream Queen modules (AC-179 to
AC-185) can run on historical data without any schema changes.

One sentence: Converts closed paper trade records into Queen memory entries so
the live Queen pipeline can learn from historical outcomes — no execution,
allocation, or gate changes.

IMPORTANT — this module is strictly non-binding:
  - no execution changes
  - no allocation changes
  - no live gate changes
  - no broker calls
  - output is written to a separate lane (historical_backfill) so it is never
    confused with true live memory

Source artifact (read-only):
    paper_trade_reconstruction.json  — produced by paper_trade_reconstruction_lite.py

Output:
    {base_output_dir}/{lane}/memory/{safe_trade_id}.json
    (default lane = "historical_backfill")

Per-record field mapping (paper → Queen memory):

  Direct mappings:
    trade_id            ← paper: trade_id
    market              ← paper: market
    strategy_key        ← paper: strategy ("UNKNOWN" if absent/empty)
    entry_ts_utc        ← paper: entry_ts   (normalised to Z suffix)
    exit_ts_utc         ← paper: exit_ts    (normalised to Z suffix)
    realized_pnl_eur    ← paper: realized_pnl
    exit_reason         ← paper: exit_reason (mapped to valid enum; else "UNKNOWN")
    hold_duration_minutes ← computed from entry_ts / exit_ts (0.0 if unavailable)

  Derived fields:
    win_loss_label      ← realized_pnl > 0 → "WIN"; < 0 → "LOSS"; else "FLAT"
    anomaly_flag        ← always False  (paper assumes clean execution)
    execution_quality_flag ← always "OK" (paper has no anomaly concept)
    queen_action_required  ← always True  (regime/volatility are UNKNOWN)

  Sentinels (genuinely unavailable in paper source):
    market_regime_at_entry ← "UNKNOWN"
    volatility_at_entry    ← "UNKNOWN"
    signal_strength        ← -1.0
    signal_key             ← "UNKNOWN"
    slippage_vs_expected_eur ← 0.0
    entry_latency_ms       ← 0

  Metadata:
    lane               ← caller-supplied (default "historical_backfill")
    memory_version     ← "1"
    record_type        ← "closed_trade_memory"
    feedback_ts_utc    ← exit_ts_utc (best available proxy)
    memory_ts_utc      ← UTC timestamp at conversion time

Only CLOSED trades are converted; OPEN trades are counted and skipped.
Fail-closed: unreadable source or unconvertible records are skipped (counted in
skipped_records); missing source file returns ok=False with clear reason.
Never raises.
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

_VALID_EXIT_REASONS = frozenset({"SL", "TP", "SIGNAL", "OPERATOR_KILL", "MANUAL", "UNKNOWN"})
_SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_\-]")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalise_ts(ts: Any) -> str | None:
    """
    Normalise a timestamp string to YYYY-MM-DDTHH:MM:SSZ.

    Accepts ISO 8601 strings with or without timezone info. Returns None if
    the value cannot be parsed. Never raises.
    """
    if not isinstance(ts, str) or not ts.strip():
        return None
    s = ts.strip()
    # Already in target format
    if s.endswith("Z") and len(s) == 20:
        return s
    # Replace +00:00 / +0000 suffixes
    for suffix in ("+00:00", "+0000"):
        if s.endswith(suffix):
            s = s[: -len(suffix)] + "Z"
            break
    # Try common formats
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
    """
    Compute hold duration in minutes from two UTC timestamp strings.

    Returns 0.0 if either timestamp is absent or unparseable. Never raises.
    """
    if not entry_ts or not exit_ts:
        return 0.0
    try:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        t0 = datetime.strptime(entry_ts, fmt)
        t1 = datetime.strptime(exit_ts, fmt)
        delta = (t1 - t0).total_seconds()
        return max(0.0, round(delta / 60.0, 4))
    except Exception:  # noqa: BLE001
        return 0.0


def _safe_filename(name: str) -> str:
    return _SAFE_NAME_RE.sub("_", name)


def _map_exit_reason(raw: Any) -> str:
    """Map a paper exit_reason string to a valid Queen memory exit_reason."""
    if not isinstance(raw, str):
        return "UNKNOWN"
    upper = raw.strip().upper()
    return upper if upper in _VALID_EXIT_REASONS else "UNKNOWN"


def _write_json_atomic(path: Path, obj: Any) -> None:
    """Write JSON atomically via a .tmp sibling then os.replace()."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Core conversion
# ---------------------------------------------------------------------------

def convert_paper_trade(
    trade: dict[str, Any],
    lane: str = DEFAULT_LANE,
    now_utc: str | None = None,
) -> dict[str, Any] | None:
    """
    Convert one closed paper trade record to Queen memory schema.

    Returns a memory entry dict, or None if the trade is OPEN or the record
    lacks the minimum required fields (trade_id, market, entry_ts, exit_ts).
    Never raises.
    """
    try:
        # Only process CLOSED trades
        state = str(trade.get("state") or trade.get("holding_state") or "").upper()
        if state != "CLOSED":
            return None

        trade_id = str(trade.get("trade_id") or "").strip()
        market   = str(trade.get("market")   or "").strip()
        if not trade_id or not market:
            return None

        entry_ts = _normalise_ts(trade.get("entry_ts"))
        exit_ts  = _normalise_ts(trade.get("exit_ts"))
        if not entry_ts or not exit_ts:
            return None

        strategy_key = str(trade.get("strategy") or "UNKNOWN").strip() or "UNKNOWN"

        try:
            pnl = float(trade.get("realized_pnl") or 0.0)
        except (TypeError, ValueError):
            pnl = 0.0

        win_loss_label = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "FLAT")
        hold_dur = _hold_duration_minutes(entry_ts, exit_ts)
        exit_reason = _map_exit_reason(trade.get("exit_reason"))
        memory_ts = now_utc or _now_utc()

        return {
            "memory_version":            "1",
            "record_type":               "closed_trade_memory",
            "lane":                      lane,
            "market":                    market,
            "strategy_key":              strategy_key,
            "trade_id":                  trade_id,
            "entry_ts_utc":              entry_ts,
            "exit_ts_utc":               exit_ts,
            "hold_duration_minutes":     hold_dur,
            "realized_pnl_eur":          round(pnl, 8),
            "win_loss_label":            win_loss_label,
            "exit_reason":               exit_reason,
            "anomaly_flag":              False,
            "execution_quality_flag":    "OK",
            # Causal sentinels — genuinely unavailable in paper source
            "market_regime_at_entry":    "UNKNOWN",
            "volatility_at_entry":       "UNKNOWN",
            "signal_strength":           -1.0,
            "signal_key":                "UNKNOWN",
            "slippage_vs_expected_eur":  0.0,
            "entry_latency_ms":          0,
            # Metadata
            "feedback_ts_utc":           exit_ts,
            "memory_ts_utc":             memory_ts,
            # queen_action_required=True because regime/volatility are UNKNOWN
            "queen_action_required":     True,
        }
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_paper_reconstruction(source_path: str | Path) -> list[dict[str, Any]] | None:
    """
    Read paper_trade_reconstruction.json and return the trades list.

    Returns the parsed list, or None if the file is missing or unreadable.
    Never raises.
    """
    try:
        p = Path(source_path)
        if not p.exists():
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        # Top-level may be a dict with a "trades" key, or directly a list
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            trades = data.get("trades") or data.get("closed_trades") or []
            if isinstance(trades, list):
                return trades
        return None
    except Exception:  # noqa: BLE001
        return None


def run(
    source_path: str | Path,
    base_output_dir: str | Path,
    lane: str = DEFAULT_LANE,
) -> dict[str, Any]:
    """
    Convert all closed paper trades to Queen memory artifacts and write to disk.

    Parameters:
        source_path     — path to paper_trade_reconstruction.json
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
    try:
        trades = read_paper_reconstruction(source_path)
        if trades is None:
            return {
                "ok":                   False,
                "reason":               f"source file missing or unreadable: {source_path}",
                "lane":                 lane,
                "output_dir":           str(Path(base_output_dir) / lane / "memory"),
                "total_source_records": 0,
                "converted":            0,
                "skipped_open":         0,
                "skipped_invalid":      0,
            }

        mem_dir = Path(base_output_dir) / lane / "memory"
        now = _now_utc()

        converted    = 0
        skipped_open = 0
        skipped_inv  = 0

        for trade in trades:
            if not isinstance(trade, dict):
                skipped_inv += 1
                continue

            state = str(trade.get("state") or trade.get("holding_state") or "").upper()
            if state != "CLOSED":
                skipped_open += 1
                continue

            entry = convert_paper_trade(trade, lane=lane, now_utc=now)
            if entry is None:
                skipped_inv += 1
                continue

            filename = _safe_filename(entry["trade_id"]) + ".json"
            _write_json_atomic(mem_dir / filename, entry)
            converted += 1

        return {
            "ok":                   True,
            "reason":               "BACKFILL_COMPLETE",
            "lane":                 lane,
            "output_dir":           str(mem_dir),
            "total_source_records": len(trades),
            "converted":            converted,
            "skipped_open":         skipped_open,
            "skipped_invalid":      skipped_inv,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok":                   False,
            "reason":               f"unexpected backfill error: {exc}",
            "lane":                 lane,
            "output_dir":           str(Path(base_output_dir) / lane / "memory"),
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
            "Usage: python -m ant_colony.live.historical_memory_backfill "
            "<source_path> <base_output_dir> [lane]"
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
