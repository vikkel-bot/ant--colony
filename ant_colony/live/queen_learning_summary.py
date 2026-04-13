"""
AC-179: Queen Learning Summary

Reads existing queen memory artifacts and aggregates a compact, non-binding
learning summary so the Queen can observe trade outcomes across grouping keys.

One sentence: Aggregates closed-trade memory entries into a per-group learning
summary that is observational only and has zero impact on execution or allocation.

IMPORTANT — this module is strictly non-binding:
  - no execution changes
  - no allocation changes
  - no live gate changes
  - output artifact is labelled observational_only=True, binding=False

Directory layout (read-only input):
    {base_output_dir}/{lane}/memory/*.json   — queen memory entries (AC-161)

Output artifact (written once per run):
    {base_output_dir}/{lane}/queen_learning_summary.json

Grouping key: (market, strategy_key, signal_key)

Per-group metrics:
    trades_count                  — total closed trades in group
    win_count                     — trades with win_loss_label="WIN"
    loss_count                    — trades with win_loss_label="LOSS"
    flat_count                    — trades with win_loss_label="FLAT"
    avg_signal_strength           — mean of non-sentinel signal_strength values
                                    (sentinels = -1.0 are excluded from mean)
    avg_slippage_vs_expected_eur  — mean slippage across all trades in group
    avg_entry_latency_ms          — mean broker round-trip latency in group
    last_market_regime            — market_regime_at_entry of most-recent record
    last_volatility               — volatility_at_entry of most-recent record
    queen_action_required_count   — trades where queen_action_required=True

Fail-closed: unreadable or invalid individual records are skipped (counted in
skipped_records); if the memory directory does not exist the summary is written
with zero groups and total_records_read=0 (not a failure).
Never raises.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json_atomic(path: Path, obj: Any) -> None:
    """Write JSON atomically via a .tmp sibling then os.replace()."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_memory_artifacts(base_output_dir: str, lane: str) -> list[dict[str, Any]]:
    """
    Read all queen memory JSON files from {base_output_dir}/{lane}/memory/.

    Returns a list of successfully parsed dicts; skips and ignores any file
    that cannot be read or parsed. Never raises.
    """
    entries: list[dict[str, Any]] = []
    try:
        mem_dir = Path(base_output_dir) / lane / "memory"
        if not mem_dir.is_dir():
            return entries
        for p in sorted(mem_dir.glob("*.json")):
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(obj, dict):
                    entries.append(obj)
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        pass
    return entries


def aggregate_learning_summary(memory_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Aggregate memory entries into per-(market, strategy_key, signal_key) groups.

    Returns a list of group dicts sorted by (market, strategy_key, signal_key).
    """
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}

    for entry in memory_entries:
        if not isinstance(entry, dict):
            continue

        market = str(entry.get("market") or "UNKNOWN")
        strategy_key = str(entry.get("strategy_key") or "UNKNOWN")
        signal_key = str(entry.get("signal_key") or "UNKNOWN")
        key = (market, strategy_key, signal_key)

        if key not in groups:
            groups[key] = {
                "market": market,
                "strategy_key": strategy_key,
                "signal_key": signal_key,
                "trades_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "flat_count": 0,
                "_signal_strengths": [],       # non-sentinel values only
                "_slippages": [],
                "_latencies": [],
                "last_market_regime": "UNKNOWN",
                "last_volatility": "UNKNOWN",
                "_last_ts": "",
                "queen_action_required_count": 0,
            }

        g = groups[key]
        g["trades_count"] += 1

        wl = str(entry.get("win_loss_label") or "FLAT").upper()
        if wl == "WIN":
            g["win_count"] += 1
        elif wl == "LOSS":
            g["loss_count"] += 1
        else:
            g["flat_count"] += 1

        ss = entry.get("signal_strength")
        try:
            ss_f = float(ss)  # type: ignore[arg-type]
            if ss_f != -1.0:  # exclude sentinel
                g["_signal_strengths"].append(ss_f)
        except (TypeError, ValueError):
            pass

        slip = entry.get("slippage_vs_expected_eur")
        try:
            g["_slippages"].append(float(slip))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            pass

        lat = entry.get("entry_latency_ms")
        try:
            g["_latencies"].append(float(lat))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            pass

        if entry.get("queen_action_required") is True:
            g["queen_action_required_count"] += 1

        # Track most-recent record by feedback_ts_utc then memory_ts_utc
        ts = str(entry.get("feedback_ts_utc") or entry.get("memory_ts_utc") or "")
        if ts > g["_last_ts"]:
            g["_last_ts"] = ts
            g["last_market_regime"] = str(entry.get("market_regime_at_entry") or "UNKNOWN")
            g["last_volatility"] = str(entry.get("volatility_at_entry") or "UNKNOWN")

    # Finalise each group: compute averages, drop internal accumulators
    result: list[dict[str, Any]] = []
    for key in sorted(groups):
        g = groups[key]
        ss_vals = g.pop("_signal_strengths")
        slip_vals = g.pop("_slippages")
        lat_vals = g.pop("_latencies")
        g.pop("_last_ts")

        g["avg_signal_strength"] = (
            round(sum(ss_vals) / len(ss_vals), 6) if ss_vals else None
        )
        g["avg_slippage_vs_expected_eur"] = (
            round(sum(slip_vals) / len(slip_vals), 8) if slip_vals else None
        )
        g["avg_entry_latency_ms"] = (
            round(sum(lat_vals) / len(lat_vals), 2) if lat_vals else None
        )
        result.append(g)

    return result


def build_summary(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Read memory artifacts and return a complete (non-written) summary dict.

    Returns the summary dict regardless of whether writing succeeds.
    Never raises.
    """
    try:
        entries = read_memory_artifacts(base_output_dir, lane)
        valid = [e for e in entries if isinstance(e, dict)]
        skipped = len(entries) - len(valid)
        groups = aggregate_learning_summary(valid)
        return {
            "summary_version": "1",
            "summary_type": "queen_learning_summary",
            "observational_only": True,
            "binding": False,
            "note": (
                "Non-binding observational summary. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane": lane,
            "source_dir": str(Path(base_output_dir) / lane / "memory"),
            "total_records_read": len(valid),
            "skipped_records": skipped,
            "groups": groups,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "summary_version": "1",
            "summary_type": "queen_learning_summary",
            "observational_only": True,
            "binding": False,
            "note": (
                "Non-binding observational summary. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane": lane,
            "source_dir": str(Path(base_output_dir) / lane / "memory"),
            "total_records_read": 0,
            "skipped_records": 0,
            "groups": [],
            "error": f"summary build error: {exc}",
        }


def run(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build and persist the Queen learning summary for one lane.

    Writes:
        {base_output_dir}/{lane}/queen_learning_summary.json

    Returns:
        {
            "ok": bool,
            "reason": str,
            "output_path": str | None,
            "summary": dict
        }

    Never raises.
    """
    try:
        summary = build_summary(base_output_dir, lane)
        out_path = Path(base_output_dir) / lane / "queen_learning_summary.json"
        _write_json_atomic(out_path, summary)
        return {
            "ok": True,
            "reason": "SUMMARY_WRITTEN",
            "output_path": str(out_path),
            "summary": summary,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected summary error: {exc}",
            "output_path": None,
            "summary": {},
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m ant_colony.live.queen_learning_summary <base_output_dir> <lane>")
        sys.exit(1)

    result = run(sys.argv[1], sys.argv[2])
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
