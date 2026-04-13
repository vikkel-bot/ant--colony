"""
AC-182: Queen Watchlist

Reads queen_advisory_summary.json and queen_advisory_delta.json, and derives
a non-binding watch status for each (market, strategy_key, signal_key) group.

One sentence: Flags groups that deserve Queen attention based on advisory and
delta signals — strictly observational with no effect on execution, allocation,
or live gates.

IMPORTANT — this module is strictly non-binding:
  - no execution changes
  - no allocation changes
  - no live gate changes
  - output artifact is labelled observational_only=True, binding=False

Input (read-only):
    {base_output_dir}/{lane}/queen_advisory_summary.json  — AC-180
    {base_output_dir}/{lane}/queen_advisory_delta.json    — AC-181

Output:
    {base_output_dir}/{lane}/queen_watchlist.json

Watch flags (checked in priority order; multiple may apply):

  WATCH_SIGNAL_DECAY
      Triggered when signal_observation == "NEGATIVE_SIGNAL" in the current
      advisory, OR signal_observation_trend == "DEGRADED" in the delta.
      Reason: win rate is weak or falling — the Queen should review this signal.

  WATCH_SLIPPAGE
      Triggered when slippage_trend == "WORSENING" in the delta.
      Reason: average slippage is increasing — execution costs are rising.

  WATCH_LATENCY
      Triggered when latency_trend == "WORSENING" in the delta.
      Reason: broker round-trip latency is increasing — infrastructure check needed.

  WATCH_REGIME_SHIFT
      Triggered when last_market_regime == "UNKNOWN" in the current advisory.
      Reason: market regime is unknown — causal context for recent trades is incomplete.

  WATCH_SAMPLE
      Triggered when sample_size_status == "INSUFFICIENT" in the current advisory.
      Reason: fewer than 5 trades in this group — observations are not yet reliable.

  NO_WATCH
      None of the above triggered.

Per-group output fields:
  watch_status       — primary (highest-priority) watch flag, or "NO_WATCH"
  watch_flags        — list of all triggered flags (empty list if NO_WATCH)
  watch_reasons      — list of one-sentence reasons, one per triggered flag
  attention_required — True when any watch flag is present, False otherwise

Flag priority order (highest first):
  WATCH_SIGNAL_DECAY > WATCH_SLIPPAGE > WATCH_LATENCY >
  WATCH_REGIME_SHIFT > WATCH_SAMPLE > NO_WATCH

If the delta file is absent or a group is not present in the delta, delta-based
rules (slippage_trend, latency_trend, signal_observation_trend) are skipped.

Fail-closed: missing/unreadable input → valid watchlist with zero groups, ok=True.
Never raises.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Watch flag priority list (highest first)
# ---------------------------------------------------------------------------

_FLAG_PRIORITY = [
    "WATCH_SIGNAL_DECAY",
    "WATCH_SLIPPAGE",
    "WATCH_LATENCY",
    "WATCH_REGIME_SHIFT",
    "WATCH_SAMPLE",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json_atomic(path: Path, obj: Any) -> None:
    """Write JSON atomically via a .tmp sibling then os.replace()."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _read_json(path: Path) -> dict[str, Any] | None:
    """Read and parse a JSON file. Returns None if missing or unreadable. Never raises."""
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def _group_key(group: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(group.get("market") or "UNKNOWN"),
        str(group.get("strategy_key") or "UNKNOWN"),
        str(group.get("signal_key") or "UNKNOWN"),
    )


def _index_groups(
    groups: list[dict[str, Any]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    return {_group_key(g): g for g in groups if isinstance(g, dict)}


# ---------------------------------------------------------------------------
# Watch rule evaluation
# ---------------------------------------------------------------------------

def _evaluate_watch_flags(
    advisory_group: dict[str, Any],
    delta_group: dict[str, Any] | None,
) -> list[tuple[str, str]]:
    """
    Return a list of (flag, reason) tuples for all triggered watch flags,
    ordered by descending priority.
    """
    triggered: dict[str, str] = {}

    # --- WATCH_SIGNAL_DECAY ---
    signal_obs = str(advisory_group.get("signal_observation") or "")
    if signal_obs == "NEGATIVE_SIGNAL":
        triggered["WATCH_SIGNAL_DECAY"] = (
            "Win rate is below 40% — signal is producing a negative outcome pattern."
        )
    if delta_group is not None:
        sig_trend = str(delta_group.get("signal_observation_trend") or "")
        if sig_trend == "DEGRADED" and "WATCH_SIGNAL_DECAY" not in triggered:
            triggered["WATCH_SIGNAL_DECAY"] = (
                "Signal observation degraded since last snapshot."
            )

    # --- WATCH_SLIPPAGE ---
    if delta_group is not None:
        slip_trend = str(delta_group.get("slippage_trend") or "")
        if slip_trend == "WORSENING":
            triggered["WATCH_SLIPPAGE"] = (
                "Average slippage is increasing — execution costs are rising."
            )

    # --- WATCH_LATENCY ---
    if delta_group is not None:
        lat_trend = str(delta_group.get("latency_trend") or "")
        if lat_trend == "WORSENING":
            triggered["WATCH_LATENCY"] = (
                "Broker round-trip latency is increasing — infrastructure check needed."
            )

    # --- WATCH_REGIME_SHIFT ---
    regime = str(advisory_group.get("last_market_regime") or "UNKNOWN")
    if regime == "UNKNOWN":
        triggered["WATCH_REGIME_SHIFT"] = (
            "Last known market regime is UNKNOWN — causal context for recent trades is incomplete."
        )

    # --- WATCH_SAMPLE ---
    sample_status = str(advisory_group.get("sample_size_status") or "")
    if sample_status == "INSUFFICIENT":
        triggered["WATCH_SAMPLE"] = (
            "Fewer than 5 trades in this group — observations are not yet reliable."
        )

    # Return in priority order
    return [(flag, triggered[flag]) for flag in _FLAG_PRIORITY if flag in triggered]


def build_watch_entry(
    advisory_group: dict[str, Any],
    delta_group: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Derive a watch entry for one group by combining advisory and delta signals.

    Returns the advisory group fields plus watch fields. Never raises.
    """
    try:
        flags_and_reasons = _evaluate_watch_flags(advisory_group, delta_group)
        watch_flags = [f for f, _ in flags_and_reasons]
        watch_reasons = [r for _, r in flags_and_reasons]
        watch_status = watch_flags[0] if watch_flags else "NO_WATCH"
        attention_required = len(watch_flags) > 0

        return {
            **advisory_group,
            "watch_status": watch_status,
            "watch_flags": watch_flags,
            "watch_reasons": watch_reasons,
            "attention_required": attention_required,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            **advisory_group,
            "watch_status": "UNKNOWN",
            "watch_flags": [],
            "watch_reasons": [f"Watch evaluation error: {exc}"],
            "attention_required": False,
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_watchlist(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build the complete watchlist dict from advisory and delta inputs.

    Returns the watchlist dict. Never raises.
    """
    try:
        lane_dir = Path(base_output_dir) / lane
        advisory = _read_json(lane_dir / "queen_advisory_summary.json")
        delta = _read_json(lane_dir / "queen_advisory_delta.json")

        advisory_groups: list[dict[str, Any]] = []
        if advisory and isinstance(advisory.get("groups"), list):
            advisory_groups = [g for g in advisory["groups"] if isinstance(g, dict)]

        delta_index: dict[tuple[str, str, str], dict[str, Any]] = {}
        if delta and isinstance(delta.get("groups"), list):
            delta_index = _index_groups(
                [g for g in delta["groups"] if isinstance(g, dict)]
            )

        watch_entries = []
        attention_count = 0
        for ag in advisory_groups:
            key = _group_key(ag)
            dg = delta_index.get(key)
            entry = build_watch_entry(ag, dg)
            watch_entries.append(entry)
            if entry.get("attention_required"):
                attention_count += 1

        return {
            "watchlist_version": "1",
            "watchlist_type": "queen_watchlist",
            "observational_only": True,
            "binding": False,
            "note": (
                "Non-binding watchlist. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane": lane,
            "total_groups": len(watch_entries),
            "attention_required_count": attention_count,
            "groups": watch_entries,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "watchlist_version": "1",
            "watchlist_type": "queen_watchlist",
            "observational_only": True,
            "binding": False,
            "note": (
                "Non-binding watchlist. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane": lane,
            "total_groups": 0,
            "attention_required_count": 0,
            "groups": [],
            "error": f"watchlist build error: {exc}",
        }


def run(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build and persist the Queen watchlist for one lane.

    Writes:
        {base_output_dir}/{lane}/queen_watchlist.json

    Returns:
        {
            "ok": bool,
            "reason": str,
            "output_path": str | None,
            "watchlist": dict
        }

    Never raises.
    """
    try:
        watchlist = build_watchlist(base_output_dir, lane)
        out_path = Path(base_output_dir) / lane / "queen_watchlist.json"
        _write_json_atomic(out_path, watchlist)
        return {
            "ok": True,
            "reason": "WATCHLIST_WRITTEN",
            "output_path": str(out_path),
            "watchlist": watchlist,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected watchlist error: {exc}",
            "output_path": None,
            "watchlist": {},
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m ant_colony.live.queen_watchlist <base_output_dir> <lane>")
        sys.exit(1)

    result = run(sys.argv[1], sys.argv[2])
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
