"""
AC-181: Queen Advisory Delta

Compares the current queen_advisory_summary.json with the previous advisory
snapshot and emits a non-binding delta artifact per (market, strategy_key,
signal_key) group.

One sentence: Detects change between two consecutive advisory snapshots so the
Queen can observe trends over time — strictly observational with no effect on
execution, allocation, or live gates.

IMPORTANT — this module is strictly non-binding:
  - no execution changes
  - no allocation changes
  - no live gate changes
  - output artifact is labelled observational_only=True, binding=False

File layout:
    Input  (read-only): {base_output_dir}/{lane}/queen_advisory_summary.json
    Prev snapshot:      {base_output_dir}/{lane}/queen_advisory_prev.json
    Delta output:       {base_output_dir}/{lane}/queen_advisory_delta.json

On each run the current advisory is saved as queen_advisory_prev.json so the
next run can compare against it.

Per-group delta fields (all strings/numbers, all deterministic):

  sample_size_trend
      "NEW"       — group not present in previous snapshot
      "GROWING"   — trades_count increased
      "SHRINKING" — trades_count decreased
      "UNCHANGED" — trades_count identical

  slippage_trend
      "NEW"       — group not present in previous snapshot
      "NO_DATA"   — avg_slippage_vs_expected_eur absent in current or previous
      "IMPROVING" — slippage decreased by more than SLIPPAGE_THRESHOLD
      "WORSENING" — slippage increased by more than SLIPPAGE_THRESHOLD
      "STABLE"    — change within ±SLIPPAGE_THRESHOLD

  latency_trend
      "NEW"       — group not present in previous snapshot
      "NO_DATA"   — avg_entry_latency_ms absent in current or previous
      "IMPROVING" — latency decreased by more than LATENCY_THRESHOLD_MS
      "WORSENING" — latency increased by more than LATENCY_THRESHOLD_MS
      "STABLE"    — change within ±LATENCY_THRESHOLD_MS

  signal_observation_trend
      "NEW"       — group not present in previous snapshot
      "UNCHANGED" — signal_observation identical to previous
      "IMPROVED"  — signal moved in a positive direction
                    (NEGATIVE→NEUTRAL, NEGATIVE→POSITIVE, NEUTRAL→POSITIVE)
      "DEGRADED"  — signal moved in a negative direction
                    (POSITIVE→NEUTRAL, POSITIVE→NEGATIVE, NEUTRAL→NEGATIVE)
      "CHANGED"   — any other change (e.g. from/to NO_DATA)

  sample_size_delta    — numeric: current trades_count minus previous (None if NEW)
  slippage_delta       — numeric: current minus previous slippage (None if NEW/NO_DATA)
  latency_delta_ms     — numeric: current minus previous latency (None if NEW/NO_DATA)
  advisory_change_note — one plain-English sentence; always ends with non-binding reminder

Top-level fields:
  comparison_status — "COMPARED" | "FIRST_SNAPSHOT" (no previous file found)
  groups_new        — groups present in current but not in previous
  groups_removed    — groups present in previous but not in current
  groups_compared   — groups present in both

Thresholds (module-level constants):
  SLIPPAGE_THRESHOLD  = 0.005   (EUR)
  LATENCY_THRESHOLD_MS = 5.0    (ms)

Fail-closed: missing/unreadable input → valid delta with FIRST_SNAPSHOT status.
Never raises.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

SLIPPAGE_THRESHOLD: float = 0.005   # EUR — changes smaller than this are STABLE
LATENCY_THRESHOLD_MS: float = 5.0   # ms  — changes smaller than this are STABLE

# Signal observation ordering for trend detection (higher index = better)
_SIGNAL_RANK: dict[str, int] = {
    "NEGATIVE_SIGNAL": 0,
    "NO_DATA": 1,
    "NEUTRAL_SIGNAL": 2,
    "POSITIVE_SIGNAL": 3,
}


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


def _group_key(group: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(group.get("market") or "UNKNOWN"),
        str(group.get("strategy_key") or "UNKNOWN"),
        str(group.get("signal_key") or "UNKNOWN"),
    )


def _index_groups(groups: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    return {_group_key(g): g for g in groups if isinstance(g, dict)}


# ---------------------------------------------------------------------------
# Per-group delta computation
# ---------------------------------------------------------------------------

def _sample_size_trend(
    cur_count: int, prev_count: int
) -> tuple[str, int | None]:
    """Return (trend_label, numeric_delta)."""
    delta = cur_count - prev_count
    if delta > 0:
        return "GROWING", delta
    if delta < 0:
        return "SHRINKING", delta
    return "UNCHANGED", 0


def _float_trend(
    cur_val: Any,
    prev_val: Any,
    threshold: float,
    positive_is_good: bool,
) -> tuple[str, float | None]:
    """
    Return (trend_label, numeric_delta) for a numeric metric.

    positive_is_good=False means a lower value is better (slippage, latency).
    """
    if cur_val is None or prev_val is None:
        return "NO_DATA", None
    try:
        delta = float(cur_val) - float(prev_val)
    except (TypeError, ValueError):
        return "NO_DATA", None

    abs_delta = abs(delta)
    if abs_delta <= threshold:
        return "STABLE", round(delta, 8)

    if positive_is_good:
        return ("IMPROVING" if delta > 0 else "WORSENING"), round(delta, 8)
    else:
        return ("IMPROVING" if delta < 0 else "WORSENING"), round(delta, 8)


def _signal_obs_trend(cur_obs: str, prev_obs: str) -> str:
    """Return UNCHANGED / IMPROVED / DEGRADED / CHANGED."""
    if cur_obs == prev_obs:
        return "UNCHANGED"
    cur_rank = _SIGNAL_RANK.get(cur_obs)
    prev_rank = _SIGNAL_RANK.get(prev_obs)
    if cur_rank is None or prev_rank is None:
        return "CHANGED"
    if cur_rank > prev_rank:
        return "IMPROVED"
    if cur_rank < prev_rank:
        return "DEGRADED"
    return "UNCHANGED"


def _change_note(
    market: str,
    signal_key: str,
    is_new: bool,
    sample_trend: str,
    sample_delta: int | None,
    slippage_trend: str,
    latency_trend: str,
    signal_trend: str,
) -> str:
    """Build one plain-English change note. Always ends with non-binding reminder."""
    if is_new:
        return (
            f"Group {market}/{signal_key} appears for the first time in this snapshot. "
            "This note is non-binding and does not affect execution or allocation."
        )

    parts: list[str] = []

    if sample_trend == "GROWING":
        parts.append(f"Sample grew by {sample_delta} trade(s).")
    elif sample_trend == "SHRINKING":
        parts.append(f"Sample shrank by {abs(sample_delta or 0)} trade(s).")
    else:
        parts.append("Sample size unchanged.")

    if signal_trend == "IMPROVED":
        parts.append("Signal observation improved.")
    elif signal_trend == "DEGRADED":
        parts.append("Signal observation degraded.")
    elif signal_trend == "CHANGED":
        parts.append("Signal observation changed.")
    else:
        parts.append("Signal observation unchanged.")

    if slippage_trend == "IMPROVING":
        parts.append("Slippage improving.")
    elif slippage_trend == "WORSENING":
        parts.append("Slippage worsening.")

    if latency_trend == "IMPROVING":
        parts.append("Latency improving.")
    elif latency_trend == "WORSENING":
        parts.append("Latency worsening.")

    parts.append("This note is non-binding and does not affect execution or allocation.")
    return " ".join(parts)


def diff_group(
    cur: dict[str, Any],
    prev: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Compute delta fields for one group.

    prev=None means the group is new (not in previous snapshot).
    Returns cur fields plus delta fields. Never raises.
    """
    try:
        market = str(cur.get("market") or "UNKNOWN")
        signal_key = str(cur.get("signal_key") or "UNKNOWN")
        is_new = prev is None

        if is_new:
            return {
                **cur,
                "sample_size_trend": "NEW",
                "sample_size_delta": None,
                "slippage_trend": "NEW",
                "slippage_delta": None,
                "latency_trend": "NEW",
                "latency_delta_ms": None,
                "signal_observation_trend": "NEW",
                "advisory_change_note": _change_note(
                    market, signal_key, True, "NEW", None, "NEW", "NEW", "NEW"
                ),
            }

        cur_count = int(cur.get("trades_count") or 0)
        prev_count = int(prev.get("trades_count") or 0)
        sample_trend, sample_delta = _sample_size_trend(cur_count, prev_count)

        slip_trend, slip_delta = _float_trend(
            cur.get("avg_slippage_vs_expected_eur"),
            prev.get("avg_slippage_vs_expected_eur"),
            SLIPPAGE_THRESHOLD,
            positive_is_good=False,
        )

        lat_trend, lat_delta = _float_trend(
            cur.get("avg_entry_latency_ms"),
            prev.get("avg_entry_latency_ms"),
            LATENCY_THRESHOLD_MS,
            positive_is_good=False,
        )

        sig_trend = _signal_obs_trend(
            str(cur.get("signal_observation") or "NO_DATA"),
            str(prev.get("signal_observation") or "NO_DATA"),
        )

        note = _change_note(
            market, signal_key, False,
            sample_trend, sample_delta,
            slip_trend, lat_trend, sig_trend,
        )

        return {
            **cur,
            "sample_size_trend": sample_trend,
            "sample_size_delta": sample_delta,
            "slippage_trend": slip_trend,
            "slippage_delta": slip_delta,
            "latency_trend": lat_trend,
            "latency_delta_ms": lat_delta,
            "signal_observation_trend": sig_trend,
            "advisory_change_note": note,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            **cur,
            "sample_size_trend": "UNKNOWN",
            "sample_size_delta": None,
            "slippage_trend": "UNKNOWN",
            "slippage_delta": None,
            "latency_trend": "UNKNOWN",
            "latency_delta_ms": None,
            "signal_observation_trend": "UNKNOWN",
            "advisory_change_note": (
                f"Delta computation error: {exc}. "
                "This note is non-binding and does not affect execution or allocation."
            ),
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_advisory(path: Path) -> dict[str, Any] | None:
    """
    Read a queen_advisory_summary JSON file.

    Returns parsed dict or None if missing/unreadable. Never raises.
    """
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def build_delta(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build the complete advisory delta dict by comparing current vs previous snapshot.

    Returns the delta dict. Never raises.
    """
    try:
        lane_dir = Path(base_output_dir) / lane
        cur_advisory = read_advisory(lane_dir / "queen_advisory_summary.json")
        prev_advisory = read_advisory(lane_dir / "queen_advisory_prev.json")

        comparison_status = "COMPARED" if prev_advisory is not None else "FIRST_SNAPSHOT"

        cur_groups: list[dict[str, Any]] = []
        if cur_advisory and isinstance(cur_advisory.get("groups"), list):
            cur_groups = [g for g in cur_advisory["groups"] if isinstance(g, dict)]

        prev_index: dict[tuple[str, str, str], dict[str, Any]] = {}
        if prev_advisory and isinstance(prev_advisory.get("groups"), list):
            prev_index = _index_groups(
                [g for g in prev_advisory["groups"] if isinstance(g, dict)]
            )

        cur_index = _index_groups(cur_groups)

        groups_new = [k for k in cur_index if k not in prev_index]
        groups_removed = [k for k in prev_index if k not in cur_index]
        groups_compared_keys = [k for k in cur_index if k in prev_index]

        diffed: list[dict[str, Any]] = []
        for g in cur_groups:
            key = _group_key(g)
            prev_g = prev_index.get(key)
            diffed.append(diff_group(g, prev_g))

        return {
            "delta_version": "1",
            "delta_type": "queen_advisory_delta",
            "observational_only": True,
            "binding": False,
            "note": (
                "Non-binding advisory delta. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane": lane,
            "comparison_status": comparison_status,
            "groups_new_count": len(groups_new),
            "groups_removed_count": len(groups_removed),
            "groups_compared_count": len(groups_compared_keys),
            "thresholds": {
                "slippage_threshold_eur": SLIPPAGE_THRESHOLD,
                "latency_threshold_ms": LATENCY_THRESHOLD_MS,
            },
            "groups": diffed,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "delta_version": "1",
            "delta_type": "queen_advisory_delta",
            "observational_only": True,
            "binding": False,
            "note": (
                "Non-binding advisory delta. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane": lane,
            "comparison_status": "ERROR",
            "groups_new_count": 0,
            "groups_removed_count": 0,
            "groups_compared_count": 0,
            "thresholds": {
                "slippage_threshold_eur": SLIPPAGE_THRESHOLD,
                "latency_threshold_ms": LATENCY_THRESHOLD_MS,
            },
            "groups": [],
            "error": f"delta build error: {exc}",
        }


def run(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build and persist the Queen advisory delta for one lane.

    Steps:
      1. Read current queen_advisory_summary.json
      2. Read queen_advisory_prev.json (previous snapshot, if any)
      3. Compute per-group delta
      4. Write queen_advisory_delta.json
      5. Save current advisory as queen_advisory_prev.json for next run

    Returns:
        {
            "ok": bool,
            "reason": str,
            "output_path": str | None,
            "delta": dict
        }

    Never raises.
    """
    try:
        lane_dir = Path(base_output_dir) / lane
        delta = build_delta(base_output_dir, lane)

        # Write delta
        delta_path = lane_dir / "queen_advisory_delta.json"
        _write_json_atomic(delta_path, delta)

        # Rotate: save current advisory as prev snapshot for next run
        cur_advisory = read_advisory(lane_dir / "queen_advisory_summary.json")
        if cur_advisory is not None:
            _write_json_atomic(lane_dir / "queen_advisory_prev.json", cur_advisory)

        return {
            "ok": True,
            "reason": "DELTA_WRITTEN",
            "output_path": str(delta_path),
            "delta": delta,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected delta error: {exc}",
            "output_path": None,
            "delta": {},
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m ant_colony.live.queen_advisory_delta <base_output_dir> <lane>")
        sys.exit(1)

    result = run(sys.argv[1], sys.argv[2])
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
