"""
AC-184: Queen Ops Summary

Reads queen_watchlist.json and queen_review_queue.json and produces one compact,
operator-facing overview of current Queen state.

One sentence: Distils the watchlist and review queue into a single glanceable
artifact so the operator can see the full Queen state in one read — non-binding.

IMPORTANT — this module is strictly non-binding:
  - no execution changes
  - no allocation changes
  - no live gate changes
  - output artifact is labelled observational_only=True, binding=False

Input (read-only):
    {base_output_dir}/{lane}/queen_watchlist.json     — AC-182
    {base_output_dir}/{lane}/queen_review_queue.json  — AC-183

Output:
    {base_output_dir}/{lane}/queen_ops_summary.json

Schema (every field explained in one sentence):

  metadata block
    generated_ts_utc   — UTC timestamp when this summary was built
    source_lane        — lane name the inputs were read from
    observational_only — always True; this artifact never affects execution
    binding            — always False; no gate or allocation reads this file

  counts block
    total_groups              — total groups in the watchlist
    attention_required_count  — groups with attention_required=True
    high_priority_count       — review items with priority=HIGH
    medium_priority_count     — review items with priority=MEDIUM
    low_priority_count        — review items with priority=LOW

  top_priority block (None when queue is empty)
    market       — market of the highest-priority item
    strategy_key — strategy of the highest-priority item
    signal_key   — signal of the highest-priority item
    watch_status — primary watch flag of the highest-priority item
    priority     — priority label (HIGH / MEDIUM / LOW)
    review_action — recommended operator action for the top item

  watched_groups — compact list of groups with attention_required=True:
    each entry: {market, strategy_key, signal_key, watch_status, priority}

  operator_summary — one plain-English paragraph summarising current state;
    always ends with a non-binding reminder

Fail-closed: missing/unreadable inputs → valid summary with zero counts, ok=True.
Never raises.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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


# ---------------------------------------------------------------------------
# Operator summary text
# ---------------------------------------------------------------------------

def _build_operator_summary(
    total_groups: int,
    attention_count: int,
    high: int,
    medium: int,
    low: int,
    top_item: dict[str, Any] | None,
) -> str:
    """
    Compose one plain-English paragraph describing current Queen state.

    Always ends with a non-binding reminder.
    """
    if total_groups == 0:
        return (
            "No groups in watchlist — Queen has no data to review yet. "
            "This summary is non-binding and does not affect execution or allocation."
        )

    if attention_count == 0:
        return (
            f"All {total_groups} group(s) are within normal parameters — "
            "no operator attention required at this time. "
            "This summary is non-binding and does not affect execution or allocation."
        )

    parts: list[str] = [
        f"{attention_count} of {total_groups} group(s) require attention."
    ]

    priority_parts: list[str] = []
    if high:
        priority_parts.append(f"{high} HIGH")
    if medium:
        priority_parts.append(f"{medium} MEDIUM")
    if low:
        priority_parts.append(f"{low} LOW")
    if priority_parts:
        parts.append(f"Priority breakdown: {', '.join(priority_parts)}.")

    if top_item:
        parts.append(
            f"Top item: {top_item['market']} / {top_item['signal_key']} "
            f"({top_item['watch_status']}, {top_item['priority']}) — "
            f"{top_item['review_action']}"
        )

    parts.append(
        "This summary is non-binding and does not affect execution or allocation."
    )
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_ops_summary(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build the compact ops summary dict from watchlist and review queue inputs.

    Returns the summary dict. Never raises.
    """
    try:
        lane_dir = Path(base_output_dir) / lane
        watchlist = _read_json(lane_dir / "queen_watchlist.json")
        queue     = _read_json(lane_dir / "queen_review_queue.json")

        # --- counts from watchlist ---
        total_groups = 0
        attention_count = 0
        wl_groups: list[dict[str, Any]] = []
        if watchlist and isinstance(watchlist.get("groups"), list):
            wl_groups = [g for g in watchlist["groups"] if isinstance(g, dict)]
            total_groups = len(wl_groups)
            attention_count = sum(
                1 for g in wl_groups if g.get("attention_required") is True
            )

        # --- counts + items from review queue ---
        high = medium = low = 0
        top_item: dict[str, Any] | None = None
        if queue and isinstance(queue.get("items"), list):
            high   = int(queue.get("high_count")   or 0)
            medium = int(queue.get("medium_count")  or 0)
            low    = int(queue.get("low_count")     or 0)
            items  = [i for i in queue["items"] if isinstance(i, dict)]
            if items:
                top_item = items[0]   # already sorted HIGH→MEDIUM→LOW

        # --- compact watched_groups list ---
        watched_groups = [
            {
                "market":       str(g.get("market")       or "UNKNOWN"),
                "strategy_key": str(g.get("strategy_key") or "UNKNOWN"),
                "signal_key":   str(g.get("signal_key")   or "UNKNOWN"),
                "watch_status": str(g.get("watch_status") or "UNKNOWN"),
                # derive priority from review queue if available
                "priority": next(
                    (
                        i["priority"]
                        for i in (queue.get("items") or [] if queue else [])
                        if isinstance(i, dict)
                        and i.get("market")       == g.get("market")
                        and i.get("strategy_key") == g.get("strategy_key")
                        and i.get("signal_key")   == g.get("signal_key")
                    ),
                    None,
                ),
            }
            for g in wl_groups
            if g.get("attention_required") is True
        ]

        operator_summary = _build_operator_summary(
            total_groups, attention_count, high, medium, low, top_item
        )

        return {
            "summary_version":   "1",
            "summary_type":      "queen_ops_summary",
            "observational_only": True,
            "binding":           False,
            "note": (
                "Non-binding ops summary. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc":       _now_utc(),
            "source_lane":            lane,
            "total_groups":           total_groups,
            "attention_required_count": attention_count,
            "high_priority_count":    high,
            "medium_priority_count":  medium,
            "low_priority_count":     low,
            "top_priority_item":      top_item,
            "watched_groups":         watched_groups,
            "operator_summary":       operator_summary,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "summary_version":   "1",
            "summary_type":      "queen_ops_summary",
            "observational_only": True,
            "binding":           False,
            "note": (
                "Non-binding ops summary. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc":       _now_utc(),
            "source_lane":            lane,
            "total_groups":           0,
            "attention_required_count": 0,
            "high_priority_count":    0,
            "medium_priority_count":  0,
            "low_priority_count":     0,
            "top_priority_item":      None,
            "watched_groups":         [],
            "operator_summary":       (
                "Ops summary build error — check inputs. "
                "This summary is non-binding and does not affect execution or allocation."
            ),
            "error": f"ops summary build error: {exc}",
        }


def run(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build and persist the Queen ops summary for one lane.

    Writes:
        {base_output_dir}/{lane}/queen_ops_summary.json

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
        summary  = build_ops_summary(base_output_dir, lane)
        out_path = Path(base_output_dir) / lane / "queen_ops_summary.json"
        _write_json_atomic(out_path, summary)
        return {
            "ok":          True,
            "reason":      "OPS_SUMMARY_WRITTEN",
            "output_path": str(out_path),
            "summary":     summary,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok":          False,
            "reason":      f"unexpected ops summary error: {exc}",
            "output_path": None,
            "summary":     {},
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m ant_colony.live.queen_ops_summary <base_output_dir> <lane>")
        sys.exit(1)

    result = run(sys.argv[1], sys.argv[2])
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
