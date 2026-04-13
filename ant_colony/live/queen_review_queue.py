"""
AC-183: Queen Review Queue

Reads queen_watchlist.json and converts every group with attention_required=True
into a concrete review item for the operator.

One sentence: Converts watched groups into a prioritised, non-binding operator
review queue — no execution, allocation, or gate changes, ever.

IMPORTANT — this module is strictly non-binding:
  - no execution changes
  - no allocation changes
  - no live gate changes
  - output artifact is labelled observational_only=True, binding=False

Input (read-only):
    {base_output_dir}/{lane}/queen_watchlist.json  — AC-182

Output:
    {base_output_dir}/{lane}/queen_review_queue.json

Only groups where attention_required=True generate a review item.
Groups with attention_required=False are counted but not included.

Per-item fields:
  market           — from watchlist group
  strategy_key     — from watchlist group
  signal_key       — from watchlist group
  watch_status     — primary watch flag driving this item
  watch_flags      — full list of triggered flags (may be >1)
  priority         — HIGH / MEDIUM / LOW  (see priority map below)
  review_action    — one-sentence recommended operator action
  operator_note    — human-readable context note (includes watch_reasons)
  attention_required — always True for items in the queue

Priority mapping (by primary watch_status):
  WATCH_SIGNAL_DECAY  → HIGH
  WATCH_SLIPPAGE      → HIGH
  WATCH_LATENCY       → MEDIUM
  WATCH_REGIME_SHIFT  → MEDIUM
  WATCH_SAMPLE        → LOW
  (anything else)     → LOW

Review action mapping (by primary watch_status):
  WATCH_SIGNAL_DECAY  → "Review negative signal pattern and monitor next trades."
  WATCH_SLIPPAGE      → "Review execution cost and expected-vs-fill gap."
  WATCH_LATENCY       → "Review broker/API latency conditions."
  WATCH_REGIME_SHIFT  → "Review whether market context changed materially."
  WATCH_SAMPLE        → "Collect more sample before drawing conclusions."
  (anything else)     → "Review group manually — watch reason unclear."

Items are sorted: HIGH before MEDIUM before LOW, then by (market, strategy_key,
signal_key) within each priority band.

Fail-closed: missing/unreadable watchlist → valid queue with zero items, ok=True.
Never raises.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Deterministic mappings
# ---------------------------------------------------------------------------

_PRIORITY_MAP: dict[str, str] = {
    "WATCH_SIGNAL_DECAY": "HIGH",
    "WATCH_SLIPPAGE":     "HIGH",
    "WATCH_LATENCY":      "MEDIUM",
    "WATCH_REGIME_SHIFT": "MEDIUM",
    "WATCH_SAMPLE":       "LOW",
}

_ACTION_MAP: dict[str, str] = {
    "WATCH_SIGNAL_DECAY": "Review negative signal pattern and monitor next trades.",
    "WATCH_SLIPPAGE":     "Review execution cost and expected-vs-fill gap.",
    "WATCH_LATENCY":      "Review broker/API latency conditions.",
    "WATCH_REGIME_SHIFT": "Review whether market context changed materially.",
    "WATCH_SAMPLE":       "Collect more sample before drawing conclusions.",
}

_PRIORITY_ORDER: dict[str, int] = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


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
# Review item builder
# ---------------------------------------------------------------------------

def build_review_item(group: dict[str, Any]) -> dict[str, Any]:
    """
    Build one review item from a watchlist group.

    Maps watch_status to priority and review_action. Composes an operator_note
    from the group's watch_reasons. Never raises.
    """
    try:
        watch_status = str(group.get("watch_status") or "UNKNOWN")
        watch_flags = group.get("watch_flags") or []
        watch_reasons = group.get("watch_reasons") or []

        priority = _PRIORITY_MAP.get(watch_status, "LOW")
        review_action = _ACTION_MAP.get(
            watch_status,
            "Review group manually — watch reason unclear.",
        )

        # Build operator note: list all triggered reasons
        if watch_reasons:
            reasons_text = " ".join(
                f"[{flag}] {reason}"
                for flag, reason in zip(watch_flags, watch_reasons)
            )
        else:
            reasons_text = "No specific reasons recorded."

        operator_note = (
            f"{reasons_text} "
            "This item is non-binding and does not affect execution or allocation."
        )

        return {
            "market":             str(group.get("market") or "UNKNOWN"),
            "strategy_key":       str(group.get("strategy_key") or "UNKNOWN"),
            "signal_key":         str(group.get("signal_key") or "UNKNOWN"),
            "watch_status":       watch_status,
            "watch_flags":        list(watch_flags),
            "priority":           priority,
            "review_action":      review_action,
            "operator_note":      operator_note,
            "attention_required": True,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "market":             str(group.get("market") or "UNKNOWN"),
            "strategy_key":       str(group.get("strategy_key") or "UNKNOWN"),
            "signal_key":         str(group.get("signal_key") or "UNKNOWN"),
            "watch_status":       "UNKNOWN",
            "watch_flags":        [],
            "priority":           "LOW",
            "review_action":      "Review group manually — watch reason unclear.",
            "operator_note":      (
                f"Review item build error: {exc}. "
                "This item is non-binding and does not affect execution or allocation."
            ),
            "attention_required": True,
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_review_queue(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build the complete review queue dict from queen_watchlist.json.

    Returns the queue dict. Never raises.
    """
    try:
        lane_dir = Path(base_output_dir) / lane
        watchlist = _read_json(lane_dir / "queen_watchlist.json")

        all_groups: list[dict[str, Any]] = []
        if watchlist and isinstance(watchlist.get("groups"), list):
            all_groups = [g for g in watchlist["groups"] if isinstance(g, dict)]

        skipped_count = 0
        items: list[dict[str, Any]] = []
        for g in all_groups:
            if g.get("attention_required") is True:
                items.append(build_review_item(g))
            else:
                skipped_count += 1

        # Sort: HIGH → MEDIUM → LOW, then by (market, strategy_key, signal_key)
        items.sort(key=lambda x: (
            _PRIORITY_ORDER.get(x["priority"], 99),
            x["market"],
            x["strategy_key"],
            x["signal_key"],
        ))

        high_count   = sum(1 for i in items if i["priority"] == "HIGH")
        medium_count = sum(1 for i in items if i["priority"] == "MEDIUM")
        low_count    = sum(1 for i in items if i["priority"] == "LOW")

        return {
            "queue_version":    "1",
            "queue_type":       "queen_review_queue",
            "observational_only": True,
            "binding":          False,
            "note": (
                "Non-binding operator review queue. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane":      lane,
            "total_items":      len(items),
            "skipped_no_watch": skipped_count,
            "high_count":       high_count,
            "medium_count":     medium_count,
            "low_count":        low_count,
            "items":            items,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "queue_version":    "1",
            "queue_type":       "queen_review_queue",
            "observational_only": True,
            "binding":          False,
            "note": (
                "Non-binding operator review queue. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane":      lane,
            "total_items":      0,
            "skipped_no_watch": 0,
            "high_count":       0,
            "medium_count":     0,
            "low_count":        0,
            "items":            [],
            "error":            f"review queue build error: {exc}",
        }


def run(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build and persist the Queen review queue for one lane.

    Writes:
        {base_output_dir}/{lane}/queen_review_queue.json

    Returns:
        {
            "ok": bool,
            "reason": str,
            "output_path": str | None,
            "queue": dict
        }

    Never raises.
    """
    try:
        queue = build_review_queue(base_output_dir, lane)
        out_path = Path(base_output_dir) / lane / "queen_review_queue.json"
        _write_json_atomic(out_path, queue)
        return {
            "ok":          True,
            "reason":      "QUEUE_WRITTEN",
            "output_path": str(out_path),
            "queue":       queue,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok":          False,
            "reason":      f"unexpected queue error: {exc}",
            "output_path": None,
            "queue":       {},
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m ant_colony.live.queen_review_queue <base_output_dir> <lane>")
        sys.exit(1)

    result = run(sys.argv[1], sys.argv[2])
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
