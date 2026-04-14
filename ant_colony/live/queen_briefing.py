"""
AC-185: Queen Briefing

Reads queen_ops_summary.json and queen_review_queue.json and produces one
compact, human-facing briefing artifact for the operator.

One sentence: Distils the ops summary and review queue into the shortest
possible daily-read artifact — non-binding, no effect on execution or allocation.

IMPORTANT — this module is strictly non-binding:
  - no execution changes
  - no allocation changes
  - no live gate changes
  - output artifact is labelled observational_only=True, binding=False

Input (read-only):
    {base_output_dir}/{lane}/queen_ops_summary.json    — AC-184
    {base_output_dir}/{lane}/queen_review_queue.json   — AC-183

Output:
    {base_output_dir}/{lane}/queen_briefing.json

Schema (every field in one sentence):

  generated_ts_utc        — UTC timestamp when this briefing was built
  source_lane             — lane name the inputs were read from
  observational_only      — always True
  binding                 — always False

  attention_required_count — number of groups currently requiring attention
  high_priority_count      — review items labelled HIGH priority
  medium_priority_count    — review items labelled MEDIUM priority
  low_priority_count       — review items labelled LOW priority

  top_priority_summary     — one-line string: primary watch status + market +
                             signal of the top item, or "NONE" when queue empty
  top_review_action        — review_action sentence of the top queue item,
                             or None when queue is empty

  key_items_today          — list of up to MAX_KEY_ITEMS compact dicts
                             (market, strategy_key, signal_key, priority,
                             watch_status, review_action) drawn from the
                             review queue in priority order

  operator_briefing_text   — one concise paragraph the operator reads first;
                             mentions attention count, top action if present,
                             and always ends with a non-binding reminder

MAX_KEY_ITEMS = 5  (top items shown; remaining are counted but not listed)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MAX_KEY_ITEMS: int = 5


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
# Briefing text
# ---------------------------------------------------------------------------

def _build_briefing_text(
    attention_count: int,
    total_groups: int,
    high: int,
    top_priority_summary: str,
    top_review_action: str | None,
) -> str:
    """
    Compose one concise operator briefing paragraph.

    Always ends with a non-binding reminder.
    """
    if total_groups == 0:
        return (
            "Queen has no live groups to report on yet. "
            "This briefing is non-binding and does not affect execution or allocation."
        )

    if attention_count == 0:
        return (
            f"All {total_groups} group(s) are within normal parameters — "
            "no action required today. "
            "This briefing is non-binding and does not affect execution or allocation."
        )

    parts: list[str] = [
        f"{attention_count} group(s) require attention"
        + (f", {high} HIGH priority" if high else "") + "."
    ]

    if top_priority_summary and top_priority_summary != "NONE":
        parts.append(f"Top item: {top_priority_summary}.")

    if top_review_action:
        parts.append(f"Recommended action: {top_review_action}")

    parts.append(
        "This briefing is non-binding and does not affect execution or allocation."
    )
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_briefing(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build the compact briefing dict from ops summary and review queue inputs.

    Returns the briefing dict. Never raises.
    """
    try:
        lane_dir  = Path(base_output_dir) / lane
        ops       = _read_json(lane_dir / "queen_ops_summary.json")
        queue     = _read_json(lane_dir / "queen_review_queue.json")

        # --- counts (prefer ops summary; fall back to queue) ---
        attention_count = int((ops or {}).get("attention_required_count") or 0)
        total_groups    = int((ops or {}).get("total_groups")             or 0)
        high            = int((ops or {}).get("high_priority_count")      or
                              (queue or {}).get("high_count")             or 0)
        medium          = int((ops or {}).get("medium_priority_count")    or
                              (queue or {}).get("medium_count")           or 0)
        low             = int((ops or {}).get("low_priority_count")       or
                              (queue or {}).get("low_count")              or 0)

        # --- top item ---
        items: list[dict[str, Any]] = []
        if queue and isinstance(queue.get("items"), list):
            items = [i for i in queue["items"] if isinstance(i, dict)]

        top_item = items[0] if items else None

        if top_item:
            top_priority_summary = (
                f"{top_item.get('watch_status', 'UNKNOWN')} — "
                f"{top_item.get('market', '?')} / {top_item.get('signal_key', '?')} "
                f"({top_item.get('priority', '?')})"
            )
            top_review_action: str | None = str(top_item.get("review_action") or "")
        else:
            top_priority_summary = "NONE"
            top_review_action = None

        # --- key_items_today: up to MAX_KEY_ITEMS compact dicts ---
        key_items = [
            {
                "market":        str(i.get("market")        or "UNKNOWN"),
                "strategy_key":  str(i.get("strategy_key")  or "UNKNOWN"),
                "signal_key":    str(i.get("signal_key")    or "UNKNOWN"),
                "priority":      str(i.get("priority")      or "UNKNOWN"),
                "watch_status":  str(i.get("watch_status")  or "UNKNOWN"),
                "review_action": str(i.get("review_action") or ""),
            }
            for i in items[:MAX_KEY_ITEMS]
        ]

        briefing_text = _build_briefing_text(
            attention_count, total_groups, high,
            top_priority_summary, top_review_action,
        )

        return {
            "briefing_version":       "1",
            "briefing_type":          "queen_briefing",
            "observational_only":     True,
            "binding":                False,
            "note": (
                "Non-binding operator briefing. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc":       _now_utc(),
            "source_lane":            lane,
            "attention_required_count": attention_count,
            "high_priority_count":    high,
            "medium_priority_count":  medium,
            "low_priority_count":     low,
            "top_priority_summary":   top_priority_summary,
            "top_review_action":      top_review_action,
            "key_items_today":        key_items,
            "operator_briefing_text": briefing_text,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "briefing_version":       "1",
            "briefing_type":          "queen_briefing",
            "observational_only":     True,
            "binding":                False,
            "note": (
                "Non-binding operator briefing. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc":       _now_utc(),
            "source_lane":            lane,
            "attention_required_count": 0,
            "high_priority_count":    0,
            "medium_priority_count":  0,
            "low_priority_count":     0,
            "top_priority_summary":   "NONE",
            "top_review_action":      None,
            "key_items_today":        [],
            "operator_briefing_text": (
                "Briefing build error — check inputs. "
                "This briefing is non-binding and does not affect execution or allocation."
            ),
            "error": f"briefing build error: {exc}",
        }


def run(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build and persist the Queen briefing for one lane.

    Writes:
        {base_output_dir}/{lane}/queen_briefing.json

    Returns:
        {"ok": bool, "reason": str, "output_path": str | None, "briefing": dict}

    Never raises.
    """
    try:
        briefing = build_briefing(base_output_dir, lane)
        out_path = Path(base_output_dir) / lane / "queen_briefing.json"
        _write_json_atomic(out_path, briefing)
        return {
            "ok":          True,
            "reason":      "BRIEFING_WRITTEN",
            "output_path": str(out_path),
            "briefing":    briefing,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok":          False,
            "reason":      f"unexpected briefing error: {exc}",
            "output_path": None,
            "briefing":    {},
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m ant_colony.live.queen_briefing <base_output_dir> <lane>")
        sys.exit(1)

    result = run(sys.argv[1], sys.argv[2])
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
