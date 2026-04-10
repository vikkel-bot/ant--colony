"""
AC-111: Operator Summary Mini-Script

Prints a compact one-screen operator summary from existing read-only outputs.
No new logic — only reads and formats existing fields.

Sources:
  C:\\Trading\\ANT_OUT\\combined_review_snapshot.json    (AC-107)
  C:\\Trading\\ANT_OUT\\source_health_review.json        (AC-105)
  C:\\Trading\\ANT_OUT\\source_freshness_recovery_plan.json (AC-109)

No execution. No file writes. Paper-only. Non-binding. Simulation-only.

Usage:
    python ant_colony/show_operator_summary.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SNAPSHOT_PATH  = Path(r"C:\Trading\ANT_OUT\combined_review_snapshot.json")
HEALTH_PATH    = Path(r"C:\Trading\ANT_OUT\source_health_review.json")
RECOVERY_PATH  = Path(r"C:\Trading\ANT_OUT\source_freshness_recovery_plan.json")
TRIGGER_PATH   = Path(r"C:\Trading\ANT_OUT\refresh_trigger.json")
READINESS_PATH = Path(r"C:\Trading\ANT_OUT\system_readiness_score.json")


def _load(path: Path) -> dict | None:
    """Load JSON. Returns None if absent or corrupt (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def build_summary(
    snapshot:  dict | None,
    health:    dict | None,
    recovery:  dict | None,
    trigger:   dict | None = None,
    readiness: dict | None = None,
) -> list[str]:
    """
    Build summary lines from loaded dicts.
    Pure function — no I/O, no side effects.
    Returns a list of print-ready strings.
    """
    lines: list[str] = []
    lines.append("=== ANT OPERATOR SUMMARY ===")

    # ── Overview (from combined snapshot) ────────────────────────────────────
    if snapshot and isinstance(snapshot, dict):
        ov_status = snapshot.get("overview_status", "?")
        sm        = snapshot.get("summary", {})
        top_risk  = sm.get("top_risk",      "—") if isinstance(sm, dict) else "—"
        human_ctx = sm.get("human_context", "—") if isinstance(sm, dict) else "—"
        lines.append(f"overview : {ov_status}")
        lines.append(f"top_risk : {top_risk}")
        lines.append(f"human    : {human_ctx}")
    else:
        lines.append("overview : NO DATA")

    lines.append("")

    # ── Source health ─────────────────────────────────────────────────────────
    if health and isinstance(health, dict):
        sh_status = health.get("source_health_status", "?")
        sh_fresh  = health.get("markets_fresh",   0)
        sh_stale  = health.get("markets_stale",   0)
        sh_miss   = health.get("markets_missing", 0)
        lines.append(
            f"source   : {sh_status} | "
            f"fresh={sh_fresh} stale={sh_stale} missing={sh_miss}"
        )
    else:
        lines.append("source   : NO DATA")

    # ── Recovery plan ────────────────────────────────────────────────────────
    if recovery and isinstance(recovery, dict):
        rp_status = recovery.get("recovery_status", "?")
        rp_sm     = recovery.get("summary", {})
        rp_req    = rp_sm.get("markets_requiring_recovery", 0) if isinstance(rp_sm, dict) else 0
        rp_po     = recovery.get("priority_order") or []
        top_mkts  = ", ".join(e["market"] for e in rp_po[:3]) if rp_po else "—"
        lines.append(
            f"recovery : {rp_status} | requiring={rp_req} | top={top_mkts}"
        )
    else:
        lines.append("recovery : NO DATA")

    # ── Refresh trigger (AC-114) ──────────────────────────────────────────────
    if trigger and isinstance(trigger, dict):
        tr_status = trigger.get("trigger_status", "?")
        tr_og     = trigger.get("operator_guidance", {})
        tr_action = tr_og.get("recommended_action", "?") if isinstance(tr_og, dict) else "?"
        tr_window = tr_og.get("recommended_window", "?") if isinstance(tr_og, dict) else "?"
        lines.append(
            f"trigger  : {tr_status} | action={tr_action} | window={tr_window}"
        )
    else:
        lines.append("trigger  : NO DATA")

    # ── System readiness (AC-117) ─────────────────────────────────────────────
    if readiness and isinstance(readiness, dict):
        rd_status = readiness.get("readiness_status", "?")
        rd_score  = readiness.get("readiness_score",  "?")
        lines.append(f"readiness: {rd_status} | score={rd_score}/100")
    else:
        lines.append("readiness: NO DATA")

    return lines


def show(
    snapshot_path:  Path = SNAPSHOT_PATH,
    health_path:    Path = HEALTH_PATH,
    recovery_path:  Path = RECOVERY_PATH,
    trigger_path:   Path = TRIGGER_PATH,
    readiness_path: Path = READINESS_PATH,
) -> None:
    """Load sources and print operator summary. No file writes."""
    snapshot  = _load(snapshot_path)
    health    = _load(health_path)
    recovery  = _load(recovery_path)
    trigger   = _load(trigger_path)
    readiness = _load(readiness_path)

    for line in build_summary(snapshot, health, recovery, trigger, readiness):
        print(line)


if __name__ == "__main__":
    show()
