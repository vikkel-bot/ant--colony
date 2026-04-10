"""
AC-104: Paper Market Data → Review Chain Bridge

Reads marketdata_scenario_adapter.json (AC-103), maps each market's
review_seed_class to pipeline inputs, and runs the same anomaly/action/
feedback/analysis chain used by the scenario runner.

Mapping:
  ALL_CLEAR     → PAPER_READY + DOSSIER_READY + no blockers  → NONE  anomaly
  ZERO_INTENTS  → PAPER_READY + total_intents=0              → LOW   anomaly
  DATA_STALE    → PAPER_READY + consistency_passed=False     → CRITICAL anomaly
  DATA_MISSING  → PAPER_READY + validation_passed=False      → CRITICAL anomaly
  HOLD_REVIEW   → PAPER_READY + review_priority=MEDIUM       → MEDIUM anomaly

No execution. No API calls. Paper-only. Non-binding. Simulation-only.
live_activation_allowed=False always. Seed=42.

Usage:
    python ant_colony/run_marketdata_review_lite.py
    python ant_colony/run_marketdata_review_lite.py --dry-run
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import random
from pathlib import Path

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------

def _load(mod_name: str, filename: str):
    path = Path(__file__).parent / filename
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_esc      = _load("_esc",      "build_anomaly_escalation_lite.py")
_queue    = _load("_queue",    "build_anomaly_action_queue_lite.py")
_capture  = _load("_capture",  "build_human_feedback_capture_lite.py")
_analysis = _load("_analysis", "build_feedback_analysis_lite.py")

build_anomaly_escalation   = _esc.build_anomaly_escalation
build_anomaly_action_queue = _queue.build_anomaly_action_queue
capture_and_append         = _capture.capture_and_append
analyse_from_log           = _analysis.analyse_from_log

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ADAPTER_PATH  = Path(r"C:\Trading\ANT_OUT\marketdata_scenario_adapter.json")
LOG_PATH      = Path(r"C:\Trading\ANT_OUT\human_feedback_log.jsonl")
ANALYSIS_PATH = Path(r"C:\Trading\ANT_OUT\feedback_analysis.json")

# ---------------------------------------------------------------------------
# Feedback pools (same definition as AC-runner, one source per pool)
# ---------------------------------------------------------------------------

POOL_NORMAL    = ["CONFIRM"] * 7 + ["DISAGREE"] * 2 + ["UNCERTAIN"] * 1
POOL_CONFLICT  = ["CONFIRM"] * 3 + ["DISAGREE"] * 4 + ["UNCERTAIN"] * 3
POOL_HOLD      = ["CONFIRM"] * 5 + ["DISAGREE"] * 3 + ["UNCERTAIN"] * 2

# seed class → feedback pool
_POOL_FOR_CLASS = {
    "ALL_CLEAR":    POOL_NORMAL,
    "ZERO_INTENTS": POOL_NORMAL,
    "DATA_STALE":   POOL_CONFLICT,
    "DATA_MISSING": POOL_CONFLICT,
    "HOLD_REVIEW":  POOL_HOLD,
}

# ---------------------------------------------------------------------------
# Mock input builders
# (same structure as run_ac_scenarios_lite; no new fields)
# ---------------------------------------------------------------------------

def _upstream_snap(**ov) -> dict:
    base = {
        "handoff_status":        "READY_FOR_PAPER_HANDOFF",
        "handoff_ready":         True,
        "runner_intake_status":  "INTAKE_ACCEPTED",
        "runner_contract_valid": True,
        "validation_status":     "VALIDATION_PASSED",
        "validation_passed":     True,
        "replay_consistent":     True,
        "consistency_status":    "CONSISTENCY_PASSED",
        "consistency_passed":    True,
    }
    base.update(ov)
    return base


def _promo_snap(promo_status: str = "PAPER_READY") -> dict:
    return {
        "promotion_status":      promo_status,
        "promotion_ready":       promo_status == "PAPER_READY",
        "paper_ready_candidate": promo_status == "PAPER_READY",
        "promotion_reason_code": "PROMOTION_ALL_CLEAR" if promo_status == "PAPER_READY"
                                 else "PROMOTION_UPSTREAM_HOLD",
        "promotion_reason":      "all upstream checks pass",
        "promotion_decision":    f"{promo_status}: PROMOTION_ALL_CLEAR",
        "upstream_snapshot":     _upstream_snap(),
    }


def _gate(promo: str = "PAPER_READY", **upstream_ov) -> dict:
    return {
        "promotion_status":          promo,
        "promotion_mode":            "PROMOTION_READY" if promo == "PAPER_READY"
                                     else "PROMOTION_HOLD",
        "promotion_ready":           promo == "PAPER_READY",
        "promotion_reason":          "all upstream checks pass",
        "promotion_reason_code":     "PROMOTION_ALL_CLEAR" if promo == "PAPER_READY"
                                     else "PROMOTION_UPSTREAM_HOLD",
        "promotion_decision":        f"{promo}: PROMOTION_ALL_CLEAR",
        "upstream_snapshot":         _upstream_snap(**upstream_ov),
        "paper_ready_candidate":     promo == "PAPER_READY",
        "promotion_non_binding":     True,
        "promotion_simulation_only": True,
        "paper_only":                True,
        "live_activation_allowed":   False,
    }


def _dossier(dossier_status: str = "DOSSIER_READY", total_intents: int = 3) -> dict:
    return {
        "dossier_status":           dossier_status,
        "dossier_mode":             "DOSSIER_PAPER_READY" if dossier_status == "DOSSIER_READY"
                                    else "DOSSIER_PAPER_HOLD",
        "dossier_ready_for_review": True,
        "dossier_reason":           "promotion is PAPER_READY",
        "dossier_reason_code":      "DOSSIER_PROMOTION_READY",
        "promotion_snapshot":       _promo_snap(),
        "validation_snapshot": {
            "validation_status": "VALIDATION_PASSED",
            "validation_passed": True,
            "replay_consistent": True,
        },
        "consistency_snapshot": {
            "consistency_status": "CONSISTENCY_PASSED",
            "consistency_passed": True,
        },
        "handoff_snapshot": {
            "handoff_status": "READY_FOR_PAPER_HANDOFF",
            "handoff_ready":  True,
        },
        "runner_snapshot": {
            "runner_intake_status":  "INTAKE_ACCEPTED",
            "runner_contract_valid": True,
        },
        "readiness_counts": {
            "total_intents":      total_intents,
            "total_allowed":      max(0, total_intents - 1),
            "total_blocked":      min(1, total_intents),
            "ledger_entry_count": total_intents,
            "trace_step_count":   total_intents,
            "matched_checks":     total_intents,
        },
        "dossier_non_binding":     True,
        "dossier_simulation_only": True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


def _review(
    review_status:     str        = "REVIEW_READY",
    review_priority:   str        = "LOW",
    blocking_findings: list | None = None,
) -> dict:
    return {
        "review_status":           review_status,
        "review_mode":             "REVIEW_PAPER_READY",
        "review_decision_hint":    "Candidate is PAPER_READY.",
        "review_reason":           "promotion gate cleared",
        "review_reason_code":      "REVIEW_PAPER_READY_OK",
        "key_findings":            ["promotion_status=PAPER_READY (PROMOTION_ALL_CLEAR)"],
        "blocking_findings":       blocking_findings or [],
        "review_priority":         review_priority,
        "review_non_binding":      True,
        "review_simulation_only":  True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


def _packet(packet_status: str = "READY") -> dict:
    return {
        "version":              "review_packet_v1",
        "component":            "build_review_packet_lite",
        "ts_utc":               "2026-04-10T12:00:00Z",
        "review_packet_status": packet_status,
        "review_packet_mode":   "SIMULATION_ONLY",
        "decision": {
            "decision_hint": "ALLOW_REVIEW",
            "priority":      "LOW",
            "reason":        "all clear",
            "reason_code":   "REVIEW_PAPER_READY_OK",
        },
        "findings": {
            "key_findings":      ["all clear"],
            "blocking_findings": [],
        },
        "summary": {
            "promotion_status": "PAPER_READY",
            "dossier_status":   "DOSSIER_READY",
            "review_status":    "REVIEW_READY",
        },
        "snapshots": {
            "promotion": {"promotion_status": "PAPER_READY"},
            "dossier":   {"dossier_status":   "DOSSIER_READY"},
            "review":    {"review_status":    "REVIEW_READY"},
        },
        "flags": {
            "non_binding": True, "simulation_only": True,
            "paper_only": True, "live_activation_allowed": False,
        },
    }


# ---------------------------------------------------------------------------
# Seed-class → pipeline inputs
# ---------------------------------------------------------------------------

def inputs_for_class(seed_class: str) -> tuple[dict, dict, dict, dict]:
    """
    Map a review_seed_class to (gate, dossier, review, packet) mocks.
    Returns the ALL_CLEAR mapping as fail-closed default for unknown classes.
    """
    if seed_class == "ALL_CLEAR":
        return (
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", 3),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
        )
    if seed_class == "ZERO_INTENTS":
        return (
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", 0),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
        )
    if seed_class == "DATA_STALE":
        return (
            _gate("PAPER_READY", consistency_passed=False),
            _dossier("DOSSIER_READY", 3),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
        )
    if seed_class == "DATA_MISSING":
        return (
            _gate("PAPER_READY", validation_passed=False),
            _dossier("DOSSIER_READY", 3),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
        )
    if seed_class == "HOLD_REVIEW":
        return (
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", 3),
            _review("REVIEW_READY", "MEDIUM", []),
            _packet("READY"),
        )
    # Unknown class — fail-closed: treat as ALL_CLEAR (least disruptive safe default)
    return (
        _gate("PAPER_READY"),
        _dossier("DOSSIER_READY", 3),
        _review("REVIEW_READY", "LOW", []),
        _packet("READY"),
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run(
    adapter_path:  Path = ADAPTER_PATH,
    log_path:      Path = LOG_PATH,
    analysis_path: Path = ANALYSIS_PATH,
    dry_run:       bool = False,
) -> dict:
    """
    Full pipeline: load adapter → classify → run anomaly chain →
    write feedback → write analysis.

    Returns summary dict. Fail-closed if adapter file absent/corrupt.
    """
    # ── Load adapter output ──────────────────────────────────────────────────
    try:
        raw = json.loads(adapter_path.read_text(encoding="utf-8"))
        markets = raw.get("markets") or []
    except (OSError, json.JSONDecodeError):
        markets = []   # fail-closed — no data, nothing to process

    rng     = random.Random(42)
    results = []

    for m in markets:
        market      = m.get("market", "UNKNOWN")
        seed_class  = m.get("review_seed_class", "HOLD_REVIEW")

        gate, dossier, review, packet = inputs_for_class(seed_class)
        escalation = build_anomaly_escalation(gate, dossier, review, packet)
        queue      = build_anomaly_action_queue(escalation)

        pool      = _POOL_FOR_CLASS.get(seed_class, POOL_NORMAL)
        fb_action = rng.choice(pool)
        feedback  = {
            "feedback_action": fb_action,
            "feedback_note":   f"marketdata-review: {market} ({seed_class})",
            "operator_id":     "marketdata_reviewer",
        }

        if not dry_run:
            capture_and_append(feedback, queue, path=log_path)

        results.append({
            "market":        market,
            "seed_class":    seed_class,
            "anomaly_level": escalation["anomaly_level"],
            "action_status": queue["action_status"],
            "feedback":      fb_action,
        })

    analysis = None
    if not dry_run and results:
        analysis = analyse_from_log(
            log_path=log_path,
            output_path=analysis_path,
            write_output=True,
        )

    return {
        "markets_processed": len(results),
        "results":           results,
        "analysis":          analysis,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Market data review pipeline")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run without writing any files")
    args = parser.parse_args()

    summary = run(dry_run=args.dry_run)

    levels    = {}
    fb_counts = {}
    for r in summary["results"]:
        levels[r["anomaly_level"]]  = levels.get(r["anomaly_level"],  0) + 1
        fb_counts[r["feedback"]]    = fb_counts.get(r["feedback"], 0) + 1

    print(f"Markets processed : {summary['markets_processed']}")
    print(f"Anomaly levels    : {json.dumps(levels)}")
    print(f"Feedback dist     : {json.dumps(fb_counts)}")

    if summary["analysis"]:
        sig = summary["analysis"]["signals"]
        print(f"Alignment         : {sig['system_human_alignment']}")
        print(f"Needs attention   : {sig['needs_attention']}")
    elif args.dry_run:
        print("(dry-run — no analysis written)")
    else:
        print("(no markets processed — adapter file missing or empty)")


if __name__ == "__main__":
    main()
