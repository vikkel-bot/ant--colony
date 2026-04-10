"""
AC-Runner: Scenario + Feedback Generator

Generates 30 deterministic scenarios, runs anomaly_escalation (AC-97)
and anomaly_action_queue (AC-98), produces seeded feedback (70/20/10),
writes to human_feedback_log.jsonl (AC-99), then runs feedback_analysis
(AC-100) and writes feedback_analysis.json.

No execution. No API calls. No new logic. Seed=42.

Usage:
    python ant_colony/run_ac_scenarios_lite.py
    python ant_colony/run_ac_scenarios_lite.py --dry-run   # no file writes
"""
from __future__ import annotations
import argparse
import importlib.util
import json
import random
import sys
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

_esc     = _load("_esc",      "build_anomaly_escalation_lite.py")
_queue   = _load("_queue",    "build_anomaly_action_queue_lite.py")
_capture = _load("_capture",  "build_human_feedback_capture_lite.py")
_analysis = _load("_analysis", "build_feedback_analysis_lite.py")

build_anomaly_escalation  = _esc.build_anomaly_escalation
build_anomaly_action_queue = _queue.build_anomaly_action_queue
capture_and_append         = _capture.capture_and_append
analyse_from_log           = _analysis.analyse_from_log
write_feedback_analysis    = _analysis.write_feedback_analysis

LOG_PATH      = Path(r"C:\Trading\ANT_OUT\human_feedback_log.jsonl")
ANALYSIS_PATH = Path(r"C:\Trading\ANT_OUT\feedback_analysis.json")

# Feedback distribution: 70% CONFIRM, 20% DISAGREE, 10% UNCERTAIN
_FEEDBACK_POOL = ["CONFIRM"] * 7 + ["DISAGREE"] * 2 + ["UNCERTAIN"] * 1

# ---------------------------------------------------------------------------
# Mock input builders
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
        "promotion_mode":            "PROMOTION_READY" if promo == "PAPER_READY" else "PROMOTION_HOLD",
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


def _dossier(
    dossier_status: str = "DOSSIER_READY",
    total_intents:  int = 3,
    promo_snap:     dict | None = None,
) -> dict:
    return {
        "dossier_status":           dossier_status,
        "dossier_mode":             "DOSSIER_PAPER_READY" if dossier_status == "DOSSIER_READY"
                                    else "DOSSIER_PAPER_HOLD",
        "dossier_ready_for_review": True,
        "dossier_reason":           f"promotion is PAPER_READY",
        "dossier_reason_code":      "DOSSIER_PROMOTION_READY",
        "promotion_snapshot":       promo_snap or _promo_snap(),
        "validation_snapshot":      {
            "validation_status": "VALIDATION_PASSED",
            "validation_passed": True,
            "replay_consistent": True,
        },
        "consistency_snapshot":     {"consistency_status": "CONSISTENCY_PASSED", "consistency_passed": True},
        "handoff_snapshot":         {"handoff_status": "READY_FOR_PAPER_HANDOFF", "handoff_ready": True},
        "runner_snapshot":          {"runner_intake_status": "INTAKE_ACCEPTED", "runner_contract_valid": True},
        "readiness_counts":         {
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
    review_status:     str  = "REVIEW_READY",
    review_priority:   str  = "LOW",
    blocking_findings: list | None = None,
) -> dict:
    return {
        "review_status":           review_status,
        "review_mode":             "REVIEW_PAPER_READY",
        "review_decision_hint":    "Candidate is PAPER_READY.",
        "review_reason":           "promotion gate cleared",
        "review_reason_code":      "REVIEW_PAPER_READY_OK",
        "key_findings":            [f"promotion_status=PAPER_READY (PROMOTION_ALL_CLEAR)"],
        "blocking_findings":       blocking_findings or [],
        "review_priority":         review_priority,
        "review_non_binding":      True,
        "review_simulation_only":  True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


def _packet(
    packet_status: str = "READY",
) -> dict:
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
            "dossier":   {"dossier_status": "DOSSIER_READY"},
            "review":    {"review_status": "REVIEW_READY"},
        },
        "flags": {
            "non_binding": True, "simulation_only": True,
            "paper_only": True, "live_activation_allowed": False,
        },
    }


# ---------------------------------------------------------------------------
# Scenario definitions (30 total, deterministic)
# ---------------------------------------------------------------------------

def _build_scenarios() -> list[dict]:
    """
    Return 30 scenario dicts, each with keys:
      name, gate, dossier, review, packet
    """
    scenarios = []

    def add(name, gate, dossier, review, packet):
        scenarios.append(dict(name=name, gate=gate, dossier=dossier,
                              review=review, packet=packet))

    markets = [
        "BTC-EUR", "ETH-EUR", "SOL-EUR", "ADA-EUR", "DOT-EUR",
        "LINK-EUR", "AVAX-EUR", "MATIC-EUR", "ATOM-EUR", "UNI-EUR",
    ]

    # 10 × ALL_CLEAR — NONE anomaly expected
    for i, mkt in enumerate(markets):
        add(
            f"ALL_CLEAR_{mkt}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=i + 1),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
        )

    # 5 × PAPER_HOLD — CRITICAL anomaly (promotion ≠ READY)
    for i in range(5):
        add(
            f"PAPER_HOLD_{i+1:02d}",
            _gate("PAPER_HOLD"),
            _dossier("DOSSIER_HOLD", total_intents=2),
            _review("REVIEW_HOLD", "MEDIUM", []),
            _packet("HOLD"),
        )

    # 4 × PAPER_REJECTED — CRITICAL anomaly
    for i in range(4):
        add(
            f"PAPER_REJECTED_{i+1:02d}",
            _gate("PAPER_REJECTED"),
            _dossier("DOSSIER_REJECTED", total_intents=2),
            _review("REVIEW_REJECTED", "HIGH", [f"promotion rejected: reason_{i}"]),
            _packet("REJECTED"),
        )

    # 2 × VALIDATION_FAILED — CRITICAL anomaly
    for i in range(2):
        add(
            f"VALIDATION_FAILED_{i+1:02d}",
            _gate("PAPER_READY", validation_passed=False),
            _dossier("DOSSIER_READY", total_intents=3),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
        )

    # 2 × CONSISTENCY_FAILED — CRITICAL anomaly
    for i in range(2):
        add(
            f"CONSISTENCY_FAILED_{i+1:02d}",
            _gate("PAPER_READY", consistency_passed=False),
            _dossier("DOSSIER_READY", total_intents=3),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
        )

    # 2 × BLOCKING_FINDINGS — HIGH anomaly
    for i in range(2):
        add(
            f"BLOCKING_FINDINGS_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=3),
            _review("REVIEW_READY", "LOW",
                    [f"validation failed: upstream inconsistency #{i+1}"]),
            _packet("READY"),
        )

    # 2 × LAYER_CONFLICT — HIGH anomaly (promotion READY, review disagrees)
    for i in range(2):
        add(
            f"LAYER_CONFLICT_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=3),
            _review("REVIEW_HOLD", "LOW", []),   # review_status ≠ REVIEW_READY → conflict
            _packet("READY"),
        )

    # 2 × ZERO_INTENTS — LOW anomaly
    for i in range(2):
        add(
            f"ZERO_INTENTS_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=0),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
        )

    # 1 × HOLD_PRIORITY — MEDIUM anomaly (review_priority=MEDIUM, all else READY)
    add(
        "HOLD_PRIORITY_01",
        _gate("PAPER_READY"),
        _dossier("DOSSIER_READY", total_intents=3),
        _review("REVIEW_READY", "MEDIUM", []),
        _packet("READY"),
    )

    assert len(scenarios) == 30, f"expected 30 scenarios, got {len(scenarios)}"
    return scenarios


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run(dry_run: bool = False, log_path: Path = LOG_PATH,
        analysis_path: Path = ANALYSIS_PATH) -> dict:
    """
    Run all 30 scenarios, write feedback entries, then write analysis.

    Returns summary dict.
    """
    rng       = random.Random(42)
    scenarios = _build_scenarios()
    results   = []

    for sc in scenarios:
        # Build anomaly escalation (AC-97)
        escalation = build_anomaly_escalation(
            sc["gate"], sc["dossier"], sc["review"], sc["packet"]
        )
        # Build action queue (AC-98)
        queue = build_anomaly_action_queue(escalation)

        # Pick feedback (seeded)
        fb_action = rng.choice(_FEEDBACK_POOL)
        feedback  = {
            "feedback_action": fb_action,
            "feedback_note":   f"auto-generated for {sc['name']}",
            "operator_id":     "scenario_runner",
        }

        # Capture feedback (AC-99)
        if not dry_run:
            capture_and_append(feedback, queue, path=log_path)

        results.append({
            "scenario":      sc["name"],
            "anomaly_level": escalation["anomaly_level"],
            "action_status": queue["action_status"],
            "feedback":      fb_action,
        })

    # Run analysis (AC-100)
    analysis = None
    if not dry_run:
        analysis = analyse_from_log(
            log_path=log_path,
            output_path=analysis_path,
            write_output=True,
        )

    return {
        "scenarios_run":  len(scenarios),
        "entries_written": 0 if dry_run else len(scenarios),
        "results":        results,
        "analysis":       analysis,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="AC scenario runner")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run without writing any files")
    args = parser.parse_args()

    summary = run(dry_run=args.dry_run)

    # Print compact summary
    levels = {}
    for r in summary["results"]:
        lvl = r["anomaly_level"]
        levels[lvl] = levels.get(lvl, 0) + 1

    fb_counts = {}
    for r in summary["results"]:
        fb = r["feedback"]
        fb_counts[fb] = fb_counts.get(fb, 0) + 1

    print(f"Scenarios run : {summary['scenarios_run']}")
    print(f"Entries written: {summary['entries_written']}")
    print(f"Anomaly levels : {json.dumps(levels)}")
    print(f"Feedback dist  : {json.dumps(fb_counts)}")

    if summary["analysis"] and not args.dry_run:
        sig = summary["analysis"]["signals"]
        print(f"Alignment      : {sig['system_human_alignment']}")
        print(f"Needs attention: {sig['needs_attention']}")
        print(f"Disagree rate  : {summary['analysis']['rates']['disagree_rate']}")
        print(f"Analysis → {ANALYSIS_PATH}")


if __name__ == "__main__":
    main()
