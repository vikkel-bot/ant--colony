"""
AC-Runner: Scenario + Feedback Generator

Generates 60 deterministic scenarios, runs anomaly_escalation (AC-97)
and anomaly_action_queue (AC-98), produces seeded per-family feedback,
writes to human_feedback_log.jsonl (AC-99), then runs feedback_analysis
(AC-100) and writes feedback_analysis.json.

No execution. No API calls. No new logic. Seed=42.

Scenario families (60 total):
  Original 30 ─────────────────────────────────────────────────────────────
  ALL_CLEAR           (10) → NONE      anomaly
  PAPER_HOLD          ( 5) → CRITICAL  anomaly
  PAPER_REJECTED      ( 4) → CRITICAL  anomaly
  VALIDATION_FAILED   ( 2) → CRITICAL  anomaly
  CONSISTENCY_FAILED  ( 2) → CRITICAL  anomaly
  BLOCKING_FINDINGS   ( 2) → HIGH      anomaly
  LAYER_CONFLICT      ( 2) → HIGH      anomaly
  ZERO_INTENTS        ( 2) → LOW       anomaly
  HOLD_PRIORITY       ( 1) → MEDIUM    anomaly

  New 30 ──────────────────────────────────────────────────────────────────
  BORDERLINE_HOLD       ( 2) → MEDIUM   anomaly  (dossier HOLD, promo READY)
  MIXED_BLOCKERS        ( 3) → HIGH     anomaly  (HIGH priority + blocking)
  REVIEW_CONFLICT_LU    ( 3) → HIGH     anomaly  (conflict, LOW urgency hint)
  CRITICAL_CONFIRM      ( 4) → CRITICAL anomaly  (operator mostly CONFIRMs)
  CRITICAL_DISAGREE     ( 4) → CRITICAL anomaly  (operator mostly DISAGREEs)
  UNCERTAIN_HEAVY       ( 3) → HIGH     anomaly  (operator mostly UNCERTAIN)
  ZERO_INTENTS_CONFLICT ( 3) → HIGH     anomaly  (0 intents + review conflict)
  VALIDATION_OK_BLOCK   ( 3) → HIGH     anomaly  (validation ok, blocking present)
  DOSSIER_HOLD_CLEAN    ( 2) → MEDIUM   anomaly  (dossier HOLD, no blockers)
  PROMO_READY_HIGH_REV  ( 3) → HIGH     anomaly  (promo READY, review priority HIGH)

Per-family feedback pools (seeded, deterministic):
  normal / clear    → 7 CONFIRM · 2 DISAGREE · 1 UNCERTAIN
  conflict/blocking → 3 CONFIRM · 4 DISAGREE · 3 UNCERTAIN
  critical_confirm  → 8 CONFIRM · 1 DISAGREE · 1 UNCERTAIN
  critical_disagree → 2 CONFIRM · 6 DISAGREE · 2 UNCERTAIN
  uncertain_heavy   → 2 CONFIRM · 3 DISAGREE · 5 UNCERTAIN
  borderline/hold   → 5 CONFIRM · 3 DISAGREE · 2 UNCERTAIN

Usage:
    python ant_colony/run_ac_scenarios_lite.py
    python ant_colony/run_ac_scenarios_lite.py --dry-run
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
write_feedback_analysis    = _analysis.write_feedback_analysis

LOG_PATH      = Path(r"C:\Trading\ANT_OUT\human_feedback_log.jsonl")
ANALYSIS_PATH = Path(r"C:\Trading\ANT_OUT\feedback_analysis.json")

# ---------------------------------------------------------------------------
# Per-family feedback pools (seeded draws)
# ---------------------------------------------------------------------------

POOL_NORMAL    = ["CONFIRM"] * 7 + ["DISAGREE"] * 2 + ["UNCERTAIN"] * 1
POOL_CONFLICT  = ["CONFIRM"] * 3 + ["DISAGREE"] * 4 + ["UNCERTAIN"] * 3
POOL_CRIT_CONF = ["CONFIRM"] * 8 + ["DISAGREE"] * 1 + ["UNCERTAIN"] * 1
POOL_CRIT_DIS  = ["CONFIRM"] * 2 + ["DISAGREE"] * 6 + ["UNCERTAIN"] * 2
POOL_UNCERTAIN = ["CONFIRM"] * 2 + ["DISAGREE"] * 3 + ["UNCERTAIN"] * 5
POOL_HOLD      = ["CONFIRM"] * 5 + ["DISAGREE"] * 3 + ["UNCERTAIN"] * 2


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
    dossier_status: str  = "DOSSIER_READY",
    total_intents:  int  = 3,
    promo_snap:     dict | None = None,
) -> dict:
    return {
        "dossier_status":           dossier_status,
        "dossier_mode":             "DOSSIER_PAPER_READY" if dossier_status == "DOSSIER_READY"
                                    else "DOSSIER_PAPER_HOLD",
        "dossier_ready_for_review": True,
        "dossier_reason":           "promotion is PAPER_READY",
        "dossier_reason_code":      "DOSSIER_PROMOTION_READY",
        "promotion_snapshot":       promo_snap or _promo_snap(),
        "validation_snapshot": {
            "validation_status": "VALIDATION_PASSED",
            "validation_passed": True,
            "replay_consistent": True,
        },
        "consistency_snapshot": {
            "consistency_status": "CONSISTENCY_PASSED",
            "consistency_passed": True,
        },
        "handoff_snapshot":  {"handoff_status": "READY_FOR_PAPER_HANDOFF", "handoff_ready": True},
        "runner_snapshot":   {"runner_intake_status": "INTAKE_ACCEPTED", "runner_contract_valid": True},
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
            "dossier":   {"dossier_status": "DOSSIER_READY"},
            "review":    {"review_status": "REVIEW_READY"},
        },
        "flags": {
            "non_binding": True, "simulation_only": True,
            "paper_only": True, "live_activation_allowed": False,
        },
    }


# ---------------------------------------------------------------------------
# Scenario definitions (60 total, deterministic)
# ---------------------------------------------------------------------------

def _build_scenarios() -> list[dict]:
    """
    Return 60 scenario dicts.
    Each dict has: name, gate, dossier, review, packet, feedback_pool.
    feedback_pool is a list used by the runner for seeded feedback draws.
    """
    scenarios: list[dict] = []

    def add(name, gate, dossier, review, packet, pool=None):
        scenarios.append(dict(
            name=name, gate=gate, dossier=dossier,
            review=review, packet=packet,
            feedback_pool=pool or POOL_NORMAL,
        ))

    markets = [
        "BTC-EUR", "ETH-EUR", "SOL-EUR", "ADA-EUR", "DOT-EUR",
        "LINK-EUR", "AVAX-EUR", "MATIC-EUR", "ATOM-EUR", "UNI-EUR",
    ]

    # ── Original 30 ─────────────────────────────────────────────────────────

    # 10 × ALL_CLEAR — NONE anomaly
    for i, mkt in enumerate(markets):
        add(
            f"ALL_CLEAR_{mkt}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=i + 1),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
            POOL_NORMAL,
        )

    # 5 × PAPER_HOLD — CRITICAL anomaly
    for i in range(5):
        add(
            f"PAPER_HOLD_{i+1:02d}",
            _gate("PAPER_HOLD"),
            _dossier("DOSSIER_HOLD", total_intents=2),
            _review("REVIEW_HOLD", "MEDIUM", []),
            _packet("HOLD"),
            POOL_HOLD,
        )

    # 4 × PAPER_REJECTED — CRITICAL anomaly
    for i in range(4):
        add(
            f"PAPER_REJECTED_{i+1:02d}",
            _gate("PAPER_REJECTED"),
            _dossier("DOSSIER_REJECTED", total_intents=2),
            _review("REVIEW_REJECTED", "HIGH", [f"promotion rejected: reason_{i}"]),
            _packet("REJECTED"),
            POOL_HOLD,
        )

    # 2 × VALIDATION_FAILED — CRITICAL anomaly
    for i in range(2):
        add(
            f"VALIDATION_FAILED_{i+1:02d}",
            _gate("PAPER_READY", validation_passed=False),
            _dossier("DOSSIER_READY", total_intents=3),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
            POOL_HOLD,
        )

    # 2 × CONSISTENCY_FAILED — CRITICAL anomaly
    for i in range(2):
        add(
            f"CONSISTENCY_FAILED_{i+1:02d}",
            _gate("PAPER_READY", consistency_passed=False),
            _dossier("DOSSIER_READY", total_intents=3),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
            POOL_HOLD,
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
            POOL_CONFLICT,
        )

    # 2 × LAYER_CONFLICT — HIGH anomaly
    for i in range(2):
        add(
            f"LAYER_CONFLICT_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=3),
            _review("REVIEW_HOLD", "LOW", []),
            _packet("READY"),
            POOL_CONFLICT,
        )

    # 2 × ZERO_INTENTS — LOW anomaly
    for i in range(2):
        add(
            f"ZERO_INTENTS_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=0),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
            POOL_NORMAL,
        )

    # 1 × HOLD_PRIORITY — MEDIUM anomaly
    add(
        "HOLD_PRIORITY_01",
        _gate("PAPER_READY"),
        _dossier("DOSSIER_READY", total_intents=3),
        _review("REVIEW_READY", "MEDIUM", []),
        _packet("READY"),
        POOL_HOLD,
    )

    assert len(scenarios) == 30, f"original block: expected 30, got {len(scenarios)}"

    # ── New 30 ───────────────────────────────────────────────────────────────

    # 2 × BORDERLINE_HOLD — MEDIUM anomaly (dossier HOLD, promo READY)
    for i in range(2):
        add(
            f"BORDERLINE_HOLD_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_HOLD", total_intents=4),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
            POOL_HOLD,
        )

    # 3 × MIXED_BLOCKERS — HIGH anomaly (HIGH priority + blocking findings)
    for i in range(3):
        add(
            f"MIXED_BLOCKERS_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=5),
            _review("REVIEW_READY", "HIGH",
                    [f"cross-layer mismatch #{i+1}", "intent count deviation"]),
            _packet("READY"),
            POOL_CONFLICT,
        )

    # 3 × REVIEW_CONFLICT_LU — HIGH anomaly (conflict, review LOW priority but HOLD status)
    for i in range(3):
        add(
            f"REVIEW_CONFLICT_LU_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=4),
            _review("REVIEW_HOLD", "LOW", []),    # status conflict, low urgency hint
            _packet("HOLD"),                       # packet also disagrees → double conflict
            POOL_CONFLICT,
        )

    # 4 × CRITICAL_CONFIRM — CRITICAL anomaly, operator confirms escalation is correct
    for i in range(4):
        add(
            f"CRITICAL_CONFIRM_{i+1:02d}",
            _gate("PAPER_REJECTED"),
            _dossier("DOSSIER_REJECTED", total_intents=3),
            _review("REVIEW_REJECTED", "HIGH",
                    [f"consistent rejection signal #{i+1}"]),
            _packet("REJECTED"),
            POOL_CRIT_CONF,
        )

    # 4 × CRITICAL_DISAGREE — CRITICAL anomaly, operator disagrees with escalation
    for i in range(4):
        add(
            f"CRITICAL_DISAGREE_{i+1:02d}",
            _gate("PAPER_HOLD"),
            _dossier("DOSSIER_HOLD", total_intents=2),
            _review("REVIEW_HOLD", "MEDIUM", []),
            _packet("HOLD"),
            POOL_CRIT_DIS,
        )

    # 3 × UNCERTAIN_HEAVY — HIGH anomaly, operator mostly uncertain
    for i in range(3):
        add(
            f"UNCERTAIN_HEAVY_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=4),
            _review("REVIEW_READY", "HIGH",
                    [f"ambiguous signal: scenario #{i+1}"]),
            _packet("READY"),
            POOL_UNCERTAIN,
        )

    # 3 × ZERO_INTENTS_CONFLICT — HIGH anomaly (0 intents + review conflict)
    for i in range(3):
        add(
            f"ZERO_INTENTS_CONFLICT_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=0),
            _review("REVIEW_HOLD", "LOW", []),    # conflict despite 0 intents
            _packet("READY"),
            POOL_CONFLICT,
        )

    # 3 × VALIDATION_OK_BLOCK — HIGH anomaly (validation ok, blocking findings)
    for i in range(3):
        add(
            f"VALIDATION_OK_BLOCK_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=3),
            _review("REVIEW_READY", "LOW",
                    [f"blocking: strategy weight anomaly #{i+1}"]),
            _packet("READY"),
            POOL_CONFLICT,
        )

    # 2 × DOSSIER_HOLD_CLEAN — MEDIUM anomaly (dossier HOLD, no blockers)
    for i in range(2):
        add(
            f"DOSSIER_HOLD_CLEAN_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_HOLD", total_intents=3),
            _review("REVIEW_READY", "LOW", []),
            _packet("READY"),
            POOL_HOLD,
        )

    # 3 × PROMO_READY_HIGH_REV — HIGH anomaly (promo READY, review priority HIGH, no blocking)
    for i in range(3):
        add(
            f"PROMO_READY_HIGH_REV_{i+1:02d}",
            _gate("PAPER_READY"),
            _dossier("DOSSIER_READY", total_intents=5),
            _review("REVIEW_READY", "HIGH", []),   # HIGH priority, no blocking → HIGH escalation
            _packet("READY"),
            POOL_CONFLICT,
        )

    assert len(scenarios) == 60, f"expected 60 scenarios, got {len(scenarios)}"
    return scenarios


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run(
    dry_run:       bool = False,
    log_path:      Path = LOG_PATH,
    analysis_path: Path = ANALYSIS_PATH,
) -> dict:
    """
    Run all 60 scenarios, write per-family seeded feedback, then write analysis.
    Returns summary dict.
    """
    rng       = random.Random(42)
    scenarios = _build_scenarios()
    results   = []

    for sc in scenarios:
        escalation = build_anomaly_escalation(
            sc["gate"], sc["dossier"], sc["review"], sc["packet"]
        )
        queue = build_anomaly_action_queue(escalation)

        pool      = sc.get("feedback_pool") or POOL_NORMAL
        fb_action = rng.choice(pool)
        feedback  = {
            "feedback_action": fb_action,
            "feedback_note":   f"auto-generated for {sc['name']}",
            "operator_id":     "scenario_runner",
        }

        if not dry_run:
            capture_and_append(feedback, queue, path=log_path)

        results.append({
            "scenario":      sc["name"],
            "anomaly_level": escalation["anomaly_level"],
            "action_status": queue["action_status"],
            "feedback":      fb_action,
        })

    analysis = None
    if not dry_run:
        analysis = analyse_from_log(
            log_path=log_path,
            output_path=analysis_path,
            write_output=True,
        )

    return {
        "scenarios_run":   len(scenarios),
        "entries_written": 0 if dry_run else len(scenarios),
        "results":         results,
        "analysis":        analysis,
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

    levels    = {}
    fb_counts = {}
    for r in summary["results"]:
        levels[r["anomaly_level"]] = levels.get(r["anomaly_level"], 0) + 1
        fb_counts[r["feedback"]]   = fb_counts.get(r["feedback"], 0) + 1

    print(f"Scenarios run  : {summary['scenarios_run']}")
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
