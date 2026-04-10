"""
AC-96: Review Packet — Unified Human Decision Object

Single bundling layer on top of AC-94 (promotion gate) and
AC-95 (paper_readiness_dossier + human_review_summary).

Produces one compact review_packet that is the standard entry point
for human review: status, decision, findings, summary, snapshots, flags.

No new decision logic. No status recalculation. Only bundling and
structuring of existing layer outputs.

Design principles:
  - flags.non_binding=True always.
  - flags.simulation_only=True always.
  - flags.paper_only=True always.
  - flags.live_activation_allowed=False always.
  - Fail-closed: missing dossier or review → REJECTED packet.
  - Deterministic: same inputs → same packet every call.
  - Pure core function (build_review_packet) — no I/O, no side effects.
  - File output is separate (write_review_packet / build_and_write).

Status mapping (human_review_summary is leading):
  REVIEW_READY    → review_packet_status = "READY"
  REVIEW_HOLD     → review_packet_status = "HOLD"
  REVIEW_REJECTED → review_packet_status = "REJECTED"
  (anything else) → review_packet_status = "REJECTED"

decision_hint mapping:
  READY    → ALLOW_REVIEW
  HOLD     → HOLD_REVIEW
  REJECTED → DO_NOT_PROMOTE

Output file: C:\\Trading\\ANT_OUT\\review_packet.json
"""
from __future__ import annotations
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

VERSION        = "review_packet_v1"
COMPONENT      = "build_review_packet_lite"
OUT_DIR        = Path(r"C:\Trading\ANT_OUT")
REVIEW_PACKET_PATH = OUT_DIR / "review_packet.json"

# Status values
PKT_READY    = "READY"
PKT_HOLD     = "HOLD"
PKT_REJECTED = "REJECTED"

PKT_MODE = "SIMULATION_ONLY"

# Decision hint values
HINT_ALLOW    = "ALLOW_REVIEW"
HINT_HOLD     = "HOLD_REVIEW"
HINT_REJECT   = "DO_NOT_PROMOTE"

# Review status mirrors (AC-95)
_REV_READY    = "REVIEW_READY"
_REV_HOLD     = "REVIEW_HOLD"
_REV_REJECTED = "REVIEW_REJECTED"

_STATUS_MAP = {
    _REV_READY:    PKT_READY,
    _REV_HOLD:     PKT_HOLD,
    _REV_REJECTED: PKT_REJECTED,
}

_HINT_MAP = {
    PKT_READY:    HINT_ALLOW,
    PKT_HOLD:     HINT_HOLD,
    PKT_REJECTED: HINT_REJECT,
}


# ---------------------------------------------------------------------------
# Core review packet function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_review_packet(
    promotion_gate:          object,
    paper_readiness_dossier: object,
    human_review_summary:    object,
) -> dict:
    """
    Bundle AC-94 and AC-95 outputs into one unified review packet.

    No new decision logic. Status and findings taken directly from
    human_review_summary (leading). Snapshots from all three layers.
    Fail-closed: invalid/missing input → REJECTED packet.

    Args:
        promotion_gate:          dict from build_promotion_gate() (AC-94).
        paper_readiness_dossier: dict from build_readiness_dossier() (AC-95).
        human_review_summary:    dict from build_human_review_summary() (AC-95).

    Returns:
        Review packet dict.
    """
    ts = _utc_ts()

    # Validate inputs
    if not isinstance(human_review_summary, dict):
        return _rejected_packet(ts, "human_review_summary is not a dict")
    if not isinstance(paper_readiness_dossier, dict):
        return _rejected_packet(ts, "paper_readiness_dossier is not a dict")
    if "review_status" not in human_review_summary:
        return _rejected_packet(ts, "human_review_summary missing review_status")
    if "dossier_status" not in paper_readiness_dossier:
        return _rejected_packet(ts, "paper_readiness_dossier missing dossier_status")

    # Status mapping — human_review_summary is leading
    rev_status = str(human_review_summary.get("review_status", ""))
    pkt_status = _STATUS_MAP.get(rev_status, PKT_REJECTED)
    hint       = _HINT_MAP.get(pkt_status, HINT_REJECT)

    # Decision fields — taken directly, no reinterpretation
    decision = {
        "decision_hint": hint,
        "priority":      str(human_review_summary.get("review_priority", "")),
        "reason":        str(human_review_summary.get("review_reason", "")),
        "reason_code":   str(human_review_summary.get("review_reason_code", "")),
    }

    # Findings — taken directly
    findings = {
        "key_findings":      list(human_review_summary.get("key_findings", [])),
        "blocking_findings": list(human_review_summary.get("blocking_findings", [])),
    }

    # Summary — top-level status from each layer
    promo_status  = ""
    if isinstance(promotion_gate, dict):
        promo_status = str(promotion_gate.get("promotion_status", ""))

    summary = {
        "promotion_status": promo_status,
        "dossier_status":   str(paper_readiness_dossier.get("dossier_status", "")),
        "review_status":    rev_status,
    }

    # Snapshots — compact copies of key fields from each layer
    snapshots = {
        "promotion": _promo_snapshot(promotion_gate),
        "dossier":   _dossier_snapshot(paper_readiness_dossier),
        "review":    _review_snapshot(human_review_summary),
    }

    return {
        "version":               VERSION,
        "component":             COMPONENT,
        "ts_utc":                ts,
        "review_packet_status":  pkt_status,
        "review_packet_mode":    PKT_MODE,
        "decision":              decision,
        "findings":              findings,
        "summary":               summary,
        "snapshots":             snapshots,
        "flags": {
            "non_binding":           True,
            "simulation_only":       True,
            "paper_only":            True,
            "live_activation_allowed": False,
        },
    }


# ---------------------------------------------------------------------------
# Snapshot helpers (compact, non-mutating)
# ---------------------------------------------------------------------------

def _promo_snapshot(gate: object) -> dict:
    if not isinstance(gate, dict):
        return {}
    return {
        "promotion_status":      str(gate.get("promotion_status", "")),
        "promotion_ready":       bool(gate.get("promotion_ready", False)),
        "paper_ready_candidate": bool(gate.get("paper_ready_candidate", False)),
        "promotion_reason_code": str(gate.get("promotion_reason_code", "")),
    }


def _dossier_snapshot(dossier: object) -> dict:
    if not isinstance(dossier, dict):
        return {}
    counts = dossier.get("readiness_counts", {})
    return {
        "dossier_status":          str(dossier.get("dossier_status", "")),
        "dossier_ready_for_review": bool(dossier.get("dossier_ready_for_review", False)),
        "dossier_reason_code":     str(dossier.get("dossier_reason_code", "")),
        "readiness_counts":        dict(counts) if isinstance(counts, dict) else {},
    }


def _review_snapshot(review: object) -> dict:
    if not isinstance(review, dict):
        return {}
    return {
        "review_status":        str(review.get("review_status", "")),
        "review_mode":          str(review.get("review_mode", "")),
        "review_priority":      str(review.get("review_priority", "")),
        "review_reason_code":   str(review.get("review_reason_code", "")),
        "review_decision_hint": str(review.get("review_decision_hint", "")),
    }


# ---------------------------------------------------------------------------
# Fail-closed helper
# ---------------------------------------------------------------------------

def _rejected_packet(ts: str, reason: str) -> dict:
    return {
        "version":               VERSION,
        "component":             COMPONENT,
        "ts_utc":                ts,
        "review_packet_status":  PKT_REJECTED,
        "review_packet_mode":    PKT_MODE,
        "decision": {
            "decision_hint": HINT_REJECT,
            "priority":      "HIGH",
            "reason":        reason,
            "reason_code":   "REVIEW_PACKET_INVALID_INPUT",
        },
        "findings": {
            "key_findings":      [reason],
            "blocking_findings": [reason],
        },
        "summary": {
            "promotion_status": "",
            "dossier_status":   "",
            "review_status":    "",
        },
        "snapshots": {
            "promotion": {},
            "dossier":   {},
            "review":    {},
        },
        "flags": {
            "non_binding":           True,
            "simulation_only":       True,
            "paper_only":            True,
            "live_activation_allowed": False,
        },
    }


def _utc_ts() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# File I/O (separated from pure core)
# ---------------------------------------------------------------------------

def write_review_packet(packet: dict, path: Path = REVIEW_PACKET_PATH) -> None:
    """Write review packet to JSON file. Creates parent dirs if needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(packet, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_dossier_module():
    path = Path(__file__).parent / "build_readiness_dossier_lite.py"
    spec = importlib.util.spec_from_file_location("_dossier", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-95 + AC-96
# ---------------------------------------------------------------------------

def build_review_packet_from_specs(
    market_specs:             object,
    total_equity_eur:         float,
    market_regimes:           object = None,
    market_capital_fractions: object = None,
    write_output:             bool   = False,
) -> dict:
    """
    Full chain: market_specs → … → readiness dossier + review (AC-95)
                             → review packet (AC-96).

    Returns dict with all pipeline keys plus review_packet.
    Optionally writes review_packet.json when write_output=True.
    live_activation_allowed=False always.
    """
    _dossier_mod = _load_dossier_module()
    pipeline     = _dossier_mod.build_dossier_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    packet = build_review_packet(
        pipeline["promotion_gate"],
        pipeline["paper_readiness_dossier"],
        pipeline["human_review_summary"],
    )
    if write_output:
        write_review_packet(packet)

    return {
        "splits_result":            pipeline["splits_result"],
        "capital_allocation":       pipeline["capital_allocation"],
        "allocation_envelope":      pipeline["allocation_envelope"],
        "regime_overlay":           pipeline["regime_overlay"],
        "allocation_proposal":      pipeline["allocation_proposal"],
        "conflict_selection":       pipeline["conflict_selection"],
        "allocation_candidate":     pipeline["allocation_candidate"],
        "paper_transition_preview": pipeline["paper_transition_preview"],
        "intent_pack":              pipeline["intent_pack"],
        "transition_audit":         pipeline["transition_audit"],
        "queen_handoff":            pipeline["queen_handoff"],
        "runner_intake":            pipeline["runner_intake"],
        "dry_run_consumption":      pipeline["dry_run_consumption"],
        "execution_ledger":         pipeline["execution_ledger"],
        "audit_trace":              pipeline["audit_trace"],
        "replay_validation":        pipeline["replay_validation"],
        "handoff_consistency":      pipeline["handoff_consistency"],
        "promotion_gate":           pipeline["promotion_gate"],
        "paper_readiness_dossier":  pipeline["paper_readiness_dossier"],
        "human_review_summary":     pipeline["human_review_summary"],
        "review_packet":            packet,
    }


# ---------------------------------------------------------------------------
# Optional main (CLI demo + file write)
# ---------------------------------------------------------------------------

def main() -> None:
    specs = [
        {
            "market": "BTC-EUR",
            "strategies": [
                {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION", "weight_fraction": 0.6},
                {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT",        "weight_fraction": 0.4},
            ],
        },
        {
            "market": "ETH-EUR",
            "strategies": [
                {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION"},
            ],
        },
    ]
    regimes = {
        "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
        "ETH-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
    }

    result = build_review_packet_from_specs(
        specs, total_equity_eur=10_000.0, market_regimes=regimes, write_output=True
    )
    print(json.dumps(result["review_packet"], indent=2))


if __name__ == "__main__":
    main()
