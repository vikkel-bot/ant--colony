import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")
COMBINED_STATUS_PATH = OUT_DIR / "combined_colony_status.json"
OUT_SUMMARY_PATH = OUT_DIR / "execution_summary.json"
TEST_OVERRIDE_PATH = OUT_DIR / "paper_execution_test_override.json"
PORTFOLIO_SUMMARY_PATH = OUT_DIR / "paper_portfolio_summary.json"
PORTFOLIO_STATE_PATH = OUT_DIR / "paper_portfolio_state.json"
WORKER_SELECTION_PATH = OUT_DIR / "worker_strategy_selection.json"

# AC-40: gate-as-signal per strategie
ENABLED_STRATEGIES = ["EDGE3", "EDGE4"]

# AC-41: expliciete per-strategie allocatieverdeling (hardcoded, zichtbaar)
STRATEGY_ALLOCATION_PCT = {
    "EDGE3": 0.5,
    "EDGE4": 0.5,
}


def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path):
    if not path.exists():
        return None, f"missing:{path.name}"

    encodings = ["utf-8-sig", "utf-8"]
    last_error = None

    for enc in encodings:
        try:
            text = path.read_text(encoding=enc)
            return json.loads(text), None
        except Exception as e:
            last_error = e

    return None, f"read_error:{path.name}:{last_error}"


def safe_str(value, default=""):
    if value is None:
        return default
    return str(value)


def to_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def add_reason(reason_counts, reason):
    key = safe_str(reason, "UNKNOWN")
    reason_counts[key] = int(reason_counts.get(key, 0)) + 1


def derive_guard_blockers(readiness: dict):
    blockers = []

    if not bool(readiness.get("global_execution_enabled", False)):
        blockers.append("GLOBAL_EXECUTION_DISABLED")

    if not bool(readiness.get("market_execution_enabled", False)):
        blockers.append("MARKET_EXECUTION_DISABLED")

    if not bool(readiness.get("freshness_ok", False)):
        blockers.append("STALE_STATUS")

    if bool(readiness.get("probe_enabled", False)) and not bool(readiness.get("probe_fresh", False)):
        blockers.append("PROBE_STALE")

    if safe_str(readiness.get("health_gate")) not in ("ALLOW", ""):
        blockers.append(f"HEALTH_{safe_str(readiness.get('health_gate'))}")

    if safe_str(readiness.get("edge3_gate")) not in ("ALLOW", ""):
        blockers.append(f"EDGE3_{safe_str(readiness.get('edge3_gate'))}")

    return blockers


def load_test_override():
    return {
        "enabled": False,
        "market": None,
        "action": "ENTER_LONG",
        "reason": "TEST_OVERRIDE_DISABLED"
    }


def load_portfolio_summary():
    obj, err = load_json(PORTFOLIO_SUMMARY_PATH)
    if err or not isinstance(obj, dict):
        return {}
    return obj


def load_portfolio_state_cash() -> float:
    """AC-41: laad beschikbare cash uit portfolio state voor requested_notional berekening."""
    obj, err = load_json(PORTFOLIO_STATE_PATH)
    if err or not isinstance(obj, dict):
        return 10000.0
    try:
        return float(obj.get("cash", 10000.0))
    except Exception:
        return 10000.0


def load_worker_selection_map():
    obj, err = load_json(WORKER_SELECTION_PATH)
    if err or not isinstance(obj, dict):
        return {}, err
    markets = obj.get("markets", {}) or {}
    if not isinstance(markets, dict):
        return {}, "invalid:worker_strategy_selection.markets"
    return markets, None


def freshness_block_active(portfolio_summary: dict):
    valuation_state = safe_str(portfolio_summary.get("valuation_state"))
    all_prices_fresh = bool(portfolio_summary.get("all_prices_fresh", False))
    return (valuation_state == "BLOCKED_FRESHNESS") or (not all_prices_fresh)


def eval_edge3_signal(edge3_gate: str, health_gate: str) -> dict:
    """AC42: EDGE3 signaal — bullish als edge3_gate én health_gate ALLOW."""
    if edge3_gate == "ALLOW" and health_gate == "ALLOW":
        return {"signal_action": "ENTER_LONG", "signal_bias": "BULLISH",
                "signal_reason": "EDGE3_GATES_CLEAR", "signal_strength": 1.0}
    if health_gate != "ALLOW":
        return {"signal_action": "NO_ACTION", "signal_bias": "NEUTRAL",
                "signal_reason": "EDGE3_HEALTH_BLOCKED", "signal_strength": 0.0}
    return {"signal_action": "NO_ACTION", "signal_bias": "NEUTRAL",
            "signal_reason": "EDGE3_GATE_BLOCKED", "signal_strength": 0.0}


def eval_edge4_signal(health_gate: str) -> dict:
    """AC42: EDGE4 signaal — bullish als health_gate ALLOW (edge3_gate irrelevant)."""
    if health_gate == "ALLOW":
        return {"signal_action": "ENTER_LONG", "signal_bias": "BULLISH",
                "signal_reason": "EDGE4_HEALTH_CLEAR", "signal_strength": 1.0}
    return {"signal_action": "NO_ACTION", "signal_bias": "NEUTRAL",
            "signal_reason": "EDGE4_HEALTH_BLOCKED", "signal_strength": 0.0}


def eval_strategy_signal(strategy: str, edge3_gate: str, health_gate: str) -> dict:
    """AC42: dispatcher naar per-strategie signal evaluator."""
    if strategy == "EDGE3":
        return eval_edge3_signal(edge3_gate, health_gate)
    if strategy == "EDGE4":
        return eval_edge4_signal(health_gate)
    return {"signal_action": "NO_ACTION", "signal_bias": "NEUTRAL",
            "signal_reason": "UNKNOWN_STRATEGY", "signal_strength": 0.0}


def derive_router_bias(strategy: str, edge3_gate: str, health_gate: str) -> dict:
    """
    AC42: router bias — expliciete voorkeur of suppressie per strategie.
    EDGE4 krijgt FAVOR als EDGE3 geblokkeerd is maar health OK:
    EDGE4 is dan het actieve alternatieve kanaal.
    """
    if strategy == "EDGE4" and edge3_gate != "ALLOW" and health_gate == "ALLOW":
        return {"router_bias": "FAVOR", "router_bias_reason": "EDGE4_ACTIVE_WHEN_EDGE3_BLOCKED"}
    return {"router_bias": "NEUTRAL", "router_bias_reason": "NO_BIAS"}


def main():
    combined, combined_err = load_json(COMBINED_STATUS_PATH)
    portfolio_summary = load_portfolio_summary()
    portfolio_cash = load_portfolio_state_cash()
    worker_selection_map, worker_selection_err = load_worker_selection_map()
    now_ts = utc_now_ts()
    test_override = load_test_override()

    if combined_err or not isinstance(combined, dict):
        summary = {
            "version": "execution_summary_v10",
            "ts_utc": now_ts,
            "source_component": "build_execution_intents_lite",
            "combined_status_ok": False,
            "combined_status_error": combined_err,
            "worker_selection_error": worker_selection_err,
            "markets_total": 0,
            "allowed_count": 0,
            "blocked_count": 0,
            "freshness_ok": False,
            "freshness_breakdown": {},
            "portfolio_valuation_state": safe_str(portfolio_summary.get("valuation_state")),
            "portfolio_all_prices_fresh": bool(portfolio_summary.get("all_prices_fresh", False)),
            "test_override": test_override,
            "reason_counts": {
                "MISSING_COMBINED_STATUS": 1 if combined_err and str(combined_err).startswith("missing:") else 0,
                "COMBINED_STATUS_ERROR": 1 if combined_err and not str(combined_err).startswith("missing:") else 0
            },
            "markets": {}
        }
        OUT_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"WROTE {OUT_SUMMARY_PATH}")
        return

    cycle_id = combined.get("cycle_id")
    combined_ts_utc = combined.get("ts_utc")
    combined_version = combined.get("version")
    markets = combined.get("markets", {}) or {}
    combined_freshness = combined.get("freshness", {}) or {}

    summary_markets = {}
    allowed_count = 0
    blocked_count = 0
    reason_counts = {}
    total_requested_eur = 0.0

    freshness_block = freshness_block_active(portfolio_summary)
    # AC-41: base_notional gebaseerd op huidige cash (gedeeld over alle markten/strategieën)
    base_notional = min(1000.0, portfolio_cash * 0.10)

    for market in sorted(markets.keys()):
        market_row = markets.get(market) or {}
        edge3 = market_row.get("edge3") or {}
        health = market_row.get("health") or {}
        readiness_base = dict(market_row.get("execution_readiness") or {})

        edge3_gate = safe_str(edge3.get("gate"), "")
        health_gate = safe_str(health.get("health_gate"), "")
        size_mult = to_float(health.get("health_size_mult", 1.0), 1.0)

        base_allowed = bool(readiness_base.get("allowed", False))
        base_reason = readiness_base.get("reason")
        base_guard_blockers = derive_guard_blockers(readiness_base)

        override_applied = bool(
            test_override["enabled"] and safe_str(test_override["market"]) == market
        )

        strategy_results = {}

        # AC42: loop over alle enabled strategieën per markt
        for strategy in ENABLED_STRATEGIES:
            # AC42: expliciete signal layer — bevroren na evaluatie, niet overschreven door guards
            sig = eval_strategy_signal(strategy, edge3_gate, health_gate)
            bias_info = derive_router_bias(strategy, edge3_gate, health_gate)

            # action start vanuit signal; guards kunnen het daarna naar NO_ACTION forceren
            action = sig["signal_action"]

            # Kopieer readiness en guard_blockers per strategie
            readiness = dict(readiness_base)
            guard_blockers = list(base_guard_blockers)
            allowed = base_allowed
            reason = base_reason

            # AC39: EDGE4 wordt niet geblokkeerd door EDGE3-gate
            if strategy == "EDGE4":
                guard_blockers = [b for b in guard_blockers if not str(b).startswith("EDGE3_")]
                if safe_str(reason) == "GATE_BLOCKED" and health_gate == "ALLOW":
                    allowed = True
                    reason = "ALLOW"
                    readiness["allowed"] = True
                    readiness["reason"] = "ALLOW"

            if override_applied:
                allowed = True
                action = test_override["action"]
                reason = test_override["reason"]
                readiness["allowed"] = True
                readiness["reason"] = test_override["reason"]
                # signal-velden NIET overschreven — override is een test-artefact

            if freshness_block:
                allowed = False
                action = "NO_ACTION"
                reason = "FRESHNESS_BLOCK"
                readiness["allowed"] = False
                readiness["reason"] = "FRESHNESS_BLOCK"

            # Geen ENTER_LONG zonder executie-toestemming
            if not allowed:
                action = "NO_ACTION"

            # AC42.1: effective fields — finale execution state, gescheiden van signal
            effective_action = action
            effective_reason = safe_str(reason, "ALLOW")
            signal_overridden = (
                override_applied
                or freshness_block
                or (not allowed and sig["signal_action"] == "ENTER_LONG")
            )

            # AC42.1: primary_block_reason volgt finale execution state, nooit UNKNOWN
            if effective_reason not in ("ALLOW", ""):
                primary_block_reason = effective_reason
            elif guard_blockers:
                primary_block_reason = guard_blockers[0]
            else:
                primary_block_reason = "ALLOW"

            position_key = f"{market}__{strategy}"
            decision_id = f"{position_key}_{safe_str(cycle_id, 'NO_CYCLE')}_{action}"

            # AC-41: expliciete allocatieberekening per strategie
            if allowed and action == "ENTER_LONG":
                allocation_pct = STRATEGY_ALLOCATION_PCT.get(strategy, 0.0)
                allocation_reason = "STRATEGY_ALLOCATION"
            else:
                allocation_pct = 0.0
                allocation_reason = "NO_SIGNAL_OR_BLOCKED"
            requested_notional_eur = round(base_notional * size_mult * allocation_pct, 2)
            total_requested_eur = round(total_requested_eur + requested_notional_eur, 2)

            intent = {
                "version": "execution_intent_v8",
                "ts_utc": now_ts,
                "source_component": "build_execution_intents_lite",
                "cycle_id": cycle_id,
                "market": market,
                "position_key": position_key,
                "decision_id": decision_id,
                "action": effective_action,
                "strategy": strategy,
                "bias": sig["signal_bias"],
                "signal_action": sig["signal_action"],
                "signal_bias": sig["signal_bias"],
                "signal_reason": sig["signal_reason"],
                "signal_strength": sig["signal_strength"],
                "router_bias": bias_info["router_bias"],
                "router_bias_reason": bias_info["router_bias_reason"],
                "effective_action": effective_action,
                "effective_reason": effective_reason,
                "signal_overridden": signal_overridden,
                "size_mult": size_mult,
                "allocation_pct": allocation_pct,
                "allocation_reason": allocation_reason,
                "requested_notional_eur": requested_notional_eur,
                "edge3_gate": edge3_gate or None,
                "health_gate": health_gate or None,
                "execution_allowed": allowed,
                "block_reason": reason,
                "primary_block_reason": primary_block_reason,
                "guard_blockers": guard_blockers,
                "execution_readiness": readiness,
                "test_override_applied": override_applied,
                "source_files": {
                    "combined_status": str(COMBINED_STATUS_PATH),
                    "worker_strategy_selection": str(WORKER_SELECTION_PATH),
                    "test_override": str(TEST_OVERRIDE_PATH),
                    "portfolio_summary": str(PORTFOLIO_SUMMARY_PATH),
                },
                "source_meta": {
                    "combined_status_version": combined_version,
                    "combined_status_ts_utc": combined_ts_utc,
                    "combined_status_cycle_id": cycle_id,
                    "portfolio_valuation_state": safe_str(portfolio_summary.get("valuation_state")),
                    "portfolio_all_prices_fresh": bool(portfolio_summary.get("all_prices_fresh", False)),
                    "worker_selection_error": worker_selection_err,
                },
            }

            out_path = OUT_DIR / f"{position_key}_execution_intent.json"
            out_path.write_text(json.dumps(intent, indent=2), encoding="utf-8")

            strategy_results[strategy] = {
                "allowed": allowed,
                "action": effective_action,
                "reason": effective_reason,
                "signal_action": sig["signal_action"],
                "signal_bias": sig["signal_bias"],
                "signal_reason": sig["signal_reason"],
                "signal_strength": sig["signal_strength"],
                "router_bias": bias_info["router_bias"],
                "router_bias_reason": bias_info["router_bias_reason"],
                "effective_action": effective_action,
                "effective_reason": effective_reason,
                "signal_overridden": signal_overridden,
                "allocation_pct": allocation_pct,
                "allocation_reason": allocation_reason,
                "requested_notional_eur": requested_notional_eur,
                "test_override_applied": override_applied,
                "intent_file": str(out_path),
            }

            if allowed and action == "ENTER_LONG":
                allowed_count += 1
            else:
                blocked_count += 1

            add_reason(reason_counts, reason)
            print(f"WROTE {out_path}")

        summary_markets[market] = {
            "edge3_gate": edge3_gate or None,
            "health_gate": health_gate or None,
            "strategies": strategy_results,
        }

    summary = {
        "version": "execution_summary_v10",
        "ts_utc": now_ts,
        "source_component": "build_execution_intents_lite",
        "combined_status_ok": True,
        "combined_status_error": None,
        "worker_selection_error": worker_selection_err,
        "markets_total": len(markets),
        "intents_total": len(markets) * len(ENABLED_STRATEGIES),
        "allowed_count": allowed_count,
        "blocked_count": blocked_count,
        "total_requested_eur": total_requested_eur,
        "portfolio_cash_snapshot": portfolio_cash,
        "freshness_ok": bool(combined_freshness.get("freshness_ok", False)),
        "freshness_breakdown": {
            "edge3_fresh": combined_freshness.get("edge3_fresh"),
            "edge4_fresh": combined_freshness.get("edge4_fresh"),
            "health_fresh": combined_freshness.get("health_fresh"),
            "execution_control_fresh": combined_freshness.get("execution_control_fresh"),
        },
        "portfolio_valuation_state": safe_str(portfolio_summary.get("valuation_state")),
        "portfolio_all_prices_fresh": bool(portfolio_summary.get("all_prices_fresh", False)),
        "test_override": test_override,
        "reason_counts": reason_counts,
        "markets": summary_markets,
    }

    # === AC40 SIGNAL VISIBILITY (MULTI-STRATEGY) ===
    try:
        visibility = {}
        for mkt in sorted(markets.keys()):
            m = markets.get(mkt, {}) or {}
            visibility[mkt] = {
                "edge3_gate": (m.get("edge3") or {}).get("gate"),
                "health_gate": (m.get("health") or {}).get("health_gate"),
                "readiness_allowed": (m.get("execution_readiness") or {}).get("allowed"),
                "readiness_reason": (m.get("execution_readiness") or {}).get("reason"),
                "strategies": {
                    s: {
                        k: v for k, v in summary_markets.get(mkt, {}).get("strategies", {}).get(s, {}).items()
                        if k != "intent_file"
                    }
                    for s in ENABLED_STRATEGIES
                },
                "market_total_requested_eur": sum(
                    summary_markets.get(mkt, {}).get("strategies", {}).get(s, {}).get("requested_notional_eur", 0.0)
                    for s in ENABLED_STRATEGIES
                ),
            }

        (OUT_DIR / "signal_visibility.json").write_text(
            json.dumps(visibility, indent=2),
            encoding="utf-8"
        )
    except Exception:
        pass

    OUT_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"WROTE {OUT_SUMMARY_PATH}")


if __name__ == "__main__":
    main()










