import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")
COMBINED_STATUS_PATH = OUT_DIR / "combined_colony_status.json"
OUT_SUMMARY_PATH = OUT_DIR / "execution_summary.json"
TEST_OVERRIDE_PATH = OUT_DIR / "paper_execution_test_override.json"
PORTFOLIO_SUMMARY_PATH = OUT_DIR / "paper_portfolio_summary.json"
WORKER_SELECTION_PATH = OUT_DIR / "worker_strategy_selection.json"

# AC-40 TEMP: gate-as-signal, vervangen in AC-41 door router bias
ENABLED_STRATEGIES = ["EDGE3", "EDGE4"]


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


def infer_strategy_signal(strategy: str, edge3_gate: str, health_gate: str):
    """
    AC-40 TEMP: gate-as-signal, vervangen in AC-41 door router bias.
    Bepaal per strategie of er een entry-signaal is op basis van gate-status.
    EDGE3: vereist zowel edge3_gate als health_gate ALLOW.
    EDGE4: vereist alleen health_gate ALLOW (ongeacht edge3_gate).
    """
    if strategy == "EDGE3":
        if edge3_gate == "ALLOW" and health_gate == "ALLOW":
            return "ENTER_LONG", "EDGE3_GATE_SIGNAL"
        return "NO_ACTION", "EDGE3_GATE_BLOCKED"
    if strategy == "EDGE4":
        if health_gate == "ALLOW":
            return "ENTER_LONG", "EDGE4_HEALTH_GATE_SIGNAL"
        return "NO_ACTION", "EDGE4_HEALTH_GATE_BLOCKED"
    return "NO_ACTION", "UNKNOWN_STRATEGY"


def main():
    combined, combined_err = load_json(COMBINED_STATUS_PATH)
    portfolio_summary = load_portfolio_summary()
    worker_selection_map, worker_selection_err = load_worker_selection_map()
    now_ts = utc_now_ts()
    test_override = load_test_override()

    if combined_err or not isinstance(combined, dict):
        summary = {
            "version": "execution_summary_v8",
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

    freshness_block = freshness_block_active(portfolio_summary)

    for market in sorted(markets.keys()):
        market_row = markets.get(market) or {}
        edge3 = market_row.get("edge3") or {}
        health = market_row.get("health") or {}
        readiness_base = dict(market_row.get("execution_readiness") or {})

        edge3_gate = safe_str(edge3.get("gate"), "")
        health_gate = safe_str(health.get("health_gate"), "")
        size_mult = health.get("health_size_mult", 1.0)

        base_allowed = bool(readiness_base.get("allowed", False))
        base_reason = readiness_base.get("reason")
        base_guard_blockers = derive_guard_blockers(readiness_base)

        override_applied = bool(
            test_override["enabled"] and safe_str(test_override["market"]) == market
        )

        strategy_results = {}

        # AC-40: loop over alle enabled strategieën per markt
        for strategy in ENABLED_STRATEGIES:
            # AC-40 TEMP: gate-as-signal, vervangen in AC-41 door router bias
            action, signal_reason = infer_strategy_signal(strategy, edge3_gate, health_gate)

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

            primary_block_reason = guard_blockers[0] if guard_blockers else safe_str(reason, "UNKNOWN")

            if override_applied:
                allowed = True
                action = test_override["action"]
                reason = test_override["reason"]
                readiness["allowed"] = True
                readiness["reason"] = test_override["reason"]
                signal_reason = test_override["reason"]

            if freshness_block:
                allowed = False
                action = "NO_ACTION"
                reason = "FRESHNESS_BLOCK"
                readiness["allowed"] = False
                readiness["reason"] = "FRESHNESS_BLOCK"

            # Geen ENTER_LONG zonder executie-toestemming
            if not allowed:
                action = "NO_ACTION"

            position_key = f"{market}__{strategy}"
            decision_id = f"{position_key}_{safe_str(cycle_id, 'NO_CYCLE')}_{action}"

            intent = {
                "version": "execution_intent_v6",
                "ts_utc": now_ts,
                "source_component": "build_execution_intents_lite",
                "cycle_id": cycle_id,
                "market": market,
                "position_key": position_key,
                "decision_id": decision_id,
                "action": action,
                "strategy": strategy,
                "bias": "LONG" if action == "ENTER_LONG" else "NEUTRAL",
                "size_mult": size_mult,
                "edge3_gate": edge3_gate or None,
                "health_gate": health_gate or None,
                "execution_allowed": allowed,
                "block_reason": reason,
                "primary_block_reason": primary_block_reason,
                "guard_blockers": guard_blockers,
                "signal_reason": signal_reason,
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
                "action": action,
                "reason": reason,
                "signal_reason": signal_reason,
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
        "version": "execution_summary_v9",
        "ts_utc": now_ts,
        "source_component": "build_execution_intents_lite",
        "combined_status_ok": True,
        "combined_status_error": None,
        "worker_selection_error": worker_selection_err,
        "markets_total": len(markets),
        "intents_total": len(markets) * len(ENABLED_STRATEGIES),
        "allowed_count": allowed_count,
        "blocked_count": blocked_count,
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
                    s: summary_markets.get(mkt, {}).get("strategies", {}).get(s, {})
                    for s in ENABLED_STRATEGIES
                },
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










