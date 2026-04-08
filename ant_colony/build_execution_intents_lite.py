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
STRATEGY_FEEDBACK_STATE_PATH = OUT_DIR / "strategy_feedback_state.json"

# AC-40: enabled strategieën per markt
ENABLED_STRATEGIES = ["EDGE3", "EDGE4"]

# AC44: allocation bias parameters (expliciet, aanpasbaar)
ALLOCATION_WEIGHT_MIN = 0.25
ALLOCATION_WEIGHT_MAX = 1.75
ALLOCATION_SCORE_BIAS = 0.25
ALLOCATION_WINRATE_BIAS = 0.25
ALLOCATION_MIN_CLOSED_FOR_WINRATE = 3


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
    EDGE4 krijgt FAVOR als EDGE3 geblokkeerd is maar health OK.
    """
    if strategy == "EDGE4" and edge3_gate != "ALLOW" and health_gate == "ALLOW":
        return {"router_bias": "FAVOR", "router_bias_reason": "EDGE4_ACTIVE_WHEN_EDGE3_BLOCKED"}
    return {"router_bias": "NEUTRAL", "router_bias_reason": "NO_BIAS"}


# === AC44: feedback-aware allocation helpers ===

def load_strategy_feedback_state() -> dict:
    """AC44: laad strategy_keys uit strategy_feedback_state.json. Leeg dict bij ontbrekend bestand."""
    obj, err = load_json(STRATEGY_FEEDBACK_STATE_PATH)
    if err or not isinstance(obj, dict):
        return {}
    keys = obj.get("strategy_keys") or {}
    return keys if isinstance(keys, dict) else {}


def get_feedback_for_key(feedback_keys: dict, position_key: str) -> dict:
    """AC44: haal feedback entry op voor position_key, of lege dict als niet gevonden."""
    return feedback_keys.get(position_key) or {}


def derive_allocation_weight(fb: dict) -> tuple:
    """
    AC44: bepaal raw en geclamped allocation weight uit feedback state.
    Formule:
      base = 1.0
      score > 0 → +0.25 (POSITIVE_SCORE)
      score < 0 → -0.25 (NEGATIVE_SCORE)
      closed >= 3 en wins > losses → +0.25 (WINRATE_ADVANTAGE)
      closed >= 3 en losses > wins → -0.25 (LOSSRATE_PENALTY)
      clamp naar [0.25, 1.75]
    Fallback bij lege fb: (1.0, 1.0, "NO_FEEDBACK_STATE")
    """
    if not fb:
        return 1.0, 1.0, "NO_FEEDBACK_STATE"

    score = to_float(fb.get("score", 0.0))
    closed_count = int(fb.get("closed_trade_count", 0) or 0)
    win_count = int(fb.get("win_count", 0) or 0)
    loss_count = int(fb.get("loss_count", 0) or 0)

    raw = 1.0
    reasons = []

    if score > 0:
        raw += ALLOCATION_SCORE_BIAS
        reasons.append("POSITIVE_SCORE")
    elif score < 0:
        raw -= ALLOCATION_SCORE_BIAS
        reasons.append("NEGATIVE_SCORE")

    if closed_count >= ALLOCATION_MIN_CLOSED_FOR_WINRATE:
        if win_count > loss_count:
            raw += ALLOCATION_WINRATE_BIAS
            reasons.append("WINRATE_ADVANTAGE")
        elif loss_count > win_count:
            raw -= ALLOCATION_WINRATE_BIAS
            reasons.append("LOSSRATE_PENALTY")

    clamped = max(ALLOCATION_WEIGHT_MIN, min(ALLOCATION_WEIGHT_MAX, raw))

    if not reasons:
        reasons.append("NEUTRAL_STATE")

    return round(raw, 4), round(clamped, 4), "+".join(reasons)


def normalize_market_allocations(market: str, strategy_eval: dict, feedback_keys: dict) -> tuple:
    """
    AC44: normaliseer allocatie per markt over actieve strategieën.
    Actief = effective_action == ENTER_LONG.
    Geeft (alloc_map, active_strategies_list) terug.
    alloc_map: {strategy -> allocation fields + feedback observability}
    """
    active = [s for s in strategy_eval if strategy_eval[s]["effective_action"] == "ENTER_LONG"]
    alloc_map = {}

    # Bereken gewichten voor actieve strategieën
    weights = {}
    for s in active:
        position_key = f"{market}__{s}"
        fb = get_feedback_for_key(feedback_keys, position_key)
        raw, clamped, bias_reason = derive_allocation_weight(fb)
        weights[s] = clamped
        alloc_map[s] = {
            "feedback_state_found": bool(fb),
            "feedback_score": to_float(fb.get("score", 0.0)) if fb else None,
            "feedback_trade_count": int(fb.get("trade_count", 0) or 0) if fb else 0,
            "feedback_closed_trade_count": int(fb.get("closed_trade_count", 0) or 0) if fb else 0,
            "feedback_win_count": int(fb.get("win_count", 0) or 0) if fb else 0,
            "feedback_loss_count": int(fb.get("loss_count", 0) or 0) if fb else 0,
            "allocation_weight_raw": raw,
            "allocation_weight_clamped": clamped,
            "allocation_bias_reason": bias_reason,
            "allocation_pct": 0.0,
        }

    # Normaliseer over actieve strategieën (som = 1.0)
    total_weight = sum(weights.values())
    for s in active:
        alloc_map[s]["allocation_pct"] = (
            round(weights[s] / total_weight, 6) if total_weight > 0 else 0.0
        )

    # Inactieve strategieën: 0 allocatie, maar feedback-state wel zichtbaar
    for s in strategy_eval:
        if s in alloc_map:
            continue
        position_key = f"{market}__{s}"
        fb = get_feedback_for_key(feedback_keys, position_key)
        alloc_map[s] = {
            "feedback_state_found": bool(fb),
            "feedback_score": to_float(fb.get("score", 0.0)) if fb else None,
            "feedback_trade_count": int(fb.get("trade_count", 0) or 0) if fb else 0,
            "feedback_closed_trade_count": int(fb.get("closed_trade_count", 0) or 0) if fb else 0,
            "feedback_win_count": int(fb.get("win_count", 0) or 0) if fb else 0,
            "feedback_loss_count": int(fb.get("loss_count", 0) or 0) if fb else 0,
            "allocation_weight_raw": 0.0,
            "allocation_weight_clamped": 0.0,
            "allocation_bias_reason": "NO_SIGNAL_OR_BLOCKED",
            "allocation_pct": 0.0,
        }

    return alloc_map, active


def main():
    combined, combined_err = load_json(COMBINED_STATUS_PATH)
    portfolio_summary = load_portfolio_summary()
    portfolio_cash = load_portfolio_state_cash()
    worker_selection_map, worker_selection_err = load_worker_selection_map()
    feedback_keys = load_strategy_feedback_state()  # AC44
    now_ts = utc_now_ts()
    test_override = load_test_override()

    if combined_err or not isinstance(combined, dict):
        summary = {
            "version": "execution_summary_v11",
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

        # === STAP 1: evalueer signalen + guards per strategie ===
        strategy_eval = {}
        for strategy in ENABLED_STRATEGIES:
            sig = eval_strategy_signal(strategy, edge3_gate, health_gate)
            bias_info = derive_router_bias(strategy, edge3_gate, health_gate)
            action = sig["signal_action"]

            readiness = dict(readiness_base)
            guard_blockers = list(base_guard_blockers)
            allowed = base_allowed
            reason = base_reason

            # AC39: EDGE4 niet geblokkeerd door EDGE3-gate
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

            if not allowed:
                action = "NO_ACTION"

            effective_action = action
            effective_reason = safe_str(reason, "ALLOW")
            signal_overridden = (
                override_applied
                or freshness_block
                or (not allowed and sig["signal_action"] == "ENTER_LONG")
            )

            if effective_reason not in ("ALLOW", ""):
                primary_block_reason = effective_reason
            elif guard_blockers:
                primary_block_reason = guard_blockers[0]
            else:
                primary_block_reason = "ALLOW"

            strategy_eval[strategy] = {
                "sig": sig,
                "bias_info": bias_info,
                "allowed": allowed,
                "effective_action": effective_action,
                "effective_reason": effective_reason,
                "signal_overridden": signal_overridden,
                "primary_block_reason": primary_block_reason,
                "guard_blockers": guard_blockers,
                "readiness": readiness,
                "override_applied": override_applied,
            }

        # === STAP 2: AC44 feedback-aware allocatie normalisatie per markt ===
        alloc_map, active_strategies = normalize_market_allocations(
            market, strategy_eval, feedback_keys
        )

        # === STAP 3: bouw intents en strategy_results ===
        strategy_results = {}

        for strategy in ENABLED_STRATEGIES:
            ev = strategy_eval[strategy]
            alloc = alloc_map[strategy]
            sig = ev["sig"]
            bias_info = ev["bias_info"]
            effective_action = ev["effective_action"]
            effective_reason = ev["effective_reason"]
            allowed = ev["allowed"]

            allocation_pct = alloc["allocation_pct"]
            allocation_bias_reason = alloc["allocation_bias_reason"]
            allocation_reason = allocation_bias_reason if effective_action == "ENTER_LONG" else "NO_SIGNAL_OR_BLOCKED"

            position_key = f"{market}__{strategy}"
            decision_id = f"{position_key}_{safe_str(cycle_id, 'NO_CYCLE')}_{effective_action}"

            requested_notional_eur = round(base_notional * size_mult * allocation_pct, 2)
            total_requested_eur = round(total_requested_eur + requested_notional_eur, 2)

            intent = {
                "version": "execution_intent_v9",
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
                "signal_overridden": ev["signal_overridden"],
                "size_mult": size_mult,
                "feedback_state_found": alloc["feedback_state_found"],
                "feedback_score": alloc["feedback_score"],
                "feedback_trade_count": alloc["feedback_trade_count"],
                "feedback_closed_trade_count": alloc["feedback_closed_trade_count"],
                "feedback_win_count": alloc["feedback_win_count"],
                "feedback_loss_count": alloc["feedback_loss_count"],
                "allocation_weight_raw": alloc["allocation_weight_raw"],
                "allocation_weight_clamped": alloc["allocation_weight_clamped"],
                "allocation_pct": allocation_pct,
                "allocation_bias_reason": allocation_bias_reason,
                "allocation_reason": allocation_reason,
                "requested_notional_eur": requested_notional_eur,
                "edge3_gate": edge3_gate or None,
                "health_gate": health_gate or None,
                "execution_allowed": allowed,
                "block_reason": effective_reason,
                "primary_block_reason": ev["primary_block_reason"],
                "guard_blockers": ev["guard_blockers"],
                "execution_readiness": ev["readiness"],
                "test_override_applied": ev["override_applied"],
                "source_files": {
                    "combined_status": str(COMBINED_STATUS_PATH),
                    "worker_strategy_selection": str(WORKER_SELECTION_PATH),
                    "test_override": str(TEST_OVERRIDE_PATH),
                    "portfolio_summary": str(PORTFOLIO_SUMMARY_PATH),
                    "strategy_feedback_state": str(STRATEGY_FEEDBACK_STATE_PATH),
                },
                "source_meta": {
                    "combined_status_version": combined_version,
                    "combined_status_ts_utc": combined_ts_utc,
                    "combined_status_cycle_id": cycle_id,
                    "portfolio_valuation_state": safe_str(portfolio_summary.get("valuation_state")),
                    "portfolio_all_prices_fresh": bool(portfolio_summary.get("all_prices_fresh", False)),
                    "worker_selection_error": worker_selection_err,
                    "feedback_keys_loaded": len(feedback_keys),
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
                "signal_overridden": ev["signal_overridden"],
                "feedback_state_found": alloc["feedback_state_found"],
                "feedback_score": alloc["feedback_score"],
                "feedback_closed_trade_count": alloc["feedback_closed_trade_count"],
                "feedback_win_count": alloc["feedback_win_count"],
                "feedback_loss_count": alloc["feedback_loss_count"],
                "allocation_weight_raw": alloc["allocation_weight_raw"],
                "allocation_weight_clamped": alloc["allocation_weight_clamped"],
                "allocation_pct": allocation_pct,
                "allocation_bias_reason": allocation_bias_reason,
                "allocation_reason": allocation_reason,
                "requested_notional_eur": requested_notional_eur,
                "test_override_applied": ev["override_applied"],
                "intent_file": str(out_path),
            }

            if allowed and effective_action == "ENTER_LONG":
                allowed_count += 1
            else:
                blocked_count += 1

            add_reason(reason_counts, effective_reason)
            print(f"WROTE {out_path}")

        summary_markets[market] = {
            "edge3_gate": edge3_gate or None,
            "health_gate": health_gate or None,
            "market_active_strategies": active_strategies,
            "market_allocation_total": round(sum(
                strategy_results.get(s, {}).get("allocation_pct", 0.0)
                for s in ENABLED_STRATEGIES
            ), 6),
            "market_allocation_mode": "FEEDBACK_BIASED",
            "strategies": strategy_results,
        }

    summary = {
        "version": "execution_summary_v11",
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
        "feedback_keys_loaded": len(feedback_keys),
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

    # === SIGNAL VISIBILITY (per strategie, inclusief allocation bias) ===
    try:
        visibility = {}
        for mkt in sorted(markets.keys()):
            m = markets.get(mkt, {}) or {}
            mkt_row = summary_markets.get(mkt, {})
            visibility[mkt] = {
                "edge3_gate": (m.get("edge3") or {}).get("gate"),
                "health_gate": (m.get("health") or {}).get("health_gate"),
                "readiness_allowed": (m.get("execution_readiness") or {}).get("allowed"),
                "readiness_reason": (m.get("execution_readiness") or {}).get("reason"),
                "market_active_strategies": mkt_row.get("market_active_strategies", []),
                "market_allocation_total": mkt_row.get("market_allocation_total"),
                "market_allocation_mode": mkt_row.get("market_allocation_mode"),
                "market_total_requested_eur": sum(
                    mkt_row.get("strategies", {}).get(s, {}).get("requested_notional_eur", 0.0)
                    for s in ENABLED_STRATEGIES
                ),
                "strategies": {
                    s: {
                        k: v for k, v in mkt_row.get("strategies", {}).get(s, {}).items()
                        if k != "intent_file"
                    }
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
