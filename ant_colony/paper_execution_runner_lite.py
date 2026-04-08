import json
from pathlib import Path
from datetime import datetime, timezone

OUT_DIR = Path(r"C:\Trading\ANT_OUT")

EXECUTION_SUMMARY_PATH = OUT_DIR / "execution_summary.json"
POSITIONS_PATH = OUT_DIR / "paper_positions.json"
PORTFOLIO_STATE_PATH = OUT_DIR / "paper_portfolio_state.json"
EXECUTION_LOG_PATH = OUT_DIR / "paper_execution_log.jsonl"
EXECUTED_IDS_PATH = OUT_DIR / "paper_executed_ids.json"
METRICS_PATH = OUT_DIR / "paper_execution_metrics.json"
SUMMARY_PATH = OUT_DIR / "paper_execution_summary.json"
MARKET_DATA_PATH = OUT_DIR / "worker_market_data.json"


def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def save_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def append_jsonl(path: Path, obj):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")


def to_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)


def main():
    ts = utc_now_ts()

    execution_summary = load_json(EXECUTION_SUMMARY_PATH, {})
    positions = load_json(POSITIONS_PATH, {})
    portfolio = load_json(PORTFOLIO_STATE_PATH, {
        "equity": 10000.0,
        "cash": 10000.0,
        "positions": {}
    })
    executed_ids = load_json(EXECUTED_IDS_PATH, [])
    market_data = load_json(MARKET_DATA_PATH, {})

    if not isinstance(positions, dict):
        positions = {}
    if not isinstance(portfolio, dict):
        portfolio = {"equity": 10000.0, "cash": 10000.0, "positions": {}}
    if not isinstance(portfolio.get("positions"), dict):
        portfolio["positions"] = {}
    if not isinstance(executed_ids, list):
        executed_ids = []

    markets = (execution_summary.get("markets") or {})
    cash = to_float(portfolio.get("cash", 10000.0), 10000.0)

    intents_processed = 0
    intents_skipped = 0
    executed_now = 0
    budget_limited_count = 0

    # === FASE 1: Verzamel ENTER_LONG kandidaten ===
    # EXIT_LONG wordt apart verwerkt (geen budget-impact, geeft cash terug).
    enter_candidates = []
    exit_candidates = []

    for market, market_row in sorted(markets.items()):
        for strategy, row in (market_row.get("strategies") or {}).items():
            intents_processed += 1
            allowed = bool(row.get("allowed", False))
            action = str(row.get("action", "NO_ACTION") or "NO_ACTION").upper()
            intent_file = row.get("intent_file")

            if not allowed or not intent_file:
                intents_skipped += 1
                continue

            intent = load_json(Path(intent_file), {})
            decision_id = str(intent.get("decision_id", ""))

            if not decision_id or decision_id in executed_ids:
                intents_skipped += 1
                continue

            intent_strategy = str(intent.get("strategy", "NONE") or "NONE").upper()
            position_key = f"{market}__{intent_strategy}"

            if action == "ENTER_LONG":
                requested_notional_eur = to_float(intent.get("requested_notional_eur", 0.0))
                if requested_notional_eur <= 0.0:
                    intents_skipped += 1
                    continue
                enter_candidates.append({
                    "market": market,
                    "strategy": intent_strategy,
                    "position_key": position_key,
                    "decision_id": decision_id,
                    "intent": intent,
                    "row": row,
                    "requested_notional_eur": requested_notional_eur,
                })
            elif action == "EXIT_LONG":
                exit_candidates.append({
                    "market": market,
                    "strategy": intent_strategy,
                    "position_key": position_key,
                    "decision_id": decision_id,
                    "intent": intent,
                    "row": row,
                })

    # === FASE 2: Bepaal granted_notional_eur per kandidaat ===
    # Proportionele scaling als totaal > beschikbare cash.
    total_requested_eur = round(sum(c["requested_notional_eur"] for c in enter_candidates), 2)
    if total_requested_eur > 0 and total_requested_eur > cash:
        scale = cash / total_requested_eur
        budget_constrained = True
    else:
        scale = 1.0
        budget_constrained = False

    total_granted_eur = 0.0

    for c in enter_candidates:
        granted = round(c["requested_notional_eur"] * scale, 2)
        c["granted_notional_eur"] = granted
        c["skipped_due_to_budget"] = budget_constrained and granted < c["requested_notional_eur"]
        c["budget_shortfall_eur"] = round(total_requested_eur - cash, 2) if budget_constrained else 0.0

    # === FASE 3a: Verwerk EXIT_LONG (geeft cash terug, geen budget nodig) ===
    for c in exit_candidates:
        market = c["market"]
        position_key = c["position_key"]
        decision_id = c["decision_id"]
        intent = c["intent"]
        reason = c["row"].get("reason")

        pos = positions.get(position_key) or portfolio["positions"].get(position_key) or {}
        if str(pos.get("position", "FLAT")).upper() != "LONG":
            intents_skipped += 1
            continue

        price = to_float(
            (market_data.get("markets", {}).get(market, {}) or {}).get("last_price"), 0.0
        )
        size = to_float(pos.get("size", 0.0), 0.0)
        entry_price = to_float(pos.get("entry_price", 0.0), 0.0)

        if price <= 0.0 or size <= 0.0:
            intents_skipped += 1
            continue

        notional_back = round(size * price, 2)
        realized_pnl = round(size * (price - entry_price), 2)
        cash = round(cash + notional_back, 2)

        append_jsonl(EXECUTION_LOG_PATH, {
            "ts_utc": ts,
            "market": market,
            "action": "EXIT_LONG",
            "decision_id": decision_id,
            "price": price,
            "size": size,
            "notional_eur": notional_back,
            "realized_pnl": realized_pnl,
            "reason": reason,
        })

        positions[position_key] = {
            "position": "FLAT",
            "size": 0.0,
            "entry_price": 0.0,
            "entry_ts": None,
            "decision_id": decision_id,
            "execution_id": decision_id.replace("_EXIT_LONG", ""),
            "notional_eur": 0.0,
            "exit_price": price,
            "exit_ts": ts,
            "realized_pnl": realized_pnl,
        }
        portfolio["positions"][position_key] = positions[position_key]
        executed_ids.append(decision_id)
        executed_now += 1

    # === FASE 3b: Verwerk ENTER_LONG op basis van granted_notional_eur ===
    for c in enter_candidates:
        market = c["market"]
        position_key = c["position_key"]
        decision_id = c["decision_id"]
        intent = c["intent"]
        reason = c["row"].get("reason")
        size_mult = to_float(intent.get("size_mult", 1.0), 1.0)
        granted = c["granted_notional_eur"]

        # AC-41: fail-closed — geen execution zonder granted_notional_eur > 0
        if granted <= 0.0:
            intents_skipped += 1
            if c["skipped_due_to_budget"]:
                budget_limited_count += 1
            continue

        price = to_float(
            (market_data.get("markets", {}).get(market, {}) or {}).get("last_price"), 0.0
        )
        if price <= 0.0:
            intents_skipped += 1
            continue

        # Dubbele check: genoeg cash (na EXIT_LONGs kan cash gestegen zijn)
        if cash < granted:
            intents_skipped += 1
            budget_limited_count += 1
            continue

        size = round(granted / price, 10)

        positions[position_key] = {
            "position": "LONG",
            "size": size,
            "entry_price": price,
            "entry_ts": ts,
            "decision_id": decision_id,
            "execution_id": decision_id.replace("_ENTER_LONG", ""),
            "notional_eur": granted,
        }
        portfolio["positions"][position_key] = positions[position_key]
        cash = round(cash - granted, 2)
        total_granted_eur = round(total_granted_eur + granted, 2)

        append_jsonl(EXECUTION_LOG_PATH, {
            "ts_utc": ts,
            "market": market,
            "action": "ENTER_LONG",
            "decision_id": decision_id,
            "price": price,
            "size": size,
            "requested_notional_eur": c["requested_notional_eur"],
            "granted_notional_eur": granted,
            "skipped_due_to_budget": c["skipped_due_to_budget"],
            "budget_shortfall_eur": c["budget_shortfall_eur"],
            "reason": reason,
        })

        executed_ids.append(decision_id)
        executed_now += 1

    portfolio["cash"] = round(cash, 2)
    portfolio["position_count"] = sum(
        1 for _, p in positions.items()
        if str((p or {}).get("position", "FLAT")).upper() == "LONG"
    )
    position_count = portfolio["position_count"]

    save_json(POSITIONS_PATH, positions)
    save_json(PORTFOLIO_STATE_PATH, portfolio)
    save_json(EXECUTED_IDS_PATH, executed_ids)

    metrics = {
        "component": "paper_execution_metrics",
        "ts": ts,
        "equity": portfolio.get("equity", 0.0),
        "cash": portfolio.get("cash", 0.0),
        "position_count": position_count,
        "markets": [m for m, p in positions.items() if str((p or {}).get("position", "FLAT")).upper() == "LONG"],
        "executed_now": executed_now,
    }
    save_json(METRICS_PATH, metrics)

    summary = {
        "component": "paper_execution_runner_lite",
        "ts": ts,
        "intents_processed": intents_processed,
        "intents_skipped": intents_skipped,
        "executed_now": executed_now,
        "total_requested_eur": total_requested_eur,
        "total_granted_eur": total_granted_eur,
        "budget_limited_count": budget_limited_count,
        "budget_constrained": budget_constrained,
        "log_file_exists": EXECUTION_LOG_PATH.exists(),
        "executed_ids_count": len(executed_ids),
        "position_count": position_count,
    }
    save_json(SUMMARY_PATH, summary)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
