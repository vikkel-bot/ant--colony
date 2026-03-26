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

    if not isinstance(positions, dict):
        positions = {}
    if not isinstance(portfolio, dict):
        portfolio = {"equity": 10000.0, "cash": 10000.0, "positions": {}}
    if not isinstance(portfolio.get("positions"), dict):
        portfolio["positions"] = {}
    if not isinstance(executed_ids, list):
        executed_ids = []

    markets = (execution_summary.get("markets") or {})
    intents_processed = 0
    intents_allowed = 0
    intents_skipped = 0
    position_count = 0
    executed_now = 0

    cash = to_float(portfolio.get("cash", 10000.0), 10000.0)

    for market, row in sorted(markets.items()):
        intents_processed += 1
        allowed = bool(row.get("allowed", False))
        action = str(row.get("action", "NO_ACTION") or "NO_ACTION").upper()
        reason = row.get("reason")
        intent_file = row.get("intent_file")

        if not allowed:
            intents_skipped += 1
            continue

        if not intent_file:
            intents_skipped += 1
            continue

        intent = load_json(Path(intent_file), {})
        decision_id = str(intent.get("decision_id", ""))

        if not decision_id or decision_id in executed_ids:
            intents_skipped += 1
            continue

        if action == "ENTER_LONG":
            price = to_float(((load_json(OUT_DIR / "worker_market_data.json", {}).get("markets", {}).get(market, {}) or {}).get("last_price")), 0.0)
            size_mult = to_float(intent.get("size_mult", 1.0), 1.0)

            if price <= 0.0:
                intents_skipped += 1
                continue

            base_notional = min(1000.0, cash * 0.10)
            notional = round(base_notional * size_mult, 2)

            if notional <= 0.0 or cash < notional:
                intents_skipped += 1
                continue

            size = round(notional / price, 10)

            positions[market] = {
                "position": "LONG",
                "size": size,
                "entry_price": price,
                "entry_ts": ts,
                "decision_id": decision_id,
                "execution_id": decision_id.replace("_ENTER_LONG", ""),
                "notional_eur": notional
            }

            portfolio["positions"][market] = positions[market]
            cash = round(cash - notional, 2)

            append_jsonl(EXECUTION_LOG_PATH, {
                "ts_utc": ts,
                "market": market,
                "action": "ENTER_LONG",
                "decision_id": decision_id,
                "price": price,
                "size": size,
                "notional_eur": notional,
                "reason": reason
            })

            executed_ids.append(decision_id)
            executed_now += 1
            intents_allowed += 1

        elif action == "EXIT_LONG":
            pos = positions.get(market) or portfolio["positions"].get(market) or {}
            if str(pos.get("position", "FLAT")).upper() != "LONG":
                intents_skipped += 1
                continue

            price = to_float(((load_json(OUT_DIR / "worker_market_data.json", {}).get("markets", {}).get(market, {}) or {}).get("last_price")), 0.0)
            size = to_float(pos.get("size", 0.0), 0.0)
            entry_price = to_float(pos.get("entry_price", 0.0), 0.0)
            notional_back = round(size * price, 2)
            realized_pnl = round(size * (price - entry_price), 2)

            if price <= 0.0 or size <= 0.0:
                intents_skipped += 1
                continue

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
                "reason": reason
            })

            positions[market] = {
                "position": "FLAT",
                "size": 0.0,
                "entry_price": 0.0,
                "entry_ts": None,
                "decision_id": decision_id,
                "execution_id": decision_id.replace("_EXIT_LONG", ""),
                "notional_eur": 0.0,
                "exit_price": price,
                "exit_ts": ts,
                "realized_pnl": realized_pnl
            }

            portfolio["positions"][market] = positions[market]
            executed_ids.append(decision_id)
            executed_now += 1
            intents_allowed += 1

        else:
            intents_skipped += 1

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
        "executed_now": executed_now
    }
    save_json(METRICS_PATH, metrics)

    summary = {
        "component": "paper_execution_runner_lite",
        "ts": ts,
        "intents_processed": intents_processed,
        "intents_allowed": intents_allowed,
        "intents_skipped": intents_skipped,
        "log_file_exists": EXECUTION_LOG_PATH.exists(),
        "executed_ids_count": len(executed_ids),
        "position_count": position_count
    }
    save_json(SUMMARY_PATH, summary)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
