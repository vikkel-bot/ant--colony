import json
import json
import os
from datetime import datetime, timezone

ANT_OUT = r"C:\Trading\ANT_OUT"

sim_file = os.path.join(ANT_OUT, "worker_execution_simulator.json")
price_file = os.path.join(ANT_OUT, "worker_market_price_feed.json")
sizing_file = os.path.join(ANT_OUT, "worker_position_sizing.json")
apply_file = os.path.join(ANT_OUT, "worker_exit_apply_stub.json")
out_json = os.path.join(ANT_OUT, "worker_portfolio_state.json")
out_tsv  = os.path.join(ANT_OUT, "worker_portfolio_state.tsv")


def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


sim = load_json(sim_file)
price_doc = load_json(price_file)
sizing_doc = load_json(sizing_file)
apply_doc = load_json(apply_file)
existing = load_json(out_json)

sim_markets = sim.get("markets", {})
if not isinstance(sim_markets, dict):
    sim_markets = {}

price_rows = price_doc.get("markets", [])
price_by_market = {}
if isinstance(price_rows, list):
    for row in price_rows:
        if isinstance(row, dict):
            market = row.get("market")
            if market:
                price_by_market[market] = row

sizing_markets = sizing_doc.get("markets", {})
if not isinstance(sizing_markets, dict):
    sizing_markets = {}

apply_rows = apply_doc.get("markets", [])
apply_by_market = {}
if isinstance(apply_rows, list):
    for row in apply_rows:
        if isinstance(row, dict):
            market = row.get("market")
            if market:
                apply_by_market[market] = row

existing_rows = existing.get("markets", [])
existing_by_market = {}
if isinstance(existing_rows, list):
    for row in existing_rows:
        if isinstance(row, dict):
            market = row.get("market")
            if market:
                existing_by_market[market] = row

markets = {}

for market in sorted(sim_markets.keys()):

    sim_row = sim_markets.get(market, {}) or {}
    price_row = price_by_market.get(market, {}) or {}
    sizing_row = sizing_markets.get(market, {}) or {}
    apply_row = apply_by_market.get(market, {}) or {}
    prev = existing_by_market.get(market, {}) or {}

    enabled = bool(sim_row.get("sim_enabled", False))
    action = str(sim_row.get("sim_action", "NO_ACTION") or "NO_ACTION").upper()
    apply_action = str(apply_row.get("apply_action", "NO_APPLY") or "NO_APPLY").upper()

    position = str(prev.get("position", "FLAT") or "FLAT").upper()
    entry_price = to_float(prev.get("entry_price", 0.0), 0.0)
    qty = to_float(prev.get("qty", 0.0), 0.0)
    mark_price = to_float(price_row.get("market_price", 0.0), 0.0)
    target_qty = to_float(sizing_row.get("target_qty", 0.0), 0.0)
    pnl = 0.0
    equity = to_float(prev.get("equity", 10000.0), 10000.0)
    recently_closed = bool(prev.get("recently_closed", False))
    last_trade_state = str(prev.get("last_trade_state", "IDLE") or "IDLE").upper()

    if apply_action == "APPLY_EXIT_CLOSE":
        position = str(apply_row.get("next_position", "FLAT") or "FLAT").upper()
        entry_price = to_float(apply_row.get("next_entry_price", 0.0), 0.0)
        qty = to_float(apply_row.get("next_qty", 0.0), 0.0)
        pnl = 0.0
        equity = to_float(apply_row.get("equity", equity), equity)
        recently_closed = True
        last_trade_state = "CLOSED"

    elif position == "FLAT":
        if enabled and action == "OPEN_LONG_SIM":
            position = "LONG"
            entry_price = 100.0
            qty = target_qty if target_qty > 0 else 0.0
            recently_closed = False
            last_trade_state = "OPEN"
        elif enabled and action == "OPEN_SHORT_SIM":
            position = "SHORT"
            entry_price = 100.0
            qty = target_qty if target_qty > 0 else 0.0
            recently_closed = False
            last_trade_state = "OPEN"
        else:
            qty = 0.0
            if recently_closed:
                last_trade_state = "CLOSED"
            else:
                last_trade_state = "IDLE"

        if position == "LONG" and entry_price > 0 and mark_price > 0 and qty > 0:
            pnl = round((mark_price - entry_price) * qty, 4)
        elif position == "SHORT" and entry_price > 0 and mark_price > 0 and qty > 0:
            pnl = round((entry_price - mark_price) * qty, 4)
        else:
            pnl = 0.0

        if position in ("LONG", "SHORT"):
            equity = round(10000.0 + pnl, 4)

    else:
        if position == "LONG" and entry_price > 0 and mark_price > 0 and qty > 0:
            pnl = round((mark_price - entry_price) * qty, 4)
        elif position == "SHORT" and entry_price > 0 and mark_price > 0 and qty > 0:
            pnl = round((entry_price - mark_price) * qty, 4)
        else:
            pnl = 0.0

        equity = round(10000.0 + pnl, 4)
        recently_closed = False
        last_trade_state = "OPEN"

    markets[market] = {
        "market": market,
        "position": position,
        "entry_price": entry_price,
        "qty": qty,
        "mark_price": mark_price,
        "pnl": pnl,
        "equity": equity,
        "recently_closed": recently_closed,
        "last_trade_state": last_trade_state,
        "ts_utc": now_utc_iso()
    }

result = {
    "version": "worker_portfolio_simulator_lite_v9",
    "markets": [markets[m] for m in sorted(markets.keys())]
}

with open(out_json, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

with open(out_tsv, "w", encoding="utf-8") as f:
    f.write("market\tposition\tentry_price\tqty\tmark_price\tpnl\tequity\trecently_closed\tlast_trade_state\n")
    for row in result["markets"]:
        f.write(
            f'{row["market"]}\t{row["position"]}\t{row["entry_price"]}\t{row["qty"]}\t{row["mark_price"]}\t{row["pnl"]}\t{row["equity"]}\t{row["recently_closed"]}\t{row["last_trade_state"]}\n'
        )

print(json.dumps({
    "ok": True,
    "output_json": out_json,
    "output_tsv": out_tsv,
    "markets": len(result["markets"]),
    "version": result["version"]
}, indent=2))
