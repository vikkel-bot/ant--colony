import json
import os
from datetime import datetime, timezone

ANT_OUT = r"C:\Trading\ANT_OUT"

exit_file = os.path.join(ANT_OUT, "worker_exit_simulator.json")
portfolio_file = os.path.join(ANT_OUT, "worker_portfolio_state.json")

out_json = os.path.join(ANT_OUT, "worker_exit_apply_stub.json")
out_tsv  = os.path.join(ANT_OUT, "worker_exit_apply_stub.tsv")


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


exit_doc = load_json(exit_file)
portfolio_doc = load_json(portfolio_file)

exit_rows = exit_doc.get("markets", [])
portfolio_rows = portfolio_doc.get("markets", [])

if not isinstance(exit_rows, list):
    exit_rows = []
if not isinstance(portfolio_rows, list):
    portfolio_rows = []

portfolio_by_market = {}
for row in portfolio_rows:
    if isinstance(row, dict):
        market = row.get("market")
        if market:
            portfolio_by_market[market] = row

result_rows = []

for exit_row in exit_rows:
    if not isinstance(exit_row, dict):
        continue

    market = exit_row.get("market", "")
    exit_action = str(exit_row.get("exit_action", "NO_EXIT") or "NO_EXIT").upper()
    exit_enabled = bool(exit_row.get("exit_enabled", False))

    port_row = portfolio_by_market.get(market, {}) or {}
    current_position = str(port_row.get("position", "FLAT") or "FLAT").upper()
    current_entry_price = to_float(port_row.get("entry_price", 0.0), 0.0)
    current_mark_price = to_float(port_row.get("mark_price", 0.0), 0.0)
    current_qty = to_float(port_row.get("qty", 0.0), 0.0)
    current_pnl = to_float(port_row.get("pnl", 0.0), 0.0)
    current_equity = to_float(port_row.get("equity", 10000.0), 10000.0)

    next_position = current_position
    next_entry_price = current_entry_price
    next_qty = current_qty
    apply_action = "NO_APPLY"
    apply_reason = "EXIT_NOT_TRIGGERED"

    if current_position == "FLAT":
        next_position = "FLAT"
        next_entry_price = 0.0
        next_qty = 0.0
        apply_action = "NO_APPLY"
        apply_reason = "ALREADY_FLAT"
    else:
        if exit_enabled and exit_action in ("TAKE_PROFIT_EXIT", "STOP_EXIT"):
            next_position = "FLAT"
            next_entry_price = 0.0
            next_qty = 0.0
            apply_action = "APPLY_EXIT_CLOSE"
            apply_reason = exit_action
        elif exit_action == "HOLD_POSITION":
            next_position = current_position
            next_entry_price = current_entry_price
            next_qty = current_qty
            apply_action = "KEEP_POSITION"
            apply_reason = "HOLD_SIGNAL"
        else:
            next_position = current_position
            next_entry_price = current_entry_price
            next_qty = current_qty
            apply_action = "NO_APPLY"
            apply_reason = "NO_EXIT_SIGNAL"

    result_rows.append({
        "market": market,
        "current_position": current_position,
        "next_position": next_position,
        "entry_price": current_entry_price,
        "next_entry_price": next_entry_price,
        "mark_price": current_mark_price,
        "qty": current_qty,
        "next_qty": next_qty,
        "pnl": current_pnl,
        "equity": current_equity,
        "apply_action": apply_action,
        "apply_reason": apply_reason,
        "ts_utc": now_utc_iso()
    })

result = {
    "version": "worker_exit_apply_stub_lite_v2",
    "markets": result_rows
}

with open(out_json, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

with open(out_tsv, "w", encoding="utf-8") as f:
    f.write("market\tcurrent_position\tnext_position\tentry_price\tnext_entry_price\tmark_price\tqty\tnext_qty\tpnl\tequity\tapply_action\tapply_reason\n")
    for row in result_rows:
        f.write(
            f'{row["market"]}\t{row["current_position"]}\t{row["next_position"]}\t{row["entry_price"]}\t{row["next_entry_price"]}\t{row["mark_price"]}\t{row["qty"]}\t{row["next_qty"]}\t{row["pnl"]}\t{row["equity"]}\t{row["apply_action"]}\t{row["apply_reason"]}\n'
        )

print(json.dumps({
    "ok": True,
    "output_json": out_json,
    "output_tsv": out_tsv,
    "markets": len(result_rows),
    "version": result["version"]
}, indent=2))