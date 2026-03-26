import json
import os
from datetime import datetime, timezone

ANT_OUT = r"C:\Trading\ANT_OUT"

portfolio_file = os.path.join(ANT_OUT, "worker_portfolio_state.json")
apply_file = os.path.join(ANT_OUT, "worker_exit_apply_stub.json")

out_json = os.path.join(ANT_OUT, "worker_trade_lifecycle.json")
out_tsv  = os.path.join(ANT_OUT, "worker_trade_lifecycle.tsv")


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


portfolio_doc = load_json(portfolio_file)
apply_doc = load_json(apply_file)

portfolio_rows = portfolio_doc.get("markets", [])
apply_rows = apply_doc.get("markets", [])

if not isinstance(portfolio_rows, list):
    portfolio_rows = []
if not isinstance(apply_rows, list):
    apply_rows = []

portfolio_by_market = {}
for row in portfolio_rows:
    if isinstance(row, dict):
        market = row.get("market")
        if market:
            portfolio_by_market[market] = row

result_rows = []

for apply_row in apply_rows:
    if not isinstance(apply_row, dict):
        continue

    market = apply_row.get("market", "")
    current_position = str(apply_row.get("current_position", "FLAT") or "FLAT").upper()
    next_position = str(apply_row.get("next_position", "FLAT") or "FLAT").upper()
    apply_action = str(apply_row.get("apply_action", "NO_APPLY") or "NO_APPLY").upper()
    apply_reason = str(apply_row.get("apply_reason", "") or "")

    port_row = portfolio_by_market.get(market, {}) or {}
    pnl = to_float(port_row.get("pnl", 0.0), 0.0)
    equity = to_float(port_row.get("equity", 10000.0), 10000.0)

    trade_state = "NO_TRADE"
    lifecycle_action = "NOOP"
    realized_pnl = 0.0
    lifecycle_reason = "FLAT_TO_FLAT"

    if current_position == "FLAT" and next_position == "FLAT":
        trade_state = "NO_TRADE"
        lifecycle_action = "NOOP"
        realized_pnl = 0.0
        lifecycle_reason = "FLAT_TO_FLAT"

    elif current_position in ("LONG", "SHORT") and next_position == current_position:
        trade_state = "TRADE_OPEN"
        lifecycle_action = "HOLD_OPEN_TRADE"
        realized_pnl = 0.0
        lifecycle_reason = apply_reason or "POSITION_STILL_OPEN"

    elif current_position in ("LONG", "SHORT") and next_position == "FLAT":
        trade_state = "TRADE_CLOSED"
        lifecycle_action = "CLOSE_TRADE"
        realized_pnl = pnl
        lifecycle_reason = apply_reason or "POSITION_CLOSED"

    else:
        trade_state = "TRANSITION_UNHANDLED"
        lifecycle_action = apply_action
        realized_pnl = 0.0
        lifecycle_reason = "UNHANDLED_STATE_TRANSITION"

    result_rows.append({
        "market": market,
        "current_position": current_position,
        "next_position": next_position,
        "trade_state": trade_state,
        "lifecycle_action": lifecycle_action,
        "realized_pnl": realized_pnl,
        "equity": equity,
        "lifecycle_reason": lifecycle_reason,
        "ts_utc": now_utc_iso()
    })

result = {
    "version": "worker_trade_lifecycle_lite_v1",
    "markets": result_rows
}

with open(out_json, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

with open(out_tsv, "w", encoding="utf-8") as f:
    f.write("market\tcurrent_position\tnext_position\ttrade_state\tlifecycle_action\trealized_pnl\tequity\tlifecycle_reason\n")
    for row in result_rows:
        f.write(
            f'{row["market"]}\t{row["current_position"]}\t{row["next_position"]}\t{row["trade_state"]}\t{row["lifecycle_action"]}\t{row["realized_pnl"]}\t{row["equity"]}\t{row["lifecycle_reason"]}\n'
        )

print(json.dumps({
    "ok": True,
    "output_json": out_json,
    "output_tsv": out_tsv,
    "markets": len(result_rows),
    "version": result["version"]
}, indent=2))