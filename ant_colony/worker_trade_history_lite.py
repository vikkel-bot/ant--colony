import json
import os
from datetime import datetime, timezone

ANT_OUT = r"C:\Trading\ANT_OUT"

life_file = os.path.join(ANT_OUT, "worker_trade_lifecycle.json")
out_json = os.path.join(ANT_OUT, "worker_trade_history.json")
out_tsv  = os.path.join(ANT_OUT, "worker_trade_history.tsv")


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


life_doc = load_json(life_file)
existing_doc = load_json(out_json)

life_rows = life_doc.get("markets", [])
if not isinstance(life_rows, list):
    life_rows = []

existing_rows = existing_doc.get("trades", [])
if not isinstance(existing_rows, list):
    existing_rows = []

trades = list(existing_rows)

next_id = len(trades) + 1

for row in life_rows:
    if not isinstance(row, dict):
        continue

    trade_state = str(row.get("trade_state", "NO_TRADE") or "NO_TRADE").upper()
    if trade_state != "TRADE_CLOSED":
        continue

    market = row.get("market", "")
    current_position = str(row.get("current_position", "FLAT") or "FLAT").upper()
    realized_pnl = to_float(row.get("realized_pnl", 0.0), 0.0)
    equity = to_float(row.get("equity", 10000.0), 10000.0)
    reason = str(row.get("lifecycle_reason", "") or "")

    trades.append({
        "trade_id": next_id,
        "market": market,
        "side": current_position,
        "realized_pnl": realized_pnl,
        "equity_after": equity,
        "close_reason": reason,
        "closed_ts_utc": now_utc_iso()
    })
    next_id += 1

result = {
    "version": "worker_trade_history_lite_v1",
    "trades": trades
}

with open(out_json, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

with open(out_tsv, "w", encoding="utf-8") as f:
    f.write("trade_id\tmarket\tside\trealized_pnl\tequity_after\tclose_reason\tclosed_ts_utc\n")
    for row in trades:
        f.write(
            f'{row["trade_id"]}\t{row["market"]}\t{row["side"]}\t{row["realized_pnl"]}\t{row["equity_after"]}\t{row["close_reason"]}\t{row["closed_ts_utc"]}\n'
        )

print(json.dumps({
    "ok": True,
    "output_json": out_json,
    "output_tsv": out_tsv,
    "trades": len(trades),
    "version": result["version"]
}, indent=2))