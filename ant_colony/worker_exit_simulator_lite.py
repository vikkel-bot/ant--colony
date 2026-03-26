import json
import os
from datetime import datetime, timezone

ANT_OUT = r"C:\Trading\ANT_OUT"

portfolio_file = os.path.join(ANT_OUT, "worker_portfolio_state.json")
out_json = os.path.join(ANT_OUT, "worker_exit_simulator.json")
out_tsv  = os.path.join(ANT_OUT, "worker_exit_simulator.tsv")


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


portfolio = load_json(portfolio_file)
portfolio_rows = portfolio.get("markets", [])
if not isinstance(portfolio_rows, list):
    portfolio_rows = []

result_rows = []

for row in portfolio_rows:
    if not isinstance(row, dict):
        continue

    market = row.get("market", "")
    position = str(row.get("position", "FLAT") or "FLAT").upper()
    entry_price = to_float(row.get("entry_price", 0.0), 0.0)
    mark_price = to_float(row.get("mark_price", 0.0), 0.0)
    pnl = to_float(row.get("pnl", 0.0), 0.0)

    pnl_pct = 0.0
    exit_enabled = False
    exit_action = "NO_EXIT"
    exit_reason = "FLAT_POSITION"

    if position in ("LONG", "SHORT") and entry_price > 0 and mark_price > 0:
        exit_enabled = True

        if position == "LONG":
            pnl_pct = (mark_price - entry_price) / entry_price
        elif position == "SHORT":
            pnl_pct = (entry_price - mark_price) / entry_price

        pnl_pct = round(pnl_pct, 6)

        if pnl_pct >= 0.02:
            exit_action = "TAKE_PROFIT_EXIT"
            exit_reason = "PNL_PCT_GTE_0P02"
        elif pnl_pct <= -0.02:
            exit_action = "STOP_EXIT"
            exit_reason = "PNL_PCT_LTE_NEG_0P02"
        else:
            exit_action = "HOLD_POSITION"
            exit_reason = "PNL_PCT_BETWEEN_BANDS"

    result_rows.append({
        "market": market,
        "position": position,
        "entry_price": entry_price,
        "mark_price": mark_price,
        "pnl": pnl,
        "pnl_pct": pnl_pct,
        "exit_enabled": exit_enabled,
        "exit_action": exit_action,
        "exit_reason": exit_reason,
        "ts_utc": now_utc_iso()
    })

result = {
    "version": "worker_exit_simulator_lite_v2",
    "markets": result_rows
}

with open(out_json, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

with open(out_tsv, "w", encoding="utf-8") as f:
    f.write("market\tposition\tentry_price\tmark_price\tpnl\tpnl_pct\texit_enabled\texit_action\texit_reason\n")
    for row in result_rows:
        f.write(
            f'{row["market"]}\t{row["position"]}\t{row["entry_price"]}\t{row["mark_price"]}\t{row["pnl"]}\t{row["pnl_pct"]}\t{row["exit_enabled"]}\t{row["exit_action"]}\t{row["exit_reason"]}\n'
        )

print(json.dumps({
    "ok": True,
    "output_json": out_json,
    "output_tsv": out_tsv,
    "markets": len(result_rows),
    "version": result["version"]
}, indent=2))