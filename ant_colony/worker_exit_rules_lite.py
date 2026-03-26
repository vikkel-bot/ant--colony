import json
import os
from datetime import datetime, timezone

ANT_OUT = r"C:\Trading\ANT_OUT"

portfolio_file = os.path.join(ANT_OUT, "worker_portfolio_state.json")
out_json = os.path.join(ANT_OUT, "worker_exit_rules.json")
out_tsv  = os.path.join(ANT_OUT, "worker_exit_rules.tsv")


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
portfolio_rows = portfolio_doc.get("markets", [])
if not isinstance(portfolio_rows, list):
    portfolio_rows = []

result_rows = []

for row in portfolio_rows:
    if not isinstance(row, dict):
        continue

    market = row.get("market", "")
    position = str(row.get("position", "FLAT") or "FLAT").upper()
    pnl = to_float(row.get("pnl", 0.0), 0.0)

    tp_level = 2.0
    sl_level = -2.0
    rule_action = "NO_POSITION"
    rule_reason = "FLAT_POSITION"

    if position in ("LONG", "SHORT"):
        if pnl >= tp_level:
            rule_action = "TAKE_PROFIT_RULE"
            rule_reason = "PNL_GTE_TP"
        elif pnl <= sl_level:
            rule_action = "STOP_LOSS_RULE"
            rule_reason = "PNL_LTE_SL"
        else:
            rule_action = "HOLD_RULE"
            rule_reason = "PNL_INSIDE_BANDS"

    result_rows.append({
        "market": market,
        "position": position,
        "pnl": pnl,
        "tp_level": tp_level,
        "sl_level": sl_level,
        "rule_action": rule_action,
        "rule_reason": rule_reason,
        "ts_utc": now_utc_iso()
    })

result = {
    "version": "worker_exit_rules_lite_v1",
    "markets": result_rows
}

with open(out_json, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

with open(out_tsv, "w", encoding="utf-8") as f:
    f.write("market\tposition\tpnl\ttp_level\tsl_level\trule_action\trule_reason\n")
    for row in result_rows:
        f.write(
            f'{row["market"]}\t{row["position"]}\t{row["pnl"]}\t{row["tp_level"]}\t{row["sl_level"]}\t{row["rule_action"]}\t{row["rule_reason"]}\n'
        )

print(json.dumps({
    "ok": True,
    "output_json": out_json,
    "output_tsv": out_tsv,
    "markets": len(result_rows),
    "version": result["version"]
}, indent=2))