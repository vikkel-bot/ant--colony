import json
from pathlib import Path
from datetime import datetime, timezone

OUT_DIR = Path(r"C:\Trading\ANT_OUT")

FEEDBACK_PATH = OUT_DIR / "paper_trade_feedback.json"
EXECUTION_LOG_PATH = OUT_DIR / "paper_execution_log.jsonl"

OUT_STATE_PATH = OUT_DIR / "strategy_feedback_state.json"
OUT_STATE_TSV_PATH = OUT_DIR / "strategy_feedback_state.tsv"


def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def load_jsonl(path: Path):
    rows = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    except Exception:
        pass
    return rows


def to_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def write_tsv(path: Path, headers, rows):
    lines = ["`t".join(headers)]
    for row in rows:
        lines.append("`t".join(str(row.get(h, "")) for h in headers))
    path.write_text("`n".join(lines), encoding="utf-8")


def label_from_score(score: float) -> str:
    if score > 0:
        return "POSITIVE"
    if score < 0:
        return "NEGATIVE"
    return "FLAT"


def build_strategy_maps(execution_rows):
    """
    Maakt deterministische lookup-tabellen:
    - decision_id -> strategy
    - execution_id -> strategy
    - market -> laatste bekende strategy (fallback)
    """
    decision_to_strategy = {}
    execution_to_strategy = {}
    market_to_strategy = {}

    for row in execution_rows:
        market = str(row.get("market", "") or "").strip()
        strategy = str(row.get("strategy", "") or "").strip().upper()
        decision_id = str(row.get("decision_id", "") or "").strip()
        execution_id = str(row.get("execution_id", "") or "").strip()

        if not strategy:
            continue

        if decision_id:
            decision_to_strategy[decision_id] = strategy

        if execution_id:
            execution_to_strategy[execution_id] = strategy

        if market:
            market_to_strategy[market] = strategy

    return decision_to_strategy, execution_to_strategy, market_to_strategy


def extract_execution_id_from_trade_id(trade_id: str) -> str:
    if not trade_id or "|" not in trade_id:
        return ""
    return str(trade_id.split("|", 1)[1]).strip()


def resolve_strategy(row, decision_to_strategy, execution_to_strategy, market_to_strategy):
    entry_decision_id = str(row.get("entry_decision_id", "") or "").strip()
    exit_decision_id = str(row.get("exit_decision_id", "") or "").strip()
    trade_id = str(row.get("trade_id", "") or "").strip()
    market = str(row.get("market", "") or "").strip()

    if entry_decision_id and entry_decision_id in decision_to_strategy:
        return decision_to_strategy[entry_decision_id]

    if exit_decision_id and exit_decision_id in decision_to_strategy:
        return decision_to_strategy[exit_decision_id]

    execution_id = extract_execution_id_from_trade_id(trade_id)
    if execution_id and execution_id in execution_to_strategy:
        return execution_to_strategy[execution_id]

    if market and market in market_to_strategy:
        return market_to_strategy[market]

    return "UNKNOWN"


def resolve_position_key(row, decision_to_strategy, execution_to_strategy, market_to_strategy):
    """
    AC43: bepaal position_key als primaire aggregatie-eenheid.
    Voorkeur: position_key direct uit feedback-row (door reconstruction gevuld).
    Fallback: market__strategy of alleen market.
    """
    market = str(row.get("market", "") or "").strip()

    # Directe position_key uit reconstruction (AC43-pad)
    pk = str(row.get("position_key", "") or "").strip()
    strategy_direct = str(row.get("strategy", "") or "").strip().upper()
    if pk and pk != market and "__" in pk:
        return pk, strategy_direct if strategy_direct else pk.split("__", 1)[1]

    # Fallback: resolve strategy via execution log
    strategy = resolve_strategy(row, decision_to_strategy, execution_to_strategy, market_to_strategy)
    if strategy and strategy != "UNKNOWN":
        return f"{market}__{strategy}", strategy

    # Laatste fallback: market zonder strategy
    return market, "UNKNOWN"


def main():
    ts = utc_now_ts()

    feedback = load_json(FEEDBACK_PATH, {})
    feedback_rows = feedback.get("rows", []) if isinstance(feedback, dict) else []
    execution_rows = load_jsonl(EXECUTION_LOG_PATH)

    decision_to_strategy, execution_to_strategy, market_to_strategy = build_strategy_maps(execution_rows)

    # AC43: groepeer op position_key als primaire eenheid
    grouped = {}
    unresolved_rows = 0

    for row in feedback_rows:
        market = str(row.get("market", "") or "").strip()
        if not market:
            continue

        position_key, strategy = resolve_position_key(
            row, decision_to_strategy, execution_to_strategy, market_to_strategy
        )

        if strategy == "UNKNOWN":
            unresolved_rows += 1

        realized_pnl = round(to_float(row.get("realized_pnl", 0.0), 0.0), 2)
        unrealized_pnl = round(to_float(row.get("unrealized_pnl", 0.0), 0.0), 2)
        row_score = round(realized_pnl + unrealized_pnl, 2)
        state = str(row.get("state", "") or "").upper()
        entry_ts = row.get("entry_ts") or row.get("exit_ts")

        if position_key not in grouped:
            grouped[position_key] = {
                "position_key": position_key,
                "market": market,
                "strategy": strategy,
                "score": 0.0,
                "label": "FLAT",
                "trade_count": 0,
                "open_trade_count": 0,
                "closed_trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "realized_pnl_sum": 0.0,
                "unrealized_pnl_sum": 0.0,
                "last_trade_ts": None,
                "last_outcome": None,
                "source_trade_ids": [],
            }

        bucket = grouped[position_key]
        bucket["trade_count"] += 1

        if state == "OPEN":
            bucket["open_trade_count"] += 1
        elif state == "CLOSED":
            bucket["closed_trade_count"] += 1
            if realized_pnl > 0:
                bucket["win_count"] += 1
            elif realized_pnl < 0:
                bucket["loss_count"] += 1
            # last_outcome gebaseerd op meest recente closed trade
            if entry_ts and (bucket["last_trade_ts"] is None or entry_ts > bucket["last_trade_ts"]):
                bucket["last_trade_ts"] = entry_ts
                bucket["last_outcome"] = "WIN" if realized_pnl > 0 else ("LOSS" if realized_pnl < 0 else "FLAT")

        bucket["realized_pnl_sum"] = round(bucket["realized_pnl_sum"] + realized_pnl, 2)
        bucket["unrealized_pnl_sum"] = round(bucket["unrealized_pnl_sum"] + unrealized_pnl, 2)
        bucket["score"] = round(bucket["score"] + row_score, 2)

        trade_id = str(row.get("trade_id", "") or "").strip()
        if trade_id:
            bucket["source_trade_ids"].append(trade_id)

    state_rows = []
    for position_key in sorted(grouped.keys()):
        bucket = grouped[position_key]
        bucket["label"] = label_from_score(bucket["score"])
        state_rows.append(bucket)

    # AC43: state_map keyed op position_key (was: market)
    state_map = {row["position_key"]: {
        "market": row["market"],
        "strategy": row["strategy"],
        "score": row["score"],
        "label": row["label"],
        "trade_count": row["trade_count"],
        "open_trade_count": row["open_trade_count"],
        "closed_trade_count": row["closed_trade_count"],
        "win_count": row["win_count"],
        "loss_count": row["loss_count"],
        "realized_pnl_sum": row["realized_pnl_sum"],
        "unrealized_pnl_sum": row["unrealized_pnl_sum"],
        "last_trade_ts": row["last_trade_ts"],
        "last_outcome": row["last_outcome"],
        "source_trade_ids": row["source_trade_ids"],
    } for row in state_rows}

    out = {
        "component": "paper_strategy_feedback_state_lite",
        "ts_utc": ts,
        "source_feedback_file": str(FEEDBACK_PATH),
        "source_execution_log_file": str(EXECUTION_LOG_PATH),
        "strategy_keys_total": len(state_map),
        "trade_rows_total": len(feedback_rows),
        "unresolved_strategy_rows": unresolved_rows,
        "score_definition": "sum(realized_pnl + unrealized_pnl) per position_key (market__strategy)",
        "rows": state_rows,
        "strategy_keys": state_map,
    }

    write_json(OUT_STATE_PATH, out)
    write_tsv(
        OUT_STATE_TSV_PATH,
        [
            "position_key",
            "market",
            "strategy",
            "score",
            "label",
            "trade_count",
            "open_trade_count",
            "closed_trade_count",
            "win_count",
            "loss_count",
            "realized_pnl_sum",
            "unrealized_pnl_sum",
            "last_trade_ts",
            "last_outcome",
        ],
        state_rows
    )

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
