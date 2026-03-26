import os
import sys
import json
import subprocess
from datetime import datetime, UTC

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PY = sys.executable

SUMMARY_PATH = r"C:\Trading\ANT_OUT\execution_cycle_runner_lite.json"
LAST_OK_PATH = r"C:\Trading\ANT_OUT\execution_cycle_runner_lite.last_ok.json"
PAPER_SUMMARY_PATH = r"C:\Trading\ANT_OUT\paper_execution_summary.json"
PORTFOLIO_SUMMARY_PATH = r"C:\Trading\ANT_OUT\paper_portfolio_summary.json"
WORKER_MARKET_DATA_PATH = r"C:\Trading\ANT_OUT\worker_market_data.json"
EXECUTION_CONTROL_PATH = r"C:\Trading\ANT_OUT\execution_control.json"
MAX_REFRESH_AGE_SECONDS = 180
MAX_EXECUTION_CONTROL_AGE_SECONDS = 180

STEPS = [
    ("combined_colony_status", os.path.join(ROOT, "ant_colony", "combined_colony_status_lite.py")),
    ("worker_market_data_producer", os.path.join(ROOT, "ant_colony", "worker_market_data_producer_lite.py")),
    ("worker_market_data_refresh", os.path.join(ROOT, "ant_colony", "worker_market_data_refresh_lite.py")),
    ("mark_to_market_equity_engine", os.path.join(ROOT, "ant_colony", "mark_to_market_equity_engine_lite.py")),
    ("build_execution_control", os.path.join(ROOT, "ant_colony", "build_execution_control_lite.py")),
    ("build_execution_intents", os.path.join(ROOT, "ant_colony", "build_execution_intents_lite.py")),
    ("paper_execution_runner", os.path.join(ROOT, "ant_colony", "paper_execution_runner_lite.py")),
    ("paper_trade_reconstruction", os.path.join(ROOT, "ant_colony", "paper_trade_reconstruction_lite.py")),
    ("paper_trade_feedback", os.path.join(ROOT, "ant_colony", "paper_trade_feedback_lite.py")),
    ("show_colony_ops_panel", os.path.join(ROOT, "ant_colony", "show_colony_ops_panel.py")),
]


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def load_json(path):
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return {}


def parse_ts(value):
    if not value:
        return None
    try:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def age_seconds(now_utc, ts_value):
    dt = parse_ts(ts_value)
    if dt is None:
        return None
    try:
        return max(0, int((now_utc - dt).total_seconds()))
    except Exception:
        return None


def extract_paper_summary():
    src = load_json(PAPER_SUMMARY_PATH)
    return {
        "intents_processed": src.get("intents_processed", 0),
        "intents_allowed": src.get("intents_allowed", 0),
        "intents_skipped": src.get("intents_skipped", 0),
        "log_file_exists": src.get("log_file_exists", False),
        "executed_ids_count": src.get("executed_ids_count", 0),
        "position_count": src.get("position_count", 0),
    }


def extract_portfolio_summary():
    src = load_json(PORTFOLIO_SUMMARY_PATH)
    return {
        "state": src.get("state", "UNKNOWN"),
        "cash": src.get("cash", 0),
        "equity": src.get("equity", 0),
        "positions_market_value": src.get("positions_market_value", 0),
        "unrealized_pnl": src.get("unrealized_pnl", 0),
        "open_positions": src.get("open_positions", 0),
        "priced_positions": src.get("priced_positions", 0),
        "unpriced_positions": src.get("unpriced_positions", 0),
        "equity_calc_mode": src.get("equity_calc_mode", "unknown"),
    }


def run_step(name, script_path):
    started = datetime.now(UTC).isoformat()
    result = subprocess.run(
        [PY, script_path],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    finished = datetime.now(UTC).isoformat()
    return {
        "name": name,
        "script_path": script_path,
        "started_utc": started,
        "finished_utc": finished,
        "returncode": result.returncode,
        "ok": result.returncode == 0,
        "stdout_tail": (result.stdout or "")[-4000:],
        "stderr_tail": (result.stderr or "")[-4000:],
    }


def validate_worker_market_data_refresh():
    now_utc = datetime.now(UTC)
    data = load_json(WORKER_MARKET_DATA_PATH)

    ts_utc = data.get("ts_utc") or data.get("ts")
    refresh_age_s = age_seconds(now_utc, ts_utc)

    market_count = int(data.get("market_count", 0) or 0)
    priced_count = int(data.get("priced_count", 0) or 0)
    zero_price_count = int(data.get("zero_price_count", 0) or 0)
    missing_markets = data.get("missing_markets", []) or []
    markets = data.get("markets", {}) or {}

    issues = []

    if not data:
        issues.append("worker_market_data_json_missing_or_unreadable")

    if refresh_age_s is None:
        issues.append("worker_market_data_ts_missing")
    elif refresh_age_s > MAX_REFRESH_AGE_SECONDS:
        issues.append(f"worker_market_data_stale age_s={refresh_age_s} max={MAX_REFRESH_AGE_SECONDS}")

    if market_count <= 0:
        issues.append("worker_market_data_market_count_invalid")

    if priced_count != market_count:
        issues.append(f"priced_count_mismatch priced={priced_count} market_count={market_count}")

    if zero_price_count != 0:
        issues.append(f"zero_price_count={zero_price_count}")

    if missing_markets:
        issues.append("missing_markets=" + ",".join(sorted(str(x) for x in missing_markets)))

    if not isinstance(markets, dict) or len(markets) != market_count:
        size_value = len(markets) if isinstance(markets, dict) else "non_dict"
        issues.append(f"markets_dict_size_invalid size={size_value} expected={market_count}")

    for market, row in markets.items():
        if not isinstance(row, dict):
            issues.append(f"{market}:row_not_dict")
            continue

        state = str(row.get("state", "") or "")
        last_price = row.get("last_price", 0.0)
        try:
            last_price = float(last_price)
        except Exception:
            last_price = 0.0

        if state != "OK":
            issues.append(f"{market}:state={state or 'EMPTY'}")

        if last_price <= 0.0:
            issues.append(f"{market}:last_price_invalid={last_price}")

    ok = len(issues) == 0

    return {
        "name": "worker_market_data_refresh_guard",
        "script_path": WORKER_MARKET_DATA_PATH,
        "started_utc": now_utc.isoformat(),
        "finished_utc": datetime.now(UTC).isoformat(),
        "returncode": 0 if ok else 1,
        "ok": ok,
        "stdout_tail": json.dumps(
            {
                "refresh_age_seconds": refresh_age_s,
                "market_count": market_count,
                "priced_count": priced_count,
                "zero_price_count": zero_price_count,
                "missing_markets": missing_markets,
                "max_refresh_age_seconds": MAX_REFRESH_AGE_SECONDS,
            },
            indent=2,
        ),
        "stderr_tail": "" if ok else json.dumps({"issues": issues}, indent=2),
    }


def validate_execution_control():
    now_utc = datetime.now(UTC)
    data = load_json(EXECUTION_CONTROL_PATH)

    ts_utc = data.get("ts_utc") or data.get("ts")
    control_age_s = age_seconds(now_utc, ts_utc)

    issues = []

    if not data:
        issues.append("execution_control_json_missing_or_unreadable")

    if control_age_s is None:
        issues.append("execution_control_ts_missing")
    elif control_age_s > MAX_EXECUTION_CONTROL_AGE_SECONDS:
        issues.append(
            f"execution_control_stale age_s={control_age_s} max={MAX_EXECUTION_CONTROL_AGE_SECONDS}"
        )

    global_execution_enabled = data.get("global_execution_enabled", None)
    market_execution_enabled = data.get("market_execution_enabled", None)

    if global_execution_enabled is None:
        issues.append("global_execution_enabled_missing")

    if market_execution_enabled is None:
        issues.append("market_execution_enabled_missing")
    elif not isinstance(market_execution_enabled, dict):
        issues.append("market_execution_enabled_not_dict")
    elif len(market_execution_enabled) == 0:
        issues.append("market_execution_enabled_empty")

    ok = len(issues) == 0

    return {
        "name": "execution_control_guard",
        "script_path": EXECUTION_CONTROL_PATH,
        "started_utc": now_utc.isoformat(),
        "finished_utc": datetime.now(UTC).isoformat(),
        "returncode": 0 if ok else 1,
        "ok": ok,
        "stdout_tail": json.dumps(
            {
                "execution_control_age_seconds": control_age_s,
                "global_execution_enabled": global_execution_enabled,
                "market_count": (
                    len(market_execution_enabled)
                    if isinstance(market_execution_enabled, dict)
                    else None
                ),
                "max_execution_control_age_seconds": MAX_EXECUTION_CONTROL_AGE_SECONDS,
            },
            indent=2,
        ),
        "stderr_tail": "" if ok else json.dumps({"issues": issues}, indent=2),
    }


def main():
    ts = datetime.now(UTC).isoformat()
    steps_out = []
    overall_ok = True

    for name, script_path in STEPS:
        step = run_step(name, script_path)
        steps_out.append(step)

        if not step["ok"]:
            overall_ok = False
            break

        if name == "worker_market_data_producer":
            stderr_text = str(step.get("stderr_tail", "") or "")
            producer_stderr_bad = (
                "Traceback" in stderr_text or
                "Exception in thread" in stderr_text or
                "ValueError:" in stderr_text
            )
            if producer_stderr_bad:
                step["ok"] = False
                step["returncode"] = 1
                step["stderr_tail"] = ("RUNNER_HARD_FAIL: producer emitted exception on stderr`n" + stderr_text)[-4000:]
                overall_ok = False
                break

        if name == "worker_market_data_refresh":
            guard_step = validate_worker_market_data_refresh()
            steps_out.append(guard_step)
            if not guard_step["ok"]:
                overall_ok = False
                break

        if name == "build_execution_control":
            guard_step = validate_execution_control()
            steps_out.append(guard_step)
            if not guard_step["ok"]:
                overall_ok = False
                break

    summary = {
        "component": "execution_cycle_runner_lite",
        "ts_utc": ts,
        "root": ROOT,
        "python": PY,
        "steps_total": len(STEPS) + 2,
        "steps_completed": len(steps_out),
        "ok": overall_ok,
        "paper_execution_summary": extract_paper_summary(),
        "paper_portfolio_summary": extract_portfolio_summary(),
        "steps": steps_out,
    }

    write_json(SUMMARY_PATH, summary)

    if overall_ok:
        last_ok = {
            "component": "execution_cycle_runner_lite_last_ok",
            "ts_utc": ts,
            "root": ROOT,
            "python": PY,
            "steps_total": len(STEPS) + 2,
            "steps_completed": len(steps_out),
            "ok": True,
        }
        write_json(LAST_OK_PATH, last_ok)

    print(json.dumps(summary, indent=2))

    if not overall_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()






