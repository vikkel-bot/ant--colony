import json
from pathlib import Path


STATUS_PATH = Path(r"C:\Trading\ANT_OUT\colony_supervisor_status_lite.json")


def read_json(path: Path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main():
    data = read_json(STATUS_PATH)

    print("COLONY SUPERVISOR PANEL")
    print()
    print("version:", str(data.get("version", "")))
    print()

    rows = [
        ("watchdog_action", data.get("watchdog_action", "")),
        ("watchdog_result", data.get("watchdog_result", "")),
        ("watchdog_action_age_seconds", data.get("watchdog_action_age_seconds", -1)),
        ("watchdog_action_is_stale", data.get("watchdog_action_is_stale", True)),
        ("watchdog_state", data.get("watchdog_state", "")),
        ("watchdog_runtime_state", data.get("watchdog_runtime_state", "")),
        ("watchdog_engine_running", data.get("watchdog_engine_running", False)),
        ("loop_pid", data.get("loop_pid", "")),
        ("loop_cycle_no", data.get("loop_cycle_no", 0)),
        ("loop_last_ok", data.get("loop_last_ok", False)),
        ("loop_stopped_reason", data.get("loop_stopped_reason", "")),
        ("loop_last_rc", data.get("loop_last_rc", 0)),
        ("loop_age_seconds", data.get("loop_age_seconds", -1)),
        ("loop_is_stale", data.get("loop_is_stale", True)),
        ("loop_last_success_ts_utc", data.get("loop_last_success_ts_utc", "")),
        ("status_ts_utc", data.get("ts_utc", "")),
    ]

    key_width = max(len(str(k)) for k, _ in rows)
    for key, value in rows:
        print(f"{str(key).ljust(key_width)} : {value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())