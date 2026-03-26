from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


VERSION = "colony_watchdog_check_lite_v5"

ROOT = Path(r"C:\Users\vikke\OneDrive\bitvavo-bot_clean")
OUT_DIR = Path(r"C:\Trading\ANT_OUT")

HEARTBEAT_JSON = OUT_DIR / "colony_cycle_loop_heartbeat.json"
OUT_JSON = OUT_DIR / "colony_watchdog_check_lite.json"
OUT_TSV = OUT_DIR / "colony_watchdog_check_lite.tsv"

DEFAULT_MAX_AGE_SECONDS = 90


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_max_age_seconds(argv: list[str]) -> int:
    if len(argv) < 2:
        return DEFAULT_MAX_AGE_SECONDS
    raw = str(argv[1]).strip()
    try:
        value = int(raw)
    except Exception:
        return DEFAULT_MAX_AGE_SECONDS
    if value <= 0:
        return DEFAULT_MAX_AGE_SECONDS
    return value


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def write_tsv(path: Path, data: dict) -> None:
    lines = [
        "field\tvalue",
        f"version\t{data.get('version', '')}",
        f"ts_utc\t{data.get('ts_utc', '')}",
        f"heartbeat_path\t{data.get('heartbeat_path', '')}",
        f"max_age_seconds\t{data.get('max_age_seconds', '')}",
        f"heartbeat_exists\t{data.get('heartbeat_exists', '')}",
        f"heartbeat_ts_utc\t{data.get('heartbeat_ts_utc', '')}",
        f"heartbeat_age_seconds\t{data.get('heartbeat_age_seconds', '')}",
        f"heartbeat_component\t{data.get('heartbeat_component', '')}",
        f"heartbeat_pid\t{data.get('heartbeat_pid', '')}",
        f"heartbeat_cycle_no\t{data.get('heartbeat_cycle_no', '')}",
        f"heartbeat_last_ok\t{data.get('heartbeat_last_ok', '')}",
        f"heartbeat_stopped_reason\t{data.get('heartbeat_stopped_reason', '')}",
        f"watchdog_state\t{data.get('watchdog_state', '')}",
        f"runtime_state\t{data.get('runtime_state', '')}",
        f"last_known_runtime_state\t{data.get('last_known_runtime_state', '')}",
        f"engine_running\t{data.get('engine_running', '')}",
        f"ok\t{data.get('ok', '')}",
        f"reason\t{data.get('reason', '')}",
        f"recovery_triggered\t{data.get('recovery_triggered', '')}",
        f"recovery_trigger_reason\t{data.get('recovery_trigger_reason', '')}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def pid_is_running(pid_text: str) -> bool:
    pid_text = str(pid_text or "").strip()
    if not pid_text or not pid_text.isdigit():
        return False
    try:
        proc = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid_text}"],
            capture_output=True,
            text=True,
            check=False,
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        return pid_text in out and "No tasks are running" not in out
    except Exception:
        return False


def main(argv: list[str]) -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    max_age_seconds = parse_max_age_seconds(argv)
    ts_now = now_utc()

    heartbeat_exists = HEARTBEAT_JSON.exists()
    heartbeat = read_json(HEARTBEAT_JSON) if heartbeat_exists else {}

    heartbeat_ts_utc = str(heartbeat.get("ts_utc", "") or "")
    heartbeat_component = str(heartbeat.get("component", "") or "")
    heartbeat_pid = str(heartbeat.get("pid", "") or "")
    heartbeat_cycle_no = int(heartbeat.get("cycle_no", 0) or 0)
    heartbeat_last_ok = bool(heartbeat.get("last_ok", False))
    heartbeat_stopped_reason = str(heartbeat.get("stopped_reason", "") or "")

    heartbeat_age_seconds = -1
    if heartbeat_ts_utc:
        try:
            heartbeat_dt = datetime.fromisoformat(heartbeat_ts_utc.replace("Z", "+00:00"))
            heartbeat_age_seconds = int((ts_now - heartbeat_dt).total_seconds())
        except Exception:
            heartbeat_age_seconds = -1

    engine_running = pid_is_running(heartbeat_pid)
    last_known_runtime_state = "RUNNING_OK" if heartbeat_last_ok else "UNKNOWN"

    if not heartbeat_exists:
        watchdog_state = "MISSING"
        runtime_state = "DOWN"
        ok = False
        reason = "HEARTBEAT_MISSING"
    elif heartbeat_age_seconds < 0:
        watchdog_state = "STALE"
        runtime_state = "STALE"
        ok = False
        reason = "HEARTBEAT_INVALID_TIMESTAMP"
    elif heartbeat_age_seconds > max_age_seconds:
        watchdog_state = "STALE"
        runtime_state = "STALE"
        ok = False
        reason = "ENGINE_NOT_RUNNING_OR_STALE"
    elif not engine_running:
        watchdog_state = "STALE"
        runtime_state = "STALE"
        ok = False
        reason = "ENGINE_NOT_RUNNING_OR_STALE"
    else:
        watchdog_state = "FRESH"
        runtime_state = "RUNNING_OK"
        ok = True
        reason = "ENGINE_RUNNING_OK"

    data = {
        "version": VERSION,
        "ts_utc": iso_z(ts_now),
        "heartbeat_path": str(HEARTBEAT_JSON),
        "max_age_seconds": int(max_age_seconds),
        "heartbeat_exists": bool(heartbeat_exists),
        "heartbeat_ts_utc": heartbeat_ts_utc,
        "heartbeat_age_seconds": int(heartbeat_age_seconds),
        "heartbeat_component": heartbeat_component,
        "heartbeat_pid": heartbeat_pid,
        "heartbeat_cycle_no": int(heartbeat_cycle_no),
        "heartbeat_last_ok": bool(heartbeat_last_ok),
        "heartbeat_stopped_reason": heartbeat_stopped_reason,
        "watchdog_state": watchdog_state,
        "runtime_state": runtime_state,
        "last_known_runtime_state": last_known_runtime_state,
        "engine_running": bool(engine_running),
        "ok": bool(ok),
        "reason": reason,
        "recovery_triggered": False,
        "recovery_trigger_reason": "",
    }

    write_json(OUT_JSON, data)
    write_tsv(OUT_TSV, data)

    print(json.dumps(data, indent=2))

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))