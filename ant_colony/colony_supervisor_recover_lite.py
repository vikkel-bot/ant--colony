import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

VERSION = "colony_supervisor_recover_lite_v2"
RECOVERY_COOLDOWN_SECONDS = 120

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = Path(r"C:\Trading\ANT_OUT")

SUPERVISOR_STATUS = OUT_DIR / "colony_supervisor_status_lite.json"
SUPERVISOR_STATUS_SCRIPT = ROOT / "ant_colony" / "colony_supervisor_status_lite.py"
WATCHDOG_CMD = ROOT / "ant_colony" / "colony_watchdog_start_if_needed.cmd"

OUT_JSON = OUT_DIR / "colony_supervisor_recover_lite.json"

SAFE_RECOVER = {
    "SUPERVISOR_LOOP_STALE",
    "SUPERVISOR_NOT_RUNNING",
    "SUPERVISOR_HEARTBEAT_MISSING",
    "SUPERVISOR_RUNNER_FAILED"
}

def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def now_dt():
    return datetime.now(timezone.utc)

def parse_iso_z(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None

def calc_age_seconds(ts_value):
    ts_dt = parse_iso_z(ts_value)
    if ts_dt is None:
        return -1
    return int((now_dt() - ts_dt).total_seconds())

def read_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def refresh_supervisor():
    return subprocess.run(
        [sys.executable, str(SUPERVISOR_STATUS_SCRIPT)],
        cwd=str(ROOT),
        capture_output=True,
        text=True
    )

def run_watchdog():
    proc = subprocess.Popen(
        [
            "cmd.exe",
            "/c",
            "start",
            "",
            str(WATCHDOG_CMD)
        ],
        cwd=str(ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    )
    return {
        "rc": 0,
        "pid": int(proc.pid),
    }

def main():
    previous = read_json(OUT_JSON)
    previous_ts = str(previous.get("ts_utc", "") or "")
    previous_age_seconds = calc_age_seconds(previous_ts)
    cooldown_active = (previous_age_seconds >= 0) and (previous_age_seconds < RECOVERY_COOLDOWN_SECONDS)

    refresh_proc = refresh_supervisor()
    status = read_json(SUPERVISOR_STATUS)

    supervisor_state = str(status.get("supervisor_state", "") or "")
    supervisor_reason = str(status.get("supervisor_reason", "") or "")

    decision = "NO_ACTION"
    decision_reason = "RECOVERY_NOT_REQUIRED"

    if supervisor_reason == "SUPERVISOR_RUNNING_OK":
        decision = "NO_ACTION"
        decision_reason = "SUPERVISOR_ALREADY_OK"
    elif supervisor_reason == "SUPERVISOR_LOCK_ACTIVE":
        decision = "NO_ACTION"
        decision_reason = "LOCK_ACTIVE_NO_RECOVERY"
    elif cooldown_active:
        decision = "NO_ACTION"
        decision_reason = "RECOVERY_COOLDOWN_ACTIVE"
    elif supervisor_reason in SAFE_RECOVER:
        decision = "RUN_WATCHDOG"
        decision_reason = f"SAFE_RECOVERY_{supervisor_reason}"
    elif supervisor_state in ("STALE", "DOWN", "ERROR"):
        decision = "RUN_WATCHDOG"
        decision_reason = f"SAFE_RECOVERY_STATE_{supervisor_state}"

    action_attempted = False
    action_rc = 0

    if decision == "RUN_WATCHDOG":
        action = run_watchdog()
        action_attempted = True
        action_rc = int(action.get("rc", 1))

    result = {
        "version": VERSION,
        "ts_utc": utc_now(),
        "refresh_rc": int(refresh_proc.returncode),
        "supervisor_state": supervisor_state,
        "supervisor_reason": supervisor_reason,
        "decision": decision,
        "decision_reason": decision_reason,
        "action_attempted": action_attempted,
        "action_rc": action_rc,
        "action_name": "WATCHDOG_START_IF_NEEDED" if action_attempted else "",
        "action_ok": (action_attempted and action_rc == 0) or (not action_attempted),
        "previous_recovery_ts": previous_ts,
        "previous_recovery_age_seconds": previous_age_seconds,
        "recovery_cooldown_seconds": RECOVERY_COOLDOWN_SECONDS,
        "recovery_cooldown_active": cooldown_active
    }

    write_json(OUT_JSON, result)
    print(json.dumps(result, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())