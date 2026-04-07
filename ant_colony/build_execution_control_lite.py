import json
import os
from datetime import datetime, timezone
from pathlib import Path

OUT_PATH = Path(r"C:\Trading\ANT_OUT\execution_control.json")


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def main():
    market_flags = {
        "BTC-EUR": True,
        "ETH-EUR": False,
        "SOL-EUR": False,
        "XRP-EUR": False,
        "ADA-EUR": False,
        "BNB-EUR": False,
    }

    global_enabled = str(os.getenv("AC_GLOBAL_EXECUTION_ENABLED", "0")).strip().lower() in ("1", "true", "yes", "on")

    data = {
        "component": "build_execution_control_lite",
        "ts_utc": now_utc(),
        "global_execution_enabled": global_enabled,
        "market_execution_enabled": market_flags,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"WROTE {OUT_PATH}")


if __name__ == "__main__":
    main()


