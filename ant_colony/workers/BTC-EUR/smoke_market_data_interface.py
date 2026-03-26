from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ant_colony.worker_io import get_worker_market_data


ANT_OUT = Path(r"C:\Trading\ANT_OUT")
MARKET = "BTC-EUR"
INTERVAL = "4h"
LIMIT = 50


def main() -> int:
    ANT_OUT.mkdir(parents=True, exist_ok=True)

    result = get_worker_market_data(
        market=MARKET,
        interval=INTERVAL,
        limit=LIMIT,
    )

    rows = result.get("rows") or []

    summary = {
        "ok": result.get("ok"),
        "source": result.get("source"),
        "market": result.get("market"),
        "interval": result.get("interval"),
        "count": result.get("count"),
        "first_row": rows[0] if rows else None,
        "last_row": rows[-1] if rows else None,
        "error": result.get("error"),
        "meta": result.get("meta"),
    }

    print("=== BTC WORKER MARKET DATA SMOKE ===")
    print(json.dumps(summary, indent=2))

    out_path = ANT_OUT / "btc_worker_market_data_smoke.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"WROTE {out_path}")

    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())