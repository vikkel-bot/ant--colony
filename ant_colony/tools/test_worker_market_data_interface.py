from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ant_colony.worker_io import get_worker_market_data


def main() -> int:
    result = get_worker_market_data(
        market="BTC-EUR",
        interval="4h",
        limit=10,
    )

    print("=== SUMMARY ===")
    print({
        "ok": result.get("ok"),
        "source": result.get("source"),
        "market": result.get("market"),
        "interval": result.get("interval"),
        "count": result.get("count"),
        "error": result.get("error"),
    })

    rows = result.get("rows") or []

    print("=== FIRST_ROW ===")
    print(rows[0] if rows else None)

    print("=== LAST_ROW ===")
    print(rows[-1] if rows else None)

    out = Path(r"C:\Trading\ANT_OUT\worker_market_data_interface_test.json")
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"WROTE {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())