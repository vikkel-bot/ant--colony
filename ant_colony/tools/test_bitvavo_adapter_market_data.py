from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ant_colony.broker_adapters import BitvavoAdapter


def main() -> int:
    adapter = BitvavoAdapter(
        ops_log_path=r"C:\Trading\ANT_OUT\bitvavo_adapter_ops.jsonl",
    )

    result = adapter.get_market_data("BTC-EUR", "4h", 10)

    print("=== SUMMARY ===")
    print({
        "ok": result.get("ok"),
        "adapter": result.get("adapter"),
        "operation": result.get("operation"),
        "error": result.get("error"),
        "count": (result.get("data") or {}).get("count") if result.get("data") else None,
    })

    print("=== FIRST_ROW ===")
    data = result.get("data") or {}
    rows = data.get("rows") or []
    print(rows[0] if rows else None)

    print("=== LAST_ROW ===")
    print(rows[-1] if rows else None)

    out_path = Path(r"C:\Trading\ANT_OUT\bitvavo_market_data_test.json")
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"WROTE {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())