from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ant_colony.broker_adapters import BitvavoAdapter


def main() -> int:
    adapter = BitvavoAdapter(
        api_key=None,
        api_secret=None,
        ops_log_path=r"C:\Trading\ANT_OUT\bitvavo_adapter_ops.jsonl",
    )

    print("=== test_connection() ===")
    print(adapter.test_connection())

    print("=== get_market_data() ===")
    print(adapter.get_market_data("BTC-EUR", "4h", 200))

    print("=== place_order() ===")
    print(
        adapter.place_order(
            {
                "market": "BTC-EUR",
                "side": "buy",
                "order_type": "market",
                "size": 0.001,
                "strategy": "EDGE4",
                "reason": "SKELETON_TEST",
                "confidence": 0.50,
                "size_mult": 1.0,
            }
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())