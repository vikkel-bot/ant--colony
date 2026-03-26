from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ant_colony.broker_adapters import BitvavoAdapter


def main() -> int:
    adapter = BitvavoAdapter(
        ops_log_path=r"C:\Trading\ANT_OUT\bitvavo_adapter_ops.jsonl",
    )

    print("=== test_connection() ===")
    print(adapter.test_connection())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())