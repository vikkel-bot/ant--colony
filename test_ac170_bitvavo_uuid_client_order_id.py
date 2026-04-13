"""
AC-170: _to_bitvavo_client_order_id preserves valid UUID input with hyphens.

Verifies:
  A. Valid UUID input is returned unchanged in canonical hyphenated form
  B. Non-UUID input still follows the old strip/hash logic (regression)
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.broker_adapters.bitvavo_adapter import _to_bitvavo_client_order_id


def test_A_uuid_input_returned_with_hyphens():
    raw = str(uuid.uuid4())
    result = _to_bitvavo_client_order_id(raw)
    assert result == raw
    assert len(result) == 36
    assert result[8] == '-' and result[13] == '-'


def test_A_uuid_input_is_parseable_as_uuid():
    raw = str(uuid.uuid4())
    result = _to_bitvavo_client_order_id(raw)
    parsed = uuid.UUID(result)   # must not raise
    assert str(parsed) == result


def test_B_non_uuid_short_string_stripped():
    assert _to_bitvavo_client_order_id("REQ123") == "REQ123"


def test_B_non_uuid_underscore_string_stripped():
    assert _to_bitvavo_client_order_id("REQ_live_test") == "REQlivetest"
