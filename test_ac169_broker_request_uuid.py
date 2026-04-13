"""
AC-169: client_request_id is now a UUID; colony_request_ref holds the trace.

Verifies:
  A. client_request_id is a valid UUID v4 string
  B. colony_request_ref contains the old REQ_lane_market_... trace structure
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.broker_request_builder import build_broker_request

_INTAKE = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "order_side": "buy",
    "qty": 0.08,
    "intended_entry_price": 600.0,
    "order_type": "market",
    "max_notional_eur": 50.0,
    "allow_broker_execution": True,
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
    "operator_approved": True,
    "ts_intake_utc": "2026-04-13T10:00:00Z",
}


def test_A_client_request_id_is_valid_uuid():
    br = build_broker_request(_INTAKE)["broker_request"]
    crid = br["client_request_id"]
    parsed = uuid.UUID(crid, version=4)   # raises if not valid UUID v4
    assert str(parsed) == crid


def test_B_colony_request_ref_has_internal_trace_structure():
    br = build_broker_request(_INTAKE)["broker_request"]
    ref = br["colony_request_ref"]
    assert ref.startswith("REQ_")
    assert "live_test" in ref
    assert "BNBEUR" in ref
    assert "EDGE3" in ref
    assert "buy" in ref
