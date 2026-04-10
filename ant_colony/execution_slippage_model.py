"""
AC-121: Execution Slippage Model (Paper-Only)

Standalone, pure slippage models for paper execution realism.
No I/O. No state. No external dependencies. No live execution.

Three models:
  NoSlippage               — fill at exact market price (default, backward-safe)
  FixedSlippage            — fixed absolute spread in quote currency (EUR)
  FixedBasisPointsSlippage — fixed basis-point cost per side

API:
  model.apply(price: float, side: str) -> float

Side semantics (explicit, fail-closed):
  BUY / LONG  → slipped price is WORSE (higher) — pays more to enter
  SELL / SHORT → slipped price is WORSE (lower)  — receives less on exit
  Anything else → ValueError

Price validation:
  price <= 0.0 → ValueError

Usage:
  from execution_slippage_model import NoSlippage, FixedBasisPointsSlippage

  model = FixedBasisPointsSlippage(bps=5.0)
  fill_price = model.apply(raw_price, "BUY")

Paper-only. Never used in live execution.
"""
from __future__ import annotations

_VALID_BUY_SIDES  = frozenset({"BUY",  "LONG"})
_VALID_SELL_SIDES = frozenset({"SELL", "SHORT"})


def _validate_inputs(price: float, side: str) -> str:
    """Validate price and side. Returns normalised upper-case side or raises."""
    if not isinstance(price, (int, float)) or price <= 0.0:
        raise ValueError(f"Invalid price: {price!r}. Must be a positive number.")
    side_up = str(side).upper()
    if side_up not in _VALID_BUY_SIDES and side_up not in _VALID_SELL_SIDES:
        raise ValueError(
            f"Invalid side: {side!r}. Expected one of: BUY, LONG, SELL, SHORT."
        )
    return side_up


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class NoSlippage:
    """
    No slippage — fill at the exact market price.
    Default model. Backward-safe: produces identical results to pre-AC121 behaviour.
    """

    def apply(self, price: float, side: str) -> float:
        """Return price unchanged after validating inputs."""
        _validate_inputs(price, side)
        return price


class FixedSlippage:
    """
    Fixed absolute spread slippage.

    spread  — total bid-ask spread in quote currency (e.g. EUR) per unit.
              Half the spread is applied per side:
                BUY  → fill_price = price + spread / 2
                SELL → fill_price = price - spread / 2

    Example: spread=0.10 on a 100 EUR asset → 5 cents slippage per side.
    """

    def __init__(self, spread: float = 0.0) -> None:
        if spread < 0.0:
            raise ValueError(f"spread must be >= 0, got {spread!r}")
        self.spread = float(spread)
        self._half  = self.spread / 2.0

    def apply(self, price: float, side: str) -> float:
        side_up = _validate_inputs(price, side)
        if side_up in _VALID_BUY_SIDES:
            return price + self._half
        return price - self._half


class FixedBasisPointsSlippage:
    """
    Fixed basis-points slippage per side.

    bps — cost in basis points (1 bps = 0.01 %) applied per trade side:
            BUY  → fill_price = price * (1 + bps / 10_000)
            SELL → fill_price = price * (1 - bps / 10_000)

    Example: bps=5 on a 100 EUR asset → fill is 100.05 EUR for a buy.
    """

    def __init__(self, bps: float = 0.0) -> None:
        if bps < 0.0:
            raise ValueError(f"bps must be >= 0, got {bps!r}")
        self.bps    = float(bps)
        self._factor = self.bps / 10_000.0

    def apply(self, price: float, side: str) -> float:
        side_up = _validate_inputs(price, side)
        if side_up in _VALID_BUY_SIDES:
            return price * (1.0 + self._factor)
        return price * (1.0 - self._factor)
