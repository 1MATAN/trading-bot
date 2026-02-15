"""Track open positions, sync with IBKR state."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from ib_insync import IB, Contract

from broker.ibkr_connection import get_connection
from utils.time_utils import now_utc

logger = logging.getLogger("trading_bot.positions")


@dataclass
class Position:
    """Local position tracking with metadata beyond what IBKR provides."""
    symbol: str
    contract: Contract
    quantity: int
    entry_price: float
    entry_time: datetime
    stop_loss_price: float = 0.0
    stop_order_id: Optional[int] = None
    partial_exits_done: int = 0
    highest_price: float = 0.0  # for trailing stop
    fib_levels: list = field(default_factory=list)
    notes: str = ""
    # Fib live strategy fields
    target_price: float = 0.0
    target_order_id: int = 0
    oca_group: str = ""
    supporting_fib_level: float = 0.0
    fib_level_index: int = -1
    strategy: str = "momentum"

    @property
    def market_value(self) -> float:
        return self.quantity * self.entry_price

    def unrealized_pnl(self, current_price: float) -> float:
        return (current_price - self.entry_price) * self.quantity

    def unrealized_pnl_pct(self, current_price: float) -> float:
        if self.entry_price == 0:
            return 0.0
        return (current_price - self.entry_price) / self.entry_price


class PositionManager:
    """Manages local position state synced with IBKR."""

    def __init__(self) -> None:
        self._conn = get_connection()
        self._positions: dict[str, Position] = {}  # symbol → Position

    @property
    def ib(self) -> IB:
        return self._conn.ib

    @property
    def positions(self) -> dict[str, Position]:
        return self._positions

    @property
    def count(self) -> int:
        return len(self._positions)

    def has_position(self, symbol: str) -> bool:
        return symbol in self._positions

    def get_position(self, symbol: str) -> Optional[Position]:
        return self._positions.get(symbol)

    def add_position(
        self,
        symbol: str,
        contract: Contract,
        quantity: int,
        entry_price: float,
        stop_loss_price: float = 0.0,
        fib_levels: Optional[list] = None,
        target_price: float = 0.0,
        target_order_id: int = 0,
        oca_group: str = "",
        supporting_fib_level: float = 0.0,
        fib_level_index: int = -1,
        strategy: str = "momentum",
    ) -> Position:
        """Register a new position after a fill."""
        pos = Position(
            symbol=symbol,
            contract=contract,
            quantity=quantity,
            entry_price=entry_price,
            entry_time=now_utc(),
            stop_loss_price=stop_loss_price,
            highest_price=entry_price,
            fib_levels=fib_levels or [],
            target_price=target_price,
            target_order_id=target_order_id,
            oca_group=oca_group,
            supporting_fib_level=supporting_fib_level,
            fib_level_index=fib_level_index,
            strategy=strategy,
        )
        self._positions[symbol] = pos
        logger.info(
            f"Position opened: {symbol} {quantity} shares @ ${entry_price:.4f}"
        )
        return pos

    def update_quantity(self, symbol: str, sold_qty: int) -> Optional[Position]:
        """Reduce position size after partial exit."""
        pos = self._positions.get(symbol)
        if pos is None:
            return None
        pos.quantity -= sold_qty
        pos.partial_exits_done += 1
        if pos.quantity <= 0:
            return self.remove_position(symbol)
        logger.info(f"Position {symbol} reduced by {sold_qty}, remaining: {pos.quantity}")
        return pos

    def remove_position(self, symbol: str) -> Optional[Position]:
        """Remove a fully closed position."""
        pos = self._positions.pop(symbol, None)
        if pos:
            logger.info(f"Position closed: {symbol}")
        return pos

    def update_highest_price(self, symbol: str, current_price: float) -> None:
        """Update high watermark for trailing stop."""
        pos = self._positions.get(symbol)
        if pos and current_price > pos.highest_price:
            pos.highest_price = current_price

    async def sync_with_ibkr(self) -> None:
        """Sync local state with IBKR's actual positions."""
        ibkr_positions = await self.ib.reqPositionsAsync()
        ibkr_symbols = set()

        for ib_pos in ibkr_positions:
            symbol = ib_pos.contract.symbol
            ibkr_symbols.add(symbol)

            if symbol not in self._positions and ib_pos.position > 0:
                # IBKR has a position we don't track — adopt it
                logger.warning(
                    f"Found untracked IBKR position: {symbol} "
                    f"{ib_pos.position} @ ${ib_pos.avgCost:.4f}"
                )
                self.add_position(
                    symbol=symbol,
                    contract=ib_pos.contract,
                    quantity=int(ib_pos.position),
                    entry_price=ib_pos.avgCost,
                    fib_levels=[],
                )
            elif symbol in self._positions:
                # Reconcile quantity
                local = self._positions[symbol]
                ibkr_qty = int(ib_pos.position)
                if local.quantity != ibkr_qty:
                    logger.warning(
                        f"Position mismatch for {symbol}: "
                        f"local={local.quantity}, IBKR={ibkr_qty}"
                    )
                    local.quantity = ibkr_qty
                    if ibkr_qty <= 0:
                        self.remove_position(symbol)

        # Remove local positions that IBKR no longer has
        stale = [s for s in self._positions if s not in ibkr_symbols]
        for symbol in stale:
            logger.warning(f"Removing stale local position: {symbol}")
            self.remove_position(symbol)

    def get_all_symbols(self) -> list[str]:
        """List all held symbols."""
        return list(self._positions.keys())

    def total_market_value(self) -> float:
        """Sum of all position market values at entry."""
        return sum(p.market_value for p in self._positions.values())

    def to_dict_list(self) -> list[dict]:
        """Serialize positions for dashboard/logging."""
        result = []
        for s, p in self._positions.items():
            result.append({
                "symbol": s,
                "quantity": p.quantity,
                "entry_price": p.entry_price,
                "entry_time": p.entry_time.isoformat(),
                "stop_loss_price": p.stop_loss_price,
                "partial_exits_done": p.partial_exits_done,
                "highest_price": p.highest_price,
                "target_price": p.target_price,
                "oca_group": p.oca_group,
                "supporting_fib_level": p.supporting_fib_level,
                "strategy": p.strategy,
            })
        return result
