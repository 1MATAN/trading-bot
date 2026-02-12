"""Staged exits at Fibonacci resistance levels (25% each)."""

import logging
from typing import Optional

from broker.order_manager import OrderManager
from broker.position_manager import PositionManager, Position
from config.settings import PARTIAL_EXIT_STAGES

logger = logging.getLogger("trading_bot.partial_exits")


class PartialExitManager:
    """Manages staged profit-taking at Fibonacci levels."""

    def __init__(
        self,
        order_manager: OrderManager,
        position_manager: PositionManager,
    ) -> None:
        self._orders = order_manager
        self._positions = position_manager

    async def check_exits(self, symbol: str, current_price: float) -> int:
        """Check if price has reached the next partial exit level.

        Returns the number of shares sold (0 if no exit triggered).
        """
        pos = self._positions.get_position(symbol)
        if pos is None or pos.quantity <= 0:
            return 0

        stage_idx = pos.partial_exits_done
        if stage_idx >= len(PARTIAL_EXIT_STAGES):
            return 0  # all stages exhausted

        stage = PARTIAL_EXIT_STAGES[stage_idx]
        target_price = self._get_target_price(pos, stage["fib_level"])
        if target_price is None:
            return 0

        if current_price < target_price:
            return 0

        # Calculate shares to sell this stage
        exit_pct = stage["exit_pct"]
        # For the last stage, sell everything remaining
        if stage_idx == len(PARTIAL_EXIT_STAGES) - 1:
            shares_to_sell = pos.quantity
        else:
            # Calculate from original position (entry value / entry price)
            original_qty = int(pos.market_value / pos.entry_price) + pos.quantity
            shares_to_sell = max(1, int(original_qty * exit_pct))
            shares_to_sell = min(shares_to_sell, pos.quantity)

        if shares_to_sell <= 0:
            return 0

        # Place the sell order
        trade = await self._orders.place_market_sell(pos.contract, shares_to_sell)
        if trade is None:
            logger.error(f"Failed to place partial exit for {symbol}")
            return 0

        self._positions.update_quantity(symbol, shares_to_sell)
        logger.info(
            f"Partial exit {stage_idx + 1}/{len(PARTIAL_EXIT_STAGES)} for {symbol}: "
            f"sold {shares_to_sell} shares @ ${current_price:.4f} "
            f"(fib {stage['fib_level']}, target ${target_price:.4f})"
        )
        return shares_to_sell

    def _get_target_price(self, pos: Position, fib_level: float) -> Optional[float]:
        """Calculate exit target price from Fibonacci levels."""
        if not pos.fib_levels or len(pos.fib_levels) < 2:
            return None

        # fib_levels stored as [(level, price), ...] sorted by level
        swing_low = pos.fib_levels[0][1]   # 0.0 level = swing low
        swing_high = pos.fib_levels[-1][1]  # 1.0 level = swing high
        price_range = swing_high - swing_low

        if price_range <= 0:
            return None

        return swing_low + (price_range * fib_level)

    def get_next_target(self, symbol: str) -> Optional[dict]:
        """Get info about the next partial exit target."""
        pos = self._positions.get_position(symbol)
        if pos is None:
            return None

        stage_idx = pos.partial_exits_done
        if stage_idx >= len(PARTIAL_EXIT_STAGES):
            return None

        stage = PARTIAL_EXIT_STAGES[stage_idx]
        target_price = self._get_target_price(pos, stage["fib_level"])

        return {
            "stage": stage_idx + 1,
            "total_stages": len(PARTIAL_EXIT_STAGES),
            "fib_level": stage["fib_level"],
            "exit_pct": stage["exit_pct"],
            "target_price": target_price,
        }
