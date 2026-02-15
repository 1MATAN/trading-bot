"""Trailing stop monitoring — primary exit mechanism, only moves up.

Uses previous candle's low minus buffer as the trailing stop level.
"""

import asyncio
import logging
from typing import Optional

from ib_insync import Trade

from broker.order_manager import OrderManager
from broker.position_manager import PositionManager, Position
from config.settings import (
    TRAILING_STOP_INITIAL_PCT,
    TRAILING_STOP_CHECK_INTERVAL,
    TRAILING_STOP_CANDLE_BUFFER,
)
from utils.helpers import round_price

logger = logging.getLogger("trading_bot.trailing_stop")


class TrailingStopManager:
    """Monitors prices and adjusts stop-losses upward.

    Primary exit mechanism — trailing stop below previous candle's low.
    Also maintains safety stop as a floor.
    """

    def __init__(
        self,
        order_manager: OrderManager,
        position_manager: PositionManager,
    ) -> None:
        self._orders = order_manager
        self._positions = position_manager
        self._stop_trades: dict[str, Trade] = {}  # symbol → stop order Trade
        self._running = False

    def register_stop(self, symbol: str, trade: Trade) -> None:
        """Register a stop-loss order for a position."""
        self._stop_trades[symbol] = trade
        logger.info(f"Trailing stop registered for {symbol}")

    def unregister_stop(self, symbol: str) -> None:
        """Remove stop tracking when position is closed."""
        self._stop_trades.pop(symbol, None)

    async def start(self) -> None:
        """Start the trailing stop monitoring loop."""
        self._running = True
        logger.info("Trailing stop monitor started")
        while self._running:
            await self._check_all()
            await asyncio.sleep(TRAILING_STOP_CHECK_INTERVAL)

    def stop(self) -> None:
        """Stop the monitoring loop."""
        self._running = False
        logger.info("Trailing stop monitor stopped")

    async def _check_all(self) -> None:
        """Check and adjust all trailing stops."""
        for symbol, pos in list(self._positions.positions.items()):
            stop_trade = self._stop_trades.get(symbol)
            if stop_trade is None:
                continue

            current_price = await self._get_current_price(pos)
            if current_price is None:
                continue

            # Update high watermark
            self._positions.update_highest_price(symbol, current_price)

            # Calculate new stop level
            new_stop = self._calculate_trailing_stop(pos)
            if new_stop is None:
                continue

            # Only move stop up
            current_stop = stop_trade.order.auxPrice
            if new_stop > current_stop:
                updated = await self._orders.modify_stop_loss(stop_trade, new_stop)
                if updated:
                    self._stop_trades[symbol] = updated
                    pos.stop_loss_price = new_stop
                    logger.info(
                        f"Trailing stop {symbol}: "
                        f"${current_stop:.4f} -> ${new_stop:.4f} "
                        f"(high=${pos.highest_price:.4f})"
                    )

    def _calculate_trailing_stop(self, pos: Position) -> Optional[float]:
        """Calculate the trailing stop price.

        Uses the position's trailing_stop_price if available (set by
        candle-low logic in the main loop), otherwise falls back to
        percentage-based calculation from highest price.
        """
        # Use candle-low trailing stop if set on position
        candle_stop = getattr(pos, 'trailing_stop_price', 0.0)
        if candle_stop > 0:
            # Never below the safety stop floor
            safety_floor = pos.entry_price * (1 - TRAILING_STOP_INITIAL_PCT)
            return round_price(max(candle_stop, safety_floor))

        # Fallback: percentage-based from highest price
        new_stop = pos.highest_price * (1 - TRAILING_STOP_INITIAL_PCT)
        initial_stop = pos.entry_price * (1 - TRAILING_STOP_INITIAL_PCT)
        return round_price(max(new_stop, initial_stop))

    async def _get_current_price(self, pos: Position) -> Optional[float]:
        """Get last price for a position's contract."""
        try:
            ticker = self._positions.ib.ticker(pos.contract)
            if ticker and ticker.last and ticker.last > 0:
                return ticker.last
            if ticker and ticker.close and ticker.close > 0:
                return ticker.close
            return None
        except Exception:
            return None
