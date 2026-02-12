"""Trailing stop monitoring — only moves up, never down."""

import asyncio
import logging
from typing import Optional

from ib_insync import Trade

from broker.order_manager import OrderManager
from broker.position_manager import PositionManager, Position
from config.settings import (
    TRAILING_STOP_INITIAL_PCT,
    TRAILING_STOP_TIGHTEN_PCT,
    TRAILING_STOP_CHECK_INTERVAL,
)
from utils.helpers import round_price

logger = logging.getLogger("trading_bot.trailing_stop")


class TrailingStopManager:
    """Monitors prices and adjusts stop-losses upward."""

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
            new_stop = self._calculate_trailing_stop(pos, current_price)
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
                        f"${current_stop:.4f} → ${new_stop:.4f} "
                        f"(price=${current_price:.4f}, high=${pos.highest_price:.4f})"
                    )

    def _calculate_trailing_stop(
        self, pos: Position, current_price: float
    ) -> Optional[float]:
        """Calculate the trailing stop price."""
        # Tighten after first partial exit
        trail_pct = (
            TRAILING_STOP_TIGHTEN_PCT
            if pos.partial_exits_done > 0
            else TRAILING_STOP_INITIAL_PCT
        )
        # Trail from the highest price seen
        new_stop = pos.highest_price * (1 - trail_pct)
        # Never below entry-based stop
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
