"""Entry orchestration pipeline: risk check → position size → order → stops."""

import logging
from typing import Optional

from ib_insync import Contract, Stock

from broker.ibkr_connection import get_connection
from broker.order_manager import OrderManager
from broker.position_manager import PositionManager
from risk.risk_manager import RiskManager
from risk.trailing_stop import TrailingStopManager
from strategies.fibonacci_engine import find_nearest_support
from strategies.signal_scorer import SignalResult
from config.settings import TRAILING_STOP_INITIAL_PCT

logger = logging.getLogger("trading_bot.momentum")


class MomentumEntry:
    """Orchestrates the full entry pipeline."""

    def __init__(
        self,
        order_manager: OrderManager,
        position_manager: PositionManager,
        risk_manager: RiskManager,
        trailing_stop_manager: TrailingStopManager,
    ) -> None:
        self._orders = order_manager
        self._positions = position_manager
        self._risk = risk_manager
        self._trailing = trailing_stop_manager
        self._conn = get_connection()

    async def execute_entry(self, signal: SignalResult) -> bool:
        """Execute the full entry pipeline for a passing signal.

        Pipeline:
        1. Check risk gate
        2. Skip if already in position
        3. Calculate position size
        4. Place market buy
        5. Place GTC stop-loss
        6. Register with trailing stop manager

        Returns True if entry was executed.
        """
        symbol = signal.symbol

        # 1. Risk gate
        can_trade, reason = self._risk.can_trade(self._positions.count)
        if not can_trade:
            logger.info(f"Risk gate blocked {symbol}: {reason}")
            return False

        # 2. Skip if already holding
        if self._positions.has_position(symbol):
            logger.debug(f"Already in position for {symbol}, skipping")
            return False

        current_price = signal.details["current_price"]
        if current_price <= 0:
            return False

        # 3. Calculate stop-loss level and position size
        stop_price = self._calculate_stop_price(signal, current_price)
        if stop_price <= 0 or stop_price >= current_price:
            logger.warning(
                f"Invalid stop price for {symbol}: "
                f"stop=${stop_price:.4f} entry=${current_price:.4f}"
            )
            return False

        quantity = self._risk.calculate_position_size(current_price, stop_price)
        if quantity <= 0:
            logger.info(f"Position size 0 for {symbol}, skipping")
            return False

        # 4. Qualify contract
        contract = Stock(symbol, "SMART", "USD")
        qualified = await self._conn.qualify_contract(contract)
        if qualified is None:
            logger.error(f"Failed to qualify contract for {symbol}")
            return False

        # 5. Place market buy
        buy_trade = await self._orders.place_market_buy(qualified, quantity)
        if buy_trade is None:
            logger.error(f"Failed to place buy order for {symbol}")
            return False

        # 6. Place GTC stop-loss (server-side, survives crashes)
        stop_trade = await self._orders.place_stop_loss(
            qualified, quantity, stop_price
        )

        # 7. Register position
        pos = self._positions.add_position(
            symbol=symbol,
            contract=qualified,
            quantity=quantity,
            entry_price=current_price,
            stop_loss_price=stop_price,
            fib_levels=signal.fib_levels,
        )

        # 8. Register trailing stop
        if stop_trade:
            pos.stop_order_id = stop_trade.order.orderId
            self._trailing.register_stop(symbol, stop_trade)

        logger.info(
            f"ENTRY: {symbol} — {quantity} shares @ ${current_price:.4f}, "
            f"stop @ ${stop_price:.4f}, "
            f"risk=${(current_price - stop_price) * quantity:.2f}"
        )
        return True

    def _calculate_stop_price(
        self, signal: SignalResult, current_price: float
    ) -> float:
        """Calculate initial stop-loss price.

        Uses Fibonacci support if available, otherwise default percentage.
        """
        # Try Fibonacci support level
        support = find_nearest_support(signal.fib_levels, current_price)
        if support:
            fib_stop = support[1] * 0.99  # 1% below support
            # Ensure stop isn't too far from entry (cap at initial %)
            max_stop_distance = current_price * TRAILING_STOP_INITIAL_PCT
            min_stop = current_price - max_stop_distance
            return max(fib_stop, min_stop)

        # Fallback: percentage-based stop
        return current_price * (1 - TRAILING_STOP_INITIAL_PCT)
