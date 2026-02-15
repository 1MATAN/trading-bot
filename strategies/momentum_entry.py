"""Entry pipeline: risk check → position size → order → safety stop."""

import logging
from typing import Optional

from ib_insync import Stock

from broker.ibkr_connection import get_connection
from broker.order_manager import OrderManager
from broker.position_manager import PositionManager
from risk.risk_manager import RiskManager
from risk.trailing_stop import TrailingStopManager
from strategies.signal_checker import EntrySignal, compute_stop_price
from config.settings import SAFETY_STOP_PCT

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

    async def execute_entry(self, signal: EntrySignal) -> bool:
        """Execute the full entry pipeline for a passing signal.

        Pipeline:
        1. Check risk gate
        2. Skip if already in position
        3. Calculate position size (90% of portfolio)
        4. Place market buy
        5. Place GTC safety stop (10% below entry)
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

        current_price = signal.current_price
        if current_price <= 0:
            return False

        # 3. Calculate position size (90% of portfolio)
        quantity = self._risk.calculate_position_size(current_price)
        if quantity <= 0:
            logger.info(f"Position size 0 for {symbol}, skipping")
            return False

        # 4. Qualify contract
        contract = Stock(symbol, "SMART", "USD")
        qualified = await self._conn.qualify_contract(contract)
        if qualified is None:
            logger.error(f"Failed to qualify contract for {symbol}")
            return False

        # 5. Place buy (market during RTH, limit during extended hours)
        buy_trade = await self._orders.place_market_buy(
            qualified, quantity, reference_price=current_price
        )
        if buy_trade is None:
            logger.error(f"Failed to place buy order for {symbol}")
            return False

        # 6. Place GTC stop (dynamic: pullback low based, or safety fallback)
        pullback_low = getattr(signal, 'pullback_low', 0.0)
        if pullback_low > 0 and pullback_low < float('inf'):
            stop_price = compute_stop_price(current_price, pullback_low)
        else:
            stop_price = round(current_price * (1 - SAFETY_STOP_PCT), 2)
        stop_trade = await self._orders.place_stop_loss(
            qualified, quantity, stop_price
        )

        # 7. Register position
        fib_levels = [
            ("pullback_low", getattr(signal, 'pullback_low', 0)),
            ("gap_pct", getattr(signal, 'gap_pct', 0)),
            ("vwap", getattr(signal, 'vwap', 0)),
        ]

        pos = self._positions.add_position(
            symbol=symbol,
            contract=qualified,
            quantity=quantity,
            entry_price=current_price,
            stop_loss_price=stop_price,
            fib_levels=fib_levels,
        )

        # 8. Register trailing stop
        if stop_trade:
            pos.stop_order_id = stop_trade.order.orderId
            self._trailing.register_stop(symbol, stop_trade)

        logger.info(
            f"ENTRY: {symbol} — {quantity} shares @ ${current_price:.4f}, "
            f"safety stop @ ${stop_price:.4f}"
        )
        return True
