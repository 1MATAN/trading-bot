"""Entry execution for live fib support strategy.

Takes an EntryRequest from the strategy, performs risk checks,
places a market buy, then an OCA bracket (stop + target).
"""

import logging
from datetime import datetime

from broker.ibkr_connection import get_connection
from broker.order_manager import OrderManager
from broker.position_manager import PositionManager
from notifications.trade_logger import TradeLogger
from risk.risk_manager import RiskManager
from strategies.fib_live_strategy import EntryRequest, FibLiveStrategy
from utils.time_utils import now_et

logger = logging.getLogger("trading_bot.fib_live_entry")


class FibLiveEntry:
    """Execute entries from fib live strategy signals."""

    def __init__(
        self,
        order_mgr: OrderManager,
        position_mgr: PositionManager,
        risk_mgr: RiskManager,
        strategy: FibLiveStrategy,
        trade_logger: TradeLogger,
    ) -> None:
        self._conn = get_connection()
        self._order_mgr = order_mgr
        self._position_mgr = position_mgr
        self._risk_mgr = risk_mgr
        self._strategy = strategy
        self._trade_logger = trade_logger

    async def execute_entry(self, request: EntryRequest) -> bool:
        """Execute a single entry from an EntryRequest.

        Steps:
          1. Risk gate (can_trade)
          2. Position sizing
          3. Market buy
          4. OCA bracket (stop + target)
          5. Register position
          6. Log + notify

        Returns True on success.
        """
        symbol = request.symbol

        # 1. Risk gate
        can, reason = self._risk_mgr.can_trade(self._position_mgr.count)
        if not can:
            logger.info(f"[{symbol}] Entry blocked by risk: {reason}")
            return False

        # Already have a position in this symbol?
        if self._position_mgr.has_position(symbol):
            logger.info(f"[{symbol}] Already in position, skipping")
            return False

        # 2. Position sizing
        qty = self._risk_mgr.calculate_position_size(request.entry_price)
        if qty <= 0:
            logger.warning(f"[{symbol}] Position size is 0, skipping")
            return False

        # 3. Market buy
        logger.info(
            f"[{symbol}] Placing market buy: {qty} shares "
            f"(est. ${request.entry_price:.4f})"
        )
        buy_trade = await self._order_mgr.place_market_buy(
            request.contract, qty, reference_price=request.entry_price,
        )
        if buy_trade is None:
            logger.error(f"[{symbol}] Market buy failed")
            return False

        # Wait for fill — check order status
        fill_price = request.entry_price  # default to estimate
        if buy_trade.orderStatus.status == "Filled":
            fill_price = buy_trade.orderStatus.avgFillPrice
        elif buy_trade.fills:
            fill_price = buy_trade.fills[0].execution.avgPrice
        logger.info(f"[{symbol}] Buy filled: {qty} shares @ ${fill_price:.4f}")

        # 4. OCA bracket
        ts = now_et().strftime("%H%M%S")
        oca_group = f"FIB_{symbol}_{ts}"

        stop_trade, target_trade = await self._order_mgr.place_oca_bracket(
            request.contract,
            qty,
            stop_price=request.stop_price,
            target_price=request.target_price,
            oca_group=oca_group,
        )

        stop_order_id = 0
        target_order_id = 0
        if stop_trade:
            stop_order_id = stop_trade.order.orderId
        if target_trade:
            target_order_id = target_trade.order.orderId

        if not stop_trade or not target_trade:
            logger.warning(
                f"[{symbol}] OCA bracket partially placed: "
                f"stop={'OK' if stop_trade else 'FAIL'}, "
                f"target={'OK' if target_trade else 'FAIL'}"
            )

        # 5. Register position
        self._position_mgr.add_position(
            symbol=symbol,
            contract=request.contract,
            quantity=qty,
            entry_price=fill_price,
            stop_loss_price=request.stop_price,
            target_price=request.target_price,
            target_order_id=target_order_id,
            oca_group=oca_group,
            supporting_fib_level=request.fib_level,
            fib_level_index=request.fib_idx,
            strategy="fib_live",
        )
        if stop_trade:
            pos = self._position_mgr.get_position(symbol)
            if pos:
                pos.stop_order_id = stop_order_id

        # 6. Update strategy state
        self._strategy.record_entry()
        self._strategy.mark_in_position(symbol)

        # 7. Log trade
        self._trade_logger.log_entry(
            symbol=symbol,
            quantity=qty,
            price=fill_price,
            stop_price=request.stop_price,
            notes=(
                f"fib_live gap={request.gap_pct:+.1f}% "
                f"fib=${request.fib_level:.4f} "
                f"target=${request.target_price:.4f} "
                f"oca={oca_group}"
            ),
        )

        # 8. Telegram notification
        try:
            from notifications.telegram_bot import notify_entry
            risk_dollars = (fill_price - request.stop_price) * qty
            await notify_entry(
                symbol=symbol,
                quantity=qty,
                price=fill_price,
                stop_price=request.stop_price,
                risk_dollars=risk_dollars,
            )
        except Exception as e:
            logger.debug(f"Telegram notification failed: {e}")

        logger.info(
            f"ENTRY: {symbol} {qty}sh @ ${fill_price:.4f} | "
            f"fib=${request.fib_level:.4f} | "
            f"stop=${request.stop_price:.4f} | "
            f"target=${request.target_price:.4f} | "
            f"gap={request.gap_pct:+.1f}% | oca={oca_group}"
        )

        return True
