"""Place, modify, and cancel orders via IBKR.

Order types by session:
  - Regular hours (9:30-16:00): MarketOrder, LimitOrder, StopOrder all work
  - Pre-market (4:00-9:30): ONLY LimitOrder, StopLimitOrder (outsideRth=True)
  - After-hours (16:00-20:00): ONLY LimitOrder, StopLimitOrder (outsideRth=True)

IMPORTANT: MarketOrder and StopOrder are silently rejected by IBKR during
extended hours. We use LimitOrder with a small offset to simulate market
fills, and StopLimitOrder for stop-losses that trigger 24h.
"""

import asyncio
import logging
from typing import Optional

from ib_insync import (
    IB, Contract, Order, Trade,
    LimitOrder, MarketOrder, StopOrder,
)

from broker.ibkr_connection import get_connection
from config.settings import LIMIT_OFFSET_CENTS, STOP_LIMIT_OFFSET_PCT
from utils.helpers import round_price
from utils.time_utils import is_market_open

logger = logging.getLogger("trading_bot.orders")


def _make_stop_limit_order(
    action: str, quantity: int, stop_price: float,
) -> Order:
    """Create a StopLimitOrder that works in all sessions.

    The limit price is set slightly below the stop (for sells) to ensure
    fill even with some slippage, while preventing extreme fill prices.
    """
    offset = stop_price * STOP_LIMIT_OFFSET_PCT
    if action == "SELL":
        lmt_price = round_price(stop_price - offset)
    else:
        lmt_price = round_price(stop_price + offset)

    order = Order()
    order.action = action
    order.orderType = "STP LMT"
    order.totalQuantity = quantity
    order.auxPrice = round_price(stop_price)   # trigger price
    order.lmtPrice = lmt_price                 # limit fill price
    order.tif = "GTC"
    order.outsideRth = True
    return order


class OrderManager:
    """Handles order lifecycle with IBKR.

    Uses session-aware order types:
    - Regular hours: MarketOrder for speed
    - Extended hours: LimitOrder with offset (market orders not allowed)
    - Stop-losses: Always StopLimitOrder (plain StopOrder doesn't trigger outside RTH)
    """

    def __init__(self) -> None:
        self._conn = get_connection()

    @property
    def ib(self) -> IB:
        return self._conn.ib

    async def place_market_buy(
        self, contract: Contract, quantity: int,
        reference_price: float = 0.0,
    ) -> Optional[Trade]:
        """Buy at market (RTH) or aggressive limit (extended hours).

        Args:
            contract: IBKR contract to buy.
            quantity: Number of shares.
            reference_price: Current ask/last price. Required during extended
                hours to calculate limit price. If 0, attempts to get from ticker.
        """
        if is_market_open():
            order = MarketOrder("BUY", quantity)
            order.tif = "DAY"
            order.outsideRth = True
        else:
            # Extended hours: use Limit order at reference + offset
            price = reference_price or self._get_last_price(contract)
            if price <= 0:
                logger.error(f"No reference price for {contract.symbol} buy, cannot place limit")
                return None
            limit_price = round_price(price + LIMIT_OFFSET_CENTS)
            order = LimitOrder("BUY", quantity, limit_price)
            order.tif = "DAY"
            order.outsideRth = True
            logger.info(
                f"Extended hours: BUY LMT {contract.symbol} "
                f"@ ${limit_price:.4f} (ref=${price:.4f} + ${LIMIT_OFFSET_CENTS})"
            )
        return await self._place(contract, order)

    async def place_limit_buy(
        self, contract: Contract, quantity: int, limit_price: float,
    ) -> Optional[Trade]:
        """Place a limit buy order (works in all sessions)."""
        order = LimitOrder("BUY", quantity, round_price(limit_price))
        order.tif = "DAY"
        order.outsideRth = True
        return await self._place(contract, order)

    async def place_market_sell(
        self, contract: Contract, quantity: int,
        reference_price: float = 0.0,
    ) -> Optional[Trade]:
        """Sell at market (RTH) or aggressive limit (extended hours).

        Args:
            contract: IBKR contract to sell.
            quantity: Number of shares.
            reference_price: Current bid/last price. Required during extended
                hours to calculate limit price.
        """
        if is_market_open():
            order = MarketOrder("SELL", quantity)
            order.tif = "DAY"
            order.outsideRth = True
        else:
            # Extended hours: use Limit order at reference - offset
            price = reference_price or self._get_last_price(contract)
            if price <= 0:
                logger.error(f"No reference price for {contract.symbol} sell, cannot place limit")
                return None
            limit_price = round_price(price - LIMIT_OFFSET_CENTS)
            limit_price = max(limit_price, 0.01)
            order = LimitOrder("SELL", quantity, limit_price)
            order.tif = "DAY"
            order.outsideRth = True
            logger.info(
                f"Extended hours: SELL LMT {contract.symbol} "
                f"@ ${limit_price:.4f} (ref=${price:.4f} - ${LIMIT_OFFSET_CENTS})"
            )
        return await self._place(contract, order)

    async def place_limit_sell(
        self, contract: Contract, quantity: int, limit_price: float,
    ) -> Optional[Trade]:
        """Place a limit sell order (works in all sessions)."""
        order = LimitOrder("SELL", quantity, round_price(limit_price))
        order.tif = "DAY"
        order.outsideRth = True
        return await self._place(contract, order)

    async def place_stop_loss(
        self, contract: Contract, quantity: int, stop_price: float,
    ) -> Optional[Trade]:
        """Place a GTC Stop-Limit order (triggers in ALL sessions).

        Uses StopLimitOrder instead of StopOrder because:
        - Plain StopOrder does NOT trigger outside RTH on IBKR
        - StopLimitOrder with outsideRth=True triggers 24h
        - Limit offset prevents extreme fill prices on gap-downs
        """
        order = _make_stop_limit_order("SELL", quantity, stop_price)
        logger.info(
            f"Stop-Limit: {contract.symbol} trigger=${stop_price:.4f} "
            f"limit=${order.lmtPrice:.4f} (GTC, outsideRth=True)"
        )
        return await self._place(contract, order)

    async def modify_stop_loss(
        self, trade: Trade, new_stop_price: float,
    ) -> Optional[Trade]:
        """Modify an existing stop-limit order (only moves up)."""
        current_stop = trade.order.auxPrice
        if current_stop >= new_stop_price:
            logger.debug(
                f"Stop for {trade.contract.symbol} already at "
                f"{current_stop} >= {new_stop_price}, skipping"
            )
            return trade

        # Update both trigger and limit prices
        trade.order.auxPrice = round_price(new_stop_price)
        offset = new_stop_price * STOP_LIMIT_OFFSET_PCT
        trade.order.lmtPrice = round_price(new_stop_price - offset)
        try:
            self.ib.placeOrder(trade.contract, trade.order)
            logger.info(
                f"Modified stop-limit for {trade.contract.symbol} → "
                f"trigger=${new_stop_price:.4f} limit=${trade.order.lmtPrice:.4f}"
            )
            return trade
        except Exception as e:
            logger.error(f"Failed to modify stop: {e}")
            return None

    async def place_oca_bracket(
        self,
        contract: Contract,
        quantity: int,
        stop_price: float,
        target_price: float,
        oca_group: str,
    ) -> tuple[Optional[Trade], Optional[Trade]]:
        """Place an OCA bracket: stop-limit sell + limit sell target.

        Both orders share the same ocaGroup with ocaType=1 (cancel remaining
        on fill). When one fills, IBKR auto-cancels the other.

        Returns (stop_trade, target_trade).
        """
        # Stop-limit sell (GTC, outsideRth)
        stop_order = _make_stop_limit_order("SELL", quantity, stop_price)
        stop_order.ocaGroup = oca_group
        stop_order.ocaType = 1  # cancel remaining on fill
        logger.info(
            f"OCA stop: {contract.symbol} trigger=${stop_price:.4f} "
            f"limit=${stop_order.lmtPrice:.4f} oca={oca_group}"
        )
        stop_trade = await self._place(contract, stop_order)

        # Limit sell target (GTC, outsideRth)
        target_order = LimitOrder("SELL", quantity, round_price(target_price))
        target_order.tif = "GTC"
        target_order.outsideRth = True
        target_order.ocaGroup = oca_group
        target_order.ocaType = 1
        logger.info(
            f"OCA target: {contract.symbol} limit=${target_price:.4f} oca={oca_group}"
        )
        target_trade = await self._place(contract, target_order)

        return stop_trade, target_trade

    async def cancel_order(self, trade: Trade) -> bool:
        """Cancel an open order."""
        try:
            self.ib.cancelOrder(trade.order)
            logger.info(f"Cancelled order {trade.order.orderId}")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order: {e}")
            return False

    async def cancel_all_for_symbol(self, symbol: str) -> int:
        """Cancel all open orders for a given symbol."""
        cancelled = 0
        for trade in self.ib.openTrades():
            if trade.contract.symbol == symbol:
                if await self.cancel_order(trade):
                    cancelled += 1
        return cancelled

    async def _place(self, contract: Contract, order: Order) -> Optional[Trade]:
        """Internal: place order with logging."""
        try:
            trade = self.ib.placeOrder(contract, order)
            price_str = getattr(order, "lmtPrice", None) or getattr(order, "auxPrice", "MKT")
            logger.info(
                f"Placed {order.action} {order.orderType} "
                f"{order.totalQuantity} {contract.symbol} @ {price_str}"
            )
            await asyncio.sleep(0.5)
            return trade
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None

    def _get_last_price(self, contract: Contract) -> float:
        """Try to get current price from IBKR ticker data."""
        try:
            ticker = self.ib.ticker(contract)
            if ticker and ticker.last and ticker.last > 0:
                return float(ticker.last)
            if ticker and ticker.close and ticker.close > 0:
                return float(ticker.close)
        except Exception:
            pass
        return 0.0

    def get_open_orders(self) -> list[Trade]:
        """Get all open orders."""
        return self.ib.openTrades()

    def get_open_orders_for_symbol(self, symbol: str) -> list[Trade]:
        """Get open orders for a specific symbol."""
        return [
            t for t in self.ib.openTrades()
            if t.contract.symbol == symbol
        ]
