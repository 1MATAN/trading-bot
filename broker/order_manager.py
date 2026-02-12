"""Place, modify, and cancel orders. GTC server-side stop-losses."""

import asyncio
import logging
from typing import Optional

from ib_insync import (
    IB, Contract, Order, Trade,
    LimitOrder, MarketOrder, StopOrder,
)

from broker.ibkr_connection import get_connection
from utils.helpers import round_price

logger = logging.getLogger("trading_bot.orders")


class OrderManager:
    """Handles order lifecycle with IBKR."""

    def __init__(self) -> None:
        self._conn = get_connection()

    @property
    def ib(self) -> IB:
        return self._conn.ib

    async def place_market_buy(
        self, contract: Contract, quantity: int
    ) -> Optional[Trade]:
        """Place a market buy order."""
        order = MarketOrder("BUY", quantity)
        order.tif = "DAY"
        return await self._place(contract, order)

    async def place_limit_buy(
        self, contract: Contract, quantity: int, limit_price: float
    ) -> Optional[Trade]:
        """Place a limit buy order."""
        order = LimitOrder("BUY", quantity, round_price(limit_price))
        order.tif = "DAY"
        return await self._place(contract, order)

    async def place_market_sell(
        self, contract: Contract, quantity: int
    ) -> Optional[Trade]:
        """Place a market sell order."""
        order = MarketOrder("SELL", quantity)
        order.tif = "DAY"
        return await self._place(contract, order)

    async def place_limit_sell(
        self, contract: Contract, quantity: int, limit_price: float
    ) -> Optional[Trade]:
        """Place a limit sell order."""
        order = LimitOrder("SELL", quantity, round_price(limit_price))
        order.tif = "DAY"
        return await self._place(contract, order)

    async def place_stop_loss(
        self, contract: Contract, quantity: int, stop_price: float
    ) -> Optional[Trade]:
        """Place a GTC stop-loss order (server-side, survives bot crashes)."""
        order = StopOrder("SELL", quantity, round_price(stop_price))
        order.tif = "GTC"  # Good-Til-Cancelled — enforced by IBKR
        order.outsideRth = True  # trigger even outside regular hours
        return await self._place(contract, order)

    async def modify_stop_loss(
        self, trade: Trade, new_stop_price: float
    ) -> Optional[Trade]:
        """Modify an existing stop-loss order price (only moves up)."""
        if trade.order.auxPrice >= new_stop_price:
            logger.debug(
                f"Stop for {trade.contract.symbol} already at "
                f"{trade.order.auxPrice} >= {new_stop_price}, skipping"
            )
            return trade
        trade.order.auxPrice = round_price(new_stop_price)
        try:
            self.ib.placeOrder(trade.contract, trade.order)
            logger.info(
                f"Modified stop for {trade.contract.symbol} → "
                f"${new_stop_price:.4f}"
            )
            return trade
        except Exception as e:
            logger.error(f"Failed to modify stop: {e}")
            return None

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
            logger.info(
                f"Placed {order.action} {order.orderType} "
                f"{order.totalQuantity} {contract.symbol} "
                f"@ {getattr(order, 'lmtPrice', getattr(order, 'auxPrice', 'MKT'))}"
            )
            # Wait briefly for order acknowledgement
            await asyncio.sleep(0.5)
            return trade
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None

    def get_open_orders(self) -> list[Trade]:
        """Get all open orders."""
        return self.ib.openTrades()

    def get_open_orders_for_symbol(self, symbol: str) -> list[Trade]:
        """Get open orders for a specific symbol."""
        return [
            t for t in self.ib.openTrades()
            if t.contract.symbol == symbol
        ]
