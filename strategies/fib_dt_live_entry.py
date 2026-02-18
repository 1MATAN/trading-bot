"""Entry execution for live fib double-touch strategy.

Takes a DTEntryRequest from the strategy, performs risk checks,
places a market buy, then split exit orders:
  - OCA bracket (stop + target) for half qty
  - Standalone stop for the other half (trailing exit later)
"""

import logging
import time as _time
from datetime import datetime
from zoneinfo import ZoneInfo

from ib_insync import IB, MarketOrder, StopOrder, LimitOrder

from broker.ibkr_connection import get_connection
from broker.order_manager import OrderManager
from broker.position_manager import PositionManager
from notifications.trade_logger import TradeLogger
from risk.risk_manager import RiskManager
from strategies.fib_dt_live_strategy import (
    DTEntryRequest, DTTrailingExit, FibDTLiveStrategy, FibDTLiveStrategySync,
)
from utils.time_utils import now_et

logger = logging.getLogger("trading_bot.fib_dt_live_entry")


class FibDTLiveEntry:
    """Execute entries from fib double-touch strategy signals."""

    def __init__(
        self,
        order_mgr: OrderManager,
        position_mgr: PositionManager,
        risk_mgr: RiskManager,
        strategy: FibDTLiveStrategy,
        trade_logger: TradeLogger,
    ) -> None:
        self._conn = get_connection()
        self._order_mgr = order_mgr
        self._position_mgr = position_mgr
        self._risk_mgr = risk_mgr
        self._strategy = strategy
        self._trade_logger = trade_logger

    async def execute_entry(self, request: DTEntryRequest) -> bool:
        """Execute a single entry from a DTEntryRequest.

        Steps:
          1. Risk gate (can_trade)
          2. Position sizing -> full qty
          3. Market buy full qty
          4. Compute half = qty // 2, other_half = qty - half
          5. OCA bracket for half qty (stop + target)
          6. Standalone stop for other_half qty
          7. Register position
          8. Log + notify

        Returns True on success.
        """
        symbol = request.symbol

        # 1. Risk gate
        can, reason = self._risk_mgr.can_trade(self._position_mgr.count)
        if not can:
            logger.info(f"[{symbol}] Entry blocked by risk: {reason}")
            return False

        if self._position_mgr.has_position(symbol):
            logger.info(f"[{symbol}] Already in position, skipping")
            return False

        # 2. Position sizing
        qty = self._risk_mgr.calculate_position_size(request.entry_price)
        if qty <= 0:
            logger.warning(f"[{symbol}] Position size is 0, skipping")
            return False

        # Need at least 2 shares for split exit
        if qty < 2:
            logger.warning(f"[{symbol}] Position size {qty} too small for split exit, skipping")
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

        # Get fill price
        fill_price = request.entry_price
        if buy_trade.orderStatus.status == "Filled":
            fill_price = buy_trade.orderStatus.avgFillPrice
        elif buy_trade.fills:
            fill_price = buy_trade.fills[0].execution.avgPrice
        logger.info(f"[{symbol}] Buy filled: {qty} shares @ ${fill_price:.4f}")

        # 4. Compute split quantities
        half = qty // 2
        other_half = qty - half

        # 5. OCA bracket for half qty (stop + target)
        ts = now_et().strftime("%H%M%S")
        oca_group = f"FIBDT_{symbol}_{ts}"

        stop_trade, target_trade = await self._order_mgr.place_oca_bracket(
            request.contract,
            half,
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

        # 6. Standalone stop for other_half qty
        trailing_stop_trade = await self._order_mgr.place_stop_loss(
            request.contract, other_half, request.stop_price,
        )

        trailing_stop_order_id = 0
        if trailing_stop_trade:
            trailing_stop_order_id = trailing_stop_trade.order.orderId

        logger.info(
            f"[{symbol}] Split exit orders placed: "
            f"OCA({half}sh stop+target) + standalone_stop({other_half}sh)"
        )

        # 7. Register position
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
            strategy="fib_dt",
        )

        # Store extra tracking info on position
        pos = self._position_mgr.get_position(symbol)
        if pos:
            pos.stop_order_id = stop_order_id
            pos.trailing_stop_order_id = trailing_stop_order_id
            pos.half_qty = half
            pos.other_half_qty = other_half

        # 8. Update strategy state
        self._strategy.record_entry()
        self._strategy.mark_in_position(symbol)

        # 9. Log trade
        self._trade_logger.log_entry(
            symbol=symbol,
            quantity=qty,
            price=fill_price,
            stop_price=request.stop_price,
            notes=(
                f"fib_dt gap={request.gap_pct:+.1f}% "
                f"fib=${request.fib_level:.4f} ratio={request.fib_ratio} "
                f"target=${request.target_price:.4f} "
                f"split={half}+{other_half} oca={oca_group}"
            ),
        )

        # 10. Telegram notification
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
            f"fib=${request.fib_level:.4f} (ratio={request.fib_ratio}) | "
            f"stop=${request.stop_price:.4f} | "
            f"target=${request.target_price:.4f} | "
            f"gap={request.gap_pct:+.1f}% | "
            f"split={half}+{other_half} | oca={oca_group}"
        )

        return True


# ── Synchronous Entry Executor (for monitor integration) ─────

class FibDTLiveEntrySync:
    """Synchronous entry executor using raw ``ib_insync.IB`` calls.

    Designed for use inside the scanner monitor thread which is not async.
    Places: market buy → OCA bracket (half) → standalone stop (other half).
    """

    def __init__(self, ib_getter, strategy: FibDTLiveStrategySync,
                 buying_power_getter=None, send_telegram_fn=None) -> None:
        """
        Parameters
        ----------
        ib_getter : callable
            Zero-arg function returning an ``IB`` instance (or None).
        strategy : FibDTLiveStrategySync
            The sync strategy instance (for record_entry / mark_in_position).
        buying_power_getter : callable or None
            Zero-arg function returning current buying power as float.
        send_telegram_fn : callable or None
            Function(text: str) -> bool for Telegram notifications.
        """
        self._ib_getter = ib_getter
        self._strategy = strategy
        self._buying_power_getter = buying_power_getter
        self._send_telegram = send_telegram_fn
        self.last_entry_info: dict | None = None

    def execute_entry(self, request: DTEntryRequest) -> bool:
        """Execute a DTEntryRequest synchronously.

        Returns True on success.
        """
        symbol = request.symbol
        ib = self._ib_getter()
        if not ib:
            logger.error(f"[{symbol}] No IBKR connection for entry")
            return False

        # Position sizing
        bp = self._buying_power_getter() if self._buying_power_getter else 0
        if bp <= 0:
            logger.warning(f"[{symbol}] No buying power available")
            return False

        qty = int(bp / request.entry_price)
        if qty < 2:
            logger.warning(f"[{symbol}] Position size {qty} too small for split exit")
            return False

        half = qty // 2
        other_half = qty - half

        try:
            contract = request.contract

            # 1. Market buy
            buy_order = MarketOrder('BUY', qty)
            buy_order.outsideRth = True
            buy_trade = ib.placeOrder(contract, buy_order)
            logger.info(f"[{symbol}] Market BUY {qty} shares — {buy_trade.orderStatus.status}")

            # Get fill price
            fill_price = request.entry_price
            if buy_trade.orderStatus.status == "Filled":
                fill_price = buy_trade.orderStatus.avgFillPrice
            elif buy_trade.fills:
                fill_price = buy_trade.fills[0].execution.avgPrice

            # 2. OCA bracket for first half
            oca_group = f"FibDT_{symbol}_{int(_time.time())}"

            oca_stop = StopOrder('SELL', half, request.stop_price)
            oca_stop.outsideRth = True
            oca_stop.ocaGroup = oca_group
            oca_stop.ocaType = 1
            oca_stop.tif = 'GTC'
            oca_stop_trade = ib.placeOrder(contract, oca_stop)

            oca_target = LimitOrder('SELL', half, request.target_price)
            oca_target.outsideRth = True
            oca_target.ocaGroup = oca_group
            oca_target.ocaType = 1
            oca_target.tif = 'GTC'
            oca_target_trade = ib.placeOrder(contract, oca_target)

            # 3. Standalone stop for other half
            solo_stop = StopOrder('SELL', other_half, request.stop_price)
            solo_stop.outsideRth = True
            solo_stop.tif = 'GTC'
            solo_stop_trade = ib.placeOrder(contract, solo_stop)

            msg = (
                f"FIB DT: BUY {qty} {symbol} @ ~${fill_price:.2f} | "
                f"OCA {half}sh stop ${request.stop_price:.2f}/target ${request.target_price:.2f} | "
                f"Solo stop {other_half}sh ${request.stop_price:.2f}"
            )
            logger.info(msg)

            # Store order info for position monitoring
            self.last_entry_info = {
                'symbol': symbol,
                'contract': contract,
                'qty': qty,
                'half': half,
                'other_half': other_half,
                'entry_price': fill_price,
                'stop_price': request.stop_price,
                'target_price': request.target_price,
                'oca_group': oca_group,
                'oca_target_trade': oca_target_trade,
                'oca_stop_trade': oca_stop_trade,
                'solo_stop_trade': solo_stop_trade,
                'phase': 'IN_POSITION',
            }

            # Update strategy state
            self._strategy.record_entry()
            self._strategy.mark_in_position(symbol)

            # Telegram notification
            if self._send_telegram:
                self._send_telegram(
                    f"📐 <b>FIB DT Auto-Entry</b>\n"
                    f"  Market BUY {qty} {symbol} @ ~${fill_price:.2f}\n"
                    f"  Fib: ${request.fib_level:.4f} (ratio={request.fib_ratio})\n"
                    f"  OCA ({half}sh): stop ${request.stop_price:.2f} / target ${request.target_price:.2f}\n"
                    f"  Standalone stop ({other_half}sh): ${request.stop_price:.2f}\n"
                    f"  Gap: {request.gap_pct:+.1f}%"
                )

            return True

        except Exception as e:
            logger.error(f"[{symbol}] FIB DT entry failed: {e}")
            if self._send_telegram:
                self._send_telegram(f"❌ FIB DT entry failed: {symbol} — {e}")
            return False

    def execute_trailing_exit(self, exit_signal: DTTrailingExit) -> bool:
        """Sell remaining shares for a trailing exit (sync).

        Returns True on success.
        """
        symbol = exit_signal.symbol
        ib = self._ib_getter()
        if not ib:
            logger.error(f"[{symbol}] No IBKR connection for trailing exit")
            return False

        try:
            # Find current position
            qty_to_sell = 0
            for item in ib.portfolio():
                if item.contract.symbol == symbol and item.position > 0:
                    qty_to_sell = int(item.position)
                    break

            if qty_to_sell <= 0:
                logger.info(f"[{symbol}] No position to sell for trailing exit")
                self._strategy.mark_position_closed(symbol)
                return True

            sell_order = MarketOrder('SELL', qty_to_sell)
            sell_order.outsideRth = True
            trade = ib.placeOrder(exit_signal.contract, sell_order)
            logger.info(
                f"[{symbol}] TRAILING EXIT: SELL {qty_to_sell} shares — "
                f"{trade.orderStatus.status} — {exit_signal.reason}"
            )

            self._strategy.mark_position_closed(symbol)

            # Cancel any remaining open orders for this symbol
            open_orders = ib.openOrders()
            for order in open_orders:
                for trade_obj in ib.openTrades():
                    if (trade_obj.contract.symbol == symbol and
                            trade_obj.order.orderId == order.orderId):
                        try:
                            ib.cancelOrder(order)
                            logger.info(f"[{symbol}] Cancelled remaining order {order.orderId}")
                        except Exception:
                            pass

            if self._send_telegram:
                now_et = datetime.now(ZoneInfo('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S ET')
                self._send_telegram(
                    f"📐 <b>FIB DT Exit</b>\n"
                    f"  🕐 {now_et}\n"
                    f"  SELL {qty_to_sell} {symbol}\n"
                    f"  Reason: {exit_signal.reason}"
                )

            return True

        except Exception as e:
            logger.error(f"[{symbol}] Trailing exit failed: {e}")
            return False
