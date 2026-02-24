"""
Standalone Order Execution Thread — independent from the Scanner.

Has its own IBKR connection (clientId 21), processes orders immediately,
and fetches account data on its own loop.

Usage from GUI:
    ot = OrderThread(on_account=..., on_order_result=...)
    ot.start()          # starts immediately, independent of scanner
    ot.submit(req)      # enqueue an order → executed within ~0.5s
    ot.stop()
"""

import asyncio
import logging
import queue
import threading
import time as time_mod
from datetime import datetime
from zoneinfo import ZoneInfo

from ib_insync import IB, Stock, LimitOrder, MarketOrder, Order

from config.settings import MONITOR_ORDER_CLIENT_ID, STOP_LIMIT_OFFSET_PCT

log = logging.getLogger("monitor.orders")

_ET = ZoneInfo("US/Eastern")

_CONNECT_TIMEOUT = 10
_ACCOUNT_POLL_INTERVAL = 5   # seconds between account data refreshes
_ACCOUNT_FETCH_TIMEOUT = 8   # max seconds for a single account data fetch
_STALE_THRESHOLD = 30        # force reconnect if no successful fetch for this long


def _make_stop_limit(action: str, qty: int, stop_price: float,
                     limit_price: float = 0.0) -> Order:
    """Create a STP LMT order that actually triggers outside RTH.

    Plain StopOrder does NOT trigger in pre/after-market on IBKR.
    Only StopLimitOrder with outsideRth=True fires 24h.
    """
    if limit_price <= 0:
        # fallback: 2% offset from stop
        offset = stop_price * STOP_LIMIT_OFFSET_PCT
        if action == "SELL":
            limit_price = round(stop_price - offset, 2)
        else:
            limit_price = round(stop_price + offset, 2)

    order = Order()
    order.action = action
    order.orderType = "STP LMT"
    order.totalQuantity = qty
    order.auxPrice = round(stop_price, 2)    # trigger price
    order.lmtPrice = round(limit_price, 2)   # worst fill price
    order.tif = "GTC"
    order.outsideRth = True
    return order


def _make_trailing_stop(action: str, qty: int, trailing_pct: float) -> Order:
    """Create a TRAIL order (percentage-based) that triggers outside RTH."""
    order = Order()
    order.action = action
    order.orderType = "TRAIL"
    order.totalQuantity = qty
    order.trailingPercent = trailing_pct
    order.tif = "GTC"
    order.outsideRth = True
    return order


class OrderThread(threading.Thread):
    """Independent order execution thread with its own IBKR connection.

    Processes orders from its queue immediately (no waiting for scanner cycles).
    Also periodically fetches account data (NetLiq, positions) for the GUI.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 7497,
                 on_account=None, on_order_result=None, on_live_price=None):
        """
        Parameters
        ----------
        host, port : IBKR TWS connection
        on_account : callable(net_liq, buying_power, positions) or None
        on_order_result : callable(msg, success) or None
        on_live_price : callable(sym, price) or None — called on each price tick
        """
        super().__init__(daemon=True, name="OrderThread")
        self._host = host
        self._port = port
        self.on_account = on_account
        self.on_order_result = on_order_result
        self.on_live_price = on_live_price

        self._queue: queue.Queue = queue.Queue()
        self._running = False
        self._ib: IB | None = None
        self._contract_cache: dict[str, Stock] = {}
        self._last_successful_fetch: float = time_mod.time()

        # Live market data subscription
        self._subscribed_sym: str = ""
        self._subscribed_contract: Stock | None = None
        self._subscribed_ticker = None
        self._live_price: float = 0.0

        # Cached account data (accessible from GUI thread)
        self.net_liq: float = 0.0
        self.buying_power: float = 0.0
        self.positions: dict[str, tuple] = {}  # sym → (qty, avgCost, mktPrice, pnl)

    # ── Public API (called from GUI thread) ──

    def submit(self, req: dict):
        """Enqueue an order for immediate execution."""
        self._queue.put(req)

    def subscribe_market_data(self, sym: str):
        """Thread-safe: enqueue a market data subscription request."""
        self._queue.put({'_type': 'subscribe', 'sym': sym})

    def get_live_price(self) -> float:
        """Thread-safe: read latest live price (float read is atomic)."""
        return self._live_price

    def stop(self):
        self._running = False

    @property
    def connected(self) -> bool:
        return self._ib is not None and self._ib.isConnected()

    # ── Thread loop ──

    def run(self):
        # Ensure asyncio loop exists (ib_insync requirement)
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

        self._running = True
        self._connect()

        # Fetch account data immediately after connection (don't wait 5s)
        if self._ib and self._ib.isConnected():
            self._fetch_account_data()

        last_account = time_mod.time()
        while self._running:
            # Process all pending orders / subscribe requests
            had_orders = False
            while not self._queue.empty():
                try:
                    req = self._queue.get_nowait()
                    if req.get('_type') == 'subscribe':
                        self._handle_subscribe(req['sym'])
                    else:
                        self._execute_order(req)
                        had_orders = True
                except queue.Empty:
                    break

            # After executing orders, force immediate account refresh
            # so SELL buttons see the new position right away.
            if had_orders:
                last_account = 0  # force refresh on next check

            # Periodic account data refresh
            now = time_mod.time()
            if now - last_account >= _ACCOUNT_POLL_INTERVAL:
                self._fetch_account_data()
                last_account = now

            # Use ib.sleep() instead of time.sleep() so ib_insync processes
            # TWS messages (portfolio updates, fills, account data).
            try:
                if self._ib and self._ib.isConnected():
                    self._ib.sleep(0.3)
                else:
                    time_mod.sleep(0.3)
            except Exception:
                time_mod.sleep(0.3)

            # Poll live market data ticker for price updates
            self._poll_live_price()

        # Cleanup
        if self._ib and self._ib.isConnected():
            if self._subscribed_ticker is not None:
                try:
                    self._ib.cancelMktData(self._subscribed_contract)
                except Exception:
                    pass
            log.info("OrderThread: disconnecting IBKR")
            self._ib.disconnect()

    # ── IBKR connection ──

    def _connect(self) -> bool:
        if self._ib and self._ib.isConnected():
            return True
        if self._ib:
            try:
                self._ib.disconnect()
            except Exception:
                pass
        try:
            self._ib = IB()
            self._ib.connect(self._host, self._port,
                             clientId=MONITOR_ORDER_CLIENT_ID,
                             timeout=_CONNECT_TIMEOUT)
            log.info(f"OrderThread: IBKR connected (clientId={MONITOR_ORDER_CLIENT_ID})")

            # ib_insync auto-subscribes to account updates on connect.
            # Pump event loop to receive the initial account snapshot.
            try:
                self._ib.sleep(2)
                log.info("OrderThread: account data ready")
            except Exception:
                pass

            # Re-subscribe to market data if we had an active subscription
            if self._subscribed_sym:
                self._handle_subscribe(self._subscribed_sym)

            return True
        except Exception as e:
            log.error(f"OrderThread: IBKR connect failed: {e}")
            self._ib = None
            return False

    def _ensure_connected(self) -> IB | None:
        """Return connected IB instance, reconnecting if needed."""
        if self._ib and self._ib.isConnected():
            return self._ib
        log.warning("OrderThread: IBKR disconnected, reconnecting...")
        if self._connect():
            return self._ib
        return None

    # ── Contract resolution ──

    def _get_contract(self, sym: str) -> Stock | None:
        if sym in self._contract_cache:
            return self._contract_cache[sym]
        ib = self._ensure_connected()
        if not ib:
            return None
        contract = Stock(sym, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        if contract.conId:
            self._contract_cache[sym] = contract
        return contract if contract.conId else None

    # ── Account data ──

    def _fetch_account_data(self):
        # Force reconnect if we haven't had a successful fetch recently
        elapsed_since_ok = time_mod.time() - self._last_successful_fetch
        if elapsed_since_ok > _STALE_THRESHOLD:
            log.warning(f"OrderThread: no successful fetch for {elapsed_since_ok:.0f}s, forcing reconnect")
            if self._ib:
                try:
                    self._ib.disconnect()
                except Exception:
                    pass
                self._ib = None

        ib = self._ensure_connected()
        if not ib:
            log.warning("OrderThread: cannot fetch account — not connected")
            return
        try:
            ib.sleep(0.2)  # pump event loop to get latest TWS data
            acct_vals = ib.accountValues()
            if not acct_vals:
                log.warning("OrderThread: accountValues() returned empty — waiting for subscription data")
                ib.sleep(1)  # give more time for TWS to send data
                acct_vals = ib.accountValues()
            net_liq = 0.0
            buying_power = 0.0
            for av in acct_vals:
                if av.tag == 'NetLiquidation' and av.currency == 'USD':
                    net_liq = float(av.value)
                elif av.tag == 'BuyingPower' and av.currency == 'USD':
                    buying_power = float(av.value)

            # Use ib.positions() (reqPositions) — more reliable than ib.portfolio()
            # which can return empty on secondary clientId connections.
            positions = {}
            for item in ib.positions():
                if item.position == 0:
                    continue
                sym = item.contract.symbol
                positions[sym] = (
                    int(item.position),
                    round(item.avgCost, 4),
                    0.0,  # marketPrice not available from positions()
                    0.0,  # unrealizedPNL not available from positions()
                )
            # Enrich with portfolio data if available (has marketPrice, PNL)
            for item in ib.portfolio():
                if item.position == 0:
                    continue
                sym = item.contract.symbol
                positions[sym] = (
                    int(item.position),
                    round(item.averageCost, 4),
                    round(item.marketPrice, 4),
                    round(item.unrealizedPNL, 2),
                )

            # If NetLiq is 0, pump event loop more to let data arrive
            if net_liq <= 0 and ib.isConnected():
                log.warning("OrderThread: NetLiq=0, pumping event loop for account data")
                try:
                    ib.sleep(2)
                    acct_vals = ib.accountValues()
                    for av in acct_vals:
                        if av.tag == 'NetLiquidation' and av.currency == 'USD':
                            net_liq = float(av.value)
                        elif av.tag == 'BuyingPower' and av.currency == 'USD':
                            buying_power = float(av.value)
                except Exception as e:
                    log.warning(f"OrderThread: retry account fetch failed: {e}")

            self.net_liq = net_liq
            self.buying_power = buying_power
            self.positions = positions
            self._last_successful_fetch = time_mod.time()

            if positions:
                pos_str = ", ".join(f"{s} {p[0]}@${p[1]:.2f}" for s, p in positions.items())
                log.info(f"OrderThread: NetLiq=${net_liq:,.0f} BP=${buying_power:,.0f} Positions: {pos_str}")
            else:
                log.info(f"OrderThread: NetLiq=${net_liq:,.0f} BP=${buying_power:,.0f} Positions: (none)")

            if self.on_account:
                self.on_account(net_liq, buying_power, positions)
        except Exception as e:
            log.error(f"OrderThread: account fetch failed: {e}")
            # If fetch keeps failing, force reconnect sooner
            if time_mod.time() - self._last_successful_fetch > _STALE_THRESHOLD / 2:
                log.warning("OrderThread: consecutive failures, will reconnect next cycle")
                if self._ib:
                    try:
                        self._ib.disconnect()
                    except Exception:
                        pass
                    self._ib = None

    # ── Live market data ──

    def _handle_subscribe(self, sym: str):
        """Subscribe to streaming market data for a symbol."""
        ib = self._ensure_connected()
        if not ib:
            return
        # Cancel previous subscription
        if self._subscribed_ticker is not None and self._subscribed_contract is not None:
            try:
                ib.cancelMktData(self._subscribed_contract)
            except Exception:
                pass
            self._subscribed_ticker = None
            self._live_price = 0.0

        contract = self._get_contract(sym)
        if not contract:
            log.warning(f"OrderThread: cannot subscribe — failed to qualify {sym}")
            return

        self._subscribed_sym = sym
        self._subscribed_contract = contract
        self._subscribed_ticker = ib.reqMktData(contract, '', False, False)
        log.info(f"OrderThread: subscribed to market data for {sym}")

    def _poll_live_price(self):
        """Check the subscribed ticker for price updates and fire callback."""
        ticker = self._subscribed_ticker
        if ticker is None:
            return
        price = ticker.marketPrice()
        if price != price:  # NaN check
            price = ticker.last
        if price != price:  # still NaN
            return
        if price <= 0:
            return
        if price != self._live_price:
            self._live_price = price
            if self.on_live_price and self._subscribed_sym:
                try:
                    self.on_live_price(self._subscribed_sym, price)
                except Exception as e:
                    log.debug(f"on_live_price callback error: {e}")

    # ── Order execution ──

    def _execute_order(self, req: dict):
        """Route and execute an order request."""
        if req.get('strategy') == 'fib_dt':
            self._execute_fib_dt_order(req)
        else:
            self._execute_standard_order(req)

    def _execute_standard_order(self, req: dict):
        """Place a standard limit order. BUY orders include automatic stop-loss."""
        sym = req['sym']
        action = req['action']
        qty = req['qty']
        price = req['price']
        stop_price = req.get('stop_price', 0)    # 0 means no stop-loss (SELL orders)
        limit_price = req.get('limit_price', 0)  # worst fill price for STP LMT

        ib = self._ensure_connected()
        if not ib:
            self._report("IBKR not connected", False)
            return

        try:
            contract = self._get_contract(sym)
            if not contract:
                raise RuntimeError(f"Could not qualify contract for {sym}")

            # Cancel existing open orders for this symbol first (e.g. stop-losses)
            if req.get('cancel_existing'):
                open_trades = ib.openTrades()
                cancelled = 0
                for trade in open_trades:
                    if trade.contract.symbol == sym and trade.orderStatus.status in (
                        'PreSubmitted', 'Submitted', 'PendingSubmit'
                    ):
                        ib.cancelOrder(trade.order)
                        cancelled += 1
                        log.info(f"Cancelled open order: {trade.order.action} {trade.order.totalQuantity} {sym} "
                                 f"({trade.order.orderType} @ ${trade.order.lmtPrice})")
                if cancelled:
                    ib.sleep(1)  # wait for cancellations to process
                    log.info(f"Cancelled {cancelled} open order(s) for {sym}")

            from monitor.screen_monitor import _get_market_session
            session = _get_market_session()
            outside_rth = session != 'market'

            # Place stop-loss for BUY orders (STP LMT — triggers outside RTH)
            stop_msg = ""
            trailing_pct = req.get('trailing_pct', 0)
            target_price = req.get('target_price', 0)
            trail_msg = ""

            # ── Determine if we need a parentId bracket ──
            use_parent_bracket = (action == 'BUY' and stop_price > 0
                                  and target_price <= 0 and trailing_pct > 0)

            if use_parent_bracket:
                # ── parentId bracket: BUY(parent) → Stop + Trail (children) ──
                # parentId tells IBKR these are protective orders — no extra margin.
                # OCA on stop+trail ensures mutual cancellation on fill.
                parent_order = LimitOrder(action, qty, price)
                parent_order.outsideRth = outside_rth
                parent_order.tif = 'DAY'
                parent_order.orderId = ib.client.getReqId()
                parent_order.transmit = False  # don't send yet

                trade = ib.placeOrder(contract, parent_order)

                oca_group = f"Bracket_{sym}_{int(time_mod.time())}"

                stop_order = _make_stop_limit('SELL', qty, stop_price, limit_price)
                stop_order.parentId = parent_order.orderId
                stop_order.ocaGroup = oca_group
                stop_order.ocaType = 1
                stop_order.transmit = False  # not yet

                stop_trade = ib.placeOrder(contract, stop_order)

                trail_order = _make_trailing_stop('SELL', qty, trailing_pct)
                trail_order.parentId = parent_order.orderId
                trail_order.ocaGroup = oca_group
                trail_order.ocaType = 1
                trail_order.transmit = True  # last child → sends the entire bracket

                trail_trade = ib.placeOrder(contract, trail_order)
                ib.sleep(3)  # wait for all orders to process

                status = trade.orderStatus.status
                if status in ('Inactive', 'Cancelled'):
                    err_log = [e for e in trade.log if e.errorCode]
                    reason = err_log[-1].message if err_log else "Unknown"
                    msg = f"Order REJECTED: {action} {qty} {sym} — {reason}"
                    log.error(msg)
                    self._report(msg, False)
                    self._telegram(
                        f"❌ <b>Order Rejected</b>\n"
                        f"  {action} {qty} {sym} @ ${price:.2f}\n"
                        f"  Reason: {reason}"
                    )
                    return

                stop_status = stop_trade.orderStatus.status
                trail_status = trail_trade.orderStatus.status
                stop_msg = f" | Stop ${stop_price:.2f} ({stop_status})"
                trail_msg = f" | Trail {trailing_pct}% ({trail_status})"
                log.info(f"parentId bracket: BUY {qty} {sym} @ ${price:.2f} — {status}")
                log.info(f"  Stop: SELL {qty} @ ${stop_price:.2f} (STP LMT) — {stop_status} [parent={parent_order.orderId}]")
                log.info(f"  Trail: SELL {qty} trail {trailing_pct}% — {trail_status} [parent={parent_order.orderId}, OCA={oca_group}]")

            else:
                # ── Standard order (no parentId bracket needed) ──
                order = LimitOrder(action, qty, price)
                order.outsideRth = outside_rth
                order.tif = 'DAY'
                trade = ib.placeOrder(contract, order)
                ib.sleep(3)

                status = trade.orderStatus.status
                if status in ('Inactive', 'Cancelled'):
                    err_log = [e for e in trade.log if e.errorCode]
                    reason = err_log[-1].message if err_log else "Unknown"
                    msg = f"Order REJECTED: {action} {qty} {sym} — {reason}"
                    log.error(msg)
                    self._report(msg, False)
                    self._telegram(
                        f"❌ <b>Order Rejected</b>\n"
                        f"  {action} {qty} {sym} @ ${price:.2f}\n"
                        f"  Reason: {reason}"
                    )
                    return

                if action == 'BUY' and stop_price > 0 and target_price > 0:
                    # ── OCA bracket: Stop + TP cancel each other ──
                    oca_group = f"Bracket_{sym}_{int(time_mod.time())}"

                    stop_order = _make_stop_limit('SELL', qty, stop_price, limit_price)
                    stop_order.ocaGroup = oca_group
                    stop_order.ocaType = 1
                    stop_trade = ib.placeOrder(contract, stop_order)
                    ib.sleep(1)
                    stop_status = stop_trade.orderStatus.status
                    stop_msg = f" | Stop ${stop_price:.2f} ({stop_status})"
                    log.info(f"OCA Stop placed: SELL {qty} {sym} @ ${stop_price:.2f} (STP LMT) — {stop_status}")

                    tp_order = LimitOrder('SELL', qty, target_price)
                    tp_order.outsideRth = True
                    tp_order.tif = 'GTC'
                    tp_order.ocaGroup = oca_group
                    tp_order.ocaType = 1
                    tp_trade = ib.placeOrder(contract, tp_order)
                    ib.sleep(1)
                    tp_status = tp_trade.orderStatus.status
                    trail_msg = f" | TP ${target_price:.2f} ({tp_status})"
                    log.info(f"OCA TP placed: SELL {qty} {sym} @ ${target_price:.2f} (LMT GTC) — {tp_status} [OCA={oca_group}]")

                elif action == 'BUY' and stop_price > 0:
                    # ── Stop only (no trailing, no TP) ──
                    stop_order = _make_stop_limit('SELL', qty, stop_price, limit_price)
                    stop_trade = ib.placeOrder(contract, stop_order)
                    ib.sleep(1)
                    stop_status = stop_trade.orderStatus.status
                    stop_msg = f" | Stop ${stop_price:.2f} ({stop_status})"
                    log.info(f"Stop-loss placed: SELL {qty} {sym} @ ${stop_price:.2f} (STP LMT) — {stop_status}")

            # Place stop-loss on remaining shares for partial SELL orders
            stop_remaining_qty = req.get('stop_remaining_qty', 0)
            if action == 'SELL' and stop_remaining_qty > 0 and stop_price > 0:
                stop_order = _make_stop_limit('SELL', stop_remaining_qty, stop_price, limit_price)
                stop_trade = ib.placeOrder(contract, stop_order)
                ib.sleep(1)
                stop_status = stop_trade.orderStatus.status
                stop_msg = f" | Stop {stop_remaining_qty}sh ${stop_price:.2f} ({stop_status})"
                log.info(f"Stop-loss on remaining: SELL {stop_remaining_qty} {sym} @ ${stop_price:.2f} (STP LMT) — {stop_status}")

            msg = f"{action} {qty} {sym} @ ${price:.2f} — {status}{stop_msg}{trail_msg}"
            log.info(f"Order placed: {msg}")
            self._report(msg, True)
            stop_desc = req.get('stop_desc', f"${stop_price:.2f}")
            if action == 'BUY' and stop_price > 0:
                bracket_type = "parentId" if use_parent_bracket else "OCA"
                tg_lines = [
                    f"📋 <b>Order Placed + Bracket ({bracket_type})</b>",
                    f"  BUY {qty} {sym} @ ${price:.2f}",
                    f"  Status: {status}",
                    f"  🛑 Stop: {stop_desc} → Lmt ${limit_price:.2f} [STP LMT GTC]",
                ]
                if target_price > 0:
                    tg_lines.append(f"  🎯 TP: ${target_price:.2f} [LMT SELL GTC]")
                    tg_lines.append(f"  🔗 OCA: Stop ↔ TP (אחד מבטל את השני)")
                elif trailing_pct > 0:
                    tg_lines.append(f"  📈 Trail: {trailing_pct}% trailing stop [GTC]")
                    tg_lines.append(f"  🔗 parentId bracket + OCA (stop ↔ trail)")
                rth_icon = "✓" if outside_rth else "✗"
                tg_lines.append(f"  outsideRth: {rth_icon}  |  TIF: DAY (buy) + GTC (stop/trail)")
                self._telegram("\n".join(tg_lines))
            elif action == 'SELL' and stop_remaining_qty > 0 and stop_price > 0:
                self._telegram(
                    f"📋 <b>Order Placed + Stop on Remaining</b>\n"
                    f"  SELL {qty} {sym} @ ${price:.2f}\n"
                    f"  Status: {status}\n"
                    f"  🛑 Stop on remaining {stop_remaining_qty}sh: {stop_desc} → Lmt ${limit_price:.2f} [STP LMT GTC]\n"
                    f"  outsideRth: {'✓' if outside_rth else '✗'}  |  TIF: DAY (sell) + GTC (stop)"
                )
            else:
                self._telegram(
                    f"📋 <b>Order Placed</b>\n"
                    f"  {action} {qty} {sym} @ ${price:.2f}\n"
                    f"  Status: {status}\n"
                    f"  outsideRth: {'✓' if outside_rth else '✗'}  |  TIF: DAY"
                )
        except Exception as e:
            msg = f"Order failed: {action} {qty} {sym} — {e}"
            log.error(msg)
            self._report(msg, False)

    def _execute_fib_dt_order(self, req: dict):
        """Execute Fib Double-Touch split-exit bracket order."""
        sym = req['sym']
        qty = req['qty']
        entry_price = req['price']
        stop_price = req['stop_price']
        target_price = req['target_price']
        half = req['half']
        other_half = req['other_half']

        ib = self._ensure_connected()
        if not ib:
            self._report("IBKR not connected", False)
            return

        try:
            contract = self._get_contract(sym)
            if not contract:
                raise RuntimeError(f"Could not qualify contract for {sym}")

            # 1. Limit buy full qty (works in pre/after market)
            buy_order = LimitOrder('BUY', qty, entry_price)
            buy_order.outsideRth = True
            buy_order.tif = 'DAY'
            buy_trade = ib.placeOrder(contract, buy_order)
            ib.sleep(3)

            buy_status = buy_trade.orderStatus.status
            if buy_status in ('Inactive', 'Cancelled'):
                err_log = [e for e in buy_trade.log if e.errorCode]
                reason = err_log[-1].message if err_log else "Unknown"
                msg = f"FIB DT REJECTED: BUY {qty} {sym} @ ${entry_price:.2f} — {reason}"
                log.error(msg)
                self._report(msg, False)
                self._telegram(
                    f"❌ <b>FIB DT Order Rejected</b>\n"
                    f"  BUY {qty} {sym} @ ${entry_price:.2f}\n"
                    f"  Reason: {reason}"
                )
                return
            log.info(f"FIB DT: Limit BUY {qty} {sym} @ ${entry_price:.2f} — {buy_status}")

            # 2. OCA bracket for first half (STP LMT — triggers outside RTH)
            oca_group = f"FibDT_{sym}_{int(time_mod.time())}"

            oca_stop = _make_stop_limit('SELL', half, stop_price)
            oca_stop.ocaGroup = oca_group
            oca_stop.ocaType = 1
            ib.placeOrder(contract, oca_stop)

            oca_target = LimitOrder('SELL', half, target_price)
            oca_target.outsideRth = True
            oca_target.ocaGroup = oca_group
            oca_target.ocaType = 1
            oca_target.tif = 'GTC'
            ib.placeOrder(contract, oca_target)

            # 3. Standalone stop for other half (STP LMT — triggers outside RTH)
            solo_stop = _make_stop_limit('SELL', other_half, stop_price)
            ib.placeOrder(contract, solo_stop)

            msg = (f"FIB DT: BUY {qty} {sym} @ ${entry_price:.2f} | "
                   f"OCA {half}sh stop ${stop_price:.2f}/target ${target_price:.2f} | "
                   f"Solo stop {other_half}sh ${stop_price:.2f}")
            log.info(msg)
            self._report(msg, True)

            now_et = datetime.now(_ET).strftime('%Y-%m-%d %H:%M:%S ET')
            self._telegram(
                f"📐 <b>FIB DT Entry</b>\n"
                f"  🕐 {now_et}\n"
                f"  Limit BUY {qty} {sym} @ ${entry_price:.2f}\n"
                f"  OCA ({half}sh): stop ${stop_price:.2f} / target ${target_price:.2f}\n"
                f"  Standalone stop ({other_half}sh): ${stop_price:.2f}\n"
                f"  outsideRth: ✓  |  TIF: DAY"
            )
        except Exception as e:
            msg = f"FIB DT failed: {sym} — {e}"
            log.error(msg)
            self._report(msg, False)

    # ── Helpers ──

    def _report(self, msg: str, success: bool):
        if self.on_order_result:
            self.on_order_result(msg, success)

    def _telegram(self, text: str):
        """Send Telegram notification (import lazily to avoid circular deps)."""
        try:
            # Import send_telegram from the monitor module
            from monitor.screen_monitor import send_telegram
            send_telegram(text)
        except Exception as e:
            log.debug(f"Telegram send failed: {e}")
