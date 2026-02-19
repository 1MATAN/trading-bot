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

from ib_insync import IB, Stock, LimitOrder, MarketOrder, StopOrder

from config.settings import MONITOR_ORDER_CLIENT_ID

log = logging.getLogger("monitor.orders")

_ET = ZoneInfo("US/Eastern")

_CONNECT_TIMEOUT = 10
_ACCOUNT_POLL_INTERVAL = 5   # seconds between account data refreshes


class OrderThread(threading.Thread):
    """Independent order execution thread with its own IBKR connection.

    Processes orders from its queue immediately (no waiting for scanner cycles).
    Also periodically fetches account data (NetLiq, positions) for the GUI.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 7497,
                 on_account=None, on_order_result=None):
        """
        Parameters
        ----------
        host, port : IBKR TWS connection
        on_account : callable(net_liq, buying_power, positions) or None
        on_order_result : callable(msg, success) or None
        """
        super().__init__(daemon=True, name="OrderThread")
        self._host = host
        self._port = port
        self.on_account = on_account
        self.on_order_result = on_order_result

        self._queue: queue.Queue = queue.Queue()
        self._running = False
        self._ib: IB | None = None
        self._contract_cache: dict[str, Stock] = {}

        # Cached account data (accessible from GUI thread)
        self.net_liq: float = 0.0
        self.buying_power: float = 0.0
        self.positions: dict[str, tuple] = {}  # sym → (qty, avgCost, mktPrice, pnl)

    # ── Public API (called from GUI thread) ──

    def submit(self, req: dict):
        """Enqueue an order for immediate execution."""
        self._queue.put(req)

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

        last_account = 0.0
        while self._running:
            # Process all pending orders immediately
            while not self._queue.empty():
                try:
                    req = self._queue.get_nowait()
                    self._execute_order(req)
                except queue.Empty:
                    break

            # Periodic account data refresh
            now = time_mod.time()
            if now - last_account >= _ACCOUNT_POLL_INTERVAL:
                self._fetch_account_data()
                last_account = now

            time_mod.sleep(0.3)  # responsive loop — checks queue ~3x/sec

        # Cleanup
        if self._ib and self._ib.isConnected():
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
        ib = self._ensure_connected()
        if not ib:
            return
        try:
            acct_vals = ib.accountValues()
            net_liq = 0.0
            buying_power = 0.0
            for av in acct_vals:
                if av.tag == 'NetLiquidation' and av.currency == 'USD':
                    net_liq = float(av.value)
                elif av.tag == 'BuyingPower' and av.currency == 'USD':
                    buying_power = float(av.value)

            positions = {}
            for item in ib.portfolio():
                sym = item.contract.symbol
                positions[sym] = (
                    int(item.position),
                    round(item.averageCost, 4),
                    round(item.marketPrice, 4),
                    round(item.unrealizedPNL, 2),
                )

            self.net_liq = net_liq
            self.buying_power = buying_power
            self.positions = positions

            if self.on_account:
                self.on_account(net_liq, buying_power, positions)
        except Exception as e:
            log.error(f"OrderThread: account fetch failed: {e}")

    # ── Order execution ──

    def _execute_order(self, req: dict):
        """Route and execute an order request."""
        if req.get('strategy') == 'fib_dt':
            self._execute_fib_dt_order(req)
        else:
            self._execute_standard_order(req)

    def _execute_standard_order(self, req: dict):
        """Place a standard limit order."""
        sym = req['sym']
        action = req['action']
        qty = req['qty']
        price = req['price']

        ib = self._ensure_connected()
        if not ib:
            self._report("IBKR not connected", False)
            return

        try:
            contract = self._get_contract(sym)
            if not contract:
                raise RuntimeError(f"Could not qualify contract for {sym}")

            order = LimitOrder(action, qty, price)
            order.outsideRth = True
            order.tif = 'DAY'
            trade = ib.placeOrder(contract, order)
            ib.sleep(3)  # wait for async status callback

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

            msg = f"{action} {qty} {sym} @ ${price:.2f} — {status}"
            log.info(f"Order placed: {msg}")
            self._report(msg, True)
            self._telegram(
                f"📋 <b>Order Placed</b>\n"
                f"  {action} {qty} {sym} @ ${price:.2f}\n"
                f"  Status: {status}\n"
                f"  outsideRth: ✓  |  TIF: DAY"
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

            # 2. OCA bracket for first half
            oca_group = f"FibDT_{sym}_{int(time_mod.time())}"

            oca_stop = StopOrder('SELL', half, stop_price)
            oca_stop.outsideRth = True
            oca_stop.ocaGroup = oca_group
            oca_stop.ocaType = 1
            oca_stop.tif = 'GTC'
            ib.placeOrder(contract, oca_stop)

            oca_target = LimitOrder('SELL', half, target_price)
            oca_target.outsideRth = True
            oca_target.ocaGroup = oca_group
            oca_target.ocaType = 1
            oca_target.tif = 'GTC'
            ib.placeOrder(contract, oca_target)

            # 3. Standalone stop for other half
            solo_stop = StopOrder('SELL', other_half, stop_price)
            solo_stop.outsideRth = True
            solo_stop.tif = 'GTC'
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
