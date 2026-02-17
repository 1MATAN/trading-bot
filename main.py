"""Main orchestrator — wires all components, runs async event loop.

Usage:
    python main.py              # Run trading bot (live/paper)
    python main.py --fib-live   # Run Fibonacci live paper trading bot
    python main.py --fib-dt     # Run Fibonacci double-touch live paper trading bot
    python main.py --fib-strength # Run multi-TF fib strength backtest
    python main.py --simulate   # Run simulation engine
    python main.py --backtest   # Run backtester
    python main.py --dashboard  # Launch dashboard only
"""

import argparse
import asyncio
import json
import logging
import subprocess
import sys
from pathlib import Path

from config.settings import (
    LIVE_STATE_PATH,
    CONTROL_PATH,
    DASHBOARD_PORT,
    SCAN_INTERVAL_SECONDS,
    PAPER_TRADING,
    STARTING_CAPITAL,
    SIMULATION_MODE,
    FIB_LIVE_SCAN_INTERVAL,
    FIB_CONFIRM_SCAN_INTERVAL,
    FIB_DT_LIVE_SCAN_INTERVAL,
    FIB_DT_LIVE_GAP_MIN_PCT,
    FIB_DT_LIVE_GAP_MAX_PCT,
    FIB_DT_LIVE_FLOAT_MAX,
    FIB_DT_LIVE_MAX_SYMBOLS,
)
from utils.helpers import setup_logging
from utils.time_utils import (
    is_market_open,
    is_premarket,
    is_afterhours,
    is_any_session_active,
    seconds_until_next_session,
    current_session_name,
    now_et,
    format_timestamp,
    now_utc,
)

logger = setup_logging("trading_bot")


class TradingBot:
    """Main trading bot orchestrator."""

    def __init__(self) -> None:
        self._running = False
        self._dashboard_proc = None

        # Lazy-initialized components
        self._conn = None
        self._order_mgr = None
        self._position_mgr = None
        self._risk_mgr = None
        self._trailing_mgr = None
        self._momentum = None
        self._scanner = None
        self._trade_logger = None

    async def start(self) -> None:
        """Initialize all components and start the main loop."""
        logger.info("=" * 60)
        logger.info("Penny Stock Trading Bot Starting")
        logger.info(f"Mode: {'PAPER' if PAPER_TRADING else 'LIVE'}")
        logger.info(f"Starting Capital: ${STARTING_CAPITAL:,.2f}")
        logger.info(f"Time: {format_timestamp(now_utc())}")
        logger.info("=" * 60)

        # Initialize components
        await self._init_components()

        # Connect to IBKR
        if not await self._conn.connect():
            logger.critical("Cannot connect to IBKR — exiting")
            return

        # Sync positions on startup
        await self._position_mgr.sync_with_ibkr()

        # Update portfolio value
        try:
            summary = await self._conn.get_account_summary()
            if "NetLiquidation" in summary:
                self._risk_mgr.update_portfolio_value(summary["NetLiquidation"])
                logger.info(f"Portfolio value: ${summary['NetLiquidation']:,.2f}")
        except Exception as e:
            logger.warning(f"Could not fetch account summary: {e}")

        # Send startup notification
        from notifications.telegram_bot import notify_system
        await notify_system(
            f"Bot started in {'PAPER' if PAPER_TRADING else 'LIVE'} mode\n"
            f"Capital: ${STARTING_CAPITAL:,.2f}\n"
            f"Positions: {self._position_mgr.count}"
        )

        # Launch dashboard
        self._launch_dashboard()

        # Start main loop
        self._running = True
        await self._main_loop()

    async def _init_components(self) -> None:
        """Initialize all trading components."""
        from broker.ibkr_connection import get_connection
        from broker.order_manager import OrderManager
        from broker.position_manager import PositionManager
        from risk.risk_manager import RiskManager
        from risk.trailing_stop import TrailingStopManager
        from strategies.momentum_entry import MomentumEntry
        from scanner.realtime_scanner import RealtimeScanner
        from notifications.trade_logger import TradeLogger

        self._conn = get_connection()
        self._order_mgr = OrderManager()
        self._position_mgr = PositionManager()
        self._risk_mgr = RiskManager()
        self._trailing_mgr = TrailingStopManager(self._order_mgr, self._position_mgr)
        self._momentum = MomentumEntry(
            self._order_mgr, self._position_mgr,
            self._risk_mgr, self._trailing_mgr,
        )
        self._scanner = RealtimeScanner()
        self._trade_logger = TradeLogger()

    async def _main_loop(self) -> None:
        """Main event loop — runs during all trading sessions (pre/regular/after)."""
        trailing_task = asyncio.create_task(self._trailing_mgr.start())

        try:
            while self._running:
                self._check_control_commands()

                if is_any_session_active():
                    session = current_session_name()
                    await self._run_trading_cycle(session)
                else:
                    wait = seconds_until_next_session()
                    if wait > 300:
                        logger.info(
                            f"All sessions closed. Next session in "
                            f"{wait / 3600:.1f} hours. Sleeping 60s..."
                        )
                        await asyncio.sleep(60)
                        continue
                    else:
                        logger.info(
                            f"Next session starts in {wait:.0f}s, standing by..."
                        )
                        await asyncio.sleep(10)
                        continue

                # Write state for dashboard
                self._write_live_state()

                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            logger.info("Main loop cancelled")
        except Exception as e:
            logger.critical(f"Unhandled error in main loop: {e}", exc_info=True)
        finally:
            self._trailing_mgr.stop()
            trailing_task.cancel()
            await self._shutdown()

    async def _run_trading_cycle(self, session: str) -> None:
        """Run one scan + trade cycle for any session (pre/regular/after).

        Args:
            session: Current session name ("pre-market", "regular", "after-hours").
        """
        # Scan for candidates (all sessions — scanner uses use_rth=False)
        signals = await self._scanner.scan_once()

        # Execute entries for passing signals
        for signal in signals:
            try:
                entered = await self._momentum.execute_entry(signal)
                if entered:
                    pos = self._position_mgr.get_position(signal.symbol)
                    self._trade_logger.log_entry(
                        symbol=signal.symbol,
                        quantity=pos.quantity,
                        price=signal.current_price,
                        stop_price=pos.stop_loss_price,
                        notes=f"session={session}",
                    )
                    from notifications.telegram_bot import notify_entry
                    await notify_entry(
                        symbol=signal.symbol,
                        quantity=pos.quantity,
                        price=pos.entry_price,
                        stop_price=pos.stop_loss_price,
                        risk_dollars=(pos.entry_price - pos.stop_loss_price) * pos.quantity,
                    )
                    logger.info(
                        f"ENTRY [{session}]: {signal.symbol} — "
                        f"{pos.quantity} shares @ ${pos.entry_price:.4f}"
                    )
            except Exception as e:
                logger.error(f"Entry execution error for {signal.symbol}: {e}")

        # Check trailing stop exits for existing positions (all sessions)
        await self._check_trailing_stop_exits(session)

        # Sync with IBKR periodically
        await self._position_mgr.sync_with_ibkr()

        # Update daily summary
        try:
            summary = await self._conn.get_account_summary()
            if "NetLiquidation" in summary:
                self._risk_mgr.update_portfolio_value(summary["NetLiquidation"])
                self._trade_logger.update_daily_summary(summary["NetLiquidation"])
        except Exception:
            pass

    async def _check_trailing_stop_exits(self, session: str = "regular") -> None:
        """Check all positions for trailing stop exit (prev candle low).

        Runs during ALL sessions (pre-market, regular, after-hours).
        Uses use_rth=False to include extended hours data.
        """
        from strategies.signal_checker import get_trailing_stop_price
        from strategies.indicators import bars_to_dataframe

        for symbol in list(self._position_mgr.get_all_symbols()):
            pos = self._position_mgr.get_position(symbol)
            if pos is None:
                continue
            try:
                # Fetch 1-min data including extended hours
                bars_1m = await self._conn.get_historical_data(
                    pos.contract,
                    duration="1 D",
                    bar_size="1 min",
                    what_to_show="TRADES",
                    use_rth=False,
                )
                df_1m = bars_to_dataframe(bars_1m)

                if len(df_1m) < 2:
                    continue

                # Update trailing stop (two-phase)
                highest_high = getattr(pos, 'highest_high', 0.0)
                bars_held = getattr(pos, 'bars_since_entry', 15)
                bar_high = float(df_1m["high"].iloc[-1])
                if bar_high > highest_high:
                    highest_high = bar_high
                    pos.highest_high = highest_high
                pos.bars_since_entry = bars_held + 1
                new_stop = get_trailing_stop_price(df_1m, highest_high, bars_held)
                if new_stop <= 0:
                    continue

                # Trailing stop only moves up
                current_stop = getattr(pos, 'trailing_stop_price', 0.0)
                if new_stop > current_stop:
                    pos.trailing_stop_price = new_stop

                # Check if current price hit trailing stop
                current_price = float(df_1m["close"].iloc[-1])
                effective_stop = getattr(pos, 'trailing_stop_price', 0.0)
                if effective_stop > 0 and current_price <= effective_stop:
                    # Place sell
                    sell_trade = await self._order_mgr.place_market_sell(
                        pos.contract, pos.quantity,
                        reference_price=current_price,
                    )

                    if sell_trade:
                        await self._order_mgr.cancel_all_for_symbol(symbol)

                        pnl = (current_price - pos.entry_price) * pos.quantity
                        pnl_pct = (
                            (current_price - pos.entry_price) / pos.entry_price
                            if pos.entry_price > 0
                            else 0
                        )
                        self._trade_logger.log_exit(
                            symbol=symbol,
                            quantity=pos.quantity,
                            price=current_price,
                            entry_price=pos.entry_price,
                            entry_time=pos.entry_time.isoformat(),
                            notes=f"trailing_stop_exit session={session}",
                        )
                        self._risk_mgr.record_trade_pnl(pnl)
                        self._position_mgr.remove_position(symbol)
                        self._trailing_mgr.unregister_stop(symbol)

                        from notifications.telegram_bot import notify_exit
                        await notify_exit(
                            symbol=symbol,
                            quantity=pos.quantity,
                            price=current_price,
                            pnl=pnl,
                            pnl_pct=pnl_pct,
                            reason=f"trailing_stop ({session})",
                        )

                        logger.info(
                            f"EXIT [{session}]: {symbol} — {pos.quantity} shares @ "
                            f"${current_price:.4f}, P&L=${pnl:+,.2f} (trailing stop)"
                        )
            except Exception as e:
                logger.error(f"Trailing stop check error for {symbol}: {e}")

    def _write_live_state(self) -> None:
        """Write current state to JSON for dashboard consumption."""
        try:
            state = {
                "timestamp": now_utc().isoformat(),
                "simulation_mode": False,
                "session": current_session_name(),
                "market_open": is_market_open(),
                "premarket": is_premarket(),
                "afterhours": is_afterhours(),
                "positions": self._position_mgr.to_dict_list(),
                "risk_status": self._risk_mgr.get_status(),
                "scanner_candidates": len(self._scanner.last_candidates),
            }
            LIVE_STATE_PATH.write_text(json.dumps(state, default=str, indent=2))
        except Exception as e:
            logger.error(f"Failed to write live state: {e}")

    def _check_control_commands(self) -> None:
        """Read control commands from dashboard."""
        try:
            if not CONTROL_PATH.exists():
                return
            data = json.loads(CONTROL_PATH.read_text())
            command = data.get("command")
            if command == "stop":
                logger.info("Stop command received from dashboard")
                self._running = False
            elif command == "pause":
                logger.info("Pause command received (no-op for now)")
            CONTROL_PATH.write_text("{}")
        except Exception:
            pass

    def _launch_dashboard(self) -> None:
        """Launch Streamlit dashboard as subprocess."""
        dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
        if not dashboard_path.exists():
            logger.warning("Dashboard not found, skipping launch")
            return
        try:
            self._dashboard_proc = subprocess.Popen(
                [
                    sys.executable, "-m", "streamlit", "run",
                    str(dashboard_path),
                    "--server.port", str(DASHBOARD_PORT),
                    "--server.headless", "true",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logger.info(f"Dashboard launched on port {DASHBOARD_PORT}")
        except Exception as e:
            logger.warning(f"Failed to launch dashboard: {e}")

    async def _shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Shutting down...")

        summary = self._trade_logger.get_daily_summary()
        if summary:
            from notifications.telegram_bot import notify_daily_summary
            await notify_daily_summary(summary)

        from notifications.telegram_bot import notify_system
        await notify_system("Bot shutting down")

        if self._conn:
            await self._conn.disconnect()

        if self._dashboard_proc:
            self._dashboard_proc.terminate()

        logger.info("Shutdown complete")


class FibLiveTradingBot:
    """Fibonacci live paper trading bot — Strategy #5 from backtest.

    Runs a 30-second cycle:
      1. Gap scanner (IBKR top % gainers)
      2. Fib strategy state machine (SCANNING → TESTING → entry)
      3. Entry execution (market buy + OCA bracket)
      4. EOD cleanup at 15:55 ET
    """

    def __init__(self) -> None:
        self._running = False
        self._dashboard_proc = None

        self._conn = None
        self._order_mgr = None
        self._position_mgr = None
        self._risk_mgr = None
        self._gap_scanner = None
        self._strategy = None
        self._entry_executor = None
        self._trade_logger = None

    async def start(self) -> None:
        """Initialize and start the fib live trading loop."""
        logger.info("=" * 60)
        logger.info("Fibonacci Live Trading Bot Starting")
        logger.info(f"Mode: {'PAPER' if PAPER_TRADING else 'LIVE'}")
        logger.info(f"Starting Capital: ${STARTING_CAPITAL:,.2f}")
        logger.info(f"Strategy: Pure Fib Support (1.5% stop, 3 levels target)")
        logger.info(f"Time: {format_timestamp(now_utc())}")
        logger.info("=" * 60)

        await self._init_components()

        if not await self._conn.connect():
            logger.critical("Cannot connect to IBKR — exiting")
            return

        await self._position_mgr.sync_with_ibkr()

        try:
            summary = await self._conn.get_account_summary()
            if "NetLiquidation" in summary:
                self._risk_mgr.update_portfolio_value(summary["NetLiquidation"])
                logger.info(f"Portfolio value: ${summary['NetLiquidation']:,.2f}")
        except Exception as e:
            logger.warning(f"Could not fetch account summary: {e}")

        try:
            from notifications.telegram_bot import notify_system
            await notify_system(
                f"Fib Live Bot started ({'PAPER' if PAPER_TRADING else 'LIVE'})\n"
                f"Strategy: Pure Fib Support\n"
                f"Capital: ${STARTING_CAPITAL:,.2f}"
            )
        except Exception:
            pass

        self._launch_dashboard()
        self._running = True
        await self._main_loop()

    async def _init_components(self) -> None:
        """Initialize all trading components."""
        from broker.ibkr_connection import get_connection
        from broker.order_manager import OrderManager
        from broker.position_manager import PositionManager
        from risk.risk_manager import RiskManager
        from scanner.gap_scanner import GapScanner
        from strategies.fib_live_strategy import FibLiveStrategy
        from strategies.fib_live_entry import FibLiveEntry
        from notifications.trade_logger import TradeLogger

        self._conn = get_connection()
        self._order_mgr = OrderManager()
        self._position_mgr = PositionManager()
        self._risk_mgr = RiskManager()
        self._gap_scanner = GapScanner()
        self._strategy = FibLiveStrategy()
        self._trade_logger = TradeLogger()
        self._entry_executor = FibLiveEntry(
            self._order_mgr, self._position_mgr,
            self._risk_mgr, self._strategy, self._trade_logger,
        )

        # Register fill callback for OCA exit detection
        self._conn.ib.orderStatusEvent += self._on_order_status

    async def _main_loop(self) -> None:
        """Main 30-second cycle loop."""
        try:
            while self._running:
                self._check_control_commands()

                if is_any_session_active():
                    await self._run_cycle()
                    # EOD cleanup at 15:55 ET
                    t = now_et().time()
                    from datetime import time as dt_time
                    if t >= dt_time(15, 55) and t < dt_time(16, 0):
                        await self._eod_cleanup()
                else:
                    wait = seconds_until_next_session()
                    if wait > 300:
                        logger.info(
                            f"Sessions closed. Next in {wait / 3600:.1f}h. Sleeping 60s..."
                        )
                        await asyncio.sleep(60)
                        continue
                    else:
                        logger.info(f"Next session in {wait:.0f}s, standing by...")
                        await asyncio.sleep(10)
                        continue

                self._write_live_state()
                await asyncio.sleep(FIB_LIVE_SCAN_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Fib live main loop cancelled")
        except Exception as e:
            logger.critical(f"Unhandled error in fib live loop: {e}", exc_info=True)
        finally:
            await self._shutdown()

    async def _run_cycle(self) -> None:
        """One scan → strategy → entry cycle."""
        # 1. Scan for gap stocks
        gap_signals = await self._gap_scanner.scan_once()

        # 2. Run strategy state machine
        entry_requests = await self._strategy.process_cycle(gap_signals)

        # 3. Execute entries
        for req in entry_requests:
            try:
                success = await self._entry_executor.execute_entry(req)
                if success:
                    logger.info(f"Fib entry executed: {req.symbol}")
            except Exception as e:
                logger.error(f"Entry execution error for {req.symbol}: {e}")

        # 4. Sync positions with IBKR
        await self._position_mgr.sync_with_ibkr()

        # 5. Update portfolio value
        try:
            summary = await self._conn.get_account_summary()
            if "NetLiquidation" in summary:
                self._risk_mgr.update_portfolio_value(summary["NetLiquidation"])
                self._trade_logger.update_daily_summary(summary["NetLiquidation"])
        except Exception:
            pass

        # Log status
        status = self._strategy.get_status()
        if status["tracked_symbols"] > 0:
            logger.info(
                f"Cycle: {status['tracked_symbols']} symbols tracked, "
                f"{status['entries_today']} entries today, "
                f"phases={status['symbol_phases']}"
            )

    def _on_order_status(self, trade) -> None:
        """Handle OCA fill events — log exits when stop or target fills."""
        if trade.orderStatus.status != "Filled":
            return

        order = trade.order
        symbol = trade.contract.symbol

        # Only process SELL orders (stop/target fills)
        if order.action != "SELL":
            return

        pos = self._position_mgr.get_position(symbol)
        if pos is None or pos.strategy != "fib_live":
            return

        fill_price = trade.orderStatus.avgFillPrice
        if fill_price <= 0 and trade.fills:
            fill_price = trade.fills[0].execution.avgPrice

        pnl = (fill_price - pos.entry_price) * pos.quantity
        pnl_pct = (fill_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0

        # Determine if this was stop or target
        if order.orderType in ("STP LMT", "STP"):
            reason = f"stop_loss (${pos.stop_loss_price:.4f})"
        else:
            reason = f"target_hit (${pos.target_price:.4f})"

        self._trade_logger.log_exit(
            symbol=symbol,
            quantity=pos.quantity,
            price=fill_price,
            entry_price=pos.entry_price,
            entry_time=pos.entry_time.isoformat(),
            notes=f"fib_live {reason} oca={pos.oca_group}",
        )
        self._risk_mgr.record_trade_pnl(pnl)
        self._strategy.mark_position_closed(symbol)
        self._position_mgr.remove_position(symbol)

        sign = "+" if pnl >= 0 else ""
        logger.info(
            f"EXIT [OCA]: {symbol} {pos.quantity}sh @ ${fill_price:.4f} "
            f"P&L={sign}${pnl:.2f} ({reason})"
        )

        # Async telegram notification (fire and forget)
        try:
            from notifications.telegram_bot import notify_exit
            asyncio.ensure_future(notify_exit(
                symbol=symbol, quantity=pos.quantity,
                price=fill_price, pnl=pnl, pnl_pct=pnl_pct,
                reason=reason,
            ))
        except Exception:
            pass

    async def _eod_cleanup(self) -> None:
        """End-of-day: cancel open orders and close all fib_live positions."""
        for symbol in list(self._position_mgr.get_all_symbols()):
            pos = self._position_mgr.get_position(symbol)
            if pos is None or pos.strategy != "fib_live":
                continue

            logger.info(f"EOD cleanup: closing {symbol}")

            # Cancel OCA orders first
            cancelled = await self._order_mgr.cancel_all_for_symbol(symbol)
            logger.info(f"EOD: cancelled {cancelled} orders for {symbol}")

            # Market sell
            sell_trade = await self._order_mgr.place_market_sell(
                pos.contract, pos.quantity,
            )
            if sell_trade:
                fill_price = pos.entry_price  # estimate, actual fill via callback
                if sell_trade.orderStatus.status == "Filled":
                    fill_price = sell_trade.orderStatus.avgFillPrice

                pnl = (fill_price - pos.entry_price) * pos.quantity
                self._trade_logger.log_exit(
                    symbol=symbol, quantity=pos.quantity,
                    price=fill_price, entry_price=pos.entry_price,
                    entry_time=pos.entry_time.isoformat(),
                    notes="fib_live eod_cleanup",
                )
                self._risk_mgr.record_trade_pnl(pnl)
                self._strategy.mark_position_closed(symbol)
                self._position_mgr.remove_position(symbol)
                logger.info(f"EOD: closed {symbol} @ ${fill_price:.4f}, P&L=${pnl:+,.2f}")

    def _write_live_state(self) -> None:
        """Write current state to JSON for dashboard."""
        try:
            state = {
                "timestamp": now_utc().isoformat(),
                "simulation_mode": False,
                "strategy": "fib_live",
                "session": current_session_name(),
                "market_open": is_market_open(),
                "premarket": is_premarket(),
                "afterhours": is_afterhours(),
                "positions": self._position_mgr.to_dict_list(),
                "risk_status": self._risk_mgr.get_status(),
                "fib_strategy_status": self._strategy.get_status(),
            }
            LIVE_STATE_PATH.write_text(json.dumps(state, default=str, indent=2))
        except Exception as e:
            logger.error(f"Failed to write live state: {e}")

    def _check_control_commands(self) -> None:
        """Read control commands from dashboard."""
        try:
            if not CONTROL_PATH.exists():
                return
            data = json.loads(CONTROL_PATH.read_text())
            command = data.get("command")
            if command == "stop":
                logger.info("Stop command received from dashboard")
                self._running = False
            CONTROL_PATH.write_text("{}")
        except Exception:
            pass

    def _launch_dashboard(self) -> None:
        """Launch Streamlit dashboard as subprocess."""
        dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
        if not dashboard_path.exists():
            logger.warning("Dashboard not found, skipping launch")
            return
        try:
            self._dashboard_proc = subprocess.Popen(
                [
                    sys.executable, "-m", "streamlit", "run",
                    str(dashboard_path),
                    "--server.port", str(DASHBOARD_PORT),
                    "--server.headless", "true",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logger.info(f"Dashboard launched on port {DASHBOARD_PORT}")
        except Exception as e:
            logger.warning(f"Failed to launch dashboard: {e}")

    async def _shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Fib live bot shutting down...")

        # Force close any remaining positions
        await self._eod_cleanup()

        summary = self._trade_logger.get_daily_summary()
        if summary:
            try:
                from notifications.telegram_bot import notify_daily_summary
                await notify_daily_summary(summary)
            except Exception:
                pass

        try:
            from notifications.telegram_bot import notify_system
            await notify_system("Fib Live Bot shutting down")
        except Exception:
            pass

        if self._conn:
            await self._conn.disconnect()

        if self._dashboard_proc:
            self._dashboard_proc.terminate()

        logger.info("Fib live bot shutdown complete")


class FibConfirmTradingBot:
    """Fibonacci confirmation trading bot — 2-candle confirmation + SMA gate.

    Runs a 30-second cycle:
      1. Strength scanner (IBKR top % gainers, float+RVOL filters)
      2. Fib confirm strategy (4-state: SCANNING → CANDLE_1 → READY → IN_POSITION)
      3. Entry execution (market buy + OCA bracket)
      4. EOD cleanup at 15:55 ET
    """

    def __init__(self) -> None:
        self._running = False
        self._dashboard_proc = None

        self._conn = None
        self._order_mgr = None
        self._position_mgr = None
        self._risk_mgr = None
        self._strength_scanner = None
        self._strategy = None
        self._entry_executor = None
        self._trade_logger = None

    async def start(self) -> None:
        """Initialize and start the fib confirm trading loop."""
        logger.info("=" * 60)
        logger.info("Fibonacci Confirmation Trading Bot Starting")
        logger.info(f"Mode: {'PAPER' if PAPER_TRADING else 'LIVE'}")
        logger.info(f"Starting Capital: ${STARTING_CAPITAL:,.2f}")
        logger.info(f"Strategy: 2-Candle Fib Confirm (SMA 20+200, 1.5% stop, 3 levels target)")
        logger.info(f"Time: {format_timestamp(now_utc())}")
        logger.info("=" * 60)

        await self._init_components()

        if not await self._conn.connect():
            logger.critical("Cannot connect to IBKR — exiting")
            return

        await self._position_mgr.sync_with_ibkr()

        try:
            summary = await self._conn.get_account_summary()
            if "NetLiquidation" in summary:
                self._risk_mgr.update_portfolio_value(summary["NetLiquidation"])
                logger.info(f"Portfolio value: ${summary['NetLiquidation']:,.2f}")
        except Exception as e:
            logger.warning(f"Could not fetch account summary: {e}")

        try:
            from notifications.telegram_bot import notify_system
            await notify_system(
                f"Fib Confirm Bot started ({'PAPER' if PAPER_TRADING else 'LIVE'})\n"
                f"Strategy: 2-Candle Fib Confirm + SMA Gate\n"
                f"Capital: ${STARTING_CAPITAL:,.2f}"
            )
        except Exception:
            pass

        self._launch_dashboard()
        self._running = True
        await self._main_loop()

    async def _init_components(self) -> None:
        """Initialize all trading components."""
        from broker.ibkr_connection import get_connection
        from broker.order_manager import OrderManager
        from broker.position_manager import PositionManager
        from risk.risk_manager import RiskManager
        from scanner.strength_scanner import StrengthScanner
        from strategies.fib_confirm_strategy import FibConfirmStrategy
        from strategies.fib_confirm_entry import FibConfirmEntry
        from notifications.trade_logger import TradeLogger

        self._conn = get_connection()
        self._order_mgr = OrderManager()
        self._position_mgr = PositionManager()
        self._risk_mgr = RiskManager()
        self._strength_scanner = StrengthScanner()
        self._strategy = FibConfirmStrategy()
        self._trade_logger = TradeLogger()
        self._entry_executor = FibConfirmEntry(
            self._order_mgr, self._position_mgr,
            self._risk_mgr, self._strategy, self._trade_logger,
        )

        # Register fill callback for OCA exit detection
        self._conn.ib.orderStatusEvent += self._on_order_status

    async def _main_loop(self) -> None:
        """Main 30-second cycle loop."""
        try:
            while self._running:
                self._check_control_commands()

                if is_any_session_active():
                    await self._run_cycle()
                    # EOD cleanup at 15:55 ET
                    t = now_et().time()
                    from datetime import time as dt_time
                    if t >= dt_time(15, 55) and t < dt_time(16, 0):
                        await self._eod_cleanup()
                else:
                    wait = seconds_until_next_session()
                    if wait > 300:
                        logger.info(
                            f"Sessions closed. Next in {wait / 3600:.1f}h. Sleeping 60s..."
                        )
                        await asyncio.sleep(60)
                        continue
                    else:
                        logger.info(f"Next session in {wait:.0f}s, standing by...")
                        await asyncio.sleep(10)
                        continue

                self._write_live_state()
                await asyncio.sleep(FIB_CONFIRM_SCAN_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Fib confirm main loop cancelled")
        except Exception as e:
            logger.critical(f"Unhandled error in fib confirm loop: {e}", exc_info=True)
        finally:
            await self._shutdown()

    async def _run_cycle(self) -> None:
        """One scan → strategy → entry cycle."""
        # 1. Scan for strong stocks
        strength_signals = await self._strength_scanner.scan_once()

        # 2. Run strategy state machine
        entry_requests = await self._strategy.process_cycle(strength_signals)

        # 3. Execute entries
        for req in entry_requests:
            try:
                success = await self._entry_executor.execute_entry(req)
                if success:
                    logger.info(f"Fib confirm entry executed: {req.symbol}")
            except Exception as e:
                logger.error(f"Entry execution error for {req.symbol}: {e}")

        # 4. Sync positions with IBKR
        await self._position_mgr.sync_with_ibkr()

        # 5. Update portfolio value
        try:
            summary = await self._conn.get_account_summary()
            if "NetLiquidation" in summary:
                self._risk_mgr.update_portfolio_value(summary["NetLiquidation"])
                self._trade_logger.update_daily_summary(summary["NetLiquidation"])
        except Exception:
            pass

        # Log status
        status = self._strategy.get_status()
        if status["tracked_symbols"] > 0:
            logger.info(
                f"Cycle: {status['tracked_symbols']} symbols tracked, "
                f"{status['entries_today']} entries today, "
                f"phases={status['symbol_phases']}"
            )

    def _on_order_status(self, trade) -> None:
        """Handle OCA fill events — log exits when stop or target fills."""
        if trade.orderStatus.status != "Filled":
            return

        order = trade.order
        symbol = trade.contract.symbol

        # Only process SELL orders (stop/target fills)
        if order.action != "SELL":
            return

        pos = self._position_mgr.get_position(symbol)
        if pos is None or pos.strategy != "fib_confirm":
            return

        fill_price = trade.orderStatus.avgFillPrice
        if fill_price <= 0 and trade.fills:
            fill_price = trade.fills[0].execution.avgPrice

        pnl = (fill_price - pos.entry_price) * pos.quantity
        pnl_pct = (fill_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0

        # Determine if this was stop or target
        if order.orderType in ("STP LMT", "STP"):
            reason = f"stop_loss (${pos.stop_loss_price:.4f})"
        else:
            reason = f"target_hit (${pos.target_price:.4f})"

        self._trade_logger.log_exit(
            symbol=symbol,
            quantity=pos.quantity,
            price=fill_price,
            entry_price=pos.entry_price,
            entry_time=pos.entry_time.isoformat(),
            notes=f"fib_confirm {reason} oca={pos.oca_group}",
        )
        self._risk_mgr.record_trade_pnl(pnl)
        self._strategy.mark_position_closed(symbol)
        self._position_mgr.remove_position(symbol)

        sign = "+" if pnl >= 0 else ""
        logger.info(
            f"EXIT [OCA]: {symbol} {pos.quantity}sh @ ${fill_price:.4f} "
            f"P&L={sign}${pnl:.2f} ({reason})"
        )

        # Async telegram notification (fire and forget)
        try:
            from notifications.telegram_bot import notify_exit
            asyncio.ensure_future(notify_exit(
                symbol=symbol, quantity=pos.quantity,
                price=fill_price, pnl=pnl, pnl_pct=pnl_pct,
                reason=reason,
            ))
        except Exception:
            pass

    async def _eod_cleanup(self) -> None:
        """End-of-day: cancel open orders and close all fib_confirm positions."""
        for symbol in list(self._position_mgr.get_all_symbols()):
            pos = self._position_mgr.get_position(symbol)
            if pos is None or pos.strategy != "fib_confirm":
                continue

            logger.info(f"EOD cleanup: closing {symbol}")

            # Cancel OCA orders first
            cancelled = await self._order_mgr.cancel_all_for_symbol(symbol)
            logger.info(f"EOD: cancelled {cancelled} orders for {symbol}")

            # Market sell
            sell_trade = await self._order_mgr.place_market_sell(
                pos.contract, pos.quantity,
            )
            if sell_trade:
                fill_price = pos.entry_price  # estimate, actual fill via callback
                if sell_trade.orderStatus.status == "Filled":
                    fill_price = sell_trade.orderStatus.avgFillPrice

                pnl = (fill_price - pos.entry_price) * pos.quantity
                self._trade_logger.log_exit(
                    symbol=symbol, quantity=pos.quantity,
                    price=fill_price, entry_price=pos.entry_price,
                    entry_time=pos.entry_time.isoformat(),
                    notes="fib_confirm eod_cleanup",
                )
                self._risk_mgr.record_trade_pnl(pnl)
                self._strategy.mark_position_closed(symbol)
                self._position_mgr.remove_position(symbol)
                logger.info(f"EOD: closed {symbol} @ ${fill_price:.4f}, P&L=${pnl:+,.2f}")

    def _write_live_state(self) -> None:
        """Write current state to JSON for dashboard."""
        try:
            state = {
                "timestamp": now_utc().isoformat(),
                "simulation_mode": False,
                "strategy": "fib_confirm",
                "session": current_session_name(),
                "market_open": is_market_open(),
                "premarket": is_premarket(),
                "afterhours": is_afterhours(),
                "positions": self._position_mgr.to_dict_list(),
                "risk_status": self._risk_mgr.get_status(),
                "fib_strategy_status": self._strategy.get_status(),
            }
            LIVE_STATE_PATH.write_text(json.dumps(state, default=str, indent=2))
        except Exception as e:
            logger.error(f"Failed to write live state: {e}")

    def _check_control_commands(self) -> None:
        """Read control commands from dashboard."""
        try:
            if not CONTROL_PATH.exists():
                return
            data = json.loads(CONTROL_PATH.read_text())
            command = data.get("command")
            if command == "stop":
                logger.info("Stop command received from dashboard")
                self._running = False
            CONTROL_PATH.write_text("{}")
        except Exception:
            pass

    def _launch_dashboard(self) -> None:
        """Launch Streamlit dashboard as subprocess."""
        dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
        if not dashboard_path.exists():
            logger.warning("Dashboard not found, skipping launch")
            return
        try:
            self._dashboard_proc = subprocess.Popen(
                [
                    sys.executable, "-m", "streamlit", "run",
                    str(dashboard_path),
                    "--server.port", str(DASHBOARD_PORT),
                    "--server.headless", "true",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logger.info(f"Dashboard launched on port {DASHBOARD_PORT}")
        except Exception as e:
            logger.warning(f"Failed to launch dashboard: {e}")

    async def _shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Fib confirm bot shutting down...")

        # Force close any remaining positions
        await self._eod_cleanup()

        summary = self._trade_logger.get_daily_summary()
        if summary:
            try:
                from notifications.telegram_bot import notify_daily_summary
                await notify_daily_summary(summary)
            except Exception:
                pass

        try:
            from notifications.telegram_bot import notify_system
            await notify_system("Fib Confirm Bot shutting down")
        except Exception:
            pass

        if self._conn:
            await self._conn.disconnect()

        if self._dashboard_proc:
            self._dashboard_proc.terminate()

        logger.info("Fib confirm bot shutdown complete")


class FibDoubleTouchTradingBot:
    """Fibonacci double-touch live paper trading bot.

    Runs a 15-second cycle:
      1. Gap scanner (IBKR top % gainers, 10-25% gap, float <= 500M)
      2. Fib DT strategy state machine (double-touch detection on 15s bars)
      3. Entry execution (market buy + split exit: OCA half + standalone stop half)
      4. Order status callbacks for split exit handling
      5. Trailing exit monitoring (no-new-high on remaining half)
      6. EOD cleanup at 15:55 ET
    """

    def __init__(self) -> None:
        self._running = False
        self._dashboard_proc = None

        self._conn = None
        self._order_mgr = None
        self._position_mgr = None
        self._risk_mgr = None
        self._gap_scanner = None
        self._strategy = None
        self._entry_executor = None
        self._trade_logger = None

    async def start(self) -> None:
        """Initialize and start the fib DT trading loop."""
        logger.info("=" * 60)
        logger.info("Fibonacci Double-Touch Trading Bot Starting")
        logger.info(f"Mode: {'PAPER' if PAPER_TRADING else 'LIVE'}")
        logger.info(f"Starting Capital: ${STARTING_CAPITAL:,.2f}")
        logger.info(f"Strategy: Double-Touch Fib (3% stop, split exit 50/50)")
        logger.info(f"Time: {format_timestamp(now_utc())}")
        logger.info("=" * 60)

        await self._init_components()

        if not await self._conn.connect():
            logger.critical("Cannot connect to IBKR — exiting")
            return

        await self._position_mgr.sync_with_ibkr()

        try:
            summary = await self._conn.get_account_summary()
            if "NetLiquidation" in summary:
                self._risk_mgr.update_portfolio_value(summary["NetLiquidation"])
                logger.info(f"Portfolio value: ${summary['NetLiquidation']:,.2f}")
        except Exception as e:
            logger.warning(f"Could not fetch account summary: {e}")

        try:
            from notifications.telegram_bot import notify_system
            await notify_system(
                f"Fib DT Bot started ({'PAPER' if PAPER_TRADING else 'LIVE'})\n"
                f"Strategy: Double-Touch Fib (3% stop, split exit)\n"
                f"Capital: ${STARTING_CAPITAL:,.2f}"
            )
        except Exception:
            pass

        self._launch_dashboard()
        self._running = True
        await self._main_loop()

    async def _init_components(self) -> None:
        """Initialize all trading components."""
        from broker.ibkr_connection import get_connection
        from broker.order_manager import OrderManager
        from broker.position_manager import PositionManager
        from risk.risk_manager import RiskManager
        from scanner.gap_scanner import GapScanner
        from strategies.fib_dt_live_strategy import FibDTLiveStrategy
        from strategies.fib_dt_live_entry import FibDTLiveEntry
        from notifications.trade_logger import TradeLogger

        self._conn = get_connection()
        self._order_mgr = OrderManager()
        self._position_mgr = PositionManager()
        self._risk_mgr = RiskManager()
        self._gap_scanner = GapScanner(
            gap_min_pct=FIB_DT_LIVE_GAP_MIN_PCT,
            gap_max_pct=FIB_DT_LIVE_GAP_MAX_PCT,
            float_max=FIB_DT_LIVE_FLOAT_MAX,
            max_symbols=FIB_DT_LIVE_MAX_SYMBOLS,
        )
        self._strategy = FibDTLiveStrategy()
        self._trade_logger = TradeLogger()
        self._entry_executor = FibDTLiveEntry(
            self._order_mgr, self._position_mgr,
            self._risk_mgr, self._strategy, self._trade_logger,
        )

        # Register fill callback for split exit detection
        self._conn.ib.orderStatusEvent += self._on_order_status

    async def _main_loop(self) -> None:
        """Main 15-second cycle loop."""
        try:
            while self._running:
                self._check_control_commands()

                if is_any_session_active():
                    await self._run_cycle()
                    # EOD cleanup at 15:55 ET
                    t = now_et().time()
                    from datetime import time as dt_time
                    if t >= dt_time(15, 55) and t < dt_time(16, 0):
                        await self._eod_cleanup()
                else:
                    wait = seconds_until_next_session()
                    if wait > 300:
                        logger.info(
                            f"Sessions closed. Next in {wait / 3600:.1f}h. Sleeping 60s..."
                        )
                        await asyncio.sleep(60)
                        continue
                    else:
                        logger.info(f"Next session in {wait:.0f}s, standing by...")
                        await asyncio.sleep(10)
                        continue

                self._write_live_state()
                await asyncio.sleep(FIB_DT_LIVE_SCAN_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Fib DT main loop cancelled")
        except Exception as e:
            logger.critical(f"Unhandled error in fib DT loop: {e}", exc_info=True)
        finally:
            await self._shutdown()

    async def _run_cycle(self) -> None:
        """One scan -> strategy -> entry cycle."""
        # 1. Scan for gap stocks
        gap_signals = await self._gap_scanner.scan_once()

        # 2. Run strategy state machine
        entry_requests, trailing_exits = await self._strategy.process_cycle(gap_signals)

        # 3. Execute entries
        for req in entry_requests:
            try:
                success = await self._entry_executor.execute_entry(req)
                if success:
                    logger.info(f"Fib DT entry executed: {req.symbol}")
            except Exception as e:
                logger.error(f"Entry execution error for {req.symbol}: {e}")

        # 4. Handle trailing exits (no-new-high on remaining half)
        for exit_signal in trailing_exits:
            try:
                await self._execute_trailing_exit(exit_signal)
            except Exception as e:
                logger.error(f"Trailing exit error for {exit_signal.symbol}: {e}")

        # 5. Sync positions with IBKR
        await self._position_mgr.sync_with_ibkr()

        # 6. Update portfolio value
        try:
            summary = await self._conn.get_account_summary()
            if "NetLiquidation" in summary:
                self._risk_mgr.update_portfolio_value(summary["NetLiquidation"])
                self._trade_logger.update_daily_summary(summary["NetLiquidation"])
        except Exception:
            pass

        # Log status
        status = self._strategy.get_status()
        if status["tracked_symbols"] > 0:
            logger.info(
                f"Cycle: {status['tracked_symbols']} symbols tracked, "
                f"{status['entries_today']} entries today, "
                f"phases={status['symbol_phases']}"
            )

    async def _execute_trailing_exit(self, exit_signal) -> None:
        """Sell remaining half via market sell and cancel standalone stop."""
        from strategies.fib_dt_live_strategy import DTTrailingExit

        symbol = exit_signal.symbol
        pos = self._position_mgr.get_position(symbol)
        if pos is None or pos.strategy != "fib_dt":
            return

        other_half = getattr(pos, "other_half_qty", pos.quantity)

        # Cancel the standalone stop order
        trailing_stop_id = getattr(pos, "trailing_stop_order_id", 0)
        if trailing_stop_id:
            for trade in self._order_mgr.get_open_orders_for_symbol(symbol):
                if trade.order.orderId == trailing_stop_id:
                    await self._order_mgr.cancel_order(trade)
                    break

        # Market sell remaining shares
        sell_trade = await self._order_mgr.place_market_sell(
            pos.contract, other_half,
        )

        if sell_trade:
            fill_price = pos.entry_price
            if sell_trade.orderStatus.status == "Filled":
                fill_price = sell_trade.orderStatus.avgFillPrice
            elif sell_trade.fills:
                fill_price = sell_trade.fills[0].execution.avgPrice

            pnl = (fill_price - pos.entry_price) * other_half
            pnl_pct = (fill_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0

            self._trade_logger.log_exit(
                symbol=symbol,
                quantity=other_half,
                price=fill_price,
                entry_price=pos.entry_price,
                entry_time=pos.entry_time.isoformat(),
                notes=f"fib_dt trailing_exit {exit_signal.reason}",
            )
            self._risk_mgr.record_trade_pnl(pnl)
            self._strategy.mark_position_closed(symbol)
            self._position_mgr.remove_position(symbol)

            sign = "+" if pnl >= 0 else ""
            logger.info(
                f"EXIT [TRAILING]: {symbol} {other_half}sh @ ${fill_price:.4f} "
                f"P&L={sign}${pnl:.2f} ({exit_signal.reason})"
            )

            try:
                from notifications.telegram_bot import notify_exit
                await notify_exit(
                    symbol=symbol, quantity=other_half,
                    price=fill_price, pnl=pnl, pnl_pct=pnl_pct,
                    reason=f"trailing ({exit_signal.reason})",
                )
            except Exception:
                pass

    def _on_order_status(self, trade) -> None:
        """Handle fill events for split exit logic.

        Scenarios:
          A. Target fills (half qty): OCA cancels paired stop -> transition to TRAILING
          B. OCA stop fills (half qty): cancel standalone stop + remaining orders -> full exit
          C. Standalone stop fills (other_half): cancel OCA bracket -> full exit
        """
        if trade.orderStatus.status != "Filled":
            return

        order = trade.order
        symbol = trade.contract.symbol

        if order.action != "SELL":
            return

        pos = self._position_mgr.get_position(symbol)
        if pos is None or pos.strategy != "fib_dt":
            return

        fill_price = trade.orderStatus.avgFillPrice
        if fill_price <= 0 and trade.fills:
            fill_price = trade.fills[0].execution.avgPrice

        fill_qty = int(trade.orderStatus.filled)
        half_qty = getattr(pos, "half_qty", 0)
        other_half_qty = getattr(pos, "other_half_qty", 0)
        target_order_id = pos.target_order_id
        stop_order_id = pos.stop_order_id
        trailing_stop_id = getattr(pos, "trailing_stop_order_id", 0)

        # Identify which order filled
        order_id = order.orderId

        if order_id == target_order_id:
            # Scenario A: Target filled (half qty) -> TRAILING
            pnl = (fill_price - pos.entry_price) * half_qty
            self._trade_logger.log_exit(
                symbol=symbol,
                quantity=half_qty,
                price=fill_price,
                entry_price=pos.entry_price,
                entry_time=pos.entry_time.isoformat(),
                notes=f"fib_dt target_hit (${pos.target_price:.4f}) partial={half_qty}sh",
            )
            self._risk_mgr.record_trade_pnl(pnl)

            # OCA auto-cancels the paired stop; standalone stop still active
            # Transition to TRAILING for the remaining half
            self._strategy.mark_trailing(symbol)

            # Update position quantity to reflect remaining shares
            self._position_mgr.update_quantity(symbol, half_qty)

            sign = "+" if pnl >= 0 else ""
            logger.info(
                f"PARTIAL EXIT [TARGET]: {symbol} {half_qty}sh @ ${fill_price:.4f} "
                f"P&L={sign}${pnl:.2f} -> TRAILING {other_half_qty}sh remaining"
            )

            try:
                from notifications.telegram_bot import notify_exit
                asyncio.ensure_future(notify_exit(
                    symbol=symbol, quantity=half_qty,
                    price=fill_price, pnl=pnl,
                    pnl_pct=(fill_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0,
                    reason=f"target_hit_partial (${pos.target_price:.4f})",
                ))
            except Exception:
                pass

        elif order_id == stop_order_id:
            # Scenario B: OCA stop filled (half qty) -> cancel standalone stop -> full exit
            # Cancel remaining orders
            asyncio.ensure_future(self._cancel_remaining_orders(symbol, trailing_stop_id))

            pnl = (fill_price - pos.entry_price) * pos.quantity
            pnl_pct = (fill_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0

            self._trade_logger.log_exit(
                symbol=symbol,
                quantity=pos.quantity,
                price=fill_price,
                entry_price=pos.entry_price,
                entry_time=pos.entry_time.isoformat(),
                notes=f"fib_dt stop_loss (${pos.stop_loss_price:.4f}) oca={pos.oca_group}",
            )
            self._risk_mgr.record_trade_pnl(pnl)
            self._strategy.mark_position_closed(symbol)
            self._position_mgr.remove_position(symbol)

            sign = "+" if pnl >= 0 else ""
            logger.info(
                f"EXIT [OCA STOP]: {symbol} {pos.quantity}sh @ ${fill_price:.4f} "
                f"P&L={sign}${pnl:.2f} (stop_loss)"
            )

            try:
                from notifications.telegram_bot import notify_exit
                asyncio.ensure_future(notify_exit(
                    symbol=symbol, quantity=pos.quantity,
                    price=fill_price, pnl=pnl, pnl_pct=pnl_pct,
                    reason=f"stop_loss (${pos.stop_loss_price:.4f})",
                ))
            except Exception:
                pass

        elif order_id == trailing_stop_id:
            # Scenario C: Standalone stop filled (other_half) -> cancel OCA -> full exit
            # Cancel remaining OCA orders
            asyncio.ensure_future(self._cancel_remaining_orders(symbol, target_order_id))

            pnl = (fill_price - pos.entry_price) * pos.quantity
            pnl_pct = (fill_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0

            self._trade_logger.log_exit(
                symbol=symbol,
                quantity=pos.quantity,
                price=fill_price,
                entry_price=pos.entry_price,
                entry_time=pos.entry_time.isoformat(),
                notes=f"fib_dt standalone_stop (${pos.stop_loss_price:.4f})",
            )
            self._risk_mgr.record_trade_pnl(pnl)
            self._strategy.mark_position_closed(symbol)
            self._position_mgr.remove_position(symbol)

            sign = "+" if pnl >= 0 else ""
            logger.info(
                f"EXIT [STANDALONE STOP]: {symbol} {pos.quantity}sh @ ${fill_price:.4f} "
                f"P&L={sign}${pnl:.2f} (standalone_stop)"
            )

            try:
                from notifications.telegram_bot import notify_exit
                asyncio.ensure_future(notify_exit(
                    symbol=symbol, quantity=pos.quantity,
                    price=fill_price, pnl=pnl, pnl_pct=pnl_pct,
                    reason=f"standalone_stop (${pos.stop_loss_price:.4f})",
                ))
            except Exception:
                pass

    async def _cancel_remaining_orders(self, symbol: str, order_id_to_cancel: int) -> None:
        """Cancel a specific order and any remaining open orders for symbol."""
        if order_id_to_cancel:
            for trade in self._order_mgr.get_open_orders_for_symbol(symbol):
                if trade.order.orderId == order_id_to_cancel:
                    await self._order_mgr.cancel_order(trade)
                    break
        # Also cancel any other remaining orders for the symbol
        await self._order_mgr.cancel_all_for_symbol(symbol)

    async def _eod_cleanup(self) -> None:
        """End-of-day: cancel open orders and close all fib_dt positions."""
        for symbol in list(self._position_mgr.get_all_symbols()):
            pos = self._position_mgr.get_position(symbol)
            if pos is None or pos.strategy != "fib_dt":
                continue

            logger.info(f"EOD cleanup: closing {symbol}")

            cancelled = await self._order_mgr.cancel_all_for_symbol(symbol)
            logger.info(f"EOD: cancelled {cancelled} orders for {symbol}")

            sell_trade = await self._order_mgr.place_market_sell(
                pos.contract, pos.quantity,
            )
            if sell_trade:
                fill_price = pos.entry_price
                if sell_trade.orderStatus.status == "Filled":
                    fill_price = sell_trade.orderStatus.avgFillPrice

                pnl = (fill_price - pos.entry_price) * pos.quantity
                self._trade_logger.log_exit(
                    symbol=symbol, quantity=pos.quantity,
                    price=fill_price, entry_price=pos.entry_price,
                    entry_time=pos.entry_time.isoformat(),
                    notes="fib_dt eod_cleanup",
                )
                self._risk_mgr.record_trade_pnl(pnl)
                self._strategy.mark_position_closed(symbol)
                self._position_mgr.remove_position(symbol)
                logger.info(f"EOD: closed {symbol} @ ${fill_price:.4f}, P&L=${pnl:+,.2f}")

    def _write_live_state(self) -> None:
        """Write current state to JSON for dashboard."""
        try:
            state = {
                "timestamp": now_utc().isoformat(),
                "simulation_mode": False,
                "strategy": "fib_dt",
                "session": current_session_name(),
                "market_open": is_market_open(),
                "premarket": is_premarket(),
                "afterhours": is_afterhours(),
                "positions": self._position_mgr.to_dict_list(),
                "risk_status": self._risk_mgr.get_status(),
                "fib_strategy_status": self._strategy.get_status(),
            }
            LIVE_STATE_PATH.write_text(json.dumps(state, default=str, indent=2))
        except Exception as e:
            logger.error(f"Failed to write live state: {e}")

    def _check_control_commands(self) -> None:
        """Read control commands from dashboard."""
        try:
            if not CONTROL_PATH.exists():
                return
            data = json.loads(CONTROL_PATH.read_text())
            command = data.get("command")
            if command == "stop":
                logger.info("Stop command received from dashboard")
                self._running = False
            CONTROL_PATH.write_text("{}")
        except Exception:
            pass

    def _launch_dashboard(self) -> None:
        """Launch Streamlit dashboard as subprocess."""
        dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
        if not dashboard_path.exists():
            logger.warning("Dashboard not found, skipping launch")
            return
        try:
            self._dashboard_proc = subprocess.Popen(
                [
                    sys.executable, "-m", "streamlit", "run",
                    str(dashboard_path),
                    "--server.port", str(DASHBOARD_PORT),
                    "--server.headless", "true",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logger.info(f"Dashboard launched on port {DASHBOARD_PORT}")
        except Exception as e:
            logger.warning(f"Failed to launch dashboard: {e}")

    async def _shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Fib DT bot shutting down...")

        await self._eod_cleanup()

        summary = self._trade_logger.get_daily_summary()
        if summary:
            try:
                from notifications.telegram_bot import notify_daily_summary
                await notify_daily_summary(summary)
            except Exception:
                pass

        try:
            from notifications.telegram_bot import notify_system
            await notify_system("Fib DT Bot shutting down")
        except Exception:
            pass

        if self._conn:
            await self._conn.disconnect()

        if self._dashboard_proc:
            self._dashboard_proc.terminate()

        logger.info("Fib DT bot shutdown complete")


def main():
    parser = argparse.ArgumentParser(description="Penny Stock Trading Bot")
    parser.add_argument("--backtest", action="store_true", help="Run backtester")
    parser.add_argument("--simulate", action="store_true", help="Run simulation engine")
    parser.add_argument("--dashboard", action="store_true", help="Launch dashboard only")
    parser.add_argument("--analyze", action="store_true", help="Run weekly strategy analysis")
    parser.add_argument("--fib-backtest", action="store_true", help="Run Fibonacci support backtest on gappers")
    parser.add_argument("--fib-live", action="store_true", help="Run Fibonacci live paper trading bot")
    parser.add_argument("--fib-confirm", action="store_true", help="Run Fibonacci confirmation paper trading bot")
    parser.add_argument("--fib-strength", action="store_true", help="Run multi-TF fib strength backtest (gap>=20%%, SMA gates, vol/float)")
    parser.add_argument("--download-cache", action="store_true", help="Download and cache all backtest data (daily, intraday, float)")
    parser.add_argument("--fib-reversal", action="store_true", help="Run fib retracement + trend reversal backtest (3-candle confirm)")
    parser.add_argument("--fib-dt", action="store_true", help="Run Fibonacci double-touch live paper trading bot")
    parser.add_argument("--fib-double-touch", action="store_true", help="Run double-touch fib backtest (15-sec bars, IBKR data)")
    parser.add_argument("--optimize", action="store_true", help="Run multi-strategy fibonacci optimizer (72 combos)")
    parser.add_argument("--symbols", nargs="*", help="Symbols to simulate (e.g., AAPL TSLA)")
    parser.add_argument("--gap-min", type=float, default=10.0, help="Minimum gap pct for fib backtest (default: 10)")
    parser.add_argument("--sma20", action="store_true", help="Require price above SMA 20 (intraday)")
    parser.add_argument("--sma200", action="store_true", help="Require price above SMA 200 (intraday)")
    parser.add_argument("--float-max", type=float, default=0, help="Max float in millions (e.g., 60 for 60M)")
    parser.add_argument("--rvol", action="store_true", help="Require unusual volume vs 14-day average")
    parser.add_argument("--bar-size", type=str, default="2m", help="Bar size: 1m or 2m (default: 2m)")
    parser.add_argument("--start-date", type=str, help="Simulation start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Simulation end date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.download_cache:
        from simulation.download_cache import download_cache
        download_cache()
        return

    if args.analyze:
        from simulation.weekly_analysis import run_weekly_analysis
        run_weekly_analysis()
        return

    if args.fib_live:
        bot = FibLiveTradingBot()
        try:
            asyncio.run(bot.start())
        except KeyboardInterrupt:
            logger.info("Fib live bot interrupted by user")
        return

    if args.fib_confirm:
        bot = FibConfirmTradingBot()
        try:
            asyncio.run(bot.start())
        except KeyboardInterrupt:
            logger.info("Fib confirm bot interrupted by user")
        return

    if args.fib_dt:
        bot = FibDoubleTouchTradingBot()
        try:
            asyncio.run(bot.start())
        except KeyboardInterrupt:
            logger.info("Fib DT bot interrupted by user")
        return

    if args.fib_strength:
        from simulation.fib_strength_backtest import run_fib_strength_backtest
        result = asyncio.run(run_fib_strength_backtest())
        if result.total_trades > 0:
            wr = result.winning_trades / result.total_trades * 100
            print(f"\nQuick recap: {result.total_trades} trades, "
                  f"{wr:.1f}% WR, net P&L ${result.total_pnl_net:+,.2f}")
        else:
            print("\nNo trades were generated.")
        return

    if args.fib_reversal:
        from simulation.fib_reversal_backtest import run_fib_reversal_backtest
        result = asyncio.run(run_fib_reversal_backtest())
        if result.total_trades > 0:
            wr = result.winning_trades / result.total_trades * 100
            print(f"\nQuick recap: {result.total_trades} trades, "
                  f"{wr:.1f}% WR, net P&L ${result.total_pnl_net:+,.2f}")
        else:
            print("\nNo trades were generated.")
        return

    if args.fib_double_touch:
        # Step 1: Download 15-sec data from IBKR (if TWS running and cache incomplete)
        try:
            from simulation.ibkr_data_downloader import download_15s_data
            print("Step 1: Downloading 15-sec bars from IBKR...")
            dl_results = asyncio.run(download_15s_data())
            if dl_results:
                print(f"  Downloaded {len(dl_results)} new gap events")
            else:
                print("  All data already cached (or IBKR not available)")
        except Exception as e:
            print(f"  Download skipped: {e}")
            print("  (Will use existing cached data)")

        # Step 2: Run backtest
        print("\nStep 2: Running double-touch backtest...")
        from simulation.fib_double_touch_backtest import FibDoubleTouchEngine
        engine = FibDoubleTouchEngine()
        result = engine.run()

        # Step 3: Generate charts
        if result.total_trades > 0:
            print("\nStep 3: Generating charts...")
            try:
                from simulation.fib_double_touch_charts import generate_double_touch_report
                chart_data = engine.get_chart_data()
                generate_double_touch_report(chart_data, result)
            except Exception as e:
                print(f"  Chart generation failed: {e}")

            wr = result.winning_trades / result.total_trades * 100
            print(f"\nQuick recap: {result.total_trades} trades, "
                  f"{wr:.1f}% WR, net P&L ${result.total_pnl_net:+,.2f}")
        else:
            print("\nNo trades were generated.")
        return

    if args.optimize:
        from simulation.strategy_optimizer import run_optimizer
        run_optimizer()
        return

    if args.fib_backtest:
        from simulation.fib_backtest import run_fib_backtest
        result = asyncio.run(run_fib_backtest(
            symbols=args.symbols,
            gap_min_pct=args.gap_min,
            require_sma200=args.sma200,
            require_sma20=args.sma20,
            float_max=args.float_max * 1e6 if args.float_max else 0,
            require_rvol=args.rvol,
            bar_size=args.bar_size,
        ))
        # Summary already printed by engine; show quick recap
        if result.total_trades > 0:
            wr = result.winning_trades / result.total_trades * 100
            print(f"\nQuick recap: {result.total_trades} trades, "
                  f"{wr:.1f}% WR, net P&L ${result.total_pnl_net:+,.2f}")
        else:
            print("\nNo trades were generated.")
        return

    if args.backtest:
        from backtesting.backtest_engine import run_backtest
        asyncio.run(run_backtest())
        return

    if args.simulate:
        from simulation.sim_engine import run_simulation
        result = asyncio.run(run_simulation(
            symbols=args.symbols,
            start_date=args.start_date,
            end_date=args.end_date,
        ))
        print(f"\nSimulation Results:")
        print(f"  Total trades: {result.total_trades}")
        print(f"  Winning: {result.winning_trades}")
        print(f"  Losing: {result.losing_trades}")
        print(f"  Win rate: {result.winning_trades / result.total_trades * 100:.1f}%" if result.total_trades > 0 else "  Win rate: N/A")
        print(f"  Gross P&L: ${result.total_pnl_gross:+,.2f}")
        print(f"  Net P&L:   ${result.total_pnl_net:+,.2f}")
        print(f"  Commissions: ${result.total_commissions:,.2f}")
        print(f"  Slippage:  ${result.total_slippage:,.2f}")
        print(f"  Max Drawdown: {result.max_drawdown:.2%}")
        print(f"\nResults written to dashboard. Run: python main.py --dashboard")
        return

    if args.dashboard:
        dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
        subprocess.run([
            sys.executable, "-m", "streamlit", "run",
            str(dashboard_path),
            "--server.port", str(DASHBOARD_PORT),
        ])
        return

    bot = TradingBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")


if __name__ == "__main__":
    main()
