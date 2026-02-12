"""Main orchestrator — wires all components, runs async event loop.

Usage:
    python main.py              # Run trading bot
    python main.py --backtest   # Run backtester instead
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
)
from utils.helpers import setup_logging
from utils.time_utils import (
    is_market_open,
    is_premarket,
    is_afterhours,
    is_any_session_active,
    seconds_until_market_open,
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
        self._partial_exit_mgr = None
        self._momentum = None
        self._scanner = None
        self._premarket_scanner = None
        self._afterhours_scanner = None
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
        from risk.partial_exits import PartialExitManager
        from strategies.momentum_entry import MomentumEntry
        from scanner.realtime_scanner import RealtimeScanner
        from scanner.premarket_scanner import PremarketScanner
        from scanner.afterhours_scanner import AfterhoursScanner
        from notifications.trade_logger import TradeLogger

        self._conn = get_connection()
        self._order_mgr = OrderManager()
        self._position_mgr = PositionManager()
        self._risk_mgr = RiskManager()
        self._trailing_mgr = TrailingStopManager(self._order_mgr, self._position_mgr)
        self._partial_exit_mgr = PartialExitManager(self._order_mgr, self._position_mgr)
        self._momentum = MomentumEntry(
            self._order_mgr, self._position_mgr,
            self._risk_mgr, self._trailing_mgr,
        )
        self._scanner = RealtimeScanner()
        self._premarket_scanner = PremarketScanner()
        self._afterhours_scanner = AfterhoursScanner()
        self._trade_logger = TradeLogger()

    async def _main_loop(self) -> None:
        """Main event loop — dispatches to session-appropriate scanner."""
        trailing_task = asyncio.create_task(self._trailing_mgr.start())

        try:
            while self._running:
                self._check_control_commands()

                if is_market_open():
                    await self._run_market_cycle()
                elif is_premarket():
                    logger.info("Pre-market session — building watchlist")
                    await self._premarket_scanner.scan_once()
                elif is_afterhours():
                    logger.info("After-hours session — building watchlist")
                    await self._afterhours_scanner.scan_once()
                else:
                    wait = seconds_until_market_open()
                    logger.info(
                        f"Market closed. Next open in {wait / 3600:.1f} hours. "
                        "Sleeping 60s..."
                    )
                    await asyncio.sleep(60)
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

    async def _run_market_cycle(self) -> None:
        """Run one market-hours scan + trade cycle."""
        # Scan for candidates
        signals = await self._scanner.scan_once()

        # Execute entries for passing signals
        for signal in signals:
            try:
                entered = await self._momentum.execute_entry(signal)
                if entered:
                    self._trade_logger.log_entry(
                        symbol=signal.symbol,
                        quantity=self._position_mgr.get_position(signal.symbol).quantity,
                        price=signal.details["current_price"],
                        stop_price=self._position_mgr.get_position(signal.symbol).stop_loss_price,
                    )
                    from notifications.telegram_bot import notify_entry
                    pos = self._position_mgr.get_position(signal.symbol)
                    await notify_entry(
                        symbol=signal.symbol,
                        quantity=pos.quantity,
                        price=pos.entry_price,
                        stop_price=pos.stop_loss_price,
                        risk_dollars=(pos.entry_price - pos.stop_loss_price) * pos.quantity,
                    )
            except Exception as e:
                logger.error(f"Entry execution error for {signal.symbol}: {e}")

        # Check partial exits for existing positions
        await self._check_partial_exits()

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

    async def _check_partial_exits(self) -> None:
        """Check all positions for partial exit opportunities."""
        for symbol in list(self._position_mgr.get_all_symbols()):
            pos = self._position_mgr.get_position(symbol)
            if pos is None:
                continue
            try:
                ticker = self._conn.ib.ticker(pos.contract)
                current_price = (
                    ticker.last if ticker and ticker.last and ticker.last > 0
                    else pos.entry_price
                )
                shares_sold = await self._partial_exit_mgr.check_exits(
                    symbol, current_price
                )
                if shares_sold > 0:
                    self._trade_logger.log_partial_exit(
                        symbol=symbol,
                        quantity=shares_sold,
                        price=current_price,
                        entry_price=pos.entry_price,
                        stage=pos.partial_exits_done,
                    )
                    from notifications.telegram_bot import notify_partial_exit
                    await notify_partial_exit(
                        symbol=symbol,
                        quantity=shares_sold,
                        price=current_price,
                        stage=pos.partial_exits_done,
                        total_stages=4,
                    )
            except Exception as e:
                logger.error(f"Partial exit check error for {symbol}: {e}")

    def _write_live_state(self) -> None:
        """Write current state to JSON for dashboard consumption."""
        try:
            state = {
                "timestamp": now_utc().isoformat(),
                "market_open": is_market_open(),
                "premarket": is_premarket(),
                "afterhours": is_afterhours(),
                "positions": self._position_mgr.to_dict_list(),
                "risk_status": self._risk_mgr.get_status(),
                "scanner_candidates": len(self._scanner.last_candidates),
                "premarket_watchlist": len(self._premarket_scanner.watchlist),
                "afterhours_watchlist": len(self._afterhours_scanner.watchlist),
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
            # Clear after processing
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

        # Send daily summary
        summary = self._trade_logger.get_daily_summary()
        if summary:
            from notifications.telegram_bot import notify_daily_summary
            await notify_daily_summary(summary)

        from notifications.telegram_bot import notify_system
        await notify_system("Bot shutting down")

        # Disconnect from IBKR
        if self._conn:
            await self._conn.disconnect()

        # Kill dashboard
        if self._dashboard_proc:
            self._dashboard_proc.terminate()

        logger.info("Shutdown complete")


def main():
    parser = argparse.ArgumentParser(description="Penny Stock Trading Bot")
    parser.add_argument("--backtest", action="store_true", help="Run backtester")
    parser.add_argument("--dashboard", action="store_true", help="Launch dashboard only")
    args = parser.parse_args()

    if args.backtest:
        from backtesting.backtest_engine import run_backtest
        asyncio.run(run_backtest())
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
