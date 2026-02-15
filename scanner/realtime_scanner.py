"""Momentum scanner: IBKR scan + yfinance float filter + VWAP pullback check.

Filters:
- Price <= $20
- Moved up > 20% intraday
- Float <= 60M shares (via yfinance)
- VWAP pullback state machine for entry
"""

import asyncio
import logging
from typing import Optional

import pandas as pd
import yfinance as yf
from ib_insync import ScannerSubscription, Stock, TagValue

from broker.ibkr_connection import get_connection
from strategies.indicators import bars_to_dataframe
from strategies.signal_checker import (
    check_entry, EntrySignal, PullbackStateMachine,
)
from config.settings import (
    SCAN_PRICE_MAX,
    SCAN_FLOAT_MAX,
    SCAN_MOVE_PCT_MIN,
    SCAN_MAX_RESULTS,
    SCAN_INTERVAL_SECONDS,
    VWAP_WARMUP_BARS,
)

logger = logging.getLogger("trading_bot.scanner")


class RealtimeScanner:
    """Momentum scanner with IBKR + yfinance enrichment."""

    def __init__(self) -> None:
        self._conn = get_connection()
        self._running = False
        self._last_candidates: list[EntrySignal] = []
        self._scan_count = 0
        self._pullback_states: dict[str, PullbackStateMachine] = {}

    @property
    def last_candidates(self) -> list[EntrySignal]:
        return self._last_candidates

    async def scan_once(self) -> list[EntrySignal]:
        """Run one full scan cycle."""
        self._scan_count += 1

        # Step 1: IBKR scanner (price + gap filters)
        raw_symbols = await self._ibkr_scan()
        if not raw_symbols:
            logger.debug("Scanner: No candidates from IBKR")
            return []
        logger.info(f"Scanner step 1: {len(raw_symbols)} candidates from IBKR")

        # Step 2: Float filter via yfinance
        filtered = self._filter_by_float(raw_symbols)
        if not filtered:
            logger.debug("Scanner: No candidates passed float filter")
            return []
        logger.info(f"Scanner step 2: {len(filtered)} passed float filter (<= 60M)")

        # Step 3: Multi-timeframe SMA + Fibonacci analysis
        signals = []
        for symbol in filtered:
            try:
                signal = await self._analyze_candidate(symbol)
                if signal and signal.passed:
                    signals.append(signal)
            except Exception as e:
                logger.error(f"Error analyzing {symbol}: {e}")

        self._last_candidates = signals
        if signals:
            symbols_str = ", ".join(s.symbol for s in signals)
            logger.info(
                f"Scan #{self._scan_count}: {len(signals)} signals — {symbols_str}"
            )
        else:
            logger.debug(f"Scan #{self._scan_count}: No signals passed")

        return signals

    async def start(self, callback) -> None:
        """Start continuous scanning loop."""
        self._running = True
        logger.info("Real-time scanner started")
        while self._running:
            try:
                signals = await self.scan_once()
                if signals:
                    await callback(signals)
            except Exception as e:
                logger.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    def stop(self) -> None:
        self._running = False
        logger.info("Real-time scanner stopped")

    async def _ibkr_scan(self) -> list[str]:
        """IBKR scanner: top % gainers under $20."""
        sub = ScannerSubscription(
            instrument="STK",
            locationCode="STK.US.MAJOR",
            scanCode="TOP_PERC_GAIN",
            numberOfRows=SCAN_MAX_RESULTS,
        )

        tag_values = [
            TagValue("priceBelow", str(SCAN_PRICE_MAX)),
            TagValue("changePercAbove", str(SCAN_MOVE_PCT_MIN)),
        ]

        try:
            results = await self._conn.ib.reqScannerDataAsync(sub, [], tag_values)
            if not results:
                return []
            return [
                r.contractDetails.contract.symbol
                for r in results
                if r.contractDetails
            ]
        except Exception as e:
            logger.error(f"IBKR scanner request failed: {e}")
            return []

    def _filter_by_float(self, symbols: list[str]) -> list[str]:
        """Filter symbols by float <= SCAN_FLOAT_MAX using yfinance."""
        passed = []
        for symbol in symbols:
            try:
                info = yf.Ticker(symbol).info
                float_shares = info.get("floatShares") or info.get("sharesOutstanding", 0)
                if float_shares and float_shares <= SCAN_FLOAT_MAX:
                    passed.append(symbol)
                    logger.debug(f"{symbol}: float={float_shares:,.0f} — PASS")
                else:
                    logger.debug(
                        f"{symbol}: float={float_shares:,.0f} — "
                        f"FAIL (> {SCAN_FLOAT_MAX:,.0f})"
                    )
            except Exception as e:
                logger.warning(f"Float lookup failed for {symbol}: {e}")
                # Include by default if we can't check (conservative approach)
                passed.append(symbol)
        return passed

    async def _analyze_candidate(self, symbol: str) -> Optional[EntrySignal]:
        """Fetch intraday data and run VWAP pullback state machine."""
        contract = Stock(symbol, "SMART", "USD")
        qualified = await self._conn.qualify_contract(contract)
        if qualified is None:
            return None

        # Fetch intraday data (1-min with extended hours)
        bars_1m = await self._conn.get_historical_data(
            qualified, duration="1 D", bar_size="1 min",
            what_to_show="TRADES", use_rth=False,
        )

        df_1m = bars_to_dataframe(bars_1m)
        if len(df_1m) < VWAP_WARMUP_BARS:
            return None

        # Get or create per-symbol pullback state
        if symbol not in self._pullback_states:
            self._pullback_states[symbol] = PullbackStateMachine()
        state = self._pullback_states[symbol]

        # Run pullback state machine
        return check_entry(symbol, df_1m, state, gap_pct=SCAN_MOVE_PCT_MIN)
