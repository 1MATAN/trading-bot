"""3-layer real-time scanning pipeline using IBKR reqScannerSubscription.

Layer 1: IBKR built-in filter (price, volume, % change)
Layer 2: Deep analysis (MA200, volume spike, Bollinger)
Layer 3: Fibonacci confirmation via signal scorer
"""

import asyncio
import logging
from typing import Optional

import pandas as pd
from ib_insync import (
    ScannerSubscription, Stock, TagValue,
)

from broker.ibkr_connection import get_connection
from strategies.indicators import bars_to_dataframe
from strategies.signal_scorer import score_signal, SignalResult
from config.settings import (
    SCAN_PRICE_MIN,
    SCAN_PRICE_MAX,
    SCAN_VOLUME_MIN,
    SCAN_CHANGE_PCT_MIN,
    SCAN_MAX_RESULTS,
    SCAN_INTERVAL_SECONDS,
)

logger = logging.getLogger("trading_bot.scanner")


class RealtimeScanner:
    """3-layer penny stock scanner."""

    def __init__(self) -> None:
        self._conn = get_connection()
        self._running = False
        self._last_candidates: list[SignalResult] = []
        self._scan_count = 0

    @property
    def last_candidates(self) -> list[SignalResult]:
        return self._last_candidates

    async def scan_once(self) -> list[SignalResult]:
        """Run one full scan cycle through all 3 layers."""
        self._scan_count += 1

        # Layer 1: IBKR built-in scanner
        layer1_symbols = await self._layer1_ibkr_scan()
        if not layer1_symbols:
            logger.debug("Layer 1: No candidates from IBKR scanner")
            return []
        logger.info(f"Layer 1: {len(layer1_symbols)} candidates from IBKR scanner")

        # Layer 2 & 3: Deep analysis + Fibonacci confirmation
        signals = []
        for symbol in layer1_symbols:
            try:
                result = await self._analyze_candidate(symbol)
                if result and result.passed:
                    signals.append(result)
            except Exception as e:
                logger.error(f"Error analyzing {symbol}: {e}")

        self._last_candidates = signals
        if signals:
            symbols_str = ", ".join(s.symbol for s in signals)
            logger.info(
                f"Scan #{self._scan_count}: {len(signals)} signals passed — {symbols_str}"
            )
        else:
            logger.debug(f"Scan #{self._scan_count}: No signals passed")

        return signals

    async def start(self, callback) -> None:
        """Start continuous scanning loop.

        Args:
            callback: async function called with list of SignalResults.
        """
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

    async def _layer1_ibkr_scan(self) -> list[str]:
        """Layer 1: Use IBKR's built-in market scanner."""
        sub = ScannerSubscription(
            instrument="STK",
            locationCode="STK.US.MAJOR",
            scanCode="TOP_PERC_GAIN",
            numberOfRows=SCAN_MAX_RESULTS,
        )

        tag_values = [
            TagValue("priceAbove", str(SCAN_PRICE_MIN)),
            TagValue("priceBelow", str(SCAN_PRICE_MAX)),
            TagValue("volumeAbove", str(SCAN_VOLUME_MIN)),
            TagValue("changePercAbove", str(SCAN_CHANGE_PCT_MIN)),
        ]

        try:
            results = await self._conn.ib.reqScannerDataAsync(sub, [], tag_values)
            if not results:
                return []
            symbols = [
                r.contractDetails.contract.symbol
                for r in results
                if r.contractDetails
            ]
            return symbols
        except Exception as e:
            logger.error(f"IBKR scanner request failed: {e}")
            return []

    async def _analyze_candidate(self, symbol: str) -> Optional[SignalResult]:
        """Layer 2 + 3: Deep analysis and Fibonacci scoring."""
        contract = Stock(symbol, "SMART", "USD")
        qualified = await self._conn.qualify_contract(contract)
        if qualified is None:
            return None

        # Fetch daily data (for SMA200, Fibonacci, Bollinger)
        daily_bars = await self._conn.get_historical_data(
            qualified,
            duration="1 Y",
            bar_size="1 day",
            what_to_show="TRADES",
            use_rth=True,
        )
        daily_df = bars_to_dataframe(daily_bars)
        if len(daily_df) < 50:  # need reasonable history
            return None

        # Fetch intraday data (for VWAP, volume spike)
        intraday_bars = await self._conn.get_historical_data(
            qualified,
            duration="1 D",
            bar_size="5 mins",
            what_to_show="TRADES",
            use_rth=False,
        )
        intraday_df = bars_to_dataframe(intraday_bars)

        # Score through all conditions
        return score_signal(symbol, daily_df, intraday_df)
