"""Gap scanner: find stocks gapping up >= 10% via IBKR scanner.

Uses IBKR's "Top % Gainers" scanner filtered to $1-$20, then
computes actual gap % from previous close using daily bars.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from ib_insync import Contract, ScannerSubscription, Stock, TagValue

from broker.ibkr_connection import get_connection
from config.settings import (
    FIB_LIVE_GAP_MIN_PCT,
    FIB_LIVE_MAX_SYMBOLS,
    SCAN_PRICE_MIN,
    SCAN_PRICE_MAX,
    SCAN_MAX_RESULTS,
)

logger = logging.getLogger("trading_bot.gap_scanner")


@dataclass
class GapSignal:
    """A stock that gapped up enough to qualify for fib strategy."""
    symbol: str
    contract: Contract
    gap_pct: float
    prev_close: float
    current_price: float


class GapScanner:
    """Scan for gap stocks via IBKR scanner API."""

    def __init__(self) -> None:
        self._conn = get_connection()
        self._scan_count = 0

    async def scan_once(self) -> list[GapSignal]:
        """Run one gap scan cycle.

        1. IBKR scanner: Top % Gainers, $1-$20
        2. Get previous close from daily bars
        3. Filter by gap_pct >= FIB_LIVE_GAP_MIN_PCT
        4. Return top N sorted by gap %
        """
        self._scan_count += 1

        # Step 1: IBKR scanner
        raw = await self._ibkr_scan()
        if not raw:
            logger.debug(f"Gap scan #{self._scan_count}: no candidates from IBKR")
            return []
        logger.info(f"Gap scan #{self._scan_count}: {len(raw)} raw candidates")

        # Step 2+3: Get prev close, compute gap, filter
        signals = []
        for symbol, contract in raw:
            try:
                signal = await self._check_gap(symbol, contract)
                if signal:
                    signals.append(signal)
            except Exception as e:
                logger.debug(f"Gap check failed for {symbol}: {e}")

        # Sort by gap % descending, limit to max symbols
        signals.sort(key=lambda s: s.gap_pct, reverse=True)
        signals = signals[:FIB_LIVE_MAX_SYMBOLS]

        if signals:
            syms = ", ".join(f"{s.symbol}(+{s.gap_pct:.1f}%)" for s in signals)
            logger.info(f"Gap scan #{self._scan_count}: {len(signals)} gappers — {syms}")

        return signals

    async def _ibkr_scan(self) -> list[tuple[str, Contract]]:
        """IBKR scanner: top % gainers under $20."""
        sub = ScannerSubscription(
            instrument="STK",
            locationCode="STK.US.MAJOR",
            scanCode="TOP_PERC_GAIN",
            numberOfRows=SCAN_MAX_RESULTS,
        )
        tag_values = [
            TagValue("priceAbove", str(SCAN_PRICE_MIN)),
            TagValue("priceBelow", str(SCAN_PRICE_MAX)),
        ]

        try:
            results = await self._conn.ib.reqScannerDataAsync(sub, [], tag_values)
            if not results:
                return []
            out = []
            for r in results:
                if r.contractDetails:
                    c = r.contractDetails.contract
                    out.append((c.symbol, c))
            return out
        except Exception as e:
            logger.error(f"IBKR gap scanner failed: {e}")
            return []

    async def _check_gap(self, symbol: str, contract: Contract) -> Optional[GapSignal]:
        """Get previous close and current price, compute gap %."""
        # Qualify the contract
        qualified = await self._conn.qualify_contract(contract)
        if qualified is None:
            return None

        # Request 2 daily bars to get previous close
        bars = await self._conn.get_historical_data(
            qualified,
            duration="2 D",
            bar_size="1 day",
            what_to_show="TRADES",
            use_rth=True,
        )

        if not bars or len(bars) < 2:
            return None

        prev_close = float(bars[-2].close)
        current_price = float(bars[-1].close)

        if prev_close <= 0:
            return None

        # Use the higher of today's open and current price for gap calc
        today_open = float(bars[-1].open)
        today_high = float(bars[-1].high)
        effective_price = max(today_open, current_price, today_high)

        gap_pct = (effective_price - prev_close) / prev_close * 100

        if gap_pct < FIB_LIVE_GAP_MIN_PCT:
            return None

        # Price filter
        if not (SCAN_PRICE_MIN <= current_price <= SCAN_PRICE_MAX):
            return None

        return GapSignal(
            symbol=symbol,
            contract=qualified,
            gap_pct=round(gap_pct, 1),
            prev_close=round(prev_close, 4),
            current_price=round(current_price, 4),
        )
