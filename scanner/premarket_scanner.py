"""Pre-market watchlist builder (4:00–9:30 AM ET).

Builds a watchlist of candidates for the regular session.
Does not trade directly — thin liquidity in pre-market.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

import yfinance as yf
from ib_insync import ScannerSubscription, Stock, TagValue

from broker.ibkr_connection import get_connection
from config.settings import (
    SCAN_PRICE_MIN,
    SCAN_PRICE_MAX,
    SCAN_MAX_RESULTS,
    WATCHLIST_REFRESH_SECONDS,
)
from utils.time_utils import is_premarket, now_et

logger = logging.getLogger("trading_bot.premarket")


class PremarketScanner:
    """Pre-market watchlist builder."""

    def __init__(self) -> None:
        self._conn = get_connection()
        self._watchlist: list[dict] = []
        self._running = False

    @property
    def watchlist(self) -> list[dict]:
        return self._watchlist

    async def scan_once(self) -> list[dict]:
        """Scan pre-market movers and build watchlist."""
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
        except Exception as e:
            logger.error(f"Pre-market scan failed: {e}")
            return self._watchlist

        if not results:
            return self._watchlist

        candidates = []
        for r in results:
            if not r.contractDetails:
                continue
            symbol = r.contractDetails.contract.symbol
            info = await self._enrich_with_float(symbol)
            candidates.append({
                "symbol": symbol,
                "scan_time": now_et().isoformat(),
                "float_shares": info.get("float_shares"),
                "market_cap": info.get("market_cap"),
                "source": "premarket",
            })

        self._watchlist = candidates
        logger.info(f"Pre-market watchlist: {len(candidates)} candidates")
        return self._watchlist

    async def start(self) -> None:
        """Run pre-market scanning loop."""
        self._running = True
        logger.info("Pre-market scanner started")
        while self._running and is_premarket():
            await self.scan_once()
            await asyncio.sleep(WATCHLIST_REFRESH_SECONDS)
        logger.info("Pre-market session ended")

    def stop(self) -> None:
        self._running = False

    async def _enrich_with_float(self, symbol: str) -> dict:
        """Supplement with float data from yfinance (IBKR lacks this)."""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            return {
                "float_shares": info.get("floatShares"),
                "market_cap": info.get("marketCap"),
            }
        except Exception:
            return {"float_shares": None, "market_cap": None}
