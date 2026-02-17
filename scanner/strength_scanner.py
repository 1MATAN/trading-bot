"""Extended hours strength scanner: pre-market + after-hours strong stocks.

Filters:
- IBKR Top % Gainers, $1-$20, all sessions (use_rth=False)
- Gain >= 20% (FIB_CONFIRM_GAIN_MIN_PCT)
- Float <= 60M shares via IBKR fundamental data
- RVOL >= 1.5x vs 14-day average (skipped before 6 AM ET)
"""

import logging
from dataclasses import dataclass
from datetime import time as dt_time
from typing import Optional

from ib_insync import Contract, ScannerSubscription, Stock, TagValue

from broker.ibkr_connection import get_connection
from config.settings import (
    FIB_CONFIRM_GAIN_MIN_PCT,
    FIB_CONFIRM_FLOAT_MAX,
    FIB_CONFIRM_MAX_SYMBOLS,
    FIB_CONFIRM_RVOL_MIN,
    SCAN_PRICE_MIN,
    SCAN_PRICE_MAX,
    SCAN_MAX_RESULTS,
)
from utils.time_utils import now_et

logger = logging.getLogger("trading_bot.strength_scanner")

# Cache IBKR float lookups for the session
_float_cache: dict[str, Optional[float]] = {}


@dataclass
class StrengthSignal:
    """A stock with strong extended-hours momentum."""
    symbol: str
    contract: Contract
    gain_pct: float
    float_shares: float
    rvol: float
    current_price: float
    prev_close: float


class StrengthScanner:
    """Scan for strong stocks in extended hours via IBKR."""

    def __init__(self) -> None:
        self._conn = get_connection()
        self._scan_count = 0

    async def scan_once(self) -> list[StrengthSignal]:
        """Run one strength scan cycle.

        1. IBKR scanner: Top % Gainers, $1-$20, all sessions
        2. Filter: gain >= 20%
        3. IBKR float filter: <= 60M shares (cached)
        4. RVOL filter: >= 1.5x (skipped before 6 AM ET)
        5. Sort by gain_pct desc, limit to max symbols
        """
        self._scan_count += 1

        # Step 1: IBKR scanner (all sessions)
        raw = await self._ibkr_scan()
        if not raw:
            logger.debug(f"Strength scan #{self._scan_count}: no candidates from IBKR")
            return []
        logger.info(f"Strength scan #{self._scan_count}: {len(raw)} raw candidates")

        # Step 2+3+4: Check gain, float, RVOL
        signals = []
        for symbol, contract in raw:
            try:
                signal = await self._check_candidate(symbol, contract)
                if signal:
                    signals.append(signal)
            except Exception as e:
                logger.debug(f"Strength check failed for {symbol}: {e}")

        # Sort by gain % descending, limit
        signals.sort(key=lambda s: s.gain_pct, reverse=True)
        signals = signals[:FIB_CONFIRM_MAX_SYMBOLS]

        if signals:
            syms = ", ".join(
                f"{s.symbol}(+{s.gain_pct:.1f}% rv={s.rvol:.1f}x)"
                for s in signals
            )
            logger.info(
                f"Strength scan #{self._scan_count}: {len(signals)} passed — {syms}"
            )

        return signals

    async def _ibkr_scan(self) -> list[tuple[str, Contract]]:
        """IBKR scanner: top % gainers under $20, all sessions."""
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
            logger.error(f"IBKR strength scanner failed: {e}")
            return []

    async def _check_candidate(
        self, symbol: str, contract: Contract
    ) -> Optional[StrengthSignal]:
        """Check gain %, float, and RVOL for a candidate."""
        # Qualify the contract
        qualified = await self._conn.qualify_contract(contract)
        if qualified is None:
            return None

        # Get daily bars (2 days for prev close + today's price)
        bars = await self._conn.get_historical_data(
            qualified,
            duration="2 D",
            bar_size="1 day",
            what_to_show="TRADES",
            use_rth=False,
        )

        if not bars or len(bars) < 2:
            return None

        prev_close = float(bars[-2].close)
        current_price = float(bars[-1].close)

        if prev_close <= 0:
            return None

        # Use the higher of today's open, high, and close for gain calc
        today_open = float(bars[-1].open)
        today_high = float(bars[-1].high)
        effective_price = max(today_open, current_price, today_high)

        gain_pct = (effective_price - prev_close) / prev_close * 100

        # Step 2: Gain filter
        if gain_pct < FIB_CONFIRM_GAIN_MIN_PCT:
            return None

        # Price filter
        if not (SCAN_PRICE_MIN <= current_price <= SCAN_PRICE_MAX):
            return None

        # Step 3: Float filter (cached, via IBKR fundamental data)
        float_shares = await self._get_float(symbol, qualified)
        if float_shares is not None and float_shares > FIB_CONFIRM_FLOAT_MAX:
            logger.debug(
                f"{symbol}: float={float_shares:,.0f} — "
                f"FAIL (> {FIB_CONFIRM_FLOAT_MAX:,.0f})"
            )
            return None

        # Step 4: RVOL filter (skip before 6 AM ET — volume too low)
        rvol_value = 0.0
        now = now_et()
        if now.time() >= dt_time(6, 0):
            rvol_value = await self._compute_rvol(qualified)
            if rvol_value > 0 and rvol_value < FIB_CONFIRM_RVOL_MIN:
                logger.debug(
                    f"{symbol}: RVOL={rvol_value:.2f}x — "
                    f"FAIL (< {FIB_CONFIRM_RVOL_MIN}x)"
                )
                return None

        return StrengthSignal(
            symbol=symbol,
            contract=qualified,
            gain_pct=round(gain_pct, 1),
            float_shares=float_shares or 0,
            rvol=rvol_value,
            current_price=round(current_price, 4),
            prev_close=round(prev_close, 4),
        )

    async def _get_float(self, symbol: str, contract: Contract) -> Optional[float]:
        """Get float shares from IBKR fundamental data (cached per session)."""
        if symbol in _float_cache:
            return _float_cache[symbol]

        try:
            import xml.etree.ElementTree as ET

            xml_data = await self._conn.ib.reqFundamentalDataAsync(
                contract, reportType="ReportSnapshot"
            )
            if xml_data:
                root = ET.fromstring(xml_data)
                for item in root.iter("SharesOut"):
                    val = item.text or item.get("TotalFloat", "0")
                    float_shares = float(val) * 1_000_000
                    _float_cache[symbol] = float_shares
                    if float_shares:
                        logger.debug(f"{symbol}: float={float_shares:,.0f}")
                    return float_shares
        except Exception as e:
            logger.warning(f"Float lookup failed for {symbol}: {e}")

        _float_cache[symbol] = None
        return None

    async def _compute_rvol(self, contract: Contract) -> float:
        """Compute relative volume: today's vol / avg(14 daily bars).

        Uses daily bars to avoid time-of-day mismatch issues in extended hours.
        """
        try:
            bars = await self._conn.get_historical_data(
                contract,
                duration="15 D",
                bar_size="1 day",
                what_to_show="TRADES",
                use_rth=False,
            )
            if not bars or len(bars) < 3:
                return 0.0

            today_vol = float(bars[-1].volume)
            if today_vol <= 0:
                return 0.0

            # Average volume of previous 14 days (excluding today)
            hist_vols = [float(b.volume) for b in bars[:-1] if float(b.volume) > 0]
            if not hist_vols:
                return 0.0

            avg_vol = sum(hist_vols) / len(hist_vols)
            if avg_vol <= 0:
                return 0.0

            return round(today_vol / avg_vol, 2)
        except Exception as e:
            logger.debug(f"RVOL computation failed: {e}")
            return 0.0
