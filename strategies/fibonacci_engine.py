"""Dual-series Fibonacci engine with auto-advance.

Series 1: anchor Low → High of the same candle.
Series 2: anchor Low → 4.236 level of Series 1.
Auto-advance: when price crosses 4.236 of current series, shift up.
24 Fibonacci levels per series.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from config.settings import FIB_LEVELS_24, FIB_LOOKBACK_YEARS, FIB_CACHE_TTL_HOURS
from utils.time_utils import now_utc

logger = logging.getLogger("trading_bot.fibonacci")


@dataclass
class FibSeries:
    """One Fibonacci series with 24 levels."""
    low: float
    high: float
    levels: list[tuple[float, float]]  # (ratio, price)


@dataclass
class DualFibSeries:
    """Dual Fibonacci series: Series 1 (primary) + Series 2 (extended)."""
    series1: FibSeries
    series2: FibSeries
    anchor_low: float
    anchor_high: float
    advance_count: int = 0


# Cache: symbol → (timestamp, DualFibSeries)
_fib_cache: dict[str, tuple[datetime, DualFibSeries]] = {}


def find_anchor_candle(daily_df: pd.DataFrame) -> Optional[tuple[float, float, str]]:
    """Find the candle with the lowest Low in the lookback period.

    Returns (low, high, date_str) of that candle, or None if insufficient data.
    The lookback is FIB_LOOKBACK_YEARS years.
    """
    if daily_df.empty or len(daily_df) < 5:
        return None

    lookback_days = FIB_LOOKBACK_YEARS * 365
    if len(daily_df) > lookback_days:
        df = daily_df.iloc[-lookback_days:]
    else:
        df = daily_df

    # Find the row with the minimum low
    min_idx = df["low"].idxmin()
    anchor_row = df.loc[min_idx]
    anchor_low = float(anchor_row["low"])
    anchor_high = float(anchor_row["high"])
    anchor_date = str(min_idx)[:10] if hasattr(min_idx, 'strftime') else str(min_idx)[:10]

    if anchor_high <= anchor_low:
        anchor_high = anchor_low * 1.01  # tiny fallback range

    logger.debug(
        f"Anchor candle: Low=${anchor_low:.4f} High=${anchor_high:.4f} "
        f"Date={min_idx}"
    )
    return anchor_low, anchor_high, anchor_date


def build_series(low: float, high: float) -> FibSeries:
    """Build a single Fibonacci series with 24 levels from low to high."""
    price_range = high - low
    levels = []
    for ratio in FIB_LEVELS_24:
        price = low + (price_range * ratio)
        levels.append((ratio, round(price, 4)))
    return FibSeries(low=low, high=high, levels=levels)


def build_dual_series(anchor_low: float, anchor_high: float) -> DualFibSeries:
    """Build both Series 1 and Series 2 from the anchor candle.

    Series 1: anchor_low → anchor_high
    Series 2: anchor_low → 4.236 level of Series 1
    """
    series1 = build_series(anchor_low, anchor_high)

    # Series 2 top = 4.236 level of Series 1
    s1_range = anchor_high - anchor_low
    s2_high = anchor_low + (s1_range * 4.236)
    series2 = build_series(anchor_low, s2_high)

    return DualFibSeries(
        series1=series1,
        series2=series2,
        anchor_low=anchor_low,
        anchor_high=anchor_high,
    )


def advance_series(dual: DualFibSeries) -> DualFibSeries:
    """Advance the series when price crosses 4.236 of current Series 1.

    New Series 1 = old Series 2
    New Series 2 = anchor_low → 4.236 of new Series 1
    """
    new_s1 = dual.series2

    # New Series 2 top = 4.236 level of new Series 1
    new_s1_range = new_s1.high - new_s1.low
    new_s2_high = new_s1.low + (new_s1_range * 4.236)
    new_s2 = build_series(new_s1.low, new_s2_high)

    advanced = DualFibSeries(
        series1=new_s1,
        series2=new_s2,
        anchor_low=dual.anchor_low,
        anchor_high=dual.anchor_high,
        advance_count=dual.advance_count + 1,
    )

    logger.info(
        f"Fibonacci auto-advance #{advanced.advance_count}: "
        f"S1 range ${new_s1.low:.4f}-${new_s1.high:.4f}, "
        f"S2 top ${new_s2_high:.4f}"
    )
    return advanced


def get_active_levels(
    daily_df: pd.DataFrame,
    current_price: float,
    symbol: str = "",
) -> Optional[DualFibSeries]:
    """Main entry point: compute or retrieve dual-series Fibonacci levels.

    Auto-advances if current price is above the 4.236 level of Series 1.
    Results are cached per symbol.
    """
    # Check cache
    if symbol and symbol in _fib_cache:
        cached_time, cached_dual = _fib_cache[symbol]
        age_hours = (now_utc() - cached_time).total_seconds() / 3600
        if age_hours < FIB_CACHE_TTL_HOURS:
            # Auto-advance if needed
            s1_4236 = get_fib_level_price(cached_dual.series1.levels, 4.236)
            if s1_4236 and current_price > s1_4236:
                cached_dual = advance_series(cached_dual)
                _fib_cache[symbol] = (now_utc(), cached_dual)
            return cached_dual

    # Build from scratch
    anchor = find_anchor_candle(daily_df)
    if anchor is None:
        return None

    anchor_low, anchor_high, _anchor_date = anchor
    dual = build_dual_series(anchor_low, anchor_high)

    # Auto-advance until current price is within Series 1 range
    while True:
        s1_4236 = get_fib_level_price(dual.series1.levels, 4.236)
        if s1_4236 is None or current_price <= s1_4236:
            break
        dual = advance_series(dual)
        if dual.advance_count > 20:  # safety limit
            break

    # Cache
    if symbol:
        _fib_cache[symbol] = (now_utc(), dual)
        logger.debug(
            f"Fibonacci {symbol}: anchor ${anchor_low:.4f}-${anchor_high:.4f}, "
            f"advances={dual.advance_count}, "
            f"S1=[${dual.series1.low:.4f}-${dual.series1.high:.4f}], "
            f"S2 top=${dual.series2.high:.4f}"
        )

    return dual


def get_fib_level_price(
    levels: list[tuple[float, float]], ratio: float
) -> Optional[float]:
    """Get the price at a specific Fibonacci ratio from a levels list."""
    for r, price in levels:
        if abs(r - ratio) < 0.001:
            return price
    return None


def get_current_fib_zone(
    dual: DualFibSeries, current_price: float
) -> Optional[tuple[float, float, float, float]]:
    """Find the Fibonacci levels bracketing the current price in Series 1.

    Returns (lower_ratio, lower_price, upper_ratio, upper_price) or None.
    """
    levels = dual.series1.levels
    lower = None
    upper = None
    for ratio, price in levels:
        if price <= current_price:
            lower = (ratio, price)
        elif upper is None:
            upper = (ratio, price)
            break

    if lower and upper:
        return lower[0], lower[1], upper[0], upper[1]
    return None


def invalidate_cache(symbol: str) -> None:
    """Remove cached Fibonacci levels for a symbol."""
    _fib_cache.pop(symbol, None)


def clear_cache() -> None:
    """Clear all cached Fibonacci data."""
    _fib_cache.clear()
