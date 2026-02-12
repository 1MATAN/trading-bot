"""Recursive Fibonacci retracement engine — the core innovation.

Finds swing high/low, computes Fibonacci levels, then recursively
computes sub-grids between adjacent levels for precision.
Results are cached with 24h TTL and invalidated when price exceeds grid top.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from config.settings import (
    FIB_LEVELS,
    FIB_LOOKBACK_DAYS,
    FIB_CACHE_TTL_HOURS,
    FIB_PROXIMITY_PCT,
    FIB_MAX_RECURSION_DEPTH,
)
from utils.time_utils import now_utc

logger = logging.getLogger("trading_bot.fibonacci")


# Cache: symbol → (timestamp, swing_low, swing_high, levels_list)
_fib_cache: dict[str, tuple[datetime, float, float, list[tuple[float, float]]]] = {}


def compute_fibonacci_levels(
    df: pd.DataFrame,
    symbol: str = "",
    depth: int = 0,
    swing_low_override: Optional[float] = None,
    swing_high_override: Optional[float] = None,
) -> list[tuple[float, float]]:
    """Compute Fibonacci retracement levels from price data.

    Returns list of (fib_ratio, price_level) tuples sorted by price.

    At depth 0, finds swing high/low from data.
    At depth > 0, uses override values for recursive sub-grids.
    """
    if depth == 0 and symbol and symbol in _fib_cache:
        cached_time, cached_low, cached_high, cached_levels = _fib_cache[symbol]
        age_hours = (now_utc() - cached_time).total_seconds() / 3600

        # Check TTL
        if age_hours < FIB_CACHE_TTL_HOURS:
            # Invalidate if price exceeded grid top
            current_price = df["close"].iloc[-1] if len(df) > 0 else 0
            if current_price <= cached_high * 1.05:  # 5% tolerance
                return cached_levels

    # Find swing high and low
    if swing_low_override is not None and swing_high_override is not None:
        swing_low = swing_low_override
        swing_high = swing_high_override
    else:
        if len(df) < 5:
            return []
        lookback = min(len(df), FIB_LOOKBACK_DAYS)
        recent = df.iloc[-lookback:]
        swing_low = recent["low"].min()
        swing_high = recent["high"].max()

    if swing_high <= swing_low:
        return []

    price_range = swing_high - swing_low
    levels = []

    for fib_ratio in FIB_LEVELS:
        price = swing_low + (price_range * fib_ratio)
        levels.append((fib_ratio, round(price, 4)))

    # Recursive sub-grids for precision
    if depth < FIB_MAX_RECURSION_DEPTH:
        current_price = df["close"].iloc[-1] if len(df) > 0 else 0
        sub_levels = _compute_sub_grids(
            df, current_price, levels, depth + 1
        )
        levels.extend(sub_levels)

    # Sort by price
    levels.sort(key=lambda x: x[1])

    # Remove duplicates (within tick tolerance)
    levels = _deduplicate_levels(levels)

    # Cache at top level
    if depth == 0 and symbol:
        _fib_cache[symbol] = (now_utc(), swing_low, swing_high, levels)
        logger.debug(
            f"Fibonacci {symbol}: {len(levels)} levels, "
            f"swing ${swing_low:.4f}–${swing_high:.4f}, depth={depth}"
        )

    return levels


def _compute_sub_grids(
    df: pd.DataFrame,
    current_price: float,
    parent_levels: list[tuple[float, float]],
    depth: int,
) -> list[tuple[float, float]]:
    """Recursively compute sub-grids between the two Fibonacci levels
    that bracket the current price."""
    if current_price <= 0 or not parent_levels:
        return []

    # Find bracketing levels
    lower_level = None
    upper_level = None
    for i, (ratio, price) in enumerate(parent_levels):
        if price <= current_price:
            lower_level = (ratio, price)
        elif upper_level is None and price > current_price:
            upper_level = (ratio, price)
            break

    if lower_level is None or upper_level is None:
        return []

    # Recurse between the bracketing levels
    return compute_fibonacci_levels(
        df,
        symbol="",
        depth=depth,
        swing_low_override=lower_level[1],
        swing_high_override=upper_level[1],
    )


def _deduplicate_levels(
    levels: list[tuple[float, float]], tolerance: float = 0.001
) -> list[tuple[float, float]]:
    """Remove levels that are too close together."""
    if not levels:
        return []
    result = [levels[0]]
    for ratio, price in levels[1:]:
        _, prev_price = result[-1]
        if prev_price > 0 and abs(price - prev_price) / prev_price < tolerance:
            continue
        result.append((ratio, price))
    return result


def find_nearest_support(
    levels: list[tuple[float, float]], current_price: float
) -> Optional[tuple[float, float]]:
    """Find the nearest Fibonacci support level below current price."""
    supports = [(r, p) for r, p in levels if p < current_price]
    if not supports:
        return None
    return supports[-1]  # sorted ascending, last one below current


def find_nearest_resistance(
    levels: list[tuple[float, float]], current_price: float
) -> Optional[tuple[float, float]]:
    """Find the nearest Fibonacci resistance level above current price."""
    resistances = [(r, p) for r, p in levels if p > current_price]
    if not resistances:
        return None
    return resistances[0]  # sorted ascending, first one above current


def is_near_fib_support(
    levels: list[tuple[float, float]],
    current_price: float,
    proximity_pct: float = FIB_PROXIMITY_PCT,
) -> bool:
    """Check if price is near a Fibonacci support level."""
    for ratio, price in levels:
        if price >= current_price:
            continue
        distance_pct = (current_price - price) / current_price
        if distance_pct <= proximity_pct:
            return True
    return False


def invalidate_cache(symbol: str) -> None:
    """Remove cached Fibonacci levels for a symbol."""
    _fib_cache.pop(symbol, None)


def clear_cache() -> None:
    """Clear all cached Fibonacci data."""
    _fib_cache.clear()
