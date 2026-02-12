"""All-or-nothing signal gate — all 5 conditions must pass."""

import logging
from dataclasses import dataclass

import pandas as pd

from strategies.indicators import (
    is_above_sma200,
    is_below_upper_bollinger,
    volume_spike,
    is_above_vwap,
    volume_ratio,
)
from strategies.fibonacci_engine import (
    compute_fibonacci_levels,
    is_near_fib_support,
    find_nearest_support,
    find_nearest_resistance,
)

logger = logging.getLogger("trading_bot.signal")


@dataclass
class SignalResult:
    """Result of signal scoring."""
    symbol: str
    passed: bool
    score: int  # 0–5
    conditions: dict[str, bool]
    details: dict
    fib_levels: list[tuple[float, float]]


def score_signal(
    symbol: str,
    daily_df: pd.DataFrame,
    intraday_df: pd.DataFrame,
) -> SignalResult:
    """Score a stock against all 5 signal conditions.

    Conditions (all must pass):
    1. Price above SMA200 (long-term uptrend)
    2. Volume spike (2x+ average volume)
    3. Price above VWAP (institutional demand)
    4. Price near Fibonacci support level
    5. Below upper Bollinger Band (not overbought)

    Args:
        symbol: Stock ticker
        daily_df: Daily bars (need 200+ for SMA200)
        intraday_df: Intraday bars for volume/VWAP analysis
    """
    current_price = daily_df["close"].iloc[-1] if len(daily_df) > 0 else 0

    # Condition 1: Above SMA200
    above_sma = is_above_sma200(daily_df)

    # Condition 2: Volume spike
    vol_spike = volume_spike(intraday_df if len(intraday_df) > 20 else daily_df)

    # Condition 3: Above VWAP
    above_vwap = is_above_vwap(intraday_df) if len(intraday_df) > 0 else False

    # Condition 4: Near Fibonacci support
    fib_levels = compute_fibonacci_levels(daily_df, symbol=symbol)
    near_fib = is_near_fib_support(fib_levels, current_price)

    # Condition 5: Not overbought (below upper Bollinger)
    below_bb = is_below_upper_bollinger(daily_df)

    conditions = {
        "above_sma200": above_sma,
        "volume_spike": vol_spike,
        "above_vwap": above_vwap,
        "near_fib_support": near_fib,
        "below_upper_bb": below_bb,
    }

    score = sum(conditions.values())
    passed = score == 5  # ALL must pass

    # Gather details for logging/dashboard
    support = find_nearest_support(fib_levels, current_price)
    resistance = find_nearest_resistance(fib_levels, current_price)
    vol_r = volume_ratio(intraday_df if len(intraday_df) > 20 else daily_df)

    details = {
        "current_price": current_price,
        "volume_ratio": round(vol_r, 2),
        "nearest_support": support[1] if support else None,
        "nearest_resistance": resistance[1] if resistance else None,
    }

    result = SignalResult(
        symbol=symbol,
        passed=passed,
        score=score,
        conditions=conditions,
        details=details,
        fib_levels=fib_levels,
    )

    if passed:
        support_str = f"${support[1]:.4f}" if support else "N/A"
        logger.info(
            f"SIGNAL PASSED: {symbol} @ ${current_price:.4f} "
            f"(vol={vol_r:.1f}x, support={support_str})"
        )
    else:
        failed = [k for k, v in conditions.items() if not v]
        logger.debug(f"Signal {symbol}: {score}/5 — failed: {failed}")

    return result
