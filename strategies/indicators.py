"""Technical indicators: SMA, RVOL, VWAP with multi-timeframe support."""

import numpy as np
import pandas as pd

from config.settings import (
    SMA_200_PERIODS, SMA_30M_PERIOD, SMA_9_PERIOD, SMA_20_PERIOD,
    MA20_SLOPE_LOOKBACK,
)


def sma(series: pd.Series, period: int) -> pd.Series:
    """Simple Moving Average."""
    return series.rolling(window=period, min_periods=period).mean()


def sma_value(df: pd.DataFrame, period: int) -> float:
    """Return the current (last) SMA value for the close column.

    Returns 0.0 if insufficient data.
    """
    ma = sma(df["close"], period)
    if ma.empty or pd.isna(ma.iloc[-1]):
        return 0.0
    return float(ma.iloc[-1])


def is_above_sma(df: pd.DataFrame, period: int) -> bool:
    """Check if latest close is above the SMA of given period."""
    ma = sma(df["close"], period)
    if ma.empty or pd.isna(ma.iloc[-1]):
        return False
    return float(df["close"].iloc[-1]) > float(ma.iloc[-1])


def sma_multi_timeframe(
    df_1m: pd.DataFrame,
    df_5m: pd.DataFrame,
    df_30m: pd.DataFrame,
    period: int = SMA_200_PERIODS,
    period_30m: int = SMA_30M_PERIOD,
) -> dict[str, bool]:
    """Check if price is above SMA on all three timeframes.

    Uses `period` for 1m and 5m, and `period_30m` for 30m (default SMA50
    since SMA200 on 30-min requires too much history for penny stocks).

    Returns dict with keys: '1m', '5m', '30m', 'all_above'.
    """
    above_1m = is_above_sma(df_1m, period)
    above_5m = is_above_sma(df_5m, period)
    above_30m = is_above_sma(df_30m, period_30m)

    return {
        "1m": above_1m,
        "5m": above_5m,
        "30m": above_30m,
        "all_above": above_1m and above_5m and above_30m,
    }


def vwap(df: pd.DataFrame) -> pd.Series:
    """Intraday VWAP — resets each trading day.

    Requires df with 'close', 'high', 'low', 'volume' columns and a
    DatetimeIndex.  Uses typical price = (H+L+C)/3.
    """
    if df.empty or "volume" not in df.columns:
        return pd.Series(dtype=float)

    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    tp_vol = typical_price * df["volume"]

    # Group by date to reset each day
    if hasattr(df.index, "date"):
        dates = df.index.date
    else:
        dates = pd.to_datetime(df.index).date

    cum_tp_vol = tp_vol.groupby(dates).cumsum()
    cum_vol = df["volume"].groupby(dates).cumsum()

    result = cum_tp_vol / cum_vol.replace(0, np.nan)
    return result


def vwap_value(df: pd.DataFrame) -> float:
    """Return current (last) VWAP value. 0.0 if unavailable."""
    v = vwap(df)
    if v.empty or pd.isna(v.iloc[-1]):
        return 0.0
    return float(v.iloc[-1])


def is_above_vwap(df: pd.DataFrame) -> bool:
    """Check if latest close is above VWAP."""
    v = vwap_value(df)
    if v <= 0:
        return False
    return float(df["close"].iloc[-1]) > v


def rvol(df: pd.DataFrame, lookback_days: int = 14) -> float:
    """Relative Volume: current bar's cumulative volume vs historical average.

    Compares today's volume-so-far to the average volume at the same
    time-of-day over the past `lookback_days` trading days.

    Returns ratio (e.g. 2.5 = volume is 2.5x the average).
    Returns 0.0 if insufficient data.
    """
    if df.empty or "volume" not in df.columns or not hasattr(df.index, "date"):
        return 0.0

    today = df.index[-1].date()
    current_time = df.index[-1].time()

    # Today's cumulative volume up to now
    today_mask = df.index.date == today
    today_vol = float(df.loc[today_mask, "volume"].sum())

    # Historical: same time window on previous days
    historical_vols = []
    unique_dates = sorted(set(d for d in df.index.date if d < today))
    for day in unique_dates[-lookback_days:]:
        day_mask = (df.index.date == day) & (df.index.time <= current_time)
        day_vol = float(df.loc[day_mask, "volume"].sum())
        if day_vol > 0:
            historical_vols.append(day_vol)

    if not historical_vols:
        return 0.0

    avg_vol = sum(historical_vols) / len(historical_vols)
    if avg_vol <= 0:
        return 0.0

    return round(today_vol / avg_vol, 2)


def rvol_daily(df: pd.DataFrame, lookback_days: int = 14) -> float:
    """Daily Relative Volume: today's total volume vs average full-day volume.

    Works regardless of time-of-day (useful for pre-market when intrabar
    RVOL returns 0.0 due to no historical data at 4-9 AM).

    Compares today's cumulative volume to the average full-day volume
    over the past `lookback_days` trading days.

    Returns ratio (e.g. 2.5 = volume is 2.5x the average day).
    Returns 0.0 if insufficient data.
    """
    if df.empty or "volume" not in df.columns or not hasattr(df.index, "date"):
        return 0.0

    today = df.index[-1].date()

    # Today's cumulative volume
    today_mask = df.index.date == today
    today_vol = float(df.loc[today_mask, "volume"].sum())

    # Historical: full-day volumes for previous days
    historical_vols = []
    unique_dates = sorted(set(d for d in df.index.date if d < today))
    for day in unique_dates[-lookback_days:]:
        day_mask = df.index.date == day
        day_vol = float(df.loc[day_mask, "volume"].sum())
        if day_vol > 0:
            historical_vols.append(day_vol)

    if not historical_vols:
        return 0.0

    avg_vol = sum(historical_vols) / len(historical_vols)
    if avg_vol <= 0:
        return 0.0

    return round(today_vol / avg_vol, 2)


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range.

    TR = max(high - low, abs(high - prev_close), abs(low - prev_close))
    ATR = rolling mean of TR over `period` bars.

    Returns a Series with ATR values (NaN for insufficient data).
    """
    if df.empty or len(df) < 2:
        return pd.Series(dtype=float)

    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    return tr.rolling(window=period, min_periods=period).mean()


def atr_value(df: pd.DataFrame, period: int = 14) -> float:
    """Return the current (last) ATR value. 0.0 if unavailable."""
    a = atr(df, period)
    if a.empty or pd.isna(a.iloc[-1]):
        return 0.0
    return float(a.iloc[-1])


def ma_slope(df: pd.DataFrame, period: int, lookback: int = MA20_SLOPE_LOOKBACK) -> float:
    """Return the slope of a moving average over the last `lookback` bars.

    Slope = (MA_now - MA_N_bars_ago) / MA_N_bars_ago.
    Returns 0.0 if insufficient data.
    """
    ma = sma(df["close"], period)
    if len(ma) < lookback + 1:
        return 0.0
    ma_now = ma.iloc[-1]
    ma_prev = ma.iloc[-1 - lookback]
    if pd.isna(ma_now) or pd.isna(ma_prev) or ma_prev == 0:
        return 0.0
    return float((ma_now - ma_prev) / ma_prev)


def is_ma_rising(df: pd.DataFrame, period: int, lookback: int = MA20_SLOPE_LOOKBACK) -> bool:
    """Check if the moving average has a positive slope over the last `lookback` bars."""
    return ma_slope(df, period, lookback) > 0


def detect_vwap_reversal(df: pd.DataFrame, proximity_pct: float) -> bool:
    """Detect a VWAP reversal: price was near/below VWAP, now crossing above.

    Conditions:
    - Previous bar's close was within `proximity_pct` of VWAP or below VWAP
    - Current bar's close is above VWAP
    """
    if len(df) < 3 or "volume" not in df.columns:
        return False

    v = vwap(df)
    if v.empty or pd.isna(v.iloc[-1]) or pd.isna(v.iloc[-2]):
        return False

    vwap_now = float(v.iloc[-1])
    vwap_prev = float(v.iloc[-2])
    close_now = float(df["close"].iloc[-1])
    close_prev = float(df["close"].iloc[-2])

    if vwap_prev <= 0 or vwap_now <= 0:
        return False

    # Previous bar was near VWAP (within proximity) or below VWAP
    prev_near_or_below = close_prev <= vwap_prev * (1 + proximity_pct)
    # Current bar is above VWAP
    now_above = close_now > vwap_now

    return prev_near_or_below and now_above


def detect_fib_reversal(
    df: pd.DataFrame,
    fib_levels: list[tuple[float, float]],
    proximity_pct: float,
) -> bool:
    """Detect a bounce off a Fibonacci support level.

    Conditions:
    - Previous bar's low touched within `proximity_pct` of a fib level (from above)
    - Current bar's close is above that fib level
    - The fib level must be below current price (support, not resistance)
    """
    if len(df) < 2 or not fib_levels:
        return False

    close_now = float(df["close"].iloc[-1])
    low_prev = float(df["low"].iloc[-2])

    for _ratio, fib_price in fib_levels:
        if fib_price <= 0 or fib_price >= close_now:
            continue  # skip levels above current price (resistance)

        # Previous bar's low was near the fib level
        distance = abs(low_prev - fib_price) / fib_price
        if distance <= proximity_pct and close_now > fib_price:
            return True

    return False


def bars_to_dataframe(bars: list) -> pd.DataFrame:
    """Convert ib_insync bars to pandas DataFrame."""
    if not bars:
        return pd.DataFrame()
    data = [
        {
            "date": b.date,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
        }
        for b in bars
    ]
    df = pd.DataFrame(data)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
    return df
