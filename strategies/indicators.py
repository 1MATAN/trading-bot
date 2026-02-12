"""Pure functions: SMA200, Bollinger Bands, volume spike, VWAP."""

import numpy as np
import pandas as pd

from config.settings import (
    MA200_PERIOD,
    BOLLINGER_PERIOD,
    BOLLINGER_STD_DEV,
    VOLUME_SPIKE_MULTIPLIER,
)


def sma(series: pd.Series, period: int) -> pd.Series:
    """Simple Moving Average."""
    return series.rolling(window=period, min_periods=period).mean()


def sma200(df: pd.DataFrame) -> pd.Series:
    """200-period Simple Moving Average of close price."""
    return sma(df["close"], MA200_PERIOD)


def bollinger_bands(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Bollinger Bands (middle, upper, lower)."""
    middle = sma(df["close"], BOLLINGER_PERIOD)
    std = df["close"].rolling(window=BOLLINGER_PERIOD, min_periods=BOLLINGER_PERIOD).std()
    upper = middle + (BOLLINGER_STD_DEV * std)
    lower = middle - (BOLLINGER_STD_DEV * std)
    return middle, upper, lower


def is_above_sma200(df: pd.DataFrame) -> bool:
    """Check if latest close is above SMA200."""
    ma = sma200(df)
    if ma.empty or pd.isna(ma.iloc[-1]):
        return False
    return df["close"].iloc[-1] > ma.iloc[-1]


def is_below_upper_bollinger(df: pd.DataFrame) -> bool:
    """Check if latest close is below upper Bollinger Band (not overbought)."""
    _, upper, _ = bollinger_bands(df)
    if upper.empty or pd.isna(upper.iloc[-1]):
        return True  # can't determine, don't block
    return df["close"].iloc[-1] < upper.iloc[-1]


def volume_spike(df: pd.DataFrame, period: int = 20) -> bool:
    """Check if latest volume is a spike (>2x average)."""
    if len(df) < period + 1:
        return False
    avg_vol = df["volume"].iloc[-(period + 1):-1].mean()
    if avg_vol <= 0:
        return False
    current_vol = df["volume"].iloc[-1]
    return current_vol >= (avg_vol * VOLUME_SPIKE_MULTIPLIER)


def volume_ratio(df: pd.DataFrame, period: int = 20) -> float:
    """Current volume as multiple of average."""
    if len(df) < period + 1:
        return 0.0
    avg_vol = df["volume"].iloc[-(period + 1):-1].mean()
    if avg_vol <= 0:
        return 0.0
    return df["volume"].iloc[-1] / avg_vol


def vwap(df: pd.DataFrame) -> pd.Series:
    """Volume-Weighted Average Price (intraday)."""
    if "volume" not in df.columns:
        return pd.Series(dtype=float)
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_tp_vol = (typical_price * df["volume"]).cumsum()
    cumulative_vol = df["volume"].cumsum()
    return cumulative_tp_vol / cumulative_vol.replace(0, np.nan)


def is_above_vwap(df: pd.DataFrame) -> bool:
    """Check if latest close is above VWAP."""
    v = vwap(df)
    if v.empty or pd.isna(v.iloc[-1]):
        return False
    return df["close"].iloc[-1] > v.iloc[-1]


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
