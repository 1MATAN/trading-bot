"""Historical data downloader from yfinance with parquet caching."""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

from config.settings import BACKTEST_DATA_DIR

logger = logging.getLogger("trading_bot.data_downloader")


def download_historical_data(
    symbol: str,
    start_date: str,
    end_date: Optional[str] = None,
    interval: str = "1d",
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Download historical OHLCV data, caching to parquet.

    Args:
        symbol: Ticker symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD), defaults to today
        interval: Bar size ('1d', '1h', '5m', etc.)
        force_refresh: Bypass cache

    Returns:
        DataFrame with columns: open, high, low, close, volume
    """
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    cache_path = _cache_path(symbol, interval, start_date, end_date)

    # Check cache
    if not force_refresh and cache_path.exists():
        logger.debug(f"Loading cached data: {cache_path}")
        df = pd.read_parquet(cache_path)
        return df

    # Download from yfinance
    logger.info(f"Downloading {symbol} {interval} from {start_date} to {end_date}")
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date, end=end_date, interval=interval)
    except Exception as e:
        logger.error(f"Download failed for {symbol}: {e}")
        return pd.DataFrame()

    if df.empty:
        logger.warning(f"No data returned for {symbol}")
        return df

    # Normalize column names
    df.columns = [c.lower() for c in df.columns]
    if "stock splits" in df.columns:
        df.drop(columns=["stock splits", "dividends"], errors="ignore", inplace=True)

    # Ensure required columns
    required = ["open", "high", "low", "close", "volume"]
    for col in required:
        if col not in df.columns:
            logger.warning(f"Missing column {col} for {symbol}")
            return pd.DataFrame()

    # Cache to parquet
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path)
    logger.debug(f"Cached {len(df)} bars to {cache_path}")

    return df


def download_multiple(
    symbols: list[str],
    start_date: str,
    end_date: Optional[str] = None,
    interval: str = "1d",
) -> dict[str, pd.DataFrame]:
    """Download data for multiple symbols."""
    result = {}
    for symbol in symbols:
        df = download_historical_data(symbol, start_date, end_date, interval)
        if not df.empty:
            result[symbol] = df
    return result


def get_penny_stock_universe(
    date: str,
    min_price: float = 0.50,
    max_price: float = 5.00,
    min_volume: int = 500_000,
) -> list[str]:
    """Get a list of penny stocks matching criteria using yfinance screen.

    Note: This is a simplified approach. In production, use IBKR scanner.
    """
    # Use a static list of common penny stock symbols for backtesting
    # In production, the IBKR scanner provides this dynamically
    candidates = [
        "SNDL", "CLOV", "WISH", "SOFI", "PLTR", "BB", "NOK",
        "GSAT", "TELL", "BNGO", "SENS", "MNMD", "TLRY", "ACB",
        "FCEL", "PLUG", "IDEX", "ZOM", "NAKD", "CTRM",
    ]
    return candidates


def _cache_path(
    symbol: str, interval: str, start: str, end: str
) -> Path:
    """Generate cache file path."""
    safe_name = f"{symbol}_{interval}_{start}_{end}.parquet"
    return BACKTEST_DATA_DIR / safe_name
