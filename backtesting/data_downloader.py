"""Historical data downloader from IBKR with parquet caching."""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from ib_insync import IB, Stock, util as ib_util

from config.settings import BACKTEST_DATA_DIR, IBKR_HOST, IBKR_PORT

logger = logging.getLogger("trading_bot.data_downloader")

# Dedicated IBKR connection for data downloads
_ib: IB | None = None
_IBKR_CLIENT_ID = 11


def _get_ib() -> IB | None:
    """Get/create a dedicated IBKR connection for data downloads."""
    global _ib
    if _ib and _ib.isConnected():
        return _ib
    try:
        _ib = IB()
        _ib.connect(IBKR_HOST, IBKR_PORT, clientId=_IBKR_CLIENT_ID, timeout=10)
        logger.info("IBKR data downloader connection established")
        return _ib
    except Exception as e:
        logger.error(f"IBKR connect failed: {e}")
        _ib = None
        return None


# Map common interval strings to IBKR bar size + duration
_INTERVAL_MAP = {
    "1d": ("1 day", "5 Y"),
    "1h": ("1 hour", "30 D"),
    "5m": ("5 mins", "5 D"),
    "2m": ("2 mins", "2 D"),
    "1m": ("1 min", "1 D"),
}


def download_historical_data(
    symbol: str,
    start_date: str,
    end_date: Optional[str] = None,
    interval: str = "1d",
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Download historical OHLCV data from IBKR, caching to parquet.

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

    # Download from IBKR
    logger.info(f"Downloading {symbol} {interval} from {start_date} to {end_date}")
    ib = _get_ib()
    if not ib:
        logger.error(f"No IBKR connection for {symbol}")
        return pd.DataFrame()

    try:
        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)

        bar_size, default_duration = _INTERVAL_MAP.get(interval, ("1 day", "5 Y"))

        # Calculate duration from start_date to end_date
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        days = (end_dt - start_dt).days
        if days <= 0:
            days = 1

        # Choose appropriate duration string
        if interval == "1d":
            if days > 365:
                duration = f"{min(days // 365 + 1, 5)} Y"
            else:
                duration = f"{days} D"
        else:
            duration = f"{min(days, 30)} D"

        time.sleep(0.5)  # rate limit courtesy
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_date.replace("-", "") + " 23:59:59",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=False,
        )
    except Exception as e:
        logger.error(f"Download failed for {symbol}: {e}")
        return pd.DataFrame()

    if not bars:
        logger.warning(f"No data returned for {symbol}")
        return pd.DataFrame()

    df = ib_util.df(bars)

    # Normalize column names
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Ensure required columns
    required = ["open", "high", "low", "close", "volume"]
    for col in required:
        if col not in df.columns:
            logger.warning(f"Missing column {col} for {symbol}")
            return pd.DataFrame()

    # Keep only required columns + date
    keep_cols = [c for c in df.columns if c in required or c == "date"]
    df = df[keep_cols]

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
    """Get a list of penny stocks matching criteria.

    Note: This is a simplified approach. In production, use IBKR scanner.
    """
    candidates = [
        "AMC", "SNAP", "HOOD", "MARA", "RIOT", "CLSK", "SOS",
        "HIVE", "BITF", "JOBY", "SKLZ", "BARK", "CLOV", "BIRD",
        "TTOO", "ASTS", "APGE", "IBRX", "WULF", "BTBT",
    ]
    return candidates


def _cache_path(
    symbol: str, interval: str, start: str, end: str
) -> Path:
    """Generate cache file path."""
    safe_name = f"{symbol}_{interval}_{start}_{end}.parquet"
    return BACKTEST_DATA_DIR / safe_name
