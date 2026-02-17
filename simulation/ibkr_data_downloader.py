"""Download 15-second bars from IBKR for gap event days.

For each gap event in data/gappers.csv:
  - Build Stock contract, qualify it
  - Download 15-sec bars for the full day (incl. extended hours)
  - Split into 2 requests (IBKR max 2000 bars per request)
  - Cache to data/backtest_cache/{SYMBOL}_15s_{DATE}.parquet
  - Rate limit: sleep between requests to respect IBKR pacing
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from ib_insync import Stock

from config.settings import BACKTEST_DATA_DIR, DATA_DIR, IBKR_HOST, IBKR_PORT

logger = logging.getLogger("trading_bot.ibkr_downloader")

GAPPERS_CSV = DATA_DIR / "gappers.csv"
SLEEP_BETWEEN_REQUESTS = 11  # seconds — stay under 60 req / 10 min pacing


def _cache_path(symbol: str, date_str: str) -> Path:
    """Return cache path for 15-sec data: {SYMBOL}_15s_{YYYY-MM-DD}.parquet."""
    return BACKTEST_DATA_DIR / f"{symbol}_15s_{date_str}.parquet"


def _bars_to_df(bars) -> pd.DataFrame:
    """Convert ib_insync BarData list to pandas DataFrame."""
    if not bars:
        return pd.DataFrame()
    records = []
    for b in bars:
        records.append({
            "date": b.date,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
            "barCount": b.barCount,
            "average": b.average,
        })
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df.sort_index(inplace=True)
    return df


async def _download_one_event(
    ib, contract, symbol: str, date_str: str,
) -> bool:
    """Download 15-sec bars for one gap day. Returns True if successful."""
    cache = _cache_path(symbol, date_str)
    if cache.exists():
        try:
            df = pd.read_parquet(cache)
            if len(df) >= 100:
                logger.debug(f"  {symbol} {date_str}: cached ({len(df)} bars)")
                return True
        except Exception:
            pass

    logger.info(f"  Downloading {symbol} {date_str} (15-sec)...")

    # IBKR requires yyyymmdd format (no dashes)
    ibkr_date = date_str.replace("-", "")

    # Request 1: morning half — 04:00 to ~12:00 ET
    # endDateTime at noon ET
    end1 = f"{ibkr_date} 12:00:00 US/Eastern"
    logger.info(f"  Morning request: endDateTime={end1!r}")
    try:
        bars1 = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime=end1,
            durationStr="28800 S",  # 8 hours = 1920 bars at 15sec
            barSizeSetting="15 secs",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=2,
        )
    except Exception as e:
        logger.warning(f"  {symbol} {date_str} morning request failed: {e}")
        bars1 = []

    await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)

    # Request 2: afternoon half — ~12:00 to 20:00 ET
    end2 = f"{ibkr_date} 20:00:00 US/Eastern"
    try:
        bars2 = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime=end2,
            durationStr="28800 S",  # 8 hours
            barSizeSetting="15 secs",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=2,
        )
    except Exception as e:
        logger.warning(f"  {symbol} {date_str} afternoon request failed: {e}")
        bars2 = []

    await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)

    # Merge and deduplicate
    df1 = _bars_to_df(bars1)
    df2 = _bars_to_df(bars2)

    if df1.empty and df2.empty:
        logger.warning(f"  {symbol} {date_str}: no data returned")
        return False

    df = pd.concat([df1, df2])
    df = df[~df.index.duplicated(keep="first")]
    df.sort_index(inplace=True)

    # Save to parquet
    df.to_parquet(cache)
    logger.info(f"  {symbol} {date_str}: saved {len(df)} bars -> {cache.name}")
    return True


async def download_15s_data() -> dict[str, int]:
    """Download 15-sec IBKR bars for all gap events in gappers.csv.

    Returns dict mapping 'symbol_date' -> bar_count for successful downloads.
    """
    if not GAPPERS_CSV.exists():
        logger.error(f"Gappers CSV not found: {GAPPERS_CSV}")
        return {}

    gappers = pd.read_csv(GAPPERS_CSV)
    logger.info(f"Loaded {len(gappers)} gap events from {GAPPERS_CSV}")

    # Count how many already cached
    cached = 0
    to_download = []
    for _, row in gappers.iterrows():
        symbol = row["symbol"]
        date_str = str(row["date"])
        cache = _cache_path(symbol, date_str)
        if cache.exists():
            try:
                df = pd.read_parquet(cache)
                if len(df) >= 100:
                    cached += 1
                    continue
            except Exception:
                pass
        to_download.append((symbol, date_str))

    logger.info(f"  Already cached: {cached}, need to download: {len(to_download)}")

    if not to_download:
        logger.info("All gap events already cached!")
        return {}

    # Connect to IBKR with a dedicated client ID (avoid conflict with main bot)
    from ib_insync import IB
    ib = IB()
    try:
        await ib.connectAsync(
            host=IBKR_HOST, port=IBKR_PORT,
            clientId=20,  # dedicated ID for data downloader
            timeout=30,
        )
    except Exception as e:
        logger.error(f"Cannot connect to IBKR: {e}")
        return {}

    # Qualify contracts (batch by unique symbols)
    unique_symbols = sorted(set(sym for sym, _ in to_download))
    contracts: dict[str, Stock] = {}

    for sym in unique_symbols:
        contract = Stock(sym, "SMART", "USD")
        try:
            qualified = await ib.qualifyContractsAsync(contract)
            if qualified:
                contracts[sym] = qualified[0]
                logger.debug(f"  Qualified: {sym}")
            else:
                logger.warning(f"  Cannot qualify: {sym}")
        except Exception as e:
            logger.warning(f"  Qualify failed for {sym}: {e}")
        await asyncio.sleep(0.5)

    logger.info(f"Qualified {len(contracts)}/{len(unique_symbols)} contracts")

    # Download each event
    results: dict[str, int] = {}
    total = len(to_download)
    for i, (symbol, date_str) in enumerate(to_download, 1):
        if symbol not in contracts:
            logger.debug(f"  Skipping {symbol} (not qualified)")
            continue

        # Reconnect if disconnected
        if not ib.isConnected():
            logger.info("  Reconnecting to IBKR...")
            try:
                await ib.connectAsync(
                    host=IBKR_HOST, port=IBKR_PORT,
                    clientId=10, timeout=30,
                )
            except Exception as e:
                logger.error(f"  Reconnect failed: {e}")
                await asyncio.sleep(5)
                continue

        logger.info(f"[{i}/{total}] {symbol} {date_str}")
        try:
            success = await _download_one_event(ib, contracts[symbol], symbol, date_str)
        except Exception as e:
            logger.warning(f"  {symbol} {date_str} download error: {e}")
            await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)
            continue

        if success:
            cache = _cache_path(symbol, date_str)
            try:
                df = pd.read_parquet(cache)
                results[f"{symbol}_{date_str}"] = len(df)
            except Exception:
                pass

    # Disconnect
    try:
        ib.disconnect()
    except Exception:
        pass

    logger.info(
        f"Download complete: {len(results)} events downloaded, "
        f"{cached} were already cached"
    )
    return results
