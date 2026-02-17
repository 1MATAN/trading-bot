"""Pre-download backtest data cache.

Downloads all data needed by FibStrengthEngine and saves to
data/backtest_cache/ so subsequent backtest runs load instantly.

Usage:
    python main.py --download-cache
"""

import json
import logging
import os
import time as _time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET

from ib_insync import IB, Stock, util as ib_util

from config.settings import (
    BACKTEST_DATA_DIR,
    IBKR_HOST,
    IBKR_PORT,
    SCAN_PRICE_MIN,
    SCAN_PRICE_MAX,
    FIB_LOOKBACK_YEARS,
    FIB_GAP_LOOKBACK_DAYS,
)
from simulation.fib_reversal_backtest import GAP_MIN_PCT

logger = logging.getLogger("trading_bot.download_cache")

CACHE_DIR = BACKTEST_DATA_DIR
RATE_LIMIT_SLEEP = 0.5  # seconds between IBKR calls

_ib: IB | None = None
_IBKR_CACHE_CLIENT = 14


def _get_ib() -> IB | None:
    global _ib
    if _ib and _ib.isConnected():
        return _ib
    try:
        _ib = IB()
        _ib.connect(IBKR_HOST, IBKR_PORT, clientId=_IBKR_CACHE_CLIENT, timeout=10)
        logger.info("IBKR cache downloader connection established")
        return _ib
    except Exception as e:
        logger.error(f"IBKR connect failed: {e}")
        _ib = None
        return None


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase strings."""
    if df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in df.columns]
    else:
        df.columns = [c.lower() for c in df.columns]
    return df


def _price_filter(symbols: list[str]) -> list[str]:
    """Keep only symbols with last close in $1-$20 range."""
    logger.info(f"Price-filtering {len(symbols)} symbols ($1-$20)...")
    ib = _get_ib()
    if not ib:
        logger.error("No IBKR connection for price filter")
        return symbols

    passed = []
    for sym in symbols:
        try:
            contract = Stock(sym, "SMART", "USD")
            ib.qualifyContracts(contract)
            _time.sleep(RATE_LIMIT_SLEEP)
            bars = ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr="5 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
            )
            if not bars:
                continue
            df_sym = ib_util.df(bars)
            if df_sym.empty or len(df_sym) < 1:
                continue
            df_sym = _clean_columns(df_sym)
            last_price = float(df_sym["close"].iloc[-1])
            if SCAN_PRICE_MIN <= last_price <= SCAN_PRICE_MAX:
                passed.append(sym)
        except Exception:
            continue

    logger.info(f"Price filter: {len(passed)} of {len(symbols)} passed")
    return passed


def _detect_gappers(symbols: list[str]) -> tuple[list[dict], pd.DataFrame]:
    """Download daily data per symbol and detect gap events >= 10% and >= 20%.

    Returns (gapper_list, combined_df).
    """
    logger.info(f"Downloading {FIB_GAP_LOOKBACK_DAYS}d daily for {len(symbols)} symbols (gapper detection)...")
    ib = _get_ib()
    if not ib:
        logger.error("No IBKR connection for gapper detection")
        return [], pd.DataFrame()

    gappers = []
    all_frames = []

    for sym in symbols:
        try:
            contract = Stock(sym, "SMART", "USD")
            ib.qualifyContracts(contract)
            _time.sleep(RATE_LIMIT_SLEEP)
            bars = ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=f"{FIB_GAP_LOOKBACK_DAYS} D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
            )
            if not bars or len(bars) < 2:
                continue
            df_sym = ib_util.df(bars)
            df_sym = _clean_columns(df_sym)
            all_frames.append(df_sym)

            prev_closes = df_sym["close"].shift(1)
            for idx in range(1, len(df_sym)):
                row = df_sym.iloc[idx]
                prev_close = float(prev_closes.iloc[idx])
                if pd.isna(prev_close) or prev_close <= 0:
                    continue
                if prev_close < SCAN_PRICE_MIN or prev_close > SCAN_PRICE_MAX:
                    continue
                open_price = float(row["open"])
                high_price = float(row["high"])
                effective_gap = max(
                    (open_price - prev_close) / prev_close * 100,
                    (high_price - prev_close) / prev_close * 100,
                )
                if effective_gap >= 10.0:
                    gap_vol = float(row["volume"]) if not pd.isna(row.get("volume", float("nan"))) else 0
                    date = df_sym.index[idx]
                    gappers.append({
                        "symbol": sym,
                        "date": date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date),
                        "gap_pct": round(effective_gap, 1),
                        "prev_close": round(prev_close, 4),
                        "open_price": round(open_price, 4),
                        "high_price": round(high_price, 4),
                        "gap_volume": gap_vol,
                    })
        except Exception as e:
            logger.debug(f"Gapper scan failed for {sym}: {e}")

    combined = pd.concat(all_frames) if all_frames else pd.DataFrame()
    return gappers, combined


def _save_gappers_csv(gappers: list[dict]) -> None:
    """Save gapper CSVs at 10% and 20% thresholds."""
    if not gappers:
        return

    df = pd.DataFrame(gappers)

    # >= 10%
    csv_10 = CACHE_DIR / "gappers_10pct.csv"
    df.to_csv(csv_10, index=False)
    logger.info(f"Saved {len(df)} gap events (>=10%) to {csv_10}")

    # >= 20%
    df_20 = df[df["gap_pct"] >= 20.0]
    csv_20 = CACHE_DIR / "gappers_20pct.csv"
    df_20.to_csv(csv_20, index=False)
    logger.info(f"Saved {len(df_20)} gap events (>=20%) to {csv_20}")


def _download_daily_5y(symbol: str) -> bool:
    """Download 5y daily OHLCV and save as parquet. Returns True on success."""
    out_path = CACHE_DIR / f"{symbol}_daily_5y.parquet"
    ib = _get_ib()
    if not ib:
        logger.error(f"  {symbol}: no IBKR connection for daily 5y")
        return False
    try:
        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=f"{FIB_LOOKBACK_YEARS} Y",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=False,
        )
        if not bars or len(bars) < 20:
            logger.warning(f"  {symbol}: daily 5y — insufficient data ({len(bars) if bars else 0} bars)")
            return False
        df = ib_util.df(bars)
        df = _clean_columns(df)
        df.to_parquet(out_path)
        logger.info(f"  {symbol}: {len(df)} daily bars → {out_path.name}")
        return True
    except Exception as e:
        logger.error(f"  {symbol}: daily 5y download failed: {e}")
        return False


def _download_intraday_2m(symbol: str) -> bool:
    """Download 30d 2m intraday OHLCV and save as parquet. Returns True on success."""
    out_path = CACHE_DIR / f"{symbol}_intraday_2m.parquet"
    ib = _get_ib()
    if not ib:
        logger.error(f"  {symbol}: no IBKR connection for intraday 2m")
        return False
    try:
        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="30 D",
            barSizeSetting="2 mins",
            whatToShow="TRADES",
            useRTH=False,
        )
        if not bars or len(bars) < 50:
            logger.warning(f"  {symbol}: intraday 2m — insufficient data ({len(bars) if bars else 0} bars)")
            return False
        df = ib_util.df(bars)
        df.columns = [c.lower() for c in df.columns]
        for col in ["dividends", "stock splits", "capital gains"]:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
        df.to_parquet(out_path)
        logger.info(f"  {symbol}: {len(df)} 2m bars → {out_path.name}")
        return True
    except Exception as e:
        logger.error(f"  {symbol}: intraday 2m download failed: {e}")
        return False


def _fetch_float_shares(symbols: list[str]) -> dict[str, float]:
    """Fetch float shares for all symbols via IBKR fundamental data and save to JSON."""
    ib = _get_ib()
    float_cache: dict[str, float] = {}
    for sym in symbols:
        try:
            if not ib:
                float_cache[sym] = 0
                continue
            contract = Stock(sym, "SMART", "USD")
            ib.qualifyContracts(contract)
            xml_data = ib.reqFundamentalData(contract, reportType="ReportSnapshot")
            flt = 0.0
            if xml_data:
                root = ET.fromstring(xml_data)
                for item in root.iter("SharesOut"):
                    val = item.text or item.get("TotalFloat", "0")
                    flt = float(val) * 1_000_000  # IBKR reports in millions
                    break
            float_cache[sym] = flt
        except Exception:
            float_cache[sym] = 0
        _time.sleep(RATE_LIMIT_SLEEP)

    out_path = CACHE_DIR / "float_cache.json"
    out_path.write_text(json.dumps(float_cache, indent=2))
    non_zero = sum(1 for v in float_cache.values() if v > 0)
    logger.info(f"Float cache: {non_zero}/{len(float_cache)} symbols with data → {out_path.name}")
    return float_cache


def _save_cache_meta(symbols: list[str]) -> None:
    """Write cache metadata."""
    meta = {
        "downloaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbols": symbols,
        "fib_lookback_years": FIB_LOOKBACK_YEARS,
        "gap_lookback_days": FIB_GAP_LOOKBACK_DAYS,
    }
    meta_path = CACHE_DIR / "cache_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    logger.info(f"Cache metadata → {meta_path.name}")


def _print_summary(symbols: list[str], daily_ok: int, intra_ok: int, gapper_count: int) -> None:
    """Print download summary with disk usage."""
    # Disk usage
    total_bytes = sum(f.stat().st_size for f in CACHE_DIR.iterdir() if f.is_file())
    total_mb = total_bytes / (1024 * 1024)

    parquet_count = len(list(CACHE_DIR.glob("*.parquet")))

    print(f"\n{'='*60}")
    print(f"Backtest Data Cache — Download Complete")
    print(f"{'='*60}")
    print(f"  Symbols filtered:     {len(symbols)}")
    print(f"  Daily 5y downloaded:  {daily_ok}")
    print(f"  Intraday 2m downloaded: {intra_ok}")
    print(f"  Gap events (>=10%):   {gapper_count}")
    print(f"  Parquet files:        {parquet_count}")
    print(f"  Total disk usage:     {total_mb:.1f} MB")
    print(f"  Cache directory:      {CACHE_DIR}")
    print(f"\nRun backtest with: python main.py --fib-strength")


def download_cache() -> None:
    """Main entry point — download all backtest data to cache."""
    logger.info("=" * 60)
    logger.info("Backtest Data Cache Download Starting")
    UNIVERSE = [
        "BZAI", "VIVS", "ANL", "LRHC", "MGLD", "RPGL", "VTIX", "SER",
        "XPON", "GOAI", "VENU", "BHAT", "DXF", "RUBI", "SOC", "VHUB",
        "CISS", "EVTV", "YJ", "CCHH", "ELAB", "MNTN", "STIM", "PRCH",
        "FSLY", "GDTC", "DHX", "CCTG", "MBOT", "ABP", "LXEH", "JZXN",
        "QNST", "LIMN", "CYCN", "AEVA", "PRFX", "ACCL", "VERO", "JAGX",
        "IBRX", "IMG", "ACRV", "ECDA", "DVLT", "SIDU", "KZIA", "AIMD",
        "KUST", "BKYI", "GCTS", "FATBB", "ISPC", "BARK", "LHSW",
    ]
    logger.info(f"Universe: {len(UNIVERSE)} symbols")
    logger.info(f"Cache dir: {CACHE_DIR}")
    logger.info("=" * 60)

    # 1. Price filter
    symbols = _price_filter(UNIVERSE)
    if not symbols:
        logger.error("No symbols passed price filter")
        return

    # 2. Detect gappers (batch download)
    gappers, _ = _detect_gappers(symbols)
    _save_gappers_csv(gappers)

    # 3. Per-symbol downloads
    daily_ok = 0
    intra_ok = 0
    total = len(symbols)

    for i, sym in enumerate(symbols, 1):
        logger.info(f"[{i}/{total}] {sym}")

        if _download_daily_5y(sym):
            daily_ok += 1
        _time.sleep(RATE_LIMIT_SLEEP)

        if _download_intraday_2m(sym):
            intra_ok += 1
        _time.sleep(RATE_LIMIT_SLEEP)

    # 4. Float shares
    logger.info(f"Fetching float shares for {len(symbols)} symbols...")
    _fetch_float_shares(symbols)

    # 5. Metadata
    _save_cache_meta(symbols)

    # 6. Summary
    _print_summary(symbols, daily_ok, intra_ok, len(gappers))
