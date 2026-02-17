"""Build stock universe from IBKR scanner and download backtest data.

Pipeline:
  1. IBKR Scanner → ~300 unique symbols (broad universe)
  2. Download 3 weeks daily bars (useRTH=False) → compute 20%+ movers
  3. Float filter via reqFundamentalData → ~20-50 symbols
  4. Download 5Y daily + 15-sec bars for filtered symbols
  5. Build gappers.csv → ready for fib double-touch backtest

Usage:
    python main.py --build-universe
"""

import json
import logging
import time as _time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pandas as pd
from ib_insync import IB, Stock, ScannerSubscription, util as ib_util

from config.settings import (
    BACKTEST_DATA_DIR,
    DATA_DIR,
    IBKR_HOST,
    IBKR_PORT,
    SCAN_PRICE_MIN,
    SCAN_PRICE_MAX,
    SCAN_FLOAT_MAX,
)

logger = logging.getLogger("trading_bot.build_universe")

CACHE_DIR = BACKTEST_DATA_DIR
RATE_LIMIT_SLEEP = 0.5
SLEEP_BETWEEN_15S = 11  # seconds between 15-sec bar requests (IBKR pacing)
_UNIVERSE_CLIENT_ID = 25

# Scanner scan types for broad discovery
SCAN_TYPES = [
    "TOP_PERC_GAIN",
    "TOP_PERC_LOSE",
    "MOST_ACTIVE",
    "HOT_BY_VOLUME",
    "HIGH_VS_13W_HL",
    "LOW_VS_13W_HL",
]

# Price buckets for more diverse results
PRICE_BUCKETS = [
    (SCAN_PRICE_MIN, 5.0),
    (5.0, SCAN_PRICE_MAX),
]

MOVER_THRESHOLD_PCT = 20.0  # minimum move to qualify


def _connect() -> IB | None:
    """Connect to IBKR TWS with dedicated client ID."""
    ib = IB()
    try:
        ib.connect(IBKR_HOST, IBKR_PORT, clientId=_UNIVERSE_CLIENT_ID, timeout=15)
        logger.info("IBKR connection established (build_universe)")
        return ib
    except Exception as e:
        logger.error(f"IBKR connect failed: {e}")
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


# ── Step 1: Broad universe via IBKR scanner ─────────────────────────

def _run_scanner(ib: IB) -> set[str]:
    """Run multiple IBKR scanner queries and return unique symbols."""
    all_symbols: set[str] = set()

    for scan_code in SCAN_TYPES:
        for price_low, price_high in PRICE_BUCKETS:
            sub = ScannerSubscription(
                instrument="STK",
                locationCode="STK.US.MAJOR",
                scanCode=scan_code,
                numberOfRows=50,
                abovePrice=price_low,
                belowPrice=price_high,
            )
            try:
                results = ib.reqScannerData(sub)
                symbols_found = []
                for item in results:
                    sym = item.contractDetails.contract.symbol
                    if sym and sym.isalpha() and len(sym) <= 5:
                        all_symbols.add(sym)
                        symbols_found.append(sym)
                logger.info(
                    f"  Scanner {scan_code} ${price_low:.0f}-${price_high:.0f}: "
                    f"{len(symbols_found)} results"
                )
            except Exception as e:
                logger.warning(f"  Scanner {scan_code} ${price_low:.0f}-${price_high:.0f} failed: {e}")
            _time.sleep(1)  # rate limit between scans

    logger.info(f"Scanner total: {len(all_symbols)} unique symbols")
    return all_symbols


# ── Step 2: Download 3 weeks daily data ──────────────────────────────

def _download_daily_3w(ib: IB, symbols: set[str]) -> dict[str, pd.DataFrame]:
    """Download 3 weeks of daily bars for all symbols. Returns {symbol: df}."""
    logger.info(f"Downloading 21-day daily bars for {len(symbols)} symbols...")
    data: dict[str, pd.DataFrame] = {}

    for i, sym in enumerate(sorted(symbols), 1):
        try:
            contract = Stock(sym, "SMART", "USD")
            ib.qualifyContracts(contract)
            _time.sleep(RATE_LIMIT_SLEEP)
            bars = ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr="21 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=False,
            )
            if not bars or len(bars) < 2:
                continue
            df = ib_util.df(bars)
            df = _clean_columns(df)
            data[sym] = df
            if i % 50 == 0:
                logger.info(f"  Daily progress: {i}/{len(symbols)} ({len(data)} with data)")
        except Exception as e:
            logger.debug(f"  {sym}: daily download failed: {e}")

    logger.info(f"Daily data: {len(data)}/{len(symbols)} symbols downloaded")
    return data


# ── Step 3: Compute 20%+ movers ─────────────────────────────────────

def _find_movers(
    daily_data: dict[str, pd.DataFrame],
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict]:
    """Find 20%+ movers from daily data.

    If start_date/end_date given, only look at those days.
    Otherwise look at all available days.
    """
    movers: list[dict] = []

    for sym, df in daily_data.items():
        if df.empty or len(df) < 2:
            continue

        # Ensure date index
        if "date" in df.columns:
            df = df.set_index("date")
        df.index = pd.to_datetime(df.index)

        # Filter date range if specified
        if start_date:
            df = df[df.index >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df.index <= pd.Timestamp(end_date)]

        if len(df) < 2:
            continue

        prev_closes = df["close"].shift(1)

        for idx in range(1, len(df)):
            row = df.iloc[idx]
            prev_close = float(prev_closes.iloc[idx])
            if pd.isna(prev_close) or prev_close <= 0:
                continue

            open_price = float(row["open"])
            high_price = float(row["high"])
            low_price = float(row["low"])
            close_price = float(row["close"])

            # Method 1: gap from previous close
            gap_pct = (open_price - prev_close) / prev_close * 100

            # Method 2: intraday range
            range_pct = (high_price - low_price) / low_price * 100 if low_price > 0 else 0

            # Method 3: change from prev close
            change_pct = abs(close_price - prev_close) / prev_close * 100

            move_pct = max(gap_pct, range_pct, change_pct)

            if move_pct >= MOVER_THRESHOLD_PCT:
                date = df.index[idx]
                gap_vol = float(row.get("volume", 0)) if not pd.isna(row.get("volume", 0)) else 0
                movers.append({
                    "symbol": sym,
                    "date": date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date),
                    "gap_pct": round(max(gap_pct, 0), 1),  # store positive gap for backtest
                    "move_pct": round(move_pct, 1),
                    "prev_close": round(prev_close, 4),
                    "open_price": round(open_price, 4),
                    "high_price": round(high_price, 4),
                    "gap_volume": gap_vol,
                })

    logger.info(f"Movers: {len(movers)} events with {MOVER_THRESHOLD_PCT}%+ move across {len(set(m['symbol'] for m in movers))} symbols")
    return movers


# ── Step 4: Float filter ─────────────────────────────────────────────

def _filter_by_float(ib: IB, symbols: list[str]) -> tuple[list[str], dict[str, float]]:
    """Filter symbols by float shares. Returns (passed_symbols, float_cache)."""
    logger.info(f"Checking float for {len(symbols)} symbols...")
    float_cache: dict[str, float] = {}
    passed: list[str] = []

    for sym in symbols:
        try:
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

            # Pass if float data missing (0 = unknown, let it through) or within limit
            if flt == 0 or flt <= SCAN_FLOAT_MAX:
                passed.append(sym)
            else:
                logger.debug(f"  {sym}: float {flt/1e6:.1f}M > {SCAN_FLOAT_MAX/1e6:.0f}M — skipped")
        except Exception:
            float_cache[sym] = 0
            passed.append(sym)  # unknown float → let through
        _time.sleep(RATE_LIMIT_SLEEP)

    # Save float cache
    out_path = CACHE_DIR / "float_cache.json"
    out_path.write_text(json.dumps(float_cache, indent=2))

    logger.info(f"Float filter: {len(passed)}/{len(symbols)} passed (float <= {SCAN_FLOAT_MAX/1e6:.0f}M)")
    return passed, float_cache


# ── Step 5: Download backtest data (5Y daily + 15-sec) ───────────────

def _download_daily_5y(ib: IB, symbol: str) -> bool:
    """Download 5Y daily OHLCV and save as parquet. Returns True on success."""
    out_path = CACHE_DIR / f"{symbol}_daily_5y.parquet"
    if out_path.exists():
        try:
            df = pd.read_parquet(out_path)
            if len(df) >= 20:
                logger.debug(f"  {symbol}: 5Y daily already cached ({len(df)} bars)")
                return True
        except Exception:
            pass

    try:
        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="5 Y",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=False,
        )
        if not bars or len(bars) < 20:
            logger.warning(f"  {symbol}: daily 5Y — insufficient data ({len(bars) if bars else 0} bars)")
            return False
        df = ib_util.df(bars)
        df = _clean_columns(df)
        df.to_parquet(out_path)
        logger.info(f"  {symbol}: {len(df)} daily bars -> {out_path.name}")
        return True
    except Exception as e:
        logger.error(f"  {symbol}: daily 5Y download failed: {e}")
        return False


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


def _download_15s_for_event(ib: IB, symbol: str, date_str: str) -> bool:
    """Download 15-sec bars for one gap day (morning + afternoon). Returns True on success."""
    cache_path = CACHE_DIR / f"{symbol}_15s_{date_str}.parquet"
    if cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            if len(df) >= 100:
                logger.debug(f"  {symbol} {date_str}: cached ({len(df)} bars)")
                return True
        except Exception:
            pass

    logger.info(f"  Downloading {symbol} {date_str} (15-sec)...")
    ibkr_date = date_str.replace("-", "")

    # Request 1: morning half (04:00 - 12:00 ET)
    end1 = f"{ibkr_date} 12:00:00 US/Eastern"
    try:
        bars1 = ib.reqHistoricalData(
            Stock(symbol, "SMART", "USD"),
            endDateTime=end1,
            durationStr="28800 S",  # 8 hours = 1920 bars at 15sec
            barSizeSetting="15 secs",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=2,
        )
    except Exception as e:
        logger.warning(f"  {symbol} {date_str} morning failed: {e}")
        bars1 = []

    _time.sleep(SLEEP_BETWEEN_15S)

    # Request 2: afternoon half (12:00 - 20:00 ET)
    end2 = f"{ibkr_date} 20:00:00 US/Eastern"
    try:
        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)
        bars2 = ib.reqHistoricalData(
            contract,
            endDateTime=end2,
            durationStr="28800 S",
            barSizeSetting="15 secs",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=2,
        )
    except Exception as e:
        logger.warning(f"  {symbol} {date_str} afternoon failed: {e}")
        bars2 = []

    _time.sleep(SLEEP_BETWEEN_15S)

    # Merge and deduplicate
    df1 = _bars_to_df(bars1)
    df2 = _bars_to_df(bars2)

    if df1.empty and df2.empty:
        logger.warning(f"  {symbol} {date_str}: no data returned")
        return False

    df = pd.concat([df1, df2])
    df = df[~df.index.duplicated(keep="first")]
    df.sort_index(inplace=True)

    df.to_parquet(cache_path)
    logger.info(f"  {symbol} {date_str}: saved {len(df)} bars -> {cache_path.name}")
    return True


# ── Step 6: Save gappers.csv ────────────────────────────────────────

def _save_gappers_csv(movers: list[dict], float_cache: dict[str, float]) -> Path:
    """Save gappers.csv with float data for the backtest engine."""
    if not movers:
        logger.warning("No movers to save")
        return DATA_DIR / "gappers.csv"

    df = pd.DataFrame(movers)

    # Add float_shares column
    df["float_shares"] = df["symbol"].map(lambda s: float_cache.get(s, 0))

    # Use gap_pct as the main column (backtest expects this)
    # For events where gap_pct is low but move_pct is high, use move_pct as gap_pct
    df["gap_pct"] = df.apply(
        lambda row: row["gap_pct"] if row["gap_pct"] >= 10.0 else row["move_pct"],
        axis=1,
    )

    # Select columns in the format expected by fib_double_touch_backtest
    out_cols = ["symbol", "date", "gap_pct", "prev_close", "open_price", "high_price", "gap_volume", "float_shares"]
    for col in out_cols:
        if col not in df.columns:
            df[col] = 0

    out_path = DATA_DIR / "gappers.csv"
    df[out_cols].to_csv(out_path, index=False)
    logger.info(f"Saved {len(df)} gap events to {out_path}")

    # Also save 20%+ only version
    df_20 = df[df["gap_pct"] >= 20.0]
    csv_20 = CACHE_DIR / "gappers_20pct.csv"
    df_20[out_cols].to_csv(csv_20, index=False)
    logger.info(f"Saved {len(df_20)} events (>=20%) to {csv_20}")

    return out_path


# ── Main entry point ─────────────────────────────────────────────────

def build_universe(
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Main pipeline: scanner → daily → movers → float → download → gappers.csv.

    Args:
        start_date: Filter movers from this date (YYYY-MM-DD). Default: all available.
        end_date: Filter movers to this date (YYYY-MM-DD). Default: all available.
    """
    print("=" * 60)
    print("Build Stock Universe — IBKR Scanner Pipeline")
    print("=" * 60)

    # Connect
    ib = _connect()
    if not ib:
        print("ERROR: Cannot connect to IBKR TWS. Is it running on port 7497?")
        return

    try:
        # Step 1: Scanner
        print("\n[Step 1/6] Running IBKR scanner...")
        universe = _run_scanner(ib)
        if not universe:
            print("ERROR: Scanner returned no results")
            return
        print(f"  Found {len(universe)} unique symbols")

        # Step 2: Download 3 weeks daily
        print(f"\n[Step 2/6] Downloading daily bars for {len(universe)} symbols...")
        daily_data = _download_daily_3w(ib, universe)
        print(f"  Downloaded {len(daily_data)} symbols with data")

        # Step 3: Find 20%+ movers
        date_range_str = ""
        if start_date:
            date_range_str += f" from {start_date}"
        if end_date:
            date_range_str += f" to {end_date}"
        print(f"\n[Step 3/6] Finding {MOVER_THRESHOLD_PCT}%+ movers{date_range_str}...")
        movers = _find_movers(daily_data, start_date, end_date)
        if not movers:
            print("  No movers found! Try adjusting date range or threshold.")
            return
        unique_mover_symbols = sorted(set(m["symbol"] for m in movers))
        print(f"  Found {len(movers)} events across {len(unique_mover_symbols)} symbols")

        # Step 4: Float filter
        print(f"\n[Step 4/6] Checking float for {len(unique_mover_symbols)} symbols...")
        passed_symbols, float_cache = _filter_by_float(ib, unique_mover_symbols)
        # Filter movers to only passed symbols
        movers = [m for m in movers if m["symbol"] in passed_symbols]
        print(f"  {len(passed_symbols)} symbols passed, {len(movers)} events remaining")

        if not movers:
            print("  No events remaining after float filter!")
            return

        # Step 5a: Download 5Y daily for filtered symbols
        filtered_symbols = sorted(set(m["symbol"] for m in movers))
        print(f"\n[Step 5/6] Downloading backtest data for {len(filtered_symbols)} symbols...")
        daily_5y_ok = 0
        for i, sym in enumerate(filtered_symbols, 1):
            if _download_daily_5y(ib, sym):
                daily_5y_ok += 1
            _time.sleep(RATE_LIMIT_SLEEP)
            if i % 10 == 0:
                print(f"  5Y daily: {i}/{len(filtered_symbols)}")
        print(f"  5Y daily: {daily_5y_ok}/{len(filtered_symbols)} downloaded")

        # Step 5b: Download 15-sec bars for each event
        print(f"\n  Downloading 15-sec bars for {len(movers)} events...")
        sec_15_ok = 0
        for i, m in enumerate(movers, 1):
            sym = m["symbol"]
            date_str = m["date"]
            # Qualify contract once
            try:
                contract = Stock(sym, "SMART", "USD")
                ib.qualifyContracts(contract)
            except Exception:
                continue
            if _download_15s_for_event(ib, sym, date_str):
                sec_15_ok += 1
            if i % 10 == 0:
                print(f"  15-sec bars: {i}/{len(movers)}")
        print(f"  15-sec bars: {sec_15_ok}/{len(movers)} downloaded")

        # Step 6: Save gappers.csv
        print(f"\n[Step 6/6] Saving gappers.csv...")
        csv_path = _save_gappers_csv(movers, float_cache)

        # Summary
        print(f"\n{'='*60}")
        print(f"Build Universe — Complete")
        print(f"{'='*60}")
        print(f"  IBKR scanner:       {len(SCAN_TYPES) * len(PRICE_BUCKETS)} scans -> {len(universe)} unique symbols")
        print(f"  Daily data:         {len(daily_data)} symbols (3 weeks each)")
        print(f"  {MOVER_THRESHOLD_PCT}%+ movers:     {len(movers)} events across {len(filtered_symbols)} symbols")
        print(f"  Float filter:       {len(passed_symbols)} symbols passed (<= {SCAN_FLOAT_MAX/1e6:.0f}M)")
        print(f"  5Y daily:           {daily_5y_ok} symbols")
        print(f"  15-sec bars:        {sec_15_ok} events")
        print(f"  Gappers CSV:        {csv_path}")
        print(f"\nRun backtest with: python main.py --fib-double-touch")

    finally:
        try:
            ib.disconnect()
        except Exception:
            pass
        logger.info("IBKR disconnected (build_universe)")
