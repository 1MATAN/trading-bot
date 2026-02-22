"""Run Gap and Go backtest on 19 specific penny stock gappers.

Steps:
  1. Connect to IBKR TWS (clientId=98)
  2. For each symbol: qualify contract, download daily bars (10D) to find gap day
  3. Download 15-sec bars for the gap day (cache in data/backtest_cache/)
  4. Run GapAndGoEngine
  5. Generate HTML report
  6. Send summary to Telegram
"""

import json
import logging
import os
import sys
import time as time_mod
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytz

sys.path.insert(0, str(Path(__file__).parent.parent))

from ib_insync import IB, Stock
from config.settings import (
    IBKR_HOST, IBKR_PORT, DATA_DIR, BACKTEST_DATA_DIR, FIB_CHARTS_DIR,
    GG_GAP_MIN_PCT,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_ENABLED,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("gap_and_go_bt")

ET = pytz.timezone("US/Eastern")

# ── Symbols to test ──────────────────────────────────────
SYMBOLS = [
    "BNAI", "SNSE", "CDIO", "WORX", "RXT", "NKTR", "CATX", "ANL",
    "LNKS", "FEED", "MLEC", "VVPR", "BUUU", "CNEY", "DBGI", "BDMD",
    "BATL", "ATOM", "SGN",
]


def connect_ibkr() -> IB:
    ib = IB()
    ib.connect(IBKR_HOST, IBKR_PORT, clientId=98, timeout=15)
    logger.info(f"Connected to IBKR. Server time: {ib.reqCurrentTime()}")
    return ib


def find_gap_day(ib: IB, sym: str, contract: Stock) -> dict | None:
    """Find the most recent gap day (>= GG_GAP_MIN_PCT%) from 10D daily bars."""
    try:
        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='10 D',
            barSizeSetting='1 day', whatToShow='TRADES',
            useRTH=True, formatDate=1, timeout=15,
        )
        if not bars or len(bars) < 2:
            logger.warning(f"  {sym}: insufficient daily bars ({len(bars) if bars else 0})")
            return None

        # Walk backwards to find the most recent gap day
        for i in range(len(bars) - 1, 0, -1):
            bar = bars[i]
            prev_close = bars[i - 1].close
            if prev_close <= 0:
                continue
            move_pct = ((bar.high - prev_close) / prev_close) * 100
            if move_pct >= GG_GAP_MIN_PCT:
                date_str = str(bar.date)[:10]
                result = {
                    "symbol": sym,
                    "date": date_str,
                    "gap_pct": round(move_pct, 1),
                    "prev_close": prev_close,
                    "open_price": bar.open,
                    "high_price": bar.high,
                    "gap_volume": bar.volume,
                }
                logger.info(
                    f"  {sym}: gap +{move_pct:.1f}% on {date_str} "
                    f"prev=${prev_close:.4f} high=${bar.high:.4f} vol={bar.volume:,.0f}"
                )
                return result

        logger.info(f"  {sym}: no gap >= {GG_GAP_MIN_PCT}% in last 10 days")
        return None

    except Exception as e:
        logger.error(f"  {sym}: daily bars error: {e}")
        return None


def get_week_trading_days(gap_date_str: str) -> list[str]:
    """Return trading days from gap day through Friday of that week."""
    gap_date = datetime.strptime(gap_date_str, "%Y-%m-%d")
    # Find Friday of that week
    days_to_friday = (4 - gap_date.weekday()) % 7
    if days_to_friday == 0 and gap_date.weekday() != 4:
        days_to_friday = 7  # next Friday if gap was on weekend (shouldn't happen)
    friday = gap_date + timedelta(days=days_to_friday)

    dates = []
    current = gap_date
    while current <= friday:
        if current.weekday() < 5:  # Mon-Fri
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def download_15s_bars(ib: IB, sym: str, contract: Stock, date_str: str) -> pd.DataFrame | None:
    """Download 15-sec bars for a specific date. Uses cache."""
    cache_path = BACKTEST_DATA_DIR / f"{sym}_15s_{date_str}.parquet"

    if cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            if len(df) >= 50:
                first_date = str(df.index[0])[:10]
                if first_date == date_str:
                    logger.info(f"  {sym}: cached ({len(df)} bars)")
                    return df
                else:
                    logger.info(f"  {sym}: cached but wrong date ({first_date}), re-downloading")
        except Exception:
            pass

    try:
        ib.qualifyContracts(contract)
        end_dt = f"{date_str.replace('-', '')} 23:59:59 US/Eastern"
        bars = ib.reqHistoricalData(
            contract, endDateTime=end_dt, durationStr='1 D',
            barSizeSetting='15 secs', whatToShow='TRADES',
            useRTH=False, formatDate=2, timeout=15,
        )
        if bars and len(bars) >= 50:
            df = pd.DataFrame([{
                'open': b.open, 'high': b.high,
                'low': b.low, 'close': b.close,
                'volume': b.volume,
            } for b in bars], index=pd.DatetimeIndex([b.date for b in bars]))
            df.to_parquet(cache_path)
            logger.info(f"  {sym}: {len(df)} bars saved")
            return df
        else:
            logger.warning(f"  {sym}: only {len(bars) if bars else 0} bars")
            return None
    except Exception as e:
        logger.error(f"  {sym}: 15s download error: {e}")
        return None


def download_daily_5y(ib: IB, sym: str, contract: Stock) -> pd.DataFrame | None:
    """Download 5Y daily data for fib anchor computation. Uses cache."""
    cache_path = BACKTEST_DATA_DIR / f"{sym}_daily_5y.parquet"

    if cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            if len(df) >= 20:
                logger.info(f"  {sym}: daily 5Y cached ({len(df)} bars)")
                return df
        except Exception:
            pass

    try:
        ib.qualifyContracts(contract)
        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='5 Y',
            barSizeSetting='1 day', whatToShow='TRADES',
            useRTH=True, formatDate=1, timeout=30,
        )
        if bars and len(bars) >= 20:
            df = pd.DataFrame([{
                'open': b.open, 'high': b.high,
                'low': b.low, 'close': b.close,
                'volume': b.volume,
            } for b in bars], index=pd.DatetimeIndex([b.date for b in bars]))
            df.to_parquet(cache_path)
            logger.info(f"  {sym}: {len(df)} daily bars saved")
            return df
        else:
            logger.warning(f"  {sym}: only {len(bars) if bars else 0} daily bars")
            return None
    except Exception as e:
        logger.error(f"  {sym}: daily 5Y download error: {e}")
        return None


def send_telegram_summary(result) -> None:
    """Send summary to Telegram."""
    if not TELEGRAM_ENABLED:
        logger.info("Telegram disabled, skipping")
        return

    import urllib.request

    bot_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

    wr = result.winning_trades / result.total_trades * 100 if result.total_trades > 0 else 0
    final_eq = 3000 + result.total_pnl_net

    tg_msg = (
        f"📊 <b>Gap and Go Backtest</b>\n\n"
        f"Strategy: VWAP + HA green 1m+5m\n"
        f"Symbols: {len(result.symbols_tested)}\n"
        f"Trades: {result.total_trades} ({result.winning_trades}W / {result.losing_trades}L)\n"
        f"Win rate: {wr:.0f}%\n"
        f"Net P&L: <b>${result.total_pnl_net:+,.2f}</b>\n"
        f"Max DD: {result.max_drawdown:.1%}\n"
        f"Final equity: ${final_eq:,.2f}\n"
    )

    if result.trades:
        tg_msg += "\nTrades:\n"
        for t in result.trades:
            pnl = t["pnl_net"]
            icon = "✅" if pnl >= 0 else "❌"
            fib_s = t.get("fib_series", "?")
            fib_r = t.get("fib_ratio", 0)
            tg_msg += (
                f"{icon} {t['symbol']} "
                f"${t['entry_fill']:.2f}→${t['exit_fill']:.2f} "
                f"P&L=${pnl:+.2f} ({t['exit_reason']})\n"
            )

    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": tg_msg,
        "parse_mode": "HTML",
    }).encode()
    req = urllib.request.Request(
        f"{bot_url}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        logger.info("Telegram summary sent")
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")


def main():
    logger.info("=" * 60)
    logger.info("Gap and Go Backtest — 19 Symbols (full week)")
    logger.info("=" * 60)

    # Step 1: Connect
    ib = connect_ibkr()

    # Step 2: For each symbol, find gap day + download 15s bars for full week
    gappers_data = []
    for sym in SYMBOLS:
        logger.info(f"\n--- {sym} ---")
        contract = Stock(sym, 'SMART', 'USD')
        try:
            ib.qualifyContracts(contract)
        except Exception as e:
            logger.error(f"  {sym}: qualify failed: {e}")
            time_mod.sleep(0.5)
            continue

        time_mod.sleep(0.3)

        # Find gap day
        gap = find_gap_day(ib, sym, contract)
        if gap is None:
            continue

        # Get all trading days from gap day through Friday
        week_days = get_week_trading_days(gap["date"])
        logger.info(f"  {sym}: testing {len(week_days)} days: {week_days}")

        for day in week_days:
            time_mod.sleep(1)
            df_15s = download_15s_bars(ib, sym, contract, day)
            if df_15s is None:
                continue

            day_gap = gap.copy()
            day_gap["date"] = day
            day_gap["df_15s"] = df_15s
            gappers_data.append(day_gap)

        time_mod.sleep(1)

    ib.disconnect()
    unique_syms = len(set(g["symbol"] for g in gappers_data))
    unique_days = len(set((g["symbol"], g["date"]) for g in gappers_data))
    logger.info(f"\nDisconnected from IBKR. {unique_syms} symbols, {unique_days} symbol-days ready.")

    if not gappers_data:
        logger.info("No gap data found. Exiting.")
        return

    # Step 3: Run backtest
    logger.info("\n=== Running Gap and Go Backtest ===")
    from simulation.gap_and_go_backtest import GapAndGoEngine

    engine = GapAndGoEngine(capital=3000.0)
    result = engine.run(gappers_data)
    chart_data = engine.get_chart_data()

    # Step 4: Generate HTML report
    logger.info("\n=== Generating HTML Report ===")
    from simulation.gap_and_go_charts import generate_gap_and_go_report

    report_path = generate_gap_and_go_report(chart_data, result)
    logger.info(f"Report: {report_path}")

    # Open in browser
    if report_path.exists():
        os.system(f"xdg-open {report_path} &")

    # Step 5: Send Telegram summary
    send_telegram_summary(result)

    logger.info("\n=== Done ===")


if __name__ == "__main__":
    main()
