"""Run FIB Double-Touch backtest on today's gappers — live end-of-day report.

Usage: python simulation/run_today_dt_backtest.py
  - Scans for today's ≥20% gappers via IBKR
  - Waits until market close (16:15 ET)
  - Downloads full-day 15-sec bars
  - Runs the FIB DT backtest engine
  - Saves report to data/fib_dt_today_report.txt
  - Sends summary via Telegram
"""

import json
import logging
import os
import sys
import time as time_mod
from datetime import datetime, time as dt_time
from pathlib import Path

import pandas as pd
import pytz

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ib_insync import IB, Stock, ScannerSubscription
from config.settings import (
    IBKR_HOST, IBKR_PORT, DATA_DIR, BACKTEST_DATA_DIR,
    FIB_DT_GAP_MIN_PCT, FIB_DT_GAP_MAX_PCT, FIB_DT_MIN_GAP_VOLUME,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_ENABLED,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(DATA_DIR / "fib_dt_today.log", mode='w'),
    ],
)
logger = logging.getLogger("fib_dt_today")

ET = pytz.timezone("US/Eastern")
REPORT_PATH = DATA_DIR / "fib_dt_today_report.txt"
GAPPERS_TODAY_CSV = DATA_DIR / "gappers_today.csv"

# ── Telegram helper ──────────────────────────────────────────

def _send_telegram(text: str) -> None:
    if not TELEGRAM_ENABLED:
        return
    import urllib.request
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


# ── Step 1: Scan for today's gappers ─────────────────────────

def scan_today_gappers() -> list[dict]:
    """Connect to IBKR, scan for today's ≥20% movers."""
    ib = IB()
    ib.connect(IBKR_HOST, IBKR_PORT, clientId=98, timeout=15)
    logger.info(f"Connected to IBKR. Server time: {ib.reqCurrentTime()}")

    sub = ScannerSubscription(
        instrument="STK",
        locationCode="STK.US.MAJOR",
        scanCode="TOP_PERC_GAIN",
        numberOfRows=50,
    )
    sub.abovePrice = 0.1
    sub.belowPrice = 20.0

    results = ib.reqScannerData(sub)
    logger.info(f"Scanner returned {len(results)} stocks")

    seen = set()
    contracts = []
    for r in results:
        sym = r.contractDetails.contract.symbol
        if sym not in seen:
            seen.add(sym)
            contracts.append(r.contractDetails.contract)

    gappers = []
    for con in contracts:
        sym = con.symbol
        try:
            ib.qualifyContracts(con)
            daily = ib.reqHistoricalData(
                con, endDateTime='', durationStr='5 D',
                barSizeSetting='1 day', whatToShow='TRADES',
                useRTH=True, formatDate=1, timeout=15,
            )
            if not daily or len(daily) < 2:
                continue

            prev_close = daily[-2].close
            today_bar = daily[-1]
            high_price = today_bar.high
            volume = today_bar.volume

            if prev_close <= 0:
                continue
            move_pct = ((high_price - prev_close) / prev_close) * 100

            if move_pct >= FIB_DT_GAP_MIN_PCT:
                gappers.append({
                    'symbol': sym,
                    'contract': con,
                    'date': datetime.now().strftime("%Y-%m-%d"),
                    'gap_pct': round(move_pct, 1),
                    'prev_close': prev_close,
                    'open_price': today_bar.open,
                    'high_price': high_price,
                    'gap_volume': volume,
                    'float_shares': 0,
                })
                logger.info(f"  + {sym}: +{move_pct:.1f}% | prev=${prev_close:.4f} | high=${high_price:.4f} | vol={volume:,.0f}")
            time_mod.sleep(0.3)
        except Exception as e:
            logger.warning(f"  {sym}: {e}")

    ib.disconnect()
    logger.info(f"Found {len(gappers)} gappers ≥{FIB_DT_GAP_MIN_PCT}%")
    return gappers


# ── Step 2: Download data ────────────────────────────────────

def download_data(gappers: list[dict]) -> None:
    """Download 15-sec bars and 5y daily for each gapper."""
    ib = IB()
    ib.connect(IBKR_HOST, IBKR_PORT, clientId=98, timeout=15)
    today_str = datetime.now().strftime("%Y-%m-%d")

    for g in gappers:
        sym = g['symbol']
        con = g['contract']

        # 15-sec bars (full day)
        cache_15s = BACKTEST_DATA_DIR / f"{sym}_15s_{today_str}.parquet"
        try:
            bars = ib.reqHistoricalData(
                con, endDateTime='', durationStr='1 D',
                barSizeSetting='15 secs', whatToShow='TRADES',
                useRTH=False, formatDate=2, timeout=15,
            )
            if bars and len(bars) >= 50:
                df = pd.DataFrame([{
                    'open': b.open, 'high': b.high,
                    'low': b.low, 'close': b.close,
                    'volume': b.volume,
                } for b in bars], index=pd.DatetimeIndex([b.date for b in bars]))
                df.to_parquet(cache_15s)
                logger.info(f"  {sym}: {len(df)} 15s bars")
            else:
                logger.warning(f"  {sym}: only {len(bars) if bars else 0} bars")
            time_mod.sleep(1)
        except Exception as e:
            logger.error(f"  {sym} 15s: {e}")

        # 5y daily (only if missing)
        daily_path = BACKTEST_DATA_DIR / f"{sym}_daily_5y.parquet"
        if not daily_path.exists():
            try:
                bars = ib.reqHistoricalData(
                    con, endDateTime='', durationStr='5 Y',
                    barSizeSetting='1 day', whatToShow='TRADES',
                    useRTH=True, formatDate=1, timeout=30,
                )
                if bars and len(bars) >= 20:
                    df = pd.DataFrame([{
                        'open': b.open, 'high': b.high,
                        'low': b.low, 'close': b.close,
                        'volume': b.volume,
                    } for b in bars], index=pd.DatetimeIndex([b.date for b in bars]))
                    df.to_parquet(daily_path)
                    logger.info(f"  {sym}: {len(df)} daily bars")
                time_mod.sleep(1)
            except Exception as e:
                logger.error(f"  {sym} daily: {e}")

    ib.disconnect()


# ── Step 3: Run backtest ─────────────────────────────────────

def run_backtest() -> str:
    """Run FIB DT backtest on today's gappers, return report text."""
    from simulation.fib_double_touch_backtest import FibDoubleTouchEngine

    original_load = FibDoubleTouchEngine._load_gappers

    def _load_today(self):
        return pd.read_csv(GAPPERS_TODAY_CSV)

    FibDoubleTouchEngine._load_gappers = _load_today

    engine = FibDoubleTouchEngine(capital=3000.0)
    result = engine.run()

    FibDoubleTouchEngine._load_gappers = original_load

    # Build report
    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"FIB Double-Touch — Today's Report ({datetime.now().strftime('%Y-%m-%d')})")
    lines.append(f"{'='*60}")

    df = pd.read_csv(GAPPERS_TODAY_CSV)
    lines.append(f"Gappers scanned: {len(df)}")
    for _, r in df.iterrows():
        filt = ""
        if r.gap_pct > FIB_DT_GAP_MAX_PCT:
            filt = " [SKIP: gap > max]"
        elif r.gap_volume < FIB_DT_MIN_GAP_VOLUME:
            filt = " [SKIP: low vol]"
        lines.append(f"  {r.symbol}: +{r.gap_pct}% vol={r.gap_volume:,.0f}{filt}")

    lines.append(f"\nTotal trades:  {result.total_trades}")
    lines.append(f"Winning:       {result.winning_trades}")
    lines.append(f"Losing:        {result.losing_trades}")
    wr = result.winning_trades / result.total_trades * 100 if result.total_trades > 0 else 0
    lines.append(f"Win rate:      {wr:.1f}%")
    lines.append(f"Net P&L:       ${result.total_pnl_net:+,.2f}")
    lines.append(f"Commissions:   ${result.total_commissions:,.2f}")
    lines.append(f"Max Drawdown:  {result.max_drawdown:.2%}")
    lines.append(f"Final Equity:  ${3000 + result.total_pnl_net:,.2f}")

    if result.trades:
        lines.append(f"\n{'─'*60}")
        lines.append(f"TRADES:")
        lines.append(f"{'─'*60}")
        for i, t in enumerate(result.trades, 1):
            pnl = t["pnl_net"]
            tag = "WIN " if pnl >= 0 else "LOSS"
            lines.append(
                f"  #{i} {tag} {t['symbol']} | {t['move_pct']}\n"
                f"    Entry: {t['entry_time'][:19]} @ ${t['entry_fill']:.4f}\n"
                f"    Exit:  {t['exit_time'][:19]} @ ${t['exit_fill']:.4f}\n"
                f"    Fib: ${t.get('fib_level','?')} ({t.get('fib_series','?')} {t.get('fib_ratio','?')})\n"
                f"    P&L: ${pnl:+.2f} ({t['pnl_pct']}) — {t['exit_reason']}"
            )
    else:
        lines.append("\nNo trades triggered today.")

    report = "\n".join(lines)
    return report


# ── Step 4: Wait for market close ────────────────────────────

def wait_for_market_close() -> None:
    """Sleep until 16:15 ET."""
    target_et = dt_time(16, 15)
    while True:
        now_et = datetime.now(ET)
        if now_et.time() >= target_et:
            break
        remaining = datetime.combine(now_et.date(), target_et, tzinfo=ET) - now_et
        secs = remaining.total_seconds()
        if secs <= 0:
            break
        logger.info(f"Waiting for market close... {secs/60:.0f} min remaining (now {now_et.strftime('%H:%M ET')})")
        time_mod.sleep(min(secs, 300))  # wake every 5 min to log


# ── Main ─────────────────────────────────────────────────────

def main():
    logger.info("=== FIB DT Today's Backtest — Starting ===")

    # Step 1: Scan
    logger.info("\n--- Scanning for today's gappers ---")
    gappers = scan_today_gappers()
    if not gappers:
        logger.info("No gappers found. Exiting.")
        _send_telegram("FIB DT Today: No gappers ≥20% found.")
        return

    # Save gappers CSV
    gappers_data = [{k: v for k, v in g.items() if k != 'contract'} for g in gappers]
    pd.DataFrame(gappers_data).to_csv(GAPPERS_TODAY_CSV, index=False)

    # Notify start
    sym_list = ", ".join(f"{g['symbol']} +{g['gap_pct']}%" for g in gappers_data)
    _send_telegram(f"🤖 FIB DT Today started\n{len(gappers_data)} gappers: {sym_list}\nWaiting for market close...")
    logger.info(f"Gappers saved: {len(gappers_data)}")

    # Step 2: Wait for market close
    logger.info("\n--- Waiting for market close (16:15 ET) ---")
    wait_for_market_close()

    # Step 3: Download full-day data
    logger.info("\n--- Downloading full-day 15-sec bars ---")
    download_data(gappers)

    # Step 4: Run backtest
    logger.info("\n--- Running FIB DT backtest ---")
    report = run_backtest()

    # Save report
    REPORT_PATH.write_text(report)
    logger.info(f"\nReport saved to {REPORT_PATH}")
    print(report)

    # Step 5: Send via Telegram
    # Telegram has 4096 char limit, send summary
    from simulation.fib_double_touch_backtest import FibDoubleTouchEngine
    original_load = FibDoubleTouchEngine._load_gappers
    def _load_today(self):
        return pd.read_csv(GAPPERS_TODAY_CSV)
    FibDoubleTouchEngine._load_gappers = _load_today
    engine = FibDoubleTouchEngine(capital=3000.0)
    result = engine.run()
    FibDoubleTouchEngine._load_gappers = original_load

    wr = result.winning_trades / result.total_trades * 100 if result.total_trades > 0 else 0
    tg_msg = (
        f"📊 <b>FIB DT — End of Day Report</b>\n"
        f"Date: {datetime.now().strftime('%Y-%m-%d')}\n\n"
        f"Gappers scanned: {len(gappers_data)}\n"
        f"Trades: {result.total_trades} ({result.winning_trades}W / {result.losing_trades}L)\n"
        f"Win rate: {wr:.0f}%\n"
        f"Net P&L: <b>${result.total_pnl_net:+,.2f}</b>\n"
        f"Final equity: ${3000 + result.total_pnl_net:,.2f}\n"
    )
    if result.trades:
        tg_msg += "\nTrades:\n"
        for t in result.trades:
            pnl = t["pnl_net"]
            icon = "✅" if pnl >= 0 else "❌"
            tg_msg += f"{icon} {t['symbol']} ${t['entry_fill']:.2f}→${t['exit_fill']:.2f} P&L=${pnl:+.2f}\n"

    _send_telegram(tg_msg)
    logger.info("Telegram report sent.")
    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
