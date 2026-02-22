"""Run FIB Double-Touch backtest on Thursday (Feb 19) + Friday (Feb 20) gappers.

Today is Saturday Feb 21 — last trading days were Thu Feb 19 and Fri Feb 20.
Steps:
  1. Connect to IBKR TWS
  2. Find Thu (Feb 19) and Fri (Feb 20) gappers from daily bars
  3. Download 15-sec bars for both days
  4. Build combined gappers CSV
  5. Run backtest engine
  6. Generate HTML report with per-trade charts
  7. Send summary + charts to Telegram
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

from ib_insync import IB, Stock, ScannerSubscription
from config.settings import (
    IBKR_HOST, IBKR_PORT, DATA_DIR, BACKTEST_DATA_DIR, FIB_CHARTS_DIR,
    FIB_DT_GAP_MIN_PCT, FIB_DT_GAP_MAX_PCT, FIB_DT_MIN_GAP_VOLUME,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_ENABLED,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("two_day_bt")

ET = pytz.timezone("US/Eastern")
COMBINED_CSV = DATA_DIR / "gappers_two_days.csv"


def connect_ibkr() -> IB:
    ib = IB()
    ib.connect(IBKR_HOST, IBKR_PORT, clientId=97, timeout=15)
    logger.info(f"Connected to IBKR. Server time: {ib.reqCurrentTime()}")
    return ib


def find_gappers_from_daily(ib: IB, target_date: str, symbols_contracts: dict) -> list[dict]:
    """Check daily bars for each symbol to find gappers on target_date."""
    logger.info(f"\n=== Finding gappers for {target_date} ===")
    gappers = []

    for sym, con in sorted(symbols_contracts.items()):
        try:
            ib.qualifyContracts(con)
            daily = ib.reqHistoricalData(
                con, endDateTime='', durationStr='10 D',
                barSizeSetting='1 day', whatToShow='TRADES',
                useRTH=True, formatDate=1, timeout=15,
            )
            if not daily or len(daily) < 3:
                continue

            for i, bar in enumerate(daily):
                bar_date = str(bar.date)[:10]
                if bar_date == target_date and i > 0:
                    prev_close = daily[i - 1].close
                    high_price = bar.high
                    volume = bar.volume
                    if prev_close <= 0:
                        continue
                    move_pct = ((high_price - prev_close) / prev_close) * 100
                    if move_pct >= FIB_DT_GAP_MIN_PCT:
                        gappers.append({
                            'symbol': sym,
                            'contract': con,
                            'date': target_date,
                            'gap_pct': round(move_pct, 1),
                            'prev_close': prev_close,
                            'open_price': bar.open,
                            'high_price': high_price,
                            'gap_volume': volume,
                            'float_shares': 0,
                        })
                        logger.info(f"  {target_date} {sym}: +{move_pct:.1f}% prev=${prev_close:.4f} high=${high_price:.4f} vol={volume:,.0f}")
                    break

            time_mod.sleep(0.3)
        except Exception as e:
            logger.warning(f"  {sym}: {e}")

    logger.info(f"Found {len(gappers)} gappers ≥{FIB_DT_GAP_MIN_PCT}% on {target_date}")
    return gappers


def get_symbol_universe(ib: IB) -> dict:
    """Build a universe of symbols from scanner + existing data."""
    # Scanner for broad coverage
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

    contracts = {}
    for r in results:
        sym = r.contractDetails.contract.symbol
        if sym not in contracts:
            contracts[sym] = r.contractDetails.contract

    # Add symbols from existing Friday gappers CSV
    fri_csv = DATA_DIR / "gappers_today.csv"
    if fri_csv.exists():
        df = pd.read_csv(fri_csv)
        for sym in df['symbol'].unique():
            if sym not in contracts:
                contracts[sym] = Stock(sym, 'SMART', 'USD')

    # Also scan HOT_BY_VOLUME for broader coverage
    sub2 = ScannerSubscription(
        instrument="STK",
        locationCode="STK.US.MAJOR",
        scanCode="HOT_BY_VOLUME",
        numberOfRows=50,
    )
    sub2.abovePrice = 0.1
    sub2.belowPrice = 20.0
    try:
        results2 = ib.reqScannerData(sub2)
        for r in results2:
            sym = r.contractDetails.contract.symbol
            if sym not in contracts:
                contracts[sym] = r.contractDetails.contract
        logger.info(f"HOT_BY_VOLUME returned {len(results2)} stocks")
    except Exception:
        pass

    logger.info(f"Total symbol universe: {len(contracts)}")
    return contracts


def download_15s_bars(ib: IB, gappers: list[dict], date_str: str) -> int:
    """Download 15-sec bars for a specific date. Returns count of successful downloads."""
    logger.info(f"\n=== Downloading 15-sec bars for {date_str} ===")
    count = 0
    for g in gappers:
        sym = g['symbol']
        con = g['contract']
        cache_path = BACKTEST_DATA_DIR / f"{sym}_15s_{date_str}.parquet"

        if cache_path.exists():
            # Verify the file actually contains data for this date
            try:
                df_check = pd.read_parquet(cache_path)
                if len(df_check) >= 50:
                    first_date = str(df_check.index[0])[:10]
                    if first_date == date_str:
                        logger.info(f"  {sym}: already cached ({len(df_check)} bars)")
                        count += 1
                        continue
                    else:
                        logger.info(f"  {sym}: cached but wrong date ({first_date}), re-downloading")
            except Exception:
                pass

        try:
            ib.qualifyContracts(con)
            end_dt = f"{date_str.replace('-', '')} 23:59:59 US/Eastern"
            bars = ib.reqHistoricalData(
                con, endDateTime=end_dt, durationStr='1 D',
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
                logger.info(f"  {sym}: {len(df)} bars saved (first={df.index[0]})")
                count += 1
            else:
                logger.warning(f"  {sym}: only {len(bars) if bars else 0} bars")
            time_mod.sleep(1)
        except Exception as e:
            logger.error(f"  {sym}: {e}")

    logger.info(f"Downloaded {count}/{len(gappers)} symbols for {date_str}")
    return count


def download_daily_5y(ib: IB, gappers: list[dict]) -> None:
    """Download 5Y daily data for symbols that don't have it."""
    logger.info("\n=== Downloading 5Y daily bars (missing only) ===")
    for g in gappers:
        sym = g['symbol']
        con = g['contract']
        daily_path = BACKTEST_DATA_DIR / f"{sym}_daily_5y.parquet"

        if daily_path.exists():
            continue

        try:
            ib.qualifyContracts(con)
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


def build_combined_csv(thu_gappers: list[dict], fri_gappers: list[dict]) -> pd.DataFrame:
    """Build combined CSV with both days' gappers."""
    logger.info("\n=== Building combined gappers CSV ===")
    all_rows = []
    for g in thu_gappers:
        all_rows.append({k: v for k, v in g.items() if k != 'contract'})
    for g in fri_gappers:
        all_rows.append({k: v for k, v in g.items() if k != 'contract'})

    df = pd.DataFrame(all_rows)
    df.to_csv(COMBINED_CSV, index=False)
    logger.info(f"Combined CSV: {len(thu_gappers)} Thu + {len(fri_gappers)} Fri = {len(all_rows)} total")
    return df


def run_backtest() -> tuple:
    """Run FIB DT backtest on combined gappers."""
    logger.info("\n=== Running FIB DT backtest ===")
    from simulation.fib_double_touch_backtest import FibDoubleTouchEngine

    original_load = FibDoubleTouchEngine._load_gappers

    def _load_combined(self):
        return pd.read_csv(COMBINED_CSV)

    FibDoubleTouchEngine._load_gappers = _load_combined
    engine = FibDoubleTouchEngine(capital=3000.0)
    result = engine.run()
    chart_data = engine.get_chart_data()
    FibDoubleTouchEngine._load_gappers = original_load

    return result, chart_data


def generate_report(result, chart_data, thu_count, fri_count) -> str:
    """Generate HTML report and text summary."""
    logger.info("\n=== Generating report ===")
    from simulation.fib_double_touch_charts import generate_double_touch_report

    report_path = generate_double_touch_report(chart_data, result)
    logger.info(f"HTML report: {report_path}")

    # Build text summary
    wr = result.winning_trades / result.total_trades * 100 if result.total_trades > 0 else 0
    lines = [
        f"{'='*60}",
        f"FIB Double-Touch — Thu+Fri Report (Feb 19-20, 2026)",
        f"{'='*60}",
        f"Thursday (Feb 19) gappers: {thu_count}",
        f"Friday (Feb 20) gappers:   {fri_count}",
        f"Total trades: {result.total_trades}",
        f"Winning: {result.winning_trades}",
        f"Losing: {result.losing_trades}",
        f"Win rate: {wr:.1f}%",
        f"Net P&L: ${result.total_pnl_net:+,.2f}",
        f"Commissions: ${result.total_commissions:,.2f}",
        f"Max Drawdown: {result.max_drawdown:.2%}",
        f"Final Equity: ${3000 + result.total_pnl_net:,.2f}",
    ]

    if result.trades:
        lines.append(f"\n{'─'*60}")
        lines.append("TRADES:")
        lines.append(f"{'─'*60}")
        for i, t in enumerate(result.trades, 1):
            pnl = t["pnl_net"]
            tag = "WIN " if pnl >= 0 else "LOSS"
            lines.append(
                f"  #{i} {tag} {t['symbol']} | {t['date']} | {t['move_pct']}\n"
                f"    Entry: {t['entry_time'][:19]} @ ${t['entry_fill']:.4f}\n"
                f"    Exit:  {t['exit_time'][:19]} @ ${t['exit_fill']:.4f}\n"
                f"    Fib: ${t.get('fib_level','?')} ({t.get('fib_series','?')} {t.get('fib_ratio','?')})\n"
                f"    P&L: ${pnl:+.2f} ({t['pnl_pct']}) — {t['exit_reason']}"
            )

    report_text = "\n".join(lines)
    report_path_txt = DATA_DIR / "fib_dt_two_day_report.txt"
    report_path_txt.write_text(report_text)
    logger.info(f"Text report: {report_path_txt}")
    return report_text


def send_telegram(result, chart_data, thu_count, fri_count) -> None:
    """Send summary + trade charts to Telegram private chat."""
    if not TELEGRAM_ENABLED:
        logger.info("Telegram disabled, skipping")
        return

    import urllib.request

    bot_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

    # Send text summary
    wr = result.winning_trades / result.total_trades * 100 if result.total_trades > 0 else 0
    tg_msg = (
        f"📊 <b>FIB DT — Thu+Fri Report (Feb 19-20)</b>\n\n"
        f"Thu gappers: {thu_count} | Fri gappers: {fri_count}\n"
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
            tg_msg += f"{icon} {t['symbol']} ({t['date']}) ${t['entry_fill']:.2f}→${t['exit_fill']:.2f} P&L=${pnl:+.2f}\n"

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
        logger.info("Telegram text sent")
    except Exception as e:
        logger.error(f"Telegram text failed: {e}")

    # Send per-trade chart images
    if chart_data:
        try:
            from simulation.fib_double_touch_charts import _build_trade_chart
            import plotly.io as pio
        except ImportError:
            logger.warning("Cannot import chart tools for images")
            return

        for i, cd in enumerate(chart_data):
            trade = cd["trade"]
            sym = trade["symbol"]
            date = trade["date"]
            pnl = trade["pnl_net"]
            try:
                fig = _build_trade_chart(cd)
                img_path = f"/tmp/fib_dt_trade_{i+1}_{sym}_{date}.png"
                fig.write_image(img_path, width=1200, height=700, scale=2)

                caption = f"#{i+1} {'✅' if pnl >= 0 else '❌'} {sym} ({date}) P&L=${pnl:+.2f}"
                with open(img_path, 'rb') as f:
                    boundary = '----WebKitFormBoundary7MA4YWxkTrZu0gW'
                    body = (
                        f'--{boundary}\r\n'
                        f'Content-Disposition: form-data; name="chat_id"\r\n\r\n'
                        f'{TELEGRAM_CHAT_ID}\r\n'
                        f'--{boundary}\r\n'
                        f'Content-Disposition: form-data; name="caption"\r\n\r\n'
                        f'{caption}\r\n'
                        f'--{boundary}\r\n'
                        f'Content-Disposition: form-data; name="photo"; filename="{os.path.basename(img_path)}"\r\n'
                        f'Content-Type: image/png\r\n\r\n'
                    ).encode() + f.read() + f'\r\n--{boundary}--\r\n'.encode()

                    req = urllib.request.Request(
                        f"{bot_url}/sendPhoto",
                        data=body,
                        headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
                    )
                    urllib.request.urlopen(req, timeout=30)
                    logger.info(f"  Sent chart #{i+1}: {sym} ({date})")
                    time_mod.sleep(1)
            except Exception as e:
                logger.error(f"  Chart #{i+1} {sym}: {e}")


def main():
    logger.info("=" * 60)
    logger.info("FIB DT — Two-Day Backtest (Thu Feb 19 + Fri Feb 20)")
    logger.info("=" * 60)

    # Connect
    ib = connect_ibkr()

    # Step 1: Get symbol universe
    sym_universe = get_symbol_universe(ib)

    # Step 2: Find gappers for both days from daily bars
    thu_gappers = find_gappers_from_daily(ib, '2026-02-19', sym_universe)
    fri_gappers = find_gappers_from_daily(ib, '2026-02-20', sym_universe)

    all_gappers = thu_gappers + fri_gappers
    if not all_gappers:
        logger.info("No gappers found for either day. Exiting.")
        ib.disconnect()
        return

    # Step 3: Download daily 5Y and 15-sec bars
    download_daily_5y(ib, all_gappers)
    download_15s_bars(ib, thu_gappers, '2026-02-19')
    download_15s_bars(ib, fri_gappers, '2026-02-20')

    ib.disconnect()
    logger.info("Disconnected from IBKR")

    # Step 4: Build combined CSV
    build_combined_csv(thu_gappers, fri_gappers)

    # Step 5: Run backtest
    result, chart_data = run_backtest()

    # Step 6: Generate report
    report = generate_report(result, chart_data, len(thu_gappers), len(fri_gappers))
    print(report)

    # Step 7: Open HTML report in browser
    html_path = FIB_CHARTS_DIR / "fib_dt_today_report.html"
    if html_path.exists():
        os.system(f"xdg-open {html_path} &")

    # Step 8: Send to Telegram
    send_telegram(result, chart_data, len(thu_gappers), len(fri_gappers))

    logger.info("\n=== Done ===")


if __name__ == "__main__":
    main()
