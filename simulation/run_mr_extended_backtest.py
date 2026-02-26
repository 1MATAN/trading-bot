"""Run Momentum Ride backtest on ~2 weeks of gappers (20-50% gap).

Steps:
  1. Connect to IBKR TWS
  2. Build symbol universe via scanners (TOP_PERC_GAIN + HOT_BY_VOLUME)
  3. Find gappers 20-50% in last ~10 trading days from daily bars
  4. Download 15-sec bars + 5Y daily for each gap event (cache-aware)
  5. Build combined gappers CSV
  6. Run MomentumRideEngine
  7. Generate HTML report + text summary
  8. Send to Telegram
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
    MR_GAP_MIN_PCT, MR_GAP_MAX_PCT, MR_MIN_GAP_VOLUME,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_ENABLED,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("mr_extended_bt")

ET = pytz.timezone("US/Eastern")
COMBINED_CSV = DATA_DIR / "gappers_mr_extended.csv"

# How many trading days to look back
LOOKBACK_TRADING_DAYS = 10


def connect_ibkr() -> IB:
    ib = IB()
    ib.connect(IBKR_HOST, IBKR_PORT, clientId=96, timeout=15)
    logger.info(f"Connected to IBKR. Server time: {ib.reqCurrentTime()}")
    return ib


def get_recent_trading_days(n: int = LOOKBACK_TRADING_DAYS) -> list[str]:
    """Return last N trading days (weekdays) as YYYY-MM-DD strings, most recent first."""
    et_now = datetime.now(ET)
    days = []
    d = et_now.date()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            days.append(d.strftime("%Y-%m-%d"))
    return days


def get_symbol_universe(ib: IB) -> dict:
    """Build a universe of symbols from multiple scanners."""
    contracts = {}

    # Scanner 1: TOP_PERC_GAIN
    sub1 = ScannerSubscription(
        instrument="STK",
        locationCode="STK.US.MAJOR",
        scanCode="TOP_PERC_GAIN",
        numberOfRows=80,
    )
    sub1.abovePrice = 0.1
    sub1.belowPrice = 20.0
    try:
        results1 = ib.reqScannerData(sub1)
        for r in results1:
            sym = r.contractDetails.contract.symbol
            if sym not in contracts:
                contracts[sym] = r.contractDetails.contract
        logger.info(f"TOP_PERC_GAIN: {len(results1)} stocks")
    except Exception as e:
        logger.warning(f"TOP_PERC_GAIN scanner failed: {e}")

    # Scanner 2: HOT_BY_VOLUME
    sub2 = ScannerSubscription(
        instrument="STK",
        locationCode="STK.US.MAJOR",
        scanCode="HOT_BY_VOLUME",
        numberOfRows=80,
    )
    sub2.abovePrice = 0.1
    sub2.belowPrice = 20.0
    try:
        results2 = ib.reqScannerData(sub2)
        for r in results2:
            sym = r.contractDetails.contract.symbol
            if sym not in contracts:
                contracts[sym] = r.contractDetails.contract
        logger.info(f"HOT_BY_VOLUME: {len(results2)} stocks")
    except Exception as e:
        logger.warning(f"HOT_BY_VOLUME scanner failed: {e}")

    # Scanner 3: MOST_ACTIVE
    sub3 = ScannerSubscription(
        instrument="STK",
        locationCode="STK.US.MAJOR",
        scanCode="MOST_ACTIVE",
        numberOfRows=50,
    )
    sub3.abovePrice = 0.1
    sub3.belowPrice = 20.0
    try:
        results3 = ib.reqScannerData(sub3)
        for r in results3:
            sym = r.contractDetails.contract.symbol
            if sym not in contracts:
                contracts[sym] = r.contractDetails.contract
        logger.info(f"MOST_ACTIVE: {len(results3)} stocks")
    except Exception as e:
        logger.warning(f"MOST_ACTIVE scanner failed: {e}")

    # Add symbols from existing gappers CSVs
    for csv_name in ["gappers_today.csv", "gappers_two_days.csv"]:
        csv_path = DATA_DIR / csv_name
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                for sym in df['symbol'].unique():
                    if sym not in contracts:
                        contracts[sym] = Stock(sym, 'SMART', 'USD')
            except Exception:
                pass

    logger.info(f"Total symbol universe: {len(contracts)}")
    return contracts


def find_gappers_from_daily(
    ib: IB,
    target_dates: list[str],
    symbols_contracts: dict,
) -> list[dict]:
    """Check 21D daily bars for each symbol, find gappers 20-50% on any target date."""
    logger.info(f"\n=== Finding gappers for {len(target_dates)} dates ===")
    logger.info(f"  Date range: {target_dates[-1]} to {target_dates[0]}")
    target_set = set(target_dates)
    gappers = []
    checked = 0

    for sym, con in sorted(symbols_contracts.items()):
        checked += 1
        if checked % 20 == 0:
            logger.info(f"  ... checked {checked}/{len(symbols_contracts)} symbols, found {len(gappers)} gappers so far")

        try:
            ib.qualifyContracts(con)
            daily = ib.reqHistoricalData(
                con, endDateTime='', durationStr='21 D',
                barSizeSetting='1 day', whatToShow='TRADES',
                useRTH=True, formatDate=1, timeout=15,
            )
            if not daily or len(daily) < 3:
                time_mod.sleep(0.3)
                continue

            for i, bar in enumerate(daily):
                bar_date = str(bar.date)[:10]
                if bar_date in target_set and i > 0:
                    prev_close = daily[i - 1].close
                    high_price = bar.high
                    volume = bar.volume
                    if prev_close <= 0:
                        continue
                    move_pct = ((high_price - prev_close) / prev_close) * 100

                    if MR_GAP_MIN_PCT <= move_pct <= MR_GAP_MAX_PCT:
                        gappers.append({
                            'symbol': sym,
                            'contract': con,
                            'date': bar_date,
                            'gap_pct': round(move_pct, 1),
                            'prev_close': prev_close,
                            'open_price': bar.open,
                            'high_price': high_price,
                            'gap_volume': volume,
                            'float_shares': 0,
                        })
                        logger.info(
                            f"  {bar_date} {sym}: +{move_pct:.1f}% "
                            f"prev=${prev_close:.4f} high=${high_price:.4f} vol={volume:,.0f}"
                        )

            time_mod.sleep(0.3)
        except Exception as e:
            logger.warning(f"  {sym}: {e}")

    logger.info(f"\nFound {len(gappers)} gappers ({MR_GAP_MIN_PCT}-{MR_GAP_MAX_PCT}%) across {len(target_dates)} trading days")
    return gappers


def download_15s_bars(ib: IB, gappers: list[dict]) -> int:
    """Download 15-sec bars for each gap event's date. Returns count of successful downloads."""
    logger.info(f"\n=== Downloading 15-sec bars for {len(gappers)} gap events ===")
    count = 0
    for i, g in enumerate(gappers):
        sym = g['symbol']
        con = g['contract']
        date_str = str(g['date'])
        cache_path = BACKTEST_DATA_DIR / f"{sym}_15s_{date_str}.parquet"

        if cache_path.exists():
            try:
                df_check = pd.read_parquet(cache_path)
                if len(df_check) >= 50:
                    first_date = str(df_check.index[0])[:10]
                    if first_date == date_str:
                        logger.info(f"  [{i+1}/{len(gappers)}] {sym} {date_str}: cached ({len(df_check)} bars)")
                        count += 1
                        continue
                    else:
                        logger.info(f"  [{i+1}/{len(gappers)}] {sym} {date_str}: wrong date in cache ({first_date}), re-downloading")
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
                logger.info(f"  [{i+1}/{len(gappers)}] {sym} {date_str}: {len(df)} bars saved")
                count += 1
            else:
                logger.warning(f"  [{i+1}/{len(gappers)}] {sym} {date_str}: only {len(bars) if bars else 0} bars")
            time_mod.sleep(1)
        except Exception as e:
            logger.error(f"  [{i+1}/{len(gappers)}] {sym} {date_str}: {e}")

    logger.info(f"Downloaded {count}/{len(gappers)} 15-sec datasets")
    return count


def download_daily_5y(ib: IB, gappers: list[dict]) -> None:
    """Download 5Y daily data for symbols that don't have it cached."""
    # Deduplicate by symbol
    seen = set()
    unique = []
    for g in gappers:
        if g['symbol'] not in seen:
            seen.add(g['symbol'])
            unique.append(g)

    logger.info(f"\n=== Downloading 5Y daily bars ({len(unique)} unique symbols, missing only) ===")
    for g in unique:
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


def build_combined_csv(gappers: list[dict]) -> pd.DataFrame:
    """Build combined CSV from all gappers (without contract objects)."""
    logger.info("\n=== Building combined gappers CSV ===")
    rows = [{k: v for k, v in g.items() if k != 'contract'} for g in gappers]
    df = pd.DataFrame(rows)
    # Sort by date then gap_pct descending
    df = df.sort_values(['date', 'gap_pct'], ascending=[True, False]).reset_index(drop=True)
    df.to_csv(COMBINED_CSV, index=False)

    # Summary by date
    date_counts = df.groupby('date').size()
    for d, cnt in date_counts.items():
        logger.info(f"  {d}: {cnt} gappers")
    logger.info(f"Total: {len(df)} gap events across {len(date_counts)} days → {COMBINED_CSV}")
    return df


def run_backtest() -> tuple:
    """Run Momentum Ride backtest on combined gappers."""
    logger.info("\n=== Running Momentum Ride backtest ===")
    from simulation.momentum_ride_backtest import MomentumRideEngine

    original_load = MomentumRideEngine._load_gappers

    def _load_extended(self):
        return pd.read_csv(COMBINED_CSV)

    MomentumRideEngine._load_gappers = _load_extended
    engine = MomentumRideEngine(capital=3000.0)
    result = engine.run()
    chart_data = engine.get_chart_data()
    MomentumRideEngine._load_gappers = original_load

    return result, chart_data


def generate_report(result, chart_data, gappers_df: pd.DataFrame) -> str:
    """Generate HTML report and text summary."""
    logger.info("\n=== Generating report ===")
    from simulation.momentum_ride_charts import generate_momentum_ride_report

    report_path = generate_momentum_ride_report(chart_data, result)
    logger.info(f"HTML report: {report_path}")

    # Determine date range from data
    dates = sorted(gappers_df['date'].unique())
    date_range = f"{dates[0]} to {dates[-1]}" if dates else "unknown"
    n_days = len(dates)

    wr = result.winning_trades / result.total_trades * 100 if result.total_trades > 0 else 0
    lines = [
        f"{'='*60}",
        f"Momentum Ride — Extended Backtest ({n_days} days)",
        f"  Date range: {date_range}",
        f"{'='*60}",
        f"Gap events:    {len(gappers_df)}",
        f"Total trades:  {result.total_trades}",
        f"Winning:       {result.winning_trades}",
        f"Losing:        {result.losing_trades}",
        f"Win rate:      {wr:.1f}%",
        f"Net P&L:       ${result.total_pnl_net:+,.2f}",
        f"Commissions:   ${result.total_commissions:,.2f}",
        f"Max Drawdown:  {result.max_drawdown:.2%}",
        f"Final Equity:  ${3000 + result.total_pnl_net:,.2f}",
    ]

    # Per-day breakdown
    if result.trades:
        day_stats: dict[str, dict] = {}
        for t in result.trades:
            d = t.get("date", "?")
            if d not in day_stats:
                day_stats[d] = {"wins": 0, "losses": 0, "pnl": 0.0}
            if t["pnl_net"] >= 0:
                day_stats[d]["wins"] += 1
            else:
                day_stats[d]["losses"] += 1
            day_stats[d]["pnl"] += t["pnl_net"]

        lines.append(f"\nPer-Day Breakdown:")
        for d in sorted(day_stats.keys()):
            s = day_stats[d]
            total_d = s["wins"] + s["losses"]
            wr_d = s["wins"] / total_d * 100 if total_d > 0 else 0
            lines.append(
                f"  {d}  {total_d} trades  "
                f"({s['wins']}W/{s['losses']}L)  "
                f"WR={wr_d:.0f}%  P&L=${s['pnl']:+,.2f}"
            )

    # Time segment analysis
    if result.trades:
        hour_stats: dict[int, dict] = {}
        for t in result.trades:
            try:
                ts = pd.Timestamp(t["entry_time"])
                hour = ts.hour
            except Exception:
                hour = -1
            if hour not in hour_stats:
                hour_stats[hour] = {"wins": 0, "losses": 0, "pnl": 0.0}
            if t["pnl_net"] >= 0:
                hour_stats[hour]["wins"] += 1
            else:
                hour_stats[hour]["losses"] += 1
            hour_stats[hour]["pnl"] += t["pnl_net"]

        lines.append(f"\nTime Segment Analysis:")
        for hour in sorted(hour_stats.keys()):
            s = hour_stats[hour]
            total_h = s["wins"] + s["losses"]
            wr_h = s["wins"] / total_h * 100 if total_h > 0 else 0
            lines.append(
                f"  {hour:02d}:00  {total_h} trades  "
                f"({s['wins']}W/{s['losses']}L)  "
                f"WR={wr_h:.0f}%  P&L=${s['pnl']:+,.2f}"
            )

    # Detailed trades
    if result.trades:
        lines.append(f"\n{'─'*60}")
        lines.append("TRADES:")
        lines.append(f"{'─'*60}")
        for i, t in enumerate(result.trades, 1):
            pnl = t["pnl_net"]
            tag = "WIN " if pnl >= 0 else "LOSS"
            lines.append(
                f"  #{i} {tag} {t['symbol']} | {t.get('date', '')} | {t['move_pct']}\n"
                f"    Entry: {t['entry_time'][:19]} @ ${t['entry_fill']:.4f} "
                f"VWAP dist: +{t.get('vwap_dist_pct', '?')}% Signal: {t.get('entry_signal', '?')}\n"
                f"    Exit:  {t['exit_time'][:19]} @ ${t['exit_fill']:.4f}\n"
                f"    Fib: ${t.get('fib_level', '?')} ({t.get('fib_series', '?')} {t.get('fib_ratio', '?')})\n"
                f"    P&L: ${pnl:+.2f} ({t['pnl_pct']}) — {t['exit_reason']}"
            )

    report_text = "\n".join(lines)
    report_path_txt = DATA_DIR / "momentum_ride_report.txt"
    report_path_txt.write_text(report_text)
    logger.info(f"Text report: {report_path_txt}")
    return report_text


def send_telegram(result, chart_data, gappers_df: pd.DataFrame) -> None:
    """Send summary + trade charts to Telegram."""
    if not TELEGRAM_ENABLED:
        logger.info("Telegram disabled, skipping")
        return

    import urllib.request

    bot_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

    dates = sorted(gappers_df['date'].unique())
    date_range = f"{dates[0]} to {dates[-1]}" if dates else "?"
    n_days = len(dates)

    wr = result.winning_trades / result.total_trades * 100 if result.total_trades > 0 else 0
    tg_msg = (
        f"<b>Momentum Ride — Extended Backtest ({n_days} days)</b>\n"
        f"{date_range}\n\n"
        f"Gap events: {len(gappers_df)}\n"
        f"Trades: {result.total_trades} ({result.winning_trades}W / {result.losing_trades}L)\n"
        f"Win rate: {wr:.0f}%\n"
        f"Net P&L: <b>${result.total_pnl_net:+,.2f}</b>\n"
        f"Final equity: ${3000 + result.total_pnl_net:,.2f}\n"
    )
    if result.trades:
        tg_msg += "\nTrades:\n"
        for t in result.trades:
            pnl = t["pnl_net"]
            icon = "+" if pnl >= 0 else "-"
            tg_msg += (
                f"{icon} {t['symbol']} ({t.get('date', '')}) "
                f"${t['entry_fill']:.2f}->${t['exit_fill']:.2f} "
                f"P&L=${pnl:+.2f}\n"
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
        logger.info("Telegram text sent")
    except Exception as e:
        logger.error(f"Telegram text failed: {e}")

    # Send per-trade chart images
    if chart_data:
        try:
            from simulation.momentum_ride_charts import _build_trade_chart
        except ImportError:
            logger.warning("Cannot import chart tools for images")
            return

        for i, cd in enumerate(chart_data):
            trade = cd["trade"]
            sym = trade["symbol"]
            date = trade.get("date", "")
            pnl = trade["pnl_net"]
            try:
                fig = _build_trade_chart(i + 1, cd)
                img_path = f"/tmp/mr_trade_{i+1}_{sym}_{date}.png"
                fig.write_image(img_path, width=1200, height=700, scale=2)

                caption = f"#{i+1} {'W' if pnl >= 0 else 'L'} {sym} ({date}) P&L=${pnl:+.2f}"
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
    trading_days = get_recent_trading_days(LOOKBACK_TRADING_DAYS)

    logger.info("=" * 60)
    logger.info(f"Momentum Ride — Extended Backtest ({len(trading_days)} trading days)")
    logger.info(f"  Date range: {trading_days[-1]} to {trading_days[0]}")
    logger.info(f"  Gap filter: {MR_GAP_MIN_PCT}-{MR_GAP_MAX_PCT}%")
    logger.info("=" * 60)

    # Step 1: Connect
    ib = connect_ibkr()

    # Step 2: Build symbol universe from scanners
    sym_universe = get_symbol_universe(ib)

    # Step 3: Find gappers across all trading days
    gappers = find_gappers_from_daily(ib, trading_days, sym_universe)

    if not gappers:
        logger.info("No gappers found. Exiting.")
        ib.disconnect()
        return

    # Step 4: Download data
    download_daily_5y(ib, gappers)
    download_15s_bars(ib, gappers)

    ib.disconnect()
    logger.info("Disconnected from IBKR")

    # Step 5: Build combined CSV
    gappers_df = build_combined_csv(gappers)

    # Step 6: Run backtest
    result, chart_data = run_backtest()

    # Step 7: Generate report
    report = generate_report(result, chart_data, gappers_df)
    print(report)

    # Step 8: Open HTML report in browser
    html_path = FIB_CHARTS_DIR / "momentum_ride_report.html"
    if html_path.exists():
        os.system(f"xdg-open {html_path} &")

    # Step 9: Send to Telegram
    send_telegram(result, chart_data, gappers_df)

    logger.info("\n=== Done ===")


if __name__ == "__main__":
    main()
