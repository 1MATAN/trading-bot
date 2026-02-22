"""Run Momentum Ride backtest on Thursday (Feb 19) + Friday (Feb 20) gappers.

Uses already-downloaded data:
  - data/gappers_two_days.csv (48 gap events)
  - data/backtest_cache/*_15s_*.parquet (15-sec bars)
  - data/backtest_cache/*_daily_5y.parquet (5Y daily)

Steps:
  1. Load combined gappers CSV
  2. Run MomentumRideEngine
  3. Generate HTML report with per-trade charts + time analysis
  4. Send summary to Telegram
"""

import json
import logging
import os
import sys
import time as time_mod
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    DATA_DIR, FIB_CHARTS_DIR,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_ENABLED,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("mr_backtest")


def run_backtest() -> tuple:
    """Run Momentum Ride backtest on combined gappers."""
    logger.info("\n=== Running Momentum Ride backtest ===")
    from simulation.momentum_ride_backtest import MomentumRideEngine

    engine = MomentumRideEngine(capital=3000.0)
    result = engine.run()
    chart_data = engine.get_chart_data()
    return result, chart_data


def generate_report(result, chart_data) -> str:
    """Generate HTML report and text summary."""
    logger.info("\n=== Generating report ===")
    from simulation.momentum_ride_charts import generate_momentum_ride_report

    report_path = generate_momentum_ride_report(chart_data, result)
    logger.info(f"HTML report: {report_path}")

    # Build text summary
    wr = result.winning_trades / result.total_trades * 100 if result.total_trades > 0 else 0
    lines = [
        f"{'='*60}",
        f"Momentum Ride — Thu+Fri Report (Feb 19-20, 2026)",
        f"{'='*60}",
        f"Total trades: {result.total_trades}",
        f"Winning: {result.winning_trades}",
        f"Losing: {result.losing_trades}",
        f"Win rate: {wr:.1f}%",
        f"Net P&L: ${result.total_pnl_net:+,.2f}",
        f"Commissions: ${result.total_commissions:,.2f}",
        f"Max Drawdown: {result.max_drawdown:.2%}",
        f"Final Equity: ${3000 + result.total_pnl_net:,.2f}",
    ]

    # Time segment analysis
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

    if hour_stats:
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

    if result.trades:
        lines.append(f"\n{'─'*60}")
        lines.append("TRADES:")
        lines.append(f"{'─'*60}")
        for i, t in enumerate(result.trades, 1):
            pnl = t["pnl_net"]
            tag = "WIN " if pnl >= 0 else "LOSS"
            lines.append(
                f"  #{i} {tag} {t['symbol']} | {t.get('date', '')} | {t['move_pct']}\n"
                f"    Entry: {t['entry_time'][:19]} @ ${t['entry_fill']:.4f} VWAP dist: +{t.get('vwap_dist_pct', '?')}%\n"
                f"    Exit:  {t['exit_time'][:19]} @ ${t['exit_fill']:.4f}\n"
                f"    Fib: ${t.get('fib_level', '?')} ({t.get('fib_series', '?')} {t.get('fib_ratio', '?')})\n"
                f"    P&L: ${pnl:+.2f} ({t['pnl_pct']}) — {t['exit_reason']}"
            )

    report_text = "\n".join(lines)
    report_path_txt = DATA_DIR / "momentum_ride_report.txt"
    report_path_txt.write_text(report_text)
    logger.info(f"Text report: {report_path_txt}")
    return report_text


def send_telegram(result, chart_data) -> None:
    """Send summary + trade charts to Telegram private chat."""
    if not TELEGRAM_ENABLED:
        logger.info("Telegram disabled, skipping")
        return

    import urllib.request

    bot_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

    wr = result.winning_trades / result.total_trades * 100 if result.total_trades > 0 else 0
    tg_msg = (
        f"<b>Momentum Ride — Thu+Fri (Feb 19-20)</b>\n\n"
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
            tg_msg += f"{icon} {t['symbol']} ({t.get('date', '')}) ${t['entry_fill']:.2f}->${t['exit_fill']:.2f} P&L=${pnl:+.2f}\n"

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
            import plotly.io as pio
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
    logger.info("=" * 60)
    logger.info("Momentum Ride — Backtest (Thu Feb 19 + Fri Feb 20)")
    logger.info("=" * 60)

    # Check gappers CSV exists
    gappers_csv = DATA_DIR / "gappers_two_days.csv"
    if not gappers_csv.exists():
        logger.error(f"Gappers CSV not found: {gappers_csv}")
        logger.error("Run run_two_day_backtest.py first to download data")
        return

    df = pd.read_csv(gappers_csv)
    logger.info(f"Loaded {len(df)} gap events from {gappers_csv}")

    # Run backtest
    result, chart_data = run_backtest()

    # Generate report
    report = generate_report(result, chart_data)
    print(report)

    # Open HTML report in browser
    html_path = FIB_CHARTS_DIR / "momentum_ride_report.html"
    if html_path.exists():
        os.system(f"xdg-open {html_path} &")

    # Send to Telegram
    send_telegram(result, chart_data)

    logger.info("\n=== Done ===")


if __name__ == "__main__":
    main()
