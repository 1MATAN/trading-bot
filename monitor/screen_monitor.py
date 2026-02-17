"""
IBKR Scanner Monitor — Real-time stock scanner via IBKR API with anomaly
detection, Fibonacci levels, and Telegram alerts.

Replaces the old OCR-based screen capture pipeline with direct
``reqScannerData`` calls for clean, reliable data.

Usage:
    python monitor/screen_monitor.py
    OR double-click the desktop shortcut
"""

import sys
from pathlib import Path
# Ensure project root is in path for imports (strategies, config, etc.)
_PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import asyncio
import csv
import json
import logging
import os
import threading
import time
import tkinter as tk
from collections import defaultdict
from datetime import datetime, timedelta

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import requests
from ib_insync import IB, Stock, ScannerSubscription, util as ib_util
from finvizfinance.quote import finvizfinance as Finviz
from deep_translator import GoogleTranslator

from strategies.fibonacci_engine import (
    find_anchor_candle, build_dual_series, advance_series,
)
from config.settings import (
    FIB_LEVELS_24, FIB_LEVEL_COLORS, IBKR_HOST, IBKR_PORT,
    MONITOR_IBKR_CLIENT_ID, MONITOR_SCAN_CODE, MONITOR_SCAN_MAX_RESULTS,
    MONITOR_PRICE_MIN, MONITOR_PRICE_MAX, MONITOR_DEFAULT_FREQ,
    MONITOR_DEFAULT_ALERT_PCT,
)

# ── Config ────────────────────────────────────────────────
_config_dir = Path(__file__).parent / "config"
_env_path = _config_dir / ".env"
_project_env = _PROJECT_ROOT / "config" / ".env"
# Try monitor-local .env first, then project-level config/.env, then default
if _env_path.exists():
    load_dotenv(_env_path)
elif _project_env.exists():
    load_dotenv(_project_env)
else:
    load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
STATE_PATH = DATA_DIR / "monitor_state.json"
LOG_CSV = DATA_DIR / "monitor_log.csv"
LOG_TXT = DATA_DIR / "monitor_log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("monitor")


# ══════════════════════════════════════════════════════════
#  IBKR Connection (single synchronous IB instance)
# ══════════════════════════════════════════════════════════

_ibkr: IB | None = None


def _get_ibkr() -> IB | None:
    """Get/create a dedicated IBKR connection for the monitor."""
    global _ibkr
    if _ibkr and _ibkr.isConnected():
        return _ibkr
    try:
        # Ensure an asyncio event loop exists in this thread
        # (ib_insync needs one; non-main threads don't get one by default)
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        _ibkr = IB()
        _ibkr.connect(IBKR_HOST, IBKR_PORT, clientId=MONITOR_IBKR_CLIENT_ID, timeout=10)
        log.info("IBKR connection established (monitor)")
        accts = _ibkr.managedAccounts() or []
        acct = accts[0] if accts else "?"
        ok = send_telegram(
            f"✅ <b>Monitor Online</b>\n"
            f"  IBKR: מחובר ✓  |  Account: {acct}\n"
            f"  Telegram: מחובר ✓\n"
            f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        if ok:
            log.info("Startup notification sent to Telegram")
        else:
            log.warning("Telegram send failed — check BOT_TOKEN / CHAT_ID")
        return _ibkr
    except Exception as e:
        log.warning(f"IBKR connect failed: {e}")
        _ibkr = None
        return None


# ══════════════════════════════════════════════════════════
#  IBKR Scanner — replaces screenshot + OCR + parse
# ══════════════════════════════════════════════════════════

def _run_ibkr_scan(price_min: float = MONITOR_PRICE_MIN,
                   price_max: float = MONITOR_PRICE_MAX) -> dict:
    """Run IBKR scanner and enrich each symbol with historical data.

    Returns dict in the same format as the old ``parse_scanner_data()``:
        {symbol: {'price': float, 'pct': float, 'volume': str, 'float': str}}
    """
    ib = _get_ibkr()
    if not ib:
        return {}

    sub = ScannerSubscription(
        instrument="STK",
        locationCode="STK.US.MAJOR",
        scanCode=MONITOR_SCAN_CODE,
        numberOfRows=MONITOR_SCAN_MAX_RESULTS,
        abovePrice=price_min,
        belowPrice=price_max,
    )

    try:
        results = ib.reqScannerData(sub)
    except Exception as e:
        log.error(f"reqScannerData failed: {e}")
        return {}

    stocks: dict[str, dict] = {}

    for item in results:
        contract = item.contractDetails.contract
        sym = contract.symbol
        if not sym or not sym.isalpha() or len(sym) > 5:
            continue

        # Enrich with 2-day historical data for price, change%, volume
        try:
            stock_contract = Stock(sym, "SMART", "USD")
            ib.qualifyContracts(stock_contract)
            bars = ib.reqHistoricalData(
                stock_contract,
                endDateTime="",
                durationStr="2 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
            )
            if not bars:
                continue

            last_bar = bars[-1]
            price = last_bar.close
            volume = last_bar.volume

            # Calculate change% from previous close
            if len(bars) >= 2:
                prev_close = bars[-2].close
                pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
            else:
                pct = 0.0

            # Format volume (e.g. 2.3M, 890K)
            if volume >= 1_000_000:
                vol_str = f"{volume / 1_000_000:.1f}M"
            elif volume >= 1_000:
                vol_str = f"{volume / 1_000:.0f}K"
            else:
                vol_str = str(int(volume))

            stocks[sym] = {
                "price": round(price, 2),
                "pct": round(pct, 1),
                "volume": vol_str,
                "volume_raw": int(volume),
                "float": "",
            }

        except Exception as e:
            log.debug(f"Enrich {sym} failed: {e}")
            continue

        time.sleep(0.05)  # light rate-limit between historical requests

    log.info(f"Scanner: {len(results)} raw → {len(stocks)} enriched symbols")
    return stocks


# ══════════════════════════════════════════════════════════
#  Stock History — momentum over time
# ══════════════════════════════════════════════════════════

class StockHistory:
    """Track price/pct history for each stock across scans."""

    def __init__(self):
        # {symbol: [(timestamp, price, pct), ...]}
        self.data = defaultdict(list)

    def record(self, symbol: str, price: float, pct: float):
        self.data[symbol].append((datetime.now(), price, pct))
        # Keep last 2 hours max
        cutoff = datetime.now() - timedelta(hours=2)
        self.data[symbol] = [(t, p, pc) for t, p, pc in self.data[symbol] if t > cutoff]

    def get_momentum(self, symbol: str) -> dict:
        """Calculate pct change over different time windows."""
        pts = self.data.get(symbol, [])
        if len(pts) < 2:
            return {}

        now_pct = pts[-1][2]
        now_price = pts[-1][1]
        momentum = {}

        for label, minutes in [('1m', 1), ('5m', 5), ('15m', 15), ('30m', 30), ('1h', 60)]:
            target_time = datetime.now() - timedelta(minutes=minutes)
            # Find closest point to target_time
            closest = min(pts, key=lambda x: abs((x[0] - target_time).total_seconds()))
            age = abs((closest[0] - target_time).total_seconds())
            # Only use if within 50% of the interval
            if age < minutes * 30:
                delta_pct = now_pct - closest[2]
                delta_price = ((now_price - closest[1]) / closest[1] * 100) if closest[1] > 0 else 0
                momentum[label] = {
                    'pct_delta': delta_pct,
                    'price_delta_pct': delta_price,
                }

        return momentum

    def format_momentum(self, symbol: str) -> str:
        """Format momentum as readable string."""
        m = self.get_momentum(symbol)
        if not m:
            return ""
        parts = []
        for label in ['1m', '5m', '15m', '30m', '1h']:
            if label in m:
                d = m[label]['pct_delta']
                arrow = "↑" if d > 0 else "↓" if d < 0 else "→"
                parts.append(f"{label}:{arrow}{d:+.1f}%")
        return "  ".join(parts)


stock_history = StockHistory()


# ══════════════════════════════════════════════════════════
#  News Fetcher
# ══════════════════════════════════════════════════════════

_translator = GoogleTranslator(source='en', target='iw')


def fetch_stock_info(symbol: str, max_news: int = 3) -> dict:
    """Fetch fundamentals + news from Finviz."""
    result = {'fundamentals': {}, 'news': []}
    try:
        stock = Finviz(symbol)
        fund = stock.ticker_fundament()

        result['fundamentals'] = {
            'short_float': fund.get('Short Float', '-'),
            'cash_per_share': fund.get('Cash/sh', '-'),
            'eps': fund.get('EPS (ttm)', '-'),
            'earnings_date': fund.get('Earnings', '-'),
            'income': fund.get('Income', '-'),
            'float': fund.get('Shs Float', '-'),
        }

        news_df = stock.ticker_news()
        for _, row in news_df.head(max_news).iterrows():
            title_en = row.get('Title', '')
            if not title_en:
                continue
            try:
                title_he = _translator.translate(title_en)
            except Exception:
                title_he = title_en
            date_str = str(row.get('Date', ''))[:10]
            result['news'].append({
                'title_he': title_he,
                'date': date_str,
            })

    except Exception as e:
        log.error(f"Finviz fetch failed for {symbol}: {e}")

    return result


def format_stock_info(symbol: str, info: dict) -> str:
    """Format fundamentals + news for Telegram (Hebrew)."""
    f = info.get('fundamentals', {})

    # Earnings indicator
    eps = f.get('eps', '-')
    try:
        eps_val = float(str(eps).replace(',', ''))
        eps_icon = "🟢" if eps_val > 0 else "🔴"
    except (ValueError, TypeError):
        eps_icon = "⚪"

    lines = [f"📊 <b>{symbol}</b>"]
    lines.append(f"  Float: {f.get('float', '-')}  |  Short: {f.get('short_float', '-')}")
    lines.append(f"  Cash: ${f.get('cash_per_share', '-')}  |  {eps_icon} EPS: {eps}")
    lines.append(f"  Income: {f.get('income', '-')}  |  Earnings: {f.get('earnings_date', '-')}")

    return "\n".join(lines)


def format_news_only(symbol: str, news: list[dict]) -> str:
    """Format news as a separate message."""
    if not news:
        return ""
    lines = [f"📰 <b>{symbol} — חדשות:</b>"]
    for n in news:
        lines.append(f"  • {n['title_he']}  <i>({n['date']})</i>")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════
#  Milestone Alerts (+5% steps for stocks ≥20%)
# ══════════════════════════════════════════════════════════

MILESTONE_START_PCT = 20.0  # only track stocks above this %
MILESTONE_STEP_PCT = 5.0    # alert every 5% step

# {symbol: last milestone alerted (e.g. 25, 30, 35...)}
_milestone_alerted: dict[str, float] = {}


def check_milestone(sym: str, pct: float) -> str | None:
    """Check if stock crossed the next +5% milestone.

    Returns alert message or None.
    E.g. stock at +27% → milestone 25. If last alerted was 20 → alert for 25.
    """
    if pct < MILESTONE_START_PCT:
        return None

    current_milestone = int(pct // MILESTONE_STEP_PCT) * MILESTONE_STEP_PCT
    last = _milestone_alerted.get(sym, MILESTONE_START_PCT - MILESTONE_STEP_PCT)

    if current_milestone > last:
        _milestone_alerted[sym] = current_milestone
        return (
            f"📈 <b>{sym}</b> חצה +{current_milestone:.0f}%!\n"
            f"  נוכחי: {pct:+.1f}%"
        )
    return None


# ══════════════════════════════════════════════════════════
#  Volume Anomaly Detection
# ══════════════════════════════════════════════════════════

HIGH_TURNOVER_PCT = 10.0  # volume > 10% of float = unusual

# {symbol} — already alerted for high volume this session
_vol_alerted: set[str] = set()


def _parse_float_to_shares(flt_str: str) -> float:
    """Parse Finviz float string like '2.14M' or '120.5K' to share count."""
    if not flt_str or flt_str == '-':
        return 0
    flt_str = flt_str.strip().upper()
    try:
        if flt_str.endswith('B'):
            return float(flt_str[:-1]) * 1_000_000_000
        if flt_str.endswith('M'):
            return float(flt_str[:-1]) * 1_000_000
        if flt_str.endswith('K'):
            return float(flt_str[:-1]) * 1_000
        return float(flt_str)
    except (ValueError, TypeError):
        return 0


def check_volume_anomaly(sym: str, volume_raw: int, enrich: dict) -> str | None:
    """Check if volume is unusually high relative to float.

    Returns alert message or None. Only alerts once per symbol per session.
    """
    if sym in _vol_alerted:
        return None

    float_shares = _parse_float_to_shares(enrich.get('float', '-'))
    if float_shares <= 0 or volume_raw <= 0:
        return None

    turnover_pct = (volume_raw / float_shares) * 100

    if turnover_pct >= HIGH_TURNOVER_PCT:
        _vol_alerted.add(sym)
        return (
            f"🔥 <b>{sym}</b> — ווליום חריג!\n"
            f"  Vol: {volume_raw:,.0f}  |  Float: {enrich['float']}\n"
            f"  Turnover: {turnover_pct:.0f}% מהפלוט"
        )
    return None


# ══════════════════════════════════════════════════════════
#  Stock Enrichment Cache (Finviz + Fib)
# ══════════════════════════════════════════════════════════

# {symbol: {float, short, eps, income, earnings, cash, fib_below, fib_above, news}}
_enrichment: dict[str, dict] = {}


def _enrich_stock(sym: str, price: float, on_status=None) -> dict:
    """Fetch Finviz fundamentals + Fib levels for a stock. Cached.

    Returns enrichment dict and sends Telegram alert with full report.
    """
    if sym in _enrichment:
        return _enrichment[sym]

    data = {
        'float': '-', 'short': '-', 'eps': '-',
        'income': '-', 'earnings': '-', 'cash': '-',
        'fib_below': [], 'fib_above': [], 'news': [],
    }

    # ── Finviz fundamentals + news ──
    if on_status:
        on_status(f"Enriching {sym}... (Finviz)")
    try:
        info = fetch_stock_info(sym)
        f = info.get('fundamentals', {})
        data['float'] = f.get('float', '-')
        data['short'] = f.get('short_float', '-')
        data['eps'] = f.get('eps', '-')
        data['income'] = f.get('income', '-')
        data['earnings'] = f.get('earnings_date', '-')
        data['cash'] = f.get('cash_per_share', '-')
        data['news'] = info.get('news', [])
    except Exception as e:
        log.error(f"Finviz {sym}: {e}")

    # ── Fibonacci levels ──
    if price > 0:
        if on_status:
            on_status(f"Enriching {sym}... (Fib)")
        try:
            below, above = calc_fib_levels(sym, price)
            data['fib_below'] = below
            data['fib_above'] = above
        except Exception as e:
            log.error(f"Fib {sym}: {e}")

    _enrichment[sym] = data
    log.info(f"Enriched {sym}: float={data['float']} short={data['short']} fib={len(data['fib_below'])}↓{len(data['fib_above'])}↑")
    return data


def generate_fib_chart(sym: str, df: pd.DataFrame, all_levels: list[float],
                       current_price: float,
                       ratio_map: dict | None = None,
                       df_1min: pd.DataFrame | None = None) -> Path | None:
    """Generate a clean 5-min candlestick chart with Fibonacci levels + SMA overlay.

    SMAs drawn:
      - SMA 9  (5-min)  — yellow solid
      - SMA 200 (5-min) — white solid
      - SMA 20  (1-min, sampled at 5-min timestamps) — cyan dashed
      - SMA 200 (1-min, sampled at 5-min timestamps) — magenta dashed

    Returns path to saved PNG or None on failure.
    """
    try:
        if len(df) < 5:
            return None

        fig, ax = plt.subplots(figsize=(14, 8), facecolor='#0e1117')
        ax.set_facecolor('#0e1117')

        # X-axis as integer indices, labeled with dates/times
        x = np.arange(len(df))
        dates = pd.to_datetime(df['date']) if 'date' in df.columns else df.index

        # Draw candlesticks
        width = 0.6
        for i, (_, row) in enumerate(df.iterrows()):
            o, h, l, c = row['open'], row['high'], row['low'], row['close']
            color = '#26a69a' if c >= o else '#ef5350'  # green / red
            ax.plot([i, i], [l, h], color=color, linewidth=0.8)
            body_bottom = min(o, c)
            body_height = abs(c - o)
            if body_height < 0.001:
                body_height = 0.001
            ax.bar(i, body_height, bottom=body_bottom, width=width,
                   color=color, edgecolor=color, linewidth=0.5)

        # ── SMA lines on 5-min candles ──
        close_5 = df['close'].values

        # SMA 9 (5-min) — yellow solid
        if len(close_5) >= 9:
            sma9 = pd.Series(close_5).rolling(9).mean().values
            ax.plot(x, sma9, color='yellow', linewidth=1.0, alpha=0.9,
                    label='SMA 9 (5m)')

        # SMA 200 (5-min) — white solid
        if len(close_5) >= 200:
            sma200 = pd.Series(close_5).rolling(200).mean().values
            ax.plot(x, sma200, color='white', linewidth=1.0, alpha=0.9,
                    label='SMA 200 (5m)')

        # 1-min SMAs sampled at 5-min timestamps
        if df_1min is not None and len(df_1min) >= 20:
            close_1 = df_1min['close']
            dates_1 = pd.to_datetime(df_1min['date']) if 'date' in df_1min.columns else df_1min.index
            dates_5 = pd.to_datetime(df['date']) if 'date' in df.columns else df.index

            # SMA 20 (1-min)
            sma20_1m = close_1.rolling(20).mean()
            sma20_1m.index = dates_1
            sampled_20 = sma20_1m.reindex(dates_5, method='ffill')
            ax.plot(x, sampled_20.values, color='cyan', linewidth=1.0,
                    alpha=0.85, linestyle='--', label='SMA 20 (1m)')

            # SMA 200 (1-min)
            if len(df_1min) >= 200:
                sma200_1m = close_1.rolling(200).mean()
                sma200_1m.index = dates_1
                sampled_200 = sma200_1m.reindex(dates_5, method='ffill')
                ax.plot(x, sampled_200.values, color='magenta', linewidth=1.0,
                        alpha=0.85, linestyle='--', label='SMA 200 (1m)')

        # Compact legend (top-left)
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(loc='upper left', fontsize=8, facecolor='#0e1117',
                      edgecolor='#444', labelcolor='#ccc', framealpha=0.9)

        # Filter fib levels to visible price range (with margin)
        price_min = df['low'].min()
        price_max = df['high'].max()
        margin = (price_max - price_min) * 0.15
        vis_min = price_min - margin
        vis_max = price_max + margin

        visible_levels = [lv for lv in all_levels if vis_min <= lv <= vis_max]

        # Draw fib levels — S1 labels right, S2 labels left, skip overlaps
        _default_color = '#888888'
        price_span = vis_max - vis_min
        min_label_gap = price_span * 0.018
        last_y_right = -999.0
        last_y_left = -999.0

        for lv in visible_levels:
            info = ratio_map.get(round(lv, 4)) if ratio_map else None
            if isinstance(info, tuple):
                ratio, series = info
            elif info is not None:
                ratio, series = info, "S1"
            else:
                ratio, series = None, "S1"

            color = FIB_LEVEL_COLORS.get(ratio, _default_color) if ratio is not None else _default_color
            ax.axhline(y=lv, color=color, linewidth=0.8, alpha=0.8, linestyle='-')

            if ratio is not None:
                label = f'{ratio}  ${lv:.4f}'
            else:
                label = f'${lv:.4f}'

            if series == "S2":
                if abs(lv - last_y_left) < min_label_gap:
                    continue
                ax.text(-0.5, lv, f'{label} ', color=color,
                        fontsize=7, va='center', ha='right', fontweight='bold')
                last_y_left = lv
            else:
                if abs(lv - last_y_right) < min_label_gap:
                    continue
                ax.text(len(df) - 0.5, lv, f' {label}', color=color,
                        fontsize=7, va='center', ha='left', fontweight='bold')
                last_y_right = lv

        # Current price line
        ax.axhline(y=current_price, color='white', linewidth=1.2,
                    linestyle='--', alpha=0.9)
        ax.text(0, current_price, f' ${current_price:.2f} ',
                color='white', fontsize=8, va='bottom', ha='left',
                fontweight='bold', bbox=dict(boxstyle='round,pad=0.2',
                facecolor='#0e1117', edgecolor='white', alpha=0.8))

        # X-axis date/time labels
        tick_step = max(1, len(df) // 10)
        tick_positions = list(range(0, len(df), tick_step))
        tick_labels = []
        date_list = list(dates)
        for pos in tick_positions:
            d = date_list[pos]
            if hasattr(d, 'strftime'):
                tick_labels.append(d.strftime('%m/%d %H:%M'))
            else:
                tick_labels.append(str(d)[:11])
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, color='#888', fontsize=7, rotation=30, ha='right')

        # Styling
        ax.set_ylim(vis_min, vis_max)
        ax.set_xlim(-1, len(df) + 3)
        ax.tick_params(colors='#888', labelsize=8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('#333')
        ax.spines['left'].set_color('#333')
        ax.yaxis.label.set_color('#888')
        ax.set_title(f'{sym} — 5min + Fibonacci (${current_price:.2f})',
                     color='white', fontsize=14, fontweight='bold', pad=12)
        ax.grid(axis='y', color='#222', linewidth=0.3, alpha=0.5)

        out_path = Path(f'/tmp/fib_{sym}.png')
        fig.savefig(out_path, dpi=100, bbox_inches='tight',
                    facecolor='#0e1117', edgecolor='none')
        plt.close(fig)
        log.info(f"Fib chart saved: {out_path}")
        return out_path

    except Exception as e:
        log.error(f"generate_fib_chart {sym}: {e}")
        plt.close('all')
        return None


def _send_stock_report(sym: str, stock: dict, enriched: dict):
    """Send comprehensive Telegram report for a newly discovered stock."""
    msgs = []

    # 1. Alert line
    msgs.append(
        f"🆕 <b>{sym}</b> — ${stock['price']:.2f}  {stock['pct']:+.1f}%  Vol:{stock.get('volume','-')}"
    )

    # 2. Fundamentals
    eps = enriched.get('eps', '-')
    try:
        eps_val = float(str(eps).replace(',', ''))
        eps_icon = "🟢" if eps_val > 0 else "🔴"
    except (ValueError, TypeError):
        eps_icon = "⚪"

    fund_lines = [f"📊 <b>{sym} — נתונים</b>"]
    fund_lines.append(f"  Float: {enriched['float']}  |  Short: {enriched['short']}")
    fund_lines.append(f"  {eps_icon} EPS: {eps}  |  Cash: ${enriched['cash']}")
    fund_lines.append(f"  Income: {enriched['income']}  |  Earnings: {enriched['earnings']}")
    msgs.append("\n".join(fund_lines))

    # 3. News (Hebrew)
    if enriched['news']:
        news_lines = [f"📰 <b>{sym} — חדשות:</b>"]
        for n in enriched['news']:
            news_lines.append(f"  • {n['title_he']}  <i>({n['date']})</i>")
        msgs.append("\n".join(news_lines))

    for m in msgs:
        send_telegram(m)

    # 4. Fib chart image (5-min candles + SMAs + fib levels)
    cached = _fib_cache.get(sym)
    if cached:
        all_levels = cached[2]
        ratio_map = cached[3]
        df_5min = _download_intraday(sym, bar_size='5 mins', duration='3 D')
        df_1min = _download_intraday(sym, bar_size='1 min', duration='1 D')
        if df_5min is not None:
            img = generate_fib_chart(sym, df_5min, all_levels, stock['price'],
                                     ratio_map=ratio_map, df_1min=df_1min)
            if img:
                send_telegram_photo(img, f"📐 {sym} — 5min + Fibonacci ${stock['price']:.2f}")


# ══════════════════════════════════════════════════════════
#  Fibonacci Levels (WTS Method)
# ══════════════════════════════════════════════════════════

# Cache: {symbol: (anchor_low, anchor_high, all_levels_sorted, ratio_map)}
# ratio_map: {price: (ratio, "S1"|"S2")}
_fib_cache: dict[str, tuple[float, float, list[float], dict]] = {}

# Cache daily DataFrames for chart generation (filled by _download_daily)
_daily_cache: dict[str, pd.DataFrame] = {}


def _download_daily(symbol: str) -> pd.DataFrame | None:
    """Download 5 years daily data from IBKR."""
    ib = _get_ibkr()
    if not ib:
        log.error(f"No IBKR connection for {symbol} daily download")
        return None
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='5 Y',
            barSizeSetting='1 day', whatToShow='TRADES', useRTH=False,
        )
        if bars:
            df = ib_util.df(bars)
            if len(df) >= 5:
                log.info(f"IBKR: {symbol} {len(df)} daily bars")
                _daily_cache[symbol] = df
                return df
    except Exception as e:
        log.warning(f"IBKR download {symbol}: {e}")
    return None


def _download_intraday(symbol: str, bar_size: str = '5 mins',
                       duration: str = '3 D') -> pd.DataFrame | None:
    """Download intraday bars from IBKR.

    Returns DataFrame with OHLCV columns, or None on failure.
    """
    ib = _get_ibkr()
    if not ib:
        log.error(f"No IBKR connection for {symbol} intraday download")
        return None
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr=duration,
            barSizeSetting=bar_size, whatToShow='TRADES', useRTH=False,
        )
        if bars:
            df = ib_util.df(bars)
            if len(df) >= 5:
                log.info(f"IBKR: {symbol} {len(df)} intraday bars ({bar_size})")
                return df
    except Exception as e:
        log.warning(f"IBKR intraday {symbol}: {e}")
    return None


def calc_fib_levels(symbol: str, current_price: float) -> tuple[list[float], list[float]]:
    """Calculate Fibonacci levels using dual-series recursive method.

    Returns (3_below, 3_above) relative to current_price.
    Auto-advances when price > 4.236 of the LOWER series (S1).
    """
    if symbol in _fib_cache:
        anchor_low, anchor_high, all_levels, _ratio_map = _fib_cache[symbol]
    else:
        df = _download_daily(symbol)
        if df is None:
            return [], []

        anchor = find_anchor_candle(df)
        if anchor is None:
            return [], []

        anchor_low, anchor_high, _ = anchor

        dual = build_dual_series(anchor_low, anchor_high)
        all_levels = set()
        ratio_map: dict[float, tuple[float, str]] = {}  # price → (ratio, series)

        for _ in range(25):
            for ratio, price in dual.series1.levels:
                all_levels.add(price)
                ratio_map[round(price, 4)] = (ratio, "S1")
            for ratio, price in dual.series2.levels:
                all_levels.add(price)
                pk = round(price, 4)
                if pk not in ratio_map:  # S1 takes priority
                    ratio_map[pk] = (ratio, "S2")

            # Advance when price crosses 4.236 of S1 (the lower series)
            s1_4236 = dual.series1.low + 4.236 * (dual.series1.high - dual.series1.low)
            if current_price <= s1_4236:
                break
            dual = advance_series(dual)

        # Deduplicate close levels (within 0.1%)
        all_levels = sorted(all_levels)
        deduped = []
        for lv in all_levels:
            if not deduped or abs(lv - deduped[-1]) / max(deduped[-1], 0.001) > 0.001:
                deduped.append(lv)
        all_levels = deduped

        _fib_cache[symbol] = (anchor_low, anchor_high, all_levels, ratio_map)

    below = [l for l in all_levels if l <= current_price][-3:]
    above = [l for l in all_levels if l > current_price][:3]
    return below, above


def format_fib_levels(symbol: str, current_price: float,
                      below: list[float], above: list[float]) -> str:
    """Format fib levels for Telegram message."""
    lines = [f"📐 <b>{symbol} — פיבונאצ'י</b>  (${current_price:.2f})"]

    if above:
        above_str = "  ".join(f"${p:.4f}" for p in above)
        lines.append(f"  ⬆️ מעל: {above_str}")

    if below:
        below_str = "  ".join(f"${p:.4f}" for p in below)
        lines.append(f"  ⬇️ מתחת: {below_str}")

    if not above and not below:
        lines.append("  ❌ אין נתונים")

    return "\n".join(lines)


# ── Fib Touch Detection ─────────────────────────────────

FIB_TOUCH_PCT = 0.8  # within 0.8% of a fib level = "touch"

# {symbol: set of level prices already alerted}
_fib_alerted: dict[str, set[float]] = {}


def check_fib_touch(symbol: str, price: float) -> str | None:
    """Check if price is touching a Fibonacci level.

    Returns alert message or None. Only alerts once per level.
    """
    if price <= 0:
        return None

    # Ensure fib levels are calculated
    if symbol not in _fib_cache:
        calc_fib_levels(symbol, price)
    if symbol not in _fib_cache:
        return None

    _, _, all_levels, _ = _fib_cache[symbol]
    if symbol not in _fib_alerted:
        _fib_alerted[symbol] = set()

    threshold = price * FIB_TOUCH_PCT / 100

    for lv in all_levels:
        if abs(price - lv) <= threshold:
            # Round to avoid float noise in the set
            lv_key = round(lv, 4)
            if lv_key in _fib_alerted[symbol]:
                continue

            _fib_alerted[symbol].add(lv_key)

            # Determine support or resistance
            if price >= lv:
                tag = "תמיכה ⬇️"
            else:
                tag = "התנגדות ⬆️"

            return (
                f"📐 <b>{symbol}</b> נוגע ברמת פיבו!\n"
                f"  {tag}  ${lv:.4f}\n"
                f"  מחיר: ${price:.2f}  (מרחק: {abs(price - lv):.4f})"
            )

    # Clear old alerts if price moved away from all alerted levels
    to_clear = set()
    for alerted_lv in _fib_alerted[symbol]:
        if abs(price - alerted_lv) > threshold * 3:
            to_clear.add(alerted_lv)
    _fib_alerted[symbol] -= to_clear

    return None


# ══════════════════════════════════════════════════════════
#  Anomaly Detection
# ══════════════════════════════════════════════════════════

# Thresholds
PCT_JUMP_THRESHOLD = 5.0    # % change jumps by 5+ between scans
PRICE_JUMP_THRESHOLD = 3.0  # price moves 3%+ between scans

def detect_anomalies(current: dict, previous: dict) -> list[dict]:
    """Only flag truly unusual events."""
    alerts = []

    # New stock appeared in scanner
    for sym in set(current) - set(previous):
        d = current[sym]
        alerts.append({
            'type': 'new',
            'symbol': sym,
            'price': d['price'],
            'pct': d['pct'],
            'volume': d.get('volume', ''),
            'float': d.get('float', ''),
            'fetch_news': True,
            'msg': f"🆕 {sym} appeared: ${d['price']:.2f}  {d['pct']:+.1f}%  Vol:{d.get('volume','-')}  Float:{d.get('float','-')}"
        })

    # Existing stocks — check for big moves
    for sym in current:
        if sym not in previous:
            continue
        c, p = current[sym], previous[sym]

        # % change jumped significantly
        if p['pct'] != 0 and c['pct'] != 0:
            diff = c['pct'] - p['pct']
            if abs(diff) >= PCT_JUMP_THRESHOLD:
                direction = "⬆️" if diff > 0 else "⬇️"
                alerts.append({
                    'type': 'pct_jump',
                    'symbol': sym,
                    'before': p['pct'],
                    'after': c['pct'],
                    'diff': diff,
                    'msg': f"{direction} {sym} moved {diff:+.1f}%  ({p['pct']:+.1f}% → {c['pct']:+.1f}%)  Price: ${c['price']:.2f}"
                })

        # Price jumped
        if p['price'] > 0 and c['price'] > 0:
            chg = (c['price'] - p['price']) / p['price'] * 100
            if abs(chg) >= PRICE_JUMP_THRESHOLD:
                direction = "🚀" if chg > 0 else "💥"
                alerts.append({
                    'type': 'price_jump',
                    'symbol': sym,
                    'price_before': p['price'],
                    'price_after': c['price'],
                    'change_pct': chg,
                    'msg': f"{direction} {sym} price ${p['price']:.2f} → ${c['price']:.2f}  ({chg:+.1f}%)"
                })

    return alerts


# ══════════════════════════════════════════════════════════
#  Telegram
# ══════════════════════════════════════════════════════════

def send_telegram(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={'chat_id': CHAT_ID, 'text': text, 'parse_mode': 'HTML'},
            timeout=10,
        )
        return resp.ok
    except Exception as e:
        log.error(f"Telegram: {e}")
        return False


def send_telegram_photo(image_path: Path, caption: str = "") -> bool:
    """Send a photo to Telegram via multipart upload."""
    if not BOT_TOKEN or not CHAT_ID:
        return False
    try:
        with open(image_path, 'rb') as photo:
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                data={'chat_id': CHAT_ID, 'caption': caption, 'parse_mode': 'HTML'},
                files={'photo': photo},
                timeout=30,
            )
        return resp.ok
    except Exception as e:
        log.error(f"Telegram photo: {e}")
        return False


# ══════════════════════════════════════════════════════════
#  File Logger
# ══════════════════════════════════════════════════════════

class FileLogger:
    def __init__(self):
        if not LOG_CSV.exists():
            with open(LOG_CSV, 'w', newline='') as f:
                csv.writer(f).writerow(['timestamp', 'event', 'symbol', 'price', 'pct', 'volume', 'float', 'detail'])

    def log_scan(self, ts, stocks):
        with open(LOG_TXT, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"SCAN: {ts}  |  {len(stocks)} symbols\n")
            for sym, d in sorted(stocks.items()):
                f.write(f"  {sym:<6} {d['pct']:>+7.1f}%  ${d['price']:<8.2f}  Vol:{d.get('volume',''):>8}  Float:{d.get('float','')}\n")

    def log_alert(self, ts, alert):
        with open(LOG_TXT, 'a', encoding='utf-8') as f:
            f.write(f"  *** {alert['msg']}\n")
        with open(LOG_CSV, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([
                ts, alert['type'], alert.get('symbol', ''),
                alert.get('price', alert.get('price_after', 0)),
                alert.get('pct', alert.get('after', 0)),
                alert.get('volume', ''), alert.get('float', ''),
                alert['msg'],
            ])


file_logger = FileLogger()


# ══════════════════════════════════════════════════════════
#  Scanner Thread (replaces MonitorThread)
# ══════════════════════════════════════════════════════════

class ScannerThread(threading.Thread):
    def __init__(self, freq: int, price_min: float, price_max: float,
                 on_status=None, on_stocks=None):
        super().__init__(daemon=True)
        self.freq = freq
        self.price_min = price_min
        self.price_max = price_max
        self.on_status = on_status
        self.on_stocks = on_stocks  # callback(dict) to update GUI table
        self.running = False
        self.previous: dict = {}
        self.count = 0

    def stop(self):
        self.running = False

    def run(self):
        self.running = True
        log.info(f"Scanner started: freq={self.freq}s, price ${self.price_min}-${self.price_max}")
        while self.running:
            try:
                self._cycle()
            except Exception as e:
                log.error(f"Scanner cycle error: {e}")
                if self.on_status:
                    self.on_status(f"Error: {e}")
            for _ in range(self.freq):
                if not self.running:
                    break
                time.sleep(1)

    def _cycle(self):
        current = _run_ibkr_scan(self.price_min, self.price_max)
        if not current and not self.previous:
            if self.on_status:
                self.on_status("No data from scanner")
            return

        self.count += 1
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for sym, d in current.items():
            stock_history.record(sym, d['price'], d['pct'])

        file_logger.log_scan(ts, current)

        # ── Immediate GUI update with scan data + any cached enrichment ──
        merged = {}
        for sym, d in current.items():
            merged[sym] = dict(d)
            if sym in _enrichment:
                merged[sym]['enrich'] = _enrichment[sym]
        if self.on_stocks and merged:
            self.on_stocks(merged)

        status = f"#{self.count}  {len(current)} stocks"

        is_baseline = not self.previous

        # ── Determine which stocks need enrichment ──
        if is_baseline:
            # First scan: enrich ALL for GUI, but NO Telegram flood
            new_syms = list(current.keys())
        else:
            # Subsequent: only truly new stocks
            new_syms = list(set(current) - set(self.previous))

        # ── Enrich stocks (Finviz + Fib) ──
        enriched_count = 0
        for sym in new_syms:
            if sym in _enrichment:
                continue
            d = current[sym]
            if self.on_status:
                self.on_status(f"#{self.count}  Enriching {sym}... ({enriched_count+1}/{len(new_syms)})")
            _enrich_stock(sym, d['price'], on_status=self.on_status)

            if not is_baseline:
                # Only send Telegram for NEW stocks after baseline
                _send_stock_report(sym, d, _enrichment[sym])
                file_logger.log_alert(ts, {
                    'type': 'new', 'symbol': sym,
                    'price': d['price'], 'pct': d['pct'],
                    'volume': d.get('volume', ''),
                    'msg': f"🆕 {sym}: ${d['price']:.2f} {d['pct']:+.1f}%",
                })

            enriched_count += 1
            # Live-update GUI after each enrichment
            if self.on_stocks:
                merged = {}
                for s, dd in current.items():
                    merged[s] = dict(dd)
                    if s in _enrichment:
                        merged[s]['enrich'] = _enrichment[s]
                self.on_stocks(merged)
            if not self.running:
                return

        if enriched_count:
            status += f"  +{enriched_count} enriched"

        # ── Baseline: send single summary to Telegram ──
        if is_baseline and current:
            top5 = sorted(current.items(), key=lambda x: x[1]['pct'], reverse=True)[:5]
            summary_lines = [f"📡 <b>Scanner started</b> — {len(current)} stocks"]
            for sym, d in top5:
                e = _enrichment.get(sym, {})
                flt = e.get('float', '-')
                short = e.get('short', '-')
                summary_lines.append(
                    f"  {sym} ${d['price']:.2f} {d['pct']:+.1f}%  Float:{flt}  Short:{short}"
                )
            if len(current) > 5:
                summary_lines.append(f"  ... +{len(current)-5} more")
            send_telegram("\n".join(summary_lines))

        # ── Anomaly detection (only after baseline) ──
        if not is_baseline and current:
            alerts = detect_anomalies(current, self.previous)
            # Filter: only price/pct jumps (new stocks handled above)
            jump_alerts = [a for a in alerts if a['type'] != 'new']
            if jump_alerts:
                header = f"🔔 <b>Alert</b> — {datetime.now().strftime('%H:%M:%S')}\n"
                lines = []
                for a in jump_alerts:
                    sym = a.get('symbol', '')
                    line = a['msg']
                    mom = stock_history.format_momentum(sym)
                    if mom:
                        line += f"\n   📊 {mom}"
                    lines.append(line)
                    file_logger.log_alert(ts, a)
                send_telegram(header + "\n".join(lines))
                status += f"  🔔{len(jump_alerts)}"
            else:
                status += "  ✓"

        # ── Milestone alerts (+5% steps) ──
        for sym, d in current.items():
            ms_msg = check_milestone(sym, d['pct'])
            if ms_msg:
                send_telegram(ms_msg)
                status += f"  📈{sym}"

        # ── Volume anomaly checks ──
        for sym, d in current.items():
            if sym in _enrichment and d.get('volume_raw', 0) > 0:
                vol_msg = check_volume_anomaly(sym, d['volume_raw'], _enrichment[sym])
                if vol_msg:
                    send_telegram(vol_msg)
                    status += f"  🔥{sym}"

        # ── Merge enrichment into stock data for GUI ──
        merged = {}
        for sym, d in current.items():
            merged[sym] = dict(d)  # copy
            if sym in _enrichment:
                merged[sym]['enrich'] = _enrichment[sym]
        if self.on_stocks and merged:
            self.on_stocks(merged)

        self.previous = current
        if self.on_status:
            self.on_status(status)
        log.info(status)


# ══════════════════════════════════════════════════════════
#  GUI
# ══════════════════════════════════════════════════════════

class App:
    BG = "#1a1a2e"
    FG = "#e0e0e0"
    ACCENT = "#00d4ff"
    GREEN = "#00c853"
    RED = "#ff1744"
    ROW_BG = "#2d2d44"
    ROW_ALT = "#1a1a2e"

    def __init__(self):
        self.scanner = None
        self._stock_data: dict = {}  # current scan results for table display

        self.root = tk.Tk()
        self.root.title("IBKR Scanner Monitor")
        self.root.geometry("680x620")
        self.root.attributes('-topmost', True)
        self.root.configure(bg=self.BG, highlightbackground=self.ACCENT,
                            highlightcolor=self.ACCENT, highlightthickness=3)
        self.root.resizable(False, False)

        # Header
        tk.Label(self.root, text="IBKR SCANNER MONITOR", font=("Helvetica", 18, "bold"),
                 bg=self.BG, fg=self.ACCENT).pack(pady=(10, 0))
        tk.Label(self.root, text="Scanner  |  Anomaly  |  Fib  |  Telegram",
                 font=("Helvetica", 9), bg=self.BG, fg="#888").pack()

        tk.Frame(self.root, bg=self.ACCENT, height=2).pack(fill='x', padx=12, pady=8)

        # Connection status
        self.conn_var = tk.StringVar(value="IBKR: Checking...")
        self.conn_label = tk.Label(self.root, textvariable=self.conn_var,
                                   font=("Courier", 10, "bold"), bg=self.BG, fg="#888")
        self.conn_label.pack(padx=12, anchor='w')

        tk.Frame(self.root, bg="#444", height=1).pack(fill='x', padx=12, pady=4)

        # Stock table header
        tk.Label(self.root, text="Tracked Stocks:", font=("Helvetica", 11, "bold"),
                 bg=self.BG, fg=self.FG).pack(padx=12, anchor='w')

        # Column headers
        hdr_frame = tk.Frame(self.root, bg=self.BG)
        hdr_frame.pack(fill='x', padx=12)
        for text, w in [("SYM", 6), ("PRICE", 8), ("CHG%", 8), ("VOL", 7), ("FLOAT", 9), ("SHORT", 7)]:
            tk.Label(hdr_frame, text=text, font=("Courier", 9, "bold"),
                     bg=self.BG, fg=self.ACCENT, width=w, anchor='w').pack(side='left')

        # Scrollable stock list
        list_frame = tk.Frame(self.root, bg=self.BG)
        list_frame.pack(fill='both', expand=True, padx=12, pady=2)

        self.canvas = tk.Canvas(list_frame, bg=self.BG, highlightthickness=0, height=250)
        scrollbar = tk.Scrollbar(list_frame, orient='vertical', command=self.canvas.yview)
        self.stock_frame = tk.Frame(self.canvas, bg=self.BG)

        self.stock_frame.bind('<Configure>',
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox('all')))
        self.canvas.create_window((0, 0), window=self.stock_frame, anchor='nw')
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        tk.Frame(self.root, bg=self.ACCENT, height=2).pack(fill='x', padx=12, pady=6)

        # Settings row 1: Freq + Alert %
        fs1 = tk.Frame(self.root, bg=self.BG)
        fs1.pack(fill='x', padx=12, pady=2)

        tk.Label(fs1, text="Freq (s):", font=("Helvetica", 10),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.freq = tk.IntVar(value=MONITOR_DEFAULT_FREQ)
        tk.Spinbox(fs1, from_=10, to=600, increment=10, textvariable=self.freq,
                   width=4, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=(2, 15))

        tk.Label(fs1, text="Alert %:", font=("Helvetica", 10),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.thresh = tk.DoubleVar(value=MONITOR_DEFAULT_ALERT_PCT)
        tk.Spinbox(fs1, from_=1, to=50, increment=1, textvariable=self.thresh,
                   width=4, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=2)

        # Settings row 2: Price Min + Max
        fs2 = tk.Frame(self.root, bg=self.BG)
        fs2.pack(fill='x', padx=12, pady=2)

        tk.Label(fs2, text="Price Min:", font=("Helvetica", 10),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.price_min = tk.DoubleVar(value=MONITOR_PRICE_MIN)
        tk.Spinbox(fs2, from_=0.01, to=100, increment=0.5, textvariable=self.price_min,
                   width=6, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat', format="%.2f").pack(side='left', padx=(2, 15))

        tk.Label(fs2, text="Price Max:", font=("Helvetica", 10),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.price_max = tk.DoubleVar(value=MONITOR_PRICE_MAX)
        tk.Spinbox(fs2, from_=1, to=500, increment=1, textvariable=self.price_max,
                   width=6, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat', format="%.2f").pack(side='left', padx=2)

        # Start/Stop
        self.btn = tk.Button(self.root, text="START", font=("Helvetica", 14, "bold"),
                             bg=self.GREEN, fg="white", command=self._toggle,
                             relief='flat', activebackground="#00a844")
        self.btn.pack(fill='x', padx=12, ipady=5, pady=(6, 0))

        # Status
        self.status = tk.StringVar(value="Ready")
        tk.Label(self.root, textvariable=self.status, font=("Courier", 9),
                 bg=self.BG, fg="#888", wraplength=650, justify='left'
                 ).pack(padx=12, pady=6, anchor='w')

        self._load()
        self.root.after(500, self._check_connection)

    def _check_connection(self):
        """Check IBKR connection status (read-only, never creates connection).

        The scanner thread owns the IB connection — creating it from the
        main/tkinter thread would break ib_insync's event loop threading.
        """
        if _ibkr and _ibkr.isConnected():
            self.conn_var.set("IBKR: Connected ✓")
            self.conn_label.config(fg=self.GREEN)
        elif _ibkr:
            self.conn_var.set("IBKR: Disconnected ✗")
            self.conn_label.config(fg=self.RED)
        else:
            self.conn_var.set("IBKR: Not connected")
            self.conn_label.config(fg="#888")
        # Re-check every 5 seconds
        self.root.after(5_000, self._check_connection)

    def _update_stock_table(self, stocks: dict):
        """Update the stock table in the GUI (called from scanner thread)."""
        self._stock_data = stocks
        self.root.after(0, self._render_stock_table)

    def _render_stock_table(self):
        """Render the stock table from self._stock_data.

        Each stock gets two lines:
          Line 1: SYM  $PRICE  +CHG%  VOL  FLOAT  SHORT%
          Line 2:   Fib ⬆ $X.XX  $X.XX  $X.XX  |  Fib ⬇ $X.XX  $X.XX
        """
        for w in self.stock_frame.winfo_children():
            w.destroy()

        if not self._stock_data:
            tk.Label(self.stock_frame, text="No stocks yet",
                     bg=self.BG, fg="#666", font=("Helvetica", 10)).pack(pady=10)
            return

        # Sort by change% descending
        sorted_stocks = sorted(self._stock_data.items(),
                               key=lambda x: x[1]['pct'], reverse=True)

        for i, (sym, d) in enumerate(sorted_stocks):
            bg = self.ROW_BG if i % 2 == 0 else self.ROW_ALT
            enrich = d.get('enrich', {})

            # Check if volume is unusually high
            vol_raw = d.get('volume_raw', 0)
            float_shares = _parse_float_to_shares(enrich.get('float', '-')) if enrich else 0
            turnover = (vol_raw / float_shares * 100) if float_shares > 0 and vol_raw > 0 else 0
            is_hot = turnover >= HIGH_TURNOVER_PCT

            # ── Line 1: SYM  PRICE  CHG%  VOL  FLOAT  SHORT ──
            row1 = tk.Frame(self.stock_frame, bg=bg)
            row1.pack(fill='x', pady=0)

            sym_text = f"🔥{sym}" if is_hot else sym
            tk.Label(row1, text=sym_text, font=("Courier", 10, "bold"),
                     bg=bg, fg="#ff6600" if is_hot else self.FG, width=8, anchor='w').pack(side='left')

            tk.Label(row1, text=f"${d['price']:.2f}", font=("Courier", 10),
                     bg=bg, fg=self.FG, width=8, anchor='w').pack(side='left')

            pct = d['pct']
            pct_color = self.GREEN if pct > 0 else self.RED if pct < 0 else self.FG
            tk.Label(row1, text=f"{pct:+.1f}%", font=("Courier", 10, "bold"),
                     bg=bg, fg=pct_color, width=8, anchor='w').pack(side='left')

            vol_color = "#ff6600" if is_hot else "#aaa"
            vol_text = d.get('volume', '')
            if is_hot:
                vol_text += f" ({turnover:.0f}%)"
            tk.Label(row1, text=vol_text, font=("Courier", 9),
                     bg=bg, fg=vol_color, width=12, anchor='w').pack(side='left')

            # Float + Short from enrichment
            flt = enrich.get('float', '') if enrich else ''
            short = enrich.get('short', '') if enrich else ''
            tk.Label(row1, text=flt, font=("Courier", 9),
                     bg=bg, fg="#cca0ff", width=8, anchor='w').pack(side='left')
            tk.Label(row1, text=short, font=("Courier", 9),
                     bg=bg, fg="#ffaa00", width=7, anchor='w').pack(side='left')

            # ── Line 2: Fib levels (if enriched) ──
            if enrich and (enrich.get('fib_above') or enrich.get('fib_below')):
                row2 = tk.Frame(self.stock_frame, bg=bg)
                row2.pack(fill='x', pady=0)

                fib_parts = []
                above = enrich.get('fib_above', [])
                below = enrich.get('fib_below', [])
                if above:
                    fib_parts.append("⬆" + " ".join(f"${p:.3f}" for p in above))
                if below:
                    fib_parts.append("⬇" + " ".join(f"${p:.3f}" for p in below))

                fib_text = "  📐 " + "  |  ".join(fib_parts)
                tk.Label(row2, text=fib_text, font=("Courier", 8),
                         bg=bg, fg="#66cccc", anchor='w').pack(side='left', padx=(12, 0))

    def _toggle(self):
        if self.scanner and self.scanner.running:
            self.scanner.stop()
            self.scanner = None
            self.btn.config(text="START", bg=self.GREEN)
            self.status.set("Stopped.")
        else:
            global PCT_JUMP_THRESHOLD, PRICE_JUMP_THRESHOLD
            PCT_JUMP_THRESHOLD = self.thresh.get()
            PRICE_JUMP_THRESHOLD = self.thresh.get()
            self._save()
            self.scanner = ScannerThread(
                freq=self.freq.get(),
                price_min=self.price_min.get(),
                price_max=self.price_max.get(),
                on_status=self._st,
                on_stocks=self._update_stock_table,
            )
            self.scanner.start()
            self.btn.config(text="STOP", bg=self.RED)
            self.status.set("Scanner running...")

    def _st(self, msg):
        self.root.after(0, lambda: self.status.set(msg))

    def _save(self):
        with open(STATE_PATH, 'w') as f:
            json.dump({
                'freq': self.freq.get(),
                'thresh': self.thresh.get(),
                'price_min': self.price_min.get(),
                'price_max': self.price_max.get(),
            }, f)

    def _load(self):
        if not STATE_PATH.exists():
            return
        try:
            s = json.load(open(STATE_PATH))
            self.freq.set(s.get('freq', MONITOR_DEFAULT_FREQ))
            self.thresh.set(s.get('thresh', MONITOR_DEFAULT_ALERT_PCT))
            self.price_min.set(s.get('price_min', MONITOR_PRICE_MIN))
            self.price_max.set(s.get('price_max', MONITOR_PRICE_MAX))
        except Exception:
            pass

    def run(self):
        self.root.mainloop()
        if self.scanner:
            self.scanner.stop()


if __name__ == "__main__":
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram not configured — file logging only")
    App().run()
