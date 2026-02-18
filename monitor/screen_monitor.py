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
import queue
import threading
import time
import tkinter as tk
from collections import defaultdict
from datetime import datetime, timedelta
from tkinter import messagebox

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import requests
from ib_insync import IB, Stock, LimitOrder, MarketOrder, StopOrder, ScannerSubscription, util as ib_util
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
    FIB_DT_LIVE_STOP_PCT, FIB_DT_LIVE_TARGET_LEVELS,
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
GROUP_CHAT_ID = os.getenv("TELEGRAM_GROUP_ID", "")
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

# Cache avg volume per symbol so repeat scans only need 2D of data
_avg_vol_cache: dict[str, float] = {}


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

        # Reuse the scanner's already-qualified contract
        stock_contract = contract

        # Known stock with cached avg volume → only 2D needed
        has_cached_avg = sym in _avg_vol_cache
        duration = "2 D" if has_cached_avg else "12 D"

        try:
            bars = ib.reqHistoricalData(
                stock_contract,
                endDateTime="",
                durationStr=duration,
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

            # RVOL: use cached avg or compute from full 12D
            if has_cached_avg:
                avg_vol = _avg_vol_cache[sym]
            elif len(bars) >= 2:
                prev_volumes = [b.volume for b in bars[:-1]]
                avg_vol = sum(prev_volumes) / len(prev_volumes) if prev_volumes else 0
                if avg_vol > 0:
                    _avg_vol_cache[sym] = avg_vol
            else:
                avg_vol = 0

            rvol = round(volume / avg_vol, 1) if avg_vol > 0 else 0.0

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
                "rvol": rvol,
                "float": "",
            }

        except Exception as e:
            log.debug(f"Enrich {sym} failed: {e}")
            continue

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

# Regex to strip IBKR headline metadata like {A:800015:L:en:K:-0.97:C:0.97}
import re
_IBKR_HEADLINE_RE = re.compile(r'\{[^}]*\}\*?\s*')


def _fetch_ibkr_news(symbol: str, max_news: int = 5) -> list[dict]:
    """Fetch recent news headlines from IBKR (Dow Jones, The Fly, Briefing).

    Returns list of {'title_en': str, 'date': str, 'source': str}.
    """
    ib = _get_ibkr()
    if not ib:
        return []
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        providers = 'DJ-N+DJ-RT+FLY+BRFG+BRFUPDN'
        headlines = ib.reqHistoricalNews(contract.conId, providers, '', '', max_news)
        if not headlines:
            return []
        results = []
        for h in headlines:
            # Strip metadata tags from headline
            clean = _IBKR_HEADLINE_RE.sub('', h.headline).strip()
            if not clean:
                continue
            date_str = h.time.strftime('%Y-%m-%d') if h.time else ''
            results.append({
                'title_en': clean,
                'date': date_str,
                'source': h.providerCode or '',
            })
        return results
    except Exception as e:
        log.debug(f"IBKR news {symbol}: {e}")
        return []


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
            'company': fund.get('Company', '-'),
            'country': fund.get('Country', '-'),
            'sector': fund.get('Sector', '-'),
            'industry': fund.get('Industry', '-'),
            'inst_own': fund.get('Inst Own', '-'),
            'inst_trans': fund.get('Inst Trans', '-'),
            'insider_own': fund.get('Insider Own', '-'),
            'insider_trans': fund.get('Insider Trans', '-'),
            'market_cap': fund.get('Market Cap', '-'),
            'vol_w': fund.get('Volatility W', '-'),
            'vol_m': fund.get('Volatility M', '-'),
            '52w_high': fund.get('52W High', '-'),
            '52w_low': fund.get('52W Low', '-'),
            'avg_volume': fund.get('Avg Volume', '-'),
            'volume': fund.get('Volume', '-'),
        }

        news_df = stock.ticker_news()
        titles_en = []
        dates = []
        for _, row in news_df.head(max_news).iterrows():
            title_en = row.get('Title', '')
            if title_en:
                titles_en.append(title_en)
                dates.append(str(row.get('Date', ''))[:10])

        # Batch translate all headlines in one call
        if titles_en:
            try:
                combined = "\n||||\n".join(titles_en)
                translated = _translator.translate(combined)
                titles_he = translated.split("\n||||\n")
            except Exception:
                titles_he = titles_en
            for i, title_he in enumerate(titles_he):
                result['news'].append({
                    'title_he': title_he.strip(),
                    'date': dates[i] if i < len(dates) else '',
                })

    except Exception as e:
        log.error(f"Finviz fetch failed for {symbol}: {e}")

    return result


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
        'company': '-', 'country': '-', 'sector': '-', 'industry': '-',
        'inst_own': '-', 'inst_trans': '-', 'insider_own': '-', 'insider_trans': '-',
        'market_cap': '-', 'vol_w': '-', 'vol_m': '-',
        '52w_high': '-', '52w_low': '-', 'avg_volume': '-', 'fvz_volume': '-',
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
        data['company'] = f.get('company', '-')
        data['country'] = f.get('country', '-')
        data['sector'] = f.get('sector', '-')
        data['industry'] = f.get('industry', '-')
        data['inst_own'] = f.get('inst_own', '-')
        data['inst_trans'] = f.get('inst_trans', '-')
        data['insider_own'] = f.get('insider_own', '-')
        data['insider_trans'] = f.get('insider_trans', '-')
        data['market_cap'] = f.get('market_cap', '-')
        data['vol_w'] = f.get('vol_w', '-')
        data['vol_m'] = f.get('vol_m', '-')
        data['52w_high'] = f.get('52w_high', '-')
        data['52w_low'] = f.get('52w_low', '-')
        data['avg_volume'] = f.get('avg_volume', '-')
        data['fvz_volume'] = f.get('volume', '-')
        data['news'] = info.get('news', [])
    except Exception as e:
        log.error(f"Finviz {sym}: {e}")

    # ── IBKR news (Dow Jones, The Fly) ──
    if on_status:
        on_status(f"Enriching {sym}... (IBKR News)")
    try:
        ibkr_news = _fetch_ibkr_news(sym, max_news=5)
        if ibkr_news:
            # Collect titles for dedup and batch translate
            finviz_titles = {n.get('title_he', '').lower() for n in data['news']}
            new_titles_en = []
            new_dates = []
            new_sources = []
            for n in ibkr_news:
                title_lower = n['title_en'].lower()
                # Skip if very similar to an existing Finviz headline
                if any(title_lower[:30] in ft or ft[:30] in title_lower
                       for ft in finviz_titles if len(ft) > 10):
                    continue
                new_titles_en.append(n['title_en'])
                new_dates.append(n['date'])
                new_sources.append(n['source'])

            if new_titles_en:
                try:
                    combined = "\n||||\n".join(new_titles_en)
                    translated = _translator.translate(combined)
                    titles_he = translated.split("\n||||\n")
                except Exception:
                    titles_he = new_titles_en
                for i, title_he in enumerate(titles_he):
                    src = new_sources[i] if i < len(new_sources) else ''
                    data['news'].append({
                        'title_he': title_he.strip(),
                        'date': new_dates[i] if i < len(new_dates) else '',
                        'source': src,
                    })
            log.info(f"IBKR news {sym}: {len(ibkr_news)} raw → {len(new_titles_en)} new")
    except Exception as e:
        log.debug(f"IBKR news enrich {sym}: {e}")

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


def _calc_ma_table(current_price: float,
                   ma_frames: dict[str, pd.DataFrame | None]) -> list[dict]:
    """Compute SMA & EMA for periods 9/20/50/100/200 across all timeframes.

    ``ma_frames``: {'1m': df, '5m': df, '15m': df, '1h': df,
                    '4h': df, 'D': df, 'W': df}
    Returns list of dicts: {tf, period, sma, ema} with None for unavailable.
    """
    periods = [9, 20, 50, 100, 200]
    tf_order = ['1m', '5m', '15m', '1h', '4h', 'D', 'W']
    rows = []
    for tf in tf_order:
        frame = ma_frames.get(tf)
        if frame is None or len(frame) < 5:
            for p in periods:
                rows.append({'tf': tf, 'period': p, 'sma': None, 'ema': None})
            continue
        close = frame['close']
        for p in periods:
            sma_val = float(close.rolling(p).mean().iloc[-1]) if len(close) >= p else None
            ema_val = float(close.ewm(span=p, adjust=False).mean().iloc[-1]) if len(close) >= p else None
            rows.append({'tf': tf, 'period': p, 'sma': sma_val, 'ema': ema_val})
    return rows


def _render_ma_overlay(ax, ma_rows: list[dict], current_price: float,
                       ma_type: str, x_start: float, y_start: float):
    """Render a compact MA table overlay on the chart axes.

    ``ma_type``: 'sma' or 'ema' — which column to display.
    ``x_start``, ``y_start``: top-left corner in axes coordinates.
    """
    green, red, grey = '#26a69a', '#ef5350', '#555'
    periods = [9, 20, 50, 100, 200]
    tf_order = ['1m', '5m', '15m', '1h', '4h', 'D', 'W']

    ma_lookup = {}
    for r in ma_rows:
        ma_lookup[(r['tf'], r['period'])] = (r['sma'], r['ema'])

    # Background box
    from matplotlib.patches import FancyBboxPatch
    box_w, box_h = 0.30, 0.44
    bg = FancyBboxPatch((x_start - 0.005, y_start - box_h),
                        box_w, box_h,
                        boxstyle="round,pad=0.005",
                        facecolor='#0e1117', edgecolor='#333',
                        alpha=0.88, linewidth=0.5,
                        transform=ax.transAxes, zorder=5)
    ax.add_patch(bg)

    z = 6  # zorder for text (above bg)
    y = y_start - 0.015
    lbl = ma_type.upper()
    ax.text(x_start + box_w / 2, y, lbl, transform=ax.transAxes,
            fontsize=7, fontweight='bold', color='#00d4ff',
            ha='center', va='top', zorder=z)

    # Column headers
    col_offsets = [0.055, 0.105, 0.155, 0.21, 0.265]
    y -= 0.025
    for co, p in zip(col_offsets, periods):
        ax.text(x_start + co, y, str(p), transform=ax.transAxes,
                fontsize=5.5, fontweight='bold', color='#888',
                ha='center', va='top', zorder=z)

    # Data rows
    key = 0 if ma_type == 'sma' else 1
    for tf in tf_order:
        y -= 0.023
        ax.text(x_start + 0.005, y, tf, transform=ax.transAxes,
                fontsize=5.5, fontweight='bold', color='#aaa',
                va='top', zorder=z)
        for co, p in zip(col_offsets, periods):
            val = ma_lookup.get((tf, p), (None, None))[key]
            if val is not None:
                clr = green if current_price >= val else red
                ax.text(x_start + co, y, f'{val:.2f}', transform=ax.transAxes,
                        fontsize=5, color=clr, ha='center', va='top',
                        fontfamily='monospace', zorder=z)
            else:
                ax.text(x_start + co, y, '—', transform=ax.transAxes,
                        fontsize=5, color=grey, ha='center', va='top', zorder=z)


def generate_fib_chart(sym: str, df: pd.DataFrame, all_levels: list[float],
                       current_price: float,
                       ratio_map: dict | None = None,
                       ma_frames: dict | None = None) -> Path | None:
    """Generate a 5-min candlestick chart with Fibonacci levels + MA overlays.

    Two compact MA tables overlaid on chart: SMA top-left, EMA bottom-left.

    Returns path to saved PNG or None on failure.
    """
    try:
        # Crop to last ~200 bars so chart focuses on recent price action
        df = df.tail(200).copy()
        if len(df) < 5:
            return None

        fig, ax = plt.subplots(figsize=(14, 8), facecolor='#0e1117')
        ax.set_facecolor('#0e1117')

        # ── Candlestick chart ──
        x = np.arange(len(df))
        dates = pd.to_datetime(df['date']) if 'date' in df.columns else df.index

        width = 0.6
        for i, (_, row) in enumerate(df.iterrows()):
            o, h, l, c = row['open'], row['high'], row['low'], row['close']
            color = '#26a69a' if c >= o else '#ef5350'
            ax.plot([i, i], [l, h], color=color, linewidth=0.8)
            body_bottom = min(o, c)
            body_height = abs(c - o)
            if body_height < 0.001:
                body_height = 0.001
            ax.bar(i, body_height, bottom=body_bottom, width=width,
                   color=color, edgecolor=color, linewidth=0.5)

        # Filter fib levels — show at least 3 above and 3 below current price
        price_min = df['low'].min()
        price_max = df['high'].max()
        margin = (price_max - price_min) * 0.15

        # Find 3 fib levels below and 3 above current price
        levels_below = sorted([lv for lv in all_levels if lv <= current_price])
        levels_above = sorted([lv for lv in all_levels if lv > current_price])
        target_below = levels_below[-3:] if len(levels_below) >= 3 else levels_below
        target_above = levels_above[:3] if len(levels_above) >= 3 else levels_above

        # Expand Y range to include these fib levels
        vis_min = price_min - margin
        vis_max = price_max + margin
        if target_below:
            vis_min = min(vis_min, min(target_below) - margin * 0.5)
        if target_above:
            vis_max = max(vis_max, max(target_above) + margin * 0.5)

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



def _find_closest_resist(price: float, ma_rows: list[dict]) -> str:
    """Find the closest SMA and EMA resistances above current price.

    Returns a compact string like: "SMA200(1h) $1.39 | EMA9(1h) $1.40"
    """
    closest_sma = None  # (val, tf, period)
    closest_ema = None

    for r in ma_rows:
        tf, period = r['tf'], r['period']
        sma_val, ema_val = r['sma'], r['ema']

        if sma_val is not None and sma_val > price:
            if closest_sma is None or sma_val < closest_sma[0]:
                closest_sma = (sma_val, tf, period)

        if ema_val is not None and ema_val > price:
            if closest_ema is None or ema_val < closest_ema[0]:
                closest_ema = (ema_val, tf, period)

    parts = []
    if closest_sma:
        parts.append(f"SMA{closest_sma[2]}({closest_sma[1]}) ${closest_sma[0]:.2f}")
    if closest_ema:
        parts.append(f"EMA{closest_ema[2]}({closest_ema[1]}) ${closest_ema[0]:.2f}")

    return " | ".join(parts) if parts else ""


def _send_stock_report(sym: str, stock: dict, enriched: dict):
    """Send comprehensive Telegram report for a newly discovered stock."""
    price = stock['price']

    # ── Download MA timeframes first (needed for resist line) ──
    ma_frames: dict[str, pd.DataFrame | None] = {}
    _tf_specs = [
        ('1m',  '1 min',   '2 D'),
        ('5m',  '5 mins',  '5 D'),
        ('15m', '15 mins', '2 W'),
        ('1h',  '1 hour',  '3 M'),
        ('4h',  '4 hours', '1 Y'),
        ('W',   '1 week',  '5 Y'),
    ]
    for tf_key, bar_size, duration in _tf_specs:
        ma_frames[tf_key] = _download_intraday(sym, bar_size=bar_size, duration=duration)
    ma_frames['D'] = _daily_cache.get(sym)

    ma_rows = _calc_ma_table(price, ma_frames)
    resist_str = _find_closest_resist(price, ma_rows)

    # ── EPS indicator ──
    eps = enriched.get('eps', '-')
    try:
        eps_val = float(str(eps).replace(',', ''))
        eps_icon = "🟢" if eps_val > 0 else "🔴"
    except (ValueError, TypeError):
        eps_icon = "⚪"

    # ── Build consolidated message ──
    lines = [
        f"🆕 <b>{sym}</b> — ${price:.2f}  {stock['pct']:+.1f}%  Vol:{stock.get('volume', '-')}",
        "",
        f"🏢 {enriched.get('company', '-')}",
        f"🌎 {enriched.get('country', '-')} | {enriched.get('sector', '-')} | {enriched.get('industry', '-')}",
        f"🏛️ Inst: {enriched.get('inst_own', '-')} ({enriched.get('inst_trans', '-')}) | Insider: {enriched.get('insider_own', '-')} ({enriched.get('insider_trans', '-')})",
        f"💰 MCap: {enriched.get('market_cap', '-')}",
        "",
        f"📊 Float: {enriched['float']} | Short: {enriched['short']}",
        f"💰 {eps_icon} EPS: {eps} | Cash: ${enriched['cash']}",
        f"📅 Earnings: {enriched['earnings']}",
        f"📉 Vol: {enriched.get('fvz_volume', '-')} | Avg: {enriched.get('avg_volume', '-')}",
        f"📊 Volatility: W {enriched.get('vol_w', '-')} | M {enriched.get('vol_m', '-')}",
        f"🎯 52W: ↑${enriched.get('52w_high', '-')} | ↓${enriched.get('52w_low', '-')}",
    ]

    # Resist line
    if resist_str:
        lines.append(f"📉 Resist: {resist_str}")
    else:
        lines.append("✅ אין התנגדויות — מחיר מעל כל הממוצעים")

    # News (Hebrew)
    if enriched['news']:
        lines.append("")
        lines.append(f"📰 <b>חדשות:</b>")
        for n in enriched['news']:
            src = n.get('source', '')
            src_tag = f" [{src}]" if src else ""
            lines.append(f"  • {n['title_he']}  <i>({n['date']}{src_tag})</i>")

    send_telegram("\n".join(lines))

    # ── Fib chart image ──
    cached = _fib_cache.get(sym)
    if not cached:
        return

    all_levels = cached[2]
    ratio_map = cached[3]

    df_5min = ma_frames.get('5m')
    if df_5min is not None:
        img = generate_fib_chart(sym, df_5min, all_levels, price,
                                 ratio_map=ratio_map)
        if img:
            send_telegram_photo(img, f"📐 {sym} — 5min + Fibonacci ${price:.2f}")


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
    """Send text message to personal chat and group (if configured)."""
    if not BOT_TOKEN or not CHAT_ID:
        return False
    ok = False
    for cid in [CHAT_ID, GROUP_CHAT_ID]:
        if not cid:
            continue
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={'chat_id': cid, 'text': text, 'parse_mode': 'HTML'},
                timeout=10,
            )
            if cid == CHAT_ID:
                ok = resp.ok
        except Exception as e:
            log.error(f"Telegram ({cid}): {e}")
    return ok


def send_telegram_photo(image_path: Path, caption: str = "") -> bool:
    """Send a photo to personal chat and group (if configured)."""
    if not BOT_TOKEN or not CHAT_ID:
        return False
    ok = False
    for cid in [CHAT_ID, GROUP_CHAT_ID]:
        if not cid:
            continue
        try:
            with open(image_path, 'rb') as photo:
                resp = requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                    data={'chat_id': cid, 'caption': caption, 'parse_mode': 'HTML'},
                    files={'photo': photo},
                    timeout=30,
                )
            if cid == CHAT_ID:
                ok = resp.ok
        except Exception as e:
            log.error(f"Telegram photo ({cid}): {e}")
    return ok


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
                 on_status=None, on_stocks=None,
                 order_queue: queue.Queue | None = None,
                 on_account=None, on_order_result=None):
        super().__init__(daemon=True)
        self.freq = freq
        self.price_min = price_min
        self.price_max = price_max
        self.on_status = on_status
        self.on_stocks = on_stocks  # callback(dict) to update GUI table
        self.order_queue = order_queue
        self.on_account = on_account          # callback(net_liq, buying_power, positions)
        self.on_order_result = on_order_result  # callback(msg, success)
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
                self._process_order_queue()
                time.sleep(1)

    def _process_order_queue(self):
        """Check and execute pending orders from the GUI thread."""
        if not self.order_queue:
            return
        while not self.order_queue.empty():
            try:
                req = self.order_queue.get_nowait()
                self._execute_order(req)
            except queue.Empty:
                break

    def _execute_order(self, req: dict):
        """Place an order via IBKR.

        Standard: req = {sym, action, qty, price}
        Fib DT:   req = {sym, action, qty, price, strategy='fib_dt',
                         stop_price, target_price, half, other_half}
        """
        sym = req['sym']
        action = req['action']  # 'BUY' or 'SELL'
        qty = req['qty']
        price = req['price']

        ib = _get_ibkr()
        if not ib:
            if self.on_order_result:
                self.on_order_result("IBKR not connected", False)
            return

        if req.get('strategy') == 'fib_dt':
            self._execute_fib_dt_order(ib, req)
            return

        try:
            contract = Stock(sym, 'SMART', 'USD')
            ib.qualifyContracts(contract)
            order = LimitOrder(action, qty, price)
            order.outsideRth = True
            order.tif = 'DAY'
            trade = ib.placeOrder(contract, order)
            msg = f"{action} {qty} {sym} @ ${price:.2f} — {trade.orderStatus.status}"
            log.info(f"Order placed: {msg}")
            if self.on_order_result:
                self.on_order_result(msg, True)
            send_telegram(
                f"📋 <b>Order Placed</b>\n"
                f"  {action} {qty} {sym} @ ${price:.2f}\n"
                f"  Status: {trade.orderStatus.status}\n"
                f"  outsideRth: ✓  |  TIF: DAY"
            )
        except Exception as e:
            msg = f"Order failed: {action} {qty} {sym} — {e}"
            log.error(msg)
            if self.on_order_result:
                self.on_order_result(msg, False)

    def _execute_fib_dt_order(self, ib: IB, req: dict):
        """Execute Fib Double-Touch split-exit bracket order."""
        sym = req['sym']
        qty = req['qty']
        stop_price = req['stop_price']
        target_price = req['target_price']
        half = req['half']
        other_half = req['other_half']

        try:
            contract = Stock(sym, 'SMART', 'USD')
            ib.qualifyContracts(contract)

            # 1. Market buy full qty
            buy_order = MarketOrder('BUY', qty)
            buy_order.outsideRth = True
            buy_trade = ib.placeOrder(contract, buy_order)
            log.info(f"FIB DT: Market BUY {qty} {sym} — {buy_trade.orderStatus.status}")

            # 2. OCA bracket for first half
            oca_group = f"FibDT_{sym}_{int(time.time())}"

            oca_stop = StopOrder('SELL', half, stop_price)
            oca_stop.outsideRth = True
            oca_stop.ocaGroup = oca_group
            oca_stop.ocaType = 1  # cancel others on fill
            oca_stop.tif = 'GTC'
            ib.placeOrder(contract, oca_stop)

            oca_target = LimitOrder('SELL', half, target_price)
            oca_target.outsideRth = True
            oca_target.ocaGroup = oca_group
            oca_target.ocaType = 1
            oca_target.tif = 'GTC'
            ib.placeOrder(contract, oca_target)

            # 3. Standalone stop for other half
            solo_stop = StopOrder('SELL', other_half, stop_price)
            solo_stop.outsideRth = True
            solo_stop.tif = 'GTC'
            ib.placeOrder(contract, solo_stop)

            msg = (f"FIB DT: BUY {qty} {sym} | "
                   f"OCA {half}sh stop ${stop_price:.2f}/target ${target_price:.2f} | "
                   f"Solo stop {other_half}sh ${stop_price:.2f}")
            log.info(msg)
            if self.on_order_result:
                self.on_order_result(msg, True)
            send_telegram(
                f"📐 <b>FIB DT Order</b>\n"
                f"  Market BUY {qty} {sym}\n"
                f"  OCA ({half}sh): stop ${stop_price:.2f} / target ${target_price:.2f}\n"
                f"  Standalone stop ({other_half}sh): ${stop_price:.2f}\n"
                f"  outsideRth: ✓  |  TIF: GTC"
            )
        except Exception as e:
            msg = f"FIB DT failed: {sym} — {e}"
            log.error(msg)
            if self.on_order_result:
                self.on_order_result(msg, False)

    def _fetch_account_data(self):
        """Fetch account values and positions from IBKR."""
        ib = _get_ibkr()
        if not ib or not self.on_account:
            return
        try:
            acct_vals = ib.accountValues()
            net_liq = 0.0
            buying_power = 0.0
            for av in acct_vals:
                if av.tag == 'NetLiquidation' and av.currency == 'USD':
                    net_liq = float(av.value)
                elif av.tag == 'BuyingPower' and av.currency == 'USD':
                    buying_power = float(av.value)

            positions = {}
            # Use ib.portfolio() for extended data (marketPrice, unrealizedPNL)
            for item in ib.portfolio():
                if item.position == 0:
                    continue  # skip closed positions
                s = item.contract.symbol
                positions[s] = (
                    int(item.position),
                    round(item.averageCost, 4),
                    round(item.marketPrice, 4),
                    round(item.unrealizedPNL, 2),
                )

            self.on_account(net_liq, buying_power, positions)
        except Exception as e:
            log.debug(f"Account fetch: {e}")

    @staticmethod
    def _merge_stocks(current: dict) -> dict:
        """Merge scan data with cached enrichment for GUI display."""
        merged = {}
        for sym, d in current.items():
            merged[sym] = dict(d)
            if sym in _enrichment:
                merged[sym]['enrich'] = _enrichment[sym]
        return merged

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
        merged = self._merge_stocks(current)
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
                self.on_stocks(self._merge_stocks(current))
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

        # ── Final GUI update with all enrichment ──
        merged = self._merge_stocks(current)
        if self.on_stocks and merged:
            self.on_stocks(merged)

        self.previous = current
        if self.on_status:
            self.on_status(status)
        log.info(status)

        # Fetch account data at end of each cycle
        self._fetch_account_data()


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

    SELECTED_BG = "#3d3d5c"

    def __init__(self):
        self.scanner = None
        self._stock_data: dict = {}  # current scan results for table display
        self._order_queue: queue.Queue = queue.Queue()
        self._selected_symbol_name: str | None = None
        self._cached_net_liq: float = 0.0
        self._cached_buying_power: float = 0.0
        self._cached_positions: dict[str, tuple] = {}  # {sym: (qty, avgCost, mktPrice, pnl)}
        self._row_widgets: dict[str, dict] = {}   # sym → cached label widgets
        self._rendered_order: list[str] = []       # last symbol render order
        self._portfolio_widgets: dict[str, dict] = {}  # sym → cached portfolio widgets
        self._portfolio_order: list[str] = []

        self.root = tk.Tk()
        self.root.title("IBKR Scanner Monitor")
        self.root.geometry("1400x900")
        self.root.attributes('-topmost', True)
        self.root.configure(bg=self.BG, highlightbackground=self.ACCENT,
                            highlightcolor=self.ACCENT, highlightthickness=3)
        self.root.resizable(True, True)

        # Header
        tk.Label(self.root, text="IBKR SCANNER MONITOR", font=("Helvetica", 36, "bold"),
                 bg=self.BG, fg=self.ACCENT).pack(pady=(10, 0))
        tk.Label(self.root, text="Scanner  |  Anomaly  |  Fib  |  Telegram",
                 font=("Helvetica", 18), bg=self.BG, fg="#888").pack()

        tk.Frame(self.root, bg=self.ACCENT, height=2).pack(fill='x', padx=12, pady=8)

        # Connection status
        self.conn_var = tk.StringVar(value="IBKR: Checking...")
        self.conn_label = tk.Label(self.root, textvariable=self.conn_var,
                                   font=("Courier", 20, "bold"), bg=self.BG, fg="#888")
        self.conn_label.pack(padx=12, anchor='w')

        tk.Frame(self.root, bg="#444", height=1).pack(fill='x', padx=12, pady=4)

        # Stock table header
        tk.Label(self.root, text="Tracked Stocks:", font=("Helvetica", 22, "bold"),
                 bg=self.BG, fg=self.FG).pack(padx=12, anchor='w')

        # Column headers
        hdr_frame = tk.Frame(self.root, bg=self.BG)
        hdr_frame.pack(fill='x', padx=12)
        for text, w in [("SYM", 6), ("PRICE", 8), ("CHG%", 8), ("VOL", 7), ("RVOL", 6), ("FLOAT", 9), ("SHORT", 7)]:
            tk.Label(hdr_frame, text=text, font=("Courier", 18, "bold"),
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

        # ── Portfolio Panel ──
        tk.Frame(self.root, bg=self.ACCENT, height=2).pack(fill='x', padx=12, pady=4)
        tk.Label(self.root, text="Portfolio:", font=("Helvetica", 20, "bold"),
                 bg=self.BG, fg=self.FG).pack(padx=12, anchor='w')
        # Portfolio column headers
        port_hdr = tk.Frame(self.root, bg=self.BG)
        port_hdr.pack(fill='x', padx=12)
        for text, w in [("SYM", 6), ("QTY", 6), ("AVG", 8), ("PRICE", 8), ("P&L", 10), ("P&L%", 7)]:
            tk.Label(port_hdr, text=text, font=("Courier", 16, "bold"),
                     bg=self.BG, fg=self.ACCENT, width=w, anchor='w').pack(side='left')
        self._portfolio_frame = tk.Frame(self.root, bg=self.BG)
        self._portfolio_frame.pack(fill='x', padx=12, pady=2)
        tk.Frame(self.root, bg=self.ACCENT, height=2).pack(fill='x', padx=12, pady=4)

        # ── Trading Panel ──
        self._build_trading_panel()

        tk.Frame(self.root, bg=self.ACCENT, height=2).pack(fill='x', padx=12, pady=6)

        # Settings row 1: Freq + Alert %
        fs1 = tk.Frame(self.root, bg=self.BG)
        fs1.pack(fill='x', padx=12, pady=2)

        tk.Label(fs1, text="Freq (s):", font=("Helvetica", 20),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.freq = tk.IntVar(value=MONITOR_DEFAULT_FREQ)
        tk.Spinbox(fs1, from_=10, to=600, increment=10, textvariable=self.freq,
                   width=4, font=("Helvetica", 20), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=(2, 15))

        tk.Label(fs1, text="Alert %:", font=("Helvetica", 20),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.thresh = tk.DoubleVar(value=MONITOR_DEFAULT_ALERT_PCT)
        tk.Spinbox(fs1, from_=1, to=50, increment=1, textvariable=self.thresh,
                   width=4, font=("Helvetica", 20), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=2)

        # Settings row 2: Price Min + Max
        fs2 = tk.Frame(self.root, bg=self.BG)
        fs2.pack(fill='x', padx=12, pady=2)

        tk.Label(fs2, text="Price Min:", font=("Helvetica", 20),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.price_min = tk.DoubleVar(value=MONITOR_PRICE_MIN)
        tk.Spinbox(fs2, from_=0.01, to=100, increment=0.5, textvariable=self.price_min,
                   width=6, font=("Helvetica", 20), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat', format="%.2f").pack(side='left', padx=(2, 15))

        tk.Label(fs2, text="Price Max:", font=("Helvetica", 20),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.price_max = tk.DoubleVar(value=MONITOR_PRICE_MAX)
        tk.Spinbox(fs2, from_=1, to=500, increment=1, textvariable=self.price_max,
                   width=6, font=("Helvetica", 20), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat', format="%.2f").pack(side='left', padx=2)

        # Settings row 3: Window size preset
        self._size_presets = {
            "Small (1100x700)": "1100x700",
            "Medium (1400x900)": "1400x900",
            "Large (1800x1050)": "1800x1050",
        }
        fs3 = tk.Frame(self.root, bg=self.BG)
        fs3.pack(fill='x', padx=12, pady=2)

        tk.Label(fs3, text="Size:", font=("Helvetica", 20),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.size_var = tk.StringVar(value="Medium (1400x900)")
        size_menu = tk.OptionMenu(fs3, self.size_var, *self._size_presets.keys(),
                                  command=self._apply_size)
        size_menu.config(font=("Helvetica", 18), bg=self.ROW_BG, fg=self.FG,
                         activebackground=self.ROW_BG, activeforeground=self.FG,
                         highlightthickness=0, relief='flat')
        size_menu["menu"].config(bg=self.ROW_BG, fg=self.FG,
                                 activebackground=self.ACCENT, activeforeground="white")
        size_menu.pack(side='left', padx=2)

        # Start/Stop
        self.btn = tk.Button(self.root, text="START", font=("Helvetica", 28, "bold"),
                             bg=self.GREEN, fg="white", command=self._toggle,
                             relief='flat', activebackground="#00a844")
        self.btn.pack(fill='x', padx=12, ipady=5, pady=(6, 0))

        # Status
        self.status = tk.StringVar(value="Ready")
        tk.Label(self.root, textvariable=self.status, font=("Courier", 18),
                 bg=self.BG, fg="#888", wraplength=1300, justify='left'
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

    def _compute_row_data(self, sym: str, d: dict, idx: int) -> dict:
        """Compute display values for a stock row."""
        is_selected = (sym == self._selected_symbol_name)
        bg = self.SELECTED_BG if is_selected else (self.ROW_BG if idx % 2 == 0 else self.ROW_ALT)
        enrich = d.get('enrich', {})

        vol_raw = d.get('volume_raw', 0)
        float_shares = _parse_float_to_shares(enrich.get('float', '-')) if enrich else 0
        turnover = (vol_raw / float_shares * 100) if float_shares > 0 and vol_raw > 0 else 0
        is_hot = turnover >= HIGH_TURNOVER_PCT

        sym_text = f"🔥{sym}" if is_hot else sym
        sym_fg = "#ff6600" if is_hot else self.FG

        pct = d['pct']
        pct_color = self.GREEN if pct > 0 else self.RED if pct < 0 else self.FG

        vol_text = d.get('volume', '')
        if is_hot:
            vol_text += f" ({turnover:.0f}%)"
        vol_color = "#ff6600" if is_hot else "#aaa"

        rvol = d.get('rvol', 0)
        if rvol > 0:
            rvol_text = f"{rvol:.1f}x"
            rvol_color = "#ff4444" if rvol >= 5 else "#ffcc00" if rvol >= 2 else "#888"
        else:
            rvol_text = "—"
            rvol_color = "#555"

        flt = enrich.get('float', '') if enrich else ''
        short = enrich.get('short', '') if enrich else ''

        # Fib text
        fib_text = ""
        if enrich and (enrich.get('fib_above') or enrich.get('fib_below')):
            parts = []
            above = enrich.get('fib_above', [])
            below = enrich.get('fib_below', [])
            if above:
                parts.append("⬆" + " ".join(f"${p:.3f}" for p in above))
            if below:
                parts.append("⬇" + " ".join(f"${p:.3f}" for p in below))
            fib_text = "  📐 " + "  |  ".join(parts)

        return {
            'bg': bg, 'sym_text': sym_text, 'sym_fg': sym_fg,
            'price_text': f"${d['price']:.2f}",
            'pct_text': f"{pct:+.1f}%", 'pct_fg': pct_color,
            'vol_text': vol_text, 'vol_fg': vol_color,
            'rvol_text': rvol_text, 'rvol_fg': rvol_color,
            'float_text': flt, 'short_text': short,
            'fib_text': fib_text,
        }

    def _build_stock_row(self, sym: str, rd: dict) -> dict:
        """Create widget row for a stock and return widget refs."""
        _click = lambda e, s=sym: self._select_stock(s)

        row1 = tk.Frame(self.stock_frame, bg=rd['bg'])
        row1.pack(fill='x', pady=0)
        row1.bind('<Button-1>', _click)

        sym_lbl = tk.Label(row1, text=rd['sym_text'], font=("Courier", 20, "bold"),
                           bg=rd['bg'], fg=rd['sym_fg'], width=8, anchor='w')
        sym_lbl.pack(side='left'); sym_lbl.bind('<Button-1>', _click)

        price_lbl = tk.Label(row1, text=rd['price_text'], font=("Courier", 20),
                             bg=rd['bg'], fg=self.FG, width=8, anchor='w')
        price_lbl.pack(side='left'); price_lbl.bind('<Button-1>', _click)

        pct_lbl = tk.Label(row1, text=rd['pct_text'], font=("Courier", 20, "bold"),
                           bg=rd['bg'], fg=rd['pct_fg'], width=8, anchor='w')
        pct_lbl.pack(side='left'); pct_lbl.bind('<Button-1>', _click)

        vol_lbl = tk.Label(row1, text=rd['vol_text'], font=("Courier", 18),
                           bg=rd['bg'], fg=rd['vol_fg'], width=12, anchor='w')
        vol_lbl.pack(side='left'); vol_lbl.bind('<Button-1>', _click)

        rvol_lbl = tk.Label(row1, text=rd['rvol_text'], font=("Courier", 18, "bold"),
                            bg=rd['bg'], fg=rd['rvol_fg'], width=6, anchor='w')
        rvol_lbl.pack(side='left'); rvol_lbl.bind('<Button-1>', _click)

        float_lbl = tk.Label(row1, text=rd['float_text'], font=("Courier", 18),
                             bg=rd['bg'], fg="#cca0ff", width=8, anchor='w')
        float_lbl.pack(side='left'); float_lbl.bind('<Button-1>', _click)

        short_lbl = tk.Label(row1, text=rd['short_text'], font=("Courier", 18),
                             bg=rd['bg'], fg="#ffaa00", width=7, anchor='w')
        short_lbl.pack(side='left'); short_lbl.bind('<Button-1>', _click)

        # Fib row
        row2 = tk.Frame(self.stock_frame, bg=rd['bg'])
        row2.pack(fill='x', pady=0)
        fib_lbl = tk.Label(row2, text=rd['fib_text'], font=("Courier", 16),
                           bg=rd['bg'], fg="#66cccc", anchor='w')
        fib_lbl.pack(side='left', padx=(12, 0))
        if not rd['fib_text']:
            row2.pack_forget()

        return {
            'row1': row1, 'row2': row2,
            'sym_lbl': sym_lbl, 'price_lbl': price_lbl, 'pct_lbl': pct_lbl,
            'vol_lbl': vol_lbl, 'rvol_lbl': rvol_lbl,
            'float_lbl': float_lbl, 'short_lbl': short_lbl,
            'fib_lbl': fib_lbl,
        }

    def _update_stock_row(self, widgets: dict, rd: dict):
        """In-place update of an existing stock row's labels."""
        bg = rd['bg']
        widgets['row1'].config(bg=bg)
        widgets['sym_lbl'].config(text=rd['sym_text'], fg=rd['sym_fg'], bg=bg)
        widgets['price_lbl'].config(text=rd['price_text'], bg=bg)
        widgets['pct_lbl'].config(text=rd['pct_text'], fg=rd['pct_fg'], bg=bg)
        widgets['vol_lbl'].config(text=rd['vol_text'], fg=rd['vol_fg'], bg=bg)
        widgets['rvol_lbl'].config(text=rd['rvol_text'], fg=rd['rvol_fg'], bg=bg)
        widgets['float_lbl'].config(text=rd['float_text'], bg=bg)
        widgets['short_lbl'].config(text=rd['short_text'], bg=bg)
        # Fib row
        if rd['fib_text']:
            widgets['fib_lbl'].config(text=rd['fib_text'], bg=bg)
            widgets['row2'].config(bg=bg)
            widgets['row2'].pack(fill='x', pady=0)
        else:
            widgets['row2'].pack_forget()

    def _render_stock_table(self):
        """Render the stock table from self._stock_data.

        Uses in-place label updates when sort order is unchanged (fast path)
        to prevent flicker. Full rebuild only when symbol order changes.
        """
        if not self._stock_data:
            for w in self.stock_frame.winfo_children():
                w.destroy()
            self._row_widgets.clear()
            self._rendered_order.clear()
            tk.Label(self.stock_frame, text="No stocks yet",
                     bg=self.BG, fg="#666", font=("Helvetica", 20)).pack(pady=10)
            return

        sorted_stocks = sorted(self._stock_data.items(),
                               key=lambda x: x[1]['pct'], reverse=True)
        new_order = [sym for sym, _ in sorted_stocks]

        if new_order == self._rendered_order:
            # ── Fast path: in-place update ──
            for i, (sym, d) in enumerate(sorted_stocks):
                rd = self._compute_row_data(sym, d, i)
                if sym in self._row_widgets:
                    self._update_stock_row(self._row_widgets[sym], rd)
        else:
            # ── Full rebuild ──
            for w in self.stock_frame.winfo_children():
                w.destroy()
            self._row_widgets.clear()
            self._rendered_order = new_order

            for i, (sym, d) in enumerate(sorted_stocks):
                rd = self._compute_row_data(sym, d, i)
                self._row_widgets[sym] = self._build_stock_row(sym, rd)

    # ── Portfolio Panel ─────────────────────────────────────

    def _portfolio_row_data(self, sym: str, pos: tuple, idx: int) -> dict:
        """Compute display values for a portfolio row."""
        qty, avg = pos[0], pos[1]
        mkt_price = pos[2] if len(pos) >= 3 else 0.0
        pnl = pos[3] if len(pos) >= 4 else 0.0
        cost = abs(qty * avg) if avg > 0 else 1
        pnl_pct = (pnl / cost * 100) if cost > 0 else 0.0
        is_selected = (sym == self._selected_symbol_name)
        bg = self.SELECTED_BG if is_selected else (self.ROW_BG if idx % 2 == 0 else self.ROW_ALT)
        pnl_fg = self.GREEN if pnl >= 0 else self.RED
        return {
            'qty': qty, 'avg': avg, 'mkt_price': mkt_price,
            'pnl': pnl, 'pnl_pct': pnl_pct, 'bg': bg, 'pnl_fg': pnl_fg,
        }

    def _render_portfolio(self):
        """Render portfolio positions with P&L. Uses in-place updates."""
        positions = self._cached_positions
        if not positions:
            for w in self._portfolio_frame.winfo_children():
                w.destroy()
            self._portfolio_widgets.clear()
            self._portfolio_order.clear()
            return

        new_order = sorted(positions.keys())

        if new_order == self._portfolio_order:
            # Fast path — in-place update
            for i, sym in enumerate(new_order):
                rd = self._portfolio_row_data(sym, positions[sym], i)
                w = self._portfolio_widgets.get(sym)
                if not w:
                    continue
                w['row'].config(bg=rd['bg'])
                w['sym_lbl'].config(bg=rd['bg'])
                w['qty_lbl'].config(text=str(rd['qty']), bg=rd['bg'])
                w['avg_lbl'].config(text=f"${rd['avg']:.2f}", bg=rd['bg'])
                w['price_lbl'].config(text=f"${rd['mkt_price']:.2f}", bg=rd['bg'])
                w['pnl_lbl'].config(text=f"${rd['pnl']:+,.2f}", fg=rd['pnl_fg'], bg=rd['bg'])
                w['pnl_pct_lbl'].config(text=f"{rd['pnl_pct']:+.1f}%", fg=rd['pnl_fg'], bg=rd['bg'])
        else:
            # Full rebuild
            for w in self._portfolio_frame.winfo_children():
                w.destroy()
            self._portfolio_widgets.clear()
            self._portfolio_order = new_order

            for i, sym in enumerate(new_order):
                rd = self._portfolio_row_data(sym, positions[sym], i)
                _click = lambda e, s=sym: self._select_stock(s)

                row = tk.Frame(self._portfolio_frame, bg=rd['bg'])
                row.pack(fill='x', pady=0)
                row.bind('<Button-1>', _click)

                sym_lbl = tk.Label(row, text=sym, font=("Courier", 18, "bold"),
                                   bg=rd['bg'], fg=self.FG, width=6, anchor='w')
                sym_lbl.pack(side='left'); sym_lbl.bind('<Button-1>', _click)

                qty_lbl = tk.Label(row, text=str(rd['qty']), font=("Courier", 18),
                                   bg=rd['bg'], fg=self.FG, width=6, anchor='w')
                qty_lbl.pack(side='left'); qty_lbl.bind('<Button-1>', _click)

                avg_lbl = tk.Label(row, text=f"${rd['avg']:.2f}", font=("Courier", 18),
                                   bg=rd['bg'], fg="#aaa", width=8, anchor='w')
                avg_lbl.pack(side='left'); avg_lbl.bind('<Button-1>', _click)

                price_lbl = tk.Label(row, text=f"${rd['mkt_price']:.2f}", font=("Courier", 18),
                                     bg=rd['bg'], fg=self.FG, width=8, anchor='w')
                price_lbl.pack(side='left'); price_lbl.bind('<Button-1>', _click)

                pnl_lbl = tk.Label(row, text=f"${rd['pnl']:+,.2f}", font=("Courier", 18, "bold"),
                                   bg=rd['bg'], fg=rd['pnl_fg'], width=10, anchor='w')
                pnl_lbl.pack(side='left'); pnl_lbl.bind('<Button-1>', _click)

                pnl_pct_lbl = tk.Label(row, text=f"{rd['pnl_pct']:+.1f}%", font=("Courier", 18, "bold"),
                                       bg=rd['bg'], fg=rd['pnl_fg'], width=7, anchor='w')
                pnl_pct_lbl.pack(side='left'); pnl_pct_lbl.bind('<Button-1>', _click)

                self._portfolio_widgets[sym] = {
                    'row': row, 'sym_lbl': sym_lbl, 'qty_lbl': qty_lbl,
                    'avg_lbl': avg_lbl, 'price_lbl': price_lbl,
                    'pnl_lbl': pnl_lbl, 'pnl_pct_lbl': pnl_pct_lbl,
                }

    # ── Trading Panel ──────────────────────────────────────

    def _build_trading_panel(self):
        """Build the trading panel UI between stock table and settings."""
        panel = tk.Frame(self.root, bg=self.BG)
        panel.pack(fill='x', padx=12, pady=2)

        # Row 1: Selected symbol + price + position + account
        row1 = tk.Frame(panel, bg=self.BG)
        row1.pack(fill='x', pady=2)

        tk.Label(row1, text="Selected:", font=("Helvetica", 18),
                 bg=self.BG, fg="#888").pack(side='left')
        self._selected_sym = tk.StringVar(value="---")
        tk.Label(row1, textvariable=self._selected_sym,
                 font=("Courier", 20, "bold"), bg=self.BG, fg=self.ACCENT
                 ).pack(side='left', padx=(4, 16))

        tk.Label(row1, text="Price:", font=("Helvetica", 18),
                 bg=self.BG, fg="#888").pack(side='left')
        self._trade_price = tk.StringVar(value="")
        tk.Entry(row1, textvariable=self._trade_price, font=("Courier", 20),
                 bg=self.ROW_BG, fg=self.FG, insertbackground=self.FG,
                 width=10, relief='flat').pack(side='left', padx=(4, 16))

        self._position_var = tk.StringVar(value="Position: ---")
        tk.Label(row1, textvariable=self._position_var,
                 font=("Courier", 18), bg=self.BG, fg="#cca0ff"
                 ).pack(side='left', padx=(0, 16))

        self._account_var = tk.StringVar(value="Account: ---")
        tk.Label(row1, textvariable=self._account_var,
                 font=("Courier", 18), bg=self.BG, fg="#aaa"
                 ).pack(side='left')

        # Row 2: BUY + SELL buttons
        row2 = tk.Frame(panel, bg=self.BG)
        row2.pack(fill='x', pady=4)

        for pct in [25, 50, 75, 100]:
            tk.Button(row2, text=f"BUY {pct}%", font=("Helvetica", 18, "bold"),
                      bg="#1b5e20", fg="white", activebackground="#2e7d32",
                      relief='flat', width=8,
                      command=lambda p=pct: self._place_order('BUY', p / 100)
                      ).pack(side='left', padx=2)

        tk.Label(row2, text=" ", bg=self.BG).pack(side='left')

        tk.Button(row2, text="FIB DT", font=("Helvetica", 18, "bold"),
                  bg="#00838f", fg="white", activebackground="#00acc1",
                  relief='flat', width=8,
                  command=self._place_fib_dt_order).pack(side='left', padx=2)

        tk.Label(row2, text=" ", bg=self.BG).pack(side='left')

        for pct in [25, 50, 75, 100]:
            tk.Button(row2, text=f"SELL {pct}%", font=("Helvetica", 18, "bold"),
                      bg="#b71c1c", fg="white", activebackground="#c62828",
                      relief='flat', width=8,
                      command=lambda p=pct: self._place_order('SELL', p / 100)
                      ).pack(side='left', padx=2)

        # Row 3: Order status
        row3 = tk.Frame(panel, bg=self.BG)
        row3.pack(fill='x', pady=2)

        self._order_status_var = tk.StringVar(value="")
        self._order_status_label = tk.Label(
            row3, textvariable=self._order_status_var,
            font=("Courier", 18), bg=self.BG, fg="#888", anchor='w')
        self._order_status_label.pack(side='left')

    def _select_stock(self, sym: str):
        """Handle stock row click — populate trading panel fields."""
        self._selected_symbol_name = sym
        self._selected_sym.set(sym)
        # Look up price from scanner data first, then portfolio
        d = self._stock_data.get(sym)
        if d:
            self._trade_price.set(f"{d['price']:.2f}")
        elif sym in self._cached_positions and len(self._cached_positions[sym]) >= 3:
            self._trade_price.set(f"{self._cached_positions[sym][2]:.2f}")
        self._update_position_display()
        # Re-render table to update highlight
        self._render_stock_table()
        self._render_portfolio()

    def _update_position_display(self):
        """Update position label for the currently selected symbol."""
        sym = self._selected_symbol_name
        if not sym or sym not in self._cached_positions:
            self._position_var.set("Position: ---")
            return
        pos = self._cached_positions[sym]
        qty, avg = pos[0], pos[1]
        if len(pos) >= 4:
            pnl = pos[3]
            self._position_var.set(f"Position: {qty} @ ${avg:.2f}  P&L: ${pnl:+,.2f}")
        else:
            self._position_var.set(f"Position: {qty} @ ${avg:.2f}")

    def _update_account_display(self):
        """Update account label with cached values."""
        nl = self._cached_net_liq
        bp = self._cached_buying_power
        if nl > 0 or bp > 0:
            self._account_var.set(f"Account: ${nl:,.0f} | BP: ${bp:,.0f}")
        else:
            self._account_var.set("Account: ---")

    def _on_account_data(self, net_liq: float, buying_power: float,
                         positions: dict[str, tuple]):
        """Callback from ScannerThread with account data."""
        self._cached_net_liq = net_liq
        self._cached_buying_power = buying_power
        self._cached_positions = positions
        def _refresh():
            self._update_account_display()
            self._update_position_display()
            self._render_portfolio()
        self.root.after(0, _refresh)

    def _on_order_result(self, msg: str, success: bool):
        """Callback from ScannerThread with order result."""
        def _update():
            self._order_status_var.set(msg)
            self._order_status_label.config(fg=self.GREEN if success else self.RED)
        self.root.after(0, _update)

    def _place_order(self, action: str, pct: float):
        """Validate and queue a BUY or SELL order."""
        sym = self._selected_symbol_name
        if not sym or sym == "---":
            messagebox.showwarning("No Stock", "Select a stock first.", parent=self.root)
            return

        try:
            price = float(self._trade_price.get())
            if price <= 0:
                raise ValueError
        except (ValueError, TypeError):
            messagebox.showwarning("Invalid Price", "Enter a valid price.", parent=self.root)
            return

        if not self.scanner or not self.scanner.running:
            messagebox.showwarning("Scanner Off", "Start the scanner first.", parent=self.root)
            return

        if action == 'BUY':
            bp = self._cached_buying_power
            if bp <= 0:
                messagebox.showwarning("No Data", "Waiting for account data...", parent=self.root)
                return
            qty = int(bp * pct / price)
            if qty <= 0:
                messagebox.showwarning("Qty Too Low",
                                       f"Not enough buying power for {pct*100:.0f}%.",
                                       parent=self.root)
                return
        else:  # SELL
            pos = self._cached_positions.get(sym)
            if not pos or pos[0] == 0:
                messagebox.showwarning("No Position", f"No position in {sym}.", parent=self.root)
                return
            # abs() handles both long (positive) and short (negative) positions
            qty = max(1, int(abs(pos[0]) * pct))

        # Confirmation dialog
        confirm = messagebox.askokcancel(
            "Confirm Order",
            f"{action} {qty} {sym} @ ${price:.2f}\n"
            f"Total: ${qty * price:,.2f}\n\n"
            f"outsideRth=True (pre/post market OK)\n"
            f"Continue?",
            parent=self.root,
        )
        if not confirm:
            return

        self._order_queue.put({
            'sym': sym, 'action': action, 'qty': qty, 'price': price,
        })
        self._order_status_var.set(f"Queued: {action} {qty} {sym} @ ${price:.2f}...")
        self._order_status_label.config(fg="#ffcc00")

    def _place_fib_dt_order(self):
        """Validate and queue a Fib Double-Touch split-exit bracket order."""
        sym = self._selected_symbol_name
        if not sym or sym == "---":
            messagebox.showwarning("No Stock", "Select a stock first.", parent=self.root)
            return

        try:
            entry_price = float(self._trade_price.get())
            if entry_price <= 0:
                raise ValueError
        except (ValueError, TypeError):
            messagebox.showwarning("Invalid Price", "Enter a valid price.", parent=self.root)
            return

        if not self.scanner or not self.scanner.running:
            messagebox.showwarning("Scanner Off", "Start the scanner first.", parent=self.root)
            return

        # Look up fib levels
        cached = _fib_cache.get(sym)
        if not cached:
            messagebox.showwarning("No Fib Data",
                                   f"No Fibonacci data for {sym}.\nWait for enrichment.",
                                   parent=self.root)
            return

        _anchor_low, _anchor_high, all_levels, _ratio_map = cached
        if not all_levels:
            messagebox.showwarning("No Fib Levels", f"Empty fib levels for {sym}.",
                                   parent=self.root)
            return

        # Nearest support fib = max level <= entry_price
        supports = [lv for lv in all_levels if lv <= entry_price]
        if not supports:
            messagebox.showwarning("No Support", f"No fib support below ${entry_price:.2f}.",
                                   parent=self.root)
            return
        nearest_support = supports[-1]  # all_levels is sorted

        # Stop price = nearest support × (1 - STOP_PCT)
        stop_price = round(nearest_support * (1 - FIB_DT_LIVE_STOP_PCT), 2)

        # Target = Nth fib level above entry
        above_levels = [lv for lv in all_levels if lv > entry_price]
        if len(above_levels) < FIB_DT_LIVE_TARGET_LEVELS:
            messagebox.showwarning("Not Enough Levels",
                                   f"Need {FIB_DT_LIVE_TARGET_LEVELS} fib levels above entry, "
                                   f"only {len(above_levels)} available.",
                                   parent=self.root)
            return
        target_price = round(above_levels[FIB_DT_LIVE_TARGET_LEVELS - 1], 2)

        # Qty = 100% of buying power
        bp = self._cached_buying_power
        if bp <= 0:
            messagebox.showwarning("No Data", "Waiting for account data...", parent=self.root)
            return
        qty = int(bp / entry_price)
        if qty <= 0:
            messagebox.showwarning("Qty Too Low", "Not enough buying power.", parent=self.root)
            return

        half = qty // 2
        other_half = qty - half

        # Confirmation dialog
        confirm = messagebox.askokcancel(
            "FIB DT Order",
            f"FIB DOUBLE-TOUCH — {sym}\n\n"
            f"Entry: MARKET BUY {qty} shares\n"
            f"Support: ${nearest_support:.4f}\n"
            f"Stop: ${stop_price:.2f} (−{FIB_DT_LIVE_STOP_PCT*100:.0f}%)\n"
            f"Target: ${target_price:.2f} (fib #{FIB_DT_LIVE_TARGET_LEVELS})\n\n"
            f"Split exit:\n"
            f"  OCA half: {half} shares (stop + target)\n"
            f"  Standalone stop: {other_half} shares\n\n"
            f"outsideRth=True\n"
            f"Continue?",
            parent=self.root,
        )
        if not confirm:
            return

        self._order_queue.put({
            'sym': sym, 'action': 'BUY', 'qty': qty, 'price': entry_price,
            'strategy': 'fib_dt',
            'stop_price': stop_price,
            'target_price': target_price,
            'half': half,
            'other_half': other_half,
        })
        self._order_status_var.set(
            f"Queued: FIB DT {qty} {sym} | stop ${stop_price:.2f} | target ${target_price:.2f}")
        self._order_status_label.config(fg="#ffcc00")

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
                order_queue=self._order_queue,
                on_account=self._on_account_data,
                on_order_result=self._on_order_result,
            )
            self.scanner.start()
            self.btn.config(text="STOP", bg=self.RED)
            self.status.set("Scanner running...")

    def _apply_size(self, choice: str):
        geo = self._size_presets.get(choice, "1400x900")
        self.root.geometry(geo)
        self._save()

    def _st(self, msg):
        self.root.after(0, lambda: self.status.set(msg))

    def _save(self):
        with open(STATE_PATH, 'w') as f:
            json.dump({
                'freq': self.freq.get(),
                'thresh': self.thresh.get(),
                'price_min': self.price_min.get(),
                'price_max': self.price_max.get(),
                'window_size': self.size_var.get(),
            }, f)

    def _load(self):
        if not STATE_PATH.exists():
            return
        try:
            with open(STATE_PATH) as f:
                s = json.load(f)
            self.freq.set(s.get('freq', MONITOR_DEFAULT_FREQ))
            self.thresh.set(s.get('thresh', MONITOR_DEFAULT_ALERT_PCT))
            self.price_min.set(s.get('price_min', MONITOR_PRICE_MIN))
            self.price_max.set(s.get('price_max', MONITOR_PRICE_MAX))
            saved_size = s.get('window_size', '')
            if saved_size and saved_size in self._size_presets:
                self.size_var.set(saved_size)
                self.root.geometry(self._size_presets[saved_size])
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
