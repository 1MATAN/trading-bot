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
import time as time_mod
import tkinter as tk
from collections import defaultdict
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
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
from strategies.fib_dt_live_strategy import (
    FibDTLiveStrategySync, DTEntryRequest, DTTrailingExit, GapSignal,
)
from strategies.fib_dt_live_entry import FibDTLiveEntrySync
from config.settings import (
    FIB_LEVELS_24, FIB_LEVEL_COLORS, IBKR_HOST, IBKR_PORT,
    MONITOR_IBKR_CLIENT_ID, MONITOR_SCAN_CODE, MONITOR_SCAN_MAX_RESULTS,
    MONITOR_PRICE_MIN, MONITOR_PRICE_MAX, MONITOR_DEFAULT_FREQ,
    MONITOR_DEFAULT_ALERT_PCT,
    FIB_DT_LIVE_STOP_PCT, FIB_DT_LIVE_TARGET_LEVELS,
    STARTING_CAPITAL,
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
_ibkr_last_attempt: float = 0.0      # time.time() of last failed connect
_ibkr_fail_count: int = 0            # consecutive failures (for backoff)
_IBKR_BACKOFF_BASE = 5               # initial backoff seconds
_IBKR_BACKOFF_MAX = 120              # max backoff seconds
_IBKR_MAX_RETRIES = 3                # retries per _get_ibkr call


def _get_ibkr() -> IB | None:
    """Get/create a dedicated IBKR connection for the monitor.

    Retries up to _IBKR_MAX_RETRIES times with exponential backoff.
    Respects a cooldown between connect attempts to avoid hammering TWS.
    """
    global _ibkr, _ibkr_last_attempt, _ibkr_fail_count
    if _ibkr and _ibkr.isConnected():
        return _ibkr

    # Respect backoff cooldown from previous failures
    if _ibkr_fail_count > 0:
        backoff = min(_IBKR_BACKOFF_BASE * (2 ** (_ibkr_fail_count - 1)), _IBKR_BACKOFF_MAX)
        elapsed = time_mod.time() - _ibkr_last_attempt
        if elapsed < backoff:
            return None

    # Ensure an asyncio event loop exists in this thread
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    # Disconnect stale instance if any
    if _ibkr:
        try:
            _ibkr.disconnect()
        except Exception:
            pass
        _ibkr = None

    for attempt in range(1, _IBKR_MAX_RETRIES + 1):
        try:
            _ibkr = IB()
            _ibkr.connect(IBKR_HOST, IBKR_PORT, clientId=MONITOR_IBKR_CLIENT_ID, timeout=10)
            log.info("IBKR connection established (monitor)")
            _ibkr_fail_count = 0
            accts = _ibkr.managedAccounts() or []
            acct = accts[0] if accts else "?"
            reconnect_note = " (reconnected)" if _ibkr_last_attempt > 0 else ""
            ok = send_telegram(
                f"✅ <b>Monitor Online{reconnect_note}</b>\n"
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
            log.warning(f"IBKR connect attempt {attempt}/{_IBKR_MAX_RETRIES} failed: {e}")
            _ibkr = None
            if attempt < _IBKR_MAX_RETRIES:
                time_mod.sleep(min(2 * attempt, 6))

    # All retries exhausted
    _ibkr_fail_count += 1
    _ibkr_last_attempt = time_mod.time()
    backoff = min(_IBKR_BACKOFF_BASE * (2 ** (_ibkr_fail_count - 1)), _IBKR_BACKOFF_MAX)
    log.warning(f"IBKR connect failed after {_IBKR_MAX_RETRIES} retries, "
                f"next attempt in {backoff:.0f}s (fail #{_ibkr_fail_count})")
    if _ibkr_fail_count == 1:
        send_telegram(
            f"⚠️ <b>IBKR מנותק!</b>\n"
            f"  {_IBKR_MAX_RETRIES} ניסיונות חיבור נכשלו\n"
            f"  ניסיון הבא עוד {backoff:.0f} שניות\n"
            f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    return None


# ══════════════════════════════════════════════════════════
#  Market Session Detection
# ══════════════════════════════════════════════════════════

_ET = ZoneInfo('US/Eastern')


def _get_market_session() -> str:
    """Return current US market session based on Eastern Time.

    Returns one of: 'pre_market', 'market', 'after_hours', 'closed'.
    """
    now_et = datetime.now(_ET)
    t = now_et.time()
    if time(4, 0) <= t < time(9, 30):
        return 'pre_market'
    elif time(9, 30) <= t < time(16, 0):
        return 'market'
    elif time(16, 0) <= t < time(20, 0):
        return 'after_hours'
    return 'closed'


def _fetch_extended_hours_price(ib: IB, contract, session: str,
                                bars: list) -> tuple[float, float, float | None, float | None, float | None]:
    """Fetch price, prev_close, and extended-hours high/low/vwap.

    Returns (price, prev_close, ext_high, ext_low, ext_vwap).
    ext_* are None when no extended-hours data is available.
    """
    last_bar = bars[-1]
    price = last_bar.close
    ext_high = None
    ext_low = None
    ext_vwap = None

    if session == 'pre_market':
        # Pre-market: last RTH bar is yesterday → it IS prev_close
        prev_close = last_bar.close
        try:
            mid_bars = ib.reqHistoricalData(
                contract, endDateTime='',
                durationStr='21600 S', barSizeSetting='1 min',
                whatToShow='MIDPOINT', useRTH=False,
            )
            if mid_bars:
                price = mid_bars[-1].close
                ext_high = max(b.high for b in mid_bars)
                ext_low = min(b.low for b in mid_bars)
                ext_vwap = sum((b.high + b.low + b.close) / 3 for b in mid_bars) / len(mid_bars)
        except Exception:
            pass
    elif session == 'after_hours':
        # After-hours: last RTH bar is today → prev_close is yesterday
        prev_close = bars[-2].close if len(bars) >= 2 else 0.0
        try:
            mid_bars = ib.reqHistoricalData(
                contract, endDateTime='',
                durationStr='14400 S', barSizeSetting='1 min',
                whatToShow='MIDPOINT', useRTH=False,
            )
            if mid_bars:
                price = mid_bars[-1].close
                ext_high = max(b.high for b in mid_bars)
                ext_low = min(b.low for b in mid_bars)
                ext_vwap = sum((b.high + b.low + b.close) / 3 for b in mid_bars) / len(mid_bars)
        except Exception:
            pass
    else:
        # Market hours (or closed): normal calculation
        prev_close = bars[-2].close if len(bars) >= 2 else 0.0

    return price, prev_close, ext_high, ext_low, ext_vwap


def _format_volume(volume: float) -> str:
    """Format volume as human-readable string (e.g. 2.3M, 890K)."""
    if volume >= 1_000_000:
        return f"{volume / 1_000_000:.1f}M"
    elif volume >= 1_000:
        return f"{volume / 1_000:.0f}K"
    return str(int(volume))


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

    # ── Phase 1: Collect valid contracts ──
    valid_items: list[tuple[str, object]] = []  # (sym, contract)
    for item in results:
        contract = item.contractDetails.contract
        sym = contract.symbol
        if not sym or not sym.isalpha() or len(sym) > 5:
            continue
        valid_items.append((sym, contract))

    # ── Phase 2: Enrich with historical data ──
    for sym, stock_contract in valid_items:

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
            volume = last_bar.volume
            session = _get_market_session()

            price, prev_close, ext_high, ext_low, ext_vwap = \
                _fetch_extended_hours_price(ib, stock_contract, session, bars)

            pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0

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
            vol_str = _format_volume(volume)
            vwap = round(last_bar.average, 4) if last_bar.average else 0.0

            # Override with extended-hours values when available
            rth_high = round(last_bar.high, 4)
            rth_low = round(last_bar.low, 4)
            if session == 'pre_market' and ext_high is not None:
                # Pre-market: use only extended-hours data (RTH bar is yesterday)
                day_high = round(ext_high, 4)
                day_low = round(ext_low, 4)
                vwap = round(ext_vwap, 4)
            elif session == 'after_hours' and ext_high is not None:
                # After-hours: combine RTH + AH data
                day_high = round(max(rth_high, ext_high), 4)
                day_low = round(min(rth_low, ext_low), 4)
                vwap = round(ext_vwap, 4)
            else:
                day_high = rth_high
                day_low = rth_low

            stocks[sym] = {
                "price": round(price, 2),
                "pct": round(pct, 1),
                "volume": vol_str,
                "volume_raw": int(volume),
                "rvol": rvol,
                "float": "",
                "vwap": vwap,
                "prev_close": round(prev_close, 4),
                "day_high": day_high,
                "day_low": day_low,
                "contract": stock_contract,
            }

        except Exception as e:
            log.debug(f"Enrich {sym} failed: {e}")
            continue

    session = _get_market_session()
    log.info(f"Scanner [{session}]: {len(results)} raw → {len(stocks)} enriched symbols")
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
            if h.time:
                utc_dt = h.time.replace(tzinfo=ZoneInfo('UTC'))
                il_dt = utc_dt.astimezone(ZoneInfo('Asia/Jerusalem'))
                date_str = il_dt.strftime('%Y-%m-%d %H:%M')
            else:
                date_str = ''
            results.append({
                'title_en': clean,
                'date': date_str,
                'source': h.providerCode or '',
            })
        return results
    except Exception as e:
        log.debug(f"IBKR news {symbol}: {e}")
        return []


# ══════════════════════════════════════════════════════════
#  Periodic News Alerts (every 5.5 min)
# ══════════════════════════════════════════════════════════

NEWS_CHECK_INTERVAL = 330  # 5.5 minutes in seconds
NEWS_MIN_PCT = 20.0        # only check stocks ≥ +20%

# {symbol: set of headline strings already sent}
_news_sent: dict[str, set[str]] = {}
_news_last_check: float = 0.0


def _check_news_updates(current_stocks: dict):
    """Check for new IBKR news headlines on stocks ≥ +20%.

    Only runs during pre_market, market, and after_hours.
    Sends Telegram alert for each new headline found.
    """
    global _news_last_check

    now = time_mod.time()
    if now - _news_last_check < NEWS_CHECK_INTERVAL:
        return
    _news_last_check = now

    session = _get_market_session()
    if session == 'closed':
        return

    # Filter stocks ≥ 20%
    candidates = {sym: d for sym, d in current_stocks.items()
                  if d.get('pct', 0) >= NEWS_MIN_PCT}
    if not candidates:
        return

    log.info(f"News check: {len(candidates)} stocks ≥ +{NEWS_MIN_PCT:.0f}%")

    for sym, d in candidates.items():
        if sym not in _news_sent:
            _news_sent[sym] = set()

        try:
            headlines = _fetch_ibkr_news(sym, max_news=5)
        except Exception as e:
            log.debug(f"News check {sym}: {e}")
            continue

        new_headlines = []
        for h in headlines:
            title = h['title_en']
            if title not in _news_sent[sym]:
                _news_sent[sym].add(title)
                new_headlines.append(h)

        if not new_headlines:
            continue

        # Translate new headlines
        titles_en = [h['title_en'] for h in new_headlines]
        try:
            combined = "\n||||\n".join(titles_en)
            translated = _translator.translate(combined)
            titles_he = translated.split("\n||||\n")
        except Exception:
            titles_he = titles_en

        price = d.get('price', 0)
        pct = d.get('pct', 0)
        enrich = _enrichment.get(sym, {})
        flt = enrich.get('float', '-')

        lines = [
            f"📰 <b>חדשות — {sym}</b>  ${price:.2f}  {pct:+.1f}%  Float:{flt}",
            f"━━━━━━━━━━━━━━━━━━",
        ]
        for i, h in enumerate(new_headlines):
            title_he = titles_he[i].strip() if i < len(titles_he) else h['title_en']
            src = h.get('source', '')
            src_tag = f" [{src}]" if src else ""
            lines.append(f"  • {title_he}  <i>({h['date']}{src_tag})</i>")

        btn = _make_lookup_button(sym)
        send_telegram("\n".join(lines), reply_markup=btn)
        log.info(f"News alert: {sym} — {len(new_headlines)} new headlines")

    # Clean up symbols that dropped below threshold
    active_syms = set(candidates.keys())
    for sym in list(_news_sent.keys()):
        if sym not in current_stocks:
            del _news_sent[sym]


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
                raw_date = str(row.get('Date', ''))
                try:
                    et_dt = datetime.strptime(raw_date[:19], '%Y-%m-%d %H:%M:%S')
                    et_dt = et_dt.replace(tzinfo=ZoneInfo('US/Eastern'))
                    il_dt = et_dt.astimezone(ZoneInfo('Asia/Jerusalem'))
                    dates.append(il_dt.strftime('%Y-%m-%d %H:%M'))
                except (ValueError, IndexError):
                    dates.append(raw_date[:16])

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
MILESTONE_VOL_RATIO = 2.4   # 1-min candle volume must be ≥ 2.4x previous

# {symbol: last milestone alerted (e.g. 25, 30, 35...)}
_milestone_alerted: dict[str, float] = {}


def _check_1min_volume_spike(sym: str) -> tuple[bool, float]:
    """Check if last 1-min candle volume ≥ MILESTONE_VOL_RATIO × previous candle.

    Returns (spike_detected, ratio).
    """
    ib = _get_ibkr()
    if not ib:
        return True, 0.0  # no connection — don't block alert
    try:
        contract = Stock(sym, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='300 S',
            barSizeSetting='1 min', whatToShow='TRADES', useRTH=False,
        )
        if bars and len(bars) >= 2:
            prev_vol = bars[-2].volume
            curr_vol = bars[-1].volume
            if prev_vol > 0:
                ratio = curr_vol / prev_vol
                return ratio >= MILESTONE_VOL_RATIO, round(ratio, 1)
    except Exception as e:
        log.debug(f"1min vol check {sym}: {e}")
    return True, 0.0  # on error — don't block alert


def _format_fib_text(sym: str, price: float) -> str:
    """Build compact fib levels text — 3 per row with ratio labels."""
    # Recalculate if needed (auto-advance when price exceeds top level)
    if price > 0:
        calc_fib_levels(sym, price)
    cached = _fib_cache.get(sym)
    if not cached:
        return ""
    all_levels = cached[2]
    ratio_map = cached[3]
    anchor_low = cached[0]
    anchor_high = cached[1]
    anchor_date = cached[4] if len(cached) > 4 else ""
    above = [lv for lv in all_levels if lv > price][:10]
    below = [lv for lv in all_levels if lv <= price][-5:]
    if not above and not below:
        return ""

    def _fmt(lv: float) -> str:
        info = ratio_map.get(round(lv, 4))
        r = f"({info[0]})" if info else ""
        return f"${lv:.4f} {r}"

    def _rows(levels: list[float]) -> list[str]:
        """Group levels into rows of 3."""
        result = []
        for i in range(0, len(levels), 3):
            chunk = levels[i:i + 3]
            result.append("   " + " | ".join(_fmt(lv) for lv in chunk))
        return result

    lines = ["\n📐 <b>פיבונאצ'י:</b>"]
    lines.append(f"🕯 נר עוגן: L ${anchor_low:.4f} — H ${anchor_high:.4f}  ({anchor_date})")
    if above:
        lines.append("⬆️ " + " | ".join(_fmt(lv) for lv in above[:3]))
        lines.extend(_rows(above[3:]))
    lines.append(f"━━━ ${price:.2f} ━━━")
    if below:
        lines.append("⬇️ " + " | ".join(_fmt(lv) for lv in below[:3]))
        lines.extend(_rows(below[3:]))
    return "\n".join(lines)


def check_milestone(sym: str, pct: float, price: float = 0.0) -> str | None:
    """Check if stock crossed a +5% milestone up or down.

    Returns alert message or None.
    E.g. stock at +27% → milestone 25. If last alerted was 20 → alert for 25.
    Also detects corrections: +30% → +24% alerts "dropped below +25%".
    """
    fib_text = _format_fib_text(sym, price) if price > 0 else ""

    if pct < MILESTONE_START_PCT:
        # Stock dropped below tracking threshold — clear and alert
        if sym in _milestone_alerted:
            last = _milestone_alerted.pop(sym)
            return (
                f"⚠️ <b>{sym}</b> ירד מתחת ל-+{MILESTONE_START_PCT:.0f}%\n"
                f"  נוכחי: {pct:+.1f}%  |  מחיר: ${price:.2f}"
                f"{fib_text}"
            )
        return None

    current_milestone = int(pct // MILESTONE_STEP_PCT) * MILESTONE_STEP_PCT
    last = _milestone_alerted.get(sym, MILESTONE_START_PCT - MILESTONE_STEP_PCT)

    if current_milestone > last:
        _milestone_alerted[sym] = current_milestone
        return (
            f"📈 <b>{sym}</b> חצה +{current_milestone:.0f}%!\n"
            f"  נוכחי: {pct:+.1f}%  |  מחיר: ${price:.2f}"
            f"{fib_text}"
        )
    elif current_milestone < last:
        _milestone_alerted[sym] = current_milestone
        return (
            f"📉 <b>{sym}</b> תיקון — ירד מתחת ל-+{last:.0f}%\n"
            f"  נוכחי: {pct:+.1f}%  |  מחיר: ${price:.2f}"
            f"{fib_text}"
        )
    return None


# ══════════════════════════════════════════════════════════
#  Pre-Market Open Reminders
# ══════════════════════════════════════════════════════════

_MARKET_OPEN_ET = time(9, 30)  # US market open
_REMINDER_MINUTES = [30, 15, 5]  # minutes before open
_reminders_sent: set[str] = set()  # "YYYY-MM-DD_30" etc.


def _check_market_reminders():
    """Send Telegram reminders before market open."""
    now_et = datetime.now(ZoneInfo('US/Eastern'))
    today_str = now_et.strftime('%Y-%m-%d')
    # Only on weekdays
    if now_et.weekday() >= 5:
        return
    open_dt = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    mins_to_open = (open_dt - now_et).total_seconds() / 60

    for m in _REMINDER_MINUTES:
        key = f"{today_str}_{m}"
        if key in _reminders_sent:
            continue
        if 0 < mins_to_open <= m and mins_to_open > (m - 2):
            _reminders_sent.add(key)
            send_telegram(f"⏰ <b>תזכורת:</b> {m} דקות לפתיחת המסחר!")
            log.info(f"Market reminder sent: {m} min to open")


# ══════════════════════════════════════════════════════════
#  Daily Summary (sent after market close)
# ══════════════════════════════════════════════════════════

_daily_summary_sent: str = ""  # "YYYY-MM-DD" of last summary sent
_daily_top_movers: dict[str, dict] = {}  # sym → {peak_pct, peak_price}
_daily_new_stocks: int = 0


def _track_daily_stats(current: dict, new_count: int = 0):
    """Accumulate daily statistics for the end-of-day summary."""
    global _daily_new_stocks
    _daily_new_stocks += new_count
    for sym, d in current.items():
        pct = d.get('pct', 0)
        price = d.get('price', 0)
        prev = _daily_top_movers.get(sym)
        if prev is None or abs(pct) > abs(prev['peak_pct']):
            _daily_top_movers[sym] = {'peak_pct': pct, 'peak_price': price}


def _check_daily_summary(positions: dict[str, tuple] | None = None):
    """Send end-of-day summary at ~16:05 ET. Only once per day."""
    global _daily_summary_sent, _daily_new_stocks
    now_et = datetime.now(ZoneInfo('US/Eastern'))
    today_str = now_et.strftime('%Y-%m-%d')

    if today_str == _daily_summary_sent:
        return
    if now_et.weekday() >= 5:
        return
    # Send between 16:03 and 16:10 ET
    t = now_et.time()
    if not (time(16, 3) <= t <= time(16, 10)):
        return

    _daily_summary_sent = today_str

    # ── Top movers (top 5 by absolute pct) ──
    sorted_movers = sorted(
        _daily_top_movers.items(),
        key=lambda x: abs(x[1]['peak_pct']),
        reverse=True,
    )[:5]

    total_alerts = sum(_daily_alert_count.values())

    lines = [
        f"📊 <b>סיכום יומי — {today_str}</b>",
        f"━━━━━━━━━━━━━━━━━━",
    ]

    if sorted_movers:
        lines.append("")
        lines.append("🏆 <b>מובילים היום:</b>")
        for i, (sym, info) in enumerate(sorted_movers, 1):
            arrow = "🟢" if info['peak_pct'] > 0 else "🔴"
            enrich = _enrichment.get(sym, {})
            flt = enrich.get('float', '-')
            lines.append(
                f"  {i}. {arrow} <b>{sym}</b> {info['peak_pct']:+.1f}% "
                f"(${info['peak_price']:.2f}) Float:{flt}"
            )

    lines.append("")
    lines.append(f"📈 מניות חדשות בסקאנר: {_daily_new_stocks}")
    lines.append(f"🔔 סה\"כ התראות: {total_alerts}")

    # ── P&L if positions exist ──
    if positions:
        total_pnl = sum(pos[3] for pos in positions.values() if len(pos) >= 4)
        lines.append("")
        pnl_icon = "💚" if total_pnl >= 0 else "❤️"
        lines.append(f"{pnl_icon} P&L פתוח: ${total_pnl:+,.2f}")
        for sym, pos in sorted(positions.items()):
            qty, avg = pos[0], pos[1]
            pnl = pos[3] if len(pos) >= 4 else 0
            icon = "🟢" if pnl >= 0 else "🔴"
            lines.append(f"  {icon} {sym}: {qty}sh @ ${avg:.2f} → ${pnl:+,.2f}")

    lines.append("")
    lines.append("📡 המוניטור ממשיך לעבוד — after hours")

    send_telegram("\n".join(lines))
    log.info("Daily summary sent")

    # Reset daily stats for next day
    _daily_top_movers.clear()
    _daily_new_stocks = 0


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
#  Real-Time Trading Alerts (5 types)
# ══════════════════════════════════════════════════════════

# ── Alert tracking state ──
_hod_break_alerted: dict[str, float] = {}              # sym -> last HOD value alerted
_fib_touch_tracker: dict[str, dict[float, int]] = {}   # sym -> {fib_level: touch_count}
_lod_touch_tracker: dict[str, int] = {}                 # sym -> touch count at day_low
_lod_was_near: dict[str, bool] = {}                     # sym -> was near LOD last cycle
_vwap_side: dict[str, str] = {}                         # sym -> 'above' | 'below'
_price_1min: dict[str, list[tuple[float, float]]] = {}  # sym -> [(timestamp, price)]
_spike_alerted: set[str] = set()                        # already alerted 8%+ spike
_multi_signal_alerted: set[str] = set()                 # already sent multi-signal alert today
_alerts_date: str = ""                                  # date for daily reset
_daily_alert_count: dict[str, int] = {}                 # sym -> total alerts sent today
_daily_volume_peak: dict[str, int] = {}                 # sym -> peak volume_raw seen today

# ── Alert score thresholds ──
ALERT_MIN_SCORE = 40       # minimum score to send any alert (0-100)
MULTI_SIGNAL_MIN = 2       # minimum signals for combined alert

# ── News catalyst keywords (bullish / bearish) ──
# Each tuple: (keyword, points, reason_label)
# Matched case-insensitively against news headlines (Hebrew + English)
_NEWS_BULLISH_KW: list[tuple[str, int, str]] = [
    # ── AI / Tech ──
    ("artificial intelligence", 20, "AI"),
    ("בינה מלאכותית", 20, "AI"),
    (" ai ", 15, "AI"),             # space-padded to avoid matching "said", "laim"
    ("machine learning", 15, "AI"),
    ("למידת מכונה", 15, "AI"),
    ("deep learning", 15, "AI"),
    ("generative ai", 20, "AI"),
    ("chatgpt", 15, "AI"),
    ("openai", 15, "AI"),
    ("nvidia", 12, "AI"),
    ("quantum comput", 15, "quantum"),
    ("מחשוב קוונטי", 15, "quantum"),
    # ── FDA / Biotech ──
    ("fda approv", 25, "FDA"),
    ("אישור fda", 25, "FDA"),
    ("fda clear", 20, "FDA"),
    ("breakthrough therapy", 20, "FDA"),
    ("טיפול פורץ דרך", 20, "FDA"),
    ("phase 3", 15, "Phase3"),
    ("phase iii", 15, "Phase3"),
    ("שלב 3", 15, "Phase3"),
    ("clinical trial", 10, "clinical"),
    ("ניסוי קליני", 10, "clinical"),
    ("positive results", 15, "results+"),
    ("תוצאות חיוביות", 15, "results+"),
    ("positive data", 15, "results+"),
    ("נתונים חיוביים", 15, "results+"),
    # ── Government / Defense ──
    ("government contract", 25, "gov"),
    ("חוזה ממשלתי", 25, "gov"),
    ("ממשלה", 12, "gov"),
    ("government", 12, "gov"),
    ("defense contract", 20, "defense"),
    ("חוזה ביטחוני", 20, "defense"),
    ("department of defense", 20, "defense"),
    ("pentagon", 15, "defense"),
    ("פנטגון", 15, "defense"),
    ("military", 12, "defense"),
    ("צבאי", 12, "defense"),
    ("nasa", 15, "NASA"),
    # ── Investment / Institutional ──
    ("major investor", 20, "investor"),
    ("משקיע", 15, "investor"),
    ("institutional buy", 15, "investor"),
    ("רכישה מוסדית", 15, "investor"),
    ("13d filing", 15, "investor"),
    ("activist investor", 18, "investor"),
    ("warren buffett", 20, "investor"),
    ("berkshire", 15, "investor"),
    ("stake", 12, "investor"),
    ("אחזקה", 10, "investor"),
    ("insider buy", 15, "insider"),
    ("רכישת פנים", 15, "insider"),
    ("buyback", 15, "buyback"),
    ("רכישה עצמית", 15, "buyback"),
    ("share repurchase", 15, "buyback"),
    # ── M&A / Partnerships ──
    ("partnership", 18, "partnership"),
    ("שותפות", 18, "partnership"),
    ("collaboration", 12, "partnership"),
    ("שיתוף פעולה", 12, "partnership"),
    ("strategic alliance", 15, "partnership"),
    ("ברית אסטרטגית", 15, "partnership"),
    ("acquisition", 18, "M&A"),
    ("רכישה", 12, "M&A"),
    ("merger", 18, "M&A"),
    ("מיזוג", 18, "M&A"),
    ("buyout", 18, "M&A"),
    ("takeover", 18, "M&A"),
    ("השתלטות", 18, "M&A"),
    # ── Earnings / Revenue ──
    ("beat estimates", 18, "beat"),
    ("עלה על התחזיות", 18, "beat"),
    ("beats expectations", 18, "beat"),
    ("revenue surge", 15, "revenue+"),
    ("זינוק בהכנסות", 15, "revenue+"),
    ("record revenue", 18, "revenue+"),
    ("הכנסות שיא", 18, "revenue+"),
    ("record earnings", 18, "earnings+"),
    ("profit surge", 15, "earnings+"),
    ("raised guidance", 18, "guidance+"),
    ("העלאת תחזית", 18, "guidance+"),
    ("raises guidance", 18, "guidance+"),
    ("upgrade", 12, "upgrade"),
    ("העלאת דירוג", 12, "upgrade"),
    ("price target raised", 15, "PT+"),
    ("יעד מחיר הועלה", 15, "PT+"),
    ("price target increase", 15, "PT+"),
    # ── Regulatory / IP ──
    ("patent", 12, "patent"),
    ("פטנט", 12, "patent"),
    ("patent granted", 18, "patent"),
    ("patent approved", 18, "patent"),
    ("license agreement", 12, "license"),
    ("הסכם רישיון", 12, "license"),
    # ── Hot sectors ──
    ("electric vehicle", 12, "EV"),
    ("רכב חשמלי", 12, "EV"),
    (" ev ", 10, "EV"),
    ("solar", 10, "solar"),
    ("סולארי", 10, "solar"),
    ("clean energy", 10, "cleanE"),
    ("אנרגיה נקייה", 10, "cleanE"),
    ("blockchain", 10, "crypto"),
    ("בלוקצ'יין", 10, "crypto"),
    ("bitcoin", 10, "crypto"),
    ("ביטקוין", 10, "crypto"),
    ("crypto", 10, "crypto"),
    ("short squeeze", 20, "squeeze"),
    ("שורט סקוויז", 20, "squeeze"),
    ("cannabis", 10, "cannabis"),
    ("קנאביס", 10, "cannabis"),
    ("marijuana", 10, "cannabis"),
    ("legalization", 12, "legal"),
    ("לגליזציה", 12, "legal"),
    ("space", 10, "space"),
    ("חלל", 10, "space"),
    ("satellite", 10, "space"),
    ("לוויין", 10, "space"),
]

# Bearish keywords — subtract points
_NEWS_BEARISH_KW: list[tuple[str, int, str]] = [
    ("dilution", -15, "dilution"),
    ("דילול", -15, "dilution"),
    ("offering", -12, "offering"),
    ("הנפק", -10, "offering"),
    ("shelf registration", -12, "shelf"),
    ("bankruptcy", -20, "bankrupt"),
    ("פשיטת רגל", -20, "bankrupt"),
    ("delisting", -20, "delist"),
    ("מחיקה מהמסחר", -20, "delist"),
    ("sec investigation", -15, "SEC"),
    ("חקירת sec", -15, "SEC"),
    ("fraud", -18, "fraud"),
    ("הונאה", -18, "fraud"),
    ("lawsuit", -8, "lawsuit"),
    ("תביע", -8, "lawsuit"),
    ("downgrade", -12, "downgrade"),
    ("הורדת דירוג", -12, "downgrade"),
    ("price target cut", -12, "PT-"),
    ("price target lower", -12, "PT-"),
    ("missed estimates", -15, "miss"),
    ("miss expectations", -15, "miss"),
    ("reverse split", -15, "r/s"),
    ("איחוד מניות", -15, "r/s"),
    ("going concern", -18, "concern"),
]


def _score_news_catalysts(sym: str) -> tuple[int, list[str]]:
    """Score news headlines for bullish/bearish catalysts. Returns (points, [labels])."""
    enrich = _enrichment.get(sym, {})
    news_list = enrich.get('news', [])
    if not news_list:
        return 0, []

    total = 0
    seen_labels: set[str] = set()
    reasons: list[str] = []

    for article in news_list:
        title = article.get('title_he', '').lower()
        if not title:
            continue
        # Add spaces around title for word-boundary matching
        padded = f" {title} "
        for kw, pts, label in _NEWS_BULLISH_KW:
            if label not in seen_labels and kw.lower() in padded:
                total += pts
                seen_labels.add(label)
                reasons.append(f"📰{label}")
        for kw, pts, label in _NEWS_BEARISH_KW:
            if label not in seen_labels and kw.lower() in padded:
                total += pts  # pts is negative
                seen_labels.add(label)
                reasons.append(f"📰⚠️{label}")

    return total, reasons


def _calc_alert_score(sym: str, d: dict) -> tuple[int, list[str]]:
    """Score a stock 0-100 for alert relevance. Higher = more likely to move big.

    Returns (score, [reasons]).
    Factors: low float, high RVOL, above VWAP, high pct change, near fib, news catalysts.
    """
    score = 0
    reasons = []
    enrich = _enrichment.get(sym, {})

    # ── Float (0-30 pts) — lower is better ──
    flt = _parse_float_to_shares(enrich.get('float', '-'))
    if 0 < flt < 3_000_000:
        score += 30
        reasons.append("float<3M")
    elif 0 < flt < 10_000_000:
        score += 20
        reasons.append("float<10M")
    elif 0 < flt < 30_000_000:
        score += 10
        reasons.append("float<30M")

    # ── RVOL (0-25 pts) — higher is better ──
    rvol = d.get('rvol', 0)
    if rvol >= 5.0:
        score += 25
        reasons.append(f"RVOL {rvol}x")
    elif rvol >= 3.0:
        score += 18
        reasons.append(f"RVOL {rvol}x")
    elif rvol >= 2.0:
        score += 10
        reasons.append(f"RVOL {rvol}x")

    # ── Above VWAP (0-15 pts) ──
    price = d.get('price', 0)
    vwap = d.get('vwap', 0)
    if price > 0 and vwap > 0 and price > vwap:
        pct_above = (price - vwap) / vwap * 100
        if pct_above > 5:
            score += 15
            reasons.append(f"VWAP+{pct_above:.0f}%")
        else:
            score += 8
            reasons.append("above VWAP")

    # ── Daily % change (0-15 pts) ──
    pct = d.get('pct', 0)
    if pct >= 50:
        score += 15
        reasons.append(f"{pct:+.0f}%")
    elif pct >= 30:
        score += 10
        reasons.append(f"{pct:+.0f}%")
    elif pct >= 20:
        score += 5
        reasons.append(f"{pct:+.0f}%")

    # ── Short interest (0-10 pts) — squeeze potential ──
    short_str = enrich.get('short', '-')
    try:
        short_pct = float(short_str.replace('%', ''))
        if short_pct >= 20:
            score += 10
            reasons.append(f"short {short_pct:.0f}%")
        elif short_pct >= 10:
            score += 5
            reasons.append(f"short {short_pct:.0f}%")
    except (ValueError, AttributeError):
        pass

    # ── Near fib level (0-5 pts) ──
    cached = _fib_cache.get(sym)
    if cached and price > 0:
        _, _, all_levels, _ = cached
        threshold = price * 0.008
        for lv in all_levels:
            if abs(price - lv) <= threshold:
                score += 5
                reasons.append("near fib")
                break

    # ── News catalysts (up to +25 / down to -20 pts) ──
    news_pts, news_reasons = _score_news_catalysts(sym)
    if news_pts != 0:
        score += news_pts
        reasons.extend(news_reasons)

    return max(min(score, 100), 0), reasons


def _reset_alerts_if_new_day():
    """Clear all alert tracking state when date changes."""
    global _alerts_date
    today = datetime.now(ZoneInfo('US/Eastern')).strftime('%Y-%m-%d')
    if today != _alerts_date:
        _alerts_date = today
        _hod_break_alerted.clear()
        _fib_touch_tracker.clear()
        _lod_touch_tracker.clear()
        _lod_was_near.clear()
        _vwap_side.clear()
        _price_1min.clear()
        _spike_alerted.clear()
        _multi_signal_alerted.clear()
        _daily_alert_count.clear()
        _daily_volume_peak.clear()
        log.info(f"Alert state reset for new day: {today}")


def check_hod_break(sym: str, current: dict, previous: dict) -> str | None:
    """Alert 1: Price broke today's high of day."""
    cur_high = current.get('day_high', 0)
    prev_high = previous.get('day_high', 0)
    price = current.get('price', 0)
    if cur_high <= 0 or prev_high <= 0:
        return None
    # New HOD: today's high is higher than last cycle AND price is at/near the high
    if cur_high > prev_high and price >= cur_high * 0.998:
        # Only alert once per new high value (allow re-alert if high changes again)
        last_alerted = _hod_break_alerted.get(sym, 0)
        if cur_high <= last_alerted:
            return None
        _hod_break_alerted[sym] = cur_high
        pct = current.get('pct', 0)
        return (
            f"🔺 <b>HOD BREAK — {sym}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 ${price:.2f} → שיא יומי חדש!\n"
            f"📊 קודם: ${prev_high:.2f} | שינוי: {pct:+.1f}%"
        )
    return None


def check_fib_second_touch(sym: str, price: float, pct: float) -> str | None:
    """Alert 2: 2nd time price touches same fib level (0.8% proximity)."""
    if price <= 0:
        return None
    if sym not in _fib_cache:
        calc_fib_levels(sym, price)
    if sym not in _fib_cache:
        return None

    _, _, all_levels, ratio_map = _fib_cache[sym]
    if sym not in _fib_touch_tracker:
        _fib_touch_tracker[sym] = {}

    threshold = price * 0.008  # 0.8%

    for lv in all_levels:
        if abs(price - lv) <= threshold:
            lv_key = round(lv, 4)
            count = _fib_touch_tracker[sym].get(lv_key, 0)
            if count < 2:
                _fib_touch_tracker[sym][lv_key] = count + 1
                if count + 1 == 2:
                    info = ratio_map.get(lv_key)
                    ratio_label = f"{info[0]} {info[1]}" if info else ""
                    proximity = abs(price - lv) / price * 100
                    return (
                        f"🎯🎯 <b>FIB TOUCH x2 — {sym}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📍 רמה: ${lv:.4f} ({ratio_label})\n"
                        f"💰 מחיר: ${price:.2f} | קרבה: {proximity:.1f}%\n"
                        f"📊 שינוי: {pct:+.1f}%"
                    )
    return None


def check_lod_touch(sym: str, price: float, day_low: float, pct: float) -> str | None:
    """Alert 3: 2nd time price touches day_low (0.5% proximity)."""
    if price <= 0 or day_low <= 0:
        return None

    threshold = day_low * 0.005  # 0.5%
    near_lod = abs(price - day_low) <= threshold

    was_near = _lod_was_near.get(sym, False)
    _lod_was_near[sym] = near_lod

    # Only count a new touch when transitioning from NOT near to near
    if near_lod and not was_near:
        count = _lod_touch_tracker.get(sym, 0) + 1
        _lod_touch_tracker[sym] = count
        if count == 2:
            return (
                f"🔻🔻 <b>LOD TOUCH x2 — {sym}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 נמוך יומי: ${day_low:.2f}\n"
                f"💰 מחיר: ${price:.2f} | שינוי: {pct:+.1f}%\n"
                f"⚠️ נגיעה שנייה — תמיכה/שבירה?"
            )
    return None


def check_vwap_cross(sym: str, price: float, vwap: float, pct: float) -> str | None:
    """Alert 4: Price crosses VWAP."""
    if price <= 0 or vwap <= 0:
        return None

    current_side = 'above' if price > vwap else 'below'
    prev_side = _vwap_side.get(sym)
    _vwap_side[sym] = current_side

    if prev_side is None:
        return None  # first observation — just record

    if prev_side == 'below' and current_side == 'above':
        return (
            f"⚡ <b>VWAP CROSS — {sym}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🟢 חצה מעל VWAP!\n"
            f"💰 ${price:.2f} > VWAP ${vwap:.2f}\n"
            f"📊 שינוי: {pct:+.1f}%"
        )
    elif prev_side == 'above' and current_side == 'below':
        return (
            f"⚡ <b>VWAP CROSS — {sym}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔴 חצה מתחת VWAP!\n"
            f"💰 ${price:.2f} < VWAP ${vwap:.2f}\n"
            f"📊 שינוי: {pct:+.1f}%"
        )
    return None


def check_1min_spike(sym: str, price: float, pct: float) -> str | None:
    """Alert 5: Price rose 8%+ compared to ~1 minute ago."""
    if price <= 0:
        return None

    now = time_mod.time()
    if sym not in _price_1min:
        _price_1min[sym] = []

    _price_1min[sym].append((now, price))
    # Prune entries older than 90 seconds
    cutoff = now - 90
    _price_1min[sym] = [(t, p) for t, p in _price_1min[sym] if t > cutoff]

    if sym in _spike_alerted:
        return None

    # Find entry closest to 60 seconds ago (window: 55-65s)
    target = now - 60
    candidates = [(t, p) for t, p in _price_1min[sym] if abs(t - target) <= 5]
    if not candidates:
        return None

    old_t, old_price = min(candidates, key=lambda x: abs(x[0] - target))
    if old_price <= 0:
        return None

    change_pct = (price - old_price) / old_price * 100
    if change_pct >= 8.0:
        _spike_alerted.add(sym)
        return (
            f"🚀 <b>SPIKE +{change_pct:.1f}% — {sym}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ עלייה של {change_pct:.1f}% בדקה!\n"
            f"💰 ${old_price:.2f} → ${price:.2f}\n"
            f"📊 שינוי יומי: {pct:+.1f}%"
        )
    return None


# ══════════════════════════════════════════════════════════
#  Stock Enrichment Cache (Finviz + Fib)
# ══════════════════════════════════════════════════════════

# {symbol: {float, short, eps, income, earnings, cash, fib_below, fib_above, news}}
_enrichment: dict[str, dict] = {}
_enrichment_ts: dict[str, float] = {}   # symbol → time.time() when enriched
ENRICHMENT_TTL_SECS = 30 * 60           # 30 minutes


def _enrich_stock(sym: str, price: float, on_status=None, force: bool = False) -> dict:
    """Fetch Finviz fundamentals + Fib levels for a stock. Cached with TTL.

    Returns enrichment dict and sends Telegram alert with full report.
    ``force=True`` bypasses cache (used by explicit lookups).
    """
    if not force and sym in _enrichment:
        age = time_mod.time() - _enrichment_ts.get(sym, 0)
        if age < ENRICHMENT_TTL_SECS:
            return _enrichment[sym]
        log.info(f"Enrichment expired for {sym} ({age/60:.0f}m old), refreshing")

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
    _enrichment_ts[sym] = time_mod.time()
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
    """Generate a daily candlestick chart with Fibonacci levels, gaps + MA overlays.

    Y-axis spans 250% of current price for full picture.

    Returns path to saved PNG or None on failure.
    """
    try:
        # Crop to last ~120 bars
        df = df.tail(120).copy()
        if len(df) < 5:
            return None

        n_bars = len(df)
        right_padding = n_bars  # equal space on right → last candle in the middle

        fig, ax = plt.subplots(figsize=(14, 8), facecolor='#0e1117')
        ax.set_facecolor('#0e1117')

        # ── Candlestick chart ──
        x = np.arange(n_bars)
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

        # Y range: 250% of current price (centered around price)
        vis_max = current_price * 2.5
        vis_min = max(0, current_price * 0.01)  # near zero but not negative

        visible_levels = [lv for lv in all_levels if vis_min <= lv <= vis_max]

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
                ax.text(n_bars + 1, lv, f' {label}', color=color,
                        fontsize=7, va='center', ha='left', fontweight='bold')
                last_y_right = lv

        # ── Unfilled gap detection on chart (≥ $0.05) ──
        # A gap is "filled" when subsequent price action returns to the pre-gap close.
        # Gap up filled when any later bar's low ≤ prev_close (bottom of gap).
        # Gap down filled when any later bar's high ≥ prev_close (top of gap).
        open_gaps: list[dict] = []
        for i in range(1, n_bars):
            prev_close_val = df.iloc[i - 1]['close']
            cur_open = df.iloc[i]['open']
            gap_abs = cur_open - prev_close_val
            if abs(gap_abs) >= 0.05:
                # Check if any subsequent bar filled this gap
                filled = False
                for j in range(i, n_bars):
                    if gap_abs > 0 and df.iloc[j]['low'] <= prev_close_val:
                        filled = True
                        break
                    elif gap_abs < 0 and df.iloc[j]['high'] >= prev_close_val:
                        filled = True
                        break
                if not filled:
                    gap_pct = abs(gap_abs) / prev_close_val * 100 if prev_close_val > 0 else 0
                    open_gaps.append({
                        'idx': i, 'bottom': min(prev_close_val, cur_open),
                        'top': max(prev_close_val, cur_open),
                        'pct': gap_pct, 'up': gap_abs > 0,
                    })

        for g in open_gaps:
            color = '#26a69a' if g['up'] else '#ef5350'
            direction = '+' if g['up'] else '-'
            # Shade from gap bar to right edge (gap still open)
            ax.axhspan(g['bottom'], g['top'],
                       xmin=g['idx'] / (n_bars + right_padding), xmax=1.0,
                       color=color, alpha=0.12)
            ax.annotate(f'OPEN GAP {direction}{g["pct"]:.0f}%',
                        xy=(g['idx'], g['top'] if g['up'] else g['bottom']),
                        fontsize=5, color=color, ha='center',
                        va='bottom' if g['up'] else 'top', fontweight='bold')

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
        ax.set_xlim(-1, n_bars + right_padding)
        ax.tick_params(colors='#888', labelsize=8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('#333')
        ax.spines['left'].set_color('#333')
        ax.yaxis.label.set_color('#888')
        ax.set_title(f'{sym} — Daily + Fibonacci (${current_price:.2f})',
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


def _build_stock_report(sym: str, stock: dict, enriched: dict) -> tuple[str, Path | None]:
    """Build full stock report text + fib chart image.

    Returns (report_text, chart_path_or_None).
    """
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

    # VWAP line
    vwap = stock.get('vwap', 0)
    if vwap > 0:
        above = price > vwap
        vwap_icon = "🟢" if above else "🔴"
        vwap_label = "מעל" if above else "מתחת"
        lines.append(f"{vwap_icon} VWAP: ${vwap:.2f} — מחיר {vwap_label} ל-VWAP")

    # Unfilled gap detection from daily data (threshold: $0.05)
    df_daily = ma_frames.get('D')
    if df_daily is not None and len(df_daily) >= 2 and price > 0:
        n_d = len(df_daily)
        unfilled_gaps = []
        for i in range(1, n_d):
            pc = df_daily.iloc[i - 1]['close']
            co = df_daily.iloc[i]['open']
            g_abs = co - pc
            if abs(g_abs) < 0.05 or pc <= 0:
                continue
            # Check if any subsequent bar filled this gap
            filled = False
            for j in range(i, n_d):
                if g_abs > 0 and df_daily.iloc[j]['low'] <= pc:
                    filled = True
                    break
                elif g_abs < 0 and df_daily.iloc[j]['high'] >= pc:
                    filled = True
                    break
            if not filled:
                g_pct = g_abs / pc * 100
                unfilled_gaps.append((g_abs, g_pct, co, pc))
        if unfilled_gaps:
            lines.append(f"🕳️ גאפים פתוחים ({len(unfilled_gaps)}):")
            for g_abs, g_pct, co, pc in unfilled_gaps[-3:]:  # show last 3
                bottom, top = (pc, co) if g_abs > 0 else (co, pc)
                icon = "⬆️" if g_abs > 0 else "⬇️"
                lines.append(f"  {icon} ${bottom:.2f}—${top:.2f} ({g_pct:+.1f}%)")

    # Resist line
    if resist_str:
        lines.append(f"📉 Resist: {resist_str}")
    else:
        lines.append("✅ אין התנגדויות — מחיר מעל כל הממוצעים")

    # Fibonacci levels (text) — chart layout: highest at top
    fib_text = _format_fib_text(sym, price)
    if fib_text:
        lines.append("")
        lines.append(fib_text.lstrip("\n"))

    # News (Hebrew)
    if enriched['news']:
        lines.append("")
        lines.append(f"📰 <b>חדשות:</b>")
        for n in enriched['news']:
            src = n.get('source', '')
            src_tag = f" [{src}]" if src else ""
            lines.append(f"  • {n['title_he']}  <i>({n['date']}{src_tag})</i>")

    text = "\n".join(lines)

    # ── Fib chart image ──
    chart_path: Path | None = None
    cached = _fib_cache.get(sym)
    if cached:
        all_levels = cached[2]
        ratio_map = cached[3]
        df_daily = ma_frames.get('D')
        if df_daily is not None:
            chart_path = generate_fib_chart(sym, df_daily, all_levels, price,
                                            ratio_map=ratio_map)

    return text, chart_path


def _send_stock_report(sym: str, stock: dict, enriched: dict):
    """Send comprehensive Telegram report for a newly discovered stock."""
    text, chart_path = _build_stock_report(sym, stock, enriched)
    send_telegram(text)
    if chart_path:
        send_telegram_photo(chart_path, f"📐 {sym} — Daily + Fibonacci ${stock['price']:.2f}")


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
    cached = _fib_cache.get(symbol)
    if cached:
        anchor_low, anchor_high, all_levels, _ratio_map, *_ = cached
        # Invalidate cache if price exceeded the top cached level (needs re-advance)
        if all_levels and current_price > all_levels[-1]:
            log.info(f"Fib cache invalidated for {symbol}: price ${current_price:.2f} > top level ${all_levels[-1]:.4f}")
            del _fib_cache[symbol]
            cached = None

    if not cached:
        df = _download_daily(symbol)
        if df is None:
            return [], []

        anchor = find_anchor_candle(df)
        if anchor is None:
            return [], []

        anchor_low, anchor_high, anchor_date = anchor

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

        _fib_cache[symbol] = (anchor_low, anchor_high, all_levels, ratio_map, anchor_date)

    below = [l for l in all_levels if l <= current_price][-5:]
    above = [l for l in all_levels if l > current_price][:10]
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

def send_telegram(text: str, reply_markup: dict | None = None) -> bool:
    """Send text message to personal chat and group (if configured).

    ``reply_markup`` can be an InlineKeyboardMarkup dict for buttons.
    """
    if not BOT_TOKEN or not CHAT_ID:
        return False
    ok = False
    for cid in [CHAT_ID, GROUP_CHAT_ID]:
        if not cid:
            continue
        try:
            payload: dict = {'chat_id': cid, 'text': text, 'parse_mode': 'HTML'}
            if reply_markup:
                payload['reply_markup'] = reply_markup
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json=payload, timeout=10,
            )
            if cid == CHAT_ID:
                ok = resp.ok
        except Exception as e:
            log.error(f"Telegram ({cid}): {e}")
    return ok


def _make_lookup_button(sym: str) -> dict:
    """Build InlineKeyboardMarkup with a 'Full Report' button for a symbol."""
    return {
        'inline_keyboard': [[
            {'text': f'📊 דוח מלא — {sym}', 'callback_data': f'lookup:{sym}'},
        ]]
    }


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


def send_telegram_to(chat_id: str, text: str, reply_to: int | None = None) -> bool:
    """Send text message to a specific chat (for lookup replies)."""
    if not BOT_TOKEN:
        return False
    try:
        payload: dict = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}
        if reply_to:
            payload['reply_to_message_id'] = reply_to
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json=payload, timeout=10,
        )
        return resp.ok
    except Exception as e:
        log.error(f"Telegram send_to ({chat_id}): {e}")
        return False


def send_telegram_photo_to(chat_id: str, image_path: Path, caption: str = "",
                           reply_to: int | None = None) -> bool:
    """Send photo to a specific chat (for lookup replies)."""
    if not BOT_TOKEN:
        return False
    try:
        data: dict = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'HTML'}
        if reply_to:
            data['reply_to_message_id'] = reply_to
        with open(image_path, 'rb') as photo:
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                data=data, files={'photo': photo}, timeout=30,
            )
        return resp.ok
    except Exception as e:
        log.error(f"Telegram photo_to ({chat_id}): {e}")
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
#  Telegram Listener (incoming messages → stock lookups)
# ══════════════════════════════════════════════════════════

class TelegramListenerThread(threading.Thread):
    """Poll Telegram getUpdates for /stock commands and @mentions."""

    _COOLDOWN_SECS = 60

    def __init__(self, lookup_queue: queue.Queue):
        super().__init__(daemon=True)
        self.lookup_queue = lookup_queue
        self.running = False
        self._offset = 0
        self._bot_username: str = ""
        self._cooldowns: dict[str, float] = {}  # symbol -> last lookup time

    def stop(self):
        self.running = False

    def _get_bot_username(self):
        """Call getMe to learn the bot's username for @mention detection."""
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getMe",
                timeout=10,
            )
            if resp.ok:
                data = resp.json()
                self._bot_username = data.get('result', {}).get('username', '')
                log.info(f"Telegram bot username: @{self._bot_username}")
        except Exception as e:
            log.warning(f"getMe failed: {e}")

    def run(self):
        if not BOT_TOKEN:
            log.warning("TelegramListener: no BOT_TOKEN, exiting")
            return
        self.running = True
        self._get_bot_username()
        log.info("TelegramListener started")
        while self.running:
            try:
                self._poll()
            except Exception as e:
                log.error(f"TelegramListener poll error: {e}")
                time_mod.sleep(5)

    def _poll(self):
        """Long-poll getUpdates and parse stock requests."""
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={'offset': self._offset, 'timeout': 30},
                timeout=35,
            )
        except requests.exceptions.Timeout:
            return
        except Exception as e:
            log.warning(f"getUpdates error: {e}")
            time_mod.sleep(3)
            return

        if not resp.ok:
            log.warning(f"getUpdates failed: {resp.status_code} {resp.text[:200]}")
            time_mod.sleep(3)
            return

        updates = resp.json().get('result', [])
        for update in updates:
            self._offset = update['update_id'] + 1

            # ── Handle inline button callbacks ──
            cb = update.get('callback_query')
            if cb:
                self._handle_callback(cb)
                continue

            msg = update.get('message')
            if not msg:
                continue
            text = msg.get('text', '').strip()
            if not text:
                continue
            chat_id = str(msg['chat']['id'])
            message_id = msg['message_id']

            symbol = self._parse_symbol(text)
            if not symbol:
                continue

            # Rate limit check
            now = time_mod.time()
            last = self._cooldowns.get(symbol, 0)
            if now - last < self._COOLDOWN_SECS:
                remaining = int(self._COOLDOWN_SECS - (now - last))
                send_telegram_to(
                    chat_id,
                    f"⏳ <b>{symbol}</b> — נבדק לאחרונה. נסה שוב עוד {remaining} שניות.",
                    reply_to=message_id,
                )
                continue

            self._cooldowns[symbol] = now
            self.lookup_queue.put({
                'chat_id': chat_id,
                'message_id': message_id,
                'symbol': symbol,
            })
            log.info(f"TelegramListener: queued lookup for {symbol} (chat={chat_id})")

    def _handle_callback(self, cb: dict):
        """Handle inline keyboard callback (e.g. 'lookup:AAPL')."""
        cb_id = cb.get('id', '')
        data = cb.get('data', '')
        chat_id = str(cb.get('message', {}).get('chat', {}).get('id', ''))
        message_id = cb.get('message', {}).get('message_id', 0)

        # Answer the callback to remove the loading spinner
        try:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
                json={'callback_query_id': cb_id, 'text': '🔍 טוען דוח...'},
                timeout=5,
            )
        except Exception:
            pass

        if data.startswith('lookup:'):
            sym = data.split(':', 1)[1].upper()
            if sym.isalpha() and 1 <= len(sym) <= 5:
                # Rate limit check
                now = time_mod.time()
                last = self._cooldowns.get(sym, 0)
                if now - last < self._COOLDOWN_SECS:
                    remaining = int(self._COOLDOWN_SECS - (now - last))
                    send_telegram_to(
                        chat_id,
                        f"⏳ <b>{sym}</b> — נבדק לאחרונה. נסה שוב עוד {remaining} שניות.",
                    )
                    return
                self._cooldowns[sym] = now
                self.lookup_queue.put({
                    'chat_id': chat_id,
                    'message_id': message_id,
                    'symbol': sym,
                })
                log.info(f"TelegramListener: callback lookup for {sym} (chat={chat_id})")

    def _parse_symbol(self, text: str) -> str | None:
        """Extract stock symbol from /SYM, /stock SYM, or @mention SYM."""
        # /stock AAPL  or  /stock@botname AAPL
        if text.startswith('/stock'):
            parts = text.split()
            if len(parts) >= 2:
                sym = parts[1].upper()
                if sym.isalpha() and 1 <= len(sym) <= 5:
                    return sym
            return None

        # /AAPL or /AAPL@botname — direct ticker command
        _IGNORE_CMDS = {'start', 'help', 'stop', 'settings', 'menu', 'stock'}
        if text.startswith('/'):
            cmd = text.split()[0][1:]  # remove leading /
            if '@' in cmd:
                cmd = cmd.split('@')[0]
            if cmd.lower() not in _IGNORE_CMDS:
                sym = cmd.upper()
                if sym.isalpha() and 1 <= len(sym) <= 5:
                    return sym

        # @botname AAPL
        if self._bot_username and f'@{self._bot_username}' in text:
            cleaned = text.replace(f'@{self._bot_username}', '').strip()
            parts = cleaned.split()
            if parts:
                sym = parts[0].upper()
                if sym.isalpha() and 1 <= len(sym) <= 5:
                    return sym

        return None


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
        # ── FIB DT Auto-Strategy ──
        self._fib_dt_strategy = FibDTLiveStrategySync(ib_getter=_get_ibkr)
        self._fib_dt_entry = FibDTLiveEntrySync(
            ib_getter=_get_ibkr,
            strategy=self._fib_dt_strategy,
            buying_power_getter=self._get_net_liq,
            send_telegram_fn=send_telegram,
        )
        self._fib_dt_current_sym: str | None = None  # best turnover symbol
        # Cache scanner contracts for FIB DT (symbol -> Contract)
        self._scanner_contracts: dict[str, object] = {}
        self._cached_buying_power: float = 0.0
        self._cached_net_liq: float = 0.0
        self._cached_positions: dict[str, tuple] = {}  # sym → (qty, avg, mkt, pnl)
        # Track FIB DT open positions for order fill monitoring
        self._fib_dt_positions: dict[str, dict] = {}  # symbol -> entry_info
        # ── Telegram stock lookup ──
        self._lookup_queue: queue.Queue = queue.Queue()
        self._telegram_listener: TelegramListenerThread | None = None

    def _get_net_liq(self) -> float:
        """Return cached NetLiquidation for position sizing."""
        return self._cached_net_liq

    def stop(self):
        self.running = False
        if self._telegram_listener:
            self._telegram_listener.stop()

    def run(self):
        # Ensure asyncio event loop exists in this thread (ib_insync needs one)
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        self.running = True
        # Start Telegram listener for stock lookups
        self._telegram_listener = TelegramListenerThread(self._lookup_queue)
        self._telegram_listener.start()
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
                self._process_lookup_queue()
                _check_news_updates(self.previous)
                time_mod.sleep(1)

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

    def _process_lookup_queue(self):
        """Process pending Telegram stock lookup requests."""
        while not self._lookup_queue.empty():
            try:
                req = self._lookup_queue.get_nowait()
                self._handle_stock_lookup(req)
            except queue.Empty:
                break

    def _handle_stock_lookup(self, req: dict):
        """Handle a stock lookup request from Telegram.

        Fetches price from IBKR, enriches via Finviz + Fib,
        and sends a full report back to the requesting chat.
        """
        sym = req['symbol']
        chat_id = req['chat_id']
        message_id = req['message_id']

        send_telegram_to(chat_id, f"🔍 מחפש מידע על <b>{sym}</b>...", reply_to=message_id)

        ib = _get_ibkr()
        if not ib:
            send_telegram_to(chat_id, f"❌ <b>{sym}</b> — IBKR לא מחובר", reply_to=message_id)
            return

        # ── Get current price from IBKR ──
        try:
            contract = Stock(sym, 'SMART', 'USD')
            ib.qualifyContracts(contract)
            if not contract.conId:
                send_telegram_to(chat_id, f"❌ <b>{sym}</b> — סימבול לא נמצא", reply_to=message_id)
                return
            bars = ib.reqHistoricalData(
                contract, endDateTime='', durationStr='2 D',
                barSizeSetting='1 day', whatToShow='TRADES', useRTH=True,
            )
            if not bars:
                send_telegram_to(chat_id, f"❌ <b>{sym}</b> — לא נמצאו נתונים", reply_to=message_id)
                return

            last_bar = bars[-1]
            volume = last_bar.volume
            session = _get_market_session()

            price, prev_close, ext_high, ext_low, ext_vwap = \
                _fetch_extended_hours_price(ib, contract, session, bars)

            pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
            vol_str = _format_volume(volume)
            vwap = round(last_bar.average, 4) if last_bar.average else 0.0
            if ext_vwap is not None:
                vwap = round(ext_vwap, 4)

            stock_data = {
                'price': round(price, 2),
                'pct': round(pct, 1),
                'volume': vol_str,
                'volume_raw': int(volume),
                'vwap': vwap,
                'prev_close': round(prev_close, 4),
            }
        except Exception as e:
            send_telegram_to(chat_id, f"❌ <b>{sym}</b> — שגיאה: {e}", reply_to=message_id)
            return

        # ── Enrich (Finviz + Fib) ──
        # Force refresh on explicit lookup
        enriched = _enrich_stock(sym, price, force=True)

        # ── Build and send report ──
        try:
            text, chart_path = _build_stock_report(sym, stock_data, enriched)
            # Replace the 🆕 prefix with 🔎 for lookups
            text = text.replace("🆕", "🔎", 1)
            send_telegram_to(chat_id, text, reply_to=message_id)
            if chart_path:
                send_telegram_photo_to(
                    chat_id, chart_path,
                    f"📐 {sym} — Daily + Fibonacci ${price:.2f}",
                    reply_to=message_id,
                )
        except Exception as e:
            log.error(f"Stock lookup report {sym}: {e}")
            send_telegram_to(chat_id, f"❌ <b>{sym}</b> — שגיאה בבניית הדוח: {e}", reply_to=message_id)

        log.info(f"Stock lookup completed: {sym} → chat={chat_id}")

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
            oca_group = f"FibDT_{sym}_{int(time_mod.time())}"

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
            now_et = datetime.now(_ET).strftime('%Y-%m-%d %H:%M:%S ET')
            send_telegram(
                f"📐 <b>FIB DT Entry</b>\n"
                f"  🕐 {now_et}\n"
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
        if not ib:
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
            # Cache for FIB DT position sizing (use NetLiq, not margin-inflated BuyingPower)
            self._cached_buying_power = buying_power
            self._cached_net_liq = net_liq
            log.info(f"Account: NetLiq=${net_liq:,.0f} BuyingPower=${buying_power:,.0f} (sizing uses NetLiq)")

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

            self._cached_positions = positions
            if self.on_account:
                self.on_account(net_liq, buying_power, positions)
        except Exception as e:
            log.debug(f"Account fetch: {e}")

    def _run_fib_dt_cycle(self, current: dict, status: str):
        """Run FIB DT auto-strategy: select best stock by turnover, feed to strategy."""
        try:
            # ── 1. Build ranked candidate list ──
            candidates = []
            for sym, d in current.items():
                pct = d.get('pct', 0)
                price = d.get('price', 0)
                vwap = d.get('vwap', 0)
                volume_raw = d.get('volume_raw', 0)
                prev_close = d.get('prev_close', 0)
                contract = d.get('contract')

                if pct < 20 or price <= 0 or prev_close <= 0 or not contract:
                    continue
                if vwap > 0 and price <= vwap:
                    continue

                # Get float from enrichment cache
                enrich = _enrichment.get(sym, {})
                flt_str = enrich.get('float', '-')
                flt_shares = _parse_float_to_shares(flt_str)
                if flt_shares <= 0 or flt_shares >= 70_000_000:
                    continue
                if volume_raw <= 0:
                    continue

                turnover = volume_raw / flt_shares
                candidates.append((sym, turnover, d, flt_shares))

            if not candidates:
                return

            # Sort by turnover descending
            candidates.sort(key=lambda x: x[1], reverse=True)
            best_sym, best_turnover, best_data, best_float = candidates[0]

            # Log ranking (top 3)
            top3 = candidates[:3]
            ranking_str = " | ".join(
                f"{s} {t:.2f}x" for s, t, _, _ in top3
            )
            log.info(f"FIB DT turnover ranking: {ranking_str}")

            # ── 2. Build GapSignal for best candidate ──
            gap_signal = GapSignal(
                symbol=best_sym,
                contract=best_data['contract'],
                gap_pct=best_data['pct'],
                prev_close=best_data['prev_close'],
                current_price=best_data['price'],
                float_shares=best_float,
            )

            if self._fib_dt_current_sym != best_sym:
                log.info(
                    f"FIB DT: tracking {best_sym} "
                    f"(turnover={best_turnover:.2f}x, "
                    f"+{best_data['pct']:.1f}%, "
                    f"float={_enrichment.get(best_sym, {}).get('float', '?')})"
                )
                self._fib_dt_current_sym = best_sym

            # ── 3. Run strategy cycle ──
            entry_requests, trailing_exits = self._fib_dt_strategy.process_cycle(
                [gap_signal]
            )

            # ── 4. Execute entries ──
            for req in entry_requests:
                now_et = datetime.now(_ET).strftime('%Y-%m-%d %H:%M:%S ET')
                log.info(
                    f"FIB DT ENTRY SIGNAL [{now_et}]: {req.symbol} "
                    f"fib=${req.fib_level:.4f} ratio={req.fib_ratio} "
                    f"stop=${req.stop_price:.4f} target=${req.target_price:.4f}"
                )
                success = self._fib_dt_entry.execute_entry(req)
                if success:
                    log.info(f"FIB DT: Entry executed for {req.symbol} [{now_et}]")
                    # Track position for order fill monitoring
                    info = self._fib_dt_entry.last_entry_info
                    if info:
                        self._fib_dt_positions[req.symbol] = dict(info)
                        log.info(f"FIB DT: Tracking position {req.symbol} for fill monitoring")
                else:
                    log.warning(f"FIB DT: Entry failed for {req.symbol} [{now_et}]")

            # ── 5. Execute trailing exits ──
            for exit_sig in trailing_exits:
                now_et = datetime.now(_ET).strftime('%Y-%m-%d %H:%M:%S ET')
                log.info(f"FIB DT TRAILING EXIT [{now_et}]: {exit_sig.symbol} — {exit_sig.reason}")
                self._fib_dt_entry.execute_trailing_exit(exit_sig)
                # Clean up tracking
                if exit_sig.symbol in self._fib_dt_positions:
                    self._fib_dt_positions[exit_sig.symbol]['phase'] = 'CLOSED'

        except Exception as e:
            log.error(f"FIB DT cycle error: {e}")

    def _monitor_fib_dt_positions(self):
        """Monitor FIB DT positions for order fills and position changes.

        Detects:
        - OCA target filled → mark_trailing (start no-new-high exit for remaining half)
        - OCA stop filled → half stopped (other half still has its own stop)
        - Position qty → 0 → fully closed, clean up
        """
        if not self._fib_dt_positions:
            return

        ib = _get_ibkr()
        if not ib:
            return

        # Build lookup: symbol → actual position qty
        pos_by_sym: dict[str, int] = {}
        try:
            for item in ib.portfolio():
                if item.position > 0:
                    pos_by_sym[item.contract.symbol] = int(item.position)
        except Exception as e:
            log.debug(f"FIB DT monitor: portfolio fetch error: {e}")
            return

        closed_symbols = []

        for sym, info in self._fib_dt_positions.items():
            phase = info.get('phase', '')
            if phase == 'CLOSED':
                closed_symbols.append(sym)
                continue

            actual_qty = pos_by_sym.get(sym, 0)

            # ── Position fully closed ──
            if actual_qty == 0:
                now_str = datetime.now(_ET).strftime('%Y-%m-%d %H:%M:%S ET')
                log.info(f"FIB DT MONITOR: {sym} position closed (qty=0)")
                self._fib_dt_strategy.mark_position_closed(sym)
                info['phase'] = 'CLOSED'
                closed_symbols.append(sym)

                # Cancel any remaining open orders for this symbol
                self._cancel_symbol_orders(ib, sym)

                if self._send_telegram_fn:
                    self._send_telegram_fn(
                        f"📐 <b>FIB DT Position Closed</b>\n"
                        f"  🕐 {now_str}\n"
                        f"  {sym} — all shares sold\n"
                        f"  Entry was ${info.get('entry_price', 0):.2f}"
                    )
                continue

            # ── Check order fills for IN_POSITION phase ──
            if phase == 'IN_POSITION':
                expected_qty = info.get('qty', 0)

                # If qty decreased, an exit order filled
                if actual_qty < expected_qty:
                    # Check which order filled
                    oca_target_trade = info.get('oca_target_trade')
                    oca_stop_trade = info.get('oca_stop_trade')

                    target_filled = (
                        oca_target_trade and
                        oca_target_trade.orderStatus.status == 'Filled'
                    )
                    stop_filled = (
                        oca_stop_trade and
                        oca_stop_trade.orderStatus.status == 'Filled'
                    )

                    if target_filled:
                        now_str = datetime.now(_ET).strftime('%Y-%m-%d %H:%M:%S ET')
                        half = info.get('half', 0)
                        target_px = info.get('target_price', 0)
                        entry_px = info.get('entry_price', 0)
                        profit = (target_px - entry_px) * half
                        log.info(
                            f"FIB DT MONITOR: {sym} OCA TARGET filled — "
                            f"{half}sh @ ${target_px:.2f} (+${profit:.2f})"
                        )
                        self._fib_dt_strategy.mark_trailing(sym)
                        info['phase'] = 'TRAILING'

                        if self._send_telegram_fn:
                            self._send_telegram_fn(
                                f"📐 <b>FIB DT Target Hit</b> 🎯\n"
                                f"  🕐 {now_str}\n"
                                f"  {sym}: {half}sh sold @ ${target_px:.2f}\n"
                                f"  Profit: +${profit:.2f}\n"
                                f"  Trailing {info.get('other_half', 0)}sh remaining"
                            )

                    elif stop_filled:
                        now_str = datetime.now(_ET).strftime('%Y-%m-%d %H:%M:%S ET')
                        half = info.get('half', 0)
                        stop_px = info.get('stop_price', 0)
                        entry_px = info.get('entry_price', 0)
                        loss = (stop_px - entry_px) * half
                        log.info(
                            f"FIB DT MONITOR: {sym} OCA STOP filled — "
                            f"{half}sh @ ${stop_px:.2f} (${loss:.2f})"
                        )
                        info['phase'] = 'HALF_STOPPED'

                        if self._send_telegram_fn:
                            self._send_telegram_fn(
                                f"📐 <b>FIB DT Half Stopped</b> 🛑\n"
                                f"  🕐 {now_str}\n"
                                f"  {sym}: {half}sh stopped @ ${stop_px:.2f}\n"
                                f"  Loss: ${loss:.2f}\n"
                                f"  {info.get('other_half', 0)}sh still held (stop active)"
                            )

            # ── HALF_STOPPED: other half still has solo stop, just watch for close ──
            # (actual_qty == 0 is already handled above)

        # Clean up fully closed positions
        for sym in closed_symbols:
            del self._fib_dt_positions[sym]

    @property
    def _send_telegram_fn(self):
        """Return the telegram send function."""
        return self._fib_dt_entry._send_telegram

    @staticmethod
    def _cancel_symbol_orders(ib: IB, symbol: str):
        """Cancel all open orders for a given symbol."""
        try:
            for trade_obj in ib.openTrades():
                if trade_obj.contract.symbol == symbol:
                    try:
                        ib.cancelOrder(trade_obj.order)
                        log.info(f"FIB DT: Cancelled order {trade_obj.order.orderId} for {symbol}")
                    except Exception:
                        pass
        except Exception as e:
            log.debug(f"FIB DT: Error cancelling orders for {symbol}: {e}")

    @staticmethod
    def _merge_stocks(current: dict) -> dict:
        """Merge scan data with cached enrichment for GUI display."""
        merged = {}
        for sym, d in current.items():
            merged[sym] = dict(d)
            if sym in _enrichment:
                merged[sym]['enrich'] = _enrichment[sym]
        return merged

    def _refresh_enrichment_fibs(self, current: dict):
        """Re-partition fib_below/fib_above in enrichment cache using current prices."""
        for sym, d in current.items():
            if sym not in _enrichment:
                continue
            price = d.get('price', 0)
            if price <= 0:
                continue
            try:
                below, above = calc_fib_levels(sym, price)
                _enrichment[sym]['fib_below'] = below
                _enrichment[sym]['fib_above'] = above
            except Exception as e:
                log.debug(f"Fib refresh {sym}: {e}")

    def _cycle(self):
        _check_market_reminders()
        _reset_alerts_if_new_day()
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
                # Filter: only send Telegram if above VWAP + ≥20% + float <70M
                vwap = d.get('vwap', 0)
                pct = d.get('pct', 0)
                price = d.get('price', 0)
                flt_shares = _parse_float_to_shares(_enrichment[sym].get('float', '-'))
                flt_ok = 0 < flt_shares < 70_000_000
                above_vwap = vwap > 0 and price > vwap
                above_pct = pct >= 20

                if above_vwap and above_pct and flt_ok:
                    _send_stock_report(sym, d, _enrichment[sym])
                    file_logger.log_alert(ts, {
                        'type': 'new', 'symbol': sym,
                        'price': d['price'], 'pct': d['pct'],
                        'volume': d.get('volume', ''),
                        'msg': f"🆕 {sym}: ${d['price']:.2f} {d['pct']:+.1f}%",
                    })
                else:
                    log.info(f"Filtered {sym}: pct={pct:+.1f}% vwap={'above' if above_vwap else 'below'} float={_enrichment[sym].get('float', '-')}")

            enriched_count += 1
            # Live-update GUI after each enrichment
            if self.on_stocks:
                self.on_stocks(self._merge_stocks(current))
            # Process any pending Telegram lookups between enrichments
            self._process_lookup_queue()
            if not self.running:
                return

        if enriched_count:
            status += f"  +{enriched_count} enriched"

        # ── Baseline: send single summary to Telegram ──
        if is_baseline and current:
            top5 = sorted(current.items(), key=lambda x: x[1]['pct'], reverse=True)[:5]
            session_label = {'pre_market': 'Pre-Market', 'market': 'Market',
                             'after_hours': 'After-Hours', 'closed': 'Closed'}
            sess = session_label.get(_get_market_session(), 'Unknown')
            summary_lines = [f"📡 <b>Scanner started</b> [{sess}] — {len(current)} stocks"]
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

        # ── Milestone alerts (+5% steps, including corrections) ──
        for sym, d in current.items():
            ms_msg = check_milestone(sym, d['pct'], d['price'])
            if ms_msg:
                spike, ratio = _check_1min_volume_spike(sym)
                if spike:
                    send_telegram(ms_msg)
                    status += f"  📈{sym}"
                else:
                    log.info(f"Milestone {sym} skipped: 1min vol ratio {ratio}x < {MILESTONE_VOL_RATIO}x")

        # ── Volume anomaly checks ──
        for sym, d in current.items():
            if sym in _enrichment and d.get('volume_raw', 0) > 0:
                vol_msg = check_volume_anomaly(sym, d['volume_raw'], _enrichment[sym])
                if vol_msg:
                    send_telegram(vol_msg)
                    status += f"  🔥{sym}"

        # ── Real-time alerts (5 types, score-filtered + multi-signal) ──
        if not is_baseline and current:
            # Update daily volume peaks for all stocks in this cycle
            for sym, d in current.items():
                vol_raw = d.get('volume_raw', 0)
                if vol_raw > _daily_volume_peak.get(sym, 0):
                    _daily_volume_peak[sym] = vol_raw

            # Find top-3 volume stocks for badge
            vol_top3 = set()
            if _daily_volume_peak:
                sorted_vol = sorted(_daily_volume_peak.items(), key=lambda x: x[1], reverse=True)
                vol_top3 = {s for s, _ in sorted_vol[:3]}

            for sym, d in current.items():
                price = d['price']
                pct = d.get('pct', 0)
                score, reasons = _calc_alert_score(sym, d)

                # Collect signals for this stock this cycle
                signals: list[str] = []

                btn = _make_lookup_button(sym)

                # Build badges: volume rank + repeat alerts
                badges = []
                if sym in vol_top3:
                    rank = [s for s, _ in sorted(_daily_volume_peak.items(), key=lambda x: x[1], reverse=True)[:3]].index(sym) + 1
                    vol_val = _daily_volume_peak[sym]
                    if vol_val >= 1_000_000:
                        vol_fmt = f"{vol_val / 1_000_000:.1f}M"
                    elif vol_val >= 1_000:
                        vol_fmt = f"{vol_val / 1_000:.0f}K"
                    else:
                        vol_fmt = str(vol_val)
                    badges.append(f"📊 #{rank} ווליום היום ({vol_fmt})")
                prev_alerts = _daily_alert_count.get(sym, 0)
                if prev_alerts >= 2:
                    badges.append(f"🔄 ×{prev_alerts} התראות היום")

                # Build score line with reasons + badges
                reason_str = f" ({', '.join(reasons)})" if reasons else ""
                score_line = f"\n🏆 ניקוד: {score}/100{reason_str}"
                if badges:
                    score_line += "\n" + " | ".join(badges)

                # 1. HOD break
                if self.previous and sym in self.previous:
                    hod_msg = check_hod_break(sym, d, self.previous[sym])
                    if hod_msg:
                        signals.append("HOD")
                        if score >= ALERT_MIN_SCORE:
                            _daily_alert_count[sym] = _daily_alert_count.get(sym, 0) + 1
                            send_telegram(hod_msg + score_line, reply_markup=btn)
                        else:
                            log.info(f"Alert filtered {sym} HOD: score {score} < {ALERT_MIN_SCORE}")
                # 2. Fib 2nd touch
                fib2_msg = check_fib_second_touch(sym, price, pct)
                if fib2_msg:
                    signals.append("FIB×2")
                    if score >= ALERT_MIN_SCORE:
                        _daily_alert_count[sym] = _daily_alert_count.get(sym, 0) + 1
                        send_telegram(fib2_msg + score_line, reply_markup=btn)
                    else:
                        log.info(f"Alert filtered {sym} FIB×2: score {score} < {ALERT_MIN_SCORE}")
                # 3. LOD 2nd touch
                lod_msg = check_lod_touch(sym, price, d.get('day_low', 0), pct)
                if lod_msg:
                    signals.append("LOD×2")
                    if score >= ALERT_MIN_SCORE:
                        _daily_alert_count[sym] = _daily_alert_count.get(sym, 0) + 1
                        send_telegram(lod_msg + score_line, reply_markup=btn)
                    else:
                        log.info(f"Alert filtered {sym} LOD×2: score {score} < {ALERT_MIN_SCORE}")
                # 4. VWAP cross
                vwap_msg = check_vwap_cross(sym, price, d.get('vwap', 0), pct)
                if vwap_msg:
                    signals.append("VWAP")
                    if score >= ALERT_MIN_SCORE:
                        _daily_alert_count[sym] = _daily_alert_count.get(sym, 0) + 1
                        send_telegram(vwap_msg + score_line, reply_markup=btn)
                    else:
                        log.info(f"Alert filtered {sym} VWAP: score {score} < {ALERT_MIN_SCORE}")
                # 5. 1-min spike
                spike_msg = check_1min_spike(sym, price, pct)
                if spike_msg:
                    signals.append("SPIKE")
                    if score >= ALERT_MIN_SCORE:
                        _daily_alert_count[sym] = _daily_alert_count.get(sym, 0) + 1
                        send_telegram(spike_msg + score_line, reply_markup=btn)
                    else:
                        log.info(f"Alert filtered {sym} SPIKE: score {score} < {ALERT_MIN_SCORE}")

                # ── Multi-signal alert ──
                if len(signals) >= MULTI_SIGNAL_MIN and sym not in _multi_signal_alerted:
                    _multi_signal_alerted.add(sym)
                    enrich = _enrichment.get(sym, {})
                    flt = enrich.get('float', '-')
                    short = enrich.get('short', '-')
                    badge_line = ("\n" + " | ".join(badges)) if badges else ""
                    send_telegram(
                        f"🔥🔥🔥 <b>MULTI-SIGNAL — {sym}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"⚡ {len(signals)} סיגנלים: {' + '.join(signals)}\n"
                        f"💰 ${price:.2f} | שינוי: {pct:+.1f}%\n"
                        f"📊 Float: {flt} | Short: {short} | RVOL: {d.get('rvol', 0)}x\n"
                        f"🏆 ניקוד: {score}/100 ({', '.join(reasons)})"
                        f"{badge_line}",
                        reply_markup=btn,
                    )

        # ── Fetch account data before FIB DT (needs buying power) ──
        self._fetch_account_data()

        # ── Monitor FIB DT open positions for fills ──
        self._monitor_fib_dt_positions()

        # ── FIB DT Auto-Strategy ──
        self._run_fib_dt_cycle(current, status)

        # ── Refresh fib partition with current prices ──
        self._refresh_enrichment_fibs(current)

        # ── Daily stats tracking + end-of-day summary ──
        new_count = len(set(current) - set(self.previous)) if self.previous else 0
        _track_daily_stats(current, new_count=new_count)
        _check_daily_summary(positions=self._cached_positions)

        # ── Final GUI update with all enrichment ──
        merged = self._merge_stocks(current)
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

        # Qty = 100% of NetLiq (not margin-inflated BuyingPower)
        bp = self._cached_net_liq
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
