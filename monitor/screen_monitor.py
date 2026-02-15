"""
Screen Monitor — Silent screen capture, OCR, anomaly detection.
Only alerts via Telegram when something unusual happens.

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

import csv
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
import tkinter as tk
from tkinter import ttk
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from PIL import Image, ImageGrab, ImageTk
from dotenv import load_dotenv
import pytesseract
import requests
from ib_insync import IB, Stock, util as ib_util
import yfinance as yf
from finvizfinance.quote import finvizfinance as Finviz
from deep_translator import GoogleTranslator

from strategies.fibonacci_engine import (
    find_anchor_candle, build_dual_series, advance_series,
)
from config.settings import FIB_LEVELS_24, IBKR_HOST, IBKR_PORT

# ── Config ────────────────────────────────────────────────
_config_dir = Path(__file__).parent / "config"
_env_path = _config_dir / ".env"
load_dotenv(_env_path) if _env_path.exists() else load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
CROP_PATH = DATA_DIR / "monitor_crop.png"
STATE_PATH = DATA_DIR / "monitor_state.json"
LOG_CSV = DATA_DIR / "monitor_log.csv"
LOG_TXT = DATA_DIR / "monitor_log.txt"

TICKER_RE = re.compile(r'\b([A-Z]{1,5})\b')
PCT_RE = re.compile(r'([+-]?\s*\d{1,4}\.?\d{0,2})\s*%')
NUM_MK_RE = re.compile(r'(\d{1,6}\.?\d{0,2})\s*([MmKk™])')

NOT_TICKERS = {
    'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL',
    'CAN', 'HAD', 'HER', 'WAS', 'ONE', 'OUR', 'OUT', 'HAS',
    'BUY', 'ASK', 'BID', 'VOL', 'AVG', 'CHG', 'PCT', 'MKT',
    'USD', 'ETF', 'IPO', 'EPS', 'CEO', 'SEC', 'NYSE', 'HIGH',
    'LOW', 'OPEN', 'CLOSE', 'LAST', 'PREV', 'NET', 'DAY',
    'PRE', 'POST', 'TOP', 'NEW', 'SYM', 'TIME', 'DATE', 'EST',
    'PM', 'AM', 'MIN', 'MAX', 'AVE', 'SUM', 'NUM', 'QTY',
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("monitor")


# ══════════════════════════════════════════════════════════
#  Silent Screenshot
# ══════════════════════════════════════════════════════════

_SCROT_PATH = "/tmp/_monitor_cap.png"


def take_screenshot(bbox: tuple[int,int,int,int] | None = None) -> Image.Image | None:
    """Silent screenshot. If bbox given, capture only that region via scrot."""
    try:
        if bbox:
            x, y, x2, y2 = bbox
            w, h = x2 - x, y2 - y
            r = subprocess.run(
                ["scrot", "-a", f"{x},{y},{w},{h}", _SCROT_PATH, "-o"],
                capture_output=True, timeout=5,
            )
            if r.returncode == 0:
                return Image.open(_SCROT_PATH)
        # Full screen fallback (for region selector)
        r = subprocess.run(
            ["scrot", _SCROT_PATH, "-o"],
            capture_output=True, timeout=5,
        )
        if r.returncode == 0:
            return Image.open(_SCROT_PATH)
    except Exception:
        pass
    # Last resort
    try:
        img = ImageGrab.grab()
        if bbox:
            return img.crop(bbox)
        return img
    except Exception as e:
        log.error(f"Screenshot failed: {e}")
        return None


# ══════════════════════════════════════════════════════════
#  OCR & Parsing
# ══════════════════════════════════════════════════════════

def ocr_image(img: Image.Image) -> str:
    w, h = img.size
    img = img.resize((w * 2, h * 2), Image.LANCZOS)
    gray = img.convert('L')
    try:
        return pytesseract.image_to_string(gray, config='--psm 6')
    except Exception as e:
        log.error(f"OCR: {e}")
        return ""


def parse_scanner_data(text: str) -> dict:
    stocks = {}
    for line in text.strip().split('\n'):
        if not line.strip():
            continue
        tickers = [t for t in TICKER_RE.findall(line)
                    if t not in NOT_TICKERS and len(t) >= 2]
        if not tickers:
            continue

        symbol = tickers[0]

        # Percentage
        pct = 0.0
        pcts = PCT_RE.findall(line)
        if pcts:
            try:
                pct = float(pcts[0].replace(' ', ''))
            except ValueError:
                pass

        # Price — remove %, M/K numbers, then find decimal or integer
        line_clean = re.sub(r'[+-]?\s*\d{1,4}\.?\d{0,2}\s*%', '', line)
        line_clean = re.sub(r'\d{1,6}\.?\d{0,2}\s*[MmKk™]', '', line_clean)
        line_clean = re.sub(r'\b[A-Z]{1,5}\b', '', line_clean)

        price_candidates = re.findall(r'(\d{1,5}\.\d{1,4})', line_clean)
        if price_candidates:
            price = float(price_candidates[0])
        else:
            int_candidates = re.findall(r'\b(\d{3,5})\b', line_clean)
            if int_candidates:
                raw = int_candidates[0]
                price = float(raw[0] + '.' + raw[1:]) if len(raw) == 4 else float('0.' + raw) if len(raw) == 3 else float(raw) / 100
            else:
                price = 0.0

        # Volume & Float (M/K numbers)
        mk = NUM_MK_RE.findall(line)
        volume = f"{mk[0][0]}{mk[0][1].upper().replace('™','M')}" if len(mk) > 0 else ''
        free_float = f"{mk[1][0]}{mk[1][1].upper().replace('™','M')}" if len(mk) > 1 else ''

        stocks[symbol] = {
            'price': price, 'pct': pct,
            'volume': volume, 'float': free_float,
            'raw': line.strip(),
        }
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
    news = info.get('news', [])

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
#  Fibonacci Levels (WTS Method)
# ══════════════════════════════════════════════════════════

# Cache: {symbol: (anchor_low, anchor_high, all_levels_sorted)}
_fib_cache: dict[str, tuple[float, float, list[float]]] = {}


_ibkr_fib: IB | None = None
_IBKR_FIB_CLIENT = 10


def _get_ibkr_fib() -> IB | None:
    """Get/create a dedicated IBKR connection for Fibonacci data."""
    global _ibkr_fib
    if _ibkr_fib and _ibkr_fib.isConnected():
        return _ibkr_fib
    try:
        _ibkr_fib = IB()
        _ibkr_fib.connect(IBKR_HOST, IBKR_PORT, clientId=_IBKR_FIB_CLIENT, timeout=5)
        log.info("IBKR Fib connection established")
        return _ibkr_fib
    except Exception as e:
        log.warning(f"IBKR Fib connect failed: {e}")
        _ibkr_fib = None
        return None


def _download_daily(symbol: str) -> pd.DataFrame | None:
    """Download 5 years daily data. IBKR first, yfinance fallback."""
    # Try IBKR
    ib = _get_ibkr_fib()
    if ib:
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
                    return df
        except Exception as e:
            log.warning(f"IBKR download {symbol}: {e}")

    # Fallback: yfinance
    try:
        df = yf.download(symbol, period='5y', interval='1d', prepost=True,
                         progress=False, timeout=10)
        if df.empty or len(df) < 5:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]
        log.info(f"yfinance fallback: {symbol} {len(df)} daily bars")
        return df
    except Exception as e:
        log.error(f"yfinance download {symbol}: {e}")
        return None


def calc_fib_levels(symbol: str, current_price: float) -> tuple[list[float], list[float]]:
    """Calculate Fibonacci levels using dual-series recursive method.

    Returns (3_below, 3_above) relative to current_price.
    Auto-advances when price > 4.236 of the LOWER series (S1).
    """
    if symbol in _fib_cache:
        anchor_low, anchor_high, all_levels = _fib_cache[symbol]
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

        for _ in range(25):
            for _, price in dual.series1.levels:
                all_levels.add(price)
            for _, price in dual.series2.levels:
                all_levels.add(price)

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

        _fib_cache[symbol] = (anchor_low, anchor_high, all_levels)

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

    _, _, all_levels = _fib_cache[symbol]
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


# ══════════════════════════════════════════════════════════
#  File Logger
# ══════════════════════════════════════════════════════════

class FileLogger:
    def __init__(self):
        if not LOG_CSV.exists():
            with open(LOG_CSV, 'w', newline='') as f:
                csv.writer(f).writerow(['timestamp', 'event', 'symbol', 'price', 'pct', 'volume', 'float', 'detail'])

    def log_scan(self, ts, stocks, raw_text):
        with open(LOG_TXT, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"SCAN: {ts}  |  {len(stocks)} symbols\n")
            for sym, d in sorted(stocks.items()):
                f.write(f"  {sym:<6} {d['pct']:>+7.1f}%  ${d['price']:<8.2f}  Vol:{d.get('volume',''):>8}  Float:{d.get('float','')}\n")
            f.write(f"OCR: {raw_text[:200]}...\n")

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
#  Window Tracker (wmctrl)
# ══════════════════════════════════════════════════════════

def list_windows() -> list[dict]:
    """List all open windows with geometry via wmctrl."""
    try:
        r = subprocess.run(['wmctrl', '-lG'], capture_output=True, text=True, timeout=5)
        if r.returncode != 0:
            return []
    except Exception:
        return []

    windows = []
    for line in r.stdout.strip().split('\n'):
        if not line.strip():
            continue
        parts = line.split(None, 7)
        if len(parts) < 8:
            continue
        wid = parts[0]
        x, y, w, h = int(parts[2]), int(parts[3]), int(parts[4]), int(parts[5])
        title = parts[7]

        if 'Screen Monitor' in title:
            continue

        windows.append({
            'wid': wid,
            'x': x, 'y': y, 'w': w, 'h': h,
            'title': title,
        })

    return windows


def get_window_bbox(wid: str) -> tuple[int,int,int,int] | None:
    """Get current bbox (x, y, x2, y2) for a window. None if gone/minimized."""
    try:
        r = subprocess.run(['wmctrl', '-lG'], capture_output=True, text=True, timeout=5)
        for line in r.stdout.strip().split('\n'):
            parts = line.split(None, 7)
            if len(parts) >= 6 and parts[0] == wid:
                x, y, w, h = int(parts[2]), int(parts[3]), int(parts[4]), int(parts[5])
                if w > 10 and h > 10 and x >= 0 and y >= 0:
                    return (x, y, x + w, y + h)
    except Exception:
        pass
    return None


def find_window_by_title(title: str) -> dict | None:
    """Find a window by title substring (for reconnecting after restart)."""
    for w in list_windows():
        if title in w['title']:
            return w
    return None


# ══════════════════════════════════════════════════════════
#  Monitor Thread
# ══════════════════════════════════════════════════════════

class MonitorThread(threading.Thread):
    def __init__(self, tracked_windows: list[dict], freq: int, on_status=None):
        super().__init__(daemon=True)
        self.tracked = tracked_windows  # [{'wid': ..., 'title': ...}, ...]
        self.freq = freq
        self.on_status = on_status
        self.running = False
        self.previous = {}
        self.count = 0

    def stop(self):
        self.running = False

    def run(self):
        self.running = True
        titles = [w['title'][:30] for w in self.tracked]
        log.info(f"Monitoring {len(self.tracked)} windows: {titles}")
        while self.running:
            try:
                self._cycle()
            except Exception as e:
                log.error(f"Error: {e}")
            for _ in range(self.freq):
                if not self.running: break
                time.sleep(1)

    def _capture_windows(self) -> tuple[dict, str]:
        """Capture all tracked windows, OCR each, merge stocks."""
        all_stocks = {}
        all_text = ""
        captured = 0

        for tw in self.tracked:
            bbox = get_window_bbox(tw['wid'])
            if not bbox:
                # Window might have restarted — try to find by title
                found = find_window_by_title(tw['title'])
                if found:
                    tw['wid'] = found['wid']
                    bbox = (found['x'], found['y'],
                            found['x'] + found['w'], found['y'] + found['h'])
                else:
                    continue

            crop = take_screenshot(bbox=bbox)
            if not crop:
                continue

            captured += 1
            text = ocr_image(crop)
            all_text += text + "\n"
            stocks = parse_scanner_data(text)
            all_stocks.update(stocks)

        log.debug(f"Captured {captured}/{len(self.tracked)} windows, {len(all_stocks)} stocks")
        return all_stocks, all_text

    def _cycle(self):
        current, raw_text = self._capture_windows()
        if not current and not self.previous:
            return

        self.count += 1
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for sym, d in current.items():
            stock_history.record(sym, d['price'], d['pct'])

        file_logger.log_scan(ts, current, raw_text)

        active = sum(1 for tw in self.tracked if get_window_bbox(tw['wid']))
        status = f"#{self.count}  {active}/{len(self.tracked)} win  {len(current)} sym"

        if self.previous and current:
            alerts = detect_anomalies(current, self.previous)
            if alerts:
                header = f"🔔 <b>Alert</b> — {datetime.now().strftime('%H:%M:%S')}\n"
                alert_lines = []
                news_msgs = []

                for a in alerts:
                    sym = a.get('symbol', '')
                    line = a['msg']

                    mom = stock_history.format_momentum(sym)
                    if mom:
                        line += f"\n   📊 {mom}"
                    alert_lines.append(line)

                    if a.get('fetch_news'):
                        price = a.get('price', 0)
                        try:
                            info = fetch_stock_info(sym)
                            # Order: Fundamentals → News → Fib
                            if info['fundamentals']:
                                news_msgs.append(format_stock_info(sym, info))
                            if info['news']:
                                nm = format_news_only(sym, info['news'])
                                if nm:
                                    news_msgs.append(nm)
                        except Exception as e:
                            log.error(f"Finviz error {sym}: {e}")
                        if price > 0:
                            try:
                                below, above = calc_fib_levels(sym, price)
                                if below or above:
                                    news_msgs.append(format_fib_levels(sym, price, below, above))
                            except Exception as e:
                                log.error(f"Fib error {sym}: {e}")

                    file_logger.log_alert(ts, a)

                send_telegram(header + "\n".join(alert_lines))
                for nm in news_msgs:
                    send_telegram(nm)

                status += f"  🔔 {len(alerts)}"
            else:
                status += "  ✓"
        elif current:
            status += "  (baseline)"

        # Check fib touches for all known stocks
        for sym, d in current.items():
            if d['price'] > 0:
                try:
                    touch_msg = check_fib_touch(sym, d['price'])
                    if touch_msg:
                        send_telegram(touch_msg)
                        status += f"  📐{sym}"
                except Exception as e:
                    log.error(f"Fib touch {sym}: {e}")

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

    def __init__(self):
        self.monitor = None
        self.windows = []       # [{wid, title, w, h, ...}, ...]
        self.check_vars = []    # [BooleanVar, ...]

        self.root = tk.Tk()
        self.root.title("Screen Monitor")
        self.root.geometry("520x500")
        self.root.attributes('-topmost', True)
        self.root.configure(bg=self.BG, highlightbackground=self.ACCENT,
                            highlightcolor=self.ACCENT, highlightthickness=3)
        self.root.resizable(False, False)

        # Header
        tk.Label(self.root, text="SCREEN MONITOR", font=("Helvetica", 18, "bold"),
                 bg=self.BG, fg=self.ACCENT).pack(pady=(10, 0))
        tk.Label(self.root, text="Window Tracker  |  Anomaly  |  Fib  |  Telegram",
                 font=("Helvetica", 9), bg=self.BG, fg="#888").pack()

        tk.Frame(self.root, bg=self.ACCENT, height=2).pack(fill='x', padx=12, pady=8)

        # Window list header
        fh = tk.Frame(self.root, bg=self.BG)
        fh.pack(fill='x', padx=12)
        tk.Label(fh, text="Windows:", font=("Helvetica", 11, "bold"),
                 bg=self.BG, fg=self.FG).pack(side='left')
        tk.Button(fh, text="Refresh", command=self._refresh_windows,
                  bg=self.ROW_BG, fg=self.ACCENT, font=("Helvetica", 9, "bold"),
                  relief='flat', padx=8, activebackground="#3d3d55").pack(side='right')

        # Scrollable window list
        list_frame = tk.Frame(self.root, bg=self.BG)
        list_frame.pack(fill='both', expand=True, padx=12, pady=4)

        self.canvas = tk.Canvas(list_frame, bg=self.BG, highlightthickness=0, height=220)
        scrollbar = tk.Scrollbar(list_frame, orient='vertical', command=self.canvas.yview)
        self.win_frame = tk.Frame(self.canvas, bg=self.BG)

        self.win_frame.bind('<Configure>',
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox('all')))
        self.canvas.create_window((0, 0), window=self.win_frame, anchor='nw')
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        tk.Frame(self.root, bg=self.ACCENT, height=2).pack(fill='x', padx=12, pady=6)

        # Settings row
        fs = tk.Frame(self.root, bg=self.BG)
        fs.pack(fill='x', padx=12, pady=2)

        tk.Label(fs, text="Freq (s):", font=("Helvetica", 10),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.freq = tk.IntVar(value=60)
        tk.Spinbox(fs, from_=10, to=600, increment=10, textvariable=self.freq,
                   width=4, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=(2, 15))

        tk.Label(fs, text="Alert %:", font=("Helvetica", 10),
                 bg=self.BG, fg=self.FG).pack(side='left')
        self.thresh = tk.DoubleVar(value=5.0)
        tk.Spinbox(fs, from_=1, to=50, increment=1, textvariable=self.thresh,
                   width=4, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=2)

        # Start/Stop
        self.btn = tk.Button(self.root, text="START", font=("Helvetica", 14, "bold"),
                             bg=self.GREEN, fg="white", command=self._toggle,
                             relief='flat', activebackground="#00a844")
        self.btn.pack(fill='x', padx=12, ipady=5, pady=(6, 0))

        # Status
        self.status = tk.StringVar(value="Refreshing windows...")
        tk.Label(self.root, textvariable=self.status, font=("Courier", 9),
                 bg=self.BG, fg="#888", wraplength=490, justify='left'
                 ).pack(padx=12, pady=6, anchor='w')

        self._load()
        self.root.after(300, self._refresh_windows)

    def _refresh_windows(self):
        """Scan for open windows and display as checkboxes."""
        saved_titles = self._load_selected_titles()

        for w in self.win_frame.winfo_children():
            w.destroy()
        self.windows = list_windows()
        self.check_vars = []

        if not self.windows:
            tk.Label(self.win_frame, text="No windows found",
                     bg=self.BG, fg="#666", font=("Helvetica", 10)).pack(pady=10)
            return

        for i, win in enumerate(self.windows):
            var = tk.BooleanVar(value=win['title'] in saved_titles)
            self.check_vars.append(var)

            row = tk.Frame(self.win_frame, bg=self.ROW_BG if i % 2 == 0 else self.BG)
            row.pack(fill='x', pady=1)

            cb = tk.Checkbutton(row, variable=var, bg=row['bg'],
                                activebackground=row['bg'], selectcolor=self.BG,
                                fg=self.ACCENT)
            cb.pack(side='left', padx=4)

            short_title = win['title'][:40]
            size_str = f"{win['w']}x{win['h']}"
            tk.Label(row, text=f"{short_title}", font=("Helvetica", 10),
                     bg=row['bg'], fg=self.FG, anchor='w').pack(side='left', fill='x', expand=True)
            tk.Label(row, text=size_str, font=("Courier", 9),
                     bg=row['bg'], fg="#888").pack(side='right', padx=6)

        n = len(self.windows)
        sel = sum(1 for v in self.check_vars if v.get())
        self.status.set(f"{n} windows found, {sel} selected")

    def _get_selected(self) -> list[dict]:
        """Return list of selected windows."""
        return [self.windows[i] for i, v in enumerate(self.check_vars) if v.get()]

    def _load_selected_titles(self) -> set[str]:
        """Load previously selected window titles from state."""
        if not STATE_PATH.exists():
            return set()
        try:
            s = json.load(open(STATE_PATH))
            return set(s.get('titles', []))
        except Exception:
            return set()

    def _toggle(self):
        if self.monitor and self.monitor.running:
            self.monitor.stop()
            self.monitor = None
            self.btn.config(text="START", bg=self.GREEN)
            self.status.set("Stopped.")
        else:
            selected = self._get_selected()
            if not selected:
                self.status.set("Select at least one window!")
                return
            global PCT_JUMP_THRESHOLD, PRICE_JUMP_THRESHOLD
            PCT_JUMP_THRESHOLD = self.thresh.get()
            PRICE_JUMP_THRESHOLD = self.thresh.get()
            self._save(selected)
            self.monitor = MonitorThread(selected, self.freq.get(), self._st)
            self.monitor.start()
            self.btn.config(text="STOP", bg=self.RED)
            self.status.set(f"Tracking {len(selected)} windows...")

    def _st(self, msg):
        self.root.after(0, lambda: self.status.set(msg))

    def _save(self, selected: list[dict]):
        titles = [w['title'] for w in selected]
        with open(STATE_PATH, 'w') as f:
            json.dump({
                'titles': titles,
                'freq': self.freq.get(),
                'thresh': self.thresh.get(),
            }, f)

    def _load(self):
        if not STATE_PATH.exists():
            return
        try:
            s = json.load(open(STATE_PATH))
            self.freq.set(s.get('freq', 60))
            self.thresh.set(s.get('thresh', 5.0))
        except Exception:
            pass

    def run(self):
        self.root.mainloop()
        if self.monitor:
            self.monitor.stop()


if __name__ == "__main__":
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram not configured — file logging only")
    App().run()
