"""
Screen Monitor — Silent screen capture, OCR, anomaly detection.
Only alerts via Telegram when something unusual happens.

Usage:
    python screen_monitor.py
"""

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

from PIL import Image, ImageGrab, ImageTk
from dotenv import load_dotenv
import pytesseract
import requests
import yfinance as yf
from deep_translator import GoogleTranslator

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
#  Silent Screenshot (Pillow — no flash)
# ══════════════════════════════════════════════════════════

def take_screenshot() -> Image.Image | None:
    """Completely silent screenshot via Pillow ImageGrab."""
    try:
        return ImageGrab.grab()
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


def fetch_news(symbol: str, max_items: int = 3) -> list[dict]:
    """Fetch recent news for a stock and translate to Hebrew."""
    try:
        ticker = yf.Ticker(symbol)
        raw_news = ticker.news or []
    except Exception as e:
        log.error(f"News fetch failed for {symbol}: {e}")
        return []

    results = []
    for item in raw_news[:max_items]:
        content = item.get('content', {})
        title_en = content.get('title', '')
        if not title_en:
            continue
        try:
            title_he = _translator.translate(title_en)
        except Exception:
            title_he = title_en

        pub = content.get('pubDate', '')[:10]  # YYYY-MM-DD
        source = content.get('provider', {}).get('displayName', '')

        results.append({
            'title_en': title_en,
            'title_he': title_he,
            'date': pub,
            'source': source,
        })

    return results


def format_news(symbol: str, news: list[dict]) -> str:
    if not news:
        return f"📰 {symbol} — אין חדשות"
    lines = [f"📰 <b>{symbol} — חדשות</b>"]
    for n in news:
        lines.append(f"• {n['title_he']}")
        lines.append(f"  <i>{n['source']} | {n['date']}</i>")
    return "\n".join(lines)


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
#  Region Selector
# ══════════════════════════════════════════════════════════

class RegionSelector:
    def __init__(self, screenshot, parent):
        self.result = None
        self.sx = self.sy = 0
        self.rect = None

        self.win = tk.Toplevel(parent)
        self.win.title("Select Region")
        self.win.attributes('-fullscreen', True)
        self.win.configure(cursor="cross")

        sw = self.win.winfo_screenwidth()
        sh = self.win.winfo_screenheight()
        self.kx = screenshot.width / sw
        self.ky = screenshot.height / sh

        disp = screenshot.resize((sw, sh), Image.LANCZOS)
        self.img = ImageTk.PhotoImage(disp)
        self.c = tk.Canvas(self.win, width=sw, height=sh, highlightthickness=0)
        self.c.pack()
        self.c.create_image(0, 0, anchor=tk.NW, image=self.img)
        self.c.create_rectangle(0, 0, sw, 50, fill="black", stipple="gray50")
        self.c.create_text(sw//2, 25, text="DRAG to select scanner area  |  ESC to cancel",
                           fill="#00ff00", font=("monospace", 18, "bold"))

        self.c.bind("<ButtonPress-1>", self._p)
        self.c.bind("<B1-Motion>", self._d)
        self.c.bind("<ButtonRelease-1>", self._r)
        self.win.bind("<Escape>", lambda e: self.win.destroy())

    def _p(self, e):
        self.sx, self.sy = e.x, e.y
        if self.rect: self.c.delete(self.rect)
        self.rect = self.c.create_rectangle(e.x, e.y, e.x, e.y, outline="#00ff00", width=3, dash=(5,3))

    def _d(self, e):
        if self.rect: self.c.coords(self.rect, self.sx, self.sy, e.x, e.y)

    def _r(self, e):
        x1, y1 = min(self.sx, e.x), min(self.sy, e.y)
        x2, y2 = max(self.sx, e.x), max(self.sy, e.y)
        if (x2-x1) < 50 or (y2-y1) < 50: return
        self.result = (int(x1*self.kx), int(y1*self.ky), int(x2*self.kx), int(y2*self.ky))
        self.win.destroy()


# ══════════════════════════════════════════════════════════
#  Monitor Thread
# ══════════════════════════════════════════════════════════

class MonitorThread(threading.Thread):
    def __init__(self, region, freq, on_status=None):
        super().__init__(daemon=True)
        self.region = region
        self.freq = freq
        self.on_status = on_status
        self.running = False
        self.previous = {}
        self.count = 0

    def stop(self):
        self.running = False

    def run(self):
        self.running = True
        log.info(f"Monitoring region={self.region} every {self.freq}s")
        while self.running:
            try:
                self._cycle()
            except Exception as e:
                log.error(f"Error: {e}")
            for _ in range(self.freq):
                if not self.running: break
                time.sleep(1)

    def _cycle(self):
        img = take_screenshot()
        if not img: return

        crop = img.crop(self.region)
        crop.save(str(CROP_PATH))
        text = ocr_image(crop)
        current = parse_scanner_data(text)
        self.count += 1
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Record history for momentum tracking
        for sym, d in current.items():
            stock_history.record(sym, d['price'], d['pct'])

        file_logger.log_scan(ts, current, text)

        status = f"#{self.count}  {len(current)} symbols"

        if self.previous and current:
            alerts = detect_anomalies(current, self.previous)
            if alerts:
                # Build main alert with momentum
                header = f"🔔 <b>Alert</b> — {datetime.now().strftime('%H:%M:%S')}\n"
                alert_lines = []
                news_msgs = []

                for a in alerts:
                    sym = a.get('symbol', '')
                    line = a['msg']

                    # Add momentum info
                    mom = stock_history.format_momentum(sym)
                    if mom:
                        line += f"\n   📊 {mom}"

                    alert_lines.append(line)

                    # Fetch news for new stocks
                    if a.get('fetch_news'):
                        try:
                            news = fetch_news(sym)
                            if news:
                                news_msgs.append(format_news(sym, news))
                        except Exception as e:
                            log.error(f"News error {sym}: {e}")

                    file_logger.log_alert(ts, a)

                send_telegram(header + "\n".join(alert_lines))

                # Send news as separate messages
                for nm in news_msgs:
                    send_telegram(nm)

                status += f"  🔔 {len(alerts)} alerts!"
            else:
                status += "  ✓"
        elif current:
            status += "  (baseline)"

        self.previous = current
        if self.on_status:
            self.on_status(status)
        log.info(status)


# ══════════════════════════════════════════════════════════
#  GUI
# ══════════════════════════════════════════════════════════

class App:
    def __init__(self):
        self.monitor = None
        self.region = None

        self.root = tk.Tk()
        self.root.title("Screen Monitor")
        self.root.geometry("380x320")
        self.root.attributes('-topmost', True)

        # Header
        tk.Label(self.root, text="Screen Monitor", font=("Helvetica", 16, "bold")).pack(pady=(10,2))
        tk.Label(self.root, text="Silent capture → Detect anomalies → Telegram alert",
                 font=("Helvetica", 9), fg="gray").pack()

        ttk.Separator(self.root).pack(fill='x', pady=8, padx=10)

        # Region
        f1 = tk.Frame(self.root); f1.pack(fill='x', padx=10, pady=3)
        self.rlbl = tk.Label(f1, text="Region: none", font=("Helvetica", 11), anchor='w')
        self.rlbl.pack(side='left', fill='x', expand=True)
        tk.Button(f1, text="Select Area", command=self._select).pack(side='right')

        # Frequency
        f2 = tk.Frame(self.root); f2.pack(fill='x', padx=10, pady=3)
        tk.Label(f2, text="Frequency (sec):", font=("Helvetica", 11)).pack(side='left')
        self.freq = tk.IntVar(value=60)
        tk.Spinbox(f2, from_=10, to=600, increment=10, textvariable=self.freq, width=5, font=("Helvetica", 11)).pack(side='right')

        # Thresholds
        f3 = tk.Frame(self.root); f3.pack(fill='x', padx=10, pady=3)
        tk.Label(f3, text="Alert threshold (%):", font=("Helvetica", 11)).pack(side='left')
        self.thresh = tk.DoubleVar(value=5.0)
        tk.Spinbox(f3, from_=1, to=50, increment=1, textvariable=self.thresh, width=5, font=("Helvetica", 11)).pack(side='right')

        ttk.Separator(self.root).pack(fill='x', pady=8, padx=10)

        # Start/Stop
        self.btn = tk.Button(self.root, text="▶  Start", font=("Helvetica", 13, "bold"),
                             bg="#4CAF50", fg="white", command=self._toggle)
        self.btn.pack(fill='x', padx=10, ipady=5)

        # Status
        self.status = tk.StringVar(value="Waiting...")
        tk.Label(self.root, textvariable=self.status, font=("Courier", 9), fg="gray40",
                 wraplength=350, justify='left').pack(padx=10, pady=6, anchor='w')

        self._load()

    def _select(self):
        self.root.withdraw()
        time.sleep(0.3)
        img = take_screenshot()
        self.root.deiconify()
        if not img:
            self.status.set("Screenshot failed"); return
        sel = RegionSelector(img, self.root)
        self.root.wait_window(sel.win)
        if sel.result:
            self.region = sel.result
            w, h = sel.result[2]-sel.result[0], sel.result[3]-sel.result[1]
            self.rlbl.config(text=f"Region: {w}x{h}")
            self.status.set("Ready. Press Start.")
            self._save()

    def _toggle(self):
        if self.monitor and self.monitor.running:
            self.monitor.stop(); self.monitor = None
            self.btn.config(text="▶  Start", bg="#4CAF50")
            self.status.set("Stopped.")
        else:
            if not self.region:
                self.status.set("Select region first!"); return
            global PCT_JUMP_THRESHOLD, PRICE_JUMP_THRESHOLD
            PCT_JUMP_THRESHOLD = self.thresh.get()
            PRICE_JUMP_THRESHOLD = self.thresh.get()
            f = self.freq.get()
            self.monitor = MonitorThread(self.region, f, self._st)
            self.monitor.start()
            self.btn.config(text="⏹  Stop", bg="#f44336")
            self.status.set(f"Monitoring... alert if >{self.thresh.get()}% move")

    def _st(self, msg):
        self.root.after(0, lambda: self.status.set(msg))

    def _save(self):
        with open(STATE_PATH, 'w') as f:
            json.dump({'region': self.region, 'freq': self.freq.get(), 'thresh': self.thresh.get()}, f)

    def _load(self):
        if not STATE_PATH.exists(): return
        try:
            s = json.load(open(STATE_PATH))
            self.region = tuple(s['region'])
            self.freq.set(s.get('freq', 60))
            self.thresh.set(s.get('thresh', 5.0))
            w, h = self.region[2]-self.region[0], self.region[3]-self.region[1]
            self.rlbl.config(text=f"Region: {w}x{h}")
            self.status.set("Previous region loaded.")
        except Exception: pass

    def run(self):
        self.root.mainloop()
        if self.monitor: self.monitor.stop()


if __name__ == "__main__":
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram not configured — file logging only")
    App().run()
