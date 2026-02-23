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
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time as time_mod
import tkinter as tk
from collections import defaultdict, deque
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from tkinter import messagebox

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import subprocess
import webbrowser
import io
import requests
from PIL import Image, ImageFilter, ImageOps, ImageTk
import pytesseract
from bidi.algorithm import get_display as bidi_display
from ib_insync import IB, Stock, LimitOrder, MarketOrder, StopOrder, ScannerSubscription, util as ib_util
from finvizfinance.quote import finvizfinance as Finviz
from deep_translator import GoogleTranslator

from strategies.fibonacci_engine import (
    find_anchor_candle, build_dual_series, advance_series,
)
from strategies.fib_dt_live_strategy import (
    FibDTLiveStrategySync, DTEntryRequest, DTTrailingExit, GapSignal,
)
from strategies.gap_go_live_strategy import (
    GapGoLiveStrategy, GGCandidate, GGEntrySignal, GGExitSignal,
)
from strategies.momentum_ride_live_strategy import (
    MomentumRideLiveStrategy, MRCandidate, MREntrySignal, MRExitSignal,
)
from monitor.order_thread import OrderThread
from config.settings import (
    FIB_LEVELS_24, FIB_LEVEL_COLORS, IBKR_HOST, IBKR_PORT,
    MONITOR_IBKR_CLIENT_ID, MONITOR_SCAN_CODE, MONITOR_SCAN_CODES, MONITOR_SCAN_MAX_RESULTS,
    MONITOR_PRICE_MIN, MONITOR_PRICE_MAX, MONITOR_DEFAULT_FREQ,
    MONITOR_DEFAULT_ALERT_PCT,
    FIB_DT_LIVE_STOP_PCT, FIB_DT_LIVE_TARGET_LEVELS,
    BRACKET_FIB_STOP_PCT, BRACKET_TRAILING_PROFIT_PCT, STOP_LIMIT_OFFSET_PCT,
    STARTING_CAPITAL,
    GG_LIVE_GAP_MIN_PCT, GG_LIVE_INITIAL_CASH, GG_LIVE_POSITION_SIZE_PCT,
    MR_GAP_MIN_PCT, MR_GAP_MAX_PCT, MR_LIVE_INITIAL_CASH, MR_LIVE_POSITION_SIZE_PCT,
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
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/tmp/monitor.log"),
    ],
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

# ── Contract cache (avoid repeated qualifyContracts round-trips) ──
_contract_cache: dict[str, Stock] = {}
# OCR symbol corrections discovered during IBKR validation (e.g. EVTIV→EVTV)
_sym_corrections: dict[str, str] = {}  # bad_ocr_sym → correct_sym


def _get_contract(sym: str) -> Stock | None:
    """Get a qualified Stock contract, using cache when available.

    If sym fails validation, tries OCR-correction variants:
    - Remove each 'I' (Tesseract inserts spurious I from l→I fix, e.g. EVTIV→EVTV)
    """
    if sym in _contract_cache:
        return _contract_cache[sym]
    ib = _get_ibkr()
    if not ib:
        return None
    contract = Stock(sym, 'SMART', 'USD')
    ib.qualifyContracts(contract)
    if contract.conId:
        _contract_cache[sym] = contract
        return contract

    # Try removing each 'I' — OCR may have inserted a spurious one (lV→IV)
    if 'I' in sym and len(sym) >= 3:
        for i, ch in enumerate(sym):
            if ch == 'I':
                variant = sym[:i] + sym[i+1:]
                if len(variant) < 2:
                    continue
                vc = Stock(variant, 'SMART', 'USD')
                ib.qualifyContracts(vc)
                if vc.conId:
                    log.info(f"OCR symbol fix: {sym} → {variant} (removed spurious I)")
                    _sym_corrections[sym] = variant
                    _contract_cache[sym] = vc       # cache under original key
                    _contract_cache[variant] = vc   # also under corrected key
                    return vc

    # Try common OCR letter swaps: K↔X, O↔0, B↔8, S↔5, G↔6
    _OCR_SWAPS = {'K': 'X', 'X': 'K'}
    for i, ch in enumerate(sym):
        replacement = _OCR_SWAPS.get(ch)
        if replacement:
            variant = sym[:i] + replacement + sym[i+1:]
            if variant in _contract_cache:
                _sym_corrections[sym] = variant
                _contract_cache[sym] = _contract_cache[variant]
                return _contract_cache[variant]
            vc = Stock(variant, 'SMART', 'USD')
            ib.qualifyContracts(vc)
            if vc.conId:
                log.info(f"OCR symbol fix: {sym} → {variant} (swapped {ch}→{replacement})")
                _sym_corrections[sym] = variant
                _contract_cache[sym] = vc
                _contract_cache[variant] = vc
                return vc

    return None  # qualification failed — don't return invalid contract (conId=0)


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
    Weekend (Sat/Sun) always returns 'closed'.
    """
    now_et = datetime.now(_ET)
    if now_et.weekday() >= 5:  # Saturday=5, Sunday=6
        return 'closed'
    t = now_et.time()
    if time(4, 0) <= t < time(9, 30):
        return 'pre_market'
    elif time(9, 30) <= t < time(16, 0):
        return 'market'
    elif time(16, 0) <= t < time(20, 0):
        return 'after_hours'
    return 'closed'


def _expected_volume_fraction() -> float:
    """Expected fraction of daily volume traded by current time of day.

    Used to normalize RVOL so that 1.0x means normal pace regardless of time.
    E.g., at 10:00 ET ~25% of daily volume is expected, so RVOL = today_vol / (avg * 0.25).
    """
    et = datetime.now(_ET)
    h = et.hour + et.minute / 60.0
    if h < 4:    return 0.01   # overnight
    if h < 9.5:  return 0.03   # pre-market
    if h < 10:   return 0.15   # first 30 min (heavy)
    if h < 10.5: return 0.25
    if h < 11:   return 0.35
    if h < 12:   return 0.45
    if h < 13:   return 0.55
    if h < 14:   return 0.65
    if h < 15:   return 0.80
    if h < 16:   return 0.95   # near close
    return 1.0                  # after-hours / closed


def _calc_vwap_from_bars(trade_bars: list) -> float | None:
    """Calculate true VWAP = Σ(TP × Volume) / Σ(Volume) from TRADES bars."""
    total_tp_vol = 0.0
    total_vol = 0.0
    for b in trade_bars:
        vol = b.volume if b.volume else 0
        if vol <= 0:
            continue
        tp = (b.high + b.low + b.close) / 3
        total_tp_vol += tp * vol
        total_vol += vol
    if total_vol > 0:
        return total_tp_vol / total_vol
    return None


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
            trade_bars = ib.reqHistoricalData(
                contract, endDateTime='',
                durationStr='21600 S', barSizeSetting='1 min',
                whatToShow='TRADES', useRTH=False,
                timeout=15,
            )
            if trade_bars:
                price = trade_bars[-1].close
                ext_high = max(b.high for b in trade_bars)
                ext_low = min(b.low for b in trade_bars)
                ext_vwap = _calc_vwap_from_bars(trade_bars)
        except Exception:
            pass
    elif session == 'after_hours':
        # After-hours: last RTH bar is today → prev_close is yesterday
        prev_close = bars[-2].close if len(bars) >= 2 else 0.0
        try:
            trade_bars = ib.reqHistoricalData(
                contract, endDateTime='',
                durationStr='14400 S', barSizeSetting='1 min',
                whatToShow='TRADES', useRTH=False,
                timeout=15,
            )
            if trade_bars:
                price = trade_bars[-1].close
                ext_high = max(b.high for b in trade_bars)
                ext_low = min(b.low for b in trade_bars)
                ext_vwap = _calc_vwap_from_bars(trade_bars)
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
    """Run multiple IBKR scanners and merge results.

    Runs TOP_PERC_GAIN + HOT_BY_VOLUME + MOST_ACTIVE for broad coverage
    across pre-market, market, and after-hours sessions.

    Returns dict: {symbol: {'price': float, 'pct': float, 'volume': str, ...}}
    """
    ib = _get_ibkr()
    if not ib:
        return {}

    # ── Phase 1: Run all scan types and collect unique contracts ──
    valid_items: list[tuple[str, object]] = []  # (sym, contract)
    seen_syms: set[str] = set()

    for scan_code in MONITOR_SCAN_CODES:
        sub = ScannerSubscription(
            instrument="STK",
            locationCode="STK.US.MAJOR",
            scanCode=scan_code,
            numberOfRows=MONITOR_SCAN_MAX_RESULTS,
            abovePrice=price_min,
            belowPrice=price_max,
        )
        try:
            results = ib.reqScannerData(sub)
        except Exception as e:
            log.error(f"reqScannerData ({scan_code}) failed: {e}")
            continue

        added = 0
        for item in results:
            contract = item.contractDetails.contract
            sym = contract.symbol
            if not sym or not sym.isalpha() or len(sym) > 5:
                continue
            if sym not in seen_syms:
                seen_syms.add(sym)
                valid_items.append((sym, contract))
                # Cache scanner contract (already qualified by IBKR)
                if contract.conId and sym not in _contract_cache:
                    _contract_cache[sym] = contract
                added += 1
        log.debug(f"Scan [{scan_code}]: {len(results)} raw → {added} new unique")

    stocks: dict[str, dict] = {}

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
                timeout=15,
            )
            if not bars:
                continue

            last_bar = bars[-1]
            volume = last_bar.volume
            session = _get_market_session()

            price, prev_close, ext_high, ext_low, ext_vwap = \
                _fetch_extended_hours_price(ib, stock_contract, session, bars)

            pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0

            # RVOL: normalized by expected volume at current time of day
            if has_cached_avg:
                avg_vol = _avg_vol_cache[sym]
            elif len(bars) >= 2:
                prev_volumes = [b.volume for b in bars[:-1]]
                avg_vol = sum(prev_volumes) / len(prev_volumes) if prev_volumes else 0
                if avg_vol > 0:
                    _avg_vol_cache[sym] = avg_vol
            else:
                avg_vol = 0

            if avg_vol > 0:
                expected = avg_vol * _expected_volume_fraction()
                rvol = round(volume / expected, 1) if expected > 0 else 0.0
            else:
                rvol = 0.0
            vol_str = _format_volume(volume)
            vwap = round(last_bar.average, 4) if last_bar.average else 0.0

            # Override with extended-hours values when available
            rth_high = round(last_bar.high, 4)
            rth_low = round(last_bar.low, 4)
            if session == 'pre_market' and ext_high is not None:
                # Pre-market: use only extended-hours data (RTH bar is yesterday)
                day_high = round(ext_high, 4)
                day_low = round(ext_low, 4)
                if ext_vwap is not None:
                    vwap = round(ext_vwap, 4)
            elif session == 'after_hours' and ext_high is not None:
                # After-hours: combine RTH + AH data
                day_high = round(max(rth_high, ext_high), 4)
                day_low = round(min(rth_low, ext_low), 4)
                if ext_vwap is not None:
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
    log.info(f"Scanner [{session}]: {len(valid_items)} unique from {len(MONITOR_SCAN_CODES)} scans → {len(stocks)} enriched symbols")
    return stocks


# ══════════════════════════════════════════════════════════
#  Webull Desktop OCR Scanner
# ══════════════════════════════════════════════════════════

# Multi-scanner sources: up to 3 user-picked windows for OCR scanning
_scanner_sources: list[dict] = []  # [{'wid': int, 'name': str}, ...] max 3
_scanner_sources_lock = threading.Lock()
_MAX_SCANNER_SOURCES = 3


def _verify_wid(wid: int) -> bool:
    """Check that a window ID still exists."""
    try:
        result = subprocess.run(
            ['xdotool', 'getwindowname', str(wid)],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0 and result.stdout.strip() != ''
    except Exception:
        return False


def _get_window_name(wid: int) -> str:
    """Get short display name for a window ID."""
    try:
        result = subprocess.run(
            ['xdotool', 'getwindowname', str(wid)],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()[:20]
    except Exception:
        pass
    return ""


def add_scanner_source(wid: int, name: str = ""):
    """Add a scanner source window. Max 3."""
    global _scanner_sources
    if not name:
        name = _get_window_name(wid)
    with _scanner_sources_lock:
        if len(_scanner_sources) >= _MAX_SCANNER_SOURCES:
            log.warning(f"Max {_MAX_SCANNER_SOURCES} scanner sources reached")
            return False
        # Don't add duplicates
        if any(s['wid'] == wid for s in _scanner_sources):
            log.info(f"Scanner source WID {wid} already added")
            return False
        _scanner_sources.append({'wid': wid, 'name': name})
        log.info(f"Scanner source added: WID={wid} name={name} (total: {len(_scanner_sources)})")
        return True


def remove_scanner_source(idx: int):
    """Remove scanner source by slot index (0-based)."""
    global _scanner_sources
    with _scanner_sources_lock:
        if 0 <= idx < len(_scanner_sources):
            removed = _scanner_sources.pop(idx)
            log.info(f"Scanner source removed: slot {idx} WID={removed['wid']}")
        else:
            log.warning(f"Invalid scanner source index: {idx}")


def get_scanner_sources() -> list[dict]:
    """Get current scanner sources list."""
    with _scanner_sources_lock:
        return list(_scanner_sources)


def _capture_and_ocr(wid: int) -> str:
    """Capture a window via import(1) and OCR with pytesseract.

    Pipeline: screenshot → 3x resize → grayscale → invert → sharpen → OCR.
    """
    try:
        result = subprocess.run(
            ['import', '-silent', '-window', str(wid), 'png:-'],
            capture_output=True, timeout=10,
        )
        if result.returncode != 0 or not result.stdout:
            log.warning(f"import -window failed (rc={result.returncode})")
            return ""
    except Exception as e:
        log.warning(f"Window capture failed: {e}")
        return ""

    img = Image.open(io.BytesIO(result.stdout))
    # 3x resize for better OCR accuracy
    img = img.resize((img.width * 3, img.height * 3), Image.LANCZOS)
    img = img.convert('L')          # grayscale
    img = ImageOps.invert(img)      # invert (dark bg → light bg)
    img = img.filter(ImageFilter.SHARPEN)

    text = pytesseract.image_to_string(img, config='--psm 6')
    return text


def _parse_ocr_volume(vol_str: str) -> tuple[int, str]:
    """Parse OCR volume/float string to (raw_int, display_str).

    Examples: "351.00K" → (351000, "351.0K"), "1.33M" → (1330000, "1.3M")
    """
    vol_str = vol_str.strip().upper()
    multiplier = 1
    if vol_str.endswith('K'):
        multiplier = 1_000
        vol_str = vol_str[:-1]
    elif vol_str.endswith('M'):
        multiplier = 1_000_000
        vol_str = vol_str[:-1]
    elif vol_str.endswith('B'):
        multiplier = 1_000_000_000
        vol_str = vol_str[:-1]

    try:
        val = float(vol_str.replace(',', ''))
    except ValueError:
        return (0, "0")

    raw = int(val * multiplier)
    return (raw, _format_volume(raw))


# Generic OCR patterns — Webull format: SYM +PCT% PRICE VOL FLOAT
# PCT always starts with +/-, PRICE is plain digits — used to distinguish columns.
# Symbols accept lowercase (Tesseract reads I→l), uppercased in parser.
_OCR_RE_5COL = re.compile(
    r'^([A-Za-z]{1,5})\s+'        # symbol (allow lowercase for OCR misreads)
    r'[^+\d-]*'                   # skip OCR junk (=, —, spaces) before +/- sign
    r'([+-][\d.,]+)%?\S*\s+'      # pct change (+/- prefix, optional %, ignore junk)
    r'([\d.]+)\s+'                # price
    r'([\d.,]+[KMBkmb]?)\s+'     # volume
    r'[^\d]*([\d.,]+[KMBkmb]?)\S*$',  # float (skip OCR junk like dashes before digits)
    re.MULTILINE,
)
_OCR_RE_4COL = re.compile(
    r'^([A-Za-z]{1,5})\s+'
    r'[^+\d-]*'                   # skip OCR junk before +/- sign
    r'([+-][\d.,]+)%?\S*\s+'
    r'([\d.]+)\s+'
    r'([\d.,]+[KMBkmb]?)\s*$',
    re.MULTILINE,
)
_OCR_RE_3COL = re.compile(
    r'^([A-Za-z]{1,5})\s+'
    r'[^+\d-]*'                   # skip OCR junk before +/- sign
    r'([+-][\d.,]+)%?\S*\s+'
    r'([\d.]+)\s*$',
    re.MULTILINE,
)


def _fix_ocr_text(text: str) -> str:
    """Pre-process OCR text to fix common Tesseract misreads.

    - Spaces inside percentage numbers: "+37 44%" → "+37.44%"
    - Spaces inside decimal prices: "15 38" → "15.38"
    """
    # Fix: Tesseract reads '.' as ' ' in percentages (e.g. "+37 44%" → "+37.44%")
    text = re.sub(r'([+-]\d+) (\d+%)', r'\1.\2', text)
    # Fix: Tesseract reads '.' as ' ' in prices after percentage column
    # Pattern: ...%  DIGITS SPACE DIGITS  → ...%  DIGITS.DIGITS
    text = re.sub(r'(%\S*\s+\d+) (\d{1,4}\b)', r'\1.\2', text)
    return text


def _parse_ocr_generic(text: str) -> list[dict]:
    """Parse OCR text into structured stock data.

    Tries 3 regex patterns in priority order (Webull column order):
    1. 5 columns: SYMBOL PCT% PRICE VOLUME FLOAT
    2. 4 columns: SYMBOL PCT% PRICE VOLUME
    3. 3 columns: SYMBOL PCT% PRICE
    """
    text = _fix_ocr_text(text)
    results = []
    seen = set()

    def _fix_ocr_sym(raw: str) -> str:
        """Fix OCR symbol: l→I, then handle spurious 'I' in 5+ char symbols."""
        sym = raw.replace('l', 'I').upper()
        if len(sym) >= 5 and 'I' in sym:
            # OCR likely inserted spurious 'l' (now 'I'). Try removing each 'I'.
            for i, ch in enumerate(sym):
                if ch == 'I':
                    variant = sym[:i] + sym[i+1:]
                    if len(variant) < 2:
                        continue
                    # Check cached corrections and known symbols
                    if (_sym_corrections.get(sym) == variant
                            or variant in _contract_cache
                            or variant in _enrichment):
                        _sym_corrections[sym] = variant
                        return variant
            # No cache hit — prefer 4-char by removing first 'I' (most common OCR error)
            for i, ch in enumerate(sym):
                if ch == 'I':
                    variant = sym[:i] + sym[i+1:]
                    if len(variant) >= 2:
                        log.info(f"OCR auto-fix: {sym} → {variant} (5+ char, removed spurious I)")
                        _sym_corrections[sym] = variant
                        return variant
        return sym

    # Try 5-column first (most data)
    for m in _OCR_RE_5COL.finditer(text):
        sym = _fix_ocr_sym(m.group(1))
        if sym in seen:
            continue
        try:
            pct = float(m.group(2).replace(',', '').rstrip('.'))
            price = float(m.group(3))
        except ValueError:
            continue
        vol_raw, vol_display = _parse_ocr_volume(m.group(4))
        flt_raw, flt_display = _parse_ocr_volume(m.group(5))
        results.append({
            'sym': sym, 'price': price, 'pct': pct,
            'volume_raw': vol_raw, 'volume': vol_display,
            'float_raw': flt_raw, 'float_display': flt_display,
        })
        seen.add(sym)

    # Try 4-column for remaining
    for m in _OCR_RE_4COL.finditer(text):
        sym = _fix_ocr_sym(m.group(1))
        if sym in seen:
            continue
        try:
            pct = float(m.group(2).replace(',', '').rstrip('.'))
            price = float(m.group(3))
        except ValueError:
            continue
        vol_raw, vol_display = _parse_ocr_volume(m.group(4))
        results.append({
            'sym': sym, 'price': price, 'pct': pct,
            'volume_raw': vol_raw, 'volume': vol_display,
            'float_raw': 0, 'float_display': '',
        })
        seen.add(sym)

    # Try 3-column for remaining
    for m in _OCR_RE_3COL.finditer(text):
        sym = _fix_ocr_sym(m.group(1))
        if sym in seen:
            continue
        try:
            pct = float(m.group(2).replace(',', '').rstrip('.'))
            price = float(m.group(3))
        except ValueError:
            continue
        results.append({
            'sym': sym, 'price': price, 'pct': pct,
            'volume_raw': 0, 'volume': '',
            'float_raw': 0, 'float_display': '',
        })
        seen.add(sym)

    return results


def _run_ocr_scan(price_min: float = MONITOR_PRICE_MIN,
                   price_max: float = MONITOR_PRICE_MAX) -> dict:
    """Scan via OCR on all configured scanner sources.

    Loops over _scanner_sources, OCR each window, merges results
    (prefer source with more fields). Returns same dict format as _run_ibkr_scan.
    Falls back to empty dict if no sources configured or all fail.
    """
    global _scanner_sources

    # Snapshot under lock, prune outside (avoid holding lock during I/O)
    with _scanner_sources_lock:
        snapshot = list(_scanner_sources)
    valid = [s for s in snapshot if _verify_wid(s['wid'])]
    with _scanner_sources_lock:
        _scanner_sources = valid

    if not valid:
        log.info("No scanner sources configured, will fallback to IBKR scan")
        return {}

    all_parsed: list[dict] = []
    for src in valid:
        text = _capture_and_ocr(src['wid'])
        if not text.strip():
            log.warning(f"OCR empty for source WID={src['wid']} ({src['name']})")
            continue
        parsed = _parse_ocr_generic(text)
        if parsed:
            log.info(f"OCR source WID={src['wid']} ({src['name']}): {len(parsed)} lines")
        all_parsed.extend(parsed)

    if not all_parsed:
        log.warning("OCR scan: no valid lines from any source")
        return {}

    # Merge by symbol — prefer entry with more fields (float > volume > basic)
    best: dict[str, dict] = {}
    for p in all_parsed:
        sym = p['sym']
        if sym not in best:
            best[sym] = p
        else:
            # Prefer entry with float data, then volume data
            old = best[sym]
            old_score = (1 if old['float_raw'] else 0) + (1 if old['volume_raw'] else 0)
            new_score = (1 if p['float_raw'] else 0) + (1 if p['volume_raw'] else 0)
            if new_score > old_score:
                best[sym] = p

    # Apply known OCR symbol corrections (e.g. EVTIV→EVTV from IBKR validation)
    if _sym_corrections:
        corrected_best: dict[str, dict] = {}
        for sym, p in best.items():
            real = _sym_corrections.get(sym, sym)
            p['sym'] = real
            corrected_best[real] = p
        best = corrected_best

    stocks: dict[str, dict] = {}
    for p in best.values():
        price = p['price']
        if price < price_min or price > price_max:
            continue

        pct = p['pct']
        prev_close = round(price / (1 + pct / 100), 4) if pct != -100 else 0.0

        stocks[p['sym']] = {
            'price': round(price, 2),
            'pct': round(pct, 1),
            'volume': p['volume'],
            'volume_raw': p['volume_raw'],
            'rvol': 0.0,
            'float': p['float_display'],
            'float_raw': p['float_raw'],
            'vwap': 0.0,
            'prev_close': prev_close,
            'day_high': round(price, 4),
            'day_low': round(price, 4),
            'contract': None,
        }

    session = _get_market_session()
    src_count = len(_scanner_sources)
    log.info(f"OCR scan [{session}]: {src_count} sources → {len(best)} parsed → {len(stocks)} stocks (price ${price_min}-${price_max})")
    return stocks


def _enrich_with_ibkr(current: dict, syms: list[str]):
    """Enrich Webull scan results with IBKR data for selected symbols.

    Fetches contract, RVOL, VWAP, prev_close, day_high, day_low.
    Modifies current dict in-place.
    """
    ib = _get_ibkr()
    if not ib:
        log.warning("IBKR not available for enrichment")
        return

    session = _get_market_session()
    enriched = 0

    for sym in syms:
        if sym not in current:
            continue
        contract = _get_contract(sym)
        if not contract:
            log.debug(f"IBKR enrich: no contract for {sym}")
            continue

        has_cached_avg = sym in _avg_vol_cache
        duration = "2 D" if has_cached_avg else "12 D"

        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
                timeout=15,
            )
            if not bars:
                continue

            last_bar = bars[-1]
            volume = last_bar.volume

            price, prev_close, ext_high, ext_low, ext_vwap = \
                _fetch_extended_hours_price(ib, contract, session, bars)

            # RVOL
            if has_cached_avg:
                avg_vol = _avg_vol_cache[sym]
            elif len(bars) >= 2:
                prev_volumes = [b.volume for b in bars[:-1]]
                avg_vol = sum(prev_volumes) / len(prev_volumes) if prev_volumes else 0
                if avg_vol > 0:
                    _avg_vol_cache[sym] = avg_vol
            else:
                avg_vol = 0

            if avg_vol > 0:
                expected = avg_vol * _expected_volume_fraction()
                rvol = round(volume / expected, 1) if expected > 0 else 0.0
            else:
                rvol = 0.0

            vwap = round(last_bar.average, 4) if last_bar.average else 0.0

            rth_high = round(last_bar.high, 4)
            rth_low = round(last_bar.low, 4)
            if session == 'pre_market' and ext_high is not None:
                day_high = round(ext_high, 4)
                day_low = round(ext_low, 4)
                if ext_vwap is not None:
                    vwap = round(ext_vwap, 4)
            elif session == 'after_hours' and ext_high is not None:
                day_high = round(max(rth_high, ext_high), 4)
                day_low = round(min(rth_low, ext_low), 4)
                if ext_vwap is not None:
                    vwap = round(ext_vwap, 4)
            else:
                day_high = rth_high
                day_low = rth_low

            # Update in-place — keep OCR volume (more accurate in pre/after)
            d = current[sym]
            d['rvol'] = rvol
            d['vwap'] = vwap
            d['prev_close'] = round(prev_close, 4)
            d['day_high'] = day_high
            d['day_low'] = day_low
            d['contract'] = contract
            if not d.get('volume_raw'):
                # Only set volume if OCR didn't provide one
                d['volume'] = _format_volume(volume)
                d['volume_raw'] = int(volume)
            enriched += 1

        except Exception as e:
            log.debug(f"IBKR enrich {sym} failed: {e}")
            continue

    log.info(f"IBKR enrichment: {enriched}/{len(syms)} symbols enriched")


# ══════════════════════════════════════════════════════════
#  Stock History — momentum over time
# ══════════════════════════════════════════════════════════

class StockHistory:
    """Track price/pct history for each stock across scans."""

    _MAX_ENTRIES = 3600  # ~2 hours at 2s intervals

    def __init__(self):
        # {symbol: deque of (timestamp, price, pct)}
        self.data: dict[str, deque] = defaultdict(lambda: deque(maxlen=StockHistory._MAX_ENTRIES))

    def record(self, symbol: str, price: float, pct: float):
        self.data[symbol].append((datetime.now(), price, pct))

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


def _batch_translate(titles_en: list[str]) -> list[str]:
    """Translate a list of English strings to Hebrew in a single API call."""
    if not titles_en:
        return []
    try:
        combined = "\n||||\n".join(titles_en)
        translated = _translator.translate(combined)
        return translated.split("\n||||\n")
    except Exception:
        return list(titles_en)


def _find_unfilled_gaps(df: pd.DataFrame, min_gap: float = 0.05) -> list[dict]:
    """Find price gaps that haven't been filled by subsequent price action.

    Returns list of dicts with keys: idx, bottom, top, pct, up.
    """
    n = len(df)
    if n < 2:
        return []
    gaps = []
    for i in range(1, n):
        pc = df.iloc[i - 1]['close']
        co = df.iloc[i]['open']
        g = co - pc
        if abs(g) < min_gap or pc <= 0:
            continue
        filled = False
        for j in range(i, n):
            if g > 0 and df.iloc[j]['low'] <= pc:
                filled = True
                break
            elif g < 0 and df.iloc[j]['high'] >= pc:
                filled = True
                break
        if not filled:
            gaps.append({
                'idx': i,
                'bottom': min(pc, co),
                'top': max(pc, co),
                'pct': abs(g) / pc * 100,
                'up': g > 0,
            })
    return gaps


# Regex to strip IBKR headline metadata like {A:800015:L:en:K:-0.97:C:0.97}
_IBKR_HEADLINE_RE = re.compile(r'\{[^}]*\}\*?\s*')


def _fetch_ibkr_news(symbol: str, max_news: int = 5) -> list[dict]:
    """Fetch recent news headlines from IBKR (Dow Jones, The Fly, Briefing).

    Returns list of {'title_en': str, 'date': str, 'source': str}.
    """
    ib = _get_ibkr()
    if not ib:
        return []
    try:
        contract = _get_contract(symbol)
        if not contract or not contract.conId:
            return []
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
                et_dt = utc_dt.astimezone(ZoneInfo('US/Eastern'))
                date_str = et_dt.strftime('%Y-%m-%d %H:%M ET')
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


def _check_news_updates(current_stocks: dict, suppress_send: bool = False):
    """Check for new IBKR news headlines on stocks ≥ +20%.

    Only runs during pre_market, market, and after_hours.
    Sends Telegram alert for each new headline found.
    If suppress_send=True, populate state without sending messages (warmup).
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
        titles_he = _batch_translate(titles_en)

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

        # Append fib + VWAP
        if price > 0:
            fib_txt = _format_fib_text(sym, price, vwap=d.get('vwap', 0))
            if fib_txt:
                lines.append(fib_txt.strip())

        if not suppress_send:
            btn = _make_lookup_button(sym)
            send_telegram_alert("\n".join(lines), reply_markup=btn)
            # Push to GUI ALERTS panel
            if _gui_alert_cb:
                _gui_alert_cb(f"📰 NEWS — {sym} ({len(new_headlines)} headlines)")
        log.info(f"News alert: {sym} — {len(new_headlines)} new headlines{' (warmup)' if suppress_send else ''}")

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
                    # Finviz default time is 08:00 — show date only
                    if et_dt.hour == 8 and et_dt.minute == 0:
                        dates.append(et_dt.strftime('%Y-%m-%d'))
                    else:
                        dates.append(et_dt.strftime('%Y-%m-%d %H:%M ET'))
                except (ValueError, IndexError):
                    dates.append(raw_date[:16])

        # Batch translate all headlines in one call
        if titles_en:
            titles_he = _batch_translate(titles_en)
            for i, title_he in enumerate(titles_he):
                result['news'].append({
                    'title_he': title_he.strip(),
                    'date': dates[i] if i < len(dates) else '',
                })

    except Exception as e:
        log.error(f"Finviz fetch failed for {symbol}: {e}")

    return result


_FIB_RATIO_ICON: dict[float, str] = {
    0: '⬜', 0.236: '🔴', 0.382: '🔹', 0.5: '🟠', 0.618: '⬜', 0.764: '🔴',
    0.88: '🔵', 1: '🟢', 1.272: '🟣', 1.414: '🔵', 1.618: '⬜',
    2: '🟢', 2.272: '🟣', 2.414: '🔵', 2.618: '⬜',
    3: '🟢', 3.272: '🟢', 3.414: '🔵', 3.618: '⬜',
    4: '🟢', 4.236: '🎯', 4.414: '🔵', 4.618: '⬜', 4.764: '🔴',
}


def _format_fib_text(sym: str, price: float, vwap: float = 0,
                     ma_rows: list[dict] | None = None) -> str:
    """Build fib levels text — 10 above (descending) + 5 below with % distance, ratio icons, and nearby MAs."""
    if price > 0:
        calc_fib_levels(sym, price)
    with _fib_cache_lock:
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

    # Build sorted list of MA values for interleaving between fib levels
    # All timeframes except 4h
    _TF_KEEP = {'1m': 'M1', '5m': 'M5', '15m': 'M15', '1h': 'H1',
                'D': 'D', 'W': 'W', 'MO': 'MO'}
    ma_all: list[tuple[float, str]] = []  # (value, label)
    if ma_rows:
        for r in ma_rows:
            tf_s = _TF_KEEP.get(r['tf'])
            if not tf_s:
                continue
            for ma_type, key in [('S', 'sma'), ('E', 'ema')]:
                val = r.get(key)
                if val and val > 0:
                    ma_all.append((val, f"{ma_type}{r['period']} {tf_s}"))

    def _icon(lv: float) -> str:
        info = ratio_map.get(round(lv, 4))
        if info:
            return _FIB_RATIO_ICON.get(info[0], '⬜')
        return '⬜'

    def _fmt(lv: float) -> str:
        pct_dist = (lv - price) / price * 100
        icon = _icon(lv)
        p = _p(lv)
        return f"{icon} {p}  {pct_dist:+.1f}%"

    def _p(v: float) -> str:
        if v >= 1: return f"${v:.2f}"
        if v >= 0.1: return f"${v:.3f}"
        return f"${v:.4f}"

    def _vwap_line(v: float) -> str:
        pct_dist = (v - price) / price * 100
        return f"📊 <b>VWAP {_p(v)}</b>  {pct_dist:+.1f}%"

    # Build descending anchor list: fib levels + VWAP + price separator
    anchors: list[float] = sorted(
        set([lv for lv in above] + [lv for lv in below]
            + ([vwap] if vwap > 0 else []) + [price]),
        reverse=True,
    )
    fib_range_lo = below[-1] if below else price
    fib_range_hi = above[0] if above else price

    # Bucket MAs into intervals between adjacent anchors (only within fib range)
    # bucket[i] holds MAs that fall between anchors[i] and anchors[i+1]
    buckets: list[list[str]] = [[] for _ in range(len(anchors) - 1)]
    for val, label in ma_all:
        if val > fib_range_hi or val < fib_range_lo:
            continue  # skip MAs outside the displayed fib range
        for i in range(len(anchors) - 1):
            if anchors[i] >= val > anchors[i + 1]:
                buckets[i].append(label)
                break

    lines = [f"\n📐 <b>פיבונאצ'י</b> ({sym})"]
    lines.append(f"🕯 עוגן: {_p(anchor_low)} — {_p(anchor_high)}  ({anchor_date})")
    lines.append("")
    for i, a in enumerate(anchors):
        if a == price:
            lines.append(f"━━━━ ${price:.2f} ━━━━")
        elif a == vwap and vwap > 0:
            lines.append(_vwap_line(vwap))
        else:
            lines.append(_fmt(a))
        # Append MA bucket between this anchor and the next
        if i < len(buckets) and buckets[i]:
            lines.append(f"  ╌╌ {', '.join(buckets[i])}")
    return "\n".join(lines)


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
#  US Market Holiday Calendar
# ══════════════════════════════════════════════════════════

# NYSE/NASDAQ holidays 2026 (+ early closes marked with *)
_US_HOLIDAYS_2026 = {
    '2026-01-01': 'New Year\'s Day',
    '2026-01-19': 'MLK Day',
    '2026-02-16': 'Presidents\' Day',
    '2026-04-03': 'Good Friday',
    '2026-05-25': 'Memorial Day',
    '2026-06-19': 'Juneteenth',
    '2026-07-03': 'Independence Day (observed)',
    '2026-09-07': 'Labor Day',
    '2026-11-26': 'Thanksgiving',
    '2026-12-25': 'Christmas',
}

_holiday_alert_sent: str = ""  # "YYYY-WW" of last weekly alert


def _check_holiday_alerts():
    """Send weekly alert on Sunday/Monday about upcoming market holidays."""
    global _holiday_alert_sent

    now_et = datetime.now(ZoneInfo('US/Eastern'))
    week_key = now_et.strftime('%Y-%W')

    if _holiday_alert_sent == week_key:
        return
    # Send on Sunday (6) or Monday (0) between 8:00-9:00
    if now_et.weekday() not in (6, 0):
        return
    if not (8 <= now_et.hour < 9):
        return

    _holiday_alert_sent = week_key

    # Check next 14 days for holidays
    upcoming = []
    for day_offset in range(1, 15):
        check_date = (now_et + timedelta(days=day_offset)).strftime('%Y-%m-%d')
        if check_date in _US_HOLIDAYS_2026:
            upcoming.append((check_date, _US_HOLIDAYS_2026[check_date]))

    if not upcoming:
        return

    lines = ["📅 <b>חגים בשבועיים הקרובים:</b>"]
    for date_str, name in upcoming:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        day_name = dt.strftime('%A')
        lines.append(f"  🔴 {date_str} ({day_name}) — {name}")
    lines.append("\n⚠️ אין מסחר בימים אלה!")
    send_telegram("\n".join(lines))
    log.info(f"Holiday alert sent: {len(upcoming)} upcoming holidays")


# ══════════════════════════════════════════════════════════
#  "Stocks in Play" — Market Open Summary (9:30 ET)
# ══════════════════════════════════════════════════════════

_stocks_in_play_sent: str = ""  # "YYYY-MM-DD"


def _check_stocks_in_play(current: dict):
    """Send 'Stocks in Play' summary at market open (9:30-9:32 ET)."""
    global _stocks_in_play_sent

    now_et = datetime.now(ZoneInfo('US/Eastern'))
    today_str = now_et.strftime('%Y-%m-%d')
    if _stocks_in_play_sent == today_str:
        return
    if now_et.weekday() >= 5:
        return
    t = now_et.time()
    if not (time(9, 30) <= t <= time(9, 32)):
        return
    if not current:
        return

    _stocks_in_play_sent = today_str

    # Rank by turnover (volume / float) — best momentum indicator
    ranked = []
    for sym, d in current.items():
        pct = d.get('pct', 0)
        price = d.get('price', 0)
        vol = d.get('volume_raw', 0)
        enrich = _enrichment.get(sym, {})
        flt_shares = _parse_float_to_shares(enrich.get('float', '-'))
        turnover = (vol / flt_shares * 100) if flt_shares > 0 else 0
        ranked.append((sym, d, enrich, turnover))

    # Sort by turnover, take top 5
    ranked.sort(key=lambda x: x[3], reverse=True)
    top = ranked[:5]

    lines = [
        f"🔔 <b>Stocks in Play — פתיחת מסחר</b>",
        f"━━━━━━━━━━━━━━━━━━",
        f"📈 {len(current)} מניות בסקאנר | טופ 5 לפי turnover:",
        "",
    ]

    for i, (sym, d, enrich, turnover) in enumerate(top, 1):
        pct = d.get('pct', 0)
        price = d.get('price', 0)
        vol = d.get('volume_raw', 0)
        vol_str = _format_volume(vol) if vol else '-'
        flt = enrich.get('float', '-')
        short = enrich.get('short', '-')
        arrow = "🟢" if pct > 0 else "🔴"

        lines.append(
            f"  {i}. {arrow} <b>{sym}</b>  ${price:.2f}  {pct:+.1f}%"
        )
        lines.append(
            f"      Float:{flt}  Short:{short}  Vol:{vol_str}  Turnover:{turnover:.0f}%"
        )

        # Latest news
        news = enrich.get('news', [])
        if news:
            title = news[0].get('title_he', news[0].get('title_en', ''))
            if title:
                lines.append(f"      📰 {title}")
        lines.append("")

    # Add buttons for all symbols
    btn_rows = []
    for sym, _, _, _ in top:
        btn_rows.append([
            {'text': f'📊 {sym}', 'callback_data': f'lookup:{sym}'},
            {'text': f'📈 TV', 'url': f'https://www.tradingview.com/chart/?symbol={sym}'},
        ])
    btn = {'inline_keyboard': btn_rows} if btn_rows else None

    send_telegram("\n".join(lines), reply_markup=btn)
    log.info(f"Stocks in play sent: {len(top)} stocks")


# ══════════════════════════════════════════════════════════
#  Daily Summary (sent after market close)
# ══════════════════════════════════════════════════════════

_daily_summary_sent: str = ""  # "YYYY-MM-DD" of last summary sent
_daily_top_movers: dict[str, dict] = {}  # sym → {peak_pct, peak_price}
_daily_new_stocks: int = 0
_daily_events: list[dict] = []           # {time, type, symbol, detail}
_daily_alert_counts: dict[str, int] = {}  # alert_type → count
_daily_reports_sent: int = 0             # full stock reports sent to group


def _log_daily_event(etype: str, symbol: str, detail: str):
    """Record an event for the end-of-day full report."""
    ts = datetime.now(ZoneInfo('US/Eastern')).strftime('%H:%M')
    _daily_events.append({'time': ts, 'type': etype, 'symbol': symbol, 'detail': detail})


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


def _check_daily_summary(positions: dict[str, tuple] | None = None,
                         net_liq: float = 0, buying_power: float = 0,
                         fib_dt_sym: str | None = None, cycle_count: int = 0,
                         virtual_portfolio_summary: str = '',
                         gg_portfolio_summary: str = '',
                         mr_portfolio_summary: str = ''):
    """Send comprehensive end-of-day report at ~16:05 ET. Only once per day."""
    global _daily_summary_sent, _daily_new_stocks, _daily_reports_sent
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

    lines = [
        f"📊 <b>דוח יומי מלא — {today_str}</b>",
        "━━━━━━━━━━━━━━━━━━",
    ]

    # ── FIB DT Section ──
    fib_tracks = [e for e in _daily_events if e['type'] == 'fib_dt_track']
    fib_signals = [e for e in _daily_events if e['type'] == 'fib_dt_signal']
    fib_entries = [e for e in _daily_events if e['type'] == 'fib_dt_entry']
    fib_closes = [e for e in _daily_events if e['type'] == 'fib_dt_close']

    lines.append("")
    lines.append("📐 <b>אסטרטגיית FIB DT:</b>")
    if fib_dt_sym:
        lines.append(f"  • מניה נסרקת: {fib_dt_sym}")
    if fib_tracks:
        tracked_syms = list(dict.fromkeys(e['symbol'] for e in fib_tracks))
        lines.append(f"  • מניות שנסרקו: {', '.join(tracked_syms)}")
    lines.append(f"  • איתותי כניסה: {len(fib_signals)}")
    for e in fib_signals:
        lines.append(f"    {e['time']} — {e['symbol']} {e['detail']}")
    lines.append(f"  • כניסות שבוצעו: {len(fib_entries)}")
    for e in fib_entries:
        lines.append(f"    {e['time']} — {e['symbol']} {e['detail']}")
    lines.append(f"  • יציאות/סגירות: {len(fib_closes)}")
    for e in fib_closes:
        lines.append(f"    {e['time']} — {e['symbol']} {e['detail']}")

    if virtual_portfolio_summary:
        lines.append("")
        lines.append(virtual_portfolio_summary)

    # ── Gap and Go Section ──
    gg_entries = [e for e in _daily_events if e['type'] == 'gg_entry']
    gg_exits = [e for e in _daily_events if e['type'] == 'gg_exit']

    lines.append("")
    lines.append("🚀 <b>אסטרטגיית Gap&Go:</b>")
    lines.append(f"  • כניסות: {len(gg_entries)}")
    for e in gg_entries:
        lines.append(f"    {e['time']} — {e['symbol']} {e['detail']}")
    lines.append(f"  • יציאות: {len(gg_exits)}")
    for e in gg_exits:
        lines.append(f"    {e['time']} — {e['symbol']} {e['detail']}")

    if gg_portfolio_summary:
        lines.append("")
        lines.append(gg_portfolio_summary)

    # ── Momentum Ride Section ──
    mr_entries = [e for e in _daily_events if e['type'] == 'mr_entry']
    mr_exits = [e for e in _daily_events if e['type'] == 'mr_exit']

    lines.append("")
    lines.append("📈 <b>אסטרטגיית Momentum Ride:</b>")
    lines.append(f"  • כניסות: {len(mr_entries)}")
    for e in mr_entries:
        lines.append(f"    {e['time']} — {e['symbol']} {e['detail']}")
    lines.append(f"  • יציאות: {len(mr_exits)}")
    for e in mr_exits:
        lines.append(f"    {e['time']} — {e['symbol']} {e['detail']}")

    if mr_portfolio_summary:
        lines.append("")
        lines.append(mr_portfolio_summary)

    # ── Open Positions ──
    if positions:
        total_pnl = sum(pos[3] for pos in positions.values() if len(pos) >= 4)
        lines.append("")
        lines.append("💼 <b>פוזיציות פתוחות:</b>")
        for sym, pos in sorted(positions.items()):
            qty, avg = pos[0], pos[1]
            mkt = pos[2] if len(pos) >= 3 else 0
            pnl = pos[3] if len(pos) >= 4 else 0
            icon = "🟢" if pnl >= 0 else "🔴"
            pct_chg = ((mkt - avg) / avg * 100) if avg > 0 else 0
            lines.append(f"  {icon} {sym}: {qty}sh @ ${avg:.2f} → ${mkt:.2f} ({pct_chg:+.1f}%)")
        pnl_icon = "💚" if total_pnl >= 0 else "❤️"
        lines.append(f"  {pnl_icon} סה\"כ P&L: ${total_pnl:+,.2f}")

    # ── Account ──
    if net_liq > 0:
        lines.append("")
        lines.append("💰 <b>חשבון:</b>")
        lines.append(f"  NetLiq: ${net_liq:,.0f} | Buying Power: ${buying_power:,.0f}")

    # ── Top 5 movers ──
    sorted_movers = sorted(
        _daily_top_movers.items(),
        key=lambda x: abs(x[1]['peak_pct']),
        reverse=True,
    )[:5]

    if sorted_movers:
        lines.append("")
        lines.append("🏆 <b>טופ 5 מובילים:</b>")
        for i, (sym, info) in enumerate(sorted_movers, 1):
            arrow = "🟢" if info['peak_pct'] > 0 else "🔴"
            enrich = _enrichment.get(sym, {})
            flt = enrich.get('float', '-')
            lines.append(
                f"  {i}. {arrow} <b>{sym}</b> {info['peak_pct']:+.1f}% "
                f"(${info['peak_price']:.2f}) Float:{flt}"
            )

    # ── Alert counts ──
    total_alerts = sum(_daily_alert_counts.values())
    lines.append("")
    lines.append(f"🔔 <b>התראות שנשלחו:</b> {total_alerts}")
    if _daily_alert_counts:
        parts = [f"{k}: {v}" for k, v in sorted(_daily_alert_counts.items())]
        lines.append(f"  {' | '.join(parts)}")

    # ── Stats ──
    lines.append("")
    lines.append("📈 <b>סטטיסטיקות:</b>")
    lines.append(f"  • סך מניות בסקאנר: {len(_daily_top_movers)}")
    lines.append(f"  • מניות חדשות: {_daily_new_stocks}")
    lines.append(f"  • דוחות מלאים (לקבוצה): {_daily_reports_sent}")
    lines.append(f"  • סייקלים: {cycle_count}")

    lines.append("")
    lines.append("📡 המוניטור ממשיך — After Hours")

    send_telegram("\n".join(lines))
    log.info("Daily full report sent")

    # Reset daily stats for next day
    _daily_top_movers.clear()
    _daily_new_stocks = 0
    _daily_events.clear()
    _daily_alert_counts.clear()
    _daily_reports_sent = 0


# ══════════════════════════════════════════════════════════
#  Session Summaries (pre-market / market / after-hours)
# ══════════════════════════════════════════════════════════

_session_summary_sent: set[str] = set()  # e.g. "2026-02-19_pre_market"
_session_stocks: dict[str, dict] = {}  # sym → {pct, price, volume_raw} per session

_SESSION_WINDOWS = {
    # session_name: (start_hour, start_min, end_hour, end_min, summary_label)
    'pre_market':  (4, 0, 9, 30, '🌅 Pre-Market'),
    'market':      (9, 30, 16, 0, '🏛️ Market'),
    'after_hours': (16, 0, 20, 0, '🌙 After-Hours'),
}


def _track_session_stocks(current: dict):
    """Track best stats per symbol for the current session."""
    for sym, d in current.items():
        pct = d.get('pct', 0)
        price = d.get('price', 0)
        vol = d.get('volume_raw', 0)
        prev = _session_stocks.get(sym)
        if prev is None or abs(pct) > abs(prev.get('pct', 0)):
            _session_stocks[sym] = {'pct': pct, 'price': price, 'volume_raw': vol}
        elif vol > prev.get('volume_raw', 0):
            _session_stocks[sym]['volume_raw'] = vol


def _check_session_summary():
    """Send summary when a session ends. Checks if we just crossed a boundary."""
    now_et = datetime.now(ZoneInfo('US/Eastern'))
    if now_et.weekday() >= 5:
        return
    today_str = now_et.strftime('%Y-%m-%d')
    t = now_et.time()

    for sess_name, (sh, sm, eh, em, label) in _SESSION_WINDOWS.items():
        key = f"{today_str}_{sess_name}"
        if key in _session_summary_sent:
            continue
        end_t = time(eh, em)
        # Send summary 1-5 min after session end
        window_start = time(eh, em)
        window_end = time(eh, min(em + 5, 59))
        if not (window_start <= t <= window_end):
            continue
        if not _session_stocks:
            continue

        _session_summary_sent.add(key)

        # Top 5 by percentage
        by_pct = sorted(_session_stocks.items(),
                        key=lambda x: abs(x[1]['pct']), reverse=True)[:5]
        # Top 5 by volume
        by_vol = sorted(_session_stocks.items(),
                        key=lambda x: x[1].get('volume_raw', 0), reverse=True)[:5]

        lines = [
            f"📋 <b>סיכום {label}</b>",
            f"━━━━━━━━━━━━━━━━━━",
        ]

        # Collect unique symbols from both lists for detail section
        seen_syms = []
        for sym, _ in by_pct + by_vol:
            if sym not in seen_syms:
                seen_syms.append(sym)

        if by_pct:
            lines.append("")
            lines.append("🏆 <b>אחוזים:</b>")
            for i, (sym, info) in enumerate(by_pct, 1):
                arrow = "🟢" if info['pct'] > 0 else "🔴"
                vol = info.get('volume_raw', 0)
                vol_str = _format_volume(vol) if vol else '-'
                enrich = _enrichment.get(sym, {})
                flt = enrich.get('float', '-')
                lines.append(
                    f"  {i}. {arrow} <b>{sym}</b> {info['pct']:+.1f}%  ${info['price']:.2f}  Vol:{vol_str}  Float:{flt}"
                )

        if by_vol:
            lines.append("")
            lines.append("📊 <b>ווליום:</b>")
            for i, (sym, info) in enumerate(by_vol, 1):
                vol = info.get('volume_raw', 0)
                vol_str = _format_volume(vol) if vol else '-'
                enrich = _enrichment.get(sym, {})
                flt = enrich.get('float', '-')
                lines.append(
                    f"  {i}. <b>{sym}</b> {info['pct']:+.1f}%  ${info['price']:.2f}  Vol:{vol_str}  Float:{flt}"
                )

        # News section — show latest headline per stock
        news_lines = []
        for sym in seen_syms:
            enrich = _enrichment.get(sym, {})
            news = enrich.get('news', [])
            if news:
                latest = news[0]
                title = latest.get('title_he', latest.get('title_en', ''))
                if title:
                    news_lines.append(f"  • <b>{sym}</b>: {title}")
        if news_lines:
            lines.append("")
            lines.append("📰 <b>חדשות:</b>")
            lines.extend(news_lines)

        lines.append(f"\n📈 סה\"כ מניות בסקאנר: {len(_session_stocks)}")
        send_telegram("\n".join(lines))
        log.info(f"Session summary sent: {sess_name}")

        # Reset for next session
        _session_stocks.clear()



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


# ══════════════════════════════════════════════════════════
#  Real-Time Trading Alerts (≥20% stocks only)
# ══════════════════════════════════════════════════════════

ALERT_MIN_PCT = 20.0  # only send alerts for stocks ≥20%

# ── Alert tracking state ──
_alerts_date: str = ""
_hod_break_alerted: dict[str, float] = {}              # sym -> last HOD value alerted
_fib_touch_tracker: dict[str, dict[float, int]] = {}   # sym -> {fib_level: touch_count}
_lod_touch_tracker: dict[str, int] = {}                 # sym -> touch count at day_low
_lod_was_near: dict[str, bool] = {}                     # sym -> was near LOD last cycle
_vwap_side: dict[str, str] = {}                         # sym -> 'above' | 'below'
_price_history: dict[str, list[tuple[float, float]]] = {}  # sym -> [(timestamp, price)]
_spike_alerted: dict[str, float] = {}                   # sym -> timestamp (cooldown)

_VWAP_COOLDOWN_SEC = 600    # 10 min between VWAP cross alerts per symbol
_vwap_last_alert: dict[str, float] = {}
_SPIKE_COOLDOWN_SEC = 300    # 5 min cooldown per symbol for spike alerts
_VOL_ALERT_COOLDOWN_SEC = 1800  # 30 min cooldown per symbol for volume alerts
_vol_alert_sent: dict[str, float] = {}  # sym -> last alert timestamp
VOL_ALERT_RVOL_MIN = 3.0  # minimum RVOL for volume alert


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
        _vwap_last_alert.clear()
        _price_history.clear()
        _spike_alerted.clear()
        _vol_alert_sent.clear()
        _session_summary_sent.clear()
        _session_stocks.clear()
        _avg_vol_cache.clear()
        _contract_cache.clear()
        _enrichment.clear()
        _enrichment_ts.clear()
        _intraday_cache.clear()
        with _fib_cache_lock:
            _fib_cache.clear()
        _daily_cache.clear()
        log.info(f"Alert state reset for new day: {today}")


def check_hod_break(sym: str, current: dict, previous: dict) -> str | None:
    """Alert when price breaks today's high of day (≥20% only)."""
    if current.get('pct', 0) < ALERT_MIN_PCT:
        return None
    cur_high = current.get('day_high', 0)
    prev_high = previous.get('day_high', 0)
    price = current.get('price', 0)
    if cur_high <= 0 or prev_high <= 0:
        return None
    if cur_high > prev_high and price >= cur_high * 0.998:
        last_alerted = _hod_break_alerted.get(sym, 0)
        if cur_high <= last_alerted:
            return None
        _hod_break_alerted[sym] = cur_high
        pct = current.get('pct', 0)
        return (
            f"🔺 <b>HOD BREAK — {sym}</b>\n"
            f"💰 ${price:.2f} → שיא יומי חדש!\n"
            f"📊 קודם: ${prev_high:.2f} | שינוי: {pct:+.1f}%"
        )
    return None


def check_fib_second_touch(sym: str, price: float, pct: float) -> str | None:
    """Alert on 2nd+ touch of a Fibonacci level (≥20% only, 0.8% proximity)."""
    if pct < ALERT_MIN_PCT or price <= 0:
        return None
    with _fib_cache_lock:
        has_cache = sym in _fib_cache
    if not has_cache:
        calc_fib_levels(sym, price)
    with _fib_cache_lock:
        cached = _fib_cache.get(sym)
    if not cached:
        return None

    _, _, all_levels, ratio_map, *_ = cached
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
                    return (
                        f"🎯🎯 <b>FIB TOUCH x2 — {sym}</b>\n"
                        f"📍 רמה: ${lv:.4f} ({ratio_label})\n"
                        f"💰 ${price:.2f} | שינוי: {pct:+.1f}%"
                    )
    return None


def check_lod_touch(sym: str, price: float, day_low: float, pct: float) -> str | None:
    """Alert on 2nd touch of day's low (≥20% only, 0.5% proximity)."""
    if pct < ALERT_MIN_PCT or price <= 0 or day_low <= 0:
        return None

    threshold = day_low * 0.005  # 0.5%
    near_lod = abs(price - day_low) <= threshold

    was_near = _lod_was_near.get(sym, False)
    _lod_was_near[sym] = near_lod

    if near_lod and not was_near:
        count = _lod_touch_tracker.get(sym, 0) + 1
        _lod_touch_tracker[sym] = count
        if count == 2:
            return (
                f"🔻🔻 <b>LOD TOUCH x2 — {sym}</b>\n"
                f"📍 נמוך יומי: ${day_low:.2f}\n"
                f"💰 ${price:.2f} | שינוי: {pct:+.1f}%\n"
                f"⚠️ נגיעה שנייה — תמיכה/שבירה?"
            )
    return None


def check_vwap_cross(sym: str, price: float, vwap: float, pct: float) -> str | None:
    """Alert when price crosses VWAP (10-min cooldown per symbol)."""
    if price <= 0 or vwap <= 0:
        return None

    current_side = 'above' if price > vwap else 'below'
    prev_side = _vwap_side.get(sym)
    _vwap_side[sym] = current_side

    if prev_side is None or prev_side == current_side:
        return None

    last = _vwap_last_alert.get(sym, 0)
    if time_mod.time() - last < _VWAP_COOLDOWN_SEC:
        return None

    _vwap_last_alert[sym] = time_mod.time()

    if prev_side == 'below':
        return (
            f"⚡ <b>VWAP CROSS — {sym}</b>\n"
            f"🟢 חצה מעל VWAP!\n"
            f"💰 ${price:.2f} > VWAP ${vwap:.2f} | {pct:+.1f}%"
        )
    else:
        return (
            f"⚡ <b>VWAP CROSS — {sym}</b>\n"
            f"🔴 חצה מתחת VWAP!\n"
            f"💰 ${price:.2f} < VWAP ${vwap:.2f} | {pct:+.1f}%"
        )


def check_spike(sym: str, price: float, pct: float) -> str | None:
    """Alert when price rises 8%+ within 1-3 minutes (≥20% only).

    Tracks price history per symbol and compares current price
    to entries 60-180 seconds ago. Cooldown of 5 min per symbol.
    """
    if pct < ALERT_MIN_PCT or price <= 0:
        return None

    now = time_mod.time()

    # Cooldown check
    last_alert = _spike_alerted.get(sym, 0)
    if now - last_alert < _SPIKE_COOLDOWN_SEC:
        # Still record price even during cooldown
        _price_history.setdefault(sym, []).append((now, price))
        return None

    if sym not in _price_history:
        _price_history[sym] = []

    _price_history[sym].append((now, price))
    # Prune entries older than 200 seconds
    cutoff = now - 200
    _price_history[sym] = [(t, p) for t, p in _price_history[sym] if t > cutoff]

    # Find oldest entry within 60-180s ago window
    candidates = [(t, p) for t, p in _price_history[sym]
                  if 60 <= (now - t) <= 180]
    if not candidates:
        return None

    old_t, old_price = min(candidates, key=lambda x: x[0])  # oldest in window
    if old_price <= 0:
        return None

    change_pct = (price - old_price) / old_price * 100
    elapsed_sec = int(now - old_t)
    elapsed_min = elapsed_sec / 60

    if change_pct >= 8.0:
        _spike_alerted[sym] = now
        return (
            f"🚀 <b>SPIKE +{change_pct:.1f}% — {sym}</b>\n"
            f"⏱️ עלייה של {change_pct:.1f}% ב-{elapsed_min:.1f} דקות!\n"
            f"💰 ${old_price:.2f} → ${price:.2f} | יומי: {pct:+.1f}%"
        )
    return None


def check_volume_alert(sym: str, price: float, vwap: float,
                       rvol: float, pct: float) -> str | None:
    """Alert on high-volume stocks above VWAP, even below 20%.

    Fires for enriched stocks (≥16%) with RVOL ≥ 3.0 and price > VWAP.
    Skips stocks already at ≥20% (those get the regular alerts).
    30-min cooldown per symbol.
    """
    if price <= 0 or vwap <= 0 or rvol < VOL_ALERT_RVOL_MIN:
        return None
    if price <= vwap:
        return None
    # Skip stocks already getting ≥20% alerts
    if pct >= ALERT_MIN_PCT:
        return None

    now = time_mod.time()
    last = _vol_alert_sent.get(sym, 0)
    if now - last < _VOL_ALERT_COOLDOWN_SEC:
        return None

    _vol_alert_sent[sym] = now

    enrich = _enrichment.get(sym, {})
    news = enrich.get('news', [])
    news_line = ""
    if news:
        title = news[0].get('title_he', news[0].get('title_en', ''))
        if title:
            news_line = f"\n📰 {title}"

    return (
        f"📊 <b>VOLUME — {sym}</b>\n"
        f"🔥 RVOL {rvol:.1f}x | מעל VWAP!\n"
        f"💰 ${price:.2f} ({pct:+.1f}%) > VWAP ${vwap:.2f}"
        f"{news_line}"
    )


# ══════════════════════════════════════════════════════════
#  Stock Enrichment Cache (Finviz + Fib)
# ══════════════════════════════════════════════════════════

# {symbol: {float, short, eps, income, earnings, cash, fib_below, fib_above, news}}
_enrichment: dict[str, dict] = {}
_enrichment_ts: dict[str, float] = {}   # symbol → time.time() when enriched
ENRICHMENT_TTL_SECS = 30 * 60           # 30 minutes

# Global GUI alert callback — set by App, used by standalone functions
# (stock reports, news alerts) to push alerts to GUI ALERTS panel.
_gui_alert_cb: callable = None


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
                titles_he = _batch_translate(new_titles_en)
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
    # Monthly: resample weekly → monthly
    w_frame = ma_frames.get('W')
    if w_frame is not None and len(w_frame) >= 5 and 'date' in w_frame.columns:
        try:
            mf = w_frame.copy()
            mf['date'] = pd.to_datetime(mf['date'])
            monthly = mf.set_index('date')['close'].resample('ME').last().dropna()
            for p in periods:
                sma_val = float(monthly.rolling(p).mean().iloc[-1]) if len(monthly) >= p else None
                ema_val = float(monthly.ewm(span=p, adjust=False).mean().iloc[-1]) if len(monthly) >= p else None
                rows.append({'tf': 'MO', 'period': p, 'sma': sma_val, 'ema': ema_val})
        except Exception:
            for p in periods:
                rows.append({'tf': 'MO', 'period': p, 'sma': None, 'ema': None})
    else:
        for p in periods:
            rows.append({'tf': 'MO', 'period': p, 'sma': None, 'ema': None})
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
        open_gaps = _find_unfilled_gaps(df)

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
    # Order: Header → News → Fundamentals → Technical → Fib
    lines = [
        f"🆕 <b>{sym}</b> — ${price:.2f}  {stock['pct']:+.1f}%  Vol:{stock.get('volume', '-')}",
    ]

    # 1. News (Hebrew) — first
    if enriched['news']:
        lines.append("")
        lines.append(f"📰 <b>חדשות:</b>")
        for n in enriched['news']:
            src = n.get('source', '')
            src_tag = f" [{src}]" if src else ""
            lines.append(f"  • {n['title_he']}  <i>({n['date']}{src_tag})</i>")

    # 2. Company background + fundamentals
    lines.append("")
    lines.append(f"🏢 {enriched.get('company', '-')}")
    lines.append(f"🌎 {enriched.get('country', '-')} | {enriched.get('sector', '-')} | {enriched.get('industry', '-')}")
    lines.append(f"🏛️ Inst: {enriched.get('inst_own', '-')} ({enriched.get('inst_trans', '-')}) | Insider: {enriched.get('insider_own', '-')} ({enriched.get('insider_trans', '-')})")
    lines.append(f"💰 MCap: {enriched.get('market_cap', '-')}")
    lines.append("")
    lines.append(f"📊 Float: {enriched['float']} | Short: {enriched['short']}")
    lines.append(f"💰 {eps_icon} EPS: {eps} | Cash: ${enriched['cash']}")
    lines.append(f"📅 Earnings: {enriched['earnings']}")
    lines.append(f"📉 Vol: {enriched.get('fvz_volume', '-')} | Avg: {enriched.get('avg_volume', '-')}")
    lines.append(f"📊 Volatility: W {enriched.get('vol_w', '-')} | M {enriched.get('vol_m', '-')}")
    lines.append(f"🎯 52W: ↑${enriched.get('52w_high', '-')} | ↓${enriched.get('52w_low', '-')}")

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
        unfilled_gaps = _find_unfilled_gaps(df_daily)
        if unfilled_gaps:
            lines.append(f"🕳️ גאפים פתוחים ({len(unfilled_gaps)}):")
            for g in unfilled_gaps[-3:]:  # show last 3
                icon = "⬆️" if g['up'] else "⬇️"
                pct = g['pct'] if g['up'] else -g['pct']
                lines.append(f"  {icon} ${g['bottom']:.2f}—${g['top']:.2f} ({pct:+.1f}%)")

    # Resist line
    if resist_str:
        lines.append(f"📉 Resist: {resist_str}")
    else:
        lines.append("✅ אין התנגדויות — מחיר מעל כל הממוצעים")

    # 3. Fibonacci levels (text) — always last in text
    fib_text = _format_fib_text(sym, price, vwap=vwap, ma_rows=ma_rows)
    if fib_text:
        lines.append("")
        lines.append(fib_text.lstrip("\n"))

    text = "\n".join(lines)

    # ── Fib chart image ──
    chart_path: Path | None = None
    with _fib_cache_lock:
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
    send_telegram_alert(text)
    if chart_path:
        send_telegram_alert_photo(chart_path, f"📐 {sym} — Daily + Fibonacci ${stock['price']:.2f}")
    # Push summary to GUI ALERTS panel
    if _gui_alert_cb:
        flt = enriched.get('float', '-')
        _gui_alert_cb(f"📋 REPORT — {sym} ${stock['price']:.2f} {stock['pct']:+.1f}% Float:{flt}")


# ══════════════════════════════════════════════════════════
#  Fibonacci Levels (WTS Method)
# ══════════════════════════════════════════════════════════

# Cache: {symbol: (anchor_low, anchor_high, all_levels_sorted, ratio_map, anchor_date)}
# ratio_map: {price: (ratio, "S1"|"S2")}
_fib_cache: dict[str, tuple] = {}
_fib_cache_lock = threading.Lock()

# Cache daily DataFrames for chart generation (filled by _download_daily)
_daily_cache: dict[str, pd.DataFrame] = {}


def _download_daily(symbol: str) -> pd.DataFrame | None:
    """Download 5 years daily data from IBKR.

    Falls back to 4h bars resampled to daily if daily bars are blocked
    (e.g. tradingClass='SCM' with no market data permissions).
    """
    ib = _get_ibkr()
    if not ib:
        log.error(f"No IBKR connection for {symbol} daily download")
        return None
    try:
        contract = _get_contract(symbol)
        if not contract:
            return None
        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='5 Y',
            barSizeSetting='1 day', whatToShow='TRADES', useRTH=False,
            timeout=15,
        )
        if bars:
            df = ib_util.df(bars)
            if len(df) >= 5:
                log.info(f"IBKR: {symbol} {len(df)} daily bars")
                _daily_cache[symbol] = df
                return df
    except Exception as e:
        log.warning(f"IBKR download {symbol}: {e}")

    # ── Fallback: 4h bars → resample to daily ──
    try:
        bars_4h = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='1 Y',
            barSizeSetting='4 hours', whatToShow='TRADES', useRTH=False,
            timeout=15,
        )
        if bars_4h and len(bars_4h) >= 20:
            df4 = ib_util.df(bars_4h)
            df4.index = pd.to_datetime(df4['date'] if 'date' in df4.columns else df4.index)
            daily = df4.resample('D').agg({
                'open': 'first', 'high': 'max', 'low': 'min',
                'close': 'last', 'volume': 'sum',
            }).dropna(subset=['close'])
            if 'average' in df4.columns:
                daily['average'] = df4['average'].resample('D').mean()
            if len(daily) >= 5:
                log.info(f"IBKR: {symbol} {len(daily)} daily bars (resampled from 4h)")
                _daily_cache[symbol] = daily
                return daily
    except Exception as e:
        log.warning(f"IBKR 4h fallback {symbol}: {e}")
    return None


_intraday_cache: dict[str, tuple[float, pd.DataFrame]] = {}  # key → (timestamp, df)
_INTRADAY_CACHE_TTL = 120  # 2 minutes


def _download_intraday(symbol: str, bar_size: str = '5 mins',
                       duration: str = '3 D') -> pd.DataFrame | None:
    """Download intraday bars from IBKR (cached for 2 min per symbol+bar_size).

    Returns DataFrame with OHLCV columns, or None on failure.
    """
    cache_key = f"{symbol}_{bar_size}"
    if cache_key in _intraday_cache:
        cached_ts, cached_df = _intraday_cache[cache_key]
        if time_mod.time() - cached_ts < _INTRADAY_CACHE_TTL:
            return cached_df

    ib = _get_ibkr()
    if not ib:
        log.error(f"No IBKR connection for {symbol} intraday download")
        return None
    try:
        contract = _get_contract(symbol)
        if not contract:
            return None
        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr=duration,
            barSizeSetting=bar_size, whatToShow='TRADES', useRTH=False,
            timeout=15,
        )
        if bars:
            df = ib_util.df(bars)
            if len(df) >= 5:
                log.info(f"IBKR: {symbol} {len(df)} intraday bars ({bar_size})")
                _intraday_cache[cache_key] = (time_mod.time(), df)
                return df
    except Exception as e:
        log.warning(f"IBKR intraday {symbol}: {e}")
    return None


def calc_fib_levels(symbol: str, current_price: float) -> tuple[list[float], list[float]]:
    """Calculate Fibonacci levels using dual-series recursive method.

    Returns (3_below, 3_above) relative to current_price.
    Auto-advances when price > 4.236 of the LOWER series (S1).
    """
    with _fib_cache_lock:
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

        with _fib_cache_lock:
            _fib_cache[symbol] = (anchor_low, anchor_high, all_levels, ratio_map, anchor_date)

    below = [l for l in all_levels if l <= current_price][-5:]
    above = [l for l in all_levels if l > current_price][:10]
    return below, above



# ══════════════════════════════════════════════════════════
#  Sound Alerts
# ══════════════════════════════════════════════════════════

_SOUND_FILES = {
    'hod':   '/usr/share/sounds/freedesktop/stereo/complete.oga',
    'spike': '/usr/share/sounds/freedesktop/stereo/alarm-clock-elapsed.oga',
    'fib':   '/usr/share/sounds/freedesktop/stereo/bell.oga',
    'lod':   '/usr/share/sounds/freedesktop/stereo/dialog-warning.oga',
    'vwap':  '/usr/share/sounds/freedesktop/stereo/device-added.oga',
}

# Global sound state (controlled by GUI)
_sound_enabled: bool = True
_sound_volume: int = 70   # 0-100


def play_alert_sound(alert_type: str = 'hod'):
    """Play a sound alert in a background thread. Non-blocking.

    ``alert_type``: one of 'hod', 'spike', 'fib', 'lod', 'vwap'.
    Respects _sound_enabled and _sound_volume globals.
    """
    if not _sound_enabled or _sound_volume <= 0:
        return
    sound_file = _SOUND_FILES.get(alert_type, _SOUND_FILES['hod'])

    def _play():
        try:
            # pw-play supports --volume as linear float (0.0 - 1.0)
            vol_float = f"{_sound_volume / 100:.2f}"
            subprocess.run(
                ['pw-play', '--volume', vol_float, sound_file],
                timeout=5, capture_output=True,
            )
        except FileNotFoundError:
            # Fallback: use aplay (no volume control)
            try:
                subprocess.run(['aplay', '-q', sound_file],
                               timeout=5, capture_output=True)
            except Exception:
                pass
        except Exception:
            pass

    threading.Thread(target=_play, daemon=True).start()


# ══════════════════════════════════════════════════════════
#  Telegram
# ══════════════════════════════════════════════════════════

_TG_MAX_LEN = 4000  # Telegram limit is 4096, keep margin for safety


def send_telegram(text: str, reply_markup: dict | None = None) -> bool:
    """Send text message to personal chat and group (if configured).

    Auto-splits messages that exceed Telegram's 4096-char limit.
    ``reply_markup`` can be an InlineKeyboardMarkup dict for buttons.
    """
    if not BOT_TOKEN or not CHAT_ID:
        return False

    # Split long messages at double-newline boundaries
    if len(text) > _TG_MAX_LEN:
        chunks = _split_telegram_message(text)
        ok = True
        for i, chunk in enumerate(chunks):
            # Only attach buttons to the last chunk
            rm = reply_markup if i == len(chunks) - 1 else None
            ok = _send_telegram_raw(chunk, rm) and ok
        return ok

    return _send_telegram_raw(text, reply_markup)


def _split_telegram_message(text: str) -> list[str]:
    """Split text into chunks ≤ _TG_MAX_LEN chars, breaking at paragraph boundaries."""
    chunks = []
    current = ""
    for paragraph in text.split("\n\n"):
        if current and len(current) + len(paragraph) + 2 > _TG_MAX_LEN:
            chunks.append(current.rstrip())
            current = ""
        current += ("" if not current else "\n\n") + paragraph
    if current:
        chunks.append(current.rstrip())
    return chunks if chunks else [text[:_TG_MAX_LEN]]


def _send_telegram_raw(text: str, reply_markup: dict | None = None) -> bool:
    """Send a single message to private chat only (system messages)."""
    if not CHAT_ID:
        return False
    try:
        payload: dict = {'chat_id': CHAT_ID, 'text': text, 'parse_mode': 'HTML'}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json=payload, timeout=10,
        )
        return resp.ok
    except Exception as e:
        log.error(f"Telegram ({CHAT_ID}): {e}")
        return False


def send_telegram_alert(text: str, reply_markup: dict | None = None) -> bool:
    """Send alert message to group chat only (stock alerts, momentum, etc.)."""
    if not BOT_TOKEN or not GROUP_CHAT_ID:
        return False

    if len(text) > _TG_MAX_LEN:
        chunks = _split_telegram_message(text)
        ok = True
        for i, chunk in enumerate(chunks):
            rm = reply_markup if i == len(chunks) - 1 else None
            ok = _send_alert_raw(chunk, rm) and ok
        return ok

    return _send_alert_raw(text, reply_markup)


def _send_alert_raw(text: str, reply_markup: dict | None = None) -> bool:
    """Send a single alert message to group chat."""
    try:
        payload: dict = {'chat_id': GROUP_CHAT_ID, 'text': text, 'parse_mode': 'HTML'}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json=payload, timeout=10,
        )
        return resp.ok
    except Exception as e:
        log.error(f"Telegram alert ({GROUP_CHAT_ID}): {e}")
        return False


def send_telegram_alert_photo(image_path: Path, caption: str = "") -> bool:
    """Send a photo alert to group chat only."""
    if not BOT_TOKEN or not GROUP_CHAT_ID:
        return False
    try:
        with open(image_path, 'rb') as photo:
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                data={'chat_id': GROUP_CHAT_ID, 'caption': caption, 'parse_mode': 'HTML'},
                files={'photo': photo},
                timeout=30,
            )
        return resp.ok
    except Exception as e:
        log.error(f"Telegram alert photo ({GROUP_CHAT_ID}): {e}")
        return False


def _make_lookup_button(sym: str) -> dict:
    """Build InlineKeyboardMarkup with Report + TradingView buttons."""
    return {
        'inline_keyboard': [[
            {'text': f'📊 דוח מלא — {sym}', 'callback_data': f'lookup:{sym}'},
            {'text': f'📈 TradingView', 'url': f'https://www.tradingview.com/chart/?symbol={sym}'},
        ]]
    }


def send_telegram_photo(image_path: Path, caption: str = "") -> bool:
    """Send a photo to private chat only (system messages)."""
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
        log.error(f"Telegram photo ({CHAT_ID}): {e}")
        return False


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
#  Virtual Portfolio for FIB DT Simulation
# ══════════════════════════════════════════════════════════

class VirtualPortfolio:
    """Virtual portfolio for FIB DT paper simulation.

    Tracks cash, positions, and P&L independently of the IBKR account.
    Uses real-time prices for stop/target checks.
    Writes all trades to a CSV journal for performance analysis.
    """

    INITIAL_CASH = 3000.0
    JOURNAL_PATH = DATA_DIR / "virtual_trades.csv"

    def __init__(self):
        self.cash: float = self.INITIAL_CASH
        self.positions: dict[str, dict] = {}
        # sym -> {qty, entry_price, stop, target, half, other_half, phase}
        # phase: IN_POSITION | TRAILING
        self.trades: list[dict] = []  # history
        self._init_journal()

    def _init_journal(self):
        """Create CSV journal with header if it doesn't exist."""
        if not self.JOURNAL_PATH.exists():
            with open(self.JOURNAL_PATH, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow([
                    'date', 'time_et', 'symbol', 'side', 'reason',
                    'qty', 'price', 'entry_price', 'pnl', 'pnl_pct',
                    'cash_after', 'net_liq', 'positions_open',
                ])

    def _log_journal(self, sym: str, side: str, reason: str,
                     qty: int, price: float, entry_price: float,
                     pnl: float, net_liq: float):
        """Append a row to the CSV journal."""
        now = datetime.now(_ET)
        pnl_pct = ((price / entry_price - 1) * 100) if entry_price > 0 and side != 'BUY' else 0.0
        try:
            with open(self.JOURNAL_PATH, 'a', newline='') as f:
                w = csv.writer(f)
                w.writerow([
                    now.strftime('%Y-%m-%d'),
                    now.strftime('%H:%M:%S'),
                    sym, side, reason,
                    qty, f'{price:.4f}', f'{entry_price:.4f}',
                    f'{pnl:.2f}', f'{pnl_pct:.1f}',
                    f'{self.cash:.2f}', f'{net_liq:.2f}',
                    len(self.positions),
                ])
        except Exception as e:
            log.warning(f"Journal write error: {e}")

    @staticmethod
    def _ts() -> str:
        """Current ET timestamp for alerts."""
        return datetime.now(_ET).strftime('%Y-%m-%d %H:%M ET')

    def buy(self, sym: str, qty: int, price: float,
            stop: float, target: float) -> str | None:
        """Open a virtual position. Returns Telegram alert text or None."""
        if qty < 2:
            return None
        if sym in self.positions:
            return None

        cost = qty * price
        if cost > self.cash:
            return None

        half = qty // 2
        other_half = qty - half
        old_cash = self.cash
        self.cash -= cost

        self.positions[sym] = {
            'qty': qty,
            'entry_price': price,
            'stop': stop,
            'target': target,
            'half': half,
            'other_half': other_half,
            'phase': 'IN_POSITION',
            'high_since_target': 0.0,
            'trailing_synced': False,
        }

        self.trades.append({
            'sym': sym, 'side': 'BUY', 'qty': qty,
            'price': price, 'pnl': 0, 'time': datetime.now(_ET),
        })

        net = self.cash + cost
        self._log_journal(sym, 'BUY', 'ENTRY', qty, price, price, 0, net)

        msg = (
            f"📐 <b>FIB DT [SIM] — BUY {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  💰 {qty}sh @ ${price:.2f} | Stop: ${stop:.2f} | Target: ${target:.2f}\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f}"
        )
        log.info(f"VIRTUAL BUY: {sym} {qty}sh @ ${price:.2f} stop=${stop:.2f} target=${target:.2f}")
        return msg

    def check_stops_and_targets(self, current_prices: dict) -> list[str]:
        """Check all positions against current prices. Returns list of alert texts."""
        alerts = []
        closed = []

        for sym, pos in self.positions.items():
            price = current_prices.get(sym, 0)
            if price <= 0:
                continue
            phase = pos['phase']

            # ── STOP HIT (any phase with remaining shares) ──
            if price <= pos['stop']:
                remaining = pos['qty']
                if phase == 'TRAILING':
                    remaining = pos['other_half']

                pnl = (price - pos['entry_price']) * remaining
                pnl_pct = (price / pos['entry_price'] - 1) * 100
                old_cash = self.cash
                self.cash += remaining * price

                self.trades.append({
                    'sym': sym, 'side': 'SELL', 'qty': remaining,
                    'price': price, 'pnl': pnl, 'time': datetime.now(_ET),
                })

                net_liq = self._net_liq_internal(current_prices, exclude=sym)
                self._log_journal(sym, 'SELL', f'STOP ({phase})', remaining, price,
                                  pos['entry_price'], pnl, net_liq)

                alert = (
                    f"📐 <b>FIB DT [SIM] — STOP HIT {sym}</b>\n"
                    f"  🕐 {self._ts()}\n"
                    f"  🔴 {remaining}sh @ ${price:.2f} (entry ${pos['entry_price']:.2f})\n"
                    f"  📉 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
                    f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
                    f"  📊 Portfolio: ${net_liq:,.0f}"
                )
                alerts.append(alert)
                closed.append(sym)
                log.info(f"VIRTUAL STOP: {sym} {remaining}sh @ ${price:.2f} P&L=${pnl:+.2f}")
                continue

            # ── UPDATE HIGH (TRAILING phase) ──
            if phase == 'TRAILING' and price > pos.get('high_since_target', 0):
                pos['high_since_target'] = price

            # ── TARGET HIT (first half) — only in IN_POSITION phase ──
            if phase == 'IN_POSITION' and price >= pos['target']:
                half = pos['half']
                pnl = (price - pos['entry_price']) * half
                pnl_pct = (price / pos['entry_price'] - 1) * 100
                old_cash = self.cash
                self.cash += half * price

                self.trades.append({
                    'sym': sym, 'side': 'SELL', 'qty': half,
                    'price': price, 'pnl': pnl, 'time': datetime.now(_ET),
                })

                pos['phase'] = 'TRAILING'
                pos['qty'] = pos['other_half']
                pos['high_since_target'] = price
                pos['stop'] = pos['entry_price']  # breakeven stop

                net_liq = self._net_liq_internal(current_prices)
                self._log_journal(sym, 'SELL', 'TARGET (half)', half, price,
                                  pos['entry_price'], pnl, net_liq)

                alert = (
                    f"📐 <b>FIB DT [SIM] — TARGET HIT {sym}</b>\n"
                    f"  🕐 {self._ts()}\n"
                    f"  🟢 {half}sh @ ${price:.2f} (entry ${pos['entry_price']:.2f})\n"
                    f"  📈 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
                    f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
                    f"  📊 Portfolio: ${net_liq:,.0f} ({pos['other_half']}sh still in position)\n"
                    f"  🛡️ Stop → breakeven ${pos['entry_price']:.2f}"
                )
                alerts.append(alert)
                log.info(f"VIRTUAL TARGET: {sym} {half}sh @ ${price:.2f} P&L=${pnl:+.2f}")

        for sym in closed:
            del self.positions[sym]

        return alerts

    def trailing_exit(self, sym: str, price: float, reason: str) -> str | None:
        """Sell remaining shares in trailing phase. Returns alert text or None."""
        pos = self.positions.get(sym)
        if not pos or pos['phase'] != 'TRAILING':
            return None

        remaining = pos['other_half']
        pnl = (price - pos['entry_price']) * remaining
        pnl_pct = (price / pos['entry_price'] - 1) * 100
        old_cash = self.cash
        self.cash += remaining * price

        self.trades.append({
            'sym': sym, 'side': 'SELL', 'qty': remaining,
            'price': price, 'pnl': pnl, 'time': datetime.now(_ET),
        })

        del self.positions[sym]

        net_liq = self.net_liq({})
        self._log_journal(sym, 'SELL', f'TRAILING ({reason})', remaining, price,
                          pos['entry_price'], pnl, net_liq)

        alert = (
            f"📐 <b>FIB DT [SIM] — TRAILING EXIT {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  🔵 {remaining}sh @ ${price:.2f} (entry ${pos['entry_price']:.2f})\n"
            f"  📈 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net_liq:,.0f}\n"
            f"  📝 Reason: {reason}"
        )
        log.info(f"VIRTUAL TRAILING EXIT: {sym} {remaining}sh @ ${price:.2f} P&L=${pnl:+.2f} ({reason})")
        return alert

    def _net_liq_internal(self, current_prices: dict, exclude: str = '') -> float:
        """Calculate net liquidation value. Optionally exclude a symbol being closed."""
        val = self.cash
        for sym, pos in self.positions.items():
            if sym == exclude:
                continue
            px = current_prices.get(sym, pos['entry_price'])
            remaining = pos['qty']
            if pos['phase'] == 'TRAILING':
                remaining = pos['other_half']
            val += remaining * px
        return val

    def net_liq(self, current_prices: dict) -> float:
        """Public net liquidation value."""
        return self._net_liq_internal(current_prices)

    def summary_text(self, current_prices: dict) -> str:
        """Generate summary text for daily report."""
        nlv = self.net_liq(current_prices)
        total_pnl = nlv - self.INITIAL_CASH
        pnl_pct = (nlv / self.INITIAL_CASH - 1) * 100

        lines = [
            f"📐 <b>FIB DT [SIM] Portfolio</b>",
            f"  💵 Cash: ${self.cash:,.0f}",
            f"  📊 Net Liq: ${nlv:,.0f} ({pnl_pct:+.1f}%)",
        ]
        if self.positions:
            lines.append(f"  📋 Open positions: {len(self.positions)}")
            for sym, pos in self.positions.items():
                px = current_prices.get(sym, pos['entry_price'])
                sym_pnl = (px - pos['entry_price']) * pos.get('qty', pos.get('other_half', 0))
                lines.append(f"    • {sym}: {pos['qty']}sh @ ${pos['entry_price']:.2f} → ${px:.2f} (${sym_pnl:+.2f})")
        if self.trades:
            today_trades = [t for t in self.trades if t['time'].date() == datetime.now(_ET).date()]
            lines.append(f"  📝 Trades today: {len(today_trades)}")
            total_realized = sum(t['pnl'] for t in today_trades if t['side'] == 'SELL')
            lines.append(f"  💰 Realized today: ${total_realized:+,.2f}")

        return "\n".join(lines)


# ══════════════════════════════════════════════════════════
#  Gap and Go Virtual Portfolio
# ══════════════════════════════════════════════════════════

class GGVirtualPortfolio:
    """Virtual portfolio for Gap and Go paper simulation.

    Simpler than VirtualPortfolio — no stop/target/half-split.
    Entry and exit are fully driven by the strategy (HA signals).
    """

    INITIAL_CASH = GG_LIVE_INITIAL_CASH
    JOURNAL_PATH = DATA_DIR / "gg_virtual_trades.csv"

    def __init__(self):
        self.cash: float = self.INITIAL_CASH
        self.positions: dict[str, dict] = {}
        # sym -> {qty, entry_price, vwap_at_entry}
        self.trades: list[dict] = []
        self._init_journal()

    def _init_journal(self):
        if not self.JOURNAL_PATH.exists():
            with open(self.JOURNAL_PATH, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow([
                    'date', 'time_et', 'symbol', 'side', 'reason',
                    'qty', 'price', 'entry_price', 'pnl', 'pnl_pct',
                    'cash_after', 'net_liq', 'positions_open',
                ])

    def _log_journal(self, sym: str, side: str, reason: str,
                     qty: int, price: float, entry_price: float,
                     pnl: float, net_liq_val: float):
        now = datetime.now(_ET)
        pnl_pct = ((price / entry_price - 1) * 100) if entry_price > 0 and side != 'BUY' else 0.0
        try:
            with open(self.JOURNAL_PATH, 'a', newline='') as f:
                w = csv.writer(f)
                w.writerow([
                    now.strftime('%Y-%m-%d'),
                    now.strftime('%H:%M:%S'),
                    sym, side, reason,
                    qty, f'{price:.4f}', f'{entry_price:.4f}',
                    f'{pnl:.2f}', f'{pnl_pct:.1f}',
                    f'{self.cash:.2f}', f'{net_liq_val:.2f}',
                    len(self.positions),
                ])
        except Exception as e:
            log.warning(f"GG journal write error: {e}")

    @staticmethod
    def _ts() -> str:
        return datetime.now(_ET).strftime('%Y-%m-%d %H:%M ET')

    def buy(self, sym: str, qty: int, price: float, vwap: float) -> str | None:
        """Open a GG virtual position. Returns Telegram alert text or None."""
        if qty < 1 or sym in self.positions:
            return None
        cost = qty * price
        if cost > self.cash:
            return None

        old_cash = self.cash
        self.cash -= cost

        self.positions[sym] = {
            'qty': qty,
            'entry_price': price,
            'vwap_at_entry': vwap,
        }

        self.trades.append({
            'sym': sym, 'side': 'BUY', 'qty': qty,
            'price': price, 'pnl': 0, 'time': datetime.now(_ET),
        })

        net = self.cash + cost
        self._log_journal(sym, 'BUY', 'ENTRY', qty, price, price, 0, net)

        msg = (
            f"🚀 <b>Gap&Go [GG-SIM] — BUY {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  💰 {qty}sh @ ${price:.4f} | VWAP: ${vwap:.4f}\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f}"
        )
        log.info(f"GG VIRTUAL BUY: {sym} {qty}sh @ ${price:.4f} vwap=${vwap:.4f}")
        return msg

    def sell(self, sym: str, price: float, reason: str) -> str | None:
        """Sell entire GG position. Returns Telegram alert text or None."""
        pos = self.positions.get(sym)
        if not pos:
            return None

        qty = pos['qty']
        entry_price = pos['entry_price']
        pnl = (price - entry_price) * qty
        pnl_pct = (price / entry_price - 1) * 100 if entry_price > 0 else 0

        old_cash = self.cash
        self.cash += qty * price

        self.trades.append({
            'sym': sym, 'side': 'SELL', 'qty': qty,
            'price': price, 'pnl': pnl, 'time': datetime.now(_ET),
        })

        del self.positions[sym]

        net = self.net_liq({})
        self._log_journal(sym, 'SELL', reason, qty, price, entry_price, pnl, net)

        pnl_icon = "🟢" if pnl >= 0 else "🔴"
        msg = (
            f"🚀 <b>Gap&Go [GG-SIM] — SELL {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  {pnl_icon} {qty}sh @ ${price:.4f} (entry ${entry_price:.4f})\n"
            f"  📈 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f}\n"
            f"  📝 Reason: {reason}"
        )
        log.info(f"GG VIRTUAL SELL: {sym} {qty}sh @ ${price:.4f} P&L=${pnl:+.2f} ({reason})")
        return msg

    def net_liq(self, current_prices: dict) -> float:
        val = self.cash
        for sym, pos in self.positions.items():
            px = current_prices.get(sym, pos['entry_price'])
            val += pos['qty'] * px
        return val

    def summary_text(self, current_prices: dict) -> str:
        nlv = self.net_liq(current_prices)
        total_pnl = nlv - self.INITIAL_CASH
        pnl_pct = (nlv / self.INITIAL_CASH - 1) * 100

        lines = [
            f"🚀 <b>Gap&Go [GG-SIM] Portfolio</b>",
            f"  💵 Cash: ${self.cash:,.0f}",
            f"  📊 Net Liq: ${nlv:,.0f} ({pnl_pct:+.1f}%)",
        ]
        if self.positions:
            lines.append(f"  📋 Open positions: {len(self.positions)}")
            for sym, pos in self.positions.items():
                px = current_prices.get(sym, pos['entry_price'])
                sym_pnl = (px - pos['entry_price']) * pos['qty']
                lines.append(f"    • {sym}: {pos['qty']}sh @ ${pos['entry_price']:.2f} → ${px:.2f} (${sym_pnl:+.2f})")
        if self.trades:
            today_trades = [t for t in self.trades if t['time'].date() == datetime.now(_ET).date()]
            lines.append(f"  📝 Trades today: {len(today_trades)}")
            total_realized = sum(t['pnl'] for t in today_trades if t['side'] == 'SELL')
            lines.append(f"  💰 Realized today: ${total_realized:+,.2f}")

        return "\n".join(lines)


# ──────────────────────────────────────────────────────────
#  MR Virtual Portfolio (Momentum Ride paper simulation)
# ──────────────────────────────────────────────────────────

class MRVirtualPortfolio:
    """Virtual portfolio for Momentum Ride paper simulation.

    Similar to GGVirtualPortfolio — buy/sell driven by strategy signals.
    Uses trailing stop + safety stop from the strategy.
    """

    INITIAL_CASH = MR_LIVE_INITIAL_CASH
    JOURNAL_PATH = DATA_DIR / "mr_virtual_trades.csv"

    def __init__(self):
        self.cash: float = self.INITIAL_CASH
        self.positions: dict[str, dict] = {}
        # sym -> {qty, entry_price, vwap_at_entry, signal_type}
        self.trades: list[dict] = []
        self._init_journal()

    def _init_journal(self):
        if not self.JOURNAL_PATH.exists():
            with open(self.JOURNAL_PATH, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow([
                    'date', 'time_et', 'symbol', 'side', 'reason',
                    'qty', 'price', 'entry_price', 'pnl', 'pnl_pct',
                    'cash_after', 'net_liq', 'positions_open',
                ])

    def _log_journal(self, sym: str, side: str, reason: str,
                     qty: int, price: float, entry_price: float,
                     pnl: float, net_liq_val: float):
        now = datetime.now(_ET)
        pnl_pct = ((price / entry_price - 1) * 100) if entry_price > 0 and side != 'BUY' else 0.0
        try:
            with open(self.JOURNAL_PATH, 'a', newline='') as f:
                w = csv.writer(f)
                w.writerow([
                    now.strftime('%Y-%m-%d'),
                    now.strftime('%H:%M:%S'),
                    sym, side, reason,
                    qty, f'{price:.4f}', f'{entry_price:.4f}',
                    f'{pnl:.2f}', f'{pnl_pct:.1f}',
                    f'{self.cash:.2f}', f'{net_liq_val:.2f}',
                    len(self.positions),
                ])
        except Exception as e:
            log.warning(f"MR journal write error: {e}")

    @staticmethod
    def _ts() -> str:
        return datetime.now(_ET).strftime('%Y-%m-%d %H:%M ET')

    def buy(self, sym: str, qty: int, price: float, vwap: float,
            signal_type: str = '') -> str | None:
        """Open an MR virtual position. Returns Telegram alert text or None."""
        if qty < 1 or sym in self.positions:
            return None
        cost = qty * price
        if cost > self.cash:
            return None

        old_cash = self.cash
        self.cash -= cost

        self.positions[sym] = {
            'qty': qty,
            'entry_price': price,
            'vwap_at_entry': vwap,
            'signal_type': signal_type,
        }

        self.trades.append({
            'sym': sym, 'side': 'BUY', 'qty': qty,
            'price': price, 'pnl': 0, 'time': datetime.now(_ET),
        })

        net = self.cash + cost
        self._log_journal(sym, 'BUY', f'ENTRY ({signal_type})', qty, price, price, 0, net)

        msg = (
            f"📈 <b>Momentum Ride [MR-SIM] — BUY {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  💰 {qty}sh @ ${price:.4f} | VWAP: ${vwap:.4f}\n"
            f"  📋 Signal: {signal_type}\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f}"
        )
        log.info(f"MR VIRTUAL BUY: {sym} {qty}sh @ ${price:.4f} vwap=${vwap:.4f} ({signal_type})")
        return msg

    def sell(self, sym: str, price: float, reason: str) -> str | None:
        """Sell entire MR position. Returns Telegram alert text or None."""
        pos = self.positions.get(sym)
        if not pos:
            return None

        qty = pos['qty']
        entry_price = pos['entry_price']
        pnl = (price - entry_price) * qty
        pnl_pct = (price / entry_price - 1) * 100 if entry_price > 0 else 0

        old_cash = self.cash
        self.cash += qty * price

        self.trades.append({
            'sym': sym, 'side': 'SELL', 'qty': qty,
            'price': price, 'pnl': pnl, 'time': datetime.now(_ET),
        })

        del self.positions[sym]

        net = self.net_liq({})
        self._log_journal(sym, 'SELL', reason, qty, price, entry_price, pnl, net)

        pnl_icon = "🟢" if pnl >= 0 else "🔴"
        msg = (
            f"📈 <b>Momentum Ride [MR-SIM] — SELL {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  {pnl_icon} {qty}sh @ ${price:.4f} (entry ${entry_price:.4f})\n"
            f"  📈 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f}\n"
            f"  📝 Reason: {reason}"
        )
        log.info(f"MR VIRTUAL SELL: {sym} {qty}sh @ ${price:.4f} P&L=${pnl:+.2f} ({reason})")
        return msg

    def net_liq(self, current_prices: dict) -> float:
        val = self.cash
        for sym, pos in self.positions.items():
            px = current_prices.get(sym, pos['entry_price'])
            val += pos['qty'] * px
        return val

    def summary_text(self, current_prices: dict) -> str:
        nlv = self.net_liq(current_prices)
        total_pnl = nlv - self.INITIAL_CASH
        pnl_pct = (nlv / self.INITIAL_CASH - 1) * 100

        lines = [
            f"📈 <b>Momentum Ride [MR-SIM] Portfolio</b>",
            f"  💵 Cash: ${self.cash:,.0f}",
            f"  📊 Net Liq: ${nlv:,.0f} ({pnl_pct:+.1f}%)",
        ]
        if self.positions:
            lines.append(f"  📋 Open positions: {len(self.positions)}")
            for sym, pos in self.positions.items():
                px = current_prices.get(sym, pos['entry_price'])
                sym_pnl = (px - pos['entry_price']) * pos['qty']
                lines.append(f"    • {sym}: {pos['qty']}sh @ ${pos['entry_price']:.2f} → ${px:.2f} (${sym_pnl:+.2f})")
        if self.trades:
            today_trades = [t for t in self.trades if t['time'].date() == datetime.now(_ET).date()]
            lines.append(f"  📝 Trades today: {len(today_trades)}")
            total_realized = sum(t['pnl'] for t in today_trades if t['side'] == 'SELL')
            lines.append(f"  💰 Realized today: ${total_realized:+,.2f}")

        return "\n".join(lines)


# ══════════════════════════════════════════════════════════
#  Scanner Thread (replaces MonitorThread)
# ══════════════════════════════════════════════════════════

class ScannerThread(threading.Thread):
    def __init__(self, freq: int, price_min: float, price_max: float,
                 on_status=None, on_stocks=None, on_alert=None):
        super().__init__(daemon=True)
        self.freq = freq
        self.price_min = price_min
        self.price_max = price_max
        self.on_status = on_status
        self.on_stocks = on_stocks  # callback(dict) to update GUI table
        self.on_alert = on_alert    # callback(str) for alert messages
        self.running = False
        self.previous: dict = {}
        self._last_seen_in_scan: dict[str, float] = {}  # sym → time.time() when last found by OCR/IBKR
        self.count = 0
        self._warmup = True   # suppress alerts on first cycle
        self._warmup_pending: dict[str, dict] = {}  # sym → stock data, enriched during warmup
        self._reports_sent: set[str] = set()      # stocks that got Telegram report this session
        self._last_session: str = _get_market_session()  # track session transitions
        # ── FIB DT Auto-Strategy ──
        self._fib_dt_strategy = FibDTLiveStrategySync(ib_getter=_get_ibkr)
        self._virtual_portfolio = VirtualPortfolio()
        self._fib_dt_current_sym: str | None = None  # best turnover symbol
        # Cache scanner contracts for FIB DT (symbol -> Contract)
        self._scanner_contracts: dict[str, object] = {}
        self._cached_buying_power: float = 0.0
        self._cached_net_liq: float = 0.0
        self._cached_positions: dict[str, tuple] = {}  # sym → (qty, avg, mkt, pnl)
        # ── Gap and Go Auto-Strategy ──
        self._gg_strategy = GapGoLiveStrategy(ib_getter=_get_ibkr)
        self._gg_portfolio = GGVirtualPortfolio()
        # ── Momentum Ride Auto-Strategy ──
        self._mr_strategy = MomentumRideLiveStrategy(ib_getter=_get_ibkr)
        self._mr_portfolio = MRVirtualPortfolio()
        # ── Telegram stock lookup ──
        self._lookup_queue: queue.Queue = queue.Queue()
        self._telegram_listener: TelegramListenerThread | None = None

    def stop(self):
        self.running = False
        if self._telegram_listener:
            self._telegram_listener.stop()

    def _quick_ocr_price_update(self):
        """Fast OCR-only price refresh between full scan cycles.

        Reads Webull screen via OCR (~1s) and updates only price/pct
        for stocks already in self.previous. No enrichment, no alerts.
        """
        if not self.previous:
            return
        raw = _run_ocr_scan(self.price_min, self.price_max)
        if not raw:
            return
        # Update last-seen timestamps for carry-over freshness
        now_ts = time_mod.time()
        for sym in raw:
            self._last_seen_in_scan[sym] = now_ts
        updated = False
        for sym, d in raw.items():
            if sym not in self.previous:
                continue
            new_price = d['price']
            old_price = self.previous[sym].get('price', 0)
            if abs(old_price - new_price) < 0.001:
                continue
            self.previous[sym]['price'] = new_price
            # Use IBKR prev_close if available for accurate pct
            prev_close = self.previous[sym].get('prev_close', 0)
            if prev_close > 0:
                self.previous[sym]['pct'] = round(
                    (new_price - prev_close) / prev_close * 100, 1)
            else:
                self.previous[sym]['pct'] = d['pct']
            updated = True
        if updated:
            self._refresh_enrichment_fibs(self.previous)
            merged = self._merge_stocks(self.previous)
            if self.on_stocks and merged:
                self.on_stocks(merged)

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
                # Backoff on error to prevent spin loop
                time_mod.sleep(5)
            for _ in range(self.freq):
                if not self.running:
                    break
                self._process_lookup_queue()
                _check_news_updates(self.previous, suppress_send=self._warmup)
                # Quick OCR price refresh (~1s per read)
                t0 = time_mod.time()
                self._quick_ocr_price_update()
                elapsed = time_mod.time() - t0
                if elapsed < 1:
                    time_mod.sleep(1 - elapsed)

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
            contract = _get_contract(sym)
            if not contract or not contract.conId:
                send_telegram_to(chat_id, f"❌ <b>{sym}</b> — סימבול לא נמצא", reply_to=message_id)
                return
            bars = ib.reqHistoricalData(
                contract, endDateTime='', durationStr='2 D',
                barSizeSetting='1 day', whatToShow='TRADES', useRTH=True,
                timeout=15,
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

                # Get float from enrichment cache (optional — no filter)
                enrich = _enrichment.get(sym, {})
                flt_str = enrich.get('float', '-')
                flt_shares = _parse_float_to_shares(flt_str)
                if flt_shares <= 0:
                    flt_shares = 10_000_000  # default if unknown
                if volume_raw <= 0:
                    continue

                turnover = volume_raw / flt_shares
                candidates.append((sym, turnover, d, flt_shares))

            if not candidates:
                return

            # Sort by turnover descending
            candidates.sort(key=lambda x: x[1], reverse=True)

            # Log ranking (top 5)
            top5 = candidates[:5]
            ranking_str = " | ".join(
                f"{s} {t:.2f}x" for s, t, _, _ in top5
            )
            log.info(f"FIB DT candidates ({len(candidates)}): {ranking_str}")

            # ── 2. Build GapSignals for ALL candidates ──
            gap_signals = []
            for sym, turnover, d, flt_shares in candidates:
                gap_signals.append(GapSignal(
                    symbol=sym,
                    contract=d['contract'],
                    gap_pct=d['pct'],
                    prev_close=d['prev_close'],
                    current_price=d['price'],
                    float_shares=flt_shares,
                ))

            best_sym = candidates[0][0]
            if self._fib_dt_current_sym != best_sym:
                best_turnover = candidates[0][1]
                best_data = candidates[0][2]
                log.info(
                    f"FIB DT: top candidate {best_sym} "
                    f"(turnover={best_turnover:.2f}x, "
                    f"+{best_data['pct']:.1f}%, "
                    f"float={_enrichment.get(best_sym, {}).get('float', '?')})"
                )
                self._fib_dt_current_sym = best_sym

            # ── 3. Run strategy cycle on ALL candidates ──
            entry_requests, trailing_exits = self._fib_dt_strategy.process_cycle(
                gap_signals
            )

            # ── 4. Execute entries (virtual portfolio) ──
            for req in entry_requests:
                now_et = datetime.now(_ET).strftime('%Y-%m-%d %H:%M:%S ET')
                log.info(
                    f"FIB DT ENTRY SIGNAL [{now_et}]: {req.symbol} "
                    f"fib=${req.fib_level:.4f} ratio={req.fib_ratio} "
                    f"stop=${req.stop_price:.4f} target=${req.target_price:.4f}"
                )
                _log_daily_event('fib_dt_signal', req.symbol,
                                 f"fib=${req.fib_level:.4f} ratio={req.fib_ratio} "
                                 f"stop=${req.stop_price:.4f} target=${req.target_price:.4f}")

                # Use scan price (same source as stop monitoring) instead of
                # strategy bar_close which can diverge from OCR/enrichment price
                scan_price = current.get(req.symbol, {}).get('price', 0)
                entry_price = scan_price if scan_price > 0 else req.entry_price

                # Adjust stop/target proportionally to scan price
                if scan_price > 0 and scan_price != req.entry_price and req.entry_price > 0:
                    stop_ratio = req.stop_price / req.entry_price
                    adjusted_stop = round(entry_price * stop_ratio, 4)
                    target_ratio = req.target_price / req.entry_price
                    adjusted_target = round(entry_price * target_ratio, 4)
                else:
                    adjusted_stop = req.stop_price
                    adjusted_target = req.target_price

                # Sanity: skip if stop is already above current price
                if entry_price <= adjusted_stop:
                    log.warning(
                        f"FIB DT: Skipping {req.symbol} — scan price ${entry_price:.4f} "
                        f"≤ stop ${adjusted_stop:.4f} (strategy bar_close was ${req.entry_price:.4f})"
                    )
                    continue

                qty = int(self._virtual_portfolio.cash * 0.95 / entry_price) if entry_price > 0 else 0
                if qty >= 2:
                    alert = self._virtual_portfolio.buy(
                        req.symbol, qty, entry_price,
                        adjusted_stop, adjusted_target,
                    )
                    if alert:
                        send_telegram(alert)  # private chat, not group
                        self._fib_dt_strategy.record_entry()
                        self._fib_dt_strategy.mark_in_position(req.symbol)
                        _log_daily_event('fib_dt_entry', req.symbol,
                                         f"[SIM] BUY {qty}sh @ ${entry_price:.2f} "
                                         f"(strategy=${req.entry_price:.2f}, scan=${scan_price:.2f})")
                    else:
                        log.warning(f"FIB DT: Virtual buy failed for {req.symbol} [{now_et}]")
                else:
                    log.warning(f"FIB DT: Insufficient virtual cash for {req.symbol} "
                                f"(cash=${self._virtual_portfolio.cash:.0f}, price=${entry_price:.2f})")

            # ── 5. Execute trailing exits (virtual portfolio) ──
            for exit_sig in trailing_exits:
                now_et = datetime.now(_ET).strftime('%Y-%m-%d %H:%M:%S ET')
                log.info(f"FIB DT TRAILING EXIT [{now_et}]: {exit_sig.symbol} — {exit_sig.reason}")
                pos = self._virtual_portfolio.positions.get(exit_sig.symbol)
                if pos:
                    price = current.get(exit_sig.symbol, {}).get('price', pos['entry_price'])
                    alert = self._virtual_portfolio.trailing_exit(exit_sig.symbol, price, exit_sig.reason)
                    if alert:
                        send_telegram(alert)  # private chat, not group
                    self._fib_dt_strategy.mark_position_closed(exit_sig.symbol)

        except Exception as e:
            log.error(f"FIB DT cycle error: {e}")

    def _monitor_virtual_positions(self, current: dict):
        """Monitor virtual portfolio positions for stop/target hits.

        Uses real-time prices from current scan data.
        """
        if not self._virtual_portfolio.positions:
            return

        # Build current prices from scan data
        current_prices = {}
        for sym, d in current.items():
            px = d.get('price', 0)
            if px > 0:
                current_prices[sym] = px

        # Check stops and targets
        alerts = self._virtual_portfolio.check_stops_and_targets(current_prices)
        for alert in alerts:
            send_telegram(alert)  # private chat, not group

        # Sync strategy state for positions entering TRAILING (once only)
        for sym in list(self._virtual_portfolio.positions.keys()):
            pos = self._virtual_portfolio.positions[sym]
            if pos['phase'] == 'TRAILING' and not pos.get('trailing_synced'):
                self._fib_dt_strategy.mark_trailing(sym)
                pos['trailing_synced'] = True

    def _run_gap_go_cycle(self, current: dict):
        """Run Gap and Go auto-strategy: find gappers >= 15%, check HA + VWAP signals."""
        try:
            # ── 1. Build candidate list (>= 15% gap, has contract) ──
            candidates = []
            for sym, d in current.items():
                pct = d.get('pct', 0)
                price = d.get('price', 0)
                contract = d.get('contract')
                prev_close = d.get('prev_close', 0)

                if pct < GG_LIVE_GAP_MIN_PCT or price <= 0 or not contract:
                    continue
                if prev_close <= 0:
                    continue

                candidates.append(GGCandidate(
                    symbol=sym,
                    contract=contract,
                    gap_pct=pct,
                    prev_close=prev_close,
                    current_price=price,
                ))

            if not candidates:
                return

            # Sort by gap % descending
            candidates.sort(key=lambda c: c.gap_pct, reverse=True)

            log.info(f"GG candidates ({len(candidates)}): "
                     + " | ".join(f"{c.symbol} +{c.gap_pct:.0f}%" for c in candidates[:5]))

            # ── 2. Run strategy cycle ──
            entries, exits = self._gg_strategy.process_cycle(candidates)

            # ── 3. Execute exits first ──
            for exit_sig in exits:
                price = current.get(exit_sig.symbol, {}).get('price', exit_sig.price)
                alert = self._gg_portfolio.sell(exit_sig.symbol, price, exit_sig.reason)
                if alert:
                    send_telegram(alert)
                    self._gg_strategy.mark_position_closed(exit_sig.symbol)
                    _log_daily_event('gg_exit', exit_sig.symbol,
                                     f"[GG-SIM] SELL @ ${price:.4f} ({exit_sig.reason})")

            # ── 4. Execute entries ──
            for entry in entries:
                scan_price = current.get(entry.symbol, {}).get('price', entry.price)
                entry_price = scan_price if scan_price > 0 else entry.price

                qty = int(self._gg_portfolio.cash * GG_LIVE_POSITION_SIZE_PCT / entry_price) if entry_price > 0 else 0
                if qty >= 1:
                    alert = self._gg_portfolio.buy(entry.symbol, qty, entry_price, entry.vwap)
                    if alert:
                        send_telegram(alert)
                        # State already marked in strategy; just log
                        entry_type = "1st" if entry.is_first_entry else "re"
                        _log_daily_event('gg_entry', entry.symbol,
                                         f"[GG-SIM] BUY({entry_type}) {qty}sh @ ${entry_price:.4f} "
                                         f"VWAP=${entry.vwap:.4f}")
                    else:
                        # Buy failed — rollback strategy state
                        self._gg_strategy.mark_position_closed(entry.symbol)
                        log.warning(f"GG: Virtual buy failed for {entry.symbol}")
                else:
                    # Insufficient cash — rollback strategy state
                    self._gg_strategy.mark_position_closed(entry.symbol)
                    log.warning(f"GG: Insufficient virtual cash for {entry.symbol} "
                                f"(cash=${self._gg_portfolio.cash:.0f}, price=${entry_price:.4f})")

        except Exception as e:
            log.error(f"GG cycle error: {e}")

    def _run_momentum_ride_cycle(self, current: dict):
        """Run Momentum Ride auto-strategy: VWAP cross/pullback + SMA9 hourly, 5% trailing stop."""
        try:
            # ── 1. Build candidate list (20-50% gap, has contract) ──
            candidates = []
            for sym, d in current.items():
                pct = d.get('pct', 0)
                price = d.get('price', 0)
                contract = d.get('contract')
                prev_close = d.get('prev_close', 0)

                if pct < MR_GAP_MIN_PCT or pct > MR_GAP_MAX_PCT or price <= 0 or not contract:
                    continue
                if prev_close <= 0:
                    continue

                candidates.append(MRCandidate(
                    symbol=sym,
                    contract=contract,
                    gap_pct=pct,
                    prev_close=prev_close,
                    current_price=price,
                ))

            if not candidates:
                return

            # Sort by gap % descending
            candidates.sort(key=lambda c: c.gap_pct, reverse=True)

            log.info(f"MR candidates ({len(candidates)}): "
                     + " | ".join(f"{c.symbol} +{c.gap_pct:.0f}%" for c in candidates[:5]))

            # ── 2. Run strategy cycle ──
            entries, exits = self._mr_strategy.process_cycle(candidates)

            # ── 3. Execute exits first ──
            for exit_sig in exits:
                price = current.get(exit_sig.symbol, {}).get('price', exit_sig.price)
                alert = self._mr_portfolio.sell(exit_sig.symbol, price, exit_sig.reason)
                if alert:
                    send_telegram(alert)
                    self._mr_strategy.mark_position_closed(exit_sig.symbol)
                    _log_daily_event('mr_exit', exit_sig.symbol,
                                     f"[MR-SIM] SELL @ ${price:.4f} ({exit_sig.reason})")

            # ── 4. Execute entries ──
            for entry in entries:
                scan_price = current.get(entry.symbol, {}).get('price', entry.price)
                entry_price = scan_price if scan_price > 0 else entry.price

                qty = int(self._mr_portfolio.cash * MR_LIVE_POSITION_SIZE_PCT / entry_price) if entry_price > 0 else 0
                if qty >= 1:
                    alert = self._mr_portfolio.buy(
                        entry.symbol, qty, entry_price, entry.vwap,
                        signal_type=entry.signal_type,
                    )
                    if alert:
                        send_telegram(alert)
                        # State already marked in strategy; just log
                        _log_daily_event('mr_entry', entry.symbol,
                                         f"[MR-SIM] BUY({entry.signal_type}) {qty}sh @ ${entry_price:.4f} "
                                         f"VWAP=${entry.vwap:.4f} SMA9h=${entry.sma9_hourly:.4f}")
                    else:
                        # Buy failed — rollback strategy state
                        self._mr_strategy.mark_position_closed(entry.symbol)
                        log.warning(f"MR: Virtual buy failed for {entry.symbol}")
                else:
                    # Insufficient cash — rollback strategy state
                    self._mr_strategy.mark_position_closed(entry.symbol)
                    log.warning(f"MR: Insufficient virtual cash for {entry.symbol} "
                                f"(cash=${self._mr_portfolio.cash:.0f}, price=${entry_price:.4f})")

        except Exception as e:
            log.error(f"MR cycle error: {e}")

    @staticmethod
    def _merge_stocks(current: dict) -> dict:
        """Merge scan data with cached enrichment for GUI display."""
        merged = {}
        for sym, d in current.items():
            merged[sym] = dict(d)
            if sym in _enrichment:
                merged[sym]['enrich'] = _enrichment[sym]
            elif d.get('float'):
                # Webull scan has float data — pass through as minimal enrichment
                merged[sym]['enrich'] = {'float': d['float']}
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
        _check_holiday_alerts()
        _reset_alerts_if_new_day()

        # ── Session transition detection ──
        current_session = _get_market_session()
        if current_session != self._last_session and current_session != 'closed':
            prev_label = {'pre_market': 'Pre-Market', 'market': 'Market',
                          'after_hours': 'After-Hours', 'closed': 'Closed'}
            cur_label = prev_label.get(current_session, current_session)
            old_label = prev_label.get(self._last_session, self._last_session)
            log.info(f"Session transition: {self._last_session} → {current_session}")
            send_telegram(
                f"🔄 <b>Session changed:</b> {old_label} → {cur_label}\n"
                f"  Resetting scanner data..."
            )
            self.previous.clear()
            self._last_seen_in_scan.clear()
            _enrichment.clear()
            _session_stocks.clear()
            self._warmup = True  # suppress alerts on first cycle of new session
            if self.on_status:
                self.on_status(f"Session: {cur_label} — rescanning...")
        self._last_session = current_session

        # ── OCR scan with IBKR fallback ──
        current = _run_ocr_scan(self.price_min, self.price_max)
        scan_source = "OCR"
        if not current:
            current = _run_ibkr_scan(self.price_min, self.price_max)
            scan_source = "IBKR"

        # Track when each stock was last found by a fresh scan
        now_ts = time_mod.time()
        for sym in current:
            self._last_seen_in_scan[sym] = now_ts

        # Keep enriched stocks from previous scan that OCR missed this cycle.
        # OCR is unreliable — symbols flicker due to misreads (I→l, spaces in numbers).
        # Only carry over if stock was seen by a scan within the last 5 minutes;
        # beyond that it has genuinely dropped off the scanner.
        _CARRYOVER_MAX_AGE = 60  # 1 minute
        if scan_source == "OCR" and self.previous:
            kept = 0
            for sym, prev_d in self.previous.items():
                if sym not in current and sym in _enrichment:
                    last_seen = self._last_seen_in_scan.get(sym, 0)
                    if now_ts - last_seen <= _CARRYOVER_MAX_AGE:
                        current[sym] = dict(prev_d)
                        kept += 1
            if kept:
                log.debug(f"Kept {kept} enriched stocks from previous scan (OCR miss, <5min)")

        # IBKR enrichment for momentum stocks from OCR scan
        if scan_source == "OCR" and current:
            momentum_syms = [s for s, d in current.items() if d.get('pct', 0) >= 16.0]
            if momentum_syms:
                if self.on_status:
                    self.on_status(f"Enriching {len(momentum_syms)} momentum stocks via IBKR...")
                _enrich_with_ibkr(current, momentum_syms)

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
        # Only enrich momentum candidates: ≥16% change
        MIN_ENRICH_PCT = 16.0
        new_syms = [sym for sym in current
                    if current[sym].get('pct', 0) >= MIN_ENRICH_PCT
                    and sym not in _enrichment]
        skipped = len(current) - len(new_syms) - sum(1 for s in current if s in _enrichment)
        if skipped > 0:
            log.info(f"Enrichment filter: {len(new_syms)} stocks ≥{MIN_ENRICH_PCT}% (skipped {skipped} below threshold)")

        # ── Enrich stocks (Finviz + Fib) ──
        enriched_count = 0
        for sym in new_syms:
            if sym in _enrichment:
                continue
            d = current[sym]
            if self.on_status:
                self.on_status(f"#{self.count}  Enriching {sym}... ({enriched_count+1}/{len(new_syms)})")
            _enrich_stock(sym, d['price'], on_status=self.on_status)

            # Filter: send Telegram report if above VWAP + has news
            enrich = _enrichment.get(sym, {})
            vwap = d.get('vwap', 0)
            pct = d.get('pct', 0)
            price = d.get('price', 0)
            above_vwap = vwap > 0 and price > vwap
            has_news = bool(enrich.get('news'))

            if above_vwap and has_news and not self._warmup:
                _send_stock_report(sym, d, enrich)
                self._reports_sent.add(sym)
                global _daily_reports_sent
                _daily_reports_sent += 1
                _log_daily_event('stock_report', sym,
                                 f"+{pct:.1f}% float={enrich.get('float', '-')}")
                file_logger.log_alert(ts, {
                    'type': 'new', 'symbol': sym,
                    'price': d['price'], 'pct': d['pct'],
                    'volume': d.get('volume', ''),
                    'msg': f"🆕 {sym}: ${d['price']:.2f} {d['pct']:+.1f}%",
                })
            elif above_vwap and has_news and self._warmup:
                self._warmup_pending[sym] = dict(d)  # save snapshot
                log.info(f"Deferred report (warmup): {sym} pct={pct:+.1f}% vwap=above news=yes")
            else:
                log.info(f"Filtered {sym}: pct={pct:+.1f}% vwap={'above' if above_vwap else 'below'} float={enrich.get('float', '-')} news={'yes' if has_news else 'no'}")

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
            # Show only momentum stocks (≥20%) with enrichment data
            MIN_TELEGRAM_PCT = 20.0
            momentum = [(s, d) for s, d in current.items()
                        if d.get('pct', 0) >= MIN_TELEGRAM_PCT and s in _enrichment]
            momentum.sort(key=lambda x: x[1]['pct'], reverse=True)
            session_label = {'pre_market': 'Pre-Market', 'market': 'Market',
                             'after_hours': 'After-Hours', 'closed': 'Closed'}
            sess = session_label.get(_get_market_session(), 'Unknown')
            total = len(current)
            mom_count = len(momentum)
            summary_lines = [f"📡 <b>Scanner started</b> [{sess}] — {mom_count} momentum / {total} total"]
            for sym, d in momentum[:5]:
                e = _enrichment.get(sym, {})
                flt = e.get('float', '-')
                short = e.get('short', '-')
                news_flag = "📰" if e.get('news') else ""
                summary_lines.append(
                    f"  {sym} ${d['price']:.2f} {d['pct']:+.1f}%  Float:{flt}  Short:{short} {news_flag}"
                )
            if mom_count > 5:
                summary_lines.append(f"  ... +{mom_count-5} more")
            elif mom_count == 0:
                summary_lines.append("  אין מניות מעל 20% כרגע")
            send_telegram("\n".join(summary_lines))

        # ── Real-time alerts (enriched ≥20% stocks) ──
        if not is_baseline and current and not self._warmup:
            batch_alerts: list[str] = []
            batch_syms: list[str] = []

            for sym, d in current.items():
                if sym not in _enrichment:
                    continue
                price = d.get('price', 0)
                pct = d.get('pct', 0)
                prev_d = self.previous.get(sym, {}) if self.previous else {}

                # HOD break
                hod_msg = check_hod_break(sym, d, prev_d)
                if hod_msg:
                    batch_alerts.append(hod_msg)
                    play_alert_sound('hod')
                    _daily_alert_counts['HOD Break'] = _daily_alert_counts.get('HOD Break', 0) + 1
                    if sym not in batch_syms:
                        batch_syms.append(sym)

                # Fib 2nd touch
                fib_msg = check_fib_second_touch(sym, price, pct)
                if fib_msg:
                    batch_alerts.append(fib_msg)
                    play_alert_sound('fib')
                    _daily_alert_counts['FIB Touch'] = _daily_alert_counts.get('FIB Touch', 0) + 1
                    if sym not in batch_syms:
                        batch_syms.append(sym)

                # LOD touch
                day_low = d.get('day_low', 0)
                lod_msg = check_lod_touch(sym, price, day_low, pct)
                if lod_msg:
                    batch_alerts.append(lod_msg)
                    play_alert_sound('lod')
                    _daily_alert_counts['LOD Touch'] = _daily_alert_counts.get('LOD Touch', 0) + 1
                    if sym not in batch_syms:
                        batch_syms.append(sym)

                # VWAP cross
                vwap = d.get('vwap', 0)
                vwap_msg = check_vwap_cross(sym, price, vwap, pct)
                if vwap_msg:
                    batch_alerts.append(vwap_msg)
                    play_alert_sound('vwap')
                    _daily_alert_counts['VWAP Cross'] = _daily_alert_counts.get('VWAP Cross', 0) + 1
                    if sym not in batch_syms:
                        batch_syms.append(sym)

                # Spike 8%+ in 1-3 minutes
                spike_msg = check_spike(sym, price, pct)
                if spike_msg:
                    batch_alerts.append(spike_msg)
                    play_alert_sound('spike')
                    _daily_alert_counts['Spike'] = _daily_alert_counts.get('Spike', 0) + 1
                    if sym not in batch_syms:
                        batch_syms.append(sym)

                # Volume alert — high RVOL + above VWAP (even below 20%)
                rvol = d.get('rvol', 0)
                vol_msg = check_volume_alert(sym, price, vwap, rvol, pct)
                if vol_msg:
                    batch_alerts.append(vol_msg)
                    play_alert_sound('vwap')
                    _daily_alert_counts['Volume'] = _daily_alert_counts.get('Volume', 0) + 1
                    if sym not in batch_syms:
                        batch_syms.append(sym)

            # Send all alerts as one batch
            if batch_alerts:
                if self.on_alert:
                    for ba in batch_alerts:
                        clean = re.sub(r'<[^>]+>', '', ba)[:100]
                        self.on_alert(clean)

                # Append fib levels for each alerted symbol
                for s in batch_syms:
                    sd = current.get(s, {})
                    sp = sd.get('price', 0)
                    if sp > 0:
                        fib_txt = _format_fib_text(s, sp, vwap=sd.get('vwap', 0))
                        if fib_txt:
                            batch_alerts.append(fib_txt.strip())

                keyboard_rows = []
                for s in batch_syms:
                    keyboard_rows.append([
                        {'text': f'📊 {s}', 'callback_data': f'lookup:{s}'},
                        {'text': f'📈 TradingView', 'url': f'https://www.tradingview.com/chart/?symbol={s}'},
                    ])
                btn = {'inline_keyboard': keyboard_rows} if keyboard_rows else None

                if len(batch_alerts) == 1:
                    send_telegram_alert(batch_alerts[0], reply_markup=btn)
                else:
                    now_et = datetime.now(ZoneInfo('US/Eastern')).strftime('%H:%M')
                    header = f"🔔 <b>התראות ({len(batch_alerts)})</b> — {now_et} ET\n━━━━━━━━━━━━━━━━━━\n\n"
                    send_telegram_alert(header + "\n\n".join(batch_alerts), reply_markup=btn)
                status += f"  🔔{len(batch_alerts)}"
            else:
                status += "  ✓"

        # ── Fetch account data (for daily summary) ──
        self._fetch_account_data()

        # ── Monitor virtual portfolio positions for stops/targets ──
        self._monitor_virtual_positions(current)

        # ── FIB DT Auto-Strategy ──
        self._run_fib_dt_cycle(current, status)

        # ── Gap and Go Auto-Strategy ──
        self._run_gap_go_cycle(current)

        # ── Momentum Ride Auto-Strategy ──
        self._run_momentum_ride_cycle(current)

        # ── Refresh fib partition with current prices ──
        self._refresh_enrichment_fibs(current)

        # ── Daily stats + session tracking + summaries ──
        if not self._warmup:
            new_count = len(set(current) - set(self.previous)) if self.previous else 0
            _track_daily_stats(current, new_count=new_count)
            _track_session_stocks(current)
            _check_stocks_in_play(current)
            _check_session_summary()
            # Build current prices for virtual portfolio summary
            vp_prices = {s: d.get('price', 0) for s, d in current.items() if d.get('price', 0) > 0}
            vp_summary = self._virtual_portfolio.summary_text(vp_prices) if self._virtual_portfolio else ''
            gg_summary = self._gg_portfolio.summary_text(vp_prices) if self._gg_portfolio else ''
            mr_summary = self._mr_portfolio.summary_text(vp_prices) if self._mr_portfolio else ''
            _check_daily_summary(
                positions=self._cached_positions,
                net_liq=self._cached_net_liq,
                buying_power=self._cached_buying_power,
                fib_dt_sym=self._fib_dt_current_sym,
                cycle_count=self.count,
                virtual_portfolio_summary=vp_summary,
                gg_portfolio_summary=gg_summary,
                mr_portfolio_summary=mr_summary,
            )

        # ── Final GUI update with all enrichment ──
        merged = self._merge_stocks(current)
        if self.on_stocks and merged:
            self.on_stocks(merged)

        self.previous = current
        if self._warmup:
            # Populate news state without sending, so next cycle won't re-send
            _check_news_updates(current, suppress_send=True)
            self._warmup = False
            log.info("Warmup complete — alerts enabled")
            # Send deferred Telegram reports for stocks enriched during warmup
            for sym, saved_d in list(self._warmup_pending.items()):
                if sym in self._reports_sent:
                    continue
                d = current.get(sym, saved_d)  # prefer fresh data, fallback to saved
                enrich = _enrichment.get(sym, {})
                if d and enrich:
                    _send_stock_report(sym, d, enrich)
                    self._reports_sent.add(sym)
                    _daily_reports_sent += 1
                    log.info(f"Sent deferred report for {sym} (was warmup)")
            self._warmup_pending.clear()
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
        self._filter_20pct: bool = False  # show top 10 by pct (no 20% filter)
        self._selected_symbol_name: str | None = None
        self._cached_net_liq: float = 0.0
        self._cached_buying_power: float = 0.0
        self._cached_positions: dict[str, tuple] = {}  # {sym: (qty, avgCost, mktPrice, pnl)}
        self._row_widgets: dict[str, dict] = {}   # sym → cached label widgets
        self._rendered_order: list[str] = []       # last symbol render order
        self._portfolio_widgets: dict[str, dict] = {}  # sym → cached portfolio widgets
        self._portfolio_order: list[str] = []
        # Separate order thread — starts immediately, independent of scanner
        self._order_thread = OrderThread(
            host=IBKR_HOST, port=IBKR_PORT,
            on_account=self._on_account_data,
            on_order_result=self._on_order_result,
        )
        self._order_thread.start()

        self.root = tk.Tk()
        self.root.title("IBKR Scanner Monitor")
        self.root.geometry("1400x900")
        self.root.attributes('-topmost', True)
        self.root.configure(bg=self.BG, highlightbackground=self.ACCENT,
                            highlightcolor=self.ACCENT, highlightthickness=2)
        self.root.resizable(True, True)

        # Font settings (user-configurable)
        self._table_font_var = tk.StringVar(value="Courier")
        self._table_size_var = tk.IntVar(value=14)
        self._alerts_font_var = tk.StringVar(value="Courier")
        self._alerts_size_var = tk.IntVar(value=12)

        # Header
        hdr = tk.Frame(self.root, bg=self.BG)
        hdr.pack(fill='x', padx=10, pady=(6, 0))
        tk.Label(hdr, text="IBKR SCANNER", font=("Helvetica", 20, "bold"),
                 bg=self.BG, fg=self.ACCENT).pack(side='left')

        # Connection status (inline with header)
        self.conn_var = tk.StringVar(value="Checking...")
        self.conn_label = tk.Label(hdr, textvariable=self.conn_var,
                                   font=("Courier", 13, "bold"), bg=self.BG, fg="#888")
        self.conn_label.pack(side='right')

        tk.Frame(self.root, bg=self.ACCENT, height=1).pack(fill='x', padx=10, pady=3)

        # ── Main content: left (stocks+portfolio) + right (alerts) ──
        content = tk.Frame(self.root, bg=self.BG)
        content.pack(fill='both', expand=True, padx=10, pady=2)

        # Right panel: Alerts (fixed width, packed first for right alignment)
        right_panel = tk.Frame(content, bg="#111122", width=360)
        right_panel.pack(side='right', fill='y', padx=(4, 0))
        right_panel.pack_propagate(False)

        tk.Label(right_panel, text=" ALERTS", font=("Helvetica", 13, "bold"),
                 bg="#111122", fg=self.ACCENT, anchor='w').pack(fill='x', pady=(4, 2))

        self._alerts_text = tk.Text(
            right_panel, bg="#111122", fg="#e0e0e0",
            font=(self._alerts_font_var.get(), self._alerts_size_var.get()),
            wrap='word', state='disabled', bd=0, highlightthickness=0,
            padx=6, pady=4, cursor='arrow',
        )
        self._alerts_text.pack(fill='both', expand=True)
        self._alerts_text.tag_configure('hod', foreground='#ff6600')
        self._alerts_text.tag_configure('fib', foreground='#66cccc')
        self._alerts_text.tag_configure('lod', foreground='#ffcc00')
        self._alerts_text.tag_configure('vwap', foreground='#00d4ff')
        self._alerts_text.tag_configure('spike', foreground='#ff4444')
        self._alerts_text.tag_configure('report', foreground='#00c853')
        self._alerts_text.tag_configure('default', foreground='#e0e0e0')

        # Vertical separator
        tk.Frame(content, bg="#444", width=1).pack(side='right', fill='y', padx=2)

        # Left panel: stocks + portfolio
        left_panel = tk.Frame(content, bg=self.BG)
        left_panel.pack(side='left', fill='both', expand=True)

        # Column headers
        self._hdr_frame = tk.Frame(left_panel, bg=self.BG)
        self._hdr_frame.pack(fill='x')
        self._rebuild_column_headers()

        # Scrollable stock list
        list_frame = tk.Frame(left_panel, bg=self.BG)
        list_frame.pack(fill='both', expand=True, pady=1)

        self.canvas = tk.Canvas(list_frame, bg=self.BG, highlightthickness=0, height=200)
        scrollbar = tk.Scrollbar(list_frame, orient='vertical', command=self.canvas.yview)
        self.stock_frame = tk.Frame(self.canvas, bg=self.BG)

        self.stock_frame.bind('<Configure>',
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox('all')))
        self.canvas.create_window((0, 0), window=self.stock_frame, anchor='nw')
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        # Mousewheel scrolling
        def _on_mousewheel(event):
            self.canvas.yview_scroll(-1 * (event.delta // 120 or (1 if event.num == 4 else -1)), "units")
        self.canvas.bind('<Button-4>', _on_mousewheel)
        self.canvas.bind('<Button-5>', _on_mousewheel)
        self.stock_frame.bind('<Button-4>', _on_mousewheel)
        self.stock_frame.bind('<Button-5>', _on_mousewheel)

        # ── Portfolio Panel ──
        tk.Frame(left_panel, bg="#444", height=1).pack(fill='x', pady=2)
        port_hdr = tk.Frame(left_panel, bg=self.BG)
        port_hdr.pack(fill='x')
        tk.Label(port_hdr, text="Portfolio", font=("Helvetica", 12, "bold"),
                 bg=self.BG, fg="#888").pack(side='left', padx=(0, 10))
        for text, w in [("SYM", 6), ("QTY", 6), ("AVG", 7), ("PRICE", 7), ("P&L", 9), ("P&L%", 6)]:
            tk.Label(port_hdr, text=text, font=("Courier", 12, "bold"),
                     bg=self.BG, fg=self.ACCENT, width=w, anchor='w').pack(side='left')
        self._portfolio_frame = tk.Frame(left_panel, bg=self.BG)
        self._portfolio_frame.pack(fill='x', pady=1)
        tk.Frame(left_panel, bg="#444", height=1).pack(fill='x', pady=2)

        # ── Trading Panel ──
        self._build_trading_panel()

        tk.Frame(self.root, bg="#444", height=1).pack(fill='x', padx=10, pady=2)

        # Settings + Scanner slots in one row
        fs = tk.Frame(self.root, bg=self.BG)
        fs.pack(fill='x', padx=10, pady=1)

        tk.Label(fs, text="Freq:", font=("Helvetica", 11),
                 bg=self.BG, fg="#888").pack(side='left')
        self.freq = tk.IntVar(value=MONITOR_DEFAULT_FREQ)
        tk.Spinbox(fs, from_=5, to=600, increment=5, textvariable=self.freq,
                   width=3, font=("Helvetica", 11), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=(1, 6))

        tk.Label(fs, text="Alert%:", font=("Helvetica", 11),
                 bg=self.BG, fg="#888").pack(side='left')
        self.thresh = tk.DoubleVar(value=MONITOR_DEFAULT_ALERT_PCT)
        tk.Spinbox(fs, from_=1, to=50, increment=1, textvariable=self.thresh,
                   width=3, font=("Helvetica", 11), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=(1, 6))

        tk.Label(fs, text="Min$:", font=("Helvetica", 11),
                 bg=self.BG, fg="#888").pack(side='left')
        self.price_min = tk.DoubleVar(value=MONITOR_PRICE_MIN)
        tk.Spinbox(fs, from_=0.01, to=100, increment=0.5, textvariable=self.price_min,
                   width=4, font=("Helvetica", 11), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat', format="%.2f").pack(side='left', padx=(1, 6))

        tk.Label(fs, text="Max$:", font=("Helvetica", 11),
                 bg=self.BG, fg="#888").pack(side='left')
        self.price_max = tk.DoubleVar(value=MONITOR_PRICE_MAX)
        tk.Spinbox(fs, from_=1, to=500, increment=1, textvariable=self.price_max,
                   width=4, font=("Helvetica", 11), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat', format="%.2f").pack(side='left', padx=(1, 6))

        tk.Label(fs, text="|", font=("Helvetica", 11),
                 bg=self.BG, fg="#444").pack(side='left', padx=4)

        tk.Label(fs, text="Scan:", font=("Helvetica", 11),
                 bg=self.BG, fg="#888").pack(side='left')
        scanner_row = fs  # scanner slots in same row
        self._scanner_slot_labels: list[tk.Label] = []
        self._scanner_slot_vars: list[tk.StringVar] = []
        for i in range(_MAX_SCANNER_SOURCES):
            var = tk.StringVar(value=f"S{i+1}: ——")
            lbl = tk.Label(scanner_row, textvariable=var,
                           font=("Courier", 9), bg=self.BG, fg="#666", width=12, anchor='w')
            lbl.pack(side='left', padx=(4, 0))
            self._scanner_slot_vars.append(var)
            self._scanner_slot_labels.append(lbl)
            tk.Button(
                scanner_row, text="Pick", font=("Helvetica", 9), bg=self.ROW_BG, fg=self.FG,
                command=lambda idx=i: self._pick_scanner(idx), relief='flat', padx=3, pady=0,
            ).pack(side='left', padx=(1, 0))
            tk.Button(
                scanner_row, text="X", font=("Helvetica", 9), bg=self.ROW_BG, fg=self.RED,
                command=lambda idx=i: self._clear_scanner(idx), relief='flat', padx=2, pady=0,
            ).pack(side='left', padx=(0, 4))

        # Filter toggle + Sound controls + Start/Stop row
        ctrl_row = tk.Frame(self.root, bg=self.BG)
        ctrl_row.pack(fill='x', padx=10, pady=(4, 0))
        self._filter_btn = tk.Button(
            ctrl_row, text="TOP10", font=("Helvetica", 11, "bold"),
            bg="#553333", fg="#aaa", relief='flat', padx=6, pady=2,
            command=self._toggle_filter,
        )
        self._filter_btn.pack(side='left', padx=(0, 6))

        # Sound mute toggle
        self._sound_muted = False
        self._sound_btn = tk.Button(
            ctrl_row, text="🔊", font=("Helvetica", 13),
            bg=self.ROW_BG, fg=self.GREEN, relief='flat', padx=4, pady=2,
            command=self._toggle_sound,
        )
        self._sound_btn.pack(side='left', padx=(0, 2))

        # Sound volume slider
        self._sound_vol = tk.IntVar(value=70)
        self._vol_slider = tk.Scale(
            ctrl_row, from_=0, to=100, orient='horizontal',
            variable=self._sound_vol, length=80, showvalue=False,
            bg=self.BG, fg=self.FG, troughcolor=self.ROW_BG,
            highlightthickness=0, sliderlength=12, width=12,
            command=self._on_volume_change,
        )
        self._vol_slider.pack(side='left', padx=(0, 2))
        self._vol_label = tk.Label(ctrl_row, text="70%", font=("Courier", 10),
                                    bg=self.BG, fg="#888", width=4)
        self._vol_label.pack(side='left', padx=(0, 4))

        # Font controls: Table font
        tk.Label(ctrl_row, text="|", font=("Helvetica", 11),
                 bg=self.BG, fg="#444").pack(side='left', padx=2)
        tk.Label(ctrl_row, text="Table:", font=("Helvetica", 10),
                 bg=self.BG, fg="#888").pack(side='left')
        _fonts = ["Courier", "Consolas", "Monospace", "DejaVu Sans Mono", "Ubuntu Mono",
                  "Liberation Mono", "Noto Sans Mono"]
        self._tbl_font_menu = tk.OptionMenu(
            ctrl_row, self._table_font_var, *_fonts,
            command=lambda _: self._apply_table_font())
        self._tbl_font_menu.config(font=("Helvetica", 9), bg=self.ROW_BG, fg=self.FG,
                                    highlightthickness=0, relief='flat', width=8)
        self._tbl_font_menu.pack(side='left', padx=(1, 2))
        tk.Spinbox(ctrl_row, from_=8, to=22, textvariable=self._table_size_var,
                   width=2, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat',
                   command=self._apply_table_font).pack(side='left', padx=(0, 4))

        # Font controls: Alerts font
        tk.Label(ctrl_row, text="Alerts:", font=("Helvetica", 10),
                 bg=self.BG, fg="#888").pack(side='left')
        self._alrt_font_menu = tk.OptionMenu(
            ctrl_row, self._alerts_font_var, *_fonts,
            command=lambda _: self._apply_alerts_font())
        self._alrt_font_menu.config(font=("Helvetica", 9), bg=self.ROW_BG, fg=self.FG,
                                     highlightthickness=0, relief='flat', width=8)
        self._alrt_font_menu.pack(side='left', padx=(1, 2))
        tk.Spinbox(ctrl_row, from_=8, to=22, textvariable=self._alerts_size_var,
                   width=2, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat',
                   command=self._apply_alerts_font).pack(side='left', padx=(0, 4))

        self.btn = tk.Button(ctrl_row, text="START", font=("Helvetica", 16, "bold"),
                             bg=self.GREEN, fg="white", command=self._toggle,
                             relief='flat', activebackground="#00a844")
        self.btn.pack(side='left', fill='x', expand=True, ipady=2)

        # Status
        self.status = tk.StringVar(value="Ready")
        tk.Label(self.root, textvariable=self.status, font=("Courier", 11),
                 bg=self.BG, fg="#888", wraplength=1000, justify='left'
                 ).pack(padx=10, pady=1, anchor='w')

        # (Alerts panel is in right_panel above)

        self._load()
        self.root.after(500, self._check_connection)
        # Auto-start scanner on launch
        self.root.after(1000, self._toggle)

    def _check_connection(self):
        """Check IBKR connection status for both scanner and order thread."""
        try:
            ib = _ibkr
            scan_ok = ib is not None and ib.isConnected()
        except Exception:
            scan_ok = False
        order_ok = self._order_thread.connected
        if scan_ok and order_ok:
            self.conn_var.set("Scan ✓ | Orders ✓")
            self.conn_label.config(fg=self.GREEN)
        elif order_ok:
            self.conn_var.set("Scan ✗ | Orders ✓")
            self.conn_label.config(fg="#ffcc00")
        elif scan_ok:
            self.conn_var.set("Scan ✓ | Orders ✗")
            self.conn_label.config(fg="#ffcc00")
        else:
            self.conn_var.set("Not connected")
            self.conn_label.config(fg=self.RED)
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
        is_hot = turnover >= 10.0  # volume > 10% of float

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

        # VWAP
        vwap = d.get('vwap', 0)
        price = d['price']
        if vwap > 0:
            vwap_text = f"${vwap:.2f}"
            vwap_fg = self.GREEN if price > vwap else self.RED
        else:
            vwap_text = "—"
            vwap_fg = "#555"

        flt = enrich.get('float', '') if enrich else ''
        short = enrich.get('short', '') if enrich else ''

        # News indicator (column "N")
        news_text = "N" if (enrich and enrich.get('news')) else ""

        # News headlines row — most recent headline, BiDi-fixed for Tkinter
        news_headlines = ""
        if enrich and enrich.get('news'):
            n = enrich['news'][0]
            title = n.get('title_he', n.get('title_en', ''))
            if title:
                if len(title) > 90:
                    title = title[:87] + "..."
                try:
                    title = bidi_display(title)
                except Exception:
                    pass
                news_headlines = f"  📰 {title}"

        # Fib text — nearest 3 below + 5 above, smart rounding
        fib_text = ""
        if enrich and (enrich.get('fib_above') or enrich.get('fib_below')):
            def _fp(p):
                return f"${p:.2f}" if p >= 1 else f"${p:.3f}" if p >= 0.1 else f"${p:.4f}"
            parts = []
            below = enrich.get('fib_below', [])[-3:]   # 3 nearest below
            above = enrich.get('fib_above', [])[:5]     # 5 nearest above
            if below:
                parts.append("⬇" + " ".join(_fp(p) for p in below))
            if above:
                parts.append("⬆" + " ".join(_fp(p) for p in above))
            fib_text = "  📐 " + "  |  ".join(parts) if parts else ""

        return {
            'bg': bg, 'sym_text': sym_text, 'sym_fg': sym_fg,
            'price_text': f"${price:.2f}",
            'pct_text': f"{pct:+.1f}%", 'pct_fg': pct_color,
            'vol_text': vol_text, 'vol_fg': vol_color,
            'rvol_text': rvol_text, 'rvol_fg': rvol_color,
            'vwap_text': vwap_text, 'vwap_fg': vwap_fg,
            'float_text': flt, 'short_text': short,
            'news_text': news_text,
            'fib_text': fib_text,
            'news_headlines': news_headlines,
        }

    def _build_stock_row(self, sym: str, rd: dict) -> dict:
        """Create widget row for a stock and return widget refs."""
        ff = self._table_font_var.get()
        fs = self._table_size_var.get()
        font_b = (ff, fs, "bold")
        font_r = (ff, fs)
        _click = lambda e, s=sym: self._select_stock(s)
        _dbl_click = lambda e, s=sym: self._open_tradingview(s)

        row1 = tk.Frame(self.stock_frame, bg=rd['bg'])
        row1.pack(fill='x', pady=0)
        row1.bind('<Button-1>', _click)
        row1.bind('<Double-Button-1>', _dbl_click)

        sym_lbl = tk.Label(row1, text=rd['sym_text'], font=font_b,
                           bg=rd['bg'], fg=rd['sym_fg'], width=8, anchor='w')
        sym_lbl.pack(side='left'); sym_lbl.bind('<Button-1>', _click); sym_lbl.bind('<Double-Button-1>', _dbl_click)

        price_lbl = tk.Label(row1, text=rd['price_text'], font=font_r,
                             bg=rd['bg'], fg=self.FG, width=8, anchor='w')
        price_lbl.pack(side='left'); price_lbl.bind('<Button-1>', _click); price_lbl.bind('<Double-Button-1>', _dbl_click)

        pct_lbl = tk.Label(row1, text=rd['pct_text'], font=font_b,
                           bg=rd['bg'], fg=rd['pct_fg'], width=8, anchor='w')
        pct_lbl.pack(side='left'); pct_lbl.bind('<Button-1>', _click); pct_lbl.bind('<Double-Button-1>', _dbl_click)

        vol_lbl = tk.Label(row1, text=rd['vol_text'], font=font_r,
                           bg=rd['bg'], fg=rd['vol_fg'], width=7, anchor='w')
        vol_lbl.pack(side='left'); vol_lbl.bind('<Button-1>', _click); vol_lbl.bind('<Double-Button-1>', _dbl_click)

        rvol_lbl = tk.Label(row1, text=rd['rvol_text'], font=font_b,
                            bg=rd['bg'], fg=rd['rvol_fg'], width=6, anchor='w')
        rvol_lbl.pack(side='left'); rvol_lbl.bind('<Button-1>', _click); rvol_lbl.bind('<Double-Button-1>', _dbl_click)

        vwap_lbl = tk.Label(row1, text=rd.get('vwap_text', '—'), font=font_r,
                            bg=rd['bg'], fg=rd.get('vwap_fg', '#555'), width=7, anchor='w')
        vwap_lbl.pack(side='left'); vwap_lbl.bind('<Button-1>', _click); vwap_lbl.bind('<Double-Button-1>', _dbl_click)

        float_lbl = tk.Label(row1, text=rd['float_text'], font=font_r,
                             bg=rd['bg'], fg="#cca0ff", width=7, anchor='w')
        float_lbl.pack(side='left'); float_lbl.bind('<Button-1>', _click); float_lbl.bind('<Double-Button-1>', _dbl_click)

        short_lbl = tk.Label(row1, text=rd['short_text'], font=font_r,
                             bg=rd['bg'], fg="#ffaa00", width=6, anchor='w')
        short_lbl.pack(side='left'); short_lbl.bind('<Button-1>', _click); short_lbl.bind('<Double-Button-1>', _dbl_click)

        news_lbl = tk.Label(row1, text=rd.get('news_text', ''), font=font_r,
                            bg=rd['bg'], fg="#ffcc00", width=2, anchor='w')
        news_lbl.pack(side='left'); news_lbl.bind('<Button-1>', _click); news_lbl.bind('<Double-Button-1>', _dbl_click)

        # Chart button
        chart_btn = tk.Button(row1, text="Ch", font=("Helvetica", 9), bg='#333',
                              fg=self.ACCENT, relief='flat', padx=2, pady=0,
                              command=lambda s=sym: self._open_chart_window(s))
        chart_btn.pack(side='left', padx=(2, 0))

        # Fib row
        row2 = tk.Frame(self.stock_frame, bg=rd['bg'])
        row2.pack(fill='x', pady=0)
        fib_lbl = tk.Label(row2, text=rd['fib_text'], font=font_r,
                           bg=rd['bg'], fg="#66cccc", anchor='w')
        fib_lbl.pack(side='left', padx=(12, 0))
        if not rd['fib_text']:
            row2.pack_forget()

        # News headlines row (wraplength prevents overflow into alerts panel)
        row3 = tk.Frame(self.stock_frame, bg=rd['bg'])
        row3.pack(fill='x', pady=0)
        news_hl_lbl = tk.Label(row3, text=rd.get('news_headlines', ''), font=font_r,
                               bg=rd['bg'], fg="#ccaa00", anchor='w', justify='left',
                               wraplength=680)
        news_hl_lbl.pack(side='left', padx=(12, 0))
        if not rd.get('news_headlines'):
            row3.pack_forget()

        return {
            'row1': row1, 'row2': row2, 'row3': row3,
            'sym_lbl': sym_lbl, 'price_lbl': price_lbl, 'pct_lbl': pct_lbl,
            'vol_lbl': vol_lbl, 'rvol_lbl': rvol_lbl,
            'vwap_lbl': vwap_lbl, 'float_lbl': float_lbl, 'short_lbl': short_lbl,
            'news_lbl': news_lbl, 'fib_lbl': fib_lbl, 'news_hl_lbl': news_hl_lbl,
            'chart_btn': chart_btn,
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
        widgets['vwap_lbl'].config(text=rd.get('vwap_text', '—'), fg=rd.get('vwap_fg', '#555'), bg=bg)
        widgets['float_lbl'].config(text=rd['float_text'], bg=bg)
        widgets['short_lbl'].config(text=rd['short_text'], bg=bg)
        widgets['news_lbl'].config(text=rd.get('news_text', ''), bg=bg)
        # Fib row
        if rd['fib_text']:
            widgets['fib_lbl'].config(text=rd['fib_text'], bg=bg)
            widgets['row2'].config(bg=bg)
            widgets['row2'].pack(fill='x', pady=0)
        else:
            widgets['row2'].pack_forget()
        # News headlines row
        if rd.get('news_headlines'):
            widgets['news_hl_lbl'].config(text=rd['news_headlines'], bg=bg)
            widgets['row3'].config(bg=bg)
            widgets['row3'].pack(fill='x', pady=0)
        else:
            widgets['row3'].pack_forget()

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

        all_sorted = sorted(self._stock_data.items(),
                            key=lambda x: x[1]['pct'], reverse=True)
        if self._filter_20pct:
            sorted_stocks = [(s, d) for s, d in all_sorted if d.get('pct', 0) >= 20.0]
        else:
            sorted_stocks = all_sorted[:10]  # top 10 by pct
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

        # Auto-update trade price for the selected (picked) stock
        sel = self._selected_symbol_name
        if sel and sel in self._stock_data:
            new_price = self._stock_data[sel]['price']
            self._trade_price.set(f"{new_price:.2f}")

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

                sym_lbl = tk.Label(row, text=sym, font=("Courier", 12, "bold"),
                                   bg=rd['bg'], fg=self.FG, width=6, anchor='w')
                sym_lbl.pack(side='left'); sym_lbl.bind('<Button-1>', _click)

                qty_lbl = tk.Label(row, text=str(rd['qty']), font=("Courier", 12),
                                   bg=rd['bg'], fg=self.FG, width=6, anchor='w')
                qty_lbl.pack(side='left'); qty_lbl.bind('<Button-1>', _click)

                avg_lbl = tk.Label(row, text=f"${rd['avg']:.2f}", font=("Courier", 12),
                                   bg=rd['bg'], fg="#aaa", width=7, anchor='w')
                avg_lbl.pack(side='left'); avg_lbl.bind('<Button-1>', _click)

                price_lbl = tk.Label(row, text=f"${rd['mkt_price']:.2f}", font=("Courier", 12),
                                     bg=rd['bg'], fg=self.FG, width=7, anchor='w')
                price_lbl.pack(side='left'); price_lbl.bind('<Button-1>', _click)

                pnl_lbl = tk.Label(row, text=f"${rd['pnl']:+,.2f}", font=("Courier", 12, "bold"),
                                   bg=rd['bg'], fg=rd['pnl_fg'], width=9, anchor='w')
                pnl_lbl.pack(side='left'); pnl_lbl.bind('<Button-1>', _click)

                pnl_pct_lbl = tk.Label(row, text=f"{rd['pnl_pct']:+.1f}%", font=("Courier", 12, "bold"),
                                       bg=rd['bg'], fg=rd['pnl_fg'], width=6, anchor='w')
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
        panel.pack(fill='x', padx=10, pady=1)

        # Row 1: Symbol + Price + Position + Account
        row1 = tk.Frame(panel, bg=self.BG)
        row1.pack(fill='x', pady=1)

        tk.Label(row1, text="Sym:", font=("Helvetica", 12),
                 bg=self.BG, fg="#888").pack(side='left')
        self._selected_sym = tk.StringVar(value="---")
        self._sym_entry = tk.Entry(row1, textvariable=self._selected_sym,
                 font=("Courier", 14, "bold"), bg=self.ROW_BG, fg=self.ACCENT,
                 insertbackground=self.ACCENT, width=6, relief='flat')
        self._sym_entry.pack(side='left', padx=(2, 8))
        self._sym_entry.bind('<Return>', self._on_sym_entry)

        tk.Label(row1, text="Price:", font=("Helvetica", 12),
                 bg=self.BG, fg="#888").pack(side='left')
        self._trade_price = tk.StringVar(value="")
        tk.Entry(row1, textvariable=self._trade_price, font=("Courier", 14),
                 bg=self.ROW_BG, fg=self.FG, insertbackground=self.FG,
                 width=8, relief='flat').pack(side='left', padx=(2, 8))

        self._position_var = tk.StringVar(value="Pos: ---")
        tk.Label(row1, textvariable=self._position_var,
                 font=("Courier", 13), bg=self.BG, fg="#cca0ff"
                 ).pack(side='left', padx=(0, 8))

        self._account_var = tk.StringVar(value="Account: ---")
        tk.Label(row1, textvariable=self._account_var,
                 font=("Courier", 13), bg=self.BG, fg="#aaa"
                 ).pack(side='left')

        # Row 2: BUY + SELL buttons
        row2 = tk.Frame(panel, bg=self.BG)
        row2.pack(fill='x', pady=2)

        for pct in [25, 50, 75, 100]:
            tk.Button(row2, text=f"BUY {pct}%", font=("Helvetica", 12, "bold"),
                      bg="#1b5e20", fg="white", activebackground="#2e7d32",
                      relief='flat', padx=6, pady=1,
                      command=lambda p=pct: self._place_order('BUY', p / 100)
                      ).pack(side='left', padx=1)

        tk.Button(row2, text="FIB DT", font=("Helvetica", 12, "bold"),
                  bg="#00838f", fg="white", activebackground="#00acc1",
                  relief='flat', padx=6, pady=1,
                  command=self._place_fib_dt_order).pack(side='left', padx=4)

        for pct in [25, 50, 75, 100]:
            tk.Button(row2, text=f"SELL {pct}%", font=("Helvetica", 12, "bold"),
                      bg="#b71c1c", fg="white", activebackground="#c62828",
                      relief='flat', padx=6, pady=1,
                      command=lambda p=pct: self._place_order('SELL', p / 100)
                      ).pack(side='left', padx=1)

        # Order status
        self._order_status_var = tk.StringVar(value="")
        self._order_status_label = tk.Label(
            panel, textvariable=self._order_status_var,
            font=("Courier", 11), bg=self.BG, fg="#888", anchor='w')
        self._order_status_label.pack(fill='x', pady=1)

    def _open_tradingview(self, sym: str):
        """Open TradingView chart in browser for the given symbol."""
        url = f"https://www.tradingview.com/chart/?symbol={sym}"
        webbrowser.open(url)

    def _open_chart_window(self, sym: str):
        """Open a fullscreen Toplevel window with 2x2 chart grid.

        Charts: StockCharts Daily | Finviz Daily | Finviz Weekly | Cached Fib
        All images resized to identical cell size, filling the entire window.
        """
        win = tk.Toplevel(self.root)
        win.title(f"Charts — {sym}")
        win.configure(bg='#0e1117')
        # Maximize window to fill screen
        win.attributes('-zoomed', True)
        win.update_idletasks()

        # Title bar with symbol + auto-refresh countdown
        title_bar = tk.Frame(win, bg='#0e1117')
        title_bar.pack(fill='x')
        status_var = tk.StringVar(value=f"Loading charts for {sym}...")
        tk.Label(title_bar, textvariable=status_var, font=("Courier", 11),
                 bg='#0e1117', fg='#888').pack(side='left', padx=8)
        countdown_var = tk.StringVar(value="")
        tk.Label(title_bar, textvariable=countdown_var, font=("Courier", 10),
                 bg='#0e1117', fg='#555').pack(side='right', padx=8)

        chart_frame = tk.Frame(win, bg='#0e1117')
        chart_frame.pack(fill='both', expand=True)
        for r in range(2):
            chart_frame.rowconfigure(r, weight=1, uniform='row')
        for c in range(2):
            chart_frame.columnconfigure(c, weight=1, uniform='col')

        # Keep references to PhotoImages to prevent GC
        win._chart_images = []
        # Auto-refresh timer ID (for cancellation on window close)
        win._refresh_after_id = None
        win._countdown_after_id = None

        self._load_charts(win, sym, chart_frame, status_var)
        self._start_chart_auto_refresh(win, sym, chart_frame, status_var, countdown_var)

        # Cancel timers on window close
        def _on_close():
            if win._refresh_after_id:
                win.after_cancel(win._refresh_after_id)
            if win._countdown_after_id:
                win.after_cancel(win._countdown_after_id)
            win.destroy()
        win.protocol("WM_DELETE_WINDOW", _on_close)

    def _start_chart_auto_refresh(self, win, sym: str, chart_frame: tk.Frame,
                                    status_var: tk.StringVar, countdown_var: tk.StringVar):
        """Schedule auto-refresh every 60 seconds with countdown display."""
        win._chart_countdown = 60  # seconds until next refresh

        def _tick():
            try:
                if not win.winfo_exists():
                    return
            except tk.TclError:
                return
            win._chart_countdown -= 1
            if win._chart_countdown <= 0:
                # Refresh now
                self._refresh_chart_window(win, sym, chart_frame, status_var)
                win._chart_countdown = 60
            countdown_var.set(f"Refresh: {win._chart_countdown}s")
            win._countdown_after_id = win.after(1000, _tick)

        win._countdown_after_id = win.after(1000, _tick)

    def _refresh_chart_window(self, win, sym: str, chart_frame: tk.Frame,
                               status_var: tk.StringVar):
        """Refresh all charts in the chart window."""
        status_var.set(f"Refreshing charts for {sym}...")
        for w in chart_frame.winfo_children():
            w.destroy()
        win._chart_images = []
        self._load_charts(win, sym, chart_frame, status_var)

    def _load_charts(self, win, sym: str, chart_frame: tk.Frame,
                      status_var: tk.StringVar):
        """Fetch and display 4 charts in the grid."""
        def _fetch_url_image(url: str, title: str) -> tuple[Image.Image | None, str]:
            try:
                resp = requests.get(url, timeout=10, headers={
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
                })
                resp.raise_for_status()
                return Image.open(io.BytesIO(resp.content)), title
            except Exception as e:
                log.error(f"Chart fetch {sym} {title}: {e}")
                return None, title

        def _load_fib_from_disk(s: str) -> tuple[Image.Image | None, str]:
            title = "Fibonacci Levels"
            path = Path(f'/tmp/fib_{s}.png')
            if path.exists():
                try:
                    return Image.open(path), title
                except Exception as e:
                    log.error(f"Fib chart load {s}: {e}")
            return None, title

        def _fetch_and_draw():
            urls = [
                (0, 0, "Daily 6M (StockCharts)",
                 f"https://stockcharts.com/c-sc/sc?s={sym}&p=D&yr=0&mn=6&dy=0&id=p75498498580"),
                (0, 1, "Daily (Finviz)",
                 f"https://finviz.com/chart.ashx?t={sym}&ty=c&ta=1&p=d&s=l"),
                (1, 0, "Weekly (Finviz)",
                 f"https://finviz.com/chart.ashx?t={sym}&ty=c&ta=1&p=w&s=l"),
            ]

            results: dict[str, tuple[Image.Image | None, int, int]] = {}

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {}
                for row, col, title, url in urls:
                    fut = pool.submit(_fetch_url_image, url, title)
                    futures[fut] = (row, col, title)
                fib_fut = pool.submit(_load_fib_from_disk, sym)
                futures[fib_fut] = (1, 1, "Fibonacci Levels")

                for fut in as_completed(futures):
                    row, col, title = futures[fut]
                    img, _ = fut.result()
                    results[title] = (img, row, col)

            def _draw():
                # Calculate cell size from actual window dimensions
                win.update_idletasks()
                win_w = chart_frame.winfo_width() or 1300
                win_h = chart_frame.winfo_height() or 800
                cell_w = (win_w // 2) - 4    # 2 columns, minus padding
                cell_h = (win_h // 2) - 4    # 2 rows, minus padding

                for title, (img, row, col) in results.items():
                    cell = tk.Frame(chart_frame, bg='#0e1117')
                    cell.grid(row=row, column=col, sticky='nsew', padx=1, pady=1)
                    if img:
                        # Force resize to exact cell dimensions (fill completely)
                        img = img.resize((cell_w, cell_h), Image.LANCZOS)
                        photo = ImageTk.PhotoImage(img)
                        win._chart_images.append(photo)
                        lbl = tk.Label(cell, image=photo, bg='#0e1117')
                        lbl.pack(fill='both', expand=True)
                    else:
                        tk.Label(cell, text=f"No data: {title}",
                                 font=("Courier", 14), bg='#0e1117', fg='#666').pack(
                                     fill='both', expand=True)
                status_var.set(f"Charts loaded for {sym} — {cell_w}x{cell_h} per chart")

            win.after(0, _draw)

        threading.Thread(target=_fetch_and_draw, daemon=True).start()

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

    def _on_sym_entry(self, _event=None):
        """Handle Enter key in symbol entry — select typed symbol."""
        sym = self._selected_sym.get().strip().upper()
        if sym:
            self._selected_sym.set(sym)
            self._select_stock(sym)

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
        """Validate and send a BUY or SELL order to the OrderThread."""
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

        if not self._order_thread.connected:
            messagebox.showwarning("Not Connected", "Order thread not connected to IBKR.", parent=self.root)
            return

        if action == 'BUY':
            nl = self._cached_net_liq
            if nl <= 0:
                messagebox.showwarning("No Data", "Waiting for account data...", parent=self.root)
                return
            qty = int(nl * 0.95 * pct / price)
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

        # BUY orders: fib-based stop-loss + trailing take-profit
        stop_price = 0.0
        limit_price = 0.0
        trailing_pct = 0.0
        stop_desc = ""       # human-readable for confirmation + Telegram
        if action == 'BUY':
            # Find nearest fib support for smart stop-loss
            with _fib_cache_lock:
                cached = _fib_cache.get(sym)
            if cached:
                _al, _ah, all_levels, _rm, *_ = cached
                supports = [lv for lv in all_levels if lv <= price]
                if supports:
                    nearest_support = supports[-1]
                    stop_price = round(nearest_support * (1 - BRACKET_FIB_STOP_PCT), 2)
                    stop_desc = f"${stop_price:.2f} ({BRACKET_FIB_STOP_PCT*100:.0f}% below fib ${nearest_support:.4f})"
                else:
                    stop_price = round(price * (1 - BRACKET_FIB_STOP_PCT), 2)
                    stop_desc = f"${stop_price:.2f} ({BRACKET_FIB_STOP_PCT*100:.0f}% fallback, no fib support)"
            else:
                stop_price = round(price * (1 - BRACKET_FIB_STOP_PCT), 2)
                stop_desc = f"${stop_price:.2f} ({BRACKET_FIB_STOP_PCT*100:.0f}% fallback, no fib data)"
            limit_price = round(stop_price * (1 - STOP_LIMIT_OFFSET_PCT), 2)
            trailing_pct = BRACKET_TRAILING_PROFIT_PCT

        # Confirmation dialog
        if action == 'BUY':
            confirm = messagebox.askokcancel(
                "Confirm Order",
                f"BUY {qty} {sym} @ ${price:.2f}\n"
                f"Total: ${qty * price:,.2f}\n\n"
                f"🛑 Stop: {stop_desc}\n"
                f"   → Limit ${limit_price:.2f} [STP LMT GTC]\n"
                f"📈 Trailing: {trailing_pct}% trailing stop [GTC]\n\n"
                f"outsideRth=True (pre/post market OK)\n"
                f"TIF: DAY (buy) + GTC (stop/trail)\n"
                f"Continue?",
                parent=self.root,
            )
        else:
            confirm = messagebox.askokcancel(
                "Confirm Order",
                f"SELL {qty} {sym} @ ${price:.2f}\n"
                f"Total: ${qty * price:,.2f}\n\n"
                f"outsideRth=True (pre/post market OK)\n"
                f"Continue?",
                parent=self.root,
            )
        if not confirm:
            return

        req = {'sym': sym, 'action': action, 'qty': qty, 'price': price}
        if action == 'BUY':
            req['stop_price'] = stop_price
            req['limit_price'] = limit_price
            req['trailing_pct'] = trailing_pct
            req['stop_desc'] = stop_desc
        self._order_thread.submit(req)
        if action == 'BUY':
            self._order_status_var.set(
                f"Sending: BUY {qty} {sym} @ ${price:.2f} | Stop {stop_desc} | Trail {trailing_pct}%...")
        else:
            self._order_status_var.set(f"Sending: SELL {qty} {sym} @ ${price:.2f}...")
        self._order_status_label.config(fg="#ffcc00")

    def _place_fib_dt_order(self):
        """Validate and send a Fib Double-Touch split-exit bracket order to OrderThread."""
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

        if not self._order_thread.connected:
            messagebox.showwarning("Not Connected", "Order thread not connected to IBKR.", parent=self.root)
            return

        # Look up fib levels
        with _fib_cache_lock:
            cached = _fib_cache.get(sym)
        if not cached:
            messagebox.showwarning("No Fib Data",
                                   f"No Fibonacci data for {sym}.\nWait for enrichment.",
                                   parent=self.root)
            return

        _anchor_low, _anchor_high, all_levels, _ratio_map, *_ = cached
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
        qty = int(bp * 0.95 / entry_price)
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

        self._order_thread.submit({
            'sym': sym, 'action': 'BUY', 'qty': qty, 'price': entry_price,
            'strategy': 'fib_dt',
            'stop_price': stop_price,
            'target_price': target_price,
            'half': half,
            'other_half': other_half,
        })
        self._order_status_var.set(
            f"Sending: FIB DT {qty} {sym} | stop ${stop_price:.2f} | target ${target_price:.2f}")
        self._order_status_label.config(fg="#ffcc00")

    def _toggle_sound(self):
        """Toggle sound alerts on/off."""
        global _sound_enabled
        self._sound_muted = not self._sound_muted
        _sound_enabled = not self._sound_muted
        if self._sound_muted:
            self._sound_btn.config(text="🔇", fg=self.RED)
        else:
            self._sound_btn.config(text="🔊", fg=self.GREEN)
            # Play a short test sound on unmute
            play_alert_sound('vwap')

    def _on_volume_change(self, val):
        """Handle volume slider change."""
        global _sound_volume
        v = int(float(val))
        _sound_volume = v
        self._vol_label.config(text=f"{v}%")
        if v == 0:
            self._sound_btn.config(text="🔇", fg=self.RED)
        elif not self._sound_muted:
            self._sound_btn.config(text="🔊", fg=self.GREEN)

    def _rebuild_column_headers(self):
        """Rebuild column header labels with current font settings."""
        for w in self._hdr_frame.winfo_children():
            w.destroy()
        ff = self._table_font_var.get()
        fs = self._table_size_var.get()
        for text, w in [("SYM", 8), ("PRICE", 8), ("CHG%", 8), ("VOL", 7),
                        ("RVOL", 6), ("VWAP", 7), ("FLOAT", 7), ("SHORT", 6), ("N", 2)]:
            tk.Label(self._hdr_frame, text=text, font=(ff, fs, "bold"),
                     bg=self.BG, fg=self.ACCENT, width=w, anchor='w').pack(side='left')

    def _apply_table_font(self, _=None):
        """Apply font changes to the stock table."""
        self._rebuild_column_headers()
        # Force full rebuild of stock rows
        self._rendered_order.clear()
        self._render_stock_table()

    def _apply_alerts_font(self, _=None):
        """Apply font changes to the alerts panel."""
        ff = self._alerts_font_var.get()
        fs = self._alerts_size_var.get()
        self._alerts_text.config(font=(ff, fs))

    def _toggle_filter(self):
        """Toggle 20%+ filter on stock table."""
        self._filter_20pct = not self._filter_20pct
        if self._filter_20pct:
            self._filter_btn.config(bg="#335533", fg=self.GREEN, text="20%+")
        else:
            self._filter_btn.config(bg="#553333", fg="#aaa", text="TOP10")
        self._render_stock_table()

    def _push_alert(self, msg: str):
        """Add alert message to alerts panel (GUI thread safe)."""
        ts = datetime.now().strftime('%H:%M:%S')
        line = f"[{ts}] {msg}\n"
        # Determine color tag based on alert type
        ml = msg.lower()
        if 'hod' in ml or 'high' in ml:
            tag = 'hod'
        elif 'fib' in ml:
            tag = 'fib'
        elif 'lod' in ml or 'low' in ml:
            tag = 'lod'
        elif 'vwap' in ml:
            tag = 'vwap'
        elif 'spike' in ml:
            tag = 'spike'
        elif 'report' in ml or 'sent' in ml:
            tag = 'report'
        else:
            tag = 'default'
        def _update():
            self._alerts_text.config(state='normal')
            self._alerts_text.insert('1.0', line, tag)
            # Keep max 50 lines
            line_count = int(self._alerts_text.index('end-1c').split('.')[0])
            if line_count > 50:
                self._alerts_text.delete('50.0', 'end')
            self._alerts_text.config(state='disabled')
        self.root.after(0, _update)

    def _pick_scanner(self, idx: int):
        """Let user click on a window to add as scanner source in slot idx."""
        self.status.set(f"Click on the scanner window for slot S{idx+1}...")
        self.root.update()
        def _do_pick():
            try:
                result = subprocess.run(
                    ['xdotool', 'selectwindow'],
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode == 0 and result.stdout.strip():
                    wid = int(result.stdout.strip())
                    name = _get_window_name(wid)
                    with _scanner_sources_lock:
                        # If slot already occupied, remove old one first
                        if idx < len(_scanner_sources):
                            removed = _scanner_sources.pop(idx)
                            log.info(f"Scanner source removed: slot {idx} WID={removed['wid']}")
                        # Pad sources list if needed
                        while len(_scanner_sources) < idx:
                            _scanner_sources.append({'wid': 0, 'name': ''})
                        if idx < len(_scanner_sources):
                            _scanner_sources[idx] = {'wid': wid, 'name': name}
                        else:
                            _scanner_sources.append({'wid': wid, 'name': name})
                            log.info(f"Scanner source added: WID={wid} name={name} (total: {len(_scanner_sources)})")
                    short_name = name[:12] if name else str(wid)
                    self.root.after(0, lambda: self._scanner_slot_vars[idx].set(f"S{idx+1}: {short_name}"))
                    self.root.after(0, lambda: self._scanner_slot_labels[idx].config(fg=self.GREEN))
                    self.root.after(0, lambda: self.status.set(f"Scanner S{idx+1}: WID={wid} ({name})"))
                    self._save()
                else:
                    self.root.after(0, lambda: self.status.set("Window pick cancelled"))
            except Exception as e:
                self.root.after(0, lambda: self.status.set(f"Pick failed: {e}"))
        threading.Thread(target=_do_pick, daemon=True).start()

    def _clear_scanner(self, idx: int):
        """Remove scanner source from slot idx."""
        remove_scanner_source(idx)
        self._scanner_slot_vars[idx].set(f"S{idx+1}: ——")
        self._scanner_slot_labels[idx].config(fg="#666")
        self.status.set(f"Scanner S{idx+1} cleared")
        self._save()
        self._refresh_scanner_slots()

    def _toggle(self):
        if self.scanner and self.scanner.running:
            self.scanner.stop()
            self.scanner = None
            self.btn.config(text="START", bg=self.GREEN)
            self.status.set("Stopped.")
        else:
            self._save()
            self.scanner = ScannerThread(
                freq=self.freq.get(),
                price_min=self.price_min.get(),
                price_max=self.price_max.get(),
                on_status=self._st,
                on_stocks=self._update_stock_table,
                on_alert=self._push_alert,
            )
            global _gui_alert_cb
            _gui_alert_cb = self._push_alert
            self.scanner.start()
            self.btn.config(text="STOP", bg=self.RED)
            self.status.set("Scanner running...")

    def _st(self, msg):
        self.root.after(0, lambda: self.status.set(msg))

    def _refresh_scanner_slots(self):
        """Refresh scanner slot labels from _scanner_sources."""
        with _scanner_sources_lock:
            snapshot = list(_scanner_sources)
        for i in range(_MAX_SCANNER_SOURCES):
            if i < len(snapshot) and snapshot[i].get('wid'):
                src = snapshot[i]
                short_name = src['name'][:12] if src['name'] else str(src['wid'])
                self._scanner_slot_vars[i].set(f"S{i+1}: {short_name}")
                self._scanner_slot_labels[i].config(fg=self.GREEN)
            else:
                self._scanner_slot_vars[i].set(f"S{i+1}: ——")
                self._scanner_slot_labels[i].config(fg="#666")

    def _save(self):
        with _scanner_sources_lock:
            scanner_data = [{'wid': s['wid'], 'name': s['name']} for s in _scanner_sources]
        with open(STATE_PATH, 'w') as f:
            json.dump({
                'freq': self.freq.get(),
                'thresh': self.thresh.get(),
                'price_min': self.price_min.get(),
                'price_max': self.price_max.get(),
                'scanner_sources': scanner_data,
                'table_font': self._table_font_var.get(),
                'table_size': self._table_size_var.get(),
                'alerts_font': self._alerts_font_var.get(),
                'alerts_size': self._alerts_size_var.get(),
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
            # Restore scanner sources
            global _scanner_sources
            saved_sources = s.get('scanner_sources', [])
            # Legacy: migrate from old webull_wid format
            if not saved_sources and s.get('webull_wid'):
                saved_sources = [{'wid': int(s['webull_wid']), 'name': 'Webull'}]
            verified = []
            for src in saved_sources:
                wid = int(src.get('wid', 0))
                if wid and _verify_wid(wid):
                    verified.append({'wid': wid, 'name': src.get('name', '')})
            with _scanner_sources_lock:
                _scanner_sources = verified
            self._refresh_scanner_slots()
            # Restore font settings
            if 'table_font' in s:
                self._table_font_var.set(s['table_font'])
            if 'table_size' in s:
                self._table_size_var.set(int(s['table_size']))
            if 'alerts_font' in s:
                self._alerts_font_var.set(s['alerts_font'])
            if 'alerts_size' in s:
                self._alerts_size_var.set(int(s['alerts_size']))
            # Apply loaded fonts
            self._apply_table_font()
            self._apply_alerts_font()
        except Exception:
            pass

    def run(self):
        self.root.mainloop()
        if self.scanner:
            self.scanner.stop()
        # Stop order thread
        self._order_thread.stop()
        # Graceful IBKR disconnect (scanner connection)
        if _ibkr and _ibkr.isConnected():
            log.info("Disconnecting IBKR...")
            _ibkr.disconnect()


if __name__ == "__main__":
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram not configured — file logging only")
    App().run()
