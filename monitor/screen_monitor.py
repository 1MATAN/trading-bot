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
from strategies.float_turnover_live_strategy import (
    FloatTurnoverLiveStrategy, FTCandidate, FTEntrySignal, FTExitSignal,
)
from monitor.order_thread import OrderThread
from config.settings import (
    FIB_LEVELS_24, FIB_LEVEL_COLORS, IBKR_HOST, IBKR_PORT,
    MONITOR_IBKR_CLIENT_ID, MONITOR_SCAN_CODE, MONITOR_SCAN_CODES, MONITOR_SCAN_MAX_RESULTS,
    MONITOR_PRICE_MIN, MONITOR_PRICE_MAX, MONITOR_DEFAULT_FREQ,
    MONITOR_DEFAULT_ALERT_PCT, ALERT_VWAP_MAX_BELOW_PCT,
    FIB_DT_LIVE_STOP_PCT, FIB_DT_LIVE_TARGET_LEVELS,
    BRACKET_FIB_STOP_PCT, BRACKET_TRAILING_PROFIT_PCT, STOP_LIMIT_OFFSET_PCT,
    STARTING_CAPITAL,
    GG_LIVE_GAP_MIN_PCT, GG_LIVE_INITIAL_CASH, GG_LIVE_POSITION_SIZE_FIXED,
    GG_LIVE_MAX_POSITIONS, GG_MAX_HOLD_MINUTES,
    GG_LIVE_RVOL_MIN, GG_LIVE_REQUIRE_NEWS,
    MR_GAP_MIN_PCT, MR_GAP_MAX_PCT, MR_LIVE_INITIAL_CASH, MR_LIVE_POSITION_SIZE_FIXED,
    MR_LIVE_MAX_POSITIONS, MR_LIVE_RVOL_MIN, MR_LIVE_REQUIRE_NEWS, MR_LIVE_PROFIT_TARGET_PCT,
    FT_LIVE_INITIAL_CASH, FT_LIVE_POSITION_SIZE_FIXED, FT_MIN_FLOAT_TURNOVER_PCT,
    FT_LIVE_MAX_POSITIONS, FT_MAX_HOLD_MINUTES, FT_LIVE_PROFIT_TARGET_PCT,
    FT_LIVE_GAP_MIN_PCT, FT_LIVE_RVOL_MIN, FT_LIVE_REQUIRE_NEWS,
    STRATEGY_MIN_PRICE, FIB_DT_POSITION_SIZE_FIXED, FIB_DT_MAX_POSITIONS,
    FIB_DT_MAX_HOLD_MINUTES,
    FIB_DT_GAP_MIN_PCT, FIB_DT_GAP_MAX_PCT, FIB_DT_RVOL_MIN, FIB_DT_REQUIRE_NEWS,
    STRATEGY_REENTRY_COOLDOWN_SEC, STRATEGY_REENTRY_COOLDOWN_AFTER_LOSS_SEC,
    STRATEGY_MAX_ENTRIES_PER_STOCK_PER_DAY, STRATEGY_DAILY_LOSS_LIMIT,
    FIB_DT_WARMUP_SEC,
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


# Suppress ib_insync "Error 162 ... scanner subscription cancelled" —
# this is expected behavior (reqScannerData auto-cancels) but ib_insync
# logs it as ERROR, flooding the log with ~95 false errors per session.
class _IbInsyncNoiseFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        # Suppress "Error 162 ... subscription cancelled" — expected from reqScannerData
        if 'Error 162' in msg and 'subscription cancelled' in msg:
            return False
        # Suppress updatePortfolio for closed positions (position=0.0) — ghost entries
        if 'updatePortfolio' in msg and 'position=0.0' in msg:
            return False
        # Suppress position callbacks for zero-size positions (old closed trades)
        if msg.startswith('position') and 'position=0.0' in msg:
            return False
        # Suppress "Warning 2104/2106/2158 ... connection is OK" — startup noise
        if 'connection is OK' in msg and ('Warning 210' in msg or 'Warning 2158' in msg):
            return False
        return True


logging.getLogger('ib_insync.wrapper').addFilter(_IbInsyncNoiseFilter())
logging.getLogger('ib_insync.ib').addFilter(_IbInsyncNoiseFilter())


# ══════════════════════════════════════════════════════════
#  IBKR Connection (single synchronous IB instance)
# ══════════════════════════════════════════════════════════

_ibkr: IB | None = None
_ibkr_last_attempt: float = 0.0      # time.time() of last failed connect
_ibkr_fail_count: int = 0            # consecutive failures (for backoff)
_IBKR_BACKOFF_BASE = 5               # initial backoff seconds
_IBKR_BACKOFF_MAX = 120              # max backoff seconds
_IBKR_MAX_RETRIES = 3                # retries per _get_ibkr call

# ── TWS Auto-Restart via IBC ──
_IBC_LAUNCH_SCRIPT = Path.home() / "ibc" / "twsstart.sh"
_TWS_RESTART_COOLDOWN = 300          # minimum seconds between restart attempts
_TWS_API_WAIT_TIMEOUT = 120          # seconds to wait for API port after launch
_TWS_PROCESS_PATTERN = "jts"         # pattern to find TWS Java process
_tws_last_restart: float = 0.0       # time.time() of last restart attempt


def _ensure_tws_running() -> None:
    """Ensure TWS is running at monitor startup. Launch via IBC if not."""
    if _is_tws_running():
        log.info("TWS is already running")
        return
    log.warning("TWS not running — launching before starting scanner...")
    # Bypass cooldown for startup launch
    global _tws_last_restart
    _tws_last_restart = 0.0
    if _restart_tws():
        log.info("TWS launched successfully at monitor startup")
    else:
        log.error("Failed to launch TWS at startup — scanner will retry via auto-restart")


def _is_tws_running() -> bool:
    """Check if TWS Java process is alive."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"java.*{_TWS_PROCESS_PATTERN}"],
            capture_output=True, timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def _wait_for_tws_api(timeout: int = _TWS_API_WAIT_TIMEOUT) -> bool:
    """Wait until TWS API port accepts connections."""
    import socket
    deadline = time_mod.time() + timeout
    while time_mod.time() < deadline:
        try:
            with socket.create_connection((IBKR_HOST, IBKR_PORT), timeout=2):
                return True
        except (ConnectionRefusedError, OSError, socket.timeout):
            time_mod.sleep(3)
    return False


def _restart_tws() -> bool:
    """Launch TWS via IBC (auto-login with credentials from ~/ibc/config.ini).

    IBC handles: username/password entry, 2FA prompt, API port setup.
    Respects a cooldown to avoid spamming restarts.
    """
    global _tws_last_restart

    # Cooldown guard
    elapsed = time_mod.time() - _tws_last_restart
    if elapsed < _TWS_RESTART_COOLDOWN:
        log.info(f"TWS restart cooldown: {_TWS_RESTART_COOLDOWN - elapsed:.0f}s remaining")
        return False

    # Already running?
    if _is_tws_running():
        log.info("TWS process is alive — not restarting")
        return False

    # IBC script exists?
    if not _IBC_LAUNCH_SCRIPT.is_file():
        log.error(f"IBC launch script not found: {_IBC_LAUNCH_SCRIPT}")
        return False

    _tws_last_restart = time_mod.time()
    log.warning("TWS process not found — launching via IBC (auto-login)...")
    send_telegram(
        "🔄 <b>TWS Auto-Restart (IBC)</b>\n"
        "  TWS not detected — launching with auto-login...\n"
        "  ⏳ 2FA approval may be needed on IBKR Mobile\n"
        f"  Waiting up to {_TWS_API_WAIT_TIMEOUT}s for API port {IBKR_PORT}"
    )

    try:
        # Launch IBC's twsstart.sh which handles login automatically
        subprocess.Popen(
            [str(_IBC_LAUNCH_SCRIPT), "-inline"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # detach from our process group
        )
        log.info("IBC launch command sent, waiting for API port...")

        if _wait_for_tws_api():
            log.info(f"TWS API port {IBKR_PORT} is accepting connections!")
            send_telegram(
                f"✅ <b>TWS Restarted Successfully</b>\n"
                f"  API port {IBKR_PORT} is ready\n"
                f"  Reconnecting scanner..."
            )
            return True
        else:
            log.error(f"TWS API port {IBKR_PORT} did not open within {_TWS_API_WAIT_TIMEOUT}s")
            send_telegram(
                f"❌ <b>TWS Restart Failed</b>\n"
                f"  API port {IBKR_PORT} not available after {_TWS_API_WAIT_TIMEOUT}s\n"
                f"  Manual intervention may be required"
            )
            return False
    except Exception as e:
        log.error(f"TWS restart failed: {e}")
        return False

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

    # ── TWS Auto-Restart: if process is dead, try to relaunch ──
    if _ibkr_fail_count >= 2 and not _is_tws_running():
        log.warning("TWS process not found after multiple IBKR failures — auto-restarting")
        if _restart_tws():
            # TWS is back — try connecting one more time
            try:
                _ibkr = IB()
                _ibkr.connect(IBKR_HOST, IBKR_PORT, clientId=MONITOR_IBKR_CLIENT_ID, timeout=15)
                log.info("IBKR connected after TWS auto-restart!")
                _ibkr_fail_count = 0
                return _ibkr
            except Exception as e:
                log.error(f"IBKR connect after TWS restart failed: {e}")
                _ibkr = None

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
                                bars: list) -> tuple[float, float, float | None, float | None, float | None, int | None]:
    """Fetch price, prev_close, and extended-hours high/low/vwap/volume.

    Returns (price, prev_close, ext_high, ext_low, ext_vwap, ext_volume).
    ext_* are None when no extended-hours data is available.
    """
    last_bar = bars[-1]
    price = last_bar.close
    ext_high = None
    ext_low = None
    ext_vwap = None
    ext_volume = None

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
                ext_volume = int(sum(b.volume for b in trade_bars))
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
                ext_volume = int(sum(b.volume for b in trade_bars))
        except Exception:
            pass
    else:
        # Market hours (or closed): normal calculation
        prev_close = bars[-2].close if len(bars) >= 2 else 0.0
        # Fetch total volume including pre-market (RTH daily bar misses pre/after)
        try:
            trade_bars = ib.reqHistoricalData(
                contract, endDateTime='',
                durationStr='1 D', barSizeSetting='1 min',
                whatToShow='TRADES', useRTH=False,
                timeout=15,
            )
            if trade_bars:
                ext_volume = int(sum(b.volume for b in trade_bars))
        except Exception:
            pass

    return price, prev_close, ext_high, ext_low, ext_vwap, ext_volume


def _format_volume(volume: float) -> str:
    """Format volume as human-readable string (e.g. 2.3M, 890K)."""
    if volume >= 1_000_000:
        return f"{volume / 1_000_000:.1f}M"
    elif volume >= 1_000:
        return f"{volume / 1_000:.0f}K"
    return str(int(volume))


def _et_to_israel(date_str: str) -> str:
    """Convert ET date string to Israel time. '2026-02-26 08:22 ET' → '26/02 15:22'."""
    if not date_str:
        return ''
    try:
        from zoneinfo import ZoneInfo
        clean = date_str.replace(' ET', '').strip()
        # Try datetime with time
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
            try:
                et_dt = datetime.strptime(clean, fmt)
                et_dt = et_dt.replace(tzinfo=ZoneInfo('US/Eastern'))
                il_dt = et_dt.astimezone(ZoneInfo('Asia/Jerusalem'))
                return il_dt.strftime('%d/%m %H:%M')
            except ValueError:
                continue
        # Date only — no time conversion needed
        if len(clean) == 10:  # '2026-02-26'
            dt = datetime.strptime(clean, '%Y-%m-%d')
            return dt.strftime('%d/%m')
    except Exception:
        pass
    return date_str


def _format_dollar_short(val: float) -> str:
    """Format dollar value as short Hebrew string: $3.16 מליון, $456 אלף."""
    if val >= 1_000_000:
        return f"${val / 1_000_000:.2f} מליון"
    elif val >= 1_000:
        return f"${val / 1_000:.0f} אלף"
    elif val > 0:
        return f"${val:,.0f}"
    return "$0"


def _format_shares_short(val: float) -> str:
    """Format share count as short Hebrew string: 3.16 מליון, 456 אלף."""
    if val >= 1_000_000:
        return f"{val / 1_000_000:.2f} מליון"
    elif val >= 1_000:
        return f"{val / 1_000:.0f} אלף"
    elif val > 0:
        return f"{val:,.0f}"
    return "0"


# ── SPY daily change cache (for Relative Strength) ──
_spy_cache: dict = {'pct': 0.0, 'price': 0.0, 'ts': 0.0}
_SPY_CACHE_TTL = 300  # refresh every 5 minutes


def _get_spy_daily_change() -> float:
    """Get SPY daily % change (cached 5 min). Returns 0.0 on failure."""
    now = time_mod.time()
    if now - _spy_cache['ts'] < _SPY_CACHE_TTL:
        return _spy_cache['pct']

    ib = _get_ibkr()
    if not ib:
        return _spy_cache['pct']

    try:
        spy = Stock('SPY', 'SMART', 'USD')
        ib.qualifyContracts(spy)
        bars = ib.reqHistoricalData(
            spy, endDateTime="", durationStr="2 D",
            barSizeSetting="1 day", whatToShow="TRADES",
            useRTH=True, timeout=10,
        )
        if bars and len(bars) >= 2:
            prev_close = bars[-2].close
            cur_close = bars[-1].close
            if prev_close > 0:
                pct = round((cur_close - prev_close) / prev_close * 100, 2)
                _spy_cache.update({'pct': pct, 'price': cur_close, 'ts': now})
                return pct
    except Exception as e:
        log.debug(f"SPY daily change fetch failed: {e}")

    return _spy_cache['pct']


# ── Robot helpers: news + SMA9 checks ──
def _stock_has_news(sym: str) -> bool:
    """Check if stock has news from enrichment (Finviz/IBKR)."""
    enrich = _enrichment.get(sym, {})
    return bool(enrich.get('news'))


# SMA9 check cache: {sym: (timestamp, above_both)}
_sma9_cache: dict[str, tuple[float, bool]] = {}
_SMA9_CACHE_TTL = 300  # 5 minutes


def _check_above_sma9(sym: str, price: float, contract) -> bool:
    """Check if price is above SMA9 on both 5-min and 4-hour timeframes.

    Uses IBKR historical data with 5-minute cache per symbol.
    Returns True if can't fetch data (fail-open).
    """
    now = time_mod.time()
    if sym in _sma9_cache:
        ts, above = _sma9_cache[sym]
        if now - ts < _SMA9_CACHE_TTL:
            return above

    ib = _get_ibkr()
    if not ib:
        return True  # fail-open

    try:
        # 5-min SMA9
        bars_5m = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='1 D',
            barSizeSetting='5 mins', whatToShow='TRADES',
            useRTH=False, timeout=10,
        )
        if not bars_5m or len(bars_5m) < 9:
            _sma9_cache[sym] = (now, True)
            return True
        sma9_5m = sum(b.close for b in bars_5m[-9:]) / 9

        # 4-hour SMA9
        bars_4h = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='5 D',
            barSizeSetting='4 hours', whatToShow='TRADES',
            useRTH=False, timeout=10,
        )
        if not bars_4h or len(bars_4h) < 9:
            _sma9_cache[sym] = (now, True)
            return True
        sma9_4h = sum(b.close for b in bars_4h[-9:]) / 9

        above = price > sma9_5m and price > sma9_4h
        if not above:
            log.debug(f"SMA9 filter: {sym} ${price:.4f} — "
                      f"SMA9(5m)=${sma9_5m:.4f} SMA9(4h)=${sma9_4h:.4f}")
        _sma9_cache[sym] = (now, above)
        return above
    except Exception as e:
        log.debug(f"SMA9 check failed for {sym}: {e}")
        _sma9_cache[sym] = (now, True)
        return True


# Alert filter: dollar volume >= $60K OR RVOL >= 3.0
_MIN_DOLLAR_VOL_ALERT = 60_000
_MIN_RVOL_ALERT = 3.0


def _build_smart_line(sym: str, d: dict) -> str:
    """Build smart context line: RVOL + Dollar Volume + RS + Float Turnover.

    Returns formatted Hebrew context string for Telegram alerts.
    """
    price = d.get('price', 0)
    vol_raw = d.get('volume_raw', 0)
    rvol = d.get('rvol', 0)
    pct = d.get('pct', 0)

    parts = []

    # Volume — shares + dollar value
    dol_vol = vol_raw * price if vol_raw and price else 0
    if dol_vol > 0:
        parts.append(f"💵 ווליום {_format_shares_short(vol_raw)} ({_format_dollar_short(dol_vol)})")

    # RVOL — Hebrew
    if rvol >= 1.5:
        parts.append(f"🔥 פי {rvol:.1f} מהרגיל")
    elif rvol > 0:
        parts.append(f"📊 פי {rvol:.1f} מהרגיל")

    # Relative Strength vs SPY
    spy_pct = _get_spy_daily_change()
    if spy_pct < -0.3 and pct > 0 and rvol >= 1.5:
        parts.append(f"💪 חזקה מהשוק (SPY {spy_pct:+.1f}%)")

    # Float Turnover — shares + dollar value
    enrich = _enrichment.get(sym, {})
    float_str = enrich.get('float', '-')
    float_shares = _parse_float_to_shares(float_str)
    if float_shares > 0 and vol_raw > 0:
        turnover = vol_raw / float_shares
        float_dol = float_shares * price
        flt_display = f"מניות פניות {_format_shares_short(float_shares)} ({_format_dollar_short(float_dol)})"
        if turnover >= 1.0:
            parts.append(f"🚀 סיבוב x{turnover:.1f} | {flt_display}")
        elif turnover >= 0.5:
            parts.append(f"⚡ סיבוב {turnover:.0%} | {flt_display}")
        elif turnover >= 0.2:
            parts.append(f"📈 סיבוב {turnover:.0%} | {flt_display}")

    return " | ".join(parts)


# ══════════════════════════════════════════════════════════
#  IBKR Scanner — replaces screenshot + OCR + parse
# ══════════════════════════════════════════════════════════

# Cache avg volume per symbol so repeat scans only need 2D of data
_avg_vol_cache: dict[str, float] = {}


def _run_ibkr_scanner_list(price_min: float = MONITOR_PRICE_MIN,
                           price_max: float = MONITOR_PRICE_MAX) -> list[tuple[str, object]]:
    """Fast IBKR scanner: only TOP_PERC_GAIN for relevant penny stock gainers.

    Uses only TOP_PERC_GAIN (not HOT_BY_VOLUME/MOST_ACTIVE which add noise).
    Returns list of (symbol, contract) tuples.
    """
    ib = _get_ibkr()
    if not ib:
        return []

    valid_items: list[tuple[str, object]] = []
    seen_syms: set[str] = set()

    # Only TOP_PERC_GAIN for the fast list — other scan codes add irrelevant stocks
    sub = ScannerSubscription(
        instrument="STK",
        locationCode="STK.US.MAJOR",
        scanCode="TOP_PERC_GAIN",
        numberOfRows=MONITOR_SCAN_MAX_RESULTS,
        abovePrice=price_min,
        belowPrice=price_max,
    )
    try:
        results = ib.reqScannerData(sub)
    except Exception as e:
        log.error(f"reqScannerData (TOP_PERC_GAIN) failed: {e}")
        return []

    for item in results:
        contract = item.contractDetails.contract
        sym = contract.symbol
        if not sym or not sym.isalpha() or len(sym) > 5:
            continue
        # Skip rights (R), warrants (W), units (U) — 5-char symbols ending with suffix
        if len(sym) == 5 and sym[-1] in ('R', 'W', 'U'):
            continue
        if sym not in seen_syms:
            seen_syms.add(sym)
            valid_items.append((sym, contract))
            if contract.conId and sym not in _contract_cache:
                _contract_cache[sym] = contract
    log.debug(f"Scan [TOP_PERC_GAIN]: {len(results)} raw → {len(valid_items)} unique")

    return valid_items


# ══════════════════════════════════════════════════════════
#  Webull Desktop OCR Scanner
# ══════════════════════════════════════════════════════════

# Multi-scanner sources: up to 4 user-picked windows for OCR scanning
_scanner_sources: list[dict] = []  # [{'wid': int, 'name': str}, ...] max 4
_scanner_sources_lock = threading.Lock()
_MAX_SCANNER_SOURCES = 4
_ocr_capture_lock = threading.Lock()  # prevent simultaneous OCR captures


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


def _get_window_full_name(wid: int) -> str:
    """Get full window title (for matching on restart)."""
    try:
        result = subprocess.run(
            ['xdotool', 'getwindowname', str(wid)],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return ""


def _find_window_by_title(title: str) -> int | None:
    """Find a window by its title. Returns first matching WID or None."""
    if not title:
        return None
    try:
        result = subprocess.run(
            ['xdotool', 'search', '--name', title],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            # xdotool returns one WID per line; pick the first
            first_wid = result.stdout.strip().split('\n')[0]
            return int(first_wid)
    except Exception:
        pass
    return None


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
    Uses _ocr_capture_lock to prevent simultaneous captures from multiple threads.
    """
    with _ocr_capture_lock:
        try:
            result = subprocess.run(
                ['import', '-silent', '-window', str(wid), 'png:-'],
                capture_output=True, timeout=5,
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


# Generic OCR patterns — two Webull column orders:
#   Format A: SYM PRICE +PCT% VOL FLOAT  (some Webull views)
#   Format B: SYM +PCT% PRICE VOL FLOAT  (Top Gainers / % Change first)
# PRICE is plain digits, PCT always starts with +/- — used to distinguish columns.
# Symbols accept lowercase (Tesseract reads I→l), uppercased in parser.
# IBKR TWS Scanner format: [icons/junk] SYMBOL [@ = + 8] PRICE VOLUME [FLOAT] [RANGE%] CHANGE% ...
_OCR_RE_TWS = re.compile(
    r'([A-Z][A-Z0-9]{1,4})\s+'          # symbol (2-5 uppercase chars)
    r'(?:[@+=»·]|\d(?=\s))?\s*=?\s*'    # optional TWS markers or stray standalone digit
    r'([\d]+\.[\d]+|[\d]+)\s+'           # price
    r'([\d.,]+[KMBkmb])\s+'             # volume with K/M/B suffix
    r'.*?'                                # skip middle columns (float, range%)
    r'([+-][\d]+[.,][\d]+)%',            # change% with +/- prefix
)

# ── Format B: SYM +PCT% PRICE VOL FLOAT (Webull "% Change" column first) ──
_OCR_RE_5COL_B = re.compile(
    r'^([A-Za-z]{1,5})\s+'        # symbol
    r'([+-][\d.,]+)%?\S*\s+'     # pct change (+/- prefix)
    r'([\d.]+)\s+'                # price (plain digits)
    r'([\d.,]+[KMBkmb]?)\s+'     # volume
    r'[^\d]*([\d.,]+[KMBkmb]?)\S*$',  # float
    re.MULTILINE,
)
_OCR_RE_4COL_B = re.compile(
    r'^([A-Za-z]{1,5})\s+'
    r'([+-][\d.,]+)%?\S*\s+'     # pct change
    r'([\d.]+)\s+'                # price
    r'([\d.,]+[KMBkmb]?)\s*$',
    re.MULTILINE,
)
_OCR_RE_3COL_B = re.compile(
    r'^([A-Za-z]{1,5})\s+'
    r'([+-][\d.,]+)%?\S*\s+'     # pct change
    r'([\d.]+)\s*$',              # price
    re.MULTILINE,
)

# ── Format A: SYM PRICE +PCT% VOL FLOAT (Webull price column first) ──
_OCR_RE_5COL = re.compile(
    r'^([A-Za-z]{1,5})\s+'        # symbol (allow lowercase for OCR misreads)
    r'([\d.]+)\s+'                # price (plain digits before +/- pct)
    r'([+-][\d.,]+)%?\S*\s+'     # pct change (+/- prefix, optional %, ignore junk)
    r'([\d.,]+[KMBkmb]?)\s+'     # volume
    r'[^\d]*([\d.,]+[KMBkmb]?)\S*$',  # float (skip OCR junk like dashes before digits)
    re.MULTILINE,
)
_OCR_RE_4COL = re.compile(
    r'^([A-Za-z]{1,5})\s+'
    r'([\d.]+)\s+'                # price
    r'([+-][\d.,]+)%?\S*\s+'     # pct change
    r'([\d.,]+[KMBkmb]?)\s*$',
    re.MULTILINE,
)
_OCR_RE_3COL = re.compile(
    r'^([A-Za-z]{1,5})\s+'
    r'([\d.]+)\s+'                # price
    r'([+-][\d.,]+)%?\S*$',       # pct change
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


def _clean_tws_line(line: str) -> str:
    """Clean IBKR TWS OCR line: strip emoji-junk prefixes that OCR produces from TWS icons.

    TWS row icons get OCR'd as: 'fe}', 'e}', 'ey', 'fey', 'fey}', 'O', 'Q', etc.
    Also strips leading digits (row numbers) and special chars.
    """
    # Strip common OCR-garbage prefixes from TWS icons (repeat to handle nested junk)
    for _ in range(3):
        line = re.sub(r'^[\s]*(?:fe[y}]?[\s}]*|e[y}][\s]*|[Oo0Q]\s+|[a-z]{1,3}[\s}]+|[\d]{1,4}\s+[,;.!?)}\]]*\s*)', '', line)
        # Remove stray single lowercase chars / digits at start
        line = re.sub(r'^[a-z0-9]\s+', '', line)
        # Remove leftover bracket/paren/special junk
        line = re.sub(r'^[(){}\[\]@+=»·&:;,\-]+\s*', '', line)
    # (intentionally not stripping V/W/I/H — they're often real symbol letters)
    # Collapse spaces inside likely symbols: "FLY W" → "FLYW" (uppercase word + space + single uppercase)
    line = re.sub(r'^([A-Z]{2,4})\s([A-Z])\s', r'\1\2 ', line)
    # Remove "HO." or similar OCR-prefix before price
    line = re.sub(r'\s+HO\.\s+', ' ', line)
    return line.strip()


def _parse_ocr_generic(text: str) -> list[dict]:
    """Parse OCR text into structured stock data.

    Tries patterns in priority order:
    0. IBKR TWS format: [junk] SYMBOL [@ = +] PRICE VOLUME ... CHANGE%
    1. 5 columns: SYMBOL PCT% PRICE VOLUME FLOAT  (Webull)
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

    # Try TWS format first (line-by-line, clean prefix junk)
    for raw_line in text.split('\n'):
        cleaned = _clean_tws_line(raw_line)
        if not cleaned:
            continue
        m = _OCR_RE_TWS.search(cleaned)
        if not m:
            continue
        sym = _fix_ocr_sym(m.group(1))
        if sym in seen:
            continue
        try:
            price = float(m.group(2))
            pct = float(m.group(4).replace(',', '.').rstrip('.'))
        except ValueError:
            continue
        vol_raw, vol_display = _parse_ocr_volume(m.group(3))
        results.append({
            'sym': sym, 'price': price, 'pct': pct,
            'volume_raw': vol_raw, 'volume': vol_display,
            'float_raw': 0, 'float_display': '',
        })
        seen.add(sym)

    # ── Format B: SYM +PCT% PRICE VOL FLOAT (Webull "% Change" first) ──
    for m in _OCR_RE_5COL_B.finditer(text):
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

    for m in _OCR_RE_4COL_B.finditer(text):
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

    for m in _OCR_RE_3COL_B.finditer(text):
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

    # ── Format A: SYM PRICE +PCT% VOL FLOAT (Webull price first) ──
    for m in _OCR_RE_5COL.finditer(text):
        sym = _fix_ocr_sym(m.group(1))
        if sym in seen:
            continue
        try:
            price = float(m.group(2))
            pct = float(m.group(3).replace(',', '').rstrip('.'))
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

    for m in _OCR_RE_4COL.finditer(text):
        sym = _fix_ocr_sym(m.group(1))
        if sym in seen:
            continue
        try:
            price = float(m.group(2))
            pct = float(m.group(3).replace(',', '').rstrip('.'))
        except ValueError:
            continue
        vol_raw, vol_display = _parse_ocr_volume(m.group(4))
        results.append({
            'sym': sym, 'price': price, 'pct': pct,
            'volume_raw': vol_raw, 'volume': vol_display,
            'float_raw': 0, 'float_display': '',
        })
        seen.add(sym)

    for m in _OCR_RE_3COL.finditer(text):
        sym = _fix_ocr_sym(m.group(1))
        if sym in seen:
            continue
        try:
            price = float(m.group(2))
            pct = float(m.group(3).replace(',', '').rstrip('.'))
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
    (prefer source with more fields). Returns dict: {sym: {price, pct, volume, ...}}.
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
        log.debug("No scanner sources configured, will fallback to IBKR scan")
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


def _quick_ocr_capture(price_min: float = MONITOR_PRICE_MIN,
                      price_max: float = MONITOR_PRICE_MAX) -> dict:
    """Ultra-fast OCR capture — skips WID verification (already validated).

    Used by the real-time refresh thread for ~1s updates.
    Returns {sym: {'price': float, 'pct': float}} — minimal fields.
    """
    with _scanner_sources_lock:
        snapshot = list(_scanner_sources)
    if not snapshot:
        return {}

    all_parsed: list[dict] = []
    for src in snapshot:
        text = _capture_and_ocr(src['wid'])
        if not text.strip():
            continue
        parsed = _parse_ocr_generic(text)
        all_parsed.extend(parsed)

    if not all_parsed:
        return {}

    stocks: dict[str, dict] = {}
    for p in all_parsed:
        sym = p['sym']
        if sym in stocks:
            continue
        price = p['price']
        if price < price_min or price > price_max:
            continue
        stocks[sym] = {
            'price': round(price, 2),
            'pct': round(p['pct'], 1),
        }
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

            price, prev_close, ext_high, ext_low, ext_vwap, ext_volume = \
                _fetch_extended_hours_price(ib, contract, session, bars)

            # Use extended-hours volume when available (more accurate in pre/after)
            if ext_volume is not None and ext_volume > 0:
                volume = ext_volume

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
            # Fill price/pct for IBKR-only stocks (OCR didn't provide them)
            if d.get('price', 0) <= 0 and price > 0:
                d['price'] = round(price, 4)
            if d.get('pct', 0) == 0 and prev_close > 0 and price > 0:
                d['pct'] = round((price - prev_close) / prev_close * 100, 1)
            d['contract'] = contract
            # Fetch 5-min bars for sparkline (24h including extended hours)
            if sym not in _spark_bars_cache:
                try:
                    spark_bars = ib.reqHistoricalData(
                        contract, endDateTime="", durationStr="1 D",
                        barSizeSetting="5 mins", whatToShow="TRADES",
                        useRTH=False, timeout=10,
                    )
                    if spark_bars:
                        _spark_bars_cache[sym] = [(b.close, b.volume) for b in spark_bars]
                except Exception:
                    pass
            # Seed running alert trackers with IBKR accurate data
            if day_high > _running_high.get(sym, 0):
                _running_high[sym] = day_high
            cur_rl = _running_low.get(sym, 0)
            if day_low > 0 and (cur_rl <= 0 or day_low < cur_rl):
                _running_low[sym] = day_low
            # Always use the higher volume between OCR and IBKR
            ibkr_vol = int(volume)
            ocr_vol = d.get('volume_raw', 0)
            if ibkr_vol >= ocr_vol:
                d['volume'] = _format_volume(ibkr_vol)
                d['volume_raw'] = ibkr_vol
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

    Returns list of {'title_en': str, 'date': str, 'source': str,
                     'article_id': str, 'provider_code': str}.
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
                'article_id': h.articleId or '',
                'provider_code': h.providerCode or '',
            })
        return results
    except Exception as e:
        log.debug(f"IBKR news {symbol}: {e}")
        return []


# ── Article body cache: {article_id: str_text} ──
_article_cache: dict[str, str] = {}
_HTML_TAG_RE = re.compile(r'<[^>]+>')


def _fetch_news_article(provider_code: str, article_id: str) -> str:
    """Fetch full article body from IBKR, strip HTML, translate to Hebrew.

    Returns translated plain text or empty string on failure.
    Cached after first successful fetch.
    """
    if not article_id:
        return ''
    if article_id in _article_cache:
        return _article_cache[article_id]
    ib = _get_ibkr()
    if not ib:
        return ''
    try:
        article = ib.reqNewsArticle(provider_code, article_id)
        if not article or not article.articleText:
            _article_cache[article_id] = ''
            return ''
        # Strip HTML tags
        text = _HTML_TAG_RE.sub('', article.articleText).strip()
        # Collapse multiple blank lines
        text = re.sub(r'\n{3,}', '\n\n', text)
        # Translate to Hebrew
        try:
            text_he = _batch_translate([text[:3000]])[0].strip()
        except Exception:
            text_he = text[:3000]
        _article_cache[article_id] = text_he
        return text_he
    except Exception as e:
        log.debug(f"reqNewsArticle({provider_code}, {article_id}): {e}")
        _article_cache[article_id] = ''
        return ''


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

        # Append fib + VWAP + MAs (recalculate from cached bars)
        if price > 0:
            fresh_ma = _get_fresh_ma_rows(sym, price)
            fib_txt = _format_fib_text(sym, price, vwap=d.get('vwap', 0), ma_rows=fresh_ma)
            if fib_txt:
                lines.append(fib_txt.strip())

        if not suppress_send:
            btn = _make_lookup_button(sym)
            send_telegram_alert("\n".join(lines), reply_markup=btn)
            # Push to GUI ALERTS panel
            # GUI alert disabled — Telegram alerts only
            # if _gui_alert_cb:
            #     _gui_alert_cb(f"📰 NEWS — {sym} ({len(new_headlines)} headlines)")
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
    _TF_KEEP = {'1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
                '1h': '1h', '2h': '2h', 'D': 'D', 'W': 'W', 'MO': 'M'}
    ma_all: list[tuple[float, str, str, int]] = []  # (value, tf_short, ma_type, period)
    if ma_rows:
        for r in ma_rows:
            tf_s = _TF_KEEP.get(r['tf'])
            if not tf_s:
                continue
            for ma_type, key in [('S', 'sma'), ('E', 'ema')]:
                val = r.get(key)
                if val and val > 0:
                    ma_all.append((val, tf_s, ma_type, r['period']))

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
    fib_range_lo = below[0] if below else price    # lowest displayed level
    fib_range_hi = above[-1] if above else price  # highest displayed level

    # Bucket MAs into intervals between adjacent anchors (only within fib range)
    # bucket[i] holds MAs that fall between anchors[i] and anchors[i+1]
    _TF_ORDER = {'1m': 0, '5m': 1, '15m': 2, '30m': 3, '1h': 4, '2h': 5, 'D': 6, 'W': 7, 'M': 8}
    buckets: list[list[tuple[str, str, int]]] = [[] for _ in range(len(anchors) - 1)]
    for val, tf_s, ma_type, period in ma_all:
        if val > fib_range_hi or val < fib_range_lo:
            continue
        for i in range(len(anchors) - 1):
            if anchors[i] >= val > anchors[i + 1]:
                buckets[i].append((tf_s, ma_type, period))
                break

    def _compact_ma(items: list[tuple[str, str, int]]) -> str:
        """Format MAs compactly grouped by timeframe: '1m-S9 E50, D-S9 E20'"""
        from collections import OrderedDict
        by_tf: dict[str, list[str]] = OrderedDict()
        for tf_s, ma_type, period in sorted(items, key=lambda x: (_TF_ORDER.get(x[0], 9), x[2])):
            by_tf.setdefault(tf_s, []).append(f"{ma_type}{period}")
        parts = []
        for tf_s, mas in by_tf.items():
            parts.append(f"{tf_s}-{' '.join(mas)}")
        return f"<code>{', '.join(parts)}</code>"

    lines = [f"\n📐 <b>פיבונאצ'י</b> ({sym})"]
    lines.append(f"🕯 עוגן: {_p(anchor_low)} — {_p(anchor_high)}  ({anchor_date})")
    lines.append("")
    for i, a in enumerate(anchors):
        # Build the main line
        if a == price:
            line = f"━━━━ ${price:.2f} ━━━━"
        elif a == vwap and vwap > 0:
            line = _vwap_line(vwap)
        else:
            line = _fmt(a)
        # Append MA bucket on same line
        if i < len(buckets) and buckets[i]:
            line += f" {_compact_ma(buckets[i])}"
        lines.append(line)
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
        dol_vol = vol * price if vol and price else 0
        vol_str = _format_shares_short(vol) if vol > 0 else '-'
        dol_str = _format_dollar_short(dol_vol) if dol_vol > 0 else '-'
        flt = enrich.get('float', '-')
        flt_shares = _parse_float_to_shares(flt)
        flt_sh_str = _format_shares_short(flt_shares) if flt_shares > 0 else flt
        flt_dol = _format_dollar_short(flt_shares * price) if flt_shares > 0 and price > 0 else '-'
        short = enrich.get('short', '-')
        arrow = "🟢" if pct > 0 else "🔴"

        lines.append(
            f"  {i}. {arrow} <b>{sym}</b>  ${price:.2f}  {pct:+.1f}%"
        )
        lines.append(
            f"      מ.פניות:{flt_sh_str} ({flt_dol})  שורט:{short}  ווליום:{vol_str} ({dol_str})  סיבוב:{turnover:.0f}%"
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
_alerts_per_stock: dict[str, dict[str, int]] = {}  # sym → {alert_type → count}
_last_alert_summary_time: float = 0.0
_news_summary_sent: set[str] = set()       # headline strings already included in a 30-min summary
_last_news_summary_time: float = 0.0
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
                         mr_portfolio_summary: str = '',
                         ft_portfolio_summary: str = ''):
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

    # ── Float Turnover Section ──
    ft_entries = [e for e in _daily_events if e['type'] == 'ft_entry']
    ft_exits = [e for e in _daily_events if e['type'] == 'ft_exit']

    lines.append("")
    lines.append("🔄 <b>אסטרטגיית Float Turnover:</b>")
    lines.append(f"  • כניסות: {len(ft_entries)}")
    for e in ft_entries:
        lines.append(f"    {e['time']} — {e['symbol']} {e['detail']}")
    lines.append(f"  • יציאות: {len(ft_exits)}")
    for e in ft_exits:
        lines.append(f"    {e['time']} — {e['symbol']} {e['detail']}")

    if ft_portfolio_summary:
        lines.append("")
        lines.append(ft_portfolio_summary)

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
    _alerts_per_stock.clear()
    _daily_reports_sent = 0


# ══════════════════════════════════════════════════════════
#  Weekly Report & Reset
# ══════════════════════════════════════════════════════════

_WEEKLY_STATE_PATH = DATA_DIR / "weekly_reset_state.json"
_weekly_report_sent: str = ''   # week key, e.g. '2026-W08'
_weekly_reset_done: str = ''    # week key

# Load persisted weekly guards on module init
try:
    if _WEEKLY_STATE_PATH.exists():
        with open(_WEEKLY_STATE_PATH) as _f:
            _ws = json.load(_f)
            _weekly_report_sent = _ws.get('report_sent', '')
            _weekly_reset_done = _ws.get('reset_done', '')
        del _ws, _f
except Exception:
    pass


def _save_weekly_state():
    """Persist weekly report/reset guards to disk."""
    try:
        with open(_WEEKLY_STATE_PATH, 'w') as f:
            json.dump({
                'report_sent': _weekly_report_sent,
                'reset_done': _weekly_reset_done,
            }, f, indent=2)
    except Exception as e:
        log.warning(f"Weekly state save error: {e}")


def _week_key() -> str:
    """Return ISO week key like '2026-W08' for the current week."""
    now = datetime.now(_ET)
    return f"{now.year}-W{now.isocalendar()[1]:02d}"


def _build_weekly_report(fib_vp, gg_vp, mr_vp, ft_vp, current_prices: dict) -> str:
    """Build a comprehensive weekly performance report for all 4 portfolios."""
    lines = [
        f"📊 <b>דוח שבועי — שבוע {_week_key()}</b>",
        "━━━━━━━━━━━━━━━━━━",
    ]

    for label, vp, journal_path in [
        ("📐 FIB DT [SIM]", fib_vp, VirtualPortfolio.JOURNAL_PATH),
        ("🚀 Gap&Go [GG-SIM]", gg_vp, GGVirtualPortfolio.JOURNAL_PATH),
        ("📈 Momentum Ride [MR-SIM]", mr_vp, MRVirtualPortfolio.JOURNAL_PATH),
        ("🔄 Float Turnover [FT-SIM]", ft_vp, FTVirtualPortfolio.JOURNAL_PATH),
    ]:
        nlv = vp.net_liq(current_prices)
        total_pnl = nlv - vp.INITIAL_CASH
        pnl_pct = (nlv / vp.INITIAL_CASH - 1) * 100

        # Read journal for trade stats
        wins, losses, total_realized = 0, 0, 0.0
        best_trade = ('', 0.0)
        worst_trade = ('', 0.0)
        total_sells = 0
        try:
            if journal_path.exists():
                df = pd.read_csv(journal_path)
                sells = df[df['side'] == 'SELL'] if 'side' in df.columns else pd.DataFrame()
                total_sells = len(sells)
                if not sells.empty and 'pnl' in sells.columns:
                    pnls = sells['pnl'].astype(float)
                    wins = int((pnls > 0).sum())
                    losses = int((pnls < 0).sum())
                    total_realized = float(pnls.sum())
                    if len(pnls) > 0:
                        best_idx = pnls.idxmax()
                        worst_idx = pnls.idxmin()
                        best_sym = sells.loc[best_idx, 'symbol'] if 'symbol' in sells.columns else '?'
                        worst_sym = sells.loc[worst_idx, 'symbol'] if 'symbol' in sells.columns else '?'
                        best_trade = (best_sym, float(pnls.loc[best_idx]))
                        worst_trade = (worst_sym, float(pnls.loc[worst_idx]))
        except Exception as e:
            log.warning(f"Weekly report CSV read error ({label}): {e}")

        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0

        lines.append("")
        lines.append(f"<b>{label}</b>")
        lines.append(f"  💵 Cash: ${vp.cash:,.0f}")
        lines.append(f"  📊 Net Liq: ${nlv:,.0f} ({pnl_pct:+.1f}%)")
        lines.append(f"  💰 Realized P&L: ${total_realized:+,.2f}")
        lines.append(f"  📝 Sells: {total_sells} | Wins: {wins} | Losses: {losses} | Win Rate: {win_rate:.0f}%")
        if best_trade[1] != 0:
            lines.append(f"  🏆 Best: {best_trade[0]} ${best_trade[1]:+.2f}")
        if worst_trade[1] != 0:
            lines.append(f"  💀 Worst: {worst_trade[0]} ${worst_trade[1]:+.2f}")
        if vp.positions:
            lines.append(f"  📋 Open positions ({len(vp.positions)}):")
            for sym, pos in vp.positions.items():
                px = current_prices.get(sym, pos['entry_price'])
                qty = pos.get('qty', pos.get('other_half', 0))
                sym_pnl = (px - pos['entry_price']) * qty
                lines.append(f"    • {sym}: {qty}sh @ ${pos['entry_price']:.2f} → ${px:.2f} (${sym_pnl:+.2f})")

    # Combined totals
    combined_nlv = sum(vp.net_liq(current_prices) for vp in [fib_vp, gg_vp, mr_vp, ft_vp])
    combined_initial = fib_vp.INITIAL_CASH + gg_vp.INITIAL_CASH + mr_vp.INITIAL_CASH + ft_vp.INITIAL_CASH
    combined_pnl = combined_nlv - combined_initial
    combined_pct = (combined_nlv / combined_initial - 1) * 100

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append(f"<b>סה\"כ משולב (4 אסטרטגיות):</b>")
    lines.append(f"  💰 השקעה: ${combined_initial:,.0f}")
    lines.append(f"  📊 Net Liq: ${combined_nlv:,.0f} ({combined_pct:+.1f}%)")
    lines.append(f"  {'💚' if combined_pnl >= 0 else '❤️'} P&L: ${combined_pnl:+,.2f}")
    lines.append("")
    lines.append("🔄 איפוס שבועי ביום שני 04:00 ET")

    return "\n".join(lines)


def _check_weekly_report(fib_vp, gg_vp, mr_vp, ft_vp, current_prices: dict):
    """Send weekly performance report on Friday 16:03-16:10 ET."""
    global _weekly_report_sent
    now_et = datetime.now(_ET)

    wk = _week_key()
    if wk == _weekly_report_sent:
        return
    # Only on Friday (weekday=4)
    if now_et.weekday() != 4:
        return
    t = now_et.time()
    if not (time(16, 3) <= t <= time(16, 10)):
        return

    _weekly_report_sent = wk
    _save_weekly_state()

    report = _build_weekly_report(fib_vp, gg_vp, mr_vp, ft_vp, current_prices)
    send_telegram(report)
    log.info(f"Weekly report sent for {wk}")


def _check_weekly_reset(fib_vp, gg_vp, mr_vp, ft_vp):
    """Reset all 4 portfolios on Monday 03:30-04:00 ET."""
    global _weekly_reset_done
    now_et = datetime.now(_ET)

    wk = _week_key()
    if wk == _weekly_reset_done:
        return
    # Only on Monday (weekday=0)
    if now_et.weekday() != 0:
        return
    t = now_et.time()
    if not (time(3, 30) <= t <= time(4, 0)):
        return

    _weekly_reset_done = wk
    _save_weekly_state()

    fib_vp.reset()
    gg_vp.reset()
    mr_vp.reset()
    ft_vp.reset()

    send_telegram(
        f"🔄 <b>איפוס שבועי — {wk}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"כל 4 הפורטפוליו אופסו ל-$3,000.\n"
        f"📐 FIB DT | 🚀 Gap&Go | 📈 Momentum Ride | 🔄 Float Turnover\n"
        f"שבוע חדש מתחיל! 🚀"
    )
    log.info(f"Weekly reset done for {wk}")


# ══════════════════════════════════════════════════════════
#  Float Coverage Report — clock-aligned every 16 minutes via Telegram
# ══════════════════════════════════════════════════════════

_last_float_report_time: float = 0.0  # time.time() of last report (debounce)
# Clock-aligned windows: fire when minute is in [target, target+2)
_FLOAT_REPORT_TARGETS = [3, 16, 32, 48]


def _format_money(val: float) -> str:
    """Format dollar amount compactly: $6.8K, $3.6M, etc."""
    if val >= 1_000_000_000:
        return f"${val/1e9:.1f}B"
    if val >= 1_000_000:
        return f"${val/1e6:.1f}M"
    if val >= 1_000:
        return f"${val/1e3:.1f}K"
    return f"${val:,.0f}"


_FORCE_FLOAT_FLAG = DATA_DIR / "_force_float_report.flag"


def _check_float_report(current: dict):
    """Send float coverage report at clock-aligned times or on force flag."""
    global _last_float_report_time

    now = time_mod.time()
    now_et = datetime.now(_ET)
    forced = _FORCE_FLOAT_FLAG.exists()

    if not forced:
        # Debounce: at least 10 minutes since last report
        if now - _last_float_report_time < 600:
            return

        # Clock-aligned: fire when minute is in [target, target+2)
        m = now_et.minute
        if not any(t <= m < t + 2 for t in _FLOAT_REPORT_TARGETS):
            return

    session = _get_market_session()
    if session == 'closed' and not forced:
        return

    # Collect data for all stocks with enrichment data
    rows = []
    for sym, d in current.items():
        price = d.get('price', 0)
        if price <= 0:
            continue

        enrich = _enrichment.get(sym, {})
        if not enrich:
            continue

        float_shares = _parse_float_to_shares(enrich.get('float', '-'))
        if float_shares <= 0:
            continue

        vol_raw = d.get('volume_raw', 0)
        turnover = (vol_raw / float_shares * 100) if vol_raw > 0 else 0

        # No turnover filter — we take top 6 by volume below

        pct = d.get('pct', 0)
        float_cost = float_shares * price
        vol_money = vol_raw * price if vol_raw > 0 else 0

        # News headlines (first 2)
        news_items = enrich.get('news', [])
        news_str = ""
        if news_items:
            headlines = [n.get('title_he', n.get('title_en', ''))[:60] for n in news_items[:2]]
            news_str = " | ".join(headlines)

        rows.append({
            'sym': sym,
            'pct': pct,
            'price': price,
            'vwap': enrich.get('vwap', 0),
            'float_str': enrich.get('float', '-'),
            'float_shares': float_shares,
            'float_cost': float_cost,
            'vol_raw': vol_raw,
            'vol_money': vol_money,
            'turnover': turnover,
            'short': enrich.get('short', '-'),
            'news': news_str,
        })

    if not rows:
        return

    _last_float_report_time = now

    # Delete force flag only after successful send
    if forced:
        try:
            _FORCE_FLOAT_FLAG.unlink()
        except Exception:
            pass

    # Top 6 stocks by float turnover % (most relevant for day trading)
    rows.sort(key=lambda r: r['turnover'], reverse=True)
    rows = rows[:6]

    session_label = {'pre_market': 'Pre-Market', 'market': 'Market',
                     'after_hours': 'After-Hours'}.get(session, session)
    lines = [f"🔥 <b>Momentum Float Report</b> [{session_label} {now_et.strftime('%H:%M')}]"]
    lines.append("")

    for i, r in enumerate(rows, 1):
        vm = _format_money(r['vol_money']) if r['vol_money'] > 0 else '-'

        # VWAP indicator
        vwap = r['vwap']
        if vwap > 0:
            if r['price'] > vwap:
                vwap_icon = "🟢"
                vwap_label = "above"
            else:
                vwap_icon = "🔴"
                vwap_label = "below"
            vwap_str = f"{vwap_icon} VWAP ${vwap:.2f} ({vwap_label})"
        else:
            vwap_str = ""

        # Short squeeze indicator
        short_str = r['short']
        try:
            short_val = float(str(short_str).replace('%', ''))
            if short_val >= 20:
                short_display = f"🩳 <b>Short {short_str}!</b>"
            else:
                short_display = f"Short: {short_str}"
        except (ValueError, TypeError):
            short_display = f"Short: {short_str}"

        # Header line: rank + symbol + price + gap
        lines.append(
            f"<b>{i}. {r['sym']}</b>  ${r['price']:.2f}  "
            f"<b>Gap {r['pct']:+.0f}%</b>  |  "
            f"🔄 <b>{r['turnover']:.0f}%</b> float traded"
        )
        # Details line
        lines.append(
            f"   {vwap_str}  |  Float: {r['float_str']}  |  Vol: {vm}"
        )
        lines.append(f"   {short_display}")
        if r['news']:
            lines.append(f"   📰 {r['news']}")
        lines.append("")

    send_telegram("\n".join(lines))
    log.info(f"Float report sent: {len(rows)} stocks")


# ── Alert Summary every 30 minutes ──────────────────────

_ALERT_SUMMARY_TARGETS = [0, 30]  # fire at :00 and :30


def _check_alert_summary(current: dict):
    """Send a summary of alerts per stock every 30 minutes."""
    global _last_alert_summary_time

    now = time_mod.time()
    # Debounce: at least 25 minutes since last summary
    if now - _last_alert_summary_time < 1500:
        return

    now_et = datetime.now(_ET)
    m = now_et.minute
    if not any(t <= m < t + 2 for t in _ALERT_SUMMARY_TARGETS):
        return

    session = _get_market_session()
    if session == 'closed':
        return

    if not _alerts_per_stock:
        return

    _last_alert_summary_time = now

    # Sort stocks by total alert count descending
    stock_totals = []
    for sym, counts in _alerts_per_stock.items():
        total = sum(counts.values())
        stock_totals.append((sym, total, counts))
    stock_totals.sort(key=lambda x: x[1], reverse=True)

    # Top stock by alert count
    top_sym, top_total, top_counts = stock_totals[0]
    d = current.get(top_sym, {})
    price = d.get('price', 0)
    pct = d.get('pct', 0)
    parts = [f"{k} ×{v}" for k, v in sorted(top_counts.items(), key=lambda x: -x[1])]

    total_all = sum(t[1] for t in stock_totals)
    session_label = {'pre_market': 'Pre-Market', 'market': 'Market',
                     'after_hours': 'After-Hours'}.get(session, session)

    lines = [
        f"🔔 <b>Most Active</b> [{session_label} {now_et.strftime('%H:%M')}]",
        f"<b>{top_sym}</b> ${price:.2f} ({pct:+.0f}%) — <b>{top_total} alerts</b>",
        f"{' | '.join(parts)}",
        f"({total_all} total alerts / {len(stock_totals)} stocks)",
    ]

    send_telegram("\n".join(lines))
    log.info(f"Alert summary sent: {len(stock_totals)} stocks, {total_all} total alerts")


_NEWS_SUMMARY_TARGETS = [0, 30]  # fire at :00 and :30

def _check_news_summary(current: dict):
    """Send a grouped news summary for gapped stocks every 30 minutes."""
    global _last_news_summary_time

    now = time_mod.time()
    # Debounce: at least 25 minutes since last summary
    if now - _last_news_summary_time < 1500:
        return

    now_et = datetime.now(_ET)
    m = now_et.minute
    if not any(t <= m < t + 2 for t in _NEWS_SUMMARY_TARGETS):
        return

    session = _get_market_session()
    if session == 'closed':
        return

    # Collect news from enrichment + _news_sent for stocks ≥ 20%
    stock_news: dict[str, list[str]] = {}  # sym → [headline, ...]
    for sym, d in current.items():
        if d.get('pct', 0) < 20:
            continue

        headlines: list[str] = []

        # From enrichment (Finviz + IBKR news fetched during enrichment)
        enrich = _enrichment.get(sym, {})
        for n in enrich.get('news', []):
            title = n.get('title_he') or n.get('title_en', '')
            if title and title not in _news_summary_sent:
                src = n.get('source', '')
                date = n.get('date', '')
                tag = f" [{src}]" if src else ""
                tag += f" ({date})" if date else ""
                headlines.append(f"  • {title}{tag}")
                _news_summary_sent.add(title)

        # From _news_sent (IBKR news sent individually by _check_news_updates)
        for title in _news_sent.get(sym, set()):
            if title not in _news_summary_sent:
                headlines.append(f"  • {title}")
                _news_summary_sent.add(title)

        if headlines:
            stock_news[sym] = headlines

    if not stock_news:
        return

    _last_news_summary_time = now

    session_label = {'pre_market': 'Pre-Market', 'market': 'Market',
                     'after_hours': 'After-Hours'}.get(session, session)

    lines = [
        f"📰 <b>סיכום חדשות</b> [{session_label} {now_et.strftime('%H:%M')}]",
        "━━━━━━━━━━━━━━━━━━",
    ]

    for sym, headlines in stock_news.items():
        d = current.get(sym, {})
        price = d.get('price', 0)
        pct = d.get('pct', 0)
        lines.append(f"\n📌 <b>{sym}</b> — ${price:.2f} ({pct:+.1f}%)")
        lines.extend(headlines[:5])  # max 5 headlines per stock

    send_telegram_alert("\n".join(lines))
    total = sum(len(h) for h in stock_news.values())
    log.info(f"News summary sent: {len(stock_news)} stocks, {total} headlines")


# ══════════════════════════════════════════════════════════
#  Trade Journal — Telegram entry after each trade close
# ══════════════════════════════════════════════════════════

_IL_TZ = ZoneInfo('Asia/Jerusalem')

# Persistent unified journal — never resets, survives weekly portfolio resets
_JOURNAL_JSON_PATH = DATA_DIR / "trade_journal.json"
_JOURNAL_HTML_PATH = Path.home() / "Desktop" / "יומן.html"
_DEMO_JOURNAL_HTML_PATH = Path.home() / "Desktop" / "דמו.html"

# Robot emoji map for HTML
_ROBOT_EMOJI = {'FIB DT': '📐', 'Gap&Go': '🚀', 'MR': '📈', 'FT': '🔄'}


def _load_journal_trades() -> list[dict]:
    """Load trade list from JSON (persistent data store)."""
    if _JOURNAL_JSON_PATH.exists():
        try:
            return json.loads(_JOURNAL_JSON_PATH.read_text(encoding='utf-8'))
        except Exception:
            return []
    return []


def _save_journal_trades(trades: list[dict]):
    """Save trade list to JSON."""
    _JOURNAL_JSON_PATH.write_text(
        json.dumps(trades, ensure_ascii=False, indent=1), encoding='utf-8')


def _generate_journal_html(trades: list[dict]):
    """Regenerate the HTML journal file on Desktop from trade data.

    Includes per-robot tabs + summary stats + collapsible logic details.
    """
    _TAB_DEFS = [
        ('all', '📒 הכל'),
        ('FIB DT', '📐 FIB DT'),
        ('Gap&Go', '🚀 Gap&Go'),
        ('MR', '📈 MR'),
        ('FT', '🔄 FT'),
    ]

    def _stats_for(tlist):
        pnl = sum(t.get('pnl', 0) for t in tlist)
        w = [t for t in tlist if t.get('pnl', 0) > 0]
        l = [t for t in tlist if t.get('pnl', 0) < 0]
        n = len(tlist)
        wr = (len(w) / n * 100) if n else 0
        aw = (sum(t['pnl'] for t in w) / len(w)) if w else 0
        al = (sum(t['pnl'] for t in l) / len(l)) if l else 0
        return pnl, n, len(w), len(l), wr, aw, al

    def _build_rows(tlist):
        rows = ''
        for i, t in enumerate(reversed(tlist)):
            pv = t.get('pnl', 0)
            pp = t.get('pnl_pct', 0)
            rc = '#4caf50' if pv >= 0 else '#f44336'
            em = _ROBOT_EMOJI.get(t.get('robot', ''), '')
            sh = 'מכירה חלקית' if t.get('side') == 'SELL_HALF' else 'מכירה'
            bg = '#1a1a2e' if i % 2 == 0 else '#16213e'
            logic = t.get('logic', '').replace('\n', '<br>')
            det = ''
            if logic:
                det = (
                    f'<tr class="detail-row" style="background:{bg}">'
                    f'<td colspan="11" style="padding:8px 16px;font-size:13px;'
                    f'color:#aaa;border-top:none">{logic}</td></tr>'
                )
            rows += (
                f'<tr style="background:{bg}">'
                f'<td>{t.get("date","")}</td>'
                f'<td style="font-weight:bold">{t.get("symbol","")}</td>'
                f'<td>{sh}</td>'
                f'<td>{t.get("qty","")}</td>'
                f'<td>${t.get("entry_price",0):.2f}</td>'
                f'<td>${t.get("exit_price",0):.2f}</td>'
                f'<td style="color:{rc};font-weight:bold">${pv:+,.2f}</td>'
                f'<td style="color:{rc}">{pp:+.1f}%</td>'
                f'<td>{t.get("entry_time","")}</td>'
                f'<td>{t.get("exit_time","")}</td>'
                f'<td class="notes">'
                f'<b>כניסה:</b> {t.get("entry_note","")}<br>'
                f'<b>יציאה:</b> {t.get("exit_note","")}</td>'
                f'</tr>\n{det}'
            )
        return rows

    def _build_stats_cards(pnl, n, w, l, wr, aw, al, robot_name=None):
        pc = '#4caf50' if pnl >= 0 else '#f44336'
        cards = (
            f'<div class="stats-bar">'
            f'<div class="stat-card main">'
            f'<div class="stat-label">{"סה\"כ" if not robot_name else robot_name}</div>'
            f'<div class="stat-value" style="color:{pc}">${pnl:+,.2f}</div>'
            f'<div class="stat-sub">{n} עסקאות</div></div>'
            f'<div class="stat-card">'
            f'<div class="stat-label">אחוז הצלחה</div>'
            f'<div class="stat-value">{wr:.0f}%</div>'
            f'<div class="stat-sub">{w}W / {l}L</div></div>'
            f'<div class="stat-card">'
            f'<div class="stat-label">ממוצע רווח</div>'
            f'<div class="stat-value" style="color:#4caf50">${aw:+,.2f}</div></div>'
            f'<div class="stat-card">'
            f'<div class="stat-label">ממוצע הפסד</div>'
            f'<div class="stat-value" style="color:#f44336">${al:+,.2f}</div></div>'
            f'</div>'
        )
        return cards

    # ── Build tab content ──
    tab_buttons = ''
    tab_panels = ''
    for tab_id, tab_label in _TAB_DEFS:
        active = ' active' if tab_id == 'all' else ''
        tab_buttons += (
            f'<button class="tab-btn{active}" '
            f'onclick="switchTab(\'{tab_id}\')">{tab_label}</button>\n'
        )
        tlist = trades if tab_id == 'all' else [t for t in trades if t.get('robot') == tab_id]
        pnl, n, w, l, wr, aw, al = _stats_for(tlist)
        rn = None if tab_id == 'all' else f'{_ROBOT_EMOJI.get(tab_id,"")} {tab_id}'
        stats = _build_stats_cards(pnl, n, w, l, wr, aw, al, rn)

        # For "all" tab, add per-robot mini cards
        if tab_id == 'all':
            for rname in ['FIB DT', 'Gap&Go', 'MR', 'FT']:
                rt = [t for t in trades if t.get('robot') == rname]
                if not rt:
                    continue
                rp = sum(t.get('pnl', 0) for t in rt)
                rw = sum(1 for t in rt if t.get('pnl', 0) > 0)
                rwp = (rw / len(rt) * 100) if rt else 0
                rcol = '#4caf50' if rp >= 0 else '#f44336'
                stats += (
                    f'<div class="stats-bar" style="margin-top:-8px">'
                    f'<div class="stat-card">'
                    f'<div class="stat-label">{_ROBOT_EMOJI.get(rname,"")} {rname}</div>'
                    f'<div class="stat-value" style="color:{rcol};font-size:20px">'
                    f'${rp:+,.2f}</div>'
                    f'<div class="stat-sub">{len(rt)} עסקאות | {rwp:.0f}% הצלחה</div>'
                    f'</div></div>'
                )

        rows = _build_rows(tlist)
        display = 'block' if tab_id == 'all' else 'none'

        # Robot column only in "all" tab
        robot_th = '<th>רובוט</th>' if tab_id == 'all' else ''

        tab_panels += f'''
<div id="tab-{tab_id}" class="tab-panel" style="display:{display}">
{stats}
<table>
<thead><tr>
    <th>תאריך</th>
    <th>סימבול</th>
    <th>סוג</th>
    <th>כמות</th>
    <th>כניסה</th>
    <th>יציאה</th>
    <th>רווח/הפסד</th>
    <th>%</th>
    <th>שעת כניסה</th>
    <th>שעת יציאה</th>
    <th>הערות</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>
</div>
'''

    html = f'''<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="30">
<title>📒 יומן עסקאות</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
    background: #0a0a1a;
    color: #e0e0e0;
    font-family: 'Segoe UI', Tahoma, sans-serif;
    padding: 20px;
    direction: rtl;
}}
h1 {{
    text-align: center;
    color: #ffd700;
    margin-bottom: 16px;
    font-size: 28px;
}}
.tabs {{
    display: flex;
    gap: 4px;
    justify-content: center;
    margin-bottom: 20px;
    flex-wrap: wrap;
}}
.tab-btn {{
    background: #16213e;
    color: #aaa;
    border: 1px solid #333;
    border-radius: 8px 8px 0 0;
    padding: 10px 24px;
    font-size: 15px;
    cursor: pointer;
    transition: all 0.2s;
}}
.tab-btn:hover {{ background: #1e3a5f; color: #fff; }}
.tab-btn.active {{
    background: #1a1a3e;
    color: #ffd700;
    border-bottom: 3px solid #ffd700;
    font-weight: bold;
}}
.stats-bar {{
    display: flex;
    gap: 12px;
    justify-content: center;
    flex-wrap: wrap;
    margin-bottom: 16px;
}}
.stat-card {{
    background: #16213e;
    border-radius: 12px;
    padding: 14px 20px;
    text-align: center;
    min-width: 140px;
    border: 1px solid #1a1a3e;
}}
.stat-card.main {{
    border: 2px solid #ffd700;
    min-width: 160px;
}}
.stat-label {{ color: #888; font-size: 13px; margin-bottom: 4px; }}
.stat-value {{ font-size: 22px; font-weight: bold; }}
.stat-sub {{ color: #666; font-size: 12px; margin-top: 4px; }}
table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 8px;
    font-size: 14px;
}}
thead th {{
    background: #1a1a3e;
    color: #ffd700;
    padding: 10px 8px;
    text-align: right;
    position: sticky;
    top: 0;
    z-index: 10;
    border-bottom: 2px solid #333;
}}
tbody td {{
    padding: 9px 8px;
    border-bottom: 1px solid #1a1a2e;
    vertical-align: top;
}}
.notes {{
    font-size: 12px;
    color: #aaa;
    max-width: 250px;
}}
.detail-row td {{ line-height: 1.6; }}
tr:hover {{ background: #1e3a5f !important; }}
.footer {{
    text-align: center;
    color: #444;
    margin-top: 20px;
    font-size: 12px;
}}
</style>
</head>
<body>
<h1>📒 יומן עסקאות — רובוטים</h1>

<div class="tabs">
{tab_buttons}
</div>

{tab_panels}

<div class="footer">עודכן לאחרונה: {datetime.now(_IL_TZ).strftime("%d/%m/%Y %H:%M:%S")} 🇮🇱</div>

<script>
function switchTab(id) {{
    document.querySelectorAll('.tab-panel').forEach(p => p.style.display = 'none');
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.getElementById('tab-' + id).style.display = 'block';
    event.target.classList.add('active');
}}
</script>
</body>
</html>'''

    try:
        _JOURNAL_HTML_PATH.write_text(html, encoding='utf-8')
    except Exception as e:
        log.warning(f"Journal HTML write error: {e}")


def _generate_demo_journal_html(trades: list[dict]):
    """Generate a clean 'Demo' HTML journal on Desktop showing all trades."""
    _ROBOT_SHORT = {'FIB DT': 'פיבו', 'Gap&Go': 'גאפ', 'MR': 'מומנטום', 'FT': 'סיבוב'}

    # Stats
    total_pnl = sum(t.get('pnl', 0) for t in trades)
    wins = [t for t in trades if t.get('pnl', 0) > 0]
    losses = [t for t in trades if t.get('pnl', 0) < 0]
    n = len(trades)
    wr = (len(wins) / n * 100) if n else 0
    avg_w = (sum(t['pnl'] for t in wins) / len(wins)) if wins else 0
    avg_l = (sum(t['pnl'] for t in losses) / len(losses)) if losses else 0

    # Per-robot stats
    robot_stats_html = ''
    for rname in ['FIB DT', 'Gap&Go', 'MR', 'FT']:
        rt = [t for t in trades if t.get('robot') == rname]
        if not rt:
            continue
        rpnl = sum(t.get('pnl', 0) for t in rt)
        rw = len([t for t in rt if t.get('pnl', 0) > 0])
        rl = len([t for t in rt if t.get('pnl', 0) < 0])
        rn = len(rt)
        rwr = (rw / rn * 100) if rn else 0
        rc = '#4caf50' if rpnl >= 0 else '#f44336'
        rhe = _ROBOT_SHORT.get(rname, rname)
        robot_stats_html += (
            f'<div style="display:inline-block;background:#16213e;border-radius:8px;'
            f'padding:8px 16px;margin:4px;text-align:center">'
            f'<div style="color:#00d4ff;font-weight:bold">{rhe}</div>'
            f'<div style="color:{rc};font-size:18px;font-weight:bold">${rpnl:+,.2f}</div>'
            f'<div style="color:#888;font-size:12px">{rn} עסקאות | {rwr:.0f}% הצלחה</div>'
            f'</div>'
        )

    # Trade rows
    rows_html = ''
    for i, t in enumerate(reversed(trades)):
        pv = t.get('pnl', 0)
        pp = t.get('pnl_pct', 0)
        rc = '#4caf50' if pv >= 0 else '#f44336'
        bg = '#1a1a2e' if i % 2 == 0 else '#16213e'
        rhe = _ROBOT_SHORT.get(t.get('robot', ''), t.get('robot', ''))
        rows_html += (
            f'<tr style="background:{bg}">'
            f'<td>{t.get("date","")}</td>'
            f'<td>{t.get("exit_time","")}</td>'
            f'<td style="color:#00d4ff">{rhe}</td>'
            f'<td style="font-weight:bold">{t.get("symbol","")}</td>'
            f'<td>${t.get("entry_price",0):.2f}</td>'
            f'<td>${t.get("exit_price",0):.2f}</td>'
            f'<td style="color:{rc};font-weight:bold">${pv:+,.2f}</td>'
            f'<td style="color:{rc}">{pp:+.1f}%</td>'
            f'<td style="color:#888;font-size:11px">{t.get("exit_note","")[:30]}</td>'
            f'</tr>\n'
        )

    pnl_color = '#4caf50' if total_pnl >= 0 else '#f44336'
    html = f'''<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
<meta charset="UTF-8">
<title>יומן דמו — WTS</title>
<style>
body {{ background:#0e1117; color:#e0e0e0; font-family:Segoe UI,sans-serif; margin:0; padding:20px }}
h1 {{ color:#00d4ff; text-align:center }}
.stats {{ text-align:center; margin:20px 0 }}
.stats .big {{ font-size:28px; font-weight:bold; color:{pnl_color} }}
.stats .sub {{ color:#888; font-size:14px }}
table {{ width:100%; border-collapse:collapse; margin-top:16px }}
th {{ background:#1a1a3e; color:#00d4ff; padding:8px; text-align:right; font-size:13px }}
td {{ padding:6px 8px; border-bottom:1px solid #222; font-size:13px }}
tr:hover {{ background:#222244 !important }}
</style>
</head>
<body>
<h1>📊 יומן מסחר — חשבון דמו</h1>
<div class="stats">
<div class="big">${total_pnl:+,.2f}</div>
<div class="sub">{n} עסקאות | {wr:.0f}% הצלחה | ממוצע רווח ${avg_w:+,.2f} | ממוצע הפסד ${avg_l:+,.2f}</div>
</div>
<div style="text-align:center">{robot_stats_html}</div>
<table>
<thead><tr>
<th>תאריך</th><th>שעה</th><th>רובוט</th><th>מניה</th>
<th>כניסה</th><th>יציאה</th><th>ר/ה $</th><th>ר/ה %</th><th>הערה</th>
</tr></thead>
<tbody>
{rows_html}
</tbody>
</table>
<div style="text-align:center;color:#555;margin-top:20px;font-size:12px">
עודכן: {datetime.now().strftime("%d/%m/%Y %H:%M")} | WTS Demo Trading System
</div>
</body>
</html>'''

    try:
        _DEMO_JOURNAL_HTML_PATH.write_text(html, encoding='utf-8')
        log.info(f"Demo journal HTML written: {_DEMO_JOURNAL_HTML_PATH}")
    except Exception as e:
        log.warning(f"Demo journal HTML write error: {e}")


def _build_trade_logic_detail(robot_name: str, prefix: str, sym: str,
                               entry_price: float, qty: int) -> str:
    """Build detailed Hebrew logic breakdown for trade journal.

    Shows the complete robot decision flow: scan → filters → signal → risk → timeline.
    """
    lines: list[str] = []
    enrich = _enrichment.get(sym, {})

    # ── 1. Scan data (from last known current data) ──
    # Try to reconstruct from enrichment + daily events
    float_str = enrich.get('float', '-')
    short_str = enrich.get('short', '-')
    market_cap = enrich.get('market_cap', '-')
    company = enrich.get('company', '-')

    lines.append("📊 <b>פירוט לוגיקה:</b>")

    # Company
    if company and company != '-':
        lines.append(f"🏢 {company}")

    # ── 2. Fundamentals ──
    fund_parts = []
    if float_str != '-':
        fund_parts.append(f"Float: {float_str}")
    if short_str != '-':
        fund_parts.append(f"Short: {short_str}")
    if market_cap != '-':
        fund_parts.append(f"MCap: {market_cap}")
    eps = enrich.get('eps', '-')
    if eps != '-':
        fund_parts.append(f"EPS: {eps}")
    cash_ps = enrich.get('cash', '-')
    if cash_ps != '-':
        fund_parts.append(f"Cash: {cash_ps}")
    if fund_parts:
        lines.append(f"📋 {' | '.join(fund_parts)}")

    # ── 3. News headlines ──
    news = enrich.get('news', [])
    if news:
        for n in news[:2]:
            title = n.get('title_he') or n.get('title_en', '')
            if title:
                lines.append(f"📰 {title[:70]}")
    else:
        lines.append("📰 ללא חדשות")

    # ── 4. Robot thresholds (what this robot requires) ──
    thresh = ''
    if robot_name == 'FIB DT':
        thresh = (
            f"🤖 חוקי רובוט: גאפ {FIB_DT_GAP_MIN_PCT:.0f}-{FIB_DT_GAP_MAX_PCT:.0f}% | "
            f"RVOL≥{FIB_DT_RVOL_MIN}x | מחיר≥${STRATEGY_MIN_PRICE} | "
            f"חדשות: {'כן' if FIB_DT_REQUIRE_NEWS else 'לא'} | SMA9(5m+4h)\n"
            f"💰 גודל: ${FIB_DT_POSITION_SIZE_FIXED}/פוז | מקס {FIB_DT_MAX_POSITIONS} פוז | "
            f"warmup {FIB_DT_WARMUP_SEC}s | מקס החזקה {FIB_DT_MAX_HOLD_MINUTES} דק"
        )
    elif robot_name == 'Gap&Go':
        thresh = (
            f"🤖 חוקי רובוט: גאפ≥{GG_LIVE_GAP_MIN_PCT:.0f}% | "
            f"RVOL≥{GG_LIVE_RVOL_MIN}x | מחיר≥${STRATEGY_MIN_PRICE} | "
            f"חדשות: {'כן' if GG_LIVE_REQUIRE_NEWS else 'לא'} | SMA9(5m+4h)\n"
            f"💰 גודל: ${GG_LIVE_POSITION_SIZE_FIXED}/פוז | מקס {GG_LIVE_MAX_POSITIONS} פוז | "
            f"מקס החזקה {GG_MAX_HOLD_MINUTES} דק"
        )
    elif robot_name == 'MR':
        thresh = (
            f"🤖 חוקי רובוט: גאפ {MR_GAP_MIN_PCT:.0f}-{MR_GAP_MAX_PCT:.0f}% | "
            f"RVOL≥{MR_LIVE_RVOL_MIN}x | מחיר≥${STRATEGY_MIN_PRICE} | "
            f"חדשות: {'כן' if MR_LIVE_REQUIRE_NEWS else 'לא'} | SMA9(5m+4h)\n"
            f"💰 גודל: ${MR_LIVE_POSITION_SIZE_FIXED}/פוז | מקס {MR_LIVE_MAX_POSITIONS} פוז | "
            f"טרגט +{MR_LIVE_PROFIT_TARGET_PCT*100:.0f}%"
        )
    elif robot_name == 'FT':
        thresh = (
            f"🤖 חוקי רובוט: turnover≥{FT_MIN_FLOAT_TURNOVER_PCT:.0f}% | "
            f"גאפ≥{FT_LIVE_GAP_MIN_PCT:.0f}% | RVOL≥{FT_LIVE_RVOL_MIN}x | "
            f"חדשות: {'כן' if FT_LIVE_REQUIRE_NEWS else 'לא'} | SMA9(5m+4h)\n"
            f"💰 גודל: ${FT_LIVE_POSITION_SIZE_FIXED}/פוז | מקס {FT_LIVE_MAX_POSITIONS} פוז | "
            f"טרגט +{FT_LIVE_PROFIT_TARGET_PCT*100:.0f}% | מקס {FT_MAX_HOLD_MINUTES} דק"
        )
    if thresh:
        lines.append(thresh)

    # ── 5. Position sizing detail ──
    cost = qty * entry_price
    lines.append(f"📐 פוזיציה: {qty}sh × ${entry_price:.2f} = ${cost:,.0f}")

    # ── 6. Timeline — all events for this stock from _daily_events ──
    sym_events = [e for e in _daily_events
                  if e['symbol'] == sym and e['type'].startswith(prefix)]
    if sym_events:
        lines.append("⏱️ <b>ציר זמן:</b>")
        for ev in sym_events[-10:]:  # last 10 events max
            t = ev.get('time', '')
            etype = ev.get('type', '').replace(prefix, '')
            detail = ev.get('detail', '')
            # Hebrew labels for event types
            type_he = {
                'track': '🔍 מעקב',
                'signal': '⚡ אות',
                'entry': '← כניסה',
                'exit': '→ יציאה',
                'close': '→ סגירה',
            }.get(etype, etype)
            short = detail[:65] + '…' if len(detail) > 65 else detail
            lines.append(f"  {t} {type_he}: {short}")

    # ── 7. Risk controls reminder ──
    lines.append(
        f"🛡️ ניהול סיכון: cooldown {STRATEGY_REENTRY_COOLDOWN_SEC//60} דק | "
        f"הפסד {STRATEGY_REENTRY_COOLDOWN_AFTER_LOSS_SEC//60} דק | "
        f"מקס {STRATEGY_MAX_ENTRIES_PER_STOCK_PER_DAY}/יום/מניה | "
        f"לימיט ${STRATEGY_DAILY_LOSS_LIMIT:.0f}/יום"
    )

    return "\n".join(lines)


def _send_trade_journal_entry(robot_emoji: str, robot_name: str,
                               sym: str, reason: str, qty: int,
                               price: float, entry_price: float,
                               pnl: float, net_liq: float, cash: float,
                               trades: list[dict]):
    """Send a Hebrew trade journal entry to Telegram and append to persistent CSV.

    Format: two-row journal (buy + sell) in Israel timezone with notes,
    followed by full robot logic breakdown (scan → filters → signal → exit).
    The CSV journal (trade_journal_all.csv) never resets — tracks all-time results.
    """
    # Find entry time from trades list (last BUY for this symbol)
    entry_time_str = ''
    entry_time_csv = ''
    for t in reversed(trades):
        if t.get('sym') == sym and t.get('side') == 'BUY':
            entry_dt = t['time']
            if entry_dt.tzinfo is None:
                entry_dt = entry_dt.replace(tzinfo=_ET)
            il_entry = entry_dt.astimezone(_IL_TZ)
            entry_time_str = il_entry.strftime('%d/%m %H:%M')
            entry_time_csv = il_entry.strftime('%H:%M:%S')
            break

    # Exit time = now in Israel TZ
    exit_dt = datetime.now(_ET).astimezone(_IL_TZ)
    exit_time_str = exit_dt.strftime('%d/%m %H:%M')
    exit_time_csv = exit_dt.strftime('%H:%M:%S')
    date_str = exit_dt.strftime('%d/%m/%Y')
    date_csv = exit_dt.strftime('%Y-%m-%d')

    # Find entry reason from _daily_events
    prefix_map = {'FIB DT': 'fib_dt_', 'Gap&Go': 'gg_', 'MR': 'mr_', 'FT': 'ft_'}
    prefix = prefix_map.get(robot_name, '')
    entry_reason = ''
    for ev in reversed(_daily_events):
        if ev['type'] == f'{prefix}entry' and ev['symbol'] == sym:
            entry_reason = ev.get('detail', '')
            break

    # Clean up entry reason — extract parenthetical reason if present
    if '(' in entry_reason and entry_reason.endswith(')'):
        entry_note = entry_reason[entry_reason.rfind('(') + 1:-1]
    elif entry_reason:
        entry_note = entry_reason[:60]
    else:
        entry_note = '-'

    # Clean up exit reason
    exit_note = reason.replace('TRAILING', 'trailing').replace('TARGET_HALF', 'target_half')
    if len(exit_note) > 60:
        exit_note = exit_note[:57] + '…'

    pnl_pct = (price / entry_price - 1) * 100 if entry_price > 0 else 0
    pnl_icon = '🟢' if pnl >= 0 else '🔴'
    sell_label = 'מכירה חלקית' if 'HALF' in reason.upper() or 'half' in reason else 'מכירה'

    # ── Build detailed logic breakdown ──
    logic_detail = _build_trade_logic_detail(robot_name, prefix, sym,
                                              entry_price, qty)
    # Plain-text version for Telegram (with HTML tags)
    logic_for_tg = logic_detail
    # Clean version for HTML journal (strip Telegram HTML bold tags)
    logic_for_html = logic_detail.replace('<b>', '').replace('</b>', '')

    # ── Append to persistent JSON + regenerate HTML ──
    side_val = 'SELL_HALF' if 'HALF' in reason.upper() or 'half' in reason else 'SELL'
    trade_record = {
        'date': date_str,
        'entry_time': entry_time_str.split(' ', 1)[-1] if ' ' in entry_time_str else entry_time_str,
        'exit_time': exit_time_str.split(' ', 1)[-1] if ' ' in exit_time_str else exit_time_str,
        'robot': robot_name,
        'symbol': sym,
        'side': side_val,
        'qty': qty,
        'entry_price': round(entry_price, 4),
        'exit_price': round(price, 4),
        'pnl': round(pnl, 2),
        'pnl_pct': round(pnl_pct, 1),
        'cash_after': round(cash, 2),
        'nlv_after': round(net_liq, 2),
        'entry_note': entry_note,
        'exit_note': exit_note,
        'logic': logic_for_html,
    }
    try:
        all_trades = _load_journal_trades()
        all_trades.append(trade_record)
        _save_journal_trades(all_trades)
        _generate_journal_html(all_trades)
        _generate_demo_journal_html(all_trades)
    except Exception as e:
        log.warning(f"Persistent journal write error: {e}")

    # ── Send Telegram message ──
    msg = (
        f"📒 <b>יומן — {robot_emoji} {robot_name} | {sym}</b>\n"
        f"📅 {date_str}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"  {entry_time_str}  ←  קנייה  {qty}sh  @  ${entry_price:.2f}\n"
        f"  {exit_time_str}  →  {sell_label}  {qty}sh  @  ${price:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{pnl_icon} רווח/הפסד: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
        f"💵 קופה: ${cash:,.0f} | NLV: ${net_liq:,.0f}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📝 כניסה: {entry_note}\n"
        f"📝 יציאה: {exit_note}\n"
        f"\n{logic_for_tg}"
    )
    send_telegram(msg)


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
        # Top 5 by dollar volume
        by_vol = sorted(_session_stocks.items(),
                        key=lambda x: x[1].get('volume_raw', 0) * x[1].get('price', 0), reverse=True)[:5]

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
                p = info.get('price', 0)
                dol_v = vol * p if vol and p else 0
                dol_str = _format_dollar_short(dol_v) if dol_v > 0 else '-'
                enrich = _enrichment.get(sym, {})
                flt = enrich.get('float', '-')
                flt_sh = _parse_float_to_shares(flt)
                flt_dol = _format_dollar_short(flt_sh * p) if flt_sh > 0 and p > 0 else flt
                lines.append(
                    f"  {i}. {arrow} <b>{sym}</b> {info['pct']:+.1f}%  ${p:.2f}  ווליום:{_format_shares_short(vol)} ({dol_str})  מ.פניות:{_format_shares_short(flt_sh) if flt_sh > 0 else flt} ({flt_dol})"
                )

        if by_vol:
            lines.append("")
            lines.append("📊 <b>ווליום:</b>")
            for i, (sym, info) in enumerate(by_vol, 1):
                vol = info.get('volume_raw', 0)
                p = info.get('price', 0)
                dol_v = vol * p if vol and p else 0
                dol_str = _format_dollar_short(dol_v) if dol_v > 0 else '-'
                enrich = _enrichment.get(sym, {})
                flt = enrich.get('float', '-')
                flt_sh = _parse_float_to_shares(flt)
                flt_dol = _format_dollar_short(flt_sh * p) if flt_sh > 0 and p > 0 else flt
                lines.append(
                    f"  {i}. <b>{sym}</b> {info['pct']:+.1f}%  ${p:.2f}  ווליום:{_format_shares_short(vol)} ({dol_str})  מ.פניות:{_format_shares_short(flt_sh) if flt_sh > 0 else flt} ({flt_dol})"
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
_hod_last_alert_time: dict[str, float] = {}            # sym -> timestamp of last HOD alert
_HOD_COOLDOWN_SEC = 300          # 5 min cooldown per symbol
_HOD_MIN_JUMP_PCT = 0.02         # require 2% above last alerted high
_fib_touch_tracker: dict[str, dict[float, int]] = {}   # sym -> {fib_level: touch_count} (legacy, kept for daily reset)
_lod_touch_tracker: dict[str, int] = {}                 # sym -> touch count at day_low
_lod_was_near: dict[str, bool] = {}                     # sym -> was near LOD last cycle
_vwap_side: dict[str, str] = {}                         # sym -> 'above' | 'below'
_running_high: dict[str, float] = {}                      # sym -> highest price seen today
_running_low: dict[str, float] = {}                       # sym -> lowest price seen today
_price_history: dict[str, list[tuple[float, float]]] = {}  # sym -> [(timestamp, price)]
_spike_alerted: dict[str, float] = {}                   # sym -> timestamp (cooldown)

_VWAP_COOLDOWN_SEC = 600    # 10 min between VWAP cross alerts per symbol
_vwap_last_alert: dict[str, float] = {}
_SPIKE_COOLDOWN_SEC = 300    # 5 min cooldown per symbol for spike alerts
_VOL_ALERT_COOLDOWN_SEC = 1800  # 30 min cooldown per symbol for volume alerts
_vol_alert_sent: dict[str, float] = {}  # sym -> last alert timestamp
VOL_ALERT_RVOL_MIN = 3.0  # minimum RVOL for volume alert

_DOJI_COOLDOWN_SEC = 300  # 5 min cooldown per symbol+timeframe
_doji_alerted: dict[str, float] = {}  # "SYM_5m" → timestamp
_doji_pending: dict[str, tuple[float, float, str]] = {}  # "SYM_5m" → (doji_high, timestamp, tf_label)

# ── Fib bounce (synthetic 1-min bars) ──
_fib_bar_forming: dict[str, dict] = {}      # sym → {minute, open, high, low, close}
_fib_bar_completed: dict[str, dict] = {}    # sym → last completed bar {open, high, low, close}
_fib_bounce_cooldown: dict[str, float] = {} # "SYM_0.1234" → timestamp
_FIB_BOUNCE_COOLDOWN_SEC = 600              # 10 min cooldown per symbol+level

# ── Fib support confirmation (2-bar hold) ──
_fib_support_candidates: dict[str, list[tuple[float, float, float]]] = {}
# {sym: [(fib_level, bar_close, timestamp), ...]}
_fib_support_cooldown: dict[str, float] = {}  # "SYM_0.1234" → timestamp
_FIB_SUPPORT_COOLDOWN_SEC = 600  # 10 min per sym+level

# ── Alert chart cooldown ──
_ALERT_CHART_COOLDOWN_SEC = 600  # 10 min cooldown per symbol for chart attachment
_alert_chart_sent: dict[str, float] = {}  # sym → last chart timestamp

# ── Timeframe High Break tracking ──
_tf_highs_cache: dict[str, dict[str, float]] = {}   # {sym: {day: X, week: Y, month: Z, quarter: Q, year: A}}
_tf_high_alerted: dict[str, bool] = {}               # {"SYM_day": True, ...} — one alert per TF per symbol per day
_TF_LABELS_HE = {'day': 'יומי', 'week': 'שבועי', 'month': 'חודשי', 'quarter': 'רבעוני', 'year': 'שנתי'}


def _reset_alerts_if_new_day():
    """Clear all alert tracking state when date changes."""
    global _alerts_date, _last_news_summary_time
    today = datetime.now(ZoneInfo('US/Eastern')).strftime('%Y-%m-%d')
    if today != _alerts_date:
        _alerts_date = today
        _hod_break_alerted.clear()
        _hod_last_alert_time.clear()
        _fib_touch_tracker.clear()
        _fib_bar_forming.clear()
        _fib_bar_completed.clear()
        _fib_bounce_cooldown.clear()
        _fib_support_candidates.clear()
        _fib_support_cooldown.clear()
        _lod_touch_tracker.clear()
        _lod_was_near.clear()
        _vwap_side.clear()
        _vwap_last_alert.clear()
        _running_high.clear()
        _running_low.clear()
        _price_history.clear()
        _spike_alerted.clear()
        _vol_alert_sent.clear()
        _doji_alerted.clear()
        _doji_pending.clear()
        _alert_chart_sent.clear()
        _tf_highs_cache.clear()
        _tf_high_alerted.clear()
        _news_summary_sent.clear()
        _last_news_summary_time = 0.0
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


def check_hod_break(sym: str, current: dict, previous: dict) -> tuple[str, str] | None:
    """Alert when price breaks today's high of day (≥20% only).

    Uses _running_high tracker (updated every cycle including quick OCR
    updates) instead of comparing stale IBKR day_high values.
    """
    if current.get('pct', 0) < ALERT_MIN_PCT:
        return None
    price = current.get('price', 0)
    if price <= 0:
        return None

    # Merge IBKR day_high with running tracker for best accuracy
    ibkr_high = current.get('day_high', 0)
    prev_running = _running_high.get(sym, 0)
    known_high = max(ibkr_high, prev_running) if ibkr_high > 0 else prev_running

    if known_high <= 0:
        # First observation — seed tracker, no alert
        _running_high[sym] = max(price, ibkr_high)
        return None

    if price > known_high:
        # New high of day
        _running_high[sym] = price
        last_alerted = _hod_break_alerted.get(sym, 0)
        if price <= last_alerted:
            return None
        # Require minimum 2% jump above last alerted high
        if last_alerted > 0 and (price - last_alerted) / last_alerted < _HOD_MIN_JUMP_PCT:
            return None
        # 5-min cooldown
        now = time_mod.time()
        if now - _hod_last_alert_time.get(sym, 0) < _HOD_COOLDOWN_SEC:
            return None
        _hod_break_alerted[sym] = price
        _hod_last_alert_time[sym] = now
        pct = current.get('pct', 0)
        full = (
            f"🔺 <b>שיא יומי — {sym}</b> ${price:.2f} ({pct:+.1f}%)\n"
            f"קודם ${known_high:.2f}"
        )
        compact = f"🔺 שיא יומי ${price:.2f} (קודם ${known_high:.2f})"
        return full, compact
    else:
        # Not a new high — keep tracker up to date with IBKR data
        if ibkr_high > prev_running:
            _running_high[sym] = ibkr_high
        return None


def _update_fib_bar(sym: str, price: float) -> dict | None:
    """Update synthetic 1-min bar from snapshot prices.

    Returns the last *completed* bar (previous minute) or None.
    """
    now = time_mod.time()
    current_minute = int(now // 60)

    forming = _fib_bar_forming.get(sym)
    if forming and forming['minute'] == current_minute:
        # Same minute — update OHLC
        forming['high'] = max(forming['high'], price)
        forming['low'] = min(forming['low'], price)
        forming['close'] = price
    else:
        # New minute — promote forming bar to completed, start fresh
        if forming:
            _fib_bar_completed[sym] = {
                'open': forming['open'],
                'high': forming['high'],
                'low': forming['low'],
                'close': forming['close'],
            }
        _fib_bar_forming[sym] = {
            'minute': current_minute,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
        }

    return _fib_bar_completed.get(sym)


def check_fib_second_touch(sym: str, price: float, pct: float) -> tuple[str, str] | None:
    """Alert on fib bounce/rejection using synthetic 1-min bar close.

    Support bounce: bar.low touched fib level (within 0.5%) + bar.close > level.
    Resistance rejection: bar.high touched fib level (within 0.5%) + bar.close < level.
    10-min cooldown per symbol+level.
    """
    if pct < ALERT_MIN_PCT or price <= 0:
        return None

    # Update synthetic bar and get last completed bar
    bar = _update_fib_bar(sym, price)
    if not bar:
        return None

    # Ensure fib levels are cached
    with _fib_cache_lock:
        has_cache = sym in _fib_cache
    if not has_cache:
        calc_fib_levels(sym, price)
    with _fib_cache_lock:
        cached = _fib_cache.get(sym)
    if not cached:
        return None

    _, _, all_levels, ratio_map, *_ = cached
    now = time_mod.time()

    bar_high = bar['high']
    bar_low = bar['low']
    bar_close = bar['close']

    for lv in all_levels:
        lv_key = round(lv, 4)
        cooldown_key = f"{sym}_{lv_key}"

        # Cooldown check
        if now - _fib_bounce_cooldown.get(cooldown_key, 0) < _FIB_BOUNCE_COOLDOWN_SEC:
            continue

        threshold = lv * 0.008  # 0.8% proximity (synthetic bars miss extremes)

        info = ratio_map.get(lv_key)
        ratio_label = f" ({info[0]} {info[1]})" if info else ""

        # Support bounce: wick touched from above, closed above
        if abs(bar_low - lv) <= threshold and bar_close > lv:
            _fib_bounce_cooldown[cooldown_key] = now
            log.debug(f"Fib bounce {sym}: off ${lv:.4f}{ratio_label} "
                      f"bar L=${bar_low:.4f} C=${bar_close:.4f} H=${bar_high:.4f}")
            full = (
                f"<b>{sym}</b> ${price:.2f} ({pct:+.1f}%)\n"
                f"🎯 באונס מפיבו — רמה ${lv:.4f}{ratio_label} | נסגר מעל"
            )
            compact = f"🎯 באונס {sym} ${lv:.4f}{ratio_label}"
            return full, compact

        # Resistance rejection: wick touched from below, closed below
        if abs(bar_high - lv) <= threshold and bar_close < lv:
            _fib_bounce_cooldown[cooldown_key] = now
            log.debug(f"Fib rejection {sym}: at ${lv:.4f}{ratio_label} "
                      f"bar L=${bar_low:.4f} C=${bar_close:.4f} H=${bar_high:.4f}")
            full = (
                f"<b>{sym}</b> ${price:.2f} ({pct:+.1f}%)\n"
                f"🎯 דחייה מפיבו — רמה ${lv:.4f}{ratio_label} | נסגר מתחת"
            )
            compact = f"🎯 דחייה {sym} ${lv:.4f}{ratio_label}"
            return full, compact

    return None


def check_fib_support_hold(sym: str, price: float, pct: float) -> tuple[str, str] | None:
    """Alert when price holds above a fib level for 2 consecutive 1-min bars.

    Bar N closes within 1.5% above a fib level → candidate.
    Bar N+1 low stays above that level → support confirmed → alert.
    10-min cooldown per symbol+level.
    """
    if pct < ALERT_MIN_PCT or price <= 0:
        return None

    bar = _update_fib_bar(sym, price)
    if not bar:
        return None

    with _fib_cache_lock:
        cached = _fib_cache.get(sym)
    if not cached:
        return None

    _, _, all_levels, ratio_map, *_ = cached
    now = time_mod.time()
    bar_low = bar['low']
    bar_close = bar['close']

    # ── Phase 1: confirm previous candidates ──
    prev_candidates = _fib_support_candidates.pop(sym, [])
    result = None
    for lv, _prev_close, _ts in prev_candidates:
        lv_key = round(lv, 4)
        cooldown_key = f"{sym}_{lv_key}"
        if now - _fib_support_cooldown.get(cooldown_key, 0) < _FIB_SUPPORT_COOLDOWN_SEC:
            continue
        if bar_low > lv:
            # Support confirmed — current bar didn't break below
            _fib_support_cooldown[cooldown_key] = now
            info = ratio_map.get(lv_key)
            ratio_label = f" ({info[0]} {info[1]})" if info else ""
            log.debug(f"Fib support hold {sym}: ${lv:.4f}{ratio_label} "
                      f"bar L=${bar_low:.4f} C=${bar_close:.4f}")
            full = (
                f"<b>{sym}</b> ${price:.2f} ({pct:+.1f}%)\n"
                f"\U0001f6e1 תמיכת פיבו — רמה ${lv:.4f}{ratio_label} | החזיק 2 נרות"
            )
            compact = f"\U0001f6e1 תמיכה {sym} ${lv:.4f}{ratio_label}"
            result = (full, compact)
            break  # one alert per cycle

    # ── Phase 2: record new candidates from current bar ──
    new_candidates: list[tuple[float, float, float]] = []
    for lv in all_levels:
        lv_key = round(lv, 4)
        cooldown_key = f"{sym}_{lv_key}"
        if now - _fib_support_cooldown.get(cooldown_key, 0) < _FIB_SUPPORT_COOLDOWN_SEC:
            continue
        if lv <= 0:
            continue
        pct_above = (bar_close - lv) / lv
        if 0 < pct_above <= 0.015:
            new_candidates.append((lv, bar_close, now))

    if new_candidates:
        _fib_support_candidates[sym] = new_candidates

    return result


def check_lod_touch(sym: str, price: float, day_low: float, pct: float) -> tuple[str, str] | None:
    """Alert on 2nd touch of day's low (≥20% only, 0.5% proximity).

    Uses _running_low tracker for accurate day low instead of stale
    IBKR day_low that may be minutes old.
    """
    if pct < ALERT_MIN_PCT or price <= 0:
        return None

    # Merge IBKR day_low with running tracker
    prev_running = _running_low.get(sym, 0)
    if day_low > 0 and prev_running > 0:
        effective_low = min(day_low, prev_running)
    elif day_low > 0:
        effective_low = day_low
    elif prev_running > 0:
        effective_low = prev_running
    else:
        return None

    # Update running tracker
    if price < effective_low:
        effective_low = price
    _running_low[sym] = effective_low

    threshold = effective_low * 0.005  # 0.5%
    near_lod = abs(price - effective_low) <= threshold

    was_near = _lod_was_near.get(sym, False)
    _lod_was_near[sym] = near_lod

    if near_lod and not was_near:
        count = _lod_touch_tracker.get(sym, 0) + 1
        _lod_touch_tracker[sym] = count
        if count == 2:
            full = (
                f"🔻 <b>נמוך יומי x2 — {sym}</b> ${price:.2f} ({pct:+.1f}%)\n"
                f"נמוך ${effective_low:.2f} — תמיכה/שבירה?"
            )
            compact = f"🔻 נמוך יומי x2 ${effective_low:.2f}"
            return full, compact
    return None


def check_vwap_cross(sym: str, price: float, vwap: float, pct: float) -> tuple[str, str] | None:
    """Alert when price crosses VWAP (10-min cooldown per symbol).

    State update is deferred until after cooldown check to prevent
    state corruption when crosses happen during cooldown window.
    """
    if price <= 0 or vwap <= 0:
        return None

    current_side = 'above' if price > vwap else 'below'
    prev_side = _vwap_side.get(sym)

    if prev_side is None:
        # First observation — seed state, no alert
        _vwap_side[sym] = current_side
        return None

    if prev_side == current_side:
        return None

    # Cross detected — check cooldown BEFORE updating state
    last = _vwap_last_alert.get(sym, 0)
    if time_mod.time() - last < _VWAP_COOLDOWN_SEC:
        # Don't update _vwap_side — cross will be re-detected next cycle
        return None

    # Cooldown passed — commit state change
    _vwap_side[sym] = current_side

    # Only alert on cross ABOVE VWAP (bullish signal)
    if prev_side == 'below':
        _vwap_last_alert[sym] = time_mod.time()
        full = (
            f"⚡ <b>חציית VWAP — {sym}</b> ${price:.2f} ({pct:+.1f}%)\n"
            f"חצה מעל ${vwap:.2f}"
        )
        compact = f"⚡ חציית VWAP ${vwap:.2f}"
        return full, compact
    else:
        # Cross below — update state silently, no alert
        return None


def check_spike(sym: str, price: float, pct: float) -> tuple[str, str] | None:
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
        full = (
            f"🚀 <b>קפיצה +{change_pct:.1f}% — {sym}</b> ב-{elapsed_min:.1f} דק\n"
            f"${old_price:.2f}→${price:.2f} | יומי {pct:+.1f}%"
        )
        compact = f"🚀 קפיצה +{change_pct:.1f}% ${old_price:.2f}→${price:.2f}"
        return full, compact
    return None


def check_volume_alert(sym: str, price: float, vwap: float,
                       rvol: float, pct: float) -> tuple[str, str] | None:
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

    # Float dollar value
    float_str = enrich.get('float', '-')
    float_shares = _parse_float_to_shares(float_str)
    flt_tag = ""
    if float_shares > 0:
        flt_tag = f" | מניות פניות {_format_shares_short(float_shares)} ({_format_dollar_short(float_shares * price)})"

    full = (
        f"📊 <b>ווליום חריג — {sym}</b> ${price:.2f} ({pct:+.1f}%)\n"
        f"פי {rvol:.1f} מהרגיל מעל VWAP ${vwap:.2f}{flt_tag}"
        f"{news_line}"
    )
    compact = f"📊 ווליום פי {rvol:.1f} מעל VWAP{' 📰' if news else ''}"
    return full, compact


def check_doji_candle(sym: str, price: float, pct: float) -> tuple[str, str] | None:
    """Alert on Doji breakout — only when price breaks above a completed Doji's high.

    Two-phase detection using CACHED bars only (no new IBKR downloads):
      1. Detect completed Doji candle → store its high in _doji_pending
      2. On next cycle, if price > doji_high → fire alert and clear pending

    Doji criteria: body ≤ 10% of range, range ≥ 0.3% of price.
    Pending dojis expire after 10 minutes (no breakout = invalidated).
    """
    if sym not in _enrichment:
        return None

    now = time_mod.time()

    # ── Phase 2: Check if price breaks above any pending doji high ──
    breakout_hits = []
    expired = []
    for key, (doji_high, doji_ts, tf_label) in _doji_pending.items():
        if not key.startswith(f"{sym}_"):
            continue
        # Expire pending dojis after 10 minutes
        if now - doji_ts > 600:
            expired.append(key)
            continue
        if price > doji_high:
            breakout_hits.append((tf_label, doji_high))
            expired.append(key)
    for key in expired:
        _doji_pending.pop(key, None)

    if breakout_hits:
        tfs = ", ".join(tf for tf, _ in breakout_hits)
        doji_h = breakout_hits[0][1]
        full = (
            f"🔺 <b>פריצת דוג׳י — {sym}</b> [{tfs}] ${price:.2f} ({pct:+.1f}%)\n"
            f"שבר ${doji_h:.4f}"
        )
        compact = f"🔺 פריצת דוג׳י [{tfs}] ${doji_h:.4f}"
        return full, compact

    # ── Phase 1: Detect new completed Doji candles → store pending ──
    # Only 15m+ timeframes (1m/5m too noisy — 91 detections, 0 breakouts)
    tf_specs = [
        ('15 mins', '15m'),
        ('30 mins', '30m'),
        ('1 hour',  '1h'),
    ]

    for bar_size, tf_label in tf_specs:
        cooldown_key = f"{sym}_{tf_label}"
        if now - _doji_alerted.get(cooldown_key, 0) < _DOJI_COOLDOWN_SEC:
            continue
        # Already have a pending doji for this sym+tf
        if cooldown_key in _doji_pending:
            continue

        cache_key = f"{sym}_{bar_size}"
        entry = _intraday_cache.get(cache_key)
        if not entry:
            continue
        cached_ts, df = entry
        if time_mod.time() - cached_ts > _INTRADAY_CACHE_TTL:
            continue
        if df is None or len(df) < 3:
            continue

        # Use second-to-last bar (completed), not last (still forming)
        bar = df.iloc[-2]
        o, h, l, c = bar['open'], bar['high'], bar['low'], bar['close']
        total_range = h - l
        if total_range <= 0 or h <= 0:
            continue
        body = abs(c - o)
        if body <= total_range * 0.10 and total_range / h >= 0.003:
            # Doji detected — store high as breakout level
            _doji_pending[cooldown_key] = (h, now, tf_label)
            _doji_alerted[cooldown_key] = now
            log.info(f"Doji detected [{tf_label}] {sym}: high=${h:.4f} — waiting for breakout")

    return None


# ── Timeframe High Break ──

def _compute_tf_highs(sym: str) -> dict[str, float] | None:
    """Compute previous-candle highs for day/week/month/quarter/year from _daily_cache.

    Returns {day: X, week: Y, month: Z, quarter: Q, year: A} or None if no data.
    Caches result in _tf_highs_cache (one computation per symbol per day).
    """
    if sym in _tf_highs_cache:
        return _tf_highs_cache[sym]

    df = _daily_cache.get(sym)
    if df is None or len(df) < 5:
        return None

    # Ensure datetime index for resampling
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'date' in df.columns:
            df = df.copy()
            df.index = pd.to_datetime(df['date'])
        else:
            return None

    highs: dict[str, float] = {}
    try:
        # Previous day high = second-to-last daily bar
        if len(df) >= 2:
            highs['day'] = float(df['high'].iloc[-2])

        # Resample to weekly, monthly, quarterly, yearly
        for tf, rule in [('week', 'W'), ('month', 'ME'), ('quarter', 'QE'), ('year', 'YE')]:
            resampled = df['high'].resample(rule).max().dropna()
            if len(resampled) >= 2:
                highs[tf] = float(resampled.iloc[-2])
    except Exception as e:
        log.debug(f"TF highs compute error {sym}: {e}")
        return None

    if not highs:
        return None

    _tf_highs_cache[sym] = highs
    return highs


def check_timeframe_high_break(sym: str, price: float, pct: float) -> tuple[str, str] | None:
    """Alert when price breaks above previous day/week/month/quarter/year candle high.

    One alert per timeframe per symbol per day (cooldown via _tf_high_alerted).
    Requires enrichment (daily data must be cached via calc_fib_levels).
    """
    if pct < 20 or price <= 0:
        return None

    highs = _compute_tf_highs(sym)
    if not highs:
        return None

    broken = []  # [(tf_key, prev_high), ...]
    for tf, prev_high in highs.items():
        alert_key = f"{sym}_{tf}"
        if _tf_high_alerted.get(alert_key):
            continue
        if price > prev_high > 0:
            broken.append((tf, prev_high))
            _tf_high_alerted[alert_key] = True

    if not broken:
        return None

    if len(broken) == 1:
        tf, prev_high = broken[0]
        label = _TF_LABELS_HE[tf]
        change_pct = (price - prev_high) / prev_high * 100
        full = (
            f"📊 <b>שבירת גבוה {label} — {sym}</b> ${price:.2f} ({pct:+.1f}%)\n"
            f"גבוה קודם ${prev_high:.2f} ({change_pct:+.1f}%)"
        )
        compact = f"📊 שבירת גבוה {label} ${prev_high:.2f}"
    else:
        # Multiple timeframes broken simultaneously
        broken.sort(key=lambda x: ['day', 'week', 'month', 'quarter', 'year'].index(x[0]))
        labels = [_TF_LABELS_HE[tf] for tf, _ in broken]
        details = " + ".join(f"{_TF_LABELS_HE[tf]} ${h:.2f}" for tf, h in broken)
        full = (
            f"📊 <b>שבירת גבוה — {sym}</b> ${price:.2f} ({pct:+.1f}%)\n"
            f"{details}"
        )
        compact = f"📊 שבירת גבוה {'/'.join(labels)} ${price:.2f}"

    return full, compact


# ══════════════════════════════════════════════════════════
#  Stock Enrichment Cache (Finviz + Fib)
# ══════════════════════════════════════════════════════════

# {symbol: {float, short, eps, income, earnings, cash, fib_below, fib_above, news}}
_enrichment: dict[str, dict] = {}
_enrichment_ts: dict[str, float] = {}   # symbol → time.time() when enriched
ENRICHMENT_TTL_SECS = 30 * 60           # 30 minutes
# Sparkline: 5-min bars cache {sym: [(close, volume), ...]}
_spark_bars_cache: dict[str, list[tuple]] = {}
# Alert highlight: {sym: timestamp} — yellow bg for 60s after alert
_alerted_at: dict[str, float] = {}

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
            new_items = []
            for n in ibkr_news:
                title_lower = n['title_en'].lower()
                # Skip if very similar to an existing Finviz headline
                if any(title_lower[:30] in ft or ft[:30] in title_lower
                       for ft in finviz_titles if len(ft) > 10):
                    continue
                new_items.append(n)

            if new_items:
                titles_he = _batch_translate([it['title_en'] for it in new_items])
                for i, title_he in enumerate(titles_he):
                    it = new_items[i]
                    data['news'].append({
                        'title_he': title_he.strip(),
                        'date': it['date'],
                        'source': it['source'],
                        'article_id': it.get('article_id', ''),
                        'provider_code': it.get('provider_code', ''),
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

    ``ma_frames``: {'1m': df, '5m': df, '15m': df, '30m': df, '1h': df, 'D': df}
    Returns list of dicts: {tf, period, sma, ema} with None for unavailable.
    """
    periods = [9, 20, 50, 100, 200]
    tf_order = ['1m', '5m', '15m', '30m', '1h', 'D']
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


def _get_fresh_ma_rows(sym: str, price: float) -> list[dict] | None:
    """Recalculate ma_rows from cached intraday bars (no new downloads).

    Returns fresh ma_rows if cached bars exist, else cached ma_rows from
    enrichment, else None.
    """
    _tf_specs = [
        ('1m',  '1 min'),  ('5m',  '5 mins'), ('15m', '15 mins'),
        ('30m', '30 mins'), ('1h',  '1 hour'),
    ]
    ma_frames: dict[str, pd.DataFrame | None] = {}
    any_found = False
    for tf_key, bar_size in _tf_specs:
        cache_key = f"{sym}_{bar_size}"
        entry = _intraday_cache.get(cache_key)
        if entry:
            ma_frames[tf_key] = entry[1]
            any_found = True
        else:
            ma_frames[tf_key] = None
    ma_frames['D'] = _daily_cache.get(sym)
    if any_found or ma_frames['D'] is not None:
        return _calc_ma_table(price, ma_frames)
    # Fallback: cached ma_rows from enrichment
    return _enrichment.get(sym, {}).get('ma_rows')


def _render_ma_overlay(ax, ma_rows: list[dict], current_price: float,
                       ma_type: str, x_start: float, y_start: float):
    """Render a compact MA table overlay on the chart axes.

    ``ma_type``: 'sma' or 'ema' — which column to display.
    ``x_start``, ``y_start``: top-left corner in axes coordinates.
    """
    green, red, grey = '#26a69a', '#ef5350', '#555'
    periods = [9, 20, 50, 100, 200]
    tf_order = ['1m', '5m', '15m', '30m', '1h', 'D']

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


def generate_alert_chart(sym: str, df: pd.DataFrame, all_levels: list[float],
                         current_price: float,
                         ratio_map: dict | None = None,
                         alert_title: str = "",
                         stock_info: dict | None = None,
                         ma_rows: list[dict] | None = None,
                         fib_anchor: tuple | None = None) -> Path | None:
    """Generate 1-min Japanese candlestick chart (48h) with Fibonacci levels.

    Uses matplotlib Rectangle patches for proper candle bodies and thin
    wicks — clean Japanese candlestick style.
    alert_title: shown as the chart title (alert reason + stock info).
    stock_info: dict with vol, float, inst_own, insider_own etc.
    ma_rows: list of {tf, period, sma, ema} for MA confluence overlay.
    fib_anchor: (anchor_low, anchor_high, anchor_date) for fib origin display.

    Returns path to saved PNG or None on failure.
    """
    from matplotlib.patches import Rectangle

    try:
        if len(df) < 10:
            return None

        n_bars = len(df)
        right_padding = max(15, n_bars // 10)

        fig, ax = plt.subplots(figsize=(18, 9), facecolor='#0e1117')
        ax.set_facecolor('#0e1117')

        dates = pd.to_datetime(df['date']) if 'date' in df.columns else df.index

        # Adaptive sizing based on bar count
        if n_bars > 1200:
            body_w = 0.9
            wick_w = 0.4
        elif n_bars > 600:
            body_w = 0.8
            wick_w = 0.5
        elif n_bars > 300:
            body_w = 0.7
            wick_w = 0.6
        else:
            body_w = 0.6
            wick_w = 0.7

        # ── Japanese candlesticks via Rectangle patches ──
        for i, (_, row) in enumerate(df.iterrows()):
            o, h, l, c = row['open'], row['high'], row['low'], row['close']
            color = '#26a69a' if c >= o else '#ef5350'

            # Wick (thin line from low to high)
            ax.plot([i, i], [l, h], color=color, linewidth=wick_w,
                    solid_capstyle='round')

            # Body (filled rectangle)
            body_bottom = min(o, c)
            body_height = abs(c - o)
            if body_height < (h - l) * 0.01:
                # Doji — thin horizontal line
                body_height = max(abs(h - l) * 0.02, 0.0001)
                body_bottom = (o + c) / 2 - body_height / 2

            rect = Rectangle((i - body_w / 2, body_bottom), body_w, body_height,
                              facecolor=color, edgecolor=color, linewidth=0.3)
            ax.add_patch(rect)

        # ── Relevant fib levels: nearest 5 below + 5 above current price ──
        fibs_below = sorted([lv for lv in all_levels if lv <= current_price])[-5:]
        fibs_above = sorted([lv for lv in all_levels if lv > current_price])[:5]
        relevant_fibs = fibs_below + fibs_above

        # ── Y-axis: current price EXACTLY at vertical center ──
        price_low = df['low'].min()
        fib_top = fibs_above[min(2, len(fibs_above) - 1)] if fibs_above else current_price * 1.15
        dist_below = current_price - price_low
        dist_above = fib_top - current_price
        half_range = max(dist_below, dist_above, current_price * 0.08) * 1.10
        vis_min = max(0, current_price - half_range)
        vis_max = current_price + half_range

        visible_levels = [lv for lv in relevant_fibs if vis_min <= lv <= vis_max]

        price_span = vis_max - vis_min

        # ── Fibonacci levels ──
        _def_clr = '#888888'
        min_label_gap = price_span * 0.030
        last_y_r = -999.0
        last_y_l = -999.0

        for lv in visible_levels:
            info = ratio_map.get(round(lv, 4)) if ratio_map else None
            if isinstance(info, tuple):
                ratio, series = info
            elif info is not None:
                ratio, series = info, "S1"
            else:
                ratio, series = None, "S1"

            clr = FIB_LEVEL_COLORS.get(ratio, _def_clr) if ratio is not None else _def_clr
            ax.axhline(y=lv, color=clr, linewidth=0.6, alpha=0.55, linestyle='-',
                       xmin=0, xmax=n_bars / (n_bars + right_padding))

            lbl = f'{ratio}  ${lv:.4f}' if ratio is not None else f'${lv:.4f}'

            if series == "S2":
                if abs(lv - last_y_l) < min_label_gap:
                    continue
                ax.text(-0.5, lv, f'{lbl} ', color=clr,
                        fontsize=7, va='center', ha='right', fontweight='bold')
                last_y_l = lv
            else:
                if abs(lv - last_y_r) < min_label_gap:
                    continue
                ax.text(n_bars + 1, lv, f' {lbl}', color=clr,
                        fontsize=7, va='center', ha='left', fontweight='bold')
                last_y_r = lv

        # ── Current price line (gold, dashed) ──
        ax.axhline(y=current_price, color='#FFD700', linewidth=1.8,
                    linestyle='--', alpha=0.9)
        ax.text(n_bars + right_padding - 1, current_price,
                f'  ${current_price:.2f}  ',
                color='#0e1117', fontsize=10, va='center', ha='right',
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3',
                          facecolor='#FFD700', edgecolor='#FFD700', alpha=0.95))

        # ── X-axis date labels ──
        tick_step = max(1, n_bars // 12)
        tick_pos = list(range(0, n_bars, tick_step))
        tick_lbl = []
        date_list = list(dates)
        for pos in tick_pos:
            d = date_list[pos]
            tick_lbl.append(d.strftime('%m/%d %H:%M') if hasattr(d, 'strftime') else str(d)[:11])
        ax.set_xticks(tick_pos)
        ax.set_xticklabels(tick_lbl, color='#888', fontsize=7, rotation=30, ha='right')

        # ── Styling ──
        ax.set_ylim(vis_min, vis_max)
        ax.set_xlim(-2, n_bars + right_padding)
        ax.tick_params(colors='#888', labelsize=8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('#333')
        ax.spines['left'].set_color('#333')
        # ── Title: 3 lines — English sym/price, Hebrew alert, stock info ──
        # Line 1: SYM $price (pure English — no bidi issues)
        title_line1 = f'{sym}  ${current_price:.2f}'

        # Line 2: Hebrew alert type + detail (bidi per segment)
        title_line2 = ''
        if alert_title:
            clean = re.sub(r'<[^>]+>', '', alert_title).strip()
            clean = re.sub(
                r'[\U00010000-\U0010ffff\u2600-\u27bf\u2300-\u23ff'
                r'\ufe00-\ufe0f\u200d\u20e3]', '', clean).strip()
            alert_lines = clean.split('\n')
            # Process each line: apply bidi to Hebrew segments only
            bidi_parts = []
            for aline in alert_lines[:2]:  # max 2 lines from alert
                aline = aline.strip()
                if not aline:
                    continue
                # Split on ' — ' to separate Hebrew from SYM/price
                if ' — ' in aline:
                    segments = aline.split(' — ')
                    bidi_segs = []
                    for seg in segments:
                        seg = seg.strip()
                        # Skip segments that are just the symbol+price (already in line1)
                        if sym in seg:
                            continue
                        # Apply bidi to segments containing Hebrew chars
                        if re.search(r'[\u0590-\u05FF]', seg):
                            bidi_segs.append(bidi_display(seg))
                        else:
                            bidi_segs.append(seg)
                    if bidi_segs:
                        bidi_parts.append(' — '.join(bidi_segs))
                else:
                    # Whole line — apply bidi if has Hebrew
                    if re.search(r'[\u0590-\u05FF]', aline):
                        bidi_parts.append(bidi_display(aline))
                    else:
                        bidi_parts.append(aline)
            title_line2 = ' | '.join(bidi_parts)

        # Add daily % to title if available
        si = stock_info or {}
        if si.get('pct'):
            title_line1 += f'  ({si["pct"]:+.1f}%)'

        # Compose title (2 lines — details moved to overlay)
        parts = [title_line1]
        if title_line2:
            parts.append(title_line2)
        chart_title = '\n'.join(parts)
        ax.set_title(chart_title, color='white', fontsize=13, fontweight='bold', pad=12)
        ax.grid(axis='y', color='#222', linewidth=0.3, alpha=0.5)

        # ── RIGHT overlay: fundamentals (top-right) ──
        right_lines = []
        if si.get('float_str') and si['float_str'] != '-':
            flt = f"Float: {si['float_str']}"
            if si.get('float_dollar'):
                flt += f"  ({si['float_dollar']})"
            right_lines.append((flt, '#cccccc'))
        if si.get('vol_shares'):
            vol = f"Vol: {si['vol_shares']}"
            if si.get('vol_dollar'):
                vol += f"  ({si['vol_dollar']})"
            # RVOL
            rvol = si.get('rvol', 0)
            if rvol and rvol >= 1.5:
                vol += f"  x{rvol:.1f}"
            right_lines.append((vol, '#cccccc'))
        # Float Turnover
        ft_pct = si.get('float_turnover', 0)
        if ft_pct and ft_pct > 0:
            ft_clr = '#ff4444' if ft_pct >= 100 else '#ff9800' if ft_pct >= 50 else '#cccccc'
            right_lines.append((f"Turnover: {ft_pct:.0f}% of float", ft_clr))
        if si.get('short') and si['short'] != '-':
            right_lines.append((f"Short: {si['short']}", '#ff6666'))
        if si.get('cash') and si['cash'] != '-':
            right_lines.append((f"Cash: {si['cash']}/sh", '#88cc88'))
        inst_parts = []
        if si.get('inst_own') and si['inst_own'] != '-':
            inst_parts.append(f"Inst: {si['inst_own']}")
        if si.get('insider_own') and si['insider_own'] != '-':
            inst_parts.append(f"Insider: {si['insider_own']}")
        if inst_parts:
            right_lines.append((' | '.join(inst_parts), '#aaaaaa'))

        # ── Fib anchor info ──
        if fib_anchor:
            a_low, a_high, a_date = fib_anchor
            date_str = a_date if isinstance(a_date, str) else str(a_date)[:10]
            right_lines.append(('', '#333'))  # separator
            a_low_s = f"${a_low:.2f}" if a_low >= 1 else f"${a_low:.4f}"
            a_high_s = f"${a_high:.2f}" if a_high >= 1 else f"${a_high:.4f}"
            right_lines.append((f"Fib: {a_low_s} -> {a_high_s} ({date_str})", '#FFD700'))

        # Render right overlay (top-right, right-aligned)
        if right_lines:
            y_pos_r = 0.97
            line_height = 0.027
            for text, color in right_lines:
                if not text:
                    y_pos_r -= line_height * 0.5  # half-space for separator
                    continue
                ax.text(0.99, y_pos_r, text,
                        transform=ax.transAxes, fontsize=7.5, color=color,
                        verticalalignment='top', horizontalalignment='right',
                        fontfamily='monospace',
                        bbox=dict(boxstyle='square,pad=0.15', facecolor='#0e1117',
                                  edgecolor='none', alpha=0.80))
                y_pos_r -= line_height

        # ── LEFT overlay: VWAP + fib table (top-left) ──
        left_lines = []

        # VWAP (green = price above, red = price below)
        vwap_val = si.get('vwap', 0)
        if vwap_val and vwap_val > 0:
            vwap_pct = (current_price - vwap_val) / vwap_val * 100
            vp = f"${vwap_val:.2f}" if vwap_val >= 1 else f"${vwap_val:.4f}"
            vwap_clr = '#26a69a' if current_price > vwap_val else '#ef5350'
            left_lines.append((f"VWAP {vp}  {vwap_pct:+.1f}%", vwap_clr))
            left_lines.append(('', '#333'))

        # ── Build MA lookup for confluence labels ──
        # Filter: skip 1m (too noisy) and only keep significant periods per TF
        _TF_KEEP = {'5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h', 'D': 'D'}
        _TF_ORDER = {'5m': 0, '15m': 1, '30m': 2, '1h': 3, 'D': 4}
        _MIN_PERIOD = {'5m': 20, '15m': 9, '30m': 9, '1h': 9, 'D': 9}
        ma_vals: list[tuple[float, str, str, int]] = []  # (value, tf, type, period)
        if ma_rows:
            for r in ma_rows:
                tf_s = _TF_KEEP.get(r['tf'])
                if not tf_s:
                    continue
                # Skip small periods on short timeframes
                if r['period'] < _MIN_PERIOD.get(tf_s, 9):
                    continue
                for ma_type, key in [('S', 'sma'), ('E', 'ema')]:
                    val = r.get(key)
                    if val and val > 0:
                        ma_vals.append((val, tf_s, ma_type, r['period']))

        # ── Fib table: above (descending) + price + below ──
        above_sorted = sorted([lv for lv in relevant_fibs if lv > current_price], reverse=True)[:10]
        below_sorted = sorted([lv for lv in relevant_fibs if lv <= current_price], reverse=True)[:5]

        # Build anchor list for MA bucketing
        anchors = sorted(set(above_sorted + below_sorted + [current_price]), reverse=True)
        fib_lo = below_sorted[-1] if below_sorted else current_price
        fib_hi = above_sorted[0] if above_sorted else current_price
        # Bucket MAs between adjacent anchors
        buckets: dict[float, list[tuple[str, str, int]]] = {a: [] for a in anchors}
        for val, tf_s, ma_type, period in ma_vals:
            if val > fib_hi or val < fib_lo:
                continue
            # Find nearest anchor above
            best = None
            for a in anchors:
                if a >= val:
                    best = a
                else:
                    break
            if best is not None and best != current_price:
                buckets[best].append((tf_s, ma_type, period))

        def _compact_ma(items):
            from collections import OrderedDict
            by_tf = OrderedDict()
            for tf_s, mt, p in sorted(items, key=lambda x: (_TF_ORDER.get(x[0], 9), x[2])):
                by_tf.setdefault(tf_s, []).append(f"{mt}{p}")
            return ', '.join(f"{t}-{' '.join(ms)}" for t, ms in by_tf.items())

        def _fib_color(lv):
            info = ratio_map.get(round(lv, 4)) if ratio_map else None
            r = info[0] if isinstance(info, tuple) else info
            return FIB_LEVEL_COLORS.get(r, '#888888') if r is not None else '#888888'

        def _fib_label(lv):
            info = ratio_map.get(round(lv, 4)) if ratio_map else None
            r = info[0] if isinstance(info, tuple) else None
            s = info[1] if isinstance(info, tuple) else 'S1'
            pct = (lv - current_price) / current_price * 100
            p = f"${lv:.2f}" if lv >= 1 else f"${lv:.4f}"
            prefix = f"{r} " if r is not None else ""
            suffix = f" {s}" if s == "S2" else ""
            lbl = f"●  {prefix}{p}  {pct:+.1f}%{suffix}"
            # Append MA confluence
            ma_items = buckets.get(lv, [])
            if ma_items:
                lbl += f"  {_compact_ma(ma_items)}"
            return lbl

        for lv in above_sorted:
            left_lines.append((_fib_label(lv), _fib_color(lv)))

        # Current price separator
        p_str = f"${current_price:.2f}" if current_price >= 1 else f"${current_price:.4f}"
        left_lines.append((f"━━  {p_str}  ━━", '#FFD700'))

        for lv in below_sorted:
            left_lines.append((_fib_label(lv), _fib_color(lv)))

        # Render left overlay (top-left, stacked vertically)
        if left_lines:
            y_pos = 0.97
            line_height = 0.027
            for text, color in left_lines:
                if not text:
                    y_pos -= line_height * 0.5  # half-space for separator
                    continue
                ax.text(0.01, y_pos, text,
                        transform=ax.transAxes, fontsize=7.5, color=color,
                        verticalalignment='top', fontfamily='monospace',
                        bbox=dict(boxstyle='square,pad=0.15', facecolor='#0e1117',
                                  edgecolor='none', alpha=0.80))
                y_pos -= line_height

        out = Path(f'/tmp/alert_chart_{sym}.png')
        fig.savefig(out, dpi=130, bbox_inches='tight',
                    facecolor='#0e1117', edgecolor='none')
        plt.close(fig)
        log.info(f"Alert chart saved: {out}")
        return out

    except Exception as e:
        log.error(f"generate_alert_chart {sym}: {e}")
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
    # Reduced from 8 to 5 timeframes to cut IBKR requests per stock report
    ma_frames: dict[str, pd.DataFrame | None] = {}
    _tf_specs = [
        ('1m',  '1 min',   '2 D'),
        ('5m',  '5 mins',  '5 D'),
        ('15m', '15 mins', '2 W'),
        ('30m', '30 mins', '1 M'),
        ('1h',  '1 hour',  '3 M'),
    ]
    for tf_key, bar_size, duration in _tf_specs:
        ma_frames[tf_key] = _download_intraday(sym, bar_size=bar_size, duration=duration)
    ma_frames['D'] = _daily_cache.get(sym)

    ma_rows = _calc_ma_table(price, ma_frames)
    # Cache ma_rows in enrichment so other alerts (news, batch) can use them
    if sym in _enrichment:
        _enrichment[sym]['ma_rows'] = ma_rows
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
    vol_raw = stock.get('volume_raw', 0)
    dol_vol = vol_raw * price if vol_raw and price else 0
    vol_shares_str = _format_shares_short(vol_raw) if vol_raw > 0 else '-'
    dol_vol_str = _format_dollar_short(dol_vol) if dol_vol > 0 else '-'
    rvol = stock.get('rvol', 0)
    rvol_tag = f"  פי {rvol:.1f} מהרגיל" if rvol > 0 else ""
    lines = [
        f"🆕 <b>{sym}</b> — ${price:.2f}  {stock['pct']:+.1f}%  ווליום {vol_shares_str} ({dol_vol_str}){rvol_tag}",
    ]

    # Warn if stock is NOT tradeable on IBKR
    contract = stock.get('contract')
    if not contract or not getattr(contract, 'conId', 0):
        lines.append("")
        lines.append("⛔ <b>לא נסחרת באינטראקטיב ברוקרס!</b>")

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
    # Float — shares + dollar value
    float_str = enriched['float']
    float_shares = _parse_float_to_shares(float_str)
    float_dol_str = f" ({_format_dollar_short(float_shares * price)})" if float_shares > 0 and price > 0 else ""
    lines.append(f"📊 מניות פניות: {float_str}{float_dol_str} | שורט: {enriched['short']}")
    lines.append(f"💰 {eps_icon} EPS: {eps} | Cash: ${enriched['cash']}")
    lines.append(f"📅 Earnings: {enriched['earnings']}")
    # Volume — shares + dollar value
    fvz_vol_str = enriched.get('fvz_volume', '-')
    avg_vol_str = enriched.get('avg_volume', '-')
    lines.append(f"📉 ווליום: {vol_shares_str} ({dol_vol_str}) | ממוצע: {avg_vol_str}")
    # Float turnover
    if float_shares > 0 and vol_raw > 0:
        turnover_ratio = vol_raw / float_shares
        if turnover_ratio >= 1.0:
            lines.append(f"🚀 סיבוב מניות פניות: x{turnover_ratio:.1f} — כל המניות הפניות עברו מיד ליד!")
        elif turnover_ratio >= 0.5:
            lines.append(f"⚡ סיבוב מניות פניות: {turnover_ratio:.0%} — חצי מהמניות עברו")
        elif turnover_ratio >= 0.2:
            lines.append(f"📈 סיבוב מניות פניות: {turnover_ratio:.0%}")
    lines.append(f"📊 Volatility: W {enriched.get('vol_w', '-')} | M {enriched.get('vol_m', '-')}")
    lines.append(f"🎯 52W: ↑${enriched.get('52w_high', '-')} | ↓${enriched.get('52w_low', '-')}")

    # VWAP line
    vwap = stock.get('vwap', 0)
    if vwap > 0:
        above = price >= vwap
        if above:
            lines.append(f"🟢 VWAP: ${vwap:.2f} — מחיר מעל ל-VWAP")
        else:
            vwap_dist = (vwap - price) / price * 100
            lines.append(f"🔴 VWAP: ${vwap:.2f} — מחיר מתחת ל-VWAP ({vwap_dist:.1f}% מתחת)")

    # Relative Strength vs SPY
    spy_pct = _get_spy_daily_change()
    if spy_pct < -0.3 and stock.get('pct', 0) > 0 and rvol >= 1.5:
        lines.append(f"💪 <b>חזקה מהשוק!</b> SPY {spy_pct:+.1f}% | המניה {stock['pct']:+.1f}% בווליום גבוה")

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


def _send_unified_report(sym: str, stock: dict, enriched: dict,
                          alert_reason: str = "",
                          alert_tags: list[str] | None = None) -> None:
    """Send a single photo message with intraday chart + complete Hebrew caption.

    Replaces the old two-message flow (_send_stock_report text + _send_alert_chart image).
    The chart includes fib levels, news headlines, fib anchor info as overlays.
    Caption is a compact Hebrew summary with all key details (≤1024 chars).
    """
    price = stock.get('price', 0)
    if price <= 0:
        return

    # ── 1-min bars, 2 days (extended hours) ──
    df = _download_intraday(sym, bar_size='1 min', duration='2 D')
    if df is None or len(df) < 20:
        return

    # ── Fib levels (uses cached daily data + fib cache) ──
    with _fib_cache_lock:
        cached = _fib_cache.get(sym)
    if cached:
        anchor_low, anchor_high, all_levels, ratio_map, anchor_date = cached
    else:
        calc_fib_levels(sym, price)
        with _fib_cache_lock:
            cached = _fib_cache.get(sym)
        if cached:
            anchor_low, anchor_high, all_levels, ratio_map, anchor_date = cached
        else:
            anchor_low, anchor_high, all_levels, ratio_map, anchor_date = 0, 0, [], {}, ''

    # ── Build stock_info for chart overlay ──
    enrich = enriched or _enrichment.get(sym, {})
    vol_raw = stock.get('volume_raw', 0)
    dol_vol = vol_raw * price if vol_raw and price else 0
    float_str = enrich.get('float', '-')
    float_shares = _parse_float_to_shares(float_str)
    rvol = stock.get('rvol', 0)
    ft_pct = (vol_raw / float_shares * 100) if float_shares > 0 and vol_raw > 0 else 0
    si = {
        'float_str': float_str,
        'float_dollar': _format_dollar_short(float_shares * price) if float_shares > 0 and price > 0 else '',
        'vol_shares': _format_shares_short(vol_raw) if vol_raw > 0 else '',
        'vol_dollar': _format_dollar_short(dol_vol) if dol_vol > 0 else '',
        'vol_str': _format_dollar_short(dol_vol) if dol_vol > 0 else (_format_shares_short(vol_raw) if vol_raw > 0 else ''),
        'short': enrich.get('short', '-'),
        'cash': enrich.get('cash', '-'),
        'inst_own': enrich.get('inst_own', '-'),
        'insider_own': enrich.get('insider_own', '-'),
        'vwap': stock.get('vwap', 0),
        'pct': stock.get('pct', 0),
        'rvol': rvol,
        'float_turnover': ft_pct,
    }
    # Replace Hebrew with English for chart rendering (monospace font)
    for key in ('float_dollar', 'vol_shares', 'vol_dollar', 'vol_str'):
        v = si.get(key, '')
        if v:
            si[key] = v.replace('מליון', 'M').replace('אלף', 'K')

    # ── Compute MAs from 1-min bars (resample to higher timeframes) ──
    ma_data = None
    try:
        if df is not None and len(df) > 20 and 'date' in df.columns:
            df_ts = df.copy()
            df_ts.index = pd.to_datetime(df_ts['date'])
            ma_frames = {'1m': df}
            for tf_key, rule in [('5m', '5min'), ('15m', '15min'),
                                 ('30m', '30min'), ('1h', '1h')]:
                resampled = df_ts.resample(rule).agg({
                    'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
                }).dropna()
                if len(resampled) > 0:
                    ma_frames[tf_key] = resampled
            ma_frames['D'] = _daily_cache.get(sym)
            ma_data = _calc_ma_table(price, ma_frames)
    except Exception as e:
        log.debug(f"MA calc for unified report {sym}: {e}")

    # ── Fib anchor tuple for chart overlay ──
    fib_anchor = None
    if anchor_low and anchor_high:
        fib_anchor = (anchor_low, anchor_high, anchor_date)

    # ── Generate chart ──
    chart_path = generate_alert_chart(sym, df, all_levels, price, ratio_map,
                                      alert_title=alert_reason,
                                      stock_info=si, ma_rows=ma_data,
                                      fib_anchor=fib_anchor)
    if not chart_path:
        return

    # ── Build compact Hebrew caption (≤1024 chars) ──
    pct = stock.get('pct', 0)
    vol_shares_str = _format_shares_short(vol_raw) if vol_raw > 0 else '-'
    dol_vol_str = _format_dollar_short(dol_vol) if dol_vol > 0 else '-'
    rvol_tag = f"פי {rvol:.1f}" if rvol > 0 else ""

    caption_lines = []

    # Line 1: Header with alert trigger reason
    tags_str = " + ".join(alert_tags) if alert_tags else "חדשה"
    caption_lines.append(f"🆕 <b>{sym}</b> ${price:.2f} {pct:+.1f}% | {tags_str}")

    # Line 2: Float + Short
    float_dol = _format_dollar_short(float_shares * price) if float_shares > 0 and price > 0 else ''
    flt_part = f"פלאט {float_str}"
    if float_dol:
        flt_part += f" ({float_dol})"
    short_str = enrich.get('short', '-')
    if short_str and short_str != '-':
        flt_part += f" | שורט {short_str}"
    caption_lines.append(f"📊 {flt_part}")

    # Line 3: Volume + RVOL + Turnover
    vol_part = f"ווליום {dol_vol_str}"
    if rvol_tag:
        vol_part += f" ({rvol_tag})"
    if ft_pct >= 20:
        vol_part += f" | סיבוב x{ft_pct / 100:.2f}"
    caption_lines.append(f"📈 {vol_part}")

    # Line 4: VWAP
    vwap = stock.get('vwap', 0)
    if vwap > 0:
        vwap_pct = (price - vwap) / vwap * 100
        vwap_icon = "🟢" if price > vwap else "🔴"
        vp = f"${vwap:.2f}" if vwap >= 1 else f"${vwap:.4f}"
        caption_lines.append(f"{vwap_icon} VWAP {vp} ({vwap_pct:+.1f}%)")

    # Line 5: Company | Sector | Country
    company = enrich.get('company', '')
    sector = enrich.get('sector', '')
    country = enrich.get('country', '')
    co_parts = [p for p in [company, sector, country] if p and p != '-']
    if co_parts:
        caption_lines.append(f"🏢 {' | '.join(co_parts)}")

    # Line 6: MCap + EPS + Cash
    mcap = enrich.get('market_cap', '-')
    eps = enrich.get('eps', '-')
    cash = enrich.get('cash', '-')
    fin_parts = []
    if mcap and mcap != '-':
        fin_parts.append(f"MCap {mcap}")
    if eps and eps != '-':
        fin_parts.append(f"EPS {eps}")
    if cash and cash != '-':
        fin_parts.append(f"Cash ${cash}")
    if fin_parts:
        caption_lines.append(f"💰 {' | '.join(fin_parts)}")

    # News headlines (up to 3, Hebrew, Israel timezone)
    news_items = enrich.get('news', [])
    if news_items:
        headlines_he = []
        headlines_en = []
        for n in news_items[:3]:
            title_he = n.get('title_he', '')
            title_en = n.get('title_en', '')
            ndate = n.get('date', '')
            if title_he:
                headlines_he.append((ndate, title_he))
            elif title_en:
                headlines_en.append((ndate, title_en))
        if headlines_en:
            try:
                translated = _batch_translate([t for _, t in headlines_en])
                for i, tr in enumerate(translated):
                    headlines_he.append((headlines_en[i][0], tr))
            except Exception:
                for d, t in headlines_en:
                    headlines_he.append((d, t))
        for ndate, title in headlines_he[:3]:
            # Convert ET → Israel time
            il_date = _et_to_israel(ndate)
            prefix = f"{il_date} — " if il_date else ""
            caption_lines.append(f"📰 {prefix}{title}")

    # Line 9: Fib anchor
    if fib_anchor:
        a_low, a_high, a_date = fib_anchor
        a_low_s = f"${a_low:.2f}" if a_low >= 1 else f"${a_low:.4f}"
        a_high_s = f"${a_high:.2f}" if a_high >= 1 else f"${a_high:.4f}"
        date_str = a_date if isinstance(a_date, str) else str(a_date)[:10]
        caption_lines.append(f"📐 פיבו: {a_low_s}→{a_high_s} ({date_str})")

    # Line 10: Fib supports/resistances (nearest 2 each)
    fibs_below = sorted([lv for lv in all_levels if lv <= price], reverse=True)[:2]
    fibs_above = sorted([lv for lv in all_levels if lv > price])[:2]
    if fibs_below or fibs_above:
        s_parts = ' '.join(f"${lv:.2f}" if lv >= 1 else f"${lv:.4f}" for lv in fibs_below) if fibs_below else '-'
        r_parts = ' '.join(f"${lv:.2f}" if lv >= 1 else f"${lv:.4f}" for lv in fibs_above) if fibs_above else '-'
        caption_lines.append(f"🎯 S: {s_parts} | R: {r_parts}")

    # Line 11: Closest resist MA
    if ma_data:
        resist_str = _find_closest_resist(price, ma_data)
        if resist_str:
            caption_lines.append(f"📉 Resist: {resist_str}")

    # IBKR tradeability warning
    contract = stock.get('contract')
    if not contract or not getattr(contract, 'conId', 0):
        caption_lines.append("⛔ לא נסחרת באינטראקטיב ברוקרס!")

    caption = "\n".join(caption_lines)
    if len(caption) > 1024:
        # Trim news lines first, then truncate
        while len(caption) > 1024 and len(caption_lines) > 5:
            caption_lines.pop(-2)  # remove before last line
            caption = "\n".join(caption_lines)
        if len(caption) > 1024:
            caption = caption[:1020] + "..."

    send_telegram_alert_photo(chart_path, caption)
    # Also send to private chat with trading buttons
    buttons = _make_trading_buttons(sym)
    _tg_sender.enqueue(_do_send_photo, CHAT_ID, str(chart_path), caption,
                        None, buttons)
    log.info(f"Unified report sent for {sym}")


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
_INTRADAY_CACHE_TTL = 300  # 5 minutes


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
#  Telegram — Background Sender Thread
# ══════════════════════════════════════════════════════════

_TG_MAX_LEN = 4000  # Telegram limit is 4096, keep margin for safety
_TG_SEND_TIMEOUT = 8       # seconds for text messages
_TG_PHOTO_TIMEOUT = 12     # seconds for photo uploads
_TG_QUEUE_MAX = 200        # max queued messages before dropping oldest


class _TelegramSender(threading.Thread):
    """Background thread that drains a queue and sends Telegram messages.

    All public send_* functions enqueue work here so the scanner thread
    is never blocked by slow/failed Telegram API calls.
    """

    def __init__(self):
        super().__init__(daemon=True, name="TelegramSender")
        self._q: queue.Queue = queue.Queue(maxsize=_TG_QUEUE_MAX)

    # ── Public (thread-safe) ──

    def enqueue(self, func, *args, **kwargs):
        """Put a send job on the queue. Drops oldest if full."""
        try:
            self._q.put_nowait((func, args, kwargs))
        except queue.Full:
            try:
                self._q.get_nowait()           # drop oldest
            except queue.Empty:
                pass
            try:
                self._q.put_nowait((func, args, kwargs))
            except queue.Full:
                pass

    # ── Thread loop ──

    def run(self):
        while True:
            try:
                func, args, kwargs = self._q.get()
                func(*args, **kwargs)
            except Exception as e:
                log.error(f"TelegramSender error: {e}")


_tg_sender = _TelegramSender()
_tg_sender.start()


# ── Low-level send helpers (run INSIDE _TelegramSender thread) ──

def _do_send_message(chat_id: str, text: str, reply_markup: dict | None = None,
                     reply_to: int | None = None) -> bool:
    """Actually POST a text message to Telegram (blocking)."""
    try:
        payload: dict = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        if reply_to:
            payload['reply_to_message_id'] = reply_to
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json=payload, timeout=_TG_SEND_TIMEOUT,
        )
        if not resp.ok:
            log.warning(f"Telegram send failed ({chat_id}): {resp.status_code}")
        return resp.ok
    except Exception as e:
        log.error(f"Telegram ({chat_id}): {e}")
        return False


def _do_send_photo(chat_id: str, image_path: str | Path, caption: str = "",
                   reply_to: int | None = None,
                   reply_markup: dict | None = None) -> bool:
    """Actually POST a photo to Telegram (blocking)."""
    try:
        data: dict = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'HTML'}
        if reply_to:
            data['reply_to_message_id'] = reply_to
        if reply_markup:
            data['reply_markup'] = json.dumps(reply_markup)
        with open(image_path, 'rb') as photo:
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                data=data, files={'photo': photo}, timeout=_TG_PHOTO_TIMEOUT,
            )
        if not resp.ok:
            log.warning(f"Telegram photo failed ({chat_id}): {resp.status_code}")
        return resp.ok
    except Exception as e:
        log.error(f"Telegram photo ({chat_id}): {e}")
        return False


# ── Public API (non-blocking — enqueue to background thread) ──

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


def send_telegram(text: str, reply_markup: dict | None = None) -> bool:
    """Send text to personal chat (non-blocking via background thread)."""
    if not BOT_TOKEN or not CHAT_ID:
        return False
    if len(text) > _TG_MAX_LEN:
        chunks = _split_telegram_message(text)
        for i, chunk in enumerate(chunks):
            rm = reply_markup if i == len(chunks) - 1 else None
            _tg_sender.enqueue(_do_send_message, CHAT_ID, chunk, rm)
    else:
        _tg_sender.enqueue(_do_send_message, CHAT_ID, text, reply_markup)
    return True


def send_telegram_alert(text: str, reply_markup: dict | None = None) -> bool:
    """Send alert to group chat (non-blocking via background thread)."""
    if not BOT_TOKEN or not GROUP_CHAT_ID:
        return False
    if len(text) > _TG_MAX_LEN:
        chunks = _split_telegram_message(text)
        for i, chunk in enumerate(chunks):
            rm = reply_markup if i == len(chunks) - 1 else None
            _tg_sender.enqueue(_do_send_message, GROUP_CHAT_ID, chunk, rm)
    else:
        _tg_sender.enqueue(_do_send_message, GROUP_CHAT_ID, text, reply_markup)
    return True


def send_telegram_alert_photo(image_path: Path, caption: str = "") -> bool:
    """Send a photo alert to group chat (non-blocking)."""
    if not BOT_TOKEN or not GROUP_CHAT_ID:
        return False
    _tg_sender.enqueue(_do_send_photo, GROUP_CHAT_ID, str(image_path), caption)
    return True


def _make_lookup_button(sym: str) -> dict:
    """Build InlineKeyboardMarkup with Report + TradingView buttons."""
    return {
        'inline_keyboard': [[
            {'text': f'📊 דוח מלא — {sym}', 'callback_data': f'lookup:{sym}'},
            {'text': f'📈 TradingView', 'url': f'https://www.tradingview.com/chart/?symbol={sym}'},
        ]]
    }


def _make_trading_buttons(sym: str) -> dict:
    """Build InlineKeyboardMarkup with BUY / SELL / CLOSE ALL buttons."""
    return {
        'inline_keyboard': [
            [
                {'text': f'🟢 BUY {sym}', 'callback_data': f'buy:{sym}'},
                {'text': f'🔴 SELL {sym}', 'callback_data': f'sell:{sym}'},
            ],
            [
                {'text': '❌ סגור הכל', 'callback_data': 'closeall'},
                {'text': f'📊 דוח — {sym}', 'callback_data': f'lookup:{sym}'},
            ],
        ]
    }


def send_telegram_photo(image_path: Path, caption: str = "") -> bool:
    """Send a photo to private chat (non-blocking)."""
    if not BOT_TOKEN or not CHAT_ID:
        return False
    _tg_sender.enqueue(_do_send_photo, CHAT_ID, str(image_path), caption)
    return True


def send_telegram_to(chat_id: str, text: str, reply_to: int | None = None) -> bool:
    """Send text to a specific chat (non-blocking)."""
    if not BOT_TOKEN:
        return False
    _tg_sender.enqueue(_do_send_message, chat_id, text, None, reply_to)
    return True


def send_telegram_photo_to(chat_id: str, image_path: Path, caption: str = "",
                           reply_to: int | None = None) -> bool:
    """Send photo to a specific chat (non-blocking)."""
    if not BOT_TOKEN:
        return False
    _tg_sender.enqueue(_do_send_photo, chat_id, str(image_path), caption, reply_to)
    return True


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
    """Poll Telegram getUpdates for /stock commands, @mentions, and trading buttons."""

    _COOLDOWN_SECS = 60
    _ORDER_COOLDOWN = 5  # seconds between buy/sell clicks

    def __init__(self, lookup_queue: queue.Queue, order_thread=None):
        super().__init__(daemon=True)
        self.lookup_queue = lookup_queue
        self.order_thread = order_thread  # OrderThread for buy/sell/close
        self.running = False
        self._offset = 0
        self._bot_username: str = ""
        self._cooldowns: dict[str, float] = {}  # symbol -> last lookup time
        self._order_cooldowns: dict[str, float] = {}  # "action:sym" -> last time

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

    def _answer_cb(self, cb_id: str, text: str):
        """Answer callback query to remove loading spinner."""
        try:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
                json={'callback_query_id': cb_id, 'text': text},
                timeout=5,
            )
        except Exception:
            pass

    def _handle_callback(self, cb: dict):
        """Handle inline keyboard callback (lookup, buy, sell, closeall)."""
        cb_id = cb.get('id', '')
        data = cb.get('data', '')
        chat_id = str(cb.get('message', {}).get('chat', {}).get('id', ''))
        message_id = cb.get('message', {}).get('message_id', 0)

        # Only allow orders from private chat
        if data.startswith(('buy:', 'sell:', 'closeall')) and chat_id != CHAT_ID:
            self._answer_cb(cb_id, '⛔ פקודות מסחר רק בצ\'אט פרטי')
            return

        if data.startswith('buy:'):
            self._answer_cb(cb_id, '🟢 מבצע קנייה...')
            self._handle_buy(data.split(':', 1)[1].upper(), chat_id)
            return

        if data.startswith('sell:'):
            self._answer_cb(cb_id, '🔴 מבצע מכירה...')
            self._handle_sell(data.split(':', 1)[1].upper(), chat_id)
            return

        if data == 'closeall':
            self._answer_cb(cb_id, '❌ סוגר הכל...')
            self._handle_close_all(chat_id)
            return

        # Default: lookup
        self._answer_cb(cb_id, '🔍 טוען דוח...')

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

    def _handle_buy(self, sym: str, chat_id: str):
        """Execute quick buy via OrderThread from Telegram button."""
        if not self.order_thread:
            send_telegram_to(chat_id, "⛔ מערכת ההזמנות לא מחוברת")
            return

        # Rate limit
        key = f"buy:{sym}"
        now = time_mod.time()
        if now - self._order_cooldowns.get(key, 0) < self._ORDER_COOLDOWN:
            send_telegram_to(chat_id, f"⏳ {sym} — המתן מספר שניות")
            return
        self._order_cooldowns[key] = now

        # Get price from OrderThread (live subscription or positions)
        price = 0.0
        if self.order_thread._subscribed_sym == sym:
            price = self.order_thread.get_live_price()
        if price <= 0:
            pos = self.order_thread.positions.get(sym)
            if pos and len(pos) >= 3:
                price = pos[2]  # mktPrice
        if price <= 0:
            send_telegram_to(chat_id, f"⛔ אין מחיר עדכני ל-{sym}. בחר מניה בסורק קודם")
            return

        # Build order — same logic as _quick_buy (80% BP, 5% stop, 20% TP)
        nl = self.order_thread.net_liq
        bp = self.order_thread.buying_power
        if nl <= 0:
            send_telegram_to(chat_id, "⛔ ממתין לנתוני חשבון מ-IBKR")
            return
        avail = bp if bp > 0 else nl
        bp_pct = 0.80
        stop_pct = 0.05
        tp_pct = 0.20

        buy_price = round(price * 1.01, 2)
        qty = int(avail * bp_pct / buy_price)
        if qty <= 0:
            send_telegram_to(chat_id, "⛔ אין כוח קנייה מספיק")
            return

        stop_price = round(price * (1 - stop_pct), 2)
        limit_price = round(stop_price * (1 - STOP_LIMIT_OFFSET_PCT), 2)
        target_price = round(price * (1 + tp_pct), 2)

        req = {
            'sym': sym, 'action': 'BUY', 'qty': qty, 'price': buy_price,
            'stop_price': stop_price, 'limit_price': limit_price,
            'stop_desc': f"${stop_price:.2f} ({stop_pct*100:.0f}% stop)",
            'target_price': target_price, 'trailing_pct': 0.0,
        }
        self.order_thread.submit(req)
        send_telegram_to(
            chat_id,
            f"🟢 <b>BUY {qty} {sym}</b> @ ${buy_price:.2f}\n"
            f"🛑 Stop: ${stop_price:.2f} | 🎯 TP: ${target_price:.2f}"
        )
        log.info(f"Telegram BUY: {qty} {sym} @ ${buy_price:.2f}")

    def _handle_sell(self, sym: str, chat_id: str):
        """Execute quick sell via OrderThread from Telegram button."""
        if not self.order_thread:
            send_telegram_to(chat_id, "⛔ מערכת ההזמנות לא מחוברת")
            return

        # Rate limit
        key = f"sell:{sym}"
        now = time_mod.time()
        if now - self._order_cooldowns.get(key, 0) < self._ORDER_COOLDOWN:
            send_telegram_to(chat_id, f"⏳ {sym} — המתן מספר שניות")
            return
        self._order_cooldowns[key] = now

        pos = self.order_thread.positions.get(sym)
        if not pos or pos[0] == 0:
            send_telegram_to(chat_id, f"⛔ אין פוזיציה ב-{sym}")
            return

        qty = abs(pos[0])
        price = 0.0
        if len(pos) >= 3:
            price = pos[2]  # mktPrice from positions tuple
        if price <= 0 and self.order_thread._subscribed_sym == sym:
            price = self.order_thread.get_live_price()
        if price <= 0:
            send_telegram_to(chat_id, f"⛔ אין מחיר עדכני ל-{sym}")
            return

        sell_price = round(price * 0.99, 2)
        if sell_price <= 0:
            sell_price = 0.01

        req = {
            'sym': sym, 'action': 'SELL', 'qty': qty, 'price': sell_price,
            'cancel_existing': True,
        }
        self.order_thread.submit(req)
        send_telegram_to(
            chat_id,
            f"🔴 <b>SELL {qty} {sym}</b> @ ${sell_price:.2f}"
        )
        log.info(f"Telegram SELL: {qty} {sym} @ ${sell_price:.2f}")

    def _handle_close_all(self, chat_id: str):
        """Close all open positions via OrderThread from Telegram button."""
        if not self.order_thread:
            send_telegram_to(chat_id, "⛔ מערכת ההזמנות לא מחוברת")
            return

        positions = self.order_thread.positions
        if not positions:
            send_telegram_to(chat_id, "✅ אין פוזיציות פתוחות")
            return

        closed = []
        for sym, pos in positions.items():
            qty = abs(pos[0])
            if qty == 0:
                continue
            mkt_price = pos[2] if len(pos) >= 3 else 0
            if mkt_price <= 0:
                continue

            # Aggressive sell: price - min($0.50, 5%)
            offset = min(0.50, mkt_price * 0.05)
            sell_price = round(max(0.01, mkt_price - offset), 2)
            req = {
                'sym': sym, 'action': 'SELL', 'qty': qty, 'price': sell_price,
                'cancel_existing': True,
            }
            self.order_thread.submit(req)
            closed.append(f"{sym} {qty}sh @ ${sell_price:.2f}")

        if closed:
            lines = "\n".join(f"  🔴 {c}" for c in closed)
            send_telegram_to(
                chat_id,
                f"❌ <b>סוגר הכל — {len(closed)} פוזיציות</b>\n{lines}"
            )
            log.info(f"Telegram CLOSE ALL: {len(closed)} positions")
        else:
            send_telegram_to(chat_id, "✅ אין פוזיציות פתוחות")

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
    _STATE_PATH = DATA_DIR / "fib_dt_state.json"

    def __init__(self):
        self.cash: float = self.INITIAL_CASH
        self.positions: dict[str, dict] = {}
        # sym -> {qty, entry_price, stop, target, half, other_half, phase, entry_ts}
        # phase: IN_POSITION | TRAILING
        self.trades: list[dict] = []  # history
        self._exit_history: list[dict] = []  # {sym, ts, pnl} for cooldown
        self._daily_realized_pnl: float = 0.0
        self._daily_pnl_date: str = ""
        self._init_journal()
        self._load_state()

    def _save_state(self):
        """Persist portfolio state to disk (survives restarts)."""
        try:
            state = {'cash': self.cash, 'positions': self.positions}
            with open(self._STATE_PATH, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            log.warning(f"FIB DT state save error: {e}")

    def _load_state(self):
        """Load portfolio state from disk."""
        try:
            if not self._STATE_PATH.exists():
                return
            with open(self._STATE_PATH) as f:
                state = json.load(f)
            self.cash = state.get('cash', self.INITIAL_CASH)
            self.positions = state.get('positions', {})
            if self.positions:
                log.info(f"FIB DT state restored: cash=${self.cash:.0f}, "
                         f"{len(self.positions)} positions: "
                         + ", ".join(self.positions.keys()))
        except Exception as e:
            log.warning(f"FIB DT state load error: {e}")

    def _reset_daily_pnl(self):
        """Reset daily P&L tracker if new day."""
        today = datetime.now(_ET).strftime('%Y-%m-%d')
        if self._daily_pnl_date != today:
            self._daily_pnl_date = today
            self._daily_realized_pnl = 0.0
            self._exit_history = [e for e in self._exit_history
                                  if time_mod.time() - e['ts'] < 86400]

    def can_enter(self, sym: str) -> tuple[bool, str]:
        """Check if entry is allowed (max positions, cooldown, daily loss, min price)."""
        self._reset_daily_pnl()
        # Max positions
        if len(self.positions) >= FIB_DT_MAX_POSITIONS:
            return False, f"max positions ({FIB_DT_MAX_POSITIONS})"
        # Daily loss limit
        if self._daily_realized_pnl <= -STRATEGY_DAILY_LOSS_LIMIT:
            return False, f"daily loss limit (${self._daily_realized_pnl:.0f})"
        # Max entries per stock per day
        today = datetime.now(_ET).strftime('%Y-%m-%d')
        today_entries = sum(1 for e in self._exit_history if e['sym'] == sym
                           and datetime.fromtimestamp(e['ts'], _ET).strftime('%Y-%m-%d') == today)
        # Count current position too
        if sym in self.positions:
            today_entries += 1
        if today_entries >= STRATEGY_MAX_ENTRIES_PER_STOCK_PER_DAY:
            return False, f"max entries/day for {sym} ({STRATEGY_MAX_ENTRIES_PER_STOCK_PER_DAY})"
        # Cooldown
        now_ts = time_mod.time()
        for e in reversed(self._exit_history):
            if e['sym'] == sym:
                cooldown = (STRATEGY_REENTRY_COOLDOWN_AFTER_LOSS_SEC if e['pnl'] < 0
                            else STRATEGY_REENTRY_COOLDOWN_SEC)
                elapsed = now_ts - e['ts']
                if elapsed < cooldown:
                    remaining = int(cooldown - elapsed)
                    return False, f"cooldown {remaining}s for {sym}"
                break
        return True, ""

    def _record_exit(self, sym: str, pnl: float):
        """Record exit for cooldown/daily tracking."""
        self._reset_daily_pnl()
        self._daily_realized_pnl += pnl
        self._exit_history.append({'sym': sym, 'ts': time_mod.time(), 'pnl': pnl})

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
        if side != 'BUY':
            _send_trade_journal_entry('📐', 'FIB DT', sym, reason, qty,
                                      price, entry_price, pnl, net_liq,
                                      self.cash, self.trades)

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
            'entry_ts': time_mod.time(),
        }

        self.trades.append({
            'sym': sym, 'side': 'BUY', 'qty': qty,
            'price': price, 'pnl': 0, 'time': datetime.now(_ET),
        })

        net = self._net_liq_internal({})
        self._log_journal(sym, 'BUY', 'ENTRY', qty, price, price, 0, net)
        self._save_state()

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

                # Simulate stop-limit: sell at stop price, not gapped-down market price.
                # In reality a stop-limit order fires at the stop level during decline.
                # Only use actual price if it's above stop (normal fill).
                fill_price = max(price, pos['stop'])

                pnl = (fill_price - pos['entry_price']) * remaining
                pnl_pct = (fill_price / pos['entry_price'] - 1) * 100
                old_cash = self.cash
                self.cash += remaining * fill_price

                self.trades.append({
                    'sym': sym, 'side': 'SELL', 'qty': remaining,
                    'price': fill_price, 'pnl': pnl, 'time': datetime.now(_ET),
                })

                net_liq = self._net_liq_internal(current_prices, exclude=sym)
                self._log_journal(sym, 'SELL', f'STOP ({phase})', remaining, fill_price,
                                  pos['entry_price'], pnl, net_liq)

                slippage_note = f" [stop-limit fill, mkt=${price:.2f}]" if fill_price > price else ""
                alert = (
                    f"📐 <b>FIB DT [SIM] — STOP HIT {sym}</b>\n"
                    f"  🕐 {self._ts()}\n"
                    f"  🔴 {remaining}sh @ ${fill_price:.2f} (entry ${pos['entry_price']:.2f}){slippage_note}\n"
                    f"  📉 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
                    f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
                    f"  📊 Portfolio: ${net_liq:,.0f}"
                )
                alerts.append(alert)
                closed.append(sym)
                self._record_exit(sym, pnl)
                log.info(f"VIRTUAL STOP: {sym} {remaining}sh @ ${fill_price:.2f} (mkt=${price:.2f}) P&L=${pnl:+.2f}")
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
                self._save_state()
                log.info(f"VIRTUAL TARGET: {sym} {half}sh @ ${price:.2f} P&L=${pnl:+.2f}")

        for sym in closed:
            del self.positions[sym]

        if closed:
            self._save_state()

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
        self._record_exit(sym, pnl)
        self._save_state()

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
                qty = pos.get('qty', pos.get('other_half', 0))
                sym_pnl = (px - pos['entry_price']) * qty
                lines.append(f"    • {sym}: {qty}sh @ ${pos['entry_price']:.2f} → ${px:.2f} (${sym_pnl:+.2f})")
        if self.trades:
            today_trades = [t for t in self.trades if t['time'].date() == datetime.now(_ET).date()]
            lines.append(f"  📝 Trades today: {len(today_trades)}")
            total_realized = sum(t['pnl'] for t in today_trades if t['side'] == 'SELL')
            lines.append(f"  💰 Realized today: ${total_realized:+,.2f}")

        return "\n".join(lines)

    def reset(self):
        """Reset portfolio for new week. Archive old journal, start fresh."""
        # Archive old journal
        if self.JOURNAL_PATH.exists():
            week_label = datetime.now(_ET).strftime('%Y-%m-%d')
            archive = self.JOURNAL_PATH.with_name(f"virtual_trades_week_{week_label}.csv")
            try:
                self.JOURNAL_PATH.rename(archive)
                log.info(f"FIB DT journal archived → {archive.name}")
            except Exception as e:
                log.warning(f"FIB DT journal archive error: {e}")
        # Reset state
        self.cash = self.INITIAL_CASH
        self.positions = {}
        self.trades = []
        self._init_journal()
        self._save_state()
        log.info("FIB DT portfolio reset to $3,000")


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
    _STATE_PATH = DATA_DIR / "gg_state.json"

    def __init__(self):
        self.cash: float = self.INITIAL_CASH
        self.positions: dict[str, dict] = {}
        # sym -> {qty, entry_price, vwap_at_entry, entry_ts}
        self.trades: list[dict] = []
        self._exit_history: list[dict] = []
        self._daily_realized_pnl: float = 0.0
        self._daily_pnl_date: str = ""
        self._init_journal()
        self._load_state()

    def _save_state(self):
        """Persist portfolio state to disk (survives restarts)."""
        try:
            state = {
                'cash': self.cash,
                'positions': self.positions,
                'date': datetime.now(_ET).strftime('%Y-%m-%d'),
            }
            with open(self._STATE_PATH, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            log.warning(f"GG state save error: {e}")

    def _load_state(self):
        """Load portfolio state from disk (persists across days within the week)."""
        try:
            if not self._STATE_PATH.exists():
                return
            with open(self._STATE_PATH) as f:
                state = json.load(f)
            self.cash = state.get('cash', self.INITIAL_CASH)
            self.positions = state.get('positions', {})
            if self.positions:
                log.info(f"GG state restored: cash=${self.cash:.0f}, "
                         f"{len(self.positions)} positions: "
                         + ", ".join(self.positions.keys()))
        except Exception as e:
            log.warning(f"GG state load error: {e}")

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
        if side != 'BUY':
            _send_trade_journal_entry('🚀', 'Gap&Go', sym, reason, qty,
                                      price, entry_price, pnl, net_liq_val,
                                      self.cash, self.trades)

    @staticmethod
    def _ts() -> str:
        return datetime.now(_ET).strftime('%Y-%m-%d %H:%M ET')

    def _reset_daily_pnl(self):
        today = datetime.now(_ET).strftime('%Y-%m-%d')
        if self._daily_pnl_date != today:
            self._daily_pnl_date = today
            self._daily_realized_pnl = 0.0
            self._exit_history = [e for e in self._exit_history
                                  if time_mod.time() - e['ts'] < 86400]

    def can_enter(self, sym: str) -> tuple[bool, str]:
        self._reset_daily_pnl()
        if len(self.positions) >= GG_LIVE_MAX_POSITIONS:
            return False, f"max positions ({GG_LIVE_MAX_POSITIONS})"
        if self._daily_realized_pnl <= -STRATEGY_DAILY_LOSS_LIMIT:
            return False, f"daily loss limit (${self._daily_realized_pnl:.0f})"
        today = datetime.now(_ET).strftime('%Y-%m-%d')
        today_entries = sum(1 for e in self._exit_history if e['sym'] == sym
                           and datetime.fromtimestamp(e['ts'], _ET).strftime('%Y-%m-%d') == today)
        if sym in self.positions:
            today_entries += 1
        if today_entries >= STRATEGY_MAX_ENTRIES_PER_STOCK_PER_DAY:
            return False, f"max entries/day for {sym}"
        now_ts = time_mod.time()
        for e in reversed(self._exit_history):
            if e['sym'] == sym:
                cooldown = (STRATEGY_REENTRY_COOLDOWN_AFTER_LOSS_SEC if e['pnl'] < 0
                            else STRATEGY_REENTRY_COOLDOWN_SEC)
                if now_ts - e['ts'] < cooldown:
                    return False, f"cooldown for {sym}"
                break
        return True, ""

    def _record_exit(self, sym: str, pnl: float):
        self._reset_daily_pnl()
        self._daily_realized_pnl += pnl
        self._exit_history.append({'sym': sym, 'ts': time_mod.time(), 'pnl': pnl})

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
            'entry_ts': time_mod.time(),
        }

        self.trades.append({
            'sym': sym, 'side': 'BUY', 'qty': qty,
            'price': price, 'pnl': 0, 'time': datetime.now(_ET),
        })

        net = self.net_liq({})
        self._log_journal(sym, 'BUY', 'ENTRY', qty, price, price, 0, net)
        self._save_state()

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
        self._record_exit(sym, pnl)
        self._save_state()

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

    def sell_half(self, sym: str, price: float, reason: str) -> str | None:
        """Sell half the GG position (profit target). Returns Telegram alert text or None."""
        pos = self.positions.get(sym)
        if not pos:
            return None

        total_qty = pos['qty']
        half = total_qty // 2
        if half < 1:
            # Position too small to split — sell all
            return self.sell(sym, price, reason)

        entry_price = pos['entry_price']
        pnl = (price - entry_price) * half
        pnl_pct = (price / entry_price - 1) * 100 if entry_price > 0 else 0

        old_cash = self.cash
        self.cash += half * price

        self.trades.append({
            'sym': sym, 'side': 'SELL', 'qty': half,
            'price': price, 'pnl': pnl, 'time': datetime.now(_ET),
        })

        # Update position: reduce qty, keep rest
        pos['qty'] = total_qty - half
        self._save_state()

        net = self.net_liq({})
        self._log_journal(sym, 'SELL', f'TARGET_HALF ({reason})', half, price, entry_price, pnl, net)

        remaining = total_qty - half
        msg = (
            f"🚀 <b>Gap&Go [GG-SIM] — TARGET HIT {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  🟢 {half}sh @ ${price:.4f} (entry ${entry_price:.4f})\n"
            f"  📈 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f} ({remaining}sh still in position)\n"
            f"  🛡️ Stop → breakeven ${entry_price:.4f}"
        )
        log.info(f"GG TARGET HALF: {sym} {half}sh @ ${price:.4f} P&L=${pnl:+.2f} ({remaining}sh remain)")
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

    def reset(self):
        """Reset portfolio for new week. Archive old journal, start fresh."""
        if self.JOURNAL_PATH.exists():
            week_label = datetime.now(_ET).strftime('%Y-%m-%d')
            archive = self.JOURNAL_PATH.with_name(f"gg_virtual_trades_week_{week_label}.csv")
            try:
                self.JOURNAL_PATH.rename(archive)
                log.info(f"GG journal archived → {archive.name}")
            except Exception as e:
                log.warning(f"GG journal archive error: {e}")
        self.cash = self.INITIAL_CASH
        self.positions = {}
        self.trades = []
        self._init_journal()
        self._save_state()
        log.info("GG portfolio reset to $3,000")


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
    _STATE_PATH = DATA_DIR / "mr_state.json"

    def __init__(self):
        self.cash: float = self.INITIAL_CASH
        self.positions: dict[str, dict] = {}
        # sym -> {qty, entry_price, vwap_at_entry, signal_type, entry_ts}
        self.trades: list[dict] = []
        self._exit_history: list[dict] = []
        self._daily_realized_pnl: float = 0.0
        self._daily_pnl_date: str = ""
        self._init_journal()
        self._load_state()

    def _save_state(self):
        """Persist portfolio state to disk (survives restarts)."""
        try:
            state = {
                'cash': self.cash,
                'positions': self.positions,
                'date': datetime.now(_ET).strftime('%Y-%m-%d'),
            }
            with open(self._STATE_PATH, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            log.warning(f"MR state save error: {e}")

    def _load_state(self):
        """Load portfolio state from disk (persists across days within the week)."""
        try:
            if not self._STATE_PATH.exists():
                return
            with open(self._STATE_PATH) as f:
                state = json.load(f)
            self.cash = state.get('cash', self.INITIAL_CASH)
            self.positions = state.get('positions', {})
            if self.positions:
                log.info(f"MR state restored: cash=${self.cash:.0f}, "
                         f"{len(self.positions)} positions: "
                         + ", ".join(self.positions.keys()))
        except Exception as e:
            log.warning(f"MR state load error: {e}")

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
        if side != 'BUY':
            _send_trade_journal_entry('📈', 'MR', sym, reason, qty,
                                      price, entry_price, pnl, net_liq_val,
                                      self.cash, self.trades)

    @staticmethod
    def _ts() -> str:
        return datetime.now(_ET).strftime('%Y-%m-%d %H:%M ET')

    def _reset_daily_pnl(self):
        today = datetime.now(_ET).strftime('%Y-%m-%d')
        if self._daily_pnl_date != today:
            self._daily_pnl_date = today
            self._daily_realized_pnl = 0.0
            self._exit_history = [e for e in self._exit_history
                                  if time_mod.time() - e['ts'] < 86400]

    def can_enter(self, sym: str) -> tuple[bool, str]:
        self._reset_daily_pnl()
        if len(self.positions) >= MR_LIVE_MAX_POSITIONS:
            return False, f"max positions ({MR_LIVE_MAX_POSITIONS})"
        if self._daily_realized_pnl <= -STRATEGY_DAILY_LOSS_LIMIT:
            return False, f"daily loss limit (${self._daily_realized_pnl:.0f})"
        today = datetime.now(_ET).strftime('%Y-%m-%d')
        today_entries = sum(1 for e in self._exit_history if e['sym'] == sym
                           and datetime.fromtimestamp(e['ts'], _ET).strftime('%Y-%m-%d') == today)
        if sym in self.positions:
            today_entries += 1
        if today_entries >= STRATEGY_MAX_ENTRIES_PER_STOCK_PER_DAY:
            return False, f"max entries/day for {sym}"
        now_ts = time_mod.time()
        for e in reversed(self._exit_history):
            if e['sym'] == sym:
                cooldown = (STRATEGY_REENTRY_COOLDOWN_AFTER_LOSS_SEC if e['pnl'] < 0
                            else STRATEGY_REENTRY_COOLDOWN_SEC)
                if now_ts - e['ts'] < cooldown:
                    return False, f"cooldown for {sym}"
                break
        return True, ""

    def _record_exit(self, sym: str, pnl: float):
        self._reset_daily_pnl()
        self._daily_realized_pnl += pnl
        self._exit_history.append({'sym': sym, 'ts': time_mod.time(), 'pnl': pnl})

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
            'entry_ts': time_mod.time(),
        }

        self.trades.append({
            'sym': sym, 'side': 'BUY', 'qty': qty,
            'price': price, 'pnl': 0, 'time': datetime.now(_ET),
        })

        net = self.net_liq({})
        self._log_journal(sym, 'BUY', f'ENTRY ({signal_type})', qty, price, price, 0, net)
        self._save_state()

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
        self._record_exit(sym, pnl)
        self._save_state()

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

    def sell_half(self, sym: str, price: float, reason: str) -> str | None:
        """Sell half the MR position (profit target). Returns Telegram alert text or None."""
        pos = self.positions.get(sym)
        if not pos:
            return None

        total_qty = pos['qty']
        half = total_qty // 2
        if half < 1:
            return self.sell(sym, price, reason)

        entry_price = pos['entry_price']
        pnl = (price - entry_price) * half
        pnl_pct = (price / entry_price - 1) * 100 if entry_price > 0 else 0

        old_cash = self.cash
        self.cash += half * price

        self.trades.append({
            'sym': sym, 'side': 'SELL', 'qty': half,
            'price': price, 'pnl': pnl, 'time': datetime.now(_ET),
        })

        pos['qty'] = total_qty - half
        pos['target_hit'] = True
        self._save_state()

        net = self.net_liq({})
        self._log_journal(sym, 'SELL', f'TARGET_HALF ({reason})', half, price, entry_price, pnl, net)

        remaining = total_qty - half
        msg = (
            f"📈 <b>Momentum Ride [MR-SIM] — TARGET HIT {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  🟢 {half}sh @ ${price:.4f} (entry ${entry_price:.4f})\n"
            f"  📈 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f} ({remaining}sh still in position)\n"
            f"  🛡️ Stop → breakeven ${entry_price:.4f}"
        )
        log.info(f"MR TARGET HALF: {sym} {half}sh @ ${price:.4f} P&L=${pnl:+.2f} ({remaining}sh remain)")
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

    def reset(self):
        """Reset portfolio for new week. Archive old journal, start fresh."""
        if self.JOURNAL_PATH.exists():
            week_label = datetime.now(_ET).strftime('%Y-%m-%d')
            archive = self.JOURNAL_PATH.with_name(f"mr_virtual_trades_week_{week_label}.csv")
            try:
                self.JOURNAL_PATH.rename(archive)
                log.info(f"MR journal archived → {archive.name}")
            except Exception as e:
                log.warning(f"MR journal archive error: {e}")
        self.cash = self.INITIAL_CASH
        self.positions = {}
        self.trades = []
        self._init_journal()
        self._save_state()
        log.info("MR portfolio reset to $3,000")


# ──────────────────────────────────────────────────────────
#  FT Virtual Portfolio (Float Turnover paper simulation)
# ──────────────────────────────────────────────────────────

class FTVirtualPortfolio:
    """Virtual portfolio for Float Turnover paper simulation.

    Entry and exit are fully driven by the strategy (1-min bar high/low).
    """

    INITIAL_CASH = FT_LIVE_INITIAL_CASH
    JOURNAL_PATH = DATA_DIR / "ft_virtual_trades.csv"
    _STATE_PATH = DATA_DIR / "ft_state.json"

    def __init__(self):
        self.cash: float = self.INITIAL_CASH
        self.positions: dict[str, dict] = {}
        # sym -> {qty, entry_price, turnover_at_entry, entry_ts}
        self.trades: list[dict] = []
        self._exit_history: list[dict] = []
        self._daily_realized_pnl: float = 0.0
        self._daily_pnl_date: str = ""
        self._init_journal()
        self._load_state()

    def _save_state(self):
        """Persist portfolio state to disk (survives restarts)."""
        try:
            state = {
                'cash': self.cash,
                'positions': self.positions,
                'date': datetime.now(_ET).strftime('%Y-%m-%d'),
            }
            with open(self._STATE_PATH, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            log.warning(f"FT state save error: {e}")

    def _load_state(self):
        """Load portfolio state from disk (persists across days within the week)."""
        try:
            if not self._STATE_PATH.exists():
                return
            with open(self._STATE_PATH) as f:
                state = json.load(f)
            self.cash = state.get('cash', self.INITIAL_CASH)
            self.positions = state.get('positions', {})
            if self.positions:
                log.info(f"FT state restored: cash=${self.cash:.0f}, "
                         f"{len(self.positions)} positions: "
                         + ", ".join(self.positions.keys()))
        except Exception as e:
            log.warning(f"FT state load error: {e}")

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
            log.warning(f"FT journal write error: {e}")
        if side != 'BUY':
            _send_trade_journal_entry('🔄', 'FT', sym, reason, qty,
                                      price, entry_price, pnl, net_liq_val,
                                      self.cash, self.trades)

    @staticmethod
    def _ts() -> str:
        return datetime.now(_ET).strftime('%Y-%m-%d %H:%M ET')

    def _reset_daily_pnl(self):
        today = datetime.now(_ET).strftime('%Y-%m-%d')
        if self._daily_pnl_date != today:
            self._daily_pnl_date = today
            self._daily_realized_pnl = 0.0
            self._exit_history = [e for e in self._exit_history
                                  if time_mod.time() - e['ts'] < 86400]

    def can_enter(self, sym: str) -> tuple[bool, str]:
        self._reset_daily_pnl()
        if len(self.positions) >= FT_LIVE_MAX_POSITIONS:
            return False, f"max positions ({FT_LIVE_MAX_POSITIONS})"
        if self._daily_realized_pnl <= -STRATEGY_DAILY_LOSS_LIMIT:
            return False, f"daily loss limit (${self._daily_realized_pnl:.0f})"
        today = datetime.now(_ET).strftime('%Y-%m-%d')
        today_entries = sum(1 for e in self._exit_history if e['sym'] == sym
                           and datetime.fromtimestamp(e['ts'], _ET).strftime('%Y-%m-%d') == today)
        if sym in self.positions:
            today_entries += 1
        if today_entries >= STRATEGY_MAX_ENTRIES_PER_STOCK_PER_DAY:
            return False, f"max entries/day for {sym}"
        now_ts = time_mod.time()
        for e in reversed(self._exit_history):
            if e['sym'] == sym:
                cooldown = (STRATEGY_REENTRY_COOLDOWN_AFTER_LOSS_SEC if e['pnl'] < 0
                            else STRATEGY_REENTRY_COOLDOWN_SEC)
                if now_ts - e['ts'] < cooldown:
                    return False, f"cooldown for {sym}"
                break
        return True, ""

    def _record_exit(self, sym: str, pnl: float):
        self._reset_daily_pnl()
        self._daily_realized_pnl += pnl
        self._exit_history.append({'sym': sym, 'ts': time_mod.time(), 'pnl': pnl})

    def buy(self, sym: str, qty: int, price: float, turnover_pct: float) -> str | None:
        """Open an FT virtual position. Returns Telegram alert text or None."""
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
            'turnover_at_entry': turnover_pct,
            'entry_ts': time_mod.time(),
        }

        self.trades.append({
            'sym': sym, 'side': 'BUY', 'qty': qty,
            'price': price, 'pnl': 0, 'time': datetime.now(_ET),
        })

        net = self.net_liq({})
        self._log_journal(sym, 'BUY', 'ENTRY', qty, price, price, 0, net)
        self._save_state()

        msg = (
            f"🔄 <b>Float Turnover [FT-SIM] — BUY {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  💰 {qty}sh @ ${price:.4f} | Turnover: {turnover_pct:.0f}%\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f}"
        )
        log.info(f"FT VIRTUAL BUY: {sym} {qty}sh @ ${price:.4f} turnover={turnover_pct:.0f}%")
        return msg

    def sell(self, sym: str, price: float, reason: str) -> str | None:
        """Sell entire FT position. Returns Telegram alert text or None."""
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
        self._record_exit(sym, pnl)
        self._save_state()

        net = self.net_liq({})
        self._log_journal(sym, 'SELL', reason, qty, price, entry_price, pnl, net)

        pnl_icon = "🟢" if pnl >= 0 else "🔴"
        msg = (
            f"🔄 <b>Float Turnover [FT-SIM] — SELL {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  {pnl_icon} {qty}sh @ ${price:.4f} (entry ${entry_price:.4f})\n"
            f"  📈 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f}\n"
            f"  📝 Reason: {reason}"
        )
        log.info(f"FT VIRTUAL SELL: {sym} {qty}sh @ ${price:.4f} P&L=${pnl:+.2f} ({reason})")
        return msg

    def sell_half(self, sym: str, price: float, reason: str) -> str | None:
        """Sell half the FT position (profit target). Returns Telegram alert text or None."""
        pos = self.positions.get(sym)
        if not pos:
            return None

        total_qty = pos['qty']
        half = total_qty // 2
        if half < 1:
            return self.sell(sym, price, reason)

        entry_price = pos['entry_price']
        pnl = (price - entry_price) * half
        pnl_pct = (price / entry_price - 1) * 100 if entry_price > 0 else 0

        old_cash = self.cash
        self.cash += half * price

        self.trades.append({
            'sym': sym, 'side': 'SELL', 'qty': half,
            'price': price, 'pnl': pnl, 'time': datetime.now(_ET),
        })

        pos['qty'] = total_qty - half
        pos['target_hit'] = True
        self._save_state()

        net = self.net_liq({})
        self._log_journal(sym, 'SELL', f'TARGET_HALF ({reason})', half, price, entry_price, pnl, net)

        remaining = total_qty - half
        msg = (
            f"🔄 <b>Float Turnover [FT-SIM] — TARGET HIT {sym}</b>\n"
            f"  🕐 {self._ts()}\n"
            f"  🟢 {half}sh @ ${price:.4f} (entry ${entry_price:.4f})\n"
            f"  📈 P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
            f"  💵 Cash: ${old_cash:,.0f} → ${self.cash:,.0f}\n"
            f"  📊 Portfolio: ${net:,.0f} ({remaining}sh still in position)\n"
            f"  🛡️ Stop → breakeven ${entry_price:.4f}"
        )
        log.info(f"FT TARGET HALF: {sym} {half}sh @ ${price:.4f} P&L=${pnl:+.2f} ({remaining}sh remain)")
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
            f"🔄 <b>Float Turnover [FT-SIM] Portfolio</b>",
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

    def reset(self):
        """Reset portfolio for new week. Archive old journal, start fresh."""
        if self.JOURNAL_PATH.exists():
            week_label = datetime.now(_ET).strftime('%Y-%m-%d')
            archive = self.JOURNAL_PATH.with_name(f"ft_virtual_trades_week_{week_label}.csv")
            try:
                self.JOURNAL_PATH.rename(archive)
                log.info(f"FT journal archived → {archive.name}")
            except Exception as e:
                log.warning(f"FT journal archive error: {e}")
        self.cash = self.INITIAL_CASH
        self.positions = {}
        self.trades = []
        self._init_journal()
        self._save_state()
        log.info("FT portfolio reset to $3,000")


# ══════════════════════════════════════════════════════════
#  Scanner Thread (replaces MonitorThread)
# ══════════════════════════════════════════════════════════

class ScannerThread(threading.Thread):
    def __init__(self, freq: int, price_min: float, price_max: float,
                 on_status=None, on_stocks=None, on_alert=None,
                 on_price_update=None, order_thread=None):
        super().__init__(daemon=True)
        self.freq = freq
        self.price_min = price_min
        self.price_max = price_max
        self.on_status = on_status
        self.on_stocks = on_stocks  # callback(dict) to update GUI table (full rebuild)
        self.on_alert = on_alert    # callback(str) for alert messages
        self.on_price_update = on_price_update  # callback(dict) for in-place price updates
        self._order_thread = order_thread  # for Telegram trading buttons
        self.running = False
        self.previous: dict = {}
        self._last_seen_in_scan: dict[str, float] = {}  # sym → time.time() when last found by OCR/IBKR
        self._fib_dt_first_seen: dict[str, float] = {}  # sym → time.time() when first seen (warmup)
        self.count = 0
        self._warmup = True   # suppress alerts on first cycle
        self._warmup_pending: dict[str, dict] = {}  # sym → stock data, enriched during warmup
        self._reports_sent: set[str] = set()      # stocks that got Telegram report this session
        self._last_session: str = _get_market_session()  # track session transitions
        # ── FIB DT Auto-Strategy ──
        self._fib_dt_strategy = FibDTLiveStrategySync(ib_getter=_get_ibkr)
        self._virtual_portfolio = VirtualPortfolio()
        # Sync strategy state from restored portfolio
        if self._virtual_portfolio.positions:
            self._fib_dt_strategy.sync_from_portfolio(self._virtual_portfolio.positions)
        self._fib_dt_current_sym: str | None = None  # best turnover symbol
        # Cache scanner contracts for FIB DT (symbol -> Contract)
        self._scanner_contracts: dict[str, object] = {}
        self._cached_buying_power: float = 0.0
        self._cached_net_liq: float = 0.0
        self._cached_positions: dict[str, tuple] = {}  # sym → (qty, avg, mkt, pnl)
        # ── Gap and Go Auto-Strategy ──
        self._gg_strategy = GapGoLiveStrategy(ib_getter=_get_ibkr)
        self._gg_portfolio = GGVirtualPortfolio()
        # Sync strategy state from restored portfolio
        if self._gg_portfolio.positions:
            self._gg_strategy.sync_from_portfolio(self._gg_portfolio.positions)
        # ── Momentum Ride Auto-Strategy ──
        self._mr_strategy = MomentumRideLiveStrategy(ib_getter=_get_ibkr)
        self._mr_portfolio = MRVirtualPortfolio()
        # Sync strategy state from restored portfolio
        if self._mr_portfolio.positions:
            self._mr_strategy.sync_from_portfolio(self._mr_portfolio.positions)
        # ── Float Turnover Auto-Strategy ──
        self._ft_strategy = FloatTurnoverLiveStrategy(ib_getter=_get_ibkr)
        self._ft_portfolio = FTVirtualPortfolio()
        # Sync strategy state from restored portfolio
        if self._ft_portfolio.positions:
            self._ft_strategy.sync_from_portfolio(self._ft_portfolio.positions)
        # ── Telegram stock lookup ──
        self._lookup_queue: queue.Queue = queue.Queue()
        self._telegram_listener: TelegramListenerThread | None = None

    def stop(self):
        self.running = False
        if self._telegram_listener:
            self._telegram_listener.stop()

    def _start_ocr_refresh_thread(self):
        """Launch a daemon thread that continuously reads OCR prices (~1s interval).

        Runs independently of the main enrichment cycle so the GUI always
        shows near-real-time prices regardless of how long enrichment takes.
        """
        def _ocr_loop():
            _n = 0
            while self.running:
                t0 = time_mod.time()
                try:
                    self._quick_ocr_price_update()
                except Exception as e:
                    log.warning(f"OCR refresh error: {e}")
                    time_mod.sleep(2)
                    continue
                elapsed = time_mod.time() - t0
                _n += 1
                if _n % 60 == 0:
                    log.info(f"OCR refresh: {_n} reads, last={elapsed:.2f}s")
                time_mod.sleep(max(0, 1.0 - elapsed))
            log.info("OCR refresh thread stopped (self.running=False)")

        t = threading.Thread(target=_ocr_loop, daemon=True, name="OCR-refresh")
        t.start()
        log.info("OCR real-time refresh thread started (1s interval)")

    def _quick_ocr_price_update(self):
        """Ultra-fast OCR price refresh for real-time GUI updates.

        Only captures OCR + updates prices/pct + pushes to GUI.
        No IBKR calls, no fib recalc, no enrichment — keeps it under 1s.
        """
        if not self.previous:
            return
        raw = _quick_ocr_capture(self.price_min, self.price_max)
        if not raw:
            return
        # Update last-seen timestamps for carry-over freshness
        now_ts = time_mod.time()
        for sym in raw:
            self._last_seen_in_scan[sym] = now_ts
        price_changes: dict[str, tuple[float, float]] = {}  # sym → (price, pct)
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
                new_pct = round((new_price - prev_close) / prev_close * 100, 1)
                self.previous[sym]['pct'] = new_pct
            else:
                new_pct = d['pct']
                self.previous[sym]['pct'] = new_pct
            # Update day_high / day_low with real-time price
            cur_high = self.previous[sym].get('day_high', 0)
            cur_low = self.previous[sym].get('day_low', 0)
            if cur_high > 0 and new_price > cur_high:
                self.previous[sym]['day_high'] = round(new_price, 4)
            if cur_low > 0 and new_price < cur_low:
                self.previous[sym]['day_low'] = round(new_price, 4)
            # Update running alert trackers
            prev_rh = _running_high.get(sym, 0)
            if new_price > prev_rh:
                _running_high[sym] = new_price
            prev_rl = _running_low.get(sym, 0)
            if prev_rl <= 0 or new_price < prev_rl:
                _running_low[sym] = new_price
            price_changes[sym] = (new_price, new_pct)
        if price_changes:
            # Push lightweight in-place price update to GUI (no rebuild)
            self._push_price_update(price_changes)

    def _push_price_update(self, changes: dict):
        """Push price/pct changes to GUI without full table rebuild."""
        if self.on_price_update:
            self.on_price_update(changes)

    _CYCLE_WARN_SECS = 60      # warn if cycle exceeds this
    _CYCLE_FORCE_SECS = 120     # force IBKR reconnect if cycle exceeds this

    def run(self):
        # Ensure TWS is running before starting the scanner
        _ensure_tws_running()
        # Ensure asyncio event loop exists in this thread (ib_insync needs one)
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        self.running = True
        # Start Telegram listener for stock lookups
        self._telegram_listener = TelegramListenerThread(
            self._lookup_queue, order_thread=self._order_thread)
        self._telegram_listener.start()
        # Start continuous OCR price refresh (independent of enrichment cycle)
        self._start_ocr_refresh_thread()
        log.info(f"Scanner started: freq={self.freq}s, price ${self.price_min}-${self.price_max}")
        self._consecutive_slow = 0
        while self.running:
            cycle_start = time_mod.time()
            try:
                self._cycle()
            except Exception as e:
                log.error(f"Scanner cycle error: {e}")
                if self.on_status:
                    self.on_status(f"Error: {e}")
                # Backoff on error to prevent spin loop
                time_mod.sleep(5)
            # ── Watchdog: detect slow/stuck cycles ──
            cycle_dur = time_mod.time() - cycle_start
            if cycle_dur > self._CYCLE_FORCE_SECS:
                self._consecutive_slow += 1
                log.warning(f"WATCHDOG: cycle took {cycle_dur:.0f}s (>{self._CYCLE_FORCE_SECS}s). "
                            f"Consecutive slow: {self._consecutive_slow}. Forcing IBKR reconnect.")
                global _ibkr
                if _ibkr:
                    try:
                        _ibkr.disconnect()
                    except Exception:
                        pass
                    _ibkr = None
                send_telegram(
                    f"⚠️ <b>Watchdog: cycle איטי</b>\n"
                    f"  משך: {cycle_dur:.0f}s | רצף: {self._consecutive_slow}\n"
                    f"  מאפס חיבור IBKR..."
                )
            elif cycle_dur > self._CYCLE_WARN_SECS:
                self._consecutive_slow += 1
                log.warning(f"WATCHDOG: cycle took {cycle_dur:.0f}s (>{self._CYCLE_WARN_SECS}s)")
            else:
                self._consecutive_slow = 0
            for _ in range(self.freq):
                if not self.running:
                    break
                self._process_lookup_queue()
                _check_news_updates(self.previous, suppress_send=self._warmup)
                # OCR price refresh is handled by the dedicated thread
                time_mod.sleep(1)

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

            price, prev_close, ext_high, ext_low, ext_vwap, ext_volume = \
                _fetch_extended_hours_price(ib, contract, session, bars)

            if ext_volume is not None and ext_volume > 0:
                volume = ext_volume
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

    def _check_timed_exits(self, current: dict):
        """Force-close positions that exceeded their max hold time."""
        now_ts = time_mod.time()

        # FIB DT — max hold
        for sym in list(self._virtual_portfolio.positions.keys()):
            pos = self._virtual_portfolio.positions[sym]
            entry_ts = pos.get('entry_ts', 0)
            if entry_ts > 0 and (now_ts - entry_ts) > FIB_DT_MAX_HOLD_MINUTES * 60:
                price = current.get(sym, {}).get('price', pos['entry_price'])
                hold_min = int((now_ts - entry_ts) / 60)
                remaining = pos['other_half'] if pos['phase'] == 'TRAILING' else pos['qty']
                pnl = (price - pos['entry_price']) * remaining
                if pos['phase'] == 'TRAILING':
                    alert = self._virtual_portfolio.trailing_exit(
                        sym, price, f"timed_exit ({hold_min}min)")
                else:
                    # Force sell via stop-like mechanism
                    old_cash = self._virtual_portfolio.cash
                    self._virtual_portfolio.cash += remaining * price
                    self._virtual_portfolio.trades.append({
                        'sym': sym, 'side': 'SELL', 'qty': remaining,
                        'price': price, 'pnl': pnl, 'time': datetime.now(_ET),
                    })
                    del self._virtual_portfolio.positions[sym]
                    self._virtual_portfolio._record_exit(sym, pnl)
                    self._virtual_portfolio._save_state()
                    pnl_pct = (price / pos['entry_price'] - 1) * 100 if pos['entry_price'] > 0 else 0
                    net = self._virtual_portfolio.net_liq({})
                    self._virtual_portfolio._log_journal(
                        sym, 'SELL', f'TIMED_EXIT ({hold_min}min)', remaining, price,
                        pos['entry_price'], pnl, net)
                    alert = (
                        f"📐 <b>FIB DT [SIM] — TIMED EXIT {sym}</b>\n"
                        f"  ⏰ Held {hold_min}min (max {FIB_DT_MAX_HOLD_MINUTES})\n"
                        f"  {'🟢' if pnl >= 0 else '🔴'} {remaining}sh @ ${price:.2f} "
                        f"P&L: ${pnl:+,.2f} ({pnl_pct:+.1f}%)"
                    )
                if alert:
                    send_telegram(alert)
                self._fib_dt_strategy.mark_position_closed(sym)
                log.info(f"FIB DT TIMED EXIT: {sym} after {hold_min}min")

        # GG — max hold
        for sym in list(self._gg_portfolio.positions.keys()):
            pos = self._gg_portfolio.positions[sym]
            entry_ts = pos.get('entry_ts', 0)
            if entry_ts > 0 and (now_ts - entry_ts) > GG_MAX_HOLD_MINUTES * 60:
                price = current.get(sym, {}).get('price', pos['entry_price'])
                hold_min = int((now_ts - entry_ts) / 60)
                alert = self._gg_portfolio.sell(sym, price, f"timed_exit ({hold_min}min)")
                if alert:
                    send_telegram(alert)
                self._gg_strategy.mark_position_closed(sym)
                log.info(f"GG TIMED EXIT: {sym} after {hold_min}min")

        # MR — no separate max hold; handled by 90-min timeout in strategy
        # But add safety for stuck positions (e.g., 4 hours)
        _MR_SAFETY_MAX_HOLD_MIN = 240
        for sym in list(self._mr_portfolio.positions.keys()):
            pos = self._mr_portfolio.positions[sym]
            entry_ts = pos.get('entry_ts', 0)
            if entry_ts > 0 and (now_ts - entry_ts) > _MR_SAFETY_MAX_HOLD_MIN * 60:
                price = current.get(sym, {}).get('price', pos['entry_price'])
                hold_min = int((now_ts - entry_ts) / 60)
                alert = self._mr_portfolio.sell(sym, price, f"timed_exit ({hold_min}min)")
                if alert:
                    send_telegram(alert)
                self._mr_strategy.mark_position_closed(sym)
                log.info(f"MR TIMED EXIT: {sym} after {hold_min}min")

        # FT — max hold
        for sym in list(self._ft_portfolio.positions.keys()):
            pos = self._ft_portfolio.positions[sym]
            entry_ts = pos.get('entry_ts', 0)
            if entry_ts > 0 and (now_ts - entry_ts) > FT_MAX_HOLD_MINUTES * 60:
                price = current.get(sym, {}).get('price', pos['entry_price'])
                hold_min = int((now_ts - entry_ts) / 60)
                alert = self._ft_portfolio.sell(sym, price, f"timed_exit ({hold_min}min)")
                if alert:
                    send_telegram(alert)
                self._ft_strategy.mark_position_closed(sym)
                log.info(f"FT TIMED EXIT: {sym} after {hold_min}min")

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

                if pct < FIB_DT_GAP_MIN_PCT or pct > FIB_DT_GAP_MAX_PCT:
                    continue
                if price <= 0 or prev_close <= 0 or not contract:
                    continue
                if price < STRATEGY_MIN_PRICE:
                    continue
                if vwap > 0 and price <= vwap:
                    continue
                # RVOL filter
                rvol = d.get('rvol', 0)
                if rvol < FIB_DT_RVOL_MIN:
                    continue
                # News filter
                if FIB_DT_REQUIRE_NEWS and not _stock_has_news(sym):
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

            # Sort by turnover descending, limit to TOP 3
            candidates.sort(key=lambda x: x[1], reverse=True)
            candidates = candidates[:3]

            # SMA9 filter (only on final candidates to limit IBKR calls)
            candidates = [
                c for c in candidates
                if _check_above_sma9(c[0], c[2]['price'], c[2]['contract'])
            ]
            if not candidates:
                return

            # Log ranking
            ranking_str = " | ".join(
                f"{s} {t:.2f}x" for s, t, _, _ in candidates
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

                # Min price filter
                if entry_price < STRATEGY_MIN_PRICE:
                    log.info(f"FIB DT: Skipping {req.symbol} — price ${entry_price:.2f} < min ${STRATEGY_MIN_PRICE}")
                    continue
                # Warmup: skip entries within first 5 min of seeing a stock
                now_ts = time_mod.time()
                first_seen = self._fib_dt_first_seen.get(req.symbol)
                if first_seen is None:
                    self._fib_dt_first_seen[req.symbol] = now_ts
                    log.info(f"FIB DT: First seen {req.symbol} — warmup {FIB_DT_WARMUP_SEC}s")
                    continue
                if now_ts - first_seen < FIB_DT_WARMUP_SEC:
                    log.debug(f"FIB DT: Skipping {req.symbol} — warmup ({now_ts - first_seen:.0f}s < {FIB_DT_WARMUP_SEC}s)")
                    continue
                # Risk controls: max positions, cooldown, daily loss
                can_buy, reason = self._virtual_portfolio.can_enter(req.symbol)
                if not can_buy:
                    log.info(f"FIB DT: Skipping {req.symbol} — {reason}")
                    continue

                qty = int(min(self._virtual_portfolio.cash, FIB_DT_POSITION_SIZE_FIXED) / entry_price) if entry_price > 0 else 0
                qty = min(qty, 5000)  # absolute max shares
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
        Checks: FIB DT stops/targets, MR/FT profit targets + breakeven stops.
        """
        # Build current prices from scan data
        current_prices = {}
        for sym, d in current.items():
            px = d.get('price', 0)
            if px > 0:
                current_prices[sym] = px

        # ── FIB DT: check stops and targets ──
        if self._virtual_portfolio.positions:
            alerts = self._virtual_portfolio.check_stops_and_targets(current_prices)
            for alert in alerts:
                send_telegram(alert)

            # Sync strategy state for positions entering TRAILING (once only)
            for sym in list(self._virtual_portfolio.positions.keys()):
                pos = self._virtual_portfolio.positions[sym]
                if pos['phase'] == 'TRAILING' and not pos.get('trailing_synced'):
                    self._fib_dt_strategy.mark_trailing(sym)
                    pos['trailing_synced'] = True

        # ── MR: profit target (+10%) → sell half + breakeven stop ──
        for sym in list(self._mr_portfolio.positions.keys()):
            pos = self._mr_portfolio.positions.get(sym)
            if not pos:
                continue
            price = current_prices.get(sym, 0)
            if price <= 0:
                continue
            entry_price = pos.get('entry_price', 0)
            if entry_price <= 0:
                continue

            # Breakeven stop: after target hit, exit if price drops to entry
            if pos.get('target_hit') and price <= entry_price:
                alert = self._mr_portfolio.sell(sym, price,
                    f"breakeven_stop (entry=${entry_price:.4f})")
                if alert:
                    send_telegram(alert)
                    self._mr_strategy.mark_position_closed(sym)
                continue

            # Profit target: sell half at +10%
            if not pos.get('target_hit') and price >= entry_price * (1 + MR_LIVE_PROFIT_TARGET_PCT):
                pct = MR_LIVE_PROFIT_TARGET_PCT * 100
                alert = self._mr_portfolio.sell_half(sym, price,
                    f"profit_target (+{pct:.0f}%, entry=${entry_price:.4f})")
                if alert:
                    send_telegram(alert)

        # ── FT: profit target (+8%) → sell half + breakeven stop ──
        for sym in list(self._ft_portfolio.positions.keys()):
            pos = self._ft_portfolio.positions.get(sym)
            if not pos:
                continue
            price = current_prices.get(sym, 0)
            if price <= 0:
                continue
            entry_price = pos.get('entry_price', 0)
            if entry_price <= 0:
                continue

            # Breakeven stop: after target hit, exit if price drops to entry
            if pos.get('target_hit') and price <= entry_price:
                alert = self._ft_portfolio.sell(sym, price,
                    f"breakeven_stop (entry=${entry_price:.4f})")
                if alert:
                    send_telegram(alert)
                    self._ft_strategy.mark_position_closed(sym)
                continue

            # Profit target: sell half at +8%
            if not pos.get('target_hit') and price >= entry_price * (1 + FT_LIVE_PROFIT_TARGET_PCT):
                pct = FT_LIVE_PROFIT_TARGET_PCT * 100
                alert = self._ft_portfolio.sell_half(sym, price,
                    f"profit_target (+{pct:.0f}%, entry=${entry_price:.4f})")
                if alert:
                    send_telegram(alert)

    def _run_gap_go_cycle(self, current: dict):
        """Run Gap and Go auto-strategy: find gappers >= 30%, check HA + VWAP signals."""
        try:
            # ── 1. Build candidate list (>= 30% gap, RVOL >= 2.5, news) ──
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
                if price < STRATEGY_MIN_PRICE:
                    continue
                # RVOL filter
                rvol = d.get('rvol', 0)
                if rvol < GG_LIVE_RVOL_MIN:
                    continue
                # News filter
                if GG_LIVE_REQUIRE_NEWS and not _stock_has_news(sym):
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

            # Sort by gap % descending, limit to TOP 3
            candidates.sort(key=lambda c: c.gap_pct, reverse=True)
            candidates = candidates[:3]

            # SMA9 filter (only on final candidates to limit IBKR calls)
            candidates = [
                c for c in candidates
                if _check_above_sma9(c.symbol, c.current_price, c.contract)
            ]
            if not candidates:
                return

            log.info(f"GG candidates ({len(candidates)}): "
                     + " | ".join(f"{c.symbol} +{c.gap_pct:.0f}%" for c in candidates))

            # ── 2. Run strategy cycle ──
            entries, exits = self._gg_strategy.process_cycle(candidates)

            # ── 3. Execute exits first ──
            for exit_sig in exits:
                price = current.get(exit_sig.symbol, {}).get('price', exit_sig.price)
                if exit_sig.sell_half:
                    # Profit target: sell half, keep position open
                    alert = self._gg_portfolio.sell_half(exit_sig.symbol, price, exit_sig.reason)
                    if alert:
                        send_telegram(alert)
                        _log_daily_event('gg_exit', exit_sig.symbol,
                                         f"[GG-SIM] SELL HALF @ ${price:.4f} ({exit_sig.reason})")
                else:
                    alert = self._gg_portfolio.sell(exit_sig.symbol, price, exit_sig.reason)
                    if alert:
                        send_telegram(alert)
                        self._gg_strategy.mark_position_closed(exit_sig.symbol)
                        _log_daily_event('gg_exit', exit_sig.symbol,
                                         f"[GG-SIM] SELL @ ${price:.4f} ({exit_sig.reason})")

            # ── 4. Execute entries ──
            for entry in entries:
                # Duplicate-entry guard: skip if already holding this symbol
                if entry.symbol in self._gg_portfolio.positions:
                    self._gg_strategy.mark_position_closed(entry.symbol)
                    continue

                scan_price = current.get(entry.symbol, {}).get('price', entry.price)
                entry_price = scan_price if scan_price > 0 else entry.price

                # Min price filter
                if entry_price < STRATEGY_MIN_PRICE:
                    self._gg_strategy.mark_position_closed(entry.symbol)
                    log.info(f"GG: Skipping {entry.symbol} — price ${entry_price:.4f} < min ${STRATEGY_MIN_PRICE}")
                    continue
                # Risk controls
                can_buy, reason = self._gg_portfolio.can_enter(entry.symbol)
                if not can_buy:
                    self._gg_strategy.mark_position_closed(entry.symbol)
                    log.info(f"GG: Skipping {entry.symbol} — {reason}")
                    continue

                qty = int(min(self._gg_portfolio.cash, GG_LIVE_POSITION_SIZE_FIXED) / entry_price) if entry_price > 0 else 0
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

            # Cache 1-min bars from GG strategy for Doji/MA alerts
            for sym, raw_bars in self._gg_strategy._last_raw_bars.items():
                if raw_bars and len(raw_bars) >= 5:
                    try:
                        df = ib_util.df(raw_bars)
                        _intraday_cache[f"{sym}_1 min"] = (time_mod.time(), df)
                    except Exception:
                        pass

        except Exception as e:
            log.error(f"GG cycle error: {e}")

    def _run_momentum_ride_cycle(self, current: dict):
        """Run Momentum Ride auto-strategy: VWAP cross/pullback + SMA9 hourly, 5% trailing stop."""
        try:
            # ── 1. Build candidate list (35-150% gap, RVOL >= 2.0, news, near/above VWAP) ──
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
                if price < STRATEGY_MIN_PRICE:
                    continue
                # Skip if too far below VWAP
                vwap = d.get('vwap', 0)
                if vwap > 0 and price < vwap:
                    if round((vwap - price) / price * 100, 1) > ALERT_VWAP_MAX_BELOW_PCT:
                        continue
                # RVOL filter
                rvol = d.get('rvol', 0)
                if rvol < MR_LIVE_RVOL_MIN:
                    continue
                # News filter
                if MR_LIVE_REQUIRE_NEWS and not _stock_has_news(sym):
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

            # Sort by gap % descending, limit to TOP 3
            candidates.sort(key=lambda c: c.gap_pct, reverse=True)
            candidates = candidates[:3]

            # SMA9 filter (only on final candidates to limit IBKR calls)
            candidates = [
                c for c in candidates
                if _check_above_sma9(c.symbol, c.current_price, c.contract)
            ]
            if not candidates:
                return

            log.info(f"MR candidates ({len(candidates)}): "
                     + " | ".join(f"{c.symbol} +{c.gap_pct:.0f}%" for c in candidates))

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
                # Duplicate-entry guard: skip if already holding this symbol
                if entry.symbol in self._mr_portfolio.positions:
                    self._mr_strategy.mark_position_closed(entry.symbol)
                    continue

                scan_price = current.get(entry.symbol, {}).get('price', entry.price)
                entry_price = scan_price if scan_price > 0 else entry.price

                # Min price filter
                if entry_price < STRATEGY_MIN_PRICE:
                    self._mr_strategy.mark_position_closed(entry.symbol)
                    log.info(f"MR: Skipping {entry.symbol} — price ${entry_price:.4f} < min ${STRATEGY_MIN_PRICE}")
                    continue
                # Risk controls
                can_buy, reason = self._mr_portfolio.can_enter(entry.symbol)
                if not can_buy:
                    self._mr_strategy.mark_position_closed(entry.symbol)
                    log.info(f"MR: Skipping {entry.symbol} — {reason}")
                    continue

                qty = int(min(self._mr_portfolio.cash, MR_LIVE_POSITION_SIZE_FIXED) / entry_price) if entry_price > 0 else 0
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

            # Cache 1-min bars from MR strategy for Doji/MA alerts
            for sym, raw_bars in self._mr_strategy._last_raw_bars.items():
                if raw_bars and len(raw_bars) >= 5:
                    try:
                        df = ib_util.df(raw_bars)
                        _intraday_cache[f"{sym}_1 min"] = (time_mod.time(), df)
                    except Exception:
                        pass

        except Exception as e:
            log.error(f"MR cycle error: {e}")

    def _run_float_turnover_cycle(self, current: dict):
        """Run Float Turnover auto-strategy: 1-min bar high/low logic on high-turnover stocks."""
        try:
            # ── 1. Build candidate list (≥25% turnover + 30% gap + RVOL 3x + news) ──
            candidates = []
            for sym, d in current.items():
                price = d.get('price', 0)
                contract = d.get('contract')
                if price <= 0 or not contract:
                    continue
                if price < STRATEGY_MIN_PRICE:
                    continue
                # Gap % filter (directional confirmation)
                pct = d.get('pct', 0)
                if pct < FT_LIVE_GAP_MIN_PCT:
                    continue
                # RVOL filter
                rvol = d.get('rvol', 0)
                if rvol < FT_LIVE_RVOL_MIN:
                    continue
                # News filter
                if FT_LIVE_REQUIRE_NEWS and not _stock_has_news(sym):
                    continue

                # Skip if too far below VWAP
                vwap = d.get('vwap', 0)
                if vwap > 0 and price < vwap:
                    if round((vwap - price) / price * 100, 1) > ALERT_VWAP_MAX_BELOW_PCT:
                        continue

                enrich = _enrichment.get(sym, {})
                if not enrich:
                    continue

                float_shares = _parse_float_to_shares(enrich.get('float', '-'))
                if float_shares <= 0:
                    continue

                vol_raw = d.get('volume_raw', 0)
                if vol_raw <= 0:
                    continue

                turnover = vol_raw / float_shares * 100
                if turnover < FT_MIN_FLOAT_TURNOVER_PCT:
                    continue

                candidates.append(FTCandidate(
                    symbol=sym,
                    contract=contract,
                    price=price,
                    float_shares=float_shares,
                    turnover_pct=turnover,
                    vwap=vwap,
                ))

            if not candidates:
                return

            # Sort by turnover descending, limit to TOP 3
            candidates.sort(key=lambda c: c.turnover_pct, reverse=True)
            candidates = candidates[:3]

            # SMA9 filter (only on final candidates to limit IBKR calls)
            candidates = [
                c for c in candidates
                if _check_above_sma9(c.symbol, c.price, c.contract)
            ]
            if not candidates:
                return

            log.info(f"FT candidates ({len(candidates)}): "
                     + " | ".join(f"{c.symbol} {c.turnover_pct:.0f}%" for c in candidates))

            # ── 2. Run strategy cycle ──
            entries, exits = self._ft_strategy.process_cycle(candidates)

            # ── 3. Execute exits first ──
            for exit_sig in exits:
                price = current.get(exit_sig.symbol, {}).get('price', exit_sig.price)
                alert = self._ft_portfolio.sell(exit_sig.symbol, price, exit_sig.reason)
                if alert:
                    send_telegram(alert)
                    self._ft_strategy.mark_position_closed(exit_sig.symbol)
                    _log_daily_event('ft_exit', exit_sig.symbol,
                                     f"[FT-SIM] SELL @ ${price:.4f} ({exit_sig.reason})")

            # ── 4. Execute entries ──
            for entry in entries:
                # Duplicate-entry guard: skip if already holding this symbol
                if entry.symbol in self._ft_portfolio.positions:
                    self._ft_strategy.mark_position_closed(entry.symbol)
                    continue

                scan_price = current.get(entry.symbol, {}).get('price', entry.price)
                entry_price = scan_price if scan_price > 0 else entry.price

                # Min price filter
                if entry_price < STRATEGY_MIN_PRICE:
                    self._ft_strategy.mark_position_closed(entry.symbol)
                    log.info(f"FT: Skipping {entry.symbol} — price ${entry_price:.4f} < min ${STRATEGY_MIN_PRICE}")
                    continue
                # Risk controls
                can_buy, reason = self._ft_portfolio.can_enter(entry.symbol)
                if not can_buy:
                    self._ft_strategy.mark_position_closed(entry.symbol)
                    log.info(f"FT: Skipping {entry.symbol} — {reason}")
                    continue

                qty = int(min(self._ft_portfolio.cash, FT_LIVE_POSITION_SIZE_FIXED) / entry_price) if entry_price > 0 else 0
                qty = min(qty, 5000)  # absolute max shares
                if qty >= 1:
                    alert = self._ft_portfolio.buy(
                        entry.symbol, qty, entry_price, entry.turnover_pct,
                    )
                    if alert:
                        send_telegram(alert)
                        _log_daily_event('ft_entry', entry.symbol,
                                         f"[FT-SIM] BUY {qty}sh @ ${entry_price:.4f} "
                                         f"turnover={entry.turnover_pct:.0f}%")
                    else:
                        # Buy failed — rollback strategy state
                        self._ft_strategy.mark_position_closed(entry.symbol)
                        log.warning(f"FT: Virtual buy failed for {entry.symbol}")
                else:
                    # Insufficient cash — rollback strategy state
                    self._ft_strategy.mark_position_closed(entry.symbol)
                    log.warning(f"FT: Insufficient virtual cash for {entry.symbol} "
                                f"(cash=${self._ft_portfolio.cash:.0f}, price=${entry_price:.4f})")

        except Exception as e:
            log.error(f"FT cycle error: {e}")

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
        _t0 = time_mod.time()
        def _phase(name):
            elapsed = time_mod.time() - _t0
            if elapsed > 10:
                log.info(f"  cycle phase '{name}' @ {elapsed:.1f}s")

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

        # ── Dual scan: OCR + IBKR scanner list, merge results ──
        _phase('scan_start')
        ocr_data = _run_ocr_scan(self.price_min, self.price_max)
        _phase('ocr_done')

        # Fast IBKR scanner list (symbols + contracts only, no enrichment)
        ibkr_list = _run_ibkr_scanner_list(self.price_min, self.price_max)
        _phase('ibkr_list_done')

        # Merge: OCR provides real-time prices, IBKR adds missing stocks + contracts
        # Limit IBKR-only additions to avoid slow enrichment cycles (each needs reqHistoricalData)
        _MAX_IBKR_ONLY = 10
        current = dict(ocr_data) if ocr_data else {}
        ibkr_added = 0
        for sym, contract in ibkr_list:
            if sym in current:
                # OCR already has this stock — just add the contract if missing
                if not current[sym].get('contract'):
                    current[sym]['contract'] = contract
            elif ibkr_added < _MAX_IBKR_ONLY:
                # IBKR-only stock — add with minimal data (enrichment will fill price/pct)
                current[sym] = {
                    'sym': sym, 'price': 0, 'pct': 0, 'volume': '',
                    'volume_raw': 0, 'contract': contract,
                }
                ibkr_added += 1
            else:
                # Just cache the contract for later use (no enrichment this cycle)
                if contract.conId and sym not in _contract_cache:
                    _contract_cache[sym] = contract

        ocr_count = len(ocr_data) if ocr_data else 0
        scan_source = "OCR+IBKR" if ocr_count > 0 else "IBKR"
        if ibkr_added:
            log.info(f"Scan merge: {ocr_count} OCR + {ibkr_added} IBKR-only = {len(current)} total")
        _phase('scan_done')

        # Track when each stock was last found by a fresh scan
        now_ts = time_mod.time()
        for sym in current:
            self._last_seen_in_scan[sym] = now_ts

        # Keep enriched stocks from previous scan that OCR missed this cycle.
        # OCR is unreliable — symbols flicker due to misreads (I→l, spaces in numbers).
        # Only carry over if stock was seen by a scan within the last 5 minutes;
        # beyond that it has genuinely dropped off the scanner.
        _CARRYOVER_MAX_AGE = 60  # 1 minute
        if "OCR" in scan_source and self.previous:
            kept = 0
            for sym, prev_d in self.previous.items():
                if sym not in current and sym in _enrichment:
                    last_seen = self._last_seen_in_scan.get(sym, 0)
                    if now_ts - last_seen <= _CARRYOVER_MAX_AGE:
                        current[sym] = dict(prev_d)
                        kept += 1
            if kept:
                log.debug(f"Kept {kept} enriched stocks from previous scan (OCR miss, <5min)")

        # IBKR enrichment for stocks that need price/VWAP/RVOL data
        if current:
            # Enrich: momentum stocks (>=16%) + IBKR-only stocks (have contract but no price)
            momentum_syms = [s for s, d in current.items()
                             if d.get('pct', 0) >= 16.0
                             or (d.get('contract') and d.get('price', 0) <= 0)]
            if momentum_syms:
                if self.on_status:
                    self.on_status(f"Enriching {len(momentum_syms)} momentum stocks via IBKR...")
                _phase('ibkr_enrich_start')
                _enrich_with_ibkr(current, momentum_syms)
                _phase('ibkr_enrich_done')

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
        _phase('finviz_enrich_start')
        enriched_count = 0
        for sym in new_syms:
            if sym in _enrichment:
                continue
            d = current[sym]
            if self.on_status:
                self.on_status(f"#{self.count}  Enriching {sym}... ({enriched_count+1}/{len(new_syms)})")
            _enrich_stock(sym, d['price'], on_status=self.on_status)

            # Filter: send Telegram report if above VWAP + has news + $60K min volume
            enrich = _enrichment.get(sym, {})
            vwap = d.get('vwap', 0)
            pct = d.get('pct', 0)
            price = d.get('price', 0)
            above_vwap = vwap > 0 and price > vwap
            has_news = bool(enrich.get('news'))
            report_vol_raw = d.get('volume_raw', 0)
            report_dol_vol = report_vol_raw * price if report_vol_raw and price else 0
            report_rvol = d.get('rvol', 0)
            passes_vol_filter = report_dol_vol >= _MIN_DOLLAR_VOL_ALERT or report_rvol >= _MIN_RVOL_ALERT

            if above_vwap and has_news and passes_vol_filter and not self._warmup:
                _send_unified_report(sym, d, enrich)
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
            elif not passes_vol_filter and above_vwap and has_news:
                log.info(f"Filtered {sym}: dol_vol={_format_dollar_short(report_dol_vol)} rvol={report_rvol:.1f}x — need $60K+ or פי 3+")
            else:
                log.info(f"Filtered {sym}: pct={pct:+.1f}% vwap={'above' if above_vwap else 'below'} float={enrich.get('float', '-')} news={'yes' if has_news else 'no'} dol_vol={_format_dollar_short(report_dol_vol)}")

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

        # ── Baseline: no summary needed (individual reports are sent) ──

        # ── Real-time alerts (enriched ≥20% stocks) ──
        if not is_baseline and current and not self._warmup:
            # Grouped alert collection: {sym: [compact_line, ...]}
            batch_by_sym: dict[str, list[str]] = {}
            batch_tags: dict[str, list[str]] = {}   # Hebrew tags per symbol
            batch_full_texts: list[str] = []   # full texts for GUI display
            batch_syms: list[str] = []         # ordered unique symbols

            _ALERT_TAG_HE = {
                'HOD Break': 'שיא יומי', 'FIB Touch': 'נגיעת פיבו',
                'FIB Support': 'תמיכת פיבו', 'LOD Touch': 'נמוך יומי',
                'VWAP Cross': 'חציית VWAP', 'Spike': 'קפיצה',
                'Volume': 'ווליום חריג', 'Doji': 'פריצת דוג׳י',
                'TF High': 'שבירת גבוה',
            }

            def _collect(sym, result, sound_type, counter_name):
                if result is None:
                    return
                full, compact = result
                batch_full_texts.append(full)
                batch_by_sym.setdefault(sym, []).append(compact)
                batch_tags.setdefault(sym, []).append(_ALERT_TAG_HE.get(counter_name, counter_name))
                # play_alert_sound(sound_type)  # disabled — Telegram alerts only
                _daily_alert_counts[counter_name] = _daily_alert_counts.get(counter_name, 0) + 1
                _alerts_per_stock.setdefault(sym, {})
                _alerts_per_stock[sym][counter_name] = _alerts_per_stock[sym].get(counter_name, 0) + 1
                if sym not in batch_syms:
                    batch_syms.append(sym)
                _alerted_at[sym] = time_mod.time()

            for sym, d in current.items():
                if sym not in _enrichment:
                    continue
                price = d.get('price', 0)
                pct = d.get('pct', 0)
                vwap = d.get('vwap', 0)

                # Filter: need $60K+ dollar volume OR RVOL 3x+
                vol_raw = d.get('volume_raw', 0)
                dollar_vol = vol_raw * price if vol_raw and price else 0
                rvol_val = d.get('rvol', 0)
                if dollar_vol < _MIN_DOLLAR_VOL_ALERT and rvol_val < _MIN_RVOL_ALERT:
                    continue

                # Skip alerts if price is below VWAP by more than 5%
                # (above VWAP or within 5% below → OK)
                if vwap > 0 and price > 0 and price < vwap:
                    below_pct = round((vwap - price) / price * 100, 1)
                    if below_pct > ALERT_VWAP_MAX_BELOW_PCT:
                        continue

                prev_d = self.previous.get(sym, {}) if self.previous else {}

                _collect(sym, check_hod_break(sym, d, prev_d), 'hod', 'HOD Break')
                _collect(sym, check_fib_second_touch(sym, price, pct), 'fib', 'FIB Touch')
                _collect(sym, check_fib_support_hold(sym, price, pct), 'fib', 'FIB Support')
                day_low = d.get('day_low', 0)
                _collect(sym, check_lod_touch(sym, price, day_low, pct), 'lod', 'LOD Touch')
                _collect(sym, check_vwap_cross(sym, price, vwap, pct), 'vwap', 'VWAP Cross')
                _collect(sym, check_spike(sym, price, pct), 'spike', 'Spike')
                rvol = d.get('rvol', 0)
                _collect(sym, check_volume_alert(sym, price, vwap, rvol, pct), 'vwap', 'Volume')
                _collect(sym, check_doji_candle(sym, price, pct), 'spike', 'Doji')
                _collect(sym, check_timeframe_high_break(sym, price, pct), 'hod', 'TF High')

            # Send all alerts — chart image per symbol (no text messages)
            if batch_by_sym:
                total_alerts = sum(len(v) for v in batch_by_sym.values())

                for s in batch_syms:
                    sd_s = current.get(s, {})
                    sp = sd_s.get('price', 0)
                    if sp <= 0:
                        continue
                    # Build alert reason for chart title
                    sym_alerts = batch_by_sym.get(s, [])
                    if len(sym_alerts) == 1 and len(batch_syms) == 1:
                        # Single alert — use full text (richer)
                        reason = batch_full_texts[0]
                    else:
                        # Multiple alerts — join compact lines
                        reason = "\n".join(f"  {a}" for a in sym_alerts) if sym_alerts else ""
                    enrich_s = _enrichment.get(s, {})
                    tags = batch_tags.get(s, [])
                    _send_unified_report(s, sd_s, enrich_s,
                                         alert_reason=reason,
                                         alert_tags=tags)

                status += f"  🔔{total_alerts}"
            else:
                status += "  ✓"

        # ── Fetch account data (for daily summary) ──
        _phase('alerts_done')
        self._fetch_account_data()

        # ── Monitor virtual portfolio positions for stops/targets ──
        self._monitor_virtual_positions(current)

        # ── Force-close positions that exceeded max hold time ──
        self._check_timed_exits(current)

        # ── FIB DT Auto-Strategy ──
        _phase('fib_dt_start')
        self._run_fib_dt_cycle(current, status)
        _phase('fib_dt_done')

        # ── Gap and Go Auto-Strategy ──
        self._run_gap_go_cycle(current)
        _phase('gg_done')

        # ── Momentum Ride Auto-Strategy ──
        self._run_momentum_ride_cycle(current)
        _phase('mr_done')

        # ── Float Turnover Auto-Strategy ──
        self._run_float_turnover_cycle(current)
        _phase('ft_done')

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
            ft_summary = self._ft_portfolio.summary_text(vp_prices) if self._ft_portfolio else ''
            _check_daily_summary(
                positions=self._cached_positions,
                net_liq=self._cached_net_liq,
                buying_power=self._cached_buying_power,
                fib_dt_sym=self._fib_dt_current_sym,
                cycle_count=self.count,
                virtual_portfolio_summary=vp_summary,
                gg_portfolio_summary=gg_summary,
                mr_portfolio_summary=mr_summary,
                ft_portfolio_summary=ft_summary,
            )
            _check_weekly_report(self._virtual_portfolio, self._gg_portfolio, self._mr_portfolio, self._ft_portfolio, vp_prices)
            _check_weekly_reset(self._virtual_portfolio, self._gg_portfolio, self._mr_portfolio, self._ft_portfolio)

        # Float report runs even during warmup (for force-flag support)
        _check_float_report(current)

        # Alert summary + news summary every 30 minutes
        if not self._warmup:
            _check_alert_summary(current)
            _check_news_summary(current)

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
                    _send_unified_report(sym, d, enrich)
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
        self._selected_symbol_name: str | None = None
        self._cached_net_liq: float = 0.0
        self._cached_buying_power: float = 0.0
        self._cached_positions: dict[str, tuple] = {}  # {sym: (qty, avgCost, mktPrice, pnl)}
        # Scanner 1 (left) widgets
        self._row_widgets: dict[str, dict] = {}   # sym → cached label widgets
        self._rendered_order: list[str] = []       # last symbol render order
        # Scanner 2 (right) widgets
        self._row_widgets_r: dict[str, dict] = {}
        self._rendered_order_r: list[str] = []
        # Portfolio widgets
        self._portfolio_widgets: dict[str, dict] = {}  # sym → cached portfolio widgets
        self._portfolio_order: list[str] = []
        # Separate order thread — starts immediately, independent of scanner
        self._order_thread = OrderThread(
            host=IBKR_HOST, port=IBKR_PORT,
            on_account=self._on_account_data,
            on_order_result=self._on_order_result,
            telegram_fn=send_telegram,
            market_session_fn=_get_market_session,
        )
        self._order_thread.start()

        self.root = tk.Tk()
        self.root.title("IBKR Scanner Monitor")
        self.root.geometry("1600x950")
        self.root.attributes('-topmost', True)
        self.root.configure(bg=self.BG, highlightbackground=self.ACCENT,
                            highlightcolor=self.ACCENT, highlightthickness=2)
        self.root.resizable(True, True)

        # Font settings (user-configurable)
        self._table_font_var = tk.StringVar(value="Courier")
        self._table_size_var = tk.IntVar(value=14)

        # Header
        hdr = tk.Frame(self.root, bg=self.BG)
        hdr.pack(fill='x', padx=10, pady=(6, 0))
        tk.Label(hdr, text="IBKR SCANNER", font=("Helvetica", 20, "bold"),
                 bg=self.BG, fg=self.ACCENT).pack(side='left')

        # Account summary (inline with header)
        self._hdr_account_var = tk.StringVar(value="")
        tk.Label(hdr, textvariable=self._hdr_account_var,
                 font=("Courier", 12, "bold"), bg=self.BG, fg="#aaa"
                 ).pack(side='left', padx=(20, 0))

        # Connection status (inline with header, right side)
        self.conn_var = tk.StringVar(value="Checking...")
        self.conn_label = tk.Label(hdr, textvariable=self.conn_var,
                                   font=("Courier", 13, "bold"), bg=self.BG, fg="#888")
        self.conn_label.pack(side='right')

        tk.Frame(self.root, bg=self.ACCENT, height=1).pack(fill='x', padx=10, pady=3)

        # ── Scanner range variables ──
        self._scan1_min_pct = tk.DoubleVar(value=20.0)
        self._scan1_max_pct = tk.DoubleVar(value=200.0)
        self._scan2_min_pct = tk.DoubleVar(value=10.0)
        self._scan2_max_pct = tk.DoubleVar(value=20.0)

        # ── Main content: dual scanners side-by-side ──
        content = tk.Frame(self.root, bg=self.BG)
        content.pack(fill='both', expand=True, padx=10, pady=2)

        # ── Scanner 1 (left) ──
        left_scanner = tk.Frame(content, bg=self.BG)
        left_scanner.pack(side='left', fill='both', expand=True)

        scan1_hdr = tk.Frame(left_scanner, bg=self.BG)
        scan1_hdr.pack(fill='x', pady=(0, 2))
        tk.Label(scan1_hdr, text="SCANNER 1", font=("Helvetica", 11, "bold"),
                 bg=self.BG, fg=self.ACCENT).pack(side='left')
        tk.Label(scan1_hdr, text="Min%:", font=("Helvetica", 10),
                 bg=self.BG, fg="#888").pack(side='left', padx=(8, 2))
        tk.Spinbox(scan1_hdr, from_=0, to=500, increment=5, textvariable=self._scan1_min_pct,
                   width=4, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=(0, 4))
        tk.Label(scan1_hdr, text="Max%:", font=("Helvetica", 10),
                 bg=self.BG, fg="#888").pack(side='left', padx=(4, 2))
        tk.Spinbox(scan1_hdr, from_=0, to=1000, increment=10, textvariable=self._scan1_max_pct,
                   width=4, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=(0, 4))

        self._hdr_frame = tk.Frame(left_scanner, bg=self.BG)
        self._hdr_frame.pack(fill='x')

        list_frame = tk.Frame(left_scanner, bg=self.BG)
        list_frame.pack(fill='both', expand=True, pady=1)

        self.canvas = tk.Canvas(list_frame, bg=self.BG, highlightthickness=0)
        scrollbar = tk.Scrollbar(list_frame, orient='vertical', command=self.canvas.yview)
        self.stock_frame = tk.Frame(self.canvas, bg=self.BG)

        self.stock_frame.bind('<Configure>',
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox('all')))
        self.canvas.create_window((0, 0), window=self.stock_frame, anchor='nw')
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        def _on_mousewheel_l(event):
            self.canvas.yview_scroll(-1 * (event.delta // 120 or (1 if event.num == 4 else -1)), "units")
        self.canvas.bind('<Button-4>', _on_mousewheel_l)
        self.canvas.bind('<Button-5>', _on_mousewheel_l)
        self.stock_frame.bind('<Button-4>', _on_mousewheel_l)
        self.stock_frame.bind('<Button-5>', _on_mousewheel_l)

        # ── Separator ──
        tk.Frame(content, bg=self.ACCENT, width=2).pack(side='left', fill='y', padx=4)

        # ── Scanner 2 (right) ──
        right_scanner = tk.Frame(content, bg=self.BG)
        right_scanner.pack(side='right', fill='both', expand=True)

        scan2_hdr = tk.Frame(right_scanner, bg=self.BG)
        scan2_hdr.pack(fill='x', pady=(0, 2))
        tk.Label(scan2_hdr, text="SCANNER 2", font=("Helvetica", 11, "bold"),
                 bg=self.BG, fg=self.ACCENT).pack(side='left')
        tk.Label(scan2_hdr, text="Min%:", font=("Helvetica", 10),
                 bg=self.BG, fg="#888").pack(side='left', padx=(8, 2))
        tk.Spinbox(scan2_hdr, from_=0, to=500, increment=5, textvariable=self._scan2_min_pct,
                   width=4, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=(0, 4))
        tk.Label(scan2_hdr, text="Max%:", font=("Helvetica", 10),
                 bg=self.BG, fg="#888").pack(side='left', padx=(4, 2))
        tk.Spinbox(scan2_hdr, from_=0, to=1000, increment=10, textvariable=self._scan2_max_pct,
                   width=4, font=("Helvetica", 10), bg=self.ROW_BG, fg=self.FG,
                   buttonbackground=self.ROW_BG, relief='flat').pack(side='left', padx=(0, 4))

        self._hdr_frame_r = tk.Frame(right_scanner, bg=self.BG)
        self._hdr_frame_r.pack(fill='x')

        list_frame_r = tk.Frame(right_scanner, bg=self.BG)
        list_frame_r.pack(fill='both', expand=True, pady=1)

        self.canvas_r = tk.Canvas(list_frame_r, bg=self.BG, highlightthickness=0)
        scrollbar_r = tk.Scrollbar(list_frame_r, orient='vertical', command=self.canvas_r.yview)
        self.stock_frame_r = tk.Frame(self.canvas_r, bg=self.BG)

        self.stock_frame_r.bind('<Configure>',
            lambda e: self.canvas_r.configure(scrollregion=self.canvas_r.bbox('all')))
        self.canvas_r.create_window((0, 0), window=self.stock_frame_r, anchor='nw')
        self.canvas_r.configure(yscrollcommand=scrollbar_r.set)

        self.canvas_r.pack(side='left', fill='both', expand=True)
        scrollbar_r.pack(side='right', fill='y')

        def _on_mousewheel_r(event):
            self.canvas_r.yview_scroll(-1 * (event.delta // 120 or (1 if event.num == 4 else -1)), "units")
        self.canvas_r.bind('<Button-4>', _on_mousewheel_r)
        self.canvas_r.bind('<Button-5>', _on_mousewheel_r)
        self.stock_frame_r.bind('<Button-4>', _on_mousewheel_r)
        self.stock_frame_r.bind('<Button-5>', _on_mousewheel_r)

        self._rebuild_column_headers()

        # ── Bottom Panel: Portfolio + Account + Trades ──
        tk.Frame(self.root, bg="#444", height=1).pack(fill='x', padx=10, pady=2)
        bottom = tk.Frame(self.root, bg=self.BG, height=200)
        bottom.pack(fill='x', padx=10, pady=2)
        bottom.pack_propagate(True)

        # Left side: account summary + IBKR positions
        left_port = tk.Frame(bottom, bg=self.BG)
        left_port.pack(side='left', fill='both', expand=True)

        # Account summary row
        acct_row = tk.Frame(left_port, bg=self.BG)
        acct_row.pack(fill='x')
        tk.Label(acct_row, text="ACCOUNT", font=("Helvetica", 11, "bold"),
                 bg=self.BG, fg=self.ACCENT).pack(side='left', padx=(0, 8))
        self._acct_nlv_lbl = tk.Label(acct_row, text="NLV: ---", font=("Courier", 11),
                                      bg=self.BG, fg=self.FG)
        self._acct_nlv_lbl.pack(side='left', padx=(0, 10))
        self._acct_bp_lbl = tk.Label(acct_row, text="BP: ---", font=("Courier", 11),
                                     bg=self.BG, fg=self.FG)
        self._acct_bp_lbl.pack(side='left', padx=(0, 10))
        self._acct_pos_lbl = tk.Label(acct_row, text="Pos: 0", font=("Courier", 11),
                                      bg=self.BG, fg=self.FG)
        self._acct_pos_lbl.pack(side='left', padx=(0, 10))
        self._acct_pnl_lbl = tk.Label(acct_row, text="P&L: ---", font=("Courier", 11, "bold"),
                                      bg=self.BG, fg='#888')
        self._acct_pnl_lbl.pack(side='left')

        # Positions row (compact, inline with account)
        self._portfolio_frame = tk.Frame(left_port, bg=self.BG)
        self._portfolio_frame.pack(fill='x', pady=1)

        # Trades header
        trade_hdr = tk.Frame(left_port, bg=self.BG)
        trade_hdr.pack(fill='x', pady=(2, 0))
        tk.Label(trade_hdr, text="עסקאות", font=("Helvetica", 10, "bold"),
                 bg=self.BG, fg="#888").pack(side='left', padx=(0, 6))
        for text, w in [("תאריך", 6), ("שעה", 5), ("רובוט", 5), ("מניה", 6),
                         ("כניסה", 7), ("יציאה", 7), ("ר/ה$", 8), ("ר/ה%", 6)]:
            tk.Label(trade_hdr, text=text, font=("Courier", 9, "bold"),
                     bg=self.BG, fg=self.ACCENT, width=w, anchor='w').pack(side='left')
        self._trades_frame = tk.Frame(left_port, bg=self.BG)
        self._trades_frame.pack(fill='x', pady=1)

        # 2px separator
        tk.Frame(bottom, bg='#444', width=2).pack(side='left', fill='y', padx=6)

        # Right side: SPY chart
        right_port = tk.Frame(bottom, bg='#0e1117', width=420)
        right_port.pack(side='right', fill='both')
        right_port.pack_propagate(False)
        tk.Label(right_port, text="SPY", font=("Helvetica", 10, "bold"),
                 bg='#0e1117', fg=self.ACCENT).pack(anchor='w', padx=4)
        self._spy_chart_label = tk.Label(right_port, bg='#0e1117')
        self._spy_chart_label.pack(fill='both', expand=True)
        self._spy_chart_image = None  # keep reference to prevent GC

        # Init trading variables (no GUI panel — right-click menu instead)
        self._init_trading_vars()

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

        # Sound controls + Font + Start/Stop row
        ctrl_row = tk.Frame(self.root, bg=self.BG)
        ctrl_row.pack(fill='x', padx=10, pady=(4, 0))

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

        self.btn = tk.Button(ctrl_row, text="START", font=("Helvetica", 16, "bold"),
                             bg=self.GREEN, fg="white", command=self._toggle,
                             relief='flat', activebackground="#00a844")
        self.btn.pack(side='left', fill='x', expand=True, ipady=2)

        # Status
        self.status = tk.StringVar(value="Ready")
        tk.Label(self.root, textvariable=self.status, font=("Courier", 11),
                 bg=self.BG, fg="#888", wraplength=1000, justify='left'
                 ).pack(padx=10, pady=1, anchor='w')

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
        nl = self._cached_net_liq
        n_pos = len(self._cached_positions)

        if scan_ok and order_ok:
            acct_info = f"${nl:,.0f}" if nl > 0 else "..."
            pos_info = f" | {n_pos} pos" if n_pos > 0 else ""
            self.conn_var.set(f"Scan ✓ | Orders ✓ | {acct_info}{pos_info}")
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

    def _update_prices_inplace(self, changes: dict):
        """In-place price/pct update — no widget rebuild, no flicker.

        changes: {sym: (price, pct)}
        Only touches existing labels in _row_widgets.
        """
        def _do():
            for sym, (price, pct) in changes.items():
                for widgets in (self._row_widgets, self._row_widgets_r):
                    w = widgets.get(sym)
                    if not w:
                        continue
                    price_text = f"${price:.2f}" if price >= 1 else f"${price:.4f}"
                    w['price_lbl'].config(text=price_text)
                    pct_text = f"{pct:+.1f}%"
                    pct_fg = self.GREEN if pct >= 0 else self.RED
                    w['pct_lbl'].config(text=pct_text, fg=pct_fg)
            # Update trade price for selected stock
            sel = self._selected_symbol_name
            if sel and sel in changes:
                self._trade_price.set(f"{changes[sel][0]:.2f}")
        self.root.after(0, _do)

    def _update_stock_table(self, stocks: dict):
        """Update the stock table in the GUI (called from scanner thread)."""
        self._stock_data = stocks
        self._accumulate_spark_data()
        self.root.after(0, self._render_stock_table)
        self.root.after(0, self._render_stock_table_r)
        self.root.after(0, self._render_spy_chart)
        self.root.after(0, self._render_recent_trades)

    def _compute_row_data(self, sym: str, d: dict, idx: int) -> dict:
        """Compute display values for a stock row."""
        is_selected = (sym == self._selected_symbol_name)
        # Yellow background if alerted in last 60 seconds
        alert_ts = _alerted_at.get(sym, 0)
        is_alerted = (time_mod.time() - alert_ts) < 60 if alert_ts else False
        if is_selected:
            bg = self.SELECTED_BG
        elif is_alerted:
            bg = "#3d3d1a"  # dark yellow tint
        else:
            bg = self.ROW_BG if idx % 2 == 0 else self.ROW_ALT
        enrich = d.get('enrich', {})

        vol_raw = d.get('volume_raw', 0)
        float_shares = _parse_float_to_shares(enrich.get('float', '-')) if enrich else 0
        turnover = (vol_raw / float_shares * 100) if float_shares > 0 and vol_raw > 0 else 0
        is_hot = turnover >= 10.0  # volume > 10% of float

        # Symbol color: hot → orange, above VWAP → green, else white
        vwap = d.get('vwap', 0)
        price = d.get('price', 0)
        above_vwap = vwap > 0 and price > vwap
        sym_text = f"🔥{sym}" if is_hot else sym
        if is_hot:
            sym_fg = "#ff6600"
        elif above_vwap:
            sym_fg = self.GREEN
        else:
            sym_fg = self.FG

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

    def _build_stock_row(self, sym: str, rd: dict, parent: tk.Frame = None) -> dict:
        """Create widget row for a stock and return widget refs."""
        parent = parent or self.stock_frame
        ff = self._table_font_var.get()
        fs = self._table_size_var.get()
        font_b = (ff, fs, "bold")
        font_r = (ff, fs)
        _click = lambda e, s=sym: self._select_stock(s)
        _dbl_click = lambda e, s=sym: self._open_stock_detail(s)

        _rclick = lambda e, s=sym: self._on_stock_right_click(s, e)

        row1 = tk.Frame(parent, bg=rd['bg'])
        row1.pack(fill='x', pady=0)
        row1.bind('<Button-1>', _click)
        row1.bind('<Double-Button-1>', _dbl_click)
        row1.bind('<Button-3>', _rclick)

        # Sparkline mini-chart
        spark_w, spark_h = 60, 18
        spark_canvas = tk.Canvas(row1, width=spark_w, height=spark_h,
                                 bg=rd['bg'], highlightthickness=0)
        spark_canvas.pack(side='left', padx=(2, 2))
        spark_canvas.bind('<Button-1>', _click)
        spark_canvas.bind('<Button-3>', _rclick)
        self._draw_sparkline(spark_canvas, sym, spark_w, spark_h)

        sym_lbl = tk.Label(row1, text=rd['sym_text'], font=font_b,
                           bg=rd['bg'], fg=rd['sym_fg'], width=7, anchor='w')
        sym_lbl.pack(side='left'); sym_lbl.bind('<Button-1>', _click); sym_lbl.bind('<Double-Button-1>', _dbl_click); sym_lbl.bind('<Button-3>', _rclick)

        price_lbl = tk.Label(row1, text=rd['price_text'], font=font_r,
                             bg=rd['bg'], fg=self.FG, width=8, anchor='w')
        price_lbl.pack(side='left'); price_lbl.bind('<Button-1>', _click); price_lbl.bind('<Double-Button-1>', _dbl_click); price_lbl.bind('<Button-3>', _rclick)

        pct_lbl = tk.Label(row1, text=rd['pct_text'], font=font_b,
                           bg=rd['bg'], fg=rd['pct_fg'], width=8, anchor='w')
        pct_lbl.pack(side='left'); pct_lbl.bind('<Button-1>', _click); pct_lbl.bind('<Button-3>', _rclick)

        vol_lbl = tk.Label(row1, text=rd['vol_text'], font=font_r,
                           bg=rd['bg'], fg=rd['vol_fg'], width=7, anchor='w')
        vol_lbl.pack(side='left'); vol_lbl.bind('<Button-1>', _click); vol_lbl.bind('<Button-3>', _rclick)

        rvol_lbl = tk.Label(row1, text=rd['rvol_text'], font=font_b,
                            bg=rd['bg'], fg=rd['rvol_fg'], width=6, anchor='w')
        rvol_lbl.pack(side='left'); rvol_lbl.bind('<Button-1>', _click); rvol_lbl.bind('<Button-3>', _rclick)

        vwap_lbl = tk.Label(row1, text=rd.get('vwap_text', '—'), font=font_r,
                            bg=rd['bg'], fg=rd.get('vwap_fg', '#555'), width=7, anchor='w')
        vwap_lbl.pack(side='left'); vwap_lbl.bind('<Button-1>', _click); vwap_lbl.bind('<Button-3>', _rclick)

        float_lbl = tk.Label(row1, text=rd['float_text'], font=font_r,
                             bg=rd['bg'], fg="#cca0ff", width=7, anchor='w')
        float_lbl.pack(side='left'); float_lbl.bind('<Button-1>', _click); float_lbl.bind('<Button-3>', _rclick)

        short_lbl = tk.Label(row1, text=rd['short_text'], font=font_r,
                             bg=rd['bg'], fg="#ffaa00", width=6, anchor='w')
        short_lbl.pack(side='left'); short_lbl.bind('<Button-1>', _click); short_lbl.bind('<Button-3>', _rclick)

        news_lbl = tk.Label(row1, text=rd.get('news_text', ''), font=font_r,
                            bg=rd['bg'], fg="#ffcc00", width=2, anchor='w')
        news_lbl.pack(side='left'); news_lbl.bind('<Button-1>', _click); news_lbl.bind('<Button-3>', _rclick)


        # Fib row
        row2 = tk.Frame(parent, bg=rd['bg'])
        row2.pack(fill='x', pady=0)
        fib_lbl = tk.Label(row2, text=rd['fib_text'], font=font_r,
                           bg=rd['bg'], fg="#66cccc", anchor='w')
        fib_lbl.pack(side='left', padx=(12, 0))
        if not rd['fib_text']:
            row2.pack_forget()

        # News headlines row
        row3 = tk.Frame(parent, bg=rd['bg'])
        row3.pack(fill='x', pady=0)
        news_hl_lbl = tk.Label(row3, text=rd.get('news_headlines', ''), font=font_r,
                               bg=rd['bg'], fg="#ccaa00", anchor='w', justify='left',
                               wraplength=500)
        news_hl_lbl.pack(side='left', padx=(12, 0))
        if not rd.get('news_headlines'):
            row3.pack_forget()

        return {
            'row1': row1, 'row2': row2, 'row3': row3,
            'sym_lbl': sym_lbl, 'price_lbl': price_lbl, 'pct_lbl': pct_lbl,
            'vol_lbl': vol_lbl, 'rvol_lbl': rvol_lbl,
            'vwap_lbl': vwap_lbl, 'float_lbl': float_lbl, 'short_lbl': short_lbl,
            'news_lbl': news_lbl, 'fib_lbl': fib_lbl, 'news_hl_lbl': news_hl_lbl,
            'spark_canvas': spark_canvas,
        }

    def _draw_sparkline(self, canvas: tk.Canvas, sym: str, w: int, h: int):
        """Draw sparkline: price line + volume bars from IBKR 5-min bars or accumulated data."""
        canvas.delete('all')
        # Prefer IBKR cached bars, fallback to accumulated scan data
        cached = _spark_bars_cache.get(sym)
        if cached and len(cached) >= 2:
            prices = [p for p, v in cached]
            volumes = [v for p, v in cached]
        else:
            prices = self._spark_data.get(sym, [])
            volumes = []

        if len(prices) < 2:
            return
        lo, hi = min(prices), max(prices)
        if hi == lo:
            hi = lo + 0.01
        margin = 1
        aw = w - 2 * margin
        # Volume bars in bottom 30% of canvas
        vol_h = int(h * 0.3)
        price_h = h - vol_h - 1
        n = len(prices)

        # Draw volume bars (if available)
        if volumes and max(volumes) > 0:
            max_vol = max(volumes)
            for i, v in enumerate(volumes):
                x = margin + i * aw / (n - 1)
                bar_h = v / max_vol * vol_h if max_vol > 0 else 0
                if bar_h > 0.5:
                    canvas.create_line(x, h - margin, x, h - margin - bar_h,
                                       fill="#334455", width=1)

        # Draw price line
        points = []
        for i, p in enumerate(prices):
            x = margin + i * aw / (n - 1)
            y = margin + price_h - (p - lo) / (hi - lo) * price_h
            points.append(x)
            points.append(y)
        color = self.GREEN if prices[-1] >= prices[0] else self.RED
        canvas.create_line(*points, fill=color, width=1.5, smooth=True)

    def _accumulate_spark_data(self):
        """Add current prices to sparkline history (called each scan cycle)."""
        for sym, d in self._stock_data.items():
            price = d.get('price', 0)
            if price <= 0:
                continue
            if sym not in self._spark_data:
                self._spark_data[sym] = []
            buf = self._spark_data[sym]
            buf.append(price)
            if len(buf) > 120:
                self._spark_data[sym] = buf[-120:]
            # Also append to IBKR cache if it exists (keep it current)
            cached = _spark_bars_cache.get(sym)
            if cached:
                _spark_bars_cache[sym].append((price, 0))
                if len(_spark_bars_cache[sym]) > 300:
                    _spark_bars_cache[sym] = _spark_bars_cache[sym][-300:]

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
        # Sparkline
        sc = widgets.get('spark_canvas')
        if sc:
            sc.config(bg=bg)
            sym = rd['sym_text'].replace('\U0001f525', '').strip()  # strip fire emoji
            self._draw_sparkline(sc, sym, 60, 18)
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
        lo = self._scan1_min_pct.get()
        hi = self._scan1_max_pct.get()
        sorted_stocks = [(s, d) for s, d in all_sorted if lo <= d.get('pct', 0) <= hi]
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

    def _render_stock_table_r(self):
        """Render scanner 2 stock table from self._stock_data (right panel)."""
        if not self._stock_data:
            for w in self.stock_frame_r.winfo_children():
                w.destroy()
            self._row_widgets_r.clear()
            self._rendered_order_r.clear()
            tk.Label(self.stock_frame_r, text="No stocks yet",
                     bg=self.BG, fg="#666", font=("Helvetica", 20)).pack(pady=10)
            return

        all_sorted = sorted(self._stock_data.items(),
                            key=lambda x: x[1]['pct'], reverse=True)
        lo = self._scan2_min_pct.get()
        hi = self._scan2_max_pct.get()
        sorted_stocks = [(s, d) for s, d in all_sorted if lo <= d.get('pct', 0) <= hi]
        new_order = [sym for sym, _ in sorted_stocks]

        if new_order == self._rendered_order_r:
            for i, (sym, d) in enumerate(sorted_stocks):
                rd = self._compute_row_data(sym, d, i)
                if sym in self._row_widgets_r:
                    self._update_stock_row(self._row_widgets_r[sym], rd)
        else:
            for w in self.stock_frame_r.winfo_children():
                w.destroy()
            self._row_widgets_r.clear()
            self._rendered_order_r = new_order

            for i, (sym, d) in enumerate(sorted_stocks):
                rd = self._compute_row_data(sym, d, i)
                self._row_widgets_r[sym] = self._build_stock_row(sym, rd, parent=self.stock_frame_r)

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

    def _render_account_summary(self):
        """Update account summary labels."""
        nl = self._cached_net_liq
        bp = self._cached_buying_power
        n_pos = len(self._cached_positions)
        total_pnl = sum(pos[3] for pos in self._cached_positions.values() if len(pos) >= 4)

        self._acct_nlv_lbl.config(text=f"NLV: ${nl:,.0f}" if nl > 0 else "NLV: ---")
        self._acct_bp_lbl.config(text=f"BP: ${bp:,.0f}" if bp > 0 else "BP: ---")
        self._acct_pos_lbl.config(text=f"Pos: {n_pos}")
        pnl_fg = self.GREEN if total_pnl >= 0 else self.RED
        self._acct_pnl_lbl.config(
            text=f"P&L: ${total_pnl:+,.2f}" if self._cached_positions else "P&L: ---",
            fg=pnl_fg)

    def _render_spy_chart(self):
        """Render SPY intraday chart in the bottom-right panel."""
        def _generate():
            try:
                df = _download_intraday('SPY', bar_size='5 mins', duration='1 D')
                if df is None or len(df) < 10:
                    return
                fig, ax = plt.subplots(figsize=(4.2, 1.8), facecolor='#0e1117')
                ax.set_facecolor('#0e1117')
                closes = df['close'].values
                xs = range(len(closes))
                color = '#26a69a' if closes[-1] >= closes[0] else '#ef5350'
                ax.plot(xs, closes, color=color, linewidth=1.2)
                ax.fill_between(xs, closes, closes.min(), alpha=0.15, color=color)
                # Current price label
                ax.text(len(closes) - 1, closes[-1], f" ${closes[-1]:.2f}",
                        fontsize=8, color=color, va='center', fontfamily='monospace')
                # Change %
                chg = (closes[-1] / closes[0] - 1) * 100
                chg_c = '#26a69a' if chg >= 0 else '#ef5350'
                ax.set_title(f"SPY {chg:+.2f}%", fontsize=9, color=chg_c,
                             loc='left', fontfamily='monospace', pad=2)
                ax.tick_params(axis='both', colors='#555', labelsize=7)
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['left'].set_color('#333')
                ax.spines['bottom'].set_color('#333')
                ax.yaxis.set_major_formatter(plt.FormatStrFormatter('$%.0f'))
                fig.tight_layout(pad=0.3)
                buf = io.BytesIO()
                fig.savefig(buf, format='png', dpi=100, facecolor='#0e1117')
                plt.close(fig)
                buf.seek(0)
                img = Image.open(buf)
                self.root.after(0, lambda: self._show_spy_image(img))
            except Exception as e:
                log.debug(f"SPY chart render: {e}")
        threading.Thread(target=_generate, daemon=True).start()

    def _show_spy_image(self, img):
        """Display SPY chart image in the panel."""
        try:
            w = self._spy_chart_label.winfo_width() or 400
            h = self._spy_chart_label.winfo_height() or 160
            if w > 10 and h > 10:
                img = img.resize((w, h), Image.LANCZOS)
            photo = ImageTk.PhotoImage(img)
            self._spy_chart_image = photo
            self._spy_chart_label.config(image=photo)
        except Exception as e:
            log.debug(f"SPY image display: {e}")

    def _render_recent_trades(self):
        """Show last trades from the journal with Hebrew labels."""
        for w in self._trades_frame.winfo_children():
            w.destroy()

        try:
            trades = _load_journal_trades()
        except Exception:
            trades = []

        if not trades:
            tk.Label(self._trades_frame, text="אין עסקאות", font=("Courier", 9),
                     bg=self.BG, fg='#555').pack(anchor='w')
            return

        # Robot short names in Hebrew
        _robot_short = {'FIB DT': 'פיבו', 'Gap&Go': 'גאפ', 'MR': 'מומנ', 'FT': 'סיבב'}

        # Take last 10 trades (newest first)
        recent = list(reversed(trades))[:10]

        for i, t in enumerate(recent):
            row_bg = self.ROW_BG if i % 2 == 0 else self.ROW_ALT
            row = tk.Frame(self._trades_frame, bg=row_bg)
            row.pack(fill='x')

            # Date (dd/mm)
            date_str = t.get('date', '')
            date_short = date_str[:5] if date_str else '-'
            tk.Label(row, text=date_short, font=("Courier", 9),
                     bg=row_bg, fg='#888', width=6, anchor='w').pack(side='left')

            # Time
            exit_t = t.get('exit_time', '')
            tk.Label(row, text=exit_t or '-', font=("Courier", 9),
                     bg=row_bg, fg='#888', width=5, anchor='w').pack(side='left')

            # Robot (Hebrew short)
            robot = t.get('robot', '?')
            robot_he = _robot_short.get(robot, robot[:4])
            tk.Label(row, text=robot_he, font=("Courier", 9),
                     bg=row_bg, fg=self.ACCENT, width=5, anchor='w').pack(side='left')

            # Symbol
            sym = t.get('symbol', '?')
            tk.Label(row, text=sym, font=("Courier", 9, "bold"),
                     bg=row_bg, fg=self.FG, width=6, anchor='w').pack(side='left')

            # Entry price
            entry_p = t.get('entry_price', 0)
            tk.Label(row, text=f"${entry_p:.2f}", font=("Courier", 9),
                     bg=row_bg, fg='#aaa', width=7, anchor='w').pack(side='left')

            # Exit price
            exit_p = t.get('exit_price', 0)
            tk.Label(row, text=f"${exit_p:.2f}", font=("Courier", 9),
                     bg=row_bg, fg=self.FG, width=7, anchor='w').pack(side='left')

            # P&L $
            pnl = t.get('pnl', 0)
            pnl_fg = self.GREEN if pnl >= 0 else self.RED
            tk.Label(row, text=f"${pnl:+.2f}", font=("Courier", 9, "bold"),
                     bg=row_bg, fg=pnl_fg, width=8, anchor='w').pack(side='left')

            # P&L %
            pnl_pct = t.get('pnl_pct', 0)
            tk.Label(row, text=f"{pnl_pct:+.1f}%", font=("Courier", 9, "bold"),
                     bg=row_bg, fg=pnl_fg, width=6, anchor='w').pack(side='left')

    # ── Trading (right-click menu) ────────────────────────

    def _init_trading_vars(self):
        """Initialize trading variables (no GUI panel — trading via right-click)."""
        self._selected_sym = tk.StringVar(value="---")
        self._trade_price = tk.StringVar(value="")
        self._position_var = tk.StringVar(value="Pos: ---")
        self._account_var = tk.StringVar(value="Account: ---")
        self._quick_bp_var = tk.StringVar(value="80")
        self._quick_stop_var = tk.StringVar(value="5")
        self._quick_tp_var = tk.StringVar(value="20")
        self._order_status_var = tk.StringVar(value="")
        # Planned trade fields — created as StringVars (no Entry widgets)
        self._planned_entry_var = tk.StringVar(value="")
        self._planned_stop_var = tk.StringVar(value="")
        self._planned_tp_var = tk.StringVar(value="")
        # Dummy label for order status (so _on_order_result doesn't crash)
        self._order_status_label = tk.Label(self.root, textvariable=self._order_status_var,
                                            font=("Courier", 10), bg=self.BG, fg="#888")
        self._order_status_label.pack(side='bottom', fill='x', padx=10)
        # Sparkline price history — {sym: [price, price, ...]}
        self._spark_data: dict[str, list[float]] = {}

    def _on_stock_right_click(self, sym: str, event):
        """Show right-click trade context menu for stock."""
        self._select_stock(sym)
        menu = tk.Menu(self.root, tearoff=0, bg="#2d2d44", fg="#e0e0e0",
                       activebackground="#00d4ff", activeforeground="black",
                       font=("Helvetica", 12))
        d = self._stock_data.get(sym, {})
        price = d.get('price', 0) if d else 0
        pos = self._cached_positions.get(sym)
        pos_text = f"  [{pos[0]} @ ${pos[1]:.2f}]" if pos else ""

        menu.add_command(label=f"{sym}  ${price:.2f}{pos_text}", state='disabled')
        menu.add_separator()
        menu.add_command(label="QUICK BUY", foreground="#00e676",
                         command=self._quick_buy)
        menu.add_command(label="QUICK SELL", foreground="#ff5252",
                         command=self._quick_sell)
        menu.add_command(label="CLOSE ALL", foreground="#ffc107",
                         command=self._close_all_position)
        menu.add_separator()
        menu.add_command(label="Trade Settings...",
                         command=lambda s=sym: self._show_trade_dialog(s))
        menu.add_separator()
        menu.add_command(label="Stock Detail", command=lambda s=sym: self._open_stock_detail(s))
        menu.add_command(label="Open Chart", command=lambda s=sym: self._open_chart_window(s))
        menu.add_command(label="Open TradingView", command=lambda s=sym: self._open_tradingview(s))
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    def _show_trade_dialog(self, sym: str):
        """Show trade settings dialog for quick/planned trades."""
        dlg = tk.Toplevel(self.root)
        dlg.title(f"Trade — {sym}")
        dlg.configure(bg=self.BG)
        dlg.geometry("380x340")
        dlg.attributes('-topmost', True)
        dlg.transient(self.root)

        d = self._stock_data.get(sym, {})
        price = d.get('price', 0) if d else 0
        pos = self._cached_positions.get(sym)

        # Header
        hdr_text = f"{sym}  ${price:.2f}"
        if pos:
            pnl = pos[3] if len(pos) >= 4 else 0
            hdr_text += f"  |  Pos: {pos[0]} @ ${pos[1]:.2f}  P&L: ${pnl:+,.2f}"
        tk.Label(dlg, text=hdr_text, font=("Courier", 13, "bold"),
                 bg=self.BG, fg=self.ACCENT).pack(fill='x', padx=8, pady=(8, 4))

        # Quick Entry fields
        tk.Frame(dlg, bg="#444", height=1).pack(fill='x', padx=8, pady=4)
        tk.Label(dlg, text="Quick Entry", font=("Helvetica", 11, "bold"),
                 bg=self.BG, fg="#888").pack(anchor='w', padx=8)

        qf = tk.Frame(dlg, bg=self.BG)
        qf.pack(fill='x', padx=8, pady=2)
        for label, var, fg in [("BP%", self._quick_bp_var, "#00e676"),
                                ("Stop%", self._quick_stop_var, "#ff5252"),
                                ("TP%", self._quick_tp_var, "#448aff")]:
            tk.Label(qf, text=f"{label}:", font=("Helvetica", 11), bg=self.BG, fg="#ccc").pack(side='left', padx=(0, 2))
            tk.Entry(qf, textvariable=var, width=5, font=("Courier", 12),
                     bg="#333", fg=fg, insertbackground="white", relief='flat').pack(side='left', padx=(0, 8))

        bf = tk.Frame(dlg, bg=self.BG)
        bf.pack(fill='x', padx=8, pady=4)
        tk.Button(bf, text="QUICK BUY", font=("Helvetica", 12, "bold"),
                  bg="#1b5e20", fg="white", relief='flat', padx=12, pady=3,
                  command=lambda: (self._quick_buy(), dlg.destroy())).pack(side='left', padx=2)
        tk.Button(bf, text="QUICK SELL", font=("Helvetica", 12, "bold"),
                  bg="#b71c1c", fg="white", relief='flat', padx=12, pady=3,
                  command=lambda: (self._quick_sell(), dlg.destroy())).pack(side='left', padx=2)
        tk.Button(bf, text="CLOSE ALL", font=("Helvetica", 12, "bold"),
                  bg="#e6a800", fg="black", relief='flat', padx=12, pady=3,
                  command=lambda: (self._close_all_position(), dlg.destroy())).pack(side='left', padx=2)

        # Planned Trade fields
        tk.Frame(dlg, bg="#444", height=1).pack(fill='x', padx=8, pady=4)
        tk.Label(dlg, text="Planned Trade", font=("Helvetica", 11, "bold"),
                 bg=self.BG, fg="#888").pack(anchor='w', padx=8)

        pf = tk.Frame(dlg, bg=self.BG)
        pf.pack(fill='x', padx=8, pady=2)
        self._planned_entry_var.set(f"{price:.2f}" if price else "")
        self._planned_stop_var.set("")
        self._planned_tp_var.set("")
        for label, var in [("Entry", self._planned_entry_var),
                           ("Stop", self._planned_stop_var),
                           ("TP", self._planned_tp_var)]:
            tk.Label(pf, text=f"{label}:", font=("Helvetica", 11), bg=self.BG, fg="#ccc").pack(side='left', padx=(0, 2))
            tk.Entry(pf, textvariable=var, width=8, font=("Courier", 12),
                     bg="#333", fg="white", insertbackground="white", relief='flat').pack(side='left', padx=(0, 6))

        pbf = tk.Frame(dlg, bg=self.BG)
        pbf.pack(fill='x', padx=8, pady=4)
        tk.Button(pbf, text="BUY PLANNED", font=("Helvetica", 11, "bold"),
                  bg="#0d47a1", fg="white", relief='flat', padx=8, pady=2,
                  command=lambda: (self._planned_buy(), dlg.destroy())).pack(side='left', padx=2)
        tk.Button(pbf, text="SELL PLANNED", font=("Helvetica", 11, "bold"),
                  bg="#880e4f", fg="white", relief='flat', padx=8, pady=2,
                  command=lambda: (self._planned_sell(), dlg.destroy())).pack(side='left', padx=2)
        tk.Label(pbf, text="OCA bracket", font=("Helvetica", 9),
                 bg=self.BG, fg="#666").pack(side='left', padx=6)

    def _open_tradingview(self, sym: str):
        """Open TradingView chart in browser for the given symbol."""
        url = f"https://www.tradingview.com/chart/?symbol={sym}"
        webbrowser.open(url)

    def _open_stock_detail(self, sym: str):
        """Open full-screen stock detail window (same as Telegram unified report).

        Shows: intraday candlestick chart with fib levels + news + fundamentals
        in a compact layout.  Chart generated via generate_alert_chart().
        """
        stock_data = self._stock_data.get(sym, {})
        price = stock_data.get('price', 0)
        if price <= 0:
            return

        win = tk.Toplevel(self.root)
        win.title(f"Detail — {sym}")
        win.configure(bg='#0e1117')
        win.attributes('-zoomed', True)
        win.update_idletasks()

        # Status bar
        status_var = tk.StringVar(value=f"Loading chart for {sym}...")
        tk.Label(win, textvariable=status_var, font=("Courier", 10),
                 bg='#0e1117', fg='#888').pack(anchor='w', padx=8, pady=2)

        # Main body: chart left (70%), info right (30%)
        body = tk.Frame(win, bg='#0e1117')
        body.pack(fill='both', expand=True, padx=4, pady=2)

        chart_frame = tk.Frame(body, bg='#0e1117')
        chart_frame.pack(side='left', fill='both', expand=True)

        info_frame = tk.Frame(body, bg='#0e1117', width=360)
        info_frame.pack(side='right', fill='y')
        info_frame.pack_propagate(False)

        # Keep reference to avoid GC
        win._chart_images = []
        win._refresh_after_id = None

        # Build info panel immediately from cached data
        self._build_detail_info(info_frame, sym, stock_data)

        # Generate chart in background thread
        def _generate():
            try:
                chart_path = self._generate_detail_chart(sym, stock_data)
                if chart_path:
                    win.after(0, lambda: self._show_detail_chart(
                        win, chart_frame, chart_path, status_var, sym))
                else:
                    win.after(0, lambda: status_var.set(f"No chart data for {sym}"))
            except Exception as e:
                log.error(f"Stock detail chart {sym}: {e}")
                win.after(0, lambda: status_var.set(f"Error: {e}"))

        threading.Thread(target=_generate, daemon=True).start()

        # Auto-refresh every 90s
        def _auto_refresh():
            try:
                if not win.winfo_exists():
                    return
            except tk.TclError:
                return

            def _regen():
                try:
                    sd = self._stock_data.get(sym, stock_data)
                    chart_path = self._generate_detail_chart(sym, sd)
                    if chart_path:
                        win.after(0, lambda: self._show_detail_chart(
                            win, chart_frame, chart_path, status_var, sym))
                except Exception as e:
                    log.debug(f"Detail refresh {sym}: {e}")

            threading.Thread(target=_regen, daemon=True).start()
            win._refresh_after_id = win.after(90_000, _auto_refresh)

        win._refresh_after_id = win.after(90_000, _auto_refresh)

        def _on_close():
            if win._refresh_after_id:
                win.after_cancel(win._refresh_after_id)
            win.destroy()
        win.protocol("WM_DELETE_WINDOW", _on_close)

    def _generate_detail_chart(self, sym: str, stock_data: dict) -> Path | None:
        """Generate the unified report chart for the stock detail popup."""
        price = stock_data.get('price', 0)
        if price <= 0:
            return None

        df = _download_intraday(sym, bar_size='1 min', duration='2 D')
        if df is None or len(df) < 20:
            return None

        # Fib levels
        with _fib_cache_lock:
            cached = _fib_cache.get(sym)
        if cached:
            anchor_low, anchor_high, all_levels, ratio_map, anchor_date = cached
        else:
            calc_fib_levels(sym, price)
            with _fib_cache_lock:
                cached = _fib_cache.get(sym)
            if cached:
                anchor_low, anchor_high, all_levels, ratio_map, anchor_date = cached
            else:
                anchor_low, anchor_high, all_levels, ratio_map, anchor_date = 0, 0, [], {}, ''

        # Build stock_info
        enrich = _enrichment.get(sym, {})
        vol_raw = stock_data.get('volume_raw', 0)
        dol_vol = vol_raw * price if vol_raw and price else 0
        float_str = enrich.get('float', '-')
        float_shares = _parse_float_to_shares(float_str)
        rvol = stock_data.get('rvol', 0)
        ft_pct = (vol_raw / float_shares * 100) if float_shares > 0 and vol_raw > 0 else 0
        si = {
            'float_str': float_str,
            'float_dollar': _format_dollar_short(float_shares * price).replace('מליון', 'M').replace('אלף', 'K') if float_shares > 0 and price > 0 else '',
            'vol_shares': _format_shares_short(vol_raw).replace('מליון', 'M').replace('אלף', 'K') if vol_raw > 0 else '',
            'vol_dollar': _format_dollar_short(dol_vol).replace('מליון', 'M').replace('אלף', 'K') if dol_vol > 0 else '',
            'vol_str': (_format_dollar_short(dol_vol) if dol_vol > 0 else (_format_shares_short(vol_raw) if vol_raw > 0 else '')).replace('מליון', 'M').replace('אלף', 'K'),
            'short': enrich.get('short', '-'),
            'cash': enrich.get('cash', '-'),
            'inst_own': enrich.get('inst_own', '-'),
            'insider_own': enrich.get('insider_own', '-'),
            'vwap': stock_data.get('vwap', 0),
            'pct': stock_data.get('pct', 0),
            'rvol': rvol,
            'float_turnover': ft_pct,
        }

        # Compute MAs
        ma_data = None
        try:
            if df is not None and len(df) > 20 and 'date' in df.columns:
                df_ts = df.copy()
                df_ts.index = pd.to_datetime(df_ts['date'])
                ma_frames = {'1m': df}
                for tf_key, rule in [('5m', '5min'), ('15m', '15min'),
                                     ('30m', '30min'), ('1h', '1h')]:
                    resampled = df_ts.resample(rule).agg({
                        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
                    }).dropna()
                    if len(resampled) > 0:
                        ma_frames[tf_key] = resampled
                ma_frames['D'] = _daily_cache.get(sym)
                ma_data = _calc_ma_table(price, ma_frames)
        except Exception as e:
            log.debug(f"MA calc detail {sym}: {e}")

        fib_anchor = None
        if anchor_low and anchor_high:
            fib_anchor = (anchor_low, anchor_high, anchor_date)

        pct = stock_data.get('pct', 0)
        title = f"{sym} ${price:.2f} ({pct:+.1f}%)"
        return generate_alert_chart(sym, df, all_levels, price, ratio_map,
                                    alert_title=title, stock_info=si,
                                    ma_rows=ma_data, fib_anchor=fib_anchor)

    def _show_detail_chart(self, win, chart_frame: tk.Frame,
                           chart_path: Path, status_var: tk.StringVar, sym: str):
        """Display the chart image in the detail window."""
        for w in chart_frame.winfo_children():
            w.destroy()
        try:
            win.update_idletasks()
            cw = chart_frame.winfo_width() or 1000
            ch = chart_frame.winfo_height() or 700
            img = Image.open(chart_path)
            img = img.resize((cw, ch), Image.LANCZOS)
            photo = ImageTk.PhotoImage(img)
            win._chart_images = [photo]
            lbl = tk.Label(chart_frame, image=photo, bg='#0e1117')
            lbl.pack(fill='both', expand=True)
            status_var.set(f"{sym} — chart loaded")
        except Exception as e:
            tk.Label(chart_frame, text=f"Error: {e}", font=("Courier", 14),
                     bg='#0e1117', fg='#ff4444').pack(fill='both', expand=True)

    def _build_detail_info(self, parent: tk.Frame, sym: str, stock_data: dict):
        """Build the right-side info panel for stock detail popup."""
        enrich = _enrichment.get(sym, {})
        price = stock_data.get('price', 0)
        pct = stock_data.get('pct', 0)
        vwap = stock_data.get('vwap', 0)
        rvol = stock_data.get('rvol', 0)
        vol_raw = stock_data.get('volume_raw', 0)
        dol_vol = vol_raw * price if vol_raw and price else 0
        float_str = enrich.get('float', '-')
        float_shares = _parse_float_to_shares(float_str)
        ft_pct = (vol_raw / float_shares * 100) if float_shares > 0 and vol_raw > 0 else 0

        bg = '#0e1117'
        hdr_font = ("Helvetica", 16, "bold")
        lbl_font = ("Helvetica", 12)
        val_font = ("Helvetica", 12, "bold")

        # Scrollable container
        canvas = tk.Canvas(parent, bg=bg, highlightthickness=0)
        inner = tk.Frame(canvas, bg=bg)
        canvas.create_window((0, 0), window=inner, anchor='nw')
        inner.bind('<Configure>', lambda e: canvas.configure(scrollregion=canvas.bbox('all')))
        canvas.pack(fill='both', expand=True)

        def _mw(event):
            canvas.yview_scroll(-1 * (event.delta // 120 or (1 if event.num == 4 else -1)), "units")
        canvas.bind('<Button-4>', _mw)
        canvas.bind('<Button-5>', _mw)

        pad = {'padx': 8, 'pady': 1, 'anchor': 'w'}

        # Header
        pct_fg = self.GREEN if pct >= 0 else self.RED
        tk.Label(inner, text=sym, font=("Helvetica", 22, "bold"),
                 bg=bg, fg=self.ACCENT).pack(**pad, pady=(8, 0))
        tk.Label(inner, text=f"${price:.2f}  ({pct:+.1f}%)", font=hdr_font,
                 bg=bg, fg=pct_fg).pack(**pad)

        tk.Frame(inner, bg='#333', height=1).pack(fill='x', padx=8, pady=4)

        # Company info
        company = enrich.get('company', '')
        sector = enrich.get('sector', '')
        country = enrich.get('country', '')
        if company:
            tk.Label(inner, text=company, font=lbl_font, bg=bg, fg='#ccc',
                     wraplength=340).pack(**pad)
        if sector or country:
            tk.Label(inner, text=f"{sector}  {country}".strip(), font=("Helvetica", 10),
                     bg=bg, fg='#888').pack(**pad)

        tk.Frame(inner, bg='#333', height=1).pack(fill='x', padx=8, pady=4)

        # Key metrics
        def _metric(label, value, fg='#ccc'):
            row = tk.Frame(inner, bg=bg)
            row.pack(fill='x', padx=8, pady=1)
            tk.Label(row, text=label, font=lbl_font, bg=bg, fg='#888',
                     width=12, anchor='w').pack(side='left')
            tk.Label(row, text=str(value), font=val_font, bg=bg, fg=fg).pack(side='left')

        # VWAP
        if vwap > 0:
            vwap_pct = (price - vwap) / vwap * 100
            vwap_fg = self.GREEN if price > vwap else self.RED
            vp = f"${vwap:.2f}" if vwap >= 1 else f"${vwap:.4f}"
            _metric("VWAP", f"{vp} ({vwap_pct:+.1f}%)", vwap_fg)

        _metric("Float", float_str, '#cca0ff')
        if float_shares > 0 and price > 0:
            _metric("Float $", _format_dollar_short(float_shares * price), '#cca0ff')
        _metric("Short", enrich.get('short', '-'), '#ffaa00')
        _metric("RVOL", f"{rvol:.1f}x" if rvol > 0 else '-',
                self.GREEN if rvol >= 2.0 else '#ccc')
        _metric("Volume", _format_shares_short(vol_raw) if vol_raw > 0 else '-')
        _metric("Vol $", _format_dollar_short(dol_vol) if dol_vol > 0 else '-')
        if ft_pct >= 5:
            _metric("Turnover", f"{ft_pct:.0f}%", self.GREEN if ft_pct >= 50 else '#ccc')

        tk.Frame(inner, bg='#333', height=1).pack(fill='x', padx=8, pady=4)

        # Financials
        mcap = enrich.get('market_cap', '-')
        eps = enrich.get('eps', '-')
        cash = enrich.get('cash', '-')
        inst_own = enrich.get('inst_own', '-')
        if mcap and mcap != '-':
            _metric("MCap", mcap)
        if eps and eps != '-':
            _metric("EPS", eps)
        if cash and cash != '-':
            _metric("Cash", f"${cash}")
        if inst_own and inst_own != '-':
            _metric("Inst Own", inst_own)

        tk.Frame(inner, bg='#333', height=1).pack(fill='x', padx=8, pady=4)

        # News headlines
        news_items = enrich.get('news', [])
        if news_items:
            tk.Label(inner, text="NEWS", font=("Helvetica", 12, "bold"),
                     bg=bg, fg='#ffcc00').pack(**pad, pady=(4, 2))
            for item in news_items[:5]:
                title = item.get('title_he', item.get('title_en', ''))
                date_str = item.get('date', '')
                try:
                    title_d = bidi_display(title)
                except Exception:
                    title_d = title
                nrow = tk.Frame(inner, bg=bg)
                nrow.pack(fill='x', padx=8, pady=1)
                if date_str:
                    il_date = _et_to_israel(date_str)
                    tk.Label(nrow, text=il_date or date_str[:10], font=("Helvetica", 9),
                             bg=bg, fg='#888').pack(side='left')
                tk.Label(nrow, text=title_d, font=("Helvetica", 10),
                         bg=bg, fg='#ddd', wraplength=300, justify='left').pack(
                    side='left', padx=(4, 0))
        else:
            tk.Label(inner, text="No news", font=lbl_font,
                     bg=bg, fg='#555').pack(**pad, pady=4)

        tk.Frame(inner, bg='#333', height=1).pack(fill='x', padx=8, pady=4)

        # Fib levels
        with _fib_cache_lock:
            cached_fib = _fib_cache.get(sym)
        if cached_fib:
            a_low, a_high, all_levels, ratio_map, a_date = cached_fib
            tk.Label(inner, text="FIB LEVELS", font=("Helvetica", 12, "bold"),
                     bg=bg, fg='#66cccc').pack(**pad, pady=(4, 2))
            if a_low and a_high:
                tk.Label(inner, text=f"Anchor: ${a_low:.4f} → ${a_high:.4f} ({a_date})",
                         font=("Helvetica", 10), bg=bg, fg='#66cccc').pack(**pad)
            fibs_below = sorted([lv for lv in all_levels if lv <= price], reverse=True)[:3]
            fibs_above = sorted([lv for lv in all_levels if lv > price])[:3]
            for lv in fibs_above[::-1]:
                r_info = ratio_map.get(round(lv, 4), ('?', '?'))
                r_str = f"{r_info[0]}" if isinstance(r_info, tuple) else str(r_info)
                fg = self.RED if lv > price else self.GREEN
                _metric(f"R {r_str}", f"${lv:.4f}" if lv < 1 else f"${lv:.2f}", fg)
            _metric("PRICE", f"${price:.2f}", self.ACCENT)
            for lv in fibs_below:
                r_info = ratio_map.get(round(lv, 4), ('?', '?'))
                r_str = f"{r_info[0]}" if isinstance(r_info, tuple) else str(r_info)
                _metric(f"S {r_str}", f"${lv:.4f}" if lv < 1 else f"${lv:.2f}", self.GREEN)

        # Action buttons at bottom
        tk.Frame(inner, bg='#333', height=1).pack(fill='x', padx=8, pady=6)
        btn_frame = tk.Frame(inner, bg=bg)
        btn_frame.pack(fill='x', padx=8, pady=4)
        tk.Button(btn_frame, text="TradingView", font=("Helvetica", 11, "bold"),
                  bg='#1a5276', fg='white', relief='flat', padx=10, pady=4,
                  command=lambda: self._open_tradingview(sym)).pack(side='left', padx=2)
        tk.Button(btn_frame, text="QUICK BUY", font=("Helvetica", 11, "bold"),
                  bg='#1b5e20', fg='white', relief='flat', padx=10, pady=4,
                  command=lambda: (self._select_stock(sym), self._quick_buy())).pack(side='left', padx=2)
        tk.Button(btn_frame, text="QUICK SELL", font=("Helvetica", 11, "bold"),
                  bg='#b71c1c', fg='white', relief='flat', padx=10, pady=4,
                  command=lambda: (self._select_stock(sym), self._quick_sell())).pack(side='left', padx=2)

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

    # ── News Window ──────────────────────────────────────────

    def _open_news_window(self, sym: str):
        """Open a Toplevel window showing expandable news headlines for sym."""
        enrich = _enrichment.get(sym, {})
        news_items = enrich.get('news', [])
        stock_data = self._stock_data.get(sym, {})
        price = stock_data.get('price', 0)
        pct = stock_data.get('pct', 0)

        win = tk.Toplevel(self.root)
        win.title(f"News — {sym}")
        win.configure(bg='#0e1117')
        win.geometry("700x500")
        win.minsize(500, 300)

        # Header
        hdr = tk.Frame(win, bg='#0e1117')
        hdr.pack(fill='x', padx=10, pady=(8, 4))
        pct_fg = self.GREEN if pct >= 0 else self.RED
        tk.Label(hdr, text=sym, font=("Helvetica", 18, "bold"),
                 bg='#0e1117', fg=self.ACCENT).pack(side='left')
        tk.Label(hdr, text=f"  ${price:.2f}  {pct:+.1f}%",
                 font=("Helvetica", 14), bg='#0e1117', fg=pct_fg).pack(side='left', padx=(8, 0))
        tk.Label(hdr, text=f"  ({len(news_items)} headlines)",
                 font=("Helvetica", 11), bg='#0e1117', fg='#888').pack(side='left', padx=(8, 0))

        tk.Frame(win, bg='#333', height=1).pack(fill='x', padx=10)

        # Scrollable body
        body_frame = tk.Frame(win, bg='#0e1117')
        body_frame.pack(fill='both', expand=True, padx=10, pady=4)

        canvas = tk.Canvas(body_frame, bg='#0e1117', highlightthickness=0)
        scrollbar = tk.Scrollbar(body_frame, orient='vertical', command=canvas.yview)
        inner = tk.Frame(canvas, bg='#0e1117')
        inner.bind('<Configure>', lambda e: canvas.configure(scrollregion=canvas.bbox('all')))
        canvas.create_window((0, 0), window=inner, anchor='nw')
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        # Mousewheel
        def _mw(event):
            canvas.yview_scroll(-1 * (event.delta // 120 or (1 if event.num == 4 else -1)), "units")
        canvas.bind('<Button-4>', _mw)
        canvas.bind('<Button-5>', _mw)
        inner.bind('<Button-4>', _mw)
        inner.bind('<Button-5>', _mw)

        if not news_items:
            tk.Label(inner, text="No news available", font=("Helvetica", 14),
                     bg='#0e1117', fg='#666').pack(pady=20)
            return

        # Build collapsible rows
        for idx, item in enumerate(news_items):
            self._build_news_row(inner, item, idx, canvas)

    def _build_news_row(self, parent: tk.Frame, item: dict, idx: int, canvas: tk.Canvas):
        """Build a single collapsible news headline row."""
        title_he = item.get('title_he', item.get('title_en', ''))
        date_str = item.get('date', '')
        source = item.get('source', '')
        article_id = item.get('article_id', '')
        provider_code = item.get('provider_code', '')

        try:
            title_display = bidi_display(title_he)
        except Exception:
            title_display = title_he

        src_tag = f" [{source}]" if source else ""
        has_article = bool(article_id)

        row_bg = '#151520' if idx % 2 == 0 else '#0e1117'
        row = tk.Frame(parent, bg=row_bg)
        row.pack(fill='x', pady=1)

        # State for expand/collapse
        expanded = [False]
        body_frame = [None]  # lazy-created

        # Toggle button
        toggle_text = tk.StringVar(value="▶" if has_article else "•")
        toggle_btn = tk.Label(row, textvariable=toggle_text, font=("Helvetica", 12),
                              bg=row_bg, fg='#00aaff' if has_article else '#555',
                              width=2, cursor='hand2' if has_article else 'arrow')
        toggle_btn.pack(side='left', padx=(4, 2))

        # Headline text
        hl_label = tk.Label(row, text=title_display, font=("Helvetica", 11),
                            bg=row_bg, fg='#e0e0e0', anchor='w', justify='left',
                            wraplength=500)
        hl_label.pack(side='left', fill='x', expand=True)

        # Date + source
        meta_text = f"{date_str}{src_tag}" if date_str else src_tag
        if meta_text:
            tk.Label(row, text=meta_text, font=("Helvetica", 9),
                     bg=row_bg, fg='#888', anchor='e').pack(side='right', padx=(4, 8))

        def _toggle(event=None):
            if not has_article:
                return
            if expanded[0]:
                # Collapse
                if body_frame[0]:
                    body_frame[0].pack_forget()
                toggle_text.set("▶")
                expanded[0] = False
            else:
                # Expand
                toggle_text.set("▼")
                expanded[0] = True
                if body_frame[0]:
                    body_frame[0].pack(fill='x', padx=(24, 8), pady=(0, 4))
                else:
                    # Create body frame with loading indicator
                    bf = tk.Frame(parent, bg=row_bg)
                    bf.pack(fill='x', padx=(24, 8), pady=(0, 4))
                    body_frame[0] = bf
                    loading_lbl = tk.Label(bf, text="Loading article...",
                                           font=("Helvetica", 10, "italic"),
                                           bg=row_bg, fg='#888')
                    loading_lbl.pack(anchor='w')

                    # Fetch in background thread
                    def _fetch():
                        text = _fetch_news_article(provider_code, article_id)
                        def _show():
                            loading_lbl.destroy()
                            if text:
                                try:
                                    display_text = bidi_display(text[:2000])
                                except Exception:
                                    display_text = text[:2000]
                                body_lbl = tk.Label(
                                    bf, text=display_text,
                                    font=("Helvetica", 10), bg=row_bg, fg='#cccccc',
                                    anchor='w', justify='left', wraplength=600,
                                )
                                body_lbl.pack(anchor='w', fill='x')
                            else:
                                tk.Label(bf, text="(Article content not available)",
                                         font=("Helvetica", 10, "italic"),
                                         bg=row_bg, fg='#666').pack(anchor='w')
                        try:
                            parent.after(0, _show)
                        except Exception:
                            pass
                    threading.Thread(target=_fetch, daemon=True).start()

        if has_article:
            toggle_btn.bind('<Button-1>', _toggle)
            hl_label.bind('<Button-1>', _toggle)
            row.bind('<Button-1>', _toggle)

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
        # Re-render tables to update highlight
        self._render_stock_table()
        self._render_stock_table_r()
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
        n_pos = len(self._cached_positions)
        if nl > 0 or bp > 0:
            self._account_var.set(f"Account: ${nl:,.0f} | BP: ${bp:,.0f}")
            pos_txt = f" | {n_pos} pos" if n_pos > 0 else ""
            self._hdr_account_var.set(f"Account ${nl:,.0f} | BP ${bp:,.0f}{pos_txt}")
        else:
            self._account_var.set("Account: ---")
            self._hdr_account_var.set("")

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
            self._render_account_summary()
            self._render_spy_chart()
            self._render_recent_trades()
        self.root.after(0, _refresh)

    def _on_order_result(self, msg: str, success: bool):
        """Callback from ScannerThread with order result."""
        def _update():
            self._order_status_var.set(msg)
            self._order_status_label.config(fg=self.GREEN if success else self.RED)
        self.root.after(0, _update)

    def _validate_order_prereqs(self):
        """Validate common order prerequisites. Returns (sym, price) or None."""
        sym = self._selected_symbol_name
        if not sym or sym == "---":
            messagebox.showwarning("No Stock", "Select a stock first.", parent=self.root)
            return None
        try:
            price = float(self._trade_price.get())
            if price <= 0:
                raise ValueError
        except (ValueError, TypeError):
            messagebox.showwarning("Invalid Price", "Enter a valid price.", parent=self.root)
            return None
        if not self._order_thread.connected:
            messagebox.showwarning(
                "Not Connected",
                "OrderThread not connected to IBKR.\n\n"
                "Make sure TWS is running with API on port 7497.\n"
                f"Status: {self.conn_var.get()}",
                parent=self.root,
            )
            return None
        return sym, price

    def _quick_buy(self):
        """Quick Buy: configurable BP%, Stop%, TP%. No confirmation dialog."""
        result = self._validate_order_prereqs()
        if not result:
            return
        sym, price = result
        log.info(f"_quick_buy called: sym={sym} price={price:.4f}")

        # Read configurable fields (defaults: 80% BP, 5% stop, 20% TP)
        try:
            bp_pct = float(self._quick_bp_var.get()) / 100.0
            if bp_pct <= 0 or bp_pct > 1:
                bp_pct = 0.80
        except (ValueError, TypeError):
            bp_pct = 0.80
        try:
            stop_pct = float(self._quick_stop_var.get()) / 100.0
            if stop_pct <= 0 or stop_pct >= 1:
                stop_pct = 0.05
        except (ValueError, TypeError):
            stop_pct = 0.05
        try:
            tp_pct = float(self._quick_tp_var.get()) / 100.0
            if tp_pct <= 0:
                tp_pct = 0.0  # 0 = no TP, trailing only
        except (ValueError, TypeError):
            tp_pct = 0.20

        buy_price = round(price * 1.01, 2)
        bp = self._cached_buying_power
        nl = self._cached_net_liq
        if nl <= 0:
            messagebox.showwarning("No Data", "Waiting for account data from IBKR...", parent=self.root)
            return
        avail = bp if bp > 0 else nl
        qty = int(avail * bp_pct / buy_price)
        if qty <= 0:
            messagebox.showwarning("Qty Too Low", "Not enough buying power.", parent=self.root)
            return

        stop_price = round(price * (1 - stop_pct), 2)
        limit_price = round(stop_price * (1 - STOP_LIMIT_OFFSET_PCT), 2)

        req = {
            'sym': sym, 'action': 'BUY', 'qty': qty, 'price': buy_price,
            'stop_price': stop_price,
            'limit_price': limit_price,
            'stop_desc': f"${stop_price:.2f} ({stop_pct*100:.0f}% stop)",
        }
        if tp_pct > 0:
            req['target_price'] = round(price * (1 + tp_pct), 2)
            req['trailing_pct'] = 0.0
        else:
            req['trailing_pct'] = stop_pct * 100  # use stop% as trailing %
        self._order_thread.submit(req)
        log.info(f"QUICK BUY submitted: {req}")
        tp_info = f"TP ${req.get('target_price', 0):.2f}" if tp_pct > 0 else f"Trail {stop_pct*100:.0f}%"
        self._order_status_var.set(
            f"QUICK BUY {qty} {sym} @ ${buy_price:.2f} | Stop {stop_pct*100:.0f}% | {tp_info}")
        self._order_status_label.config(fg="#00e676")

    def _quick_sell(self):
        """Quick Sell: 100% of position at price*0.99, cancel existing stops. No confirmation."""
        result = self._validate_order_prereqs()
        if not result:
            return
        sym, price = result
        log.info(f"_quick_sell called: sym={sym} price={price:.4f}")

        pos = self._cached_positions.get(sym)
        if not pos or pos[0] == 0:
            messagebox.showwarning("No Position", f"No position in {sym}.", parent=self.root)
            return
        qty = abs(pos[0])
        sell_price = round(price * 0.99, 2)
        if sell_price <= 0:
            sell_price = 0.01

        req = {
            'sym': sym, 'action': 'SELL', 'qty': qty, 'price': sell_price,
            'cancel_existing': True,
        }
        self._order_thread.submit(req)
        log.info(f"QUICK SELL submitted: {req}")
        self._order_status_var.set(
            f"QUICK SELL {qty} {sym} @ ${sell_price:.2f} (-1%) + cancel stops")
        self._order_status_label.config(fg="#ff5252")

    def _planned_buy(self):
        """Planned Buy: user-specified entry/stop/TP, OCA bracket. With confirmation dialog."""
        result = self._validate_order_prereqs()
        if not result:
            return
        sym, price = result
        log.info(f"_planned_buy called: sym={sym}")

        # Read entry/stop/tp fields
        try:
            entry_str = self._planned_entry_var.get().strip()
            entry_price = float(entry_str) if entry_str else price
            if entry_price <= 0:
                raise ValueError
        except (ValueError, TypeError):
            messagebox.showwarning("Invalid Entry", "Enter a valid entry price.", parent=self.root)
            return

        try:
            stop_str = self._planned_stop_var.get().strip()
            stop_price = float(stop_str) if stop_str else 0.0
        except (ValueError, TypeError):
            stop_price = 0.0

        try:
            tp_str = self._planned_tp_var.get().strip()
            target_price = float(tp_str) if tp_str else 0.0
        except (ValueError, TypeError):
            target_price = 0.0

        # If no stop given, use fib-based or 5% fallback
        if stop_price <= 0:
            with _fib_cache_lock:
                cached = _fib_cache.get(sym)
            if cached:
                _al, _ah, all_levels, _rm, *_ = cached
                supports = [lv for lv in all_levels if lv <= entry_price]
                if supports:
                    nearest_support = supports[-1]
                    stop_price = round(nearest_support * (1 - BRACKET_FIB_STOP_PCT), 2)
                else:
                    stop_price = round(entry_price * 0.95, 2)
            else:
                stop_price = round(entry_price * 0.95, 2)
        stop_price = round(stop_price, 2)
        limit_price = round(stop_price * (1 - STOP_LIMIT_OFFSET_PCT), 2)

        # Validate: stop < entry
        if stop_price >= entry_price:
            messagebox.showwarning("Invalid Stop", f"Stop ${stop_price:.2f} must be below entry ${entry_price:.2f}.", parent=self.root)
            return
        if target_price > 0 and target_price <= entry_price:
            messagebox.showwarning("Invalid TP", f"TP ${target_price:.2f} must be above entry ${entry_price:.2f}.", parent=self.root)
            return

        # Qty based on buying power
        bp = self._cached_buying_power
        nl = self._cached_net_liq
        if nl <= 0:
            messagebox.showwarning("No Data", "Waiting for account data from IBKR...", parent=self.root)
            return
        avail = bp if bp > 0 else nl
        buy_limit = round(entry_price, 2)
        qty = int(avail * 0.80 / buy_limit)
        if qty <= 0:
            messagebox.showwarning("Qty Too Low", "Not enough buying power.", parent=self.root)
            return

        # Confirmation dialog
        tp_line = f"TP: ${target_price:.2f} [LMT SELL GTC]" if target_price > 0 else "TP: (none, trailing 3%)"
        trailing_pct = 0.0 if target_price > 0 else BRACKET_TRAILING_PROFIT_PCT

        # VWAP distance warning
        vwap_warning = ""
        stock_data = self._stock_data.get(sym, {})
        vwap_val = stock_data.get('vwap', 0)
        if vwap_val > 0 and entry_price < vwap_val:
            vwap_dist_pct = (vwap_val - entry_price) / entry_price * 100
            vwap_warning = f"\nBelow VWAP! ${entry_price:.2f} < VWAP ${vwap_val:.2f} ({vwap_dist_pct:.1f}%)\n"

        confirm = messagebox.askokcancel(
            "Confirm Planned BUY",
            f"BUY {qty} {sym} @ ${buy_limit:.2f}\n"
            f"Total: ${qty * buy_limit:,.2f}\n\n"
            f"Stop: ${stop_price:.2f} (limit ${limit_price:.2f}) [STP LMT GTC]\n"
            f"{tp_line}\n"
            f"OCA: Stop + TP cancel each other"
            f"{vwap_warning}\n\n"
            f"outsideRth=True | Continue?",
            parent=self.root,
        )
        if not confirm:
            return

        req = {
            'sym': sym, 'action': 'BUY', 'qty': qty, 'price': buy_limit,
            'stop_price': stop_price,
            'limit_price': limit_price,
            'stop_desc': f"${stop_price:.2f} (planned)",
        }
        if target_price > 0:
            req['target_price'] = target_price
            req['trailing_pct'] = 0.0
        else:
            req['trailing_pct'] = trailing_pct
        self._order_thread.submit(req)
        log.info(f"PLANNED BUY submitted: {req}")
        tp_info = f"TP ${target_price:.2f}" if target_price > 0 else f"Trail {trailing_pct}%"
        self._order_status_var.set(
            f"PLANNED BUY {qty} {sym} @ ${buy_limit:.2f} | Stop ${stop_price:.2f} | {tp_info}")
        self._order_status_label.config(fg="#448aff")

    def _planned_sell(self):
        """Planned Sell: sell position with stop on remaining shares. With confirmation dialog."""
        result = self._validate_order_prereqs()
        if not result:
            return
        sym, price = result
        log.info(f"_planned_sell called: sym={sym}")

        pos = self._cached_positions.get(sym)
        if not pos or pos[0] == 0:
            messagebox.showwarning("No Position", f"No position in {sym}.", parent=self.root)
            return
        total_qty = abs(pos[0])

        # Read stop/tp fields
        try:
            stop_str = self._planned_stop_var.get().strip()
            stop_price = float(stop_str) if stop_str else 0.0
        except (ValueError, TypeError):
            stop_price = 0.0

        try:
            tp_str = self._planned_tp_var.get().strip()
            sell_price_val = float(tp_str) if tp_str else price
        except (ValueError, TypeError):
            sell_price_val = price

        sell_price = round(sell_price_val, 2)
        if sell_price <= 0:
            sell_price = round(price, 2)

        # Confirmation
        stop_info = ""
        if stop_price > 0:
            limit_price = round(stop_price * (1 - STOP_LIMIT_OFFSET_PCT), 2)
            stop_info = f"\nStop on remaining: ${stop_price:.2f} (limit ${limit_price:.2f}) [STP LMT GTC]"

        confirm = messagebox.askokcancel(
            "Confirm Planned SELL",
            f"SELL {total_qty} {sym} @ ${sell_price:.2f}\n"
            f"Total: ${total_qty * sell_price:,.2f}"
            f"{stop_info}\n\n"
            f"outsideRth=True | Continue?",
            parent=self.root,
        )
        if not confirm:
            return

        req = {
            'sym': sym, 'action': 'SELL', 'qty': total_qty, 'price': sell_price,
            'cancel_existing': True,
        }
        if stop_price > 0:
            limit_price = round(stop_price * (1 - STOP_LIMIT_OFFSET_PCT), 2)
            req['stop_price'] = stop_price
            req['limit_price'] = limit_price
            req['stop_desc'] = f"${stop_price:.2f} (planned)"
        self._order_thread.submit(req)
        log.info(f"PLANNED SELL submitted: {req}")
        self._order_status_var.set(
            f"PLANNED SELL {total_qty} {sym} @ ${sell_price:.2f}")
        self._order_status_label.config(fg="#ff80ab")

    def _close_all_position(self):
        """Close entire position for selected symbol — aggressive limit $0.50 below current price."""
        sym = self._selected_symbol_name
        log.info(f"_close_all_position called: sym={sym}")
        if not sym or sym == "---":
            messagebox.showwarning("No Stock", "בחר מניה לפני סגירה.", parent=self.root)
            return

        if not self._order_thread.connected:
            messagebox.showwarning(
                "Not Connected",
                "OrderThread לא מחובר ל-IBKR.\n"
                f"סטטוס: {self.conn_var.get()}",
                parent=self.root,
            )
            return

        pos = self._cached_positions.get(sym)
        if not pos or pos[0] == 0:
            messagebox.showwarning("No Position", f"אין פוזיציה ב-{sym}.", parent=self.root)
            return

        qty = abs(pos[0])
        avg_cost = pos[1]

        # Get current price from trade price field or scan data
        try:
            current_price = float(self._trade_price.get())
            if current_price <= 0:
                raise ValueError
        except (ValueError, TypeError):
            messagebox.showwarning("Invalid Price", "הכנס מחיר תקין.", parent=self.root)
            return

        # Aggressive limit: 5% or $0.50 below current (whichever is smaller)
        offset = min(0.50, current_price * 0.05)
        sell_price = round(current_price - offset, 2)
        if sell_price <= 0:
            sell_price = 0.01

        pnl_est = (current_price - avg_cost) * qty
        pnl_pct = ((current_price / avg_cost) - 1) * 100 if avg_cost > 0 else 0

        confirm = messagebox.askokcancel(
            "⚠️ CLOSE ALL",
            f"סגירת כל הפוזיציה ב-{sym}\n\n"
            f"SELL {qty} shares\n"
            f"מחיר נוכחי: ${current_price:.2f}\n"
            f"Limit: ${sell_price:.2f} (−${offset:.2f} למילוי מיידי)\n"
            f"עלות ממוצעת: ${avg_cost:.4f}\n"
            f"רווח/הפסד משוער: ${pnl_est:+,.2f} ({pnl_pct:+.1f}%)\n\n"
            f"outsideRth=True\n"
            f"Continue?",
            parent=self.root,
        )
        if not confirm:
            return

        self._order_thread.submit({
            'sym': sym, 'action': 'SELL', 'qty': qty, 'price': sell_price,
            'cancel_existing': True,  # cancel open orders first (stops etc.)
        })
        log.info(f"CLOSE ALL submitted: SELL {qty} {sym} @ ${sell_price:.2f} (limit)")
        self._order_status_var.set(
            f"Sending: CLOSE ALL {qty} {sym} @ ${sell_price:.2f} (−${offset:.2f})")
        self._order_status_label.config(fg="#e6a800")

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
        """Rebuild column header labels with current font settings (both scanners)."""
        ff = self._table_font_var.get()
        fs = self._table_size_var.get()
        cols = [("SYM", 8), ("PRICE", 8), ("CHG%", 8), ("VOL", 7),
                ("RVOL", 6), ("VWAP", 7), ("FLOAT", 7), ("SHORT", 6), ("N", 2)]
        for frame in (self._hdr_frame, self._hdr_frame_r):
            for w in frame.winfo_children():
                w.destroy()
            for text, w in cols:
                tk.Label(frame, text=text, font=(ff, fs, "bold"),
                         bg=self.BG, fg=self.ACCENT, width=w, anchor='w').pack(side='left')

    def _apply_table_font(self, _=None):
        """Apply font changes to both stock tables."""
        self._rebuild_column_headers()
        # Force full rebuild of stock rows in both scanners
        self._rendered_order.clear()
        self._rendered_order_r.clear()
        self._render_stock_table()
        self._render_stock_table_r()

    def _push_alert(self, msg: str):
        """Alert stub — GUI alerts panel removed; Telegram-only alerts."""
        pass

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
                on_alert=None,  # GUI alerts disabled — Telegram only
                on_price_update=self._update_prices_inplace,
                order_thread=self._order_thread,
            )
            # GUI alerts disabled — Telegram alerts only
            # global _gui_alert_cb
            # _gui_alert_cb = self._push_alert
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
            scanner_data = [
                {'wid': s['wid'], 'name': s['name'],
                 'wm_title': _get_window_full_name(s['wid'])}
                for s in _scanner_sources
            ]
        with open(STATE_PATH, 'w') as f:
            json.dump({
                'freq': self.freq.get(),
                'thresh': self.thresh.get(),
                'price_min': self.price_min.get(),
                'price_max': self.price_max.get(),
                'scanner_sources': scanner_data,
                'table_font': self._table_font_var.get(),
                'table_size': self._table_size_var.get(),
                'scan1_min_pct': self._scan1_min_pct.get(),
                'scan1_max_pct': self._scan1_max_pct.get(),
                'scan2_min_pct': self._scan2_min_pct.get(),
                'scan2_max_pct': self._scan2_max_pct.get(),
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
                name = src.get('name', '')
                wm_title = src.get('wm_title', '')
                if wid and _verify_wid(wid):
                    verified.append({'wid': wid, 'name': name})
                elif wm_title:
                    # WID died (app restarted) — try to find window by title
                    new_wid = _find_window_by_title(wm_title)
                    if new_wid and _verify_wid(new_wid):
                        new_name = _get_window_name(new_wid)
                        verified.append({'wid': new_wid, 'name': new_name})
                        log.info(f"Scanner source recovered by title: '{wm_title}' → WID={new_wid}")
            with _scanner_sources_lock:
                _scanner_sources = verified
            self._refresh_scanner_slots()
            # Restore font settings
            if 'table_font' in s:
                self._table_font_var.set(s['table_font'])
            if 'table_size' in s:
                self._table_size_var.set(int(s['table_size']))
            # Restore scanner ranges
            if 'scan1_min_pct' in s:
                self._scan1_min_pct.set(float(s['scan1_min_pct']))
            if 'scan1_max_pct' in s:
                self._scan1_max_pct.set(float(s['scan1_max_pct']))
            if 'scan2_min_pct' in s:
                self._scan2_min_pct.set(float(s['scan2_min_pct']))
            if 'scan2_max_pct' in s:
                self._scan2_max_pct.set(float(s['scan2_max_pct']))
            # Apply loaded fonts
            self._apply_table_font()
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
