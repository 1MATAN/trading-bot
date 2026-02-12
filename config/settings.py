"""All tunable parameters as named constants."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from config directory
_config_dir = Path(__file__).parent
_env_path = _config_dir / ".env"
if _env_path.exists():
    load_dotenv(_env_path)
else:
    load_dotenv()  # fall back to project root .env

# ── Paths ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

TRADES_DB_PATH = DATA_DIR / "trades.db"
TRADES_CSV_PATH = DATA_DIR / "trades.csv"
LIVE_STATE_PATH = DATA_DIR / "live_state.json"
CONTROL_PATH = DATA_DIR / "control.json"
BACKTEST_DATA_DIR = DATA_DIR / "backtest_cache"
BACKTEST_DATA_DIR.mkdir(exist_ok=True)

# ── IBKR Connection ───────────────────────────────────
IBKR_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
IBKR_PORT = int(os.getenv("IBKR_PORT", "7497"))  # 7497=paper, 7496=live
IBKR_CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", "1"))
IBKR_TIMEOUT = 30  # seconds
IBKR_RECONNECT_DELAY = 5  # seconds between reconnection attempts
IBKR_MAX_RECONNECT_ATTEMPTS = 10

# ── Rate Limiting ──────────────────────────────────────
IBKR_MSG_RATE_LIMIT = 45  # messages per second (conservative under 50)
IBKR_MAX_CONCURRENT_HIST = 3  # max concurrent historical data requests

# ── Telegram ───────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# ── Trading Parameters ─────────────────────────────────
PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
STARTING_CAPITAL = 3_000.00
MAX_POSITION_SIZE_PCT = 0.15  # 15% of portfolio per position
MAX_OPEN_POSITIONS = 5
MAX_DAILY_LOSS_PCT = 0.03  # 3% of portfolio = $90 hard stop
MAX_LOSS_PER_TRADE_PCT = 0.01  # 1% of portfolio risk per trade

# ── PDT (Pattern Day Trader) ──────────────────────────
PDT_MAX_DAY_TRADES = 3  # max day trades in rolling 5 business days
PDT_ROLLING_WINDOW_DAYS = 5

# ── Cooldown ───────────────────────────────────────────
COOLDOWN_AFTER_LOSS_SECONDS = 300  # 5 minutes after a losing trade
COOLDOWN_AFTER_MAX_DAILY_LOSS = 86400  # rest of day (24h fallback)

# ── Scanner Filters (Layer 1 — IBKR built-in) ─────────
SCAN_PRICE_MIN = 0.50
SCAN_PRICE_MAX = 5.00
SCAN_VOLUME_MIN = 500_000  # shares
SCAN_CHANGE_PCT_MIN = 5.0  # minimum % gain today
SCAN_MAX_RESULTS = 50

# ── Deep Analysis (Layer 2) ───────────────────────────
MA200_PERIOD = 200
BOLLINGER_PERIOD = 20
BOLLINGER_STD_DEV = 2.0
VOLUME_SPIKE_MULTIPLIER = 2.0  # 2x average volume
VWAP_LOOKBACK_MINUTES = 390  # full trading day

# ── Fibonacci Engine (Layer 3) ─────────────────────────
FIB_LOOKBACK_DAYS = 30  # days to find swing high/low
FIB_LEVELS = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
FIB_CACHE_TTL_HOURS = 24
FIB_PROXIMITY_PCT = 0.02  # 2% threshold to consider price "at" a level
FIB_MAX_RECURSION_DEPTH = 3  # recursive sub-grid depth

# ── Signal Scorer (all-or-nothing gate) ────────────────
# All 5 conditions must pass:
# 1. Price above MA200 (uptrend)
# 2. Volume spike (2x+ average)
# 3. Price above VWAP (institutional demand)
# 4. Price near Fibonacci support level
# 5. Bollinger Band not overbought (below upper band)

# ── Partial Exit Strategy ──────────────────────────────
# Staged exits at Fibonacci resistance levels
PARTIAL_EXIT_STAGES = [
    {"fib_level": 0.382, "exit_pct": 0.25},
    {"fib_level": 0.500, "exit_pct": 0.25},
    {"fib_level": 0.618, "exit_pct": 0.25},
    {"fib_level": 0.786, "exit_pct": 0.25},
]

# ── Trailing Stop ──────────────────────────────────────
TRAILING_STOP_INITIAL_PCT = 0.05  # 5% below entry
TRAILING_STOP_TIGHTEN_PCT = 0.03  # tighten to 3% after first partial exit
TRAILING_STOP_CHECK_INTERVAL = 2  # seconds between checks

# ── Market Hours (US/Eastern) ──────────────────────────
TIMEZONE = "US/Eastern"
PREMARKET_START = "04:00"
MARKET_OPEN = "09:30"
MARKET_CLOSE = "16:00"
AFTERHOURS_END = "20:00"

# ── Scan Intervals ─────────────────────────────────────
SCAN_INTERVAL_SECONDS = 30  # how often to scan for new candidates
WATCHLIST_REFRESH_SECONDS = 60  # pre/after-market watchlist refresh

# ── Dashboard ──────────────────────────────────────────
DASHBOARD_PORT = 8501
DASHBOARD_REFRESH_SECONDS = 5

# ── Logging ────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
