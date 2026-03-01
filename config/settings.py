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
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")          # private chat — system msgs
TELEGRAM_GROUP_ID = os.getenv("TELEGRAM_GROUP_ID", "")         # group chat — alerts only
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# ── Mode ──────────────────────────────────────────────
PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
SIMULATION_MODE = os.getenv("SIMULATION_MODE", "true").lower() == "true"

# ── Trading Parameters ─────────────────────────────────
STARTING_CAPITAL = 3_000.00
POSITION_SIZE_PCT = 0.90  # 90% of portfolio in single position
MAX_OPEN_POSITIONS = 1  # one position at a time (90% allocation)
MAX_DAILY_LOSS_PCT = 0.03  # 3% of portfolio = $90 hard stop
SAFETY_STOP_PCT = 0.10  # 10% GTC safety stop below entry

# ── PDT (Pattern Day Trader) ──────────────────────────
PDT_MAX_DAY_TRADES = 3  # max day trades in rolling 5 business days
PDT_ROLLING_WINDOW_DAYS = 5

# ── Cooldown ───────────────────────────────────────────
COOLDOWN_AFTER_LOSS_SECONDS = 300  # 5 minutes after a losing trade
COOLDOWN_AFTER_MAX_DAILY_LOSS = 86400  # rest of day (24h fallback)

# ── Scanner Filters ───────────────────────────────────
SCAN_PRICE_MIN = 0.10  # minimum $0.10
SCAN_PRICE_MAX = 18.00
SCAN_FLOAT_MAX = 60_000_000  # 60M shares
SCAN_MOVE_PCT_MIN = 20.0  # start watching once stock moves 20%+ intraday
SCAN_MAX_RESULTS = 50

# ── SMA Settings ──────────────────────────────────────
SMA_200_PERIODS = 200  # trend filter on 1-min, 5-min
SMA_30M_PERIOD = 50  # 50 bars of 30-min = 25 hours ~ 3 trading days
SMA_9_PERIOD = 9  # entry confirmation on 5-min
SMA_20_PERIOD = 20  # exit signal on 1-min

MA20_SLOPE_LOOKBACK = 5               # check MA20 slope over last 5 bars
TRAILING_STOP_CANDLE_BUFFER = 0.001   # 0.1% below prev candle low

# ── Order Execution ───────────────────────────────────
SLIPPAGE_PCT = 0.001  # 0.1% slippage (simulation only)
COMMISSION_PER_SHARE = 0.005  # $0.005 per share (IBKR tiered)
MIN_COMMISSION = 1.00  # minimum $1 per order
LIMIT_OFFSET_CENTS = 0.02  # $0.02 above ask (buy) / below bid (sell)
STOP_LIMIT_OFFSET_PCT = 0.02  # 2% below stop price for limit portion

# Smart bracket: fib-based stop + trailing take-profit
BRACKET_FIB_STOP_PCT = 0.03        # 3% below nearest fib support
BRACKET_TRAILING_PROFIT_PCT = 3.0   # 3% trailing stop for take-profit

# ── Fibonacci Engine (Dual-Series) ────────────────────
FIB_LOOKBACK_YEARS = 5  # find lowest daily candle in 5 years
FIB_CACHE_TTL_HOURS = 24
FIB_LEVELS_24 = [
    0, 0.236, 0.382, 0.5, 0.618, 0.764, 0.88, 1,
    1.272, 1.414, 1.618, 2, 2.272, 2.414, 2.618, 3,
    3.272, 3.414, 3.618, 4, 4.236, 4.414, 4.618, 4.764,
]

# Color per fib ratio (matches TradingView preset)
FIB_LEVEL_COLORS = {
    0:     '#808080',  # gray
    0.236: '#ef5350',  # red
    0.382: '#00bcd4',  # cyan
    0.5:   '#ff9800',  # orange
    0.618: '#ffffff',  # white
    0.764: '#ef5350',  # red
    0.88:  '#2196f3',  # blue
    1:     '#4caf50',  # green
    1.272: '#b388ff',  # purple
    1.414: '#2196f3',  # blue
    1.618: '#cccccc',  # light gray
    2:     '#4caf50',  # green
    2.272: '#b388ff',  # purple
    2.414: '#2196f3',  # blue
    2.618: '#cccccc',  # light gray
    3:     '#4caf50',  # green
    3.272: '#4caf50',  # green
    3.414: '#2196f3',  # blue
    3.618: '#cccccc',  # light gray
    4:     '#4caf50',  # green
    4.236: '#4caf50',  # green
    4.414: '#2196f3',  # blue
    4.618: '#cccccc',  # light gray
    4.764: '#ef5350',  # red
}

# ── Pre-Market Gapper Scanner ────────────────────────
PM_VOLUME_MIN = 50_000           # minimum cumulative volume before entry

# ── VWAP Pullback Entry ─────────────────────────────
VWAP_PROXIMITY_PCT = 0.005       # within 0.5% of VWAP = "near VWAP"
VWAP_OVERSHOOT_MAX_PCT = 0.015   # max 1.5% below VWAP (still valid pullback)
MIN_BARS_ABOVE_VWAP = 3          # bars above VWAP before pullback counts
BOUNCE_CONFIRM_BARS = 2          # bars closing above VWAP to confirm bounce
MAX_PULLBACK_BARS = 30           # timeout: abandon pullback after 30 bars (~1hr)
MAX_PULLBACK_DEPTH_PCT = 0.05    # abandon if pullback > 5% deep
VWAP_WARMUP_BARS = 10            # VWAP needs 10+ bars to be reliable
MAX_ENTRIES_PER_DAY = 2          # max entries per symbol per day
ENTRY_WINDOW_END = "11:30"       # no new entries after 11:30 AM ET

# ── Dynamic Stop Loss ───────────────────────────────
STOP_BUFFER_PCT = 0.003          # 0.3% below pullback low
STOP_MIN_DISTANCE_PCT = 0.015    # stop at least 1.5% below entry
STOP_MAX_DISTANCE_PCT = 0.05     # stop at most 5% below entry

# ── Two-Phase Trailing Stop ─────────────────────────
TRAILING_PHASE1_BARS = 15        # phase 1: conservative (~30 min)
TRAILING_PHASE2_ATR_MULT = 1.5   # phase 2: ATR x 1.5 (tighter than current 2.0)
VWAP_EXIT_BARS = 3               # exit if 3 consecutive closes below VWAP

# ── Re-Entry Rules ──────────────────────────────────
RE_ENTRY_COOLDOWN_BARS = 15      # wait ~30 min after exit

# ── Trailing Stop (safety net) ────────────────────────
TRAILING_STOP_INITIAL_PCT = 0.10  # 10% below entry as safety
TRAILING_STOP_CHECK_INTERVAL = 2  # seconds between checks
TRAILING_ATR_PERIOD = 14  # ATR lookback period
# ── Market Hours (US/Eastern) ──────────────────────────
TIMEZONE = "US/Eastern"
PREMARKET_START = "04:00"
MARKET_OPEN = "09:30"
MARKET_CLOSE = "16:00"
AFTERHOURS_END = "20:00"

# ── Scan Intervals ─────────────────────────────────────
SCAN_INTERVAL_SECONDS = 30  # how often to scan for new candidates
WATCHLIST_REFRESH_SECONDS = 60  # pre/after-market watchlist refresh

DASHBOARD_PORT = 8501

# ── Fibonacci Backtest ───────────────────────────────
FIB_STOP_BUFFER_PCT = 0.015       # 1.5% below entry for stop
FIB_TARGET_LEVELS_ABOVE = 3       # take-profit 3 fib levels above entry
FIB_MAX_ENTRIES_PER_DAY = 2       # max entries per gap day
FIB_WARMUP_BARS = 15              # first 30 min (2-min bars) to establish gap high
FIB_ENTRY_WINDOW_END = "15:00"    # no new entries after 3 PM ET
FIB_GAP_LOOKBACK_DAYS = 30        # 30-day lookback for gap detection
FIB_CHARTS_DIR = DATA_DIR / "fib_charts"
FIB_CHARTS_DIR.mkdir(exist_ok=True)

# ── Fibonacci Live Trading ────────────────────────────
FIB_LIVE_SCAN_INTERVAL = 30          # seconds between scan cycles
FIB_LIVE_BAR_SIZE = "2 mins"         # bar size for strategy
FIB_LIVE_BAR_DURATION = "1 D"        # how much history to request
FIB_LIVE_MAX_SYMBOLS = 5             # max symbols to monitor simultaneously
FIB_LIVE_STOP_PCT = 0.015            # 1.5% below entry (matches backtest)
FIB_LIVE_TARGET_LEVELS = 3           # 3 fib levels above entry (matches backtest)
FIB_LIVE_MAX_ENTRIES_PER_DAY = 2     # max entries per day total
FIB_LIVE_ENTRY_WINDOW_END = "15:00"  # no entries after 3 PM ET
FIB_LIVE_WARMUP_BARS = 15            # first 30 min to establish gap high
FIB_LIVE_GAP_MIN_PCT = 10.0          # minimum gap % to qualify

# ── Fibonacci Confirmation Strategy ──────────────────
FIB_CONFIRM_SCAN_INTERVAL = 30           # seconds between scan cycles
FIB_CONFIRM_BAR_SIZE = "1 min"           # bar size for strategy (1-min candles)
FIB_CONFIRM_BAR_DURATION = "2 D"         # 2 days for SMA 200 on 1-min
FIB_CONFIRM_MAX_SYMBOLS = 5             # max symbols to monitor simultaneously
FIB_CONFIRM_STOP_PCT = 0.015            # 1.5% below entry fib level
FIB_CONFIRM_TARGET_LEVELS = 3           # 3 fib levels above entry
FIB_CONFIRM_MAX_ENTRIES_PER_DAY = 2     # max entries per day total
FIB_CONFIRM_ENTRY_WINDOW_END = "15:00"  # no entries after 3 PM ET
FIB_CONFIRM_WARMUP_BARS = 30            # warmup period for indicators
FIB_CONFIRM_GAIN_MIN_PCT = 20.0         # minimum gain % to qualify
FIB_CONFIRM_FLOAT_MAX = 60_000_000      # max float 60M shares
FIB_CONFIRM_RVOL_MIN = 1.5             # minimum relative volume vs 14-day avg
FIB_CONFIRM_SMA_SHORT = 20             # short SMA period for gate
FIB_CONFIRM_SMA_LONG = 200             # long SMA period for gate

# ── Fibonacci Double-Touch Backtest ────────────────────
FIB_DT_STOP_PCT = 0.04             # 4% below fib level (was 3%, too tight)
FIB_DT_TARGET_LEVELS = 3           # exit 50% at 3rd fib level above
FIB_DT_PROXIMITY_PCT = 0.008       # 0.8% proximity to fib level
FIB_DT_MIN_BOUNCE_BARS = 3         # min bars between first and second touch
FIB_DT_MAX_ENTRIES_PER_DAY = 3     # max entries per gap day
FIB_DT_GAP_MIN_PCT = 30.0             # skip gaps < 30% (focus on extreme movers)
FIB_DT_GAP_MAX_PCT = 80.0             # skip gaps > 80%
FIB_DT_RVOL_MIN = 2.0                 # minimum relative volume
FIB_DT_REQUIRE_NEWS = True            # require news catalyst
FIB_DT_MIN_GAP_VOLUME = 10_000        # minimum gap-day volume (filter illiquid)
FIB_DT_ENTRY_WINDOW_START = "08:00"   # no entries before 8 AM ET (pre-market noise)
FIB_DT_ENTRY_WINDOW_END = "12:00"     # no entries after 12 PM ET
FIB_DT_PREFERRED_RATIOS = {0.382, 0.618, 2.272, 2.414, 3.272, 3.414, 3.618}
FIB_DT_USE_RATIO_FILTER = True        # only enter on preferred fib ratios
FIB_DT_S1_ONLY = True                 # only use S1 series (S2 has poor WR)

# ── Fibonacci Double-Touch LIVE Trading ──────────────
FIB_DT_LIVE_SCAN_INTERVAL = 15           # 15-sec cycle (match bar size)
FIB_DT_LIVE_BAR_SIZE = "15 secs"         # 15-second bars from IBKR
FIB_DT_LIVE_BAR_DURATION = "1 D"         # request full day of bars
FIB_DT_LIVE_MAX_SYMBOLS = 5              # max symbols to track
FIB_DT_LIVE_MAX_ENTRIES_PER_DAY = 3      # max entries per day
FIB_DT_LIVE_ENTRY_START = "08:00"        # no entries before 8 AM ET
FIB_DT_LIVE_ENTRY_END = "14:00"          # no entries after 2 PM ET
FIB_DT_LIVE_GAP_MIN_PCT = 10.0           # minimum gap %
FIB_DT_LIVE_GAP_MAX_PCT = 25.0           # maximum gap %
FIB_DT_LIVE_FLOAT_MAX = 500_000_000      # max float 500M shares
FIB_DT_LIVE_STOP_PCT = 0.06              # 6% below fib level (was 3%, too tight on pennies)
FIB_DT_LIVE_TARGET_LEVELS = 3            # 3rd fib level above entry
FIB_DT_LIVE_PROXIMITY_PCT = 0.008        # 0.8% proximity threshold
FIB_DT_LIVE_MIN_BOUNCE_BARS = 6          # min bars between touches (6 × 15s = 90s)
FIB_DT_LIVE_TRAILING_BARS = 10            # bars without new high before trailing exit (10 × 15s = 150s)
FIB_DT_LIVE_PREFERRED_RATIOS = {0.382, 0.5, 0.764, 0.88, 2.272, 2.414, 3.272, 3.414, 3.618}
FIB_DT_LIVE_MIN_TARGET_PCT = 0.04        # 4% minimum target distance
FIB_DT_LIVE_ATR_STOP_MULT = 2.0          # stop = fib - ATR * 2
FIB_DT_LIVE_ATR_STOP_MIN_PCT = 0.03      # clamp min 3% below fib
FIB_DT_LIVE_ATR_STOP_MAX_PCT = 0.10      # clamp max 10% below fib
FIB_DT_LIVE_MAX_BAR_RANGE_PCT = 0.03     # skip if avg bar range > 3% (spread proxy)

# ── Momentum Ride Backtest ──────────────────────────────
MR_STOP_EXTRA_PCT = 0.02             # 2% below nearest fib level
MR_VWAP_MAX_DISTANCE_PCT = 0.15      # max 15% above VWAP (skip stretched)
MR_TRACKING_MINUTES = 90             # track stock for 90 min from first scan
MR_GAP_MIN_PCT = 35.0                # minimum gap % (focus on extreme movers)
MR_GAP_MAX_PCT = 150.0               # maximum gap % (allow big winners)
MR_LIVE_RVOL_MIN = 2.0               # minimum relative volume
MR_LIVE_REQUIRE_NEWS = True           # require news catalyst
MR_MIN_GAP_VOLUME = 10_000           # minimum gap-day volume
MR_EXIT_BARS_IN_MINUTE = 3           # exit after 3 of 4 15s bars with no new high

# ── Gap and Go ────────────────────────────────────────
GG_GAP_MIN_PCT = 15.0             # backtest min gap %
GG_ENTRY_AFTER_MINUTES = 5        # enter after 9:35 (backtest)

# ── Gap and Go LIVE ───────────────────────────────────
GG_LIVE_GAP_MIN_PCT = 30.0
GG_LIVE_RVOL_MIN = 2.5                # minimum relative volume (strong momentum)
GG_LIVE_REQUIRE_NEWS = True            # require news catalyst
GG_LIVE_INITIAL_CASH = 3000.0
GG_LIVE_POSITION_SIZE_FIXED = 700      # $700 per position (was $1K — risk $42/trade at 6% stop)
GG_LIVE_VWAP_PROXIMITY_PCT = 0.05      # first entry: within 5% of VWAP (was 2%, too tight for 30%+ gappers)
GG_LIVE_REENTRY_VWAP_MAX_PCT = 0.06   # re-entry: max 6% above VWAP (was unlimited)
GG_LIVE_REENTRY_MIN_VOLUME = 5000     # re-entry: min 5K shares last bar (vs 1K first entry)
GG_LIVE_MAX_TRACKED_SYMBOLS = 8
GG_LIVE_MAX_POSITIONS = 3              # max open positions
GG_LIVE_SAFETY_STOP_PCT = 0.08         # 8% max loss from entry (6% was too tight for penny volatility)
GG_LIVE_TRAILING_STOP_PCT = 0.07       # 7% trailing from highest high (was 10%)
GG_LIVE_MIN_HOLD_SEC = 600             # 10 min minimum hold before HA exit allowed
GG_LIVE_HA_EXIT_BARS = 3              # 3 consecutive HA red bars required for exit
GG_LIVE_PROFIT_TARGET_PCT = 0.05      # +5% → sell half, move stop to breakeven
GG_MAX_HOLD_MINUTES = 120              # 2 hours max hold

# ── Momentum Ride LIVE ───────────────────────────────
MR_LIVE_INITIAL_CASH = 3000.0
MR_LIVE_POSITION_SIZE_FIXED = 700      # $700 per position (was $1K — risk $49/trade at 7% stop)
MR_LIVE_MAX_TRACKED_SYMBOLS = 5
MR_LIVE_MAX_POSITIONS = 3              # max open positions
MR_TRAILING_STOP_PCT = 0.07            # 7% trailing from highest high (was 12%, too wide)
MR_SAFETY_STOP_PCT = 0.07             # 7% below entry (was 10%)
MR_LIVE_TRAILING_STOP_PCT = MR_TRAILING_STOP_PCT
MR_LIVE_SAFETY_STOP_PCT = MR_SAFETY_STOP_PCT
MR_LIVE_PULLBACK_TOUCH_PCT = 0.02      # low within 2% of VWAP
MR_ATR_STOP_MAX_PCT = 0.05            # cap ATR-based stop distance at 5% (penny 1.5x = 7.5%) — was 8%, too wide
MR_ATR_CAP = 0.10                     # if ATR > 0.10, reject ATR trailing — use fixed % instead
MR_REENTRY_COOLDOWN_SEC = 300         # 5 min between re-entries on same symbol
MR_MAX_ENTRIES_PER_SYM_PER_DAY = 5    # max 5 entries per symbol per day
MR_LIVE_PROFIT_TARGET_PCT = 0.10      # +10% → sell half, move stop to breakeven

# ── Float Turnover (FT) Robot ──────────────────────────
FT_LIVE_INITIAL_CASH = 3000.0
FT_LIVE_POSITION_SIZE_FIXED = 700      # $700 per position (was $1K — risk $42/trade at 6% stop)
FT_LIVE_MAX_TRACKED_SYMBOLS = 5        # was 20 — concentrate on top turnover
FT_LIVE_MAX_POSITIONS = 3              # max open positions
FT_MIN_FLOAT_TURNOVER_PCT = 25.0     # minimum % of float traded (was 15%)
FT_LIVE_GAP_MIN_PCT = 30.0           # minimum gap % (directional filter)
FT_LIVE_RVOL_MIN = 3.0               # minimum relative volume (extreme volume)
FT_LIVE_REQUIRE_NEWS = True           # require news catalyst
FT_SAFETY_STOP_PCT = 0.06            # 6% below entry — absolute safety net
FT_TRAILING_STOP_PCT = 0.06          # 6% trailing from highest high
FT_MAX_HOLD_MINUTES = 180             # 3 hours max hold (was 60min, killed winners mid-move)
FT_LIVE_PROFIT_TARGET_PCT = 0.08      # +8% → sell half, move stop to breakeven
FT_BREAKEVEN_OFFSET_PCT = 0.01       # breakeven triggers at entry + 1% (not exact entry — avoids noise)
FT_ENTRY_SKIP_BEFORE = "09:30"       # no FT entries before 9:30 ET (pre-market too volatile)

# ── Strategy-Wide Risk Controls ──────────────────────
STRATEGY_MIN_PRICE = 0.50              # skip stocks under $0.50 (huge spreads)
STRATEGY_MAX_BAR_RANGE_PCT = 0.05      # skip stocks with avg bar range > 5% (spread/chop proxy)
FIB_DT_POSITION_SIZE_FIXED = 500       # $500 per position (highest WR robot — risk $30/trade at 6% stop)
FIB_DT_MAX_POSITIONS = 3              # max open positions for FIB DT
FIB_DT_MAX_HOLD_MINUTES = 180         # 3 hours max hold
FIB_DT_WARMUP_SEC = 300               # 5 min warmup: skip entries for stocks seen < 5 min ago
STRATEGY_REENTRY_COOLDOWN_SEC = 600              # 10 min after exit before re-entry
STRATEGY_REENTRY_COOLDOWN_AFTER_LOSS_SEC = 1800  # 30 min after a losing exit
STRATEGY_MAX_ENTRIES_PER_STOCK_PER_DAY = 2       # max 2 entries per stock per day
STRATEGY_DAILY_LOSS_LIMIT = 300.0      # $300 max realized loss per strategy per day

# ── Smart Timed Exits (P&L-aware) ────────────────────────
TIMED_EXIT_IMMEDIATE_MAX_LOSS_PCT = 0.05   # close immediately if loss ≤ 5%
TIMED_EXIT_TIGHT_TRAIL_PCT = 0.02          # tight 2% trailing for deep-loss positions
TIMED_EXIT_TIGHT_TRAIL_PENNY_PCT = 0.03    # 3% for sub-$2 stocks

# ── EOD Profit Protection ────────────────────────────────
EOD_PROFIT_LOCK_TIME = "15:30"             # after this time, lock profits with tight trail
EOD_PROFIT_LOCK_TRAIL_PCT = 0.03           # 3% trailing stop on profitable positions
EOD_PROFIT_LOCK_PENNY_TRAIL_PCT = 0.04     # 4% for sub-$2 stocks

# ── VWAP Zone Live Robot ──────────────────────────────
VZ_LIVE_INITIAL_CASH = 3000.0
VZ_LIVE_POSITION_SIZE = 300               # $300 per position (default)
VZ_LIVE_POSITION_SIZE_SUB2 = 400          # $400 for sub-$2 stocks
VZ_LIVE_MAX_SLOTS_PER_SYM = 3            # 3 concurrent slots per symbol (SYM_0/1/2)
VZ_LIVE_VWAP_BAND_PCT = 0.03             # ±3% of VWAP for entry zone
VZ_LIVE_SMA_PERIOD = 20                  # SMA20 on 1-min bars
VZ_LIVE_ATR_PERIOD = 14                  # ATR(14)
VZ_LIVE_ATR_TRAIL_MULT = 1.5             # trailing stop = ATR × 1.5
VZ_LIVE_MIN_HOLD_BARS = 3                # min 3 bars before exit check
VZ_LIVE_MAX_ENTRIES_PER_SYM = 9          # max entries per symbol per day
VZ_LIVE_LOSS_COOLDOWN_SEC = 300           # 5 min cooldown after loss
VZ_LIVE_GAP_MIN_PCT = 20.0               # min gap % filter
VZ_LIVE_RVOL_MIN = 2.0                   # min relative volume
VZ_LIVE_REQUIRE_NEWS = True              # require news catalyst
VZ_LIVE_MAX_TRACKED_SYMBOLS = 3          # top N candidates to track
VZ_LIVE_MAX_HOLD_MINUTES = 240           # 4h max hold
VZ_LIVE_DEAD_ZONES = [                   # no entries during these windows (ET)
    ("11:00", "11:30"),
    ("13:00", "13:30"),
    ("14:00", "15:00"),
]

# ── Screen Monitor (IBKR Scanner) ────────────────────
MONITOR_IBKR_CLIENT_ID = 20
MONITOR_ORDER_CLIENT_ID = 21   # separate IBKR connection for order execution
MONITOR_SCAN_MAX_RESULTS = 50
MONITOR_PRICE_MIN = 0.01
MONITOR_PRICE_MAX = 20.00
MONITOR_DEFAULT_FREQ = 5
MONITOR_DEFAULT_ALERT_PCT = 7.0
ALERT_VWAP_MAX_BELOW_PCT = 5.0     # skip alerts if price > 5% below VWAP

# ── Logging ────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
