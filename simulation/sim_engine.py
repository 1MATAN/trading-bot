"""Simulation engine: VWAP Pullback on Gapper strategy.

Strategy:
  1. Stock gaps 20%+ → strong momentum/catalyst
  2. Wait for pullback to VWAP → enter on weakness, not chasing
  3. Stop below pullback low → tight, defined risk (~2-4%)
  4. VWAP acts as institutional support / volume-weighted reference

State machine:
  WATCHING → price above VWAP for 3+ bars, then starts dropping
  PULLING_BACK → price drops toward VWAP, track pullback low
    → touches VWAP (within 0.5%) or crosses below (max 1.5%)
  BOUNCE → 2 bars close above VWAP → ENTRY SIGNAL

Order execution model:
  - Entry signal fires at bar[i] → PENDING market buy
  - Fills at bar[i+1].open + slippage
  - Stop = pullback_low * (1 - 0.3%), clamped to 1.5-5% of entry
  - Two-phase trailing: phase1 anchored at pullback low, phase2 ATR x 1.5
  - VWAP exit: 3 consecutive closes below VWAP → sell

Session coverage:
  - Pre-market (4:00-9:30), regular (9:30-16:00), after-hours (16:00-20:00)
  - Uses extended hours data (prepost=True)
"""

import json
import logging
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time
from enum import Enum
from typing import Optional

import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
from ib_insync import IB, Stock, util as ib_util

from config.settings import (
    IBKR_HOST,
    IBKR_PORT,
    STARTING_CAPITAL,
    POSITION_SIZE_PCT,
    SCAN_PRICE_MIN,
    SCAN_PRICE_MAX,
    SCAN_FLOAT_MAX,
    SCAN_MOVE_PCT_MIN,
    SLIPPAGE_PCT,
    COMMISSION_PER_SHARE,
    MIN_COMMISSION,
    LIVE_STATE_PATH,
    # VWAP Pullback settings
    PM_VOLUME_MIN,
    VWAP_PROXIMITY_PCT,
    VWAP_OVERSHOOT_MAX_PCT,
    MIN_BARS_ABOVE_VWAP,
    BOUNCE_CONFIRM_BARS,
    MAX_PULLBACK_BARS,
    MAX_PULLBACK_DEPTH_PCT,
    VWAP_WARMUP_BARS,
    MAX_ENTRIES_PER_DAY,
    ENTRY_WINDOW_END,
    # Dynamic stop
    STOP_BUFFER_PCT,
    STOP_MIN_DISTANCE_PCT,
    STOP_MAX_DISTANCE_PCT,
    # Two-phase trailing
    TRAILING_PHASE1_BARS,
    TRAILING_PHASE2_ATR_MULT,
    VWAP_EXIT_BARS,
    TRAILING_ATR_PERIOD,
    # Re-entry
    RE_ENTRY_COOLDOWN_BARS,
)
from strategies.indicators import (
    vwap, vwap_value, atr_value,
)
from notifications.trade_logger import TradeLogger
from utils.time_utils import now_utc

logger = logging.getLogger("trading_bot.simulation")

# ── IBKR Connection Helper ──────────────────────────────

_ib: IB | None = None
_IBKR_SIM_CLIENT = 12


def _get_ib() -> IB | None:
    global _ib
    if _ib and _ib.isConnected():
        return _ib
    try:
        _ib = IB()
        _ib.connect(IBKR_HOST, IBKR_PORT, clientId=_IBKR_SIM_CLIENT, timeout=10)
        return _ib
    except Exception as e:
        logger.error(f"IBKR connect failed: {e}")
        _ib = None
        return None


# ── Order State Machine ──────────────────────────────────

class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    MARKET = "MARKET"
    STOP = "STOP"


class OrderStatus(Enum):
    PENDING = "PENDING"    # signal fired, waiting for next bar to fill
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"


@dataclass
class SimOrder:
    """A simulated order with realistic fill logic."""
    side: OrderSide
    order_type: OrderType
    quantity: int
    signal_price: float      # price when signal fired
    signal_time: datetime
    stop_price: float = 0.0  # for STOP orders only
    status: OrderStatus = OrderStatus.PENDING
    fill_price: float = 0.0
    fill_time: Optional[datetime] = None
    slippage: float = 0.0
    commission: float = 0.0
    reason: str = ""


@dataclass
class SimPosition:
    """An open simulated position."""
    symbol: str
    quantity: int
    entry_price: float        # actual fill price (with slippage)
    entry_time: datetime
    signal_price: float       # price when entry signal fired
    stop_price: float         # GTC safety stop
    trailing_stop_price: float  # trailing stop (prev candle low - buffer)
    entry_commission: float
    fib_info: dict = field(default_factory=dict)


@dataclass
class SimTrade:
    """A completed round-trip trade."""
    symbol: str
    quantity: int
    entry_signal_price: float
    entry_fill_price: float
    entry_time: datetime
    exit_signal_price: float
    exit_fill_price: float
    exit_time: datetime
    exit_reason: str
    pnl_gross: float          # before commissions
    pnl_net: float            # after commissions
    pnl_pct: float
    total_commission: float
    slippage_entry: float
    slippage_exit: float
    move_pct: float           # intraday move % that qualified this stock
    fib_info: dict = field(default_factory=dict)
    session: str = ""         # pre-market / regular / after-hours


@dataclass
class SimResult:
    """Simulation results summary."""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl_gross: float = 0.0
    total_pnl_net: float = 0.0
    total_commissions: float = 0.0
    total_slippage: float = 0.0
    max_drawdown: float = 0.0
    equity_curve: list = field(default_factory=list)
    trades: list = field(default_factory=list)
    symbols_tested: list = field(default_factory=list)
    days_with_move: int = 0


# ── Main Engine ──────────────────────────────────────────

class SimulationEngine:
    """Walks through 1-min bars simulating the full strategy."""

    def __init__(
        self,
        symbols: Optional[list[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        capital: float = STARTING_CAPITAL,
    ) -> None:
        self._symbols = symbols or []
        self._start_date = start_date
        self._end_date = end_date
        self._capital = capital
        self._cash = capital
        self._position: Optional[SimPosition] = None
        self._pending_buy: Optional[SimOrder] = None
        self._pending_sell: Optional[SimOrder] = None
        self._result = SimResult()
        self._trade_logger = TradeLogger()
        self._peak_equity = capital
        self._current_move_pct = 0.0    # intraday move % of current day

        # VWAP pullback state machine
        self._pullback_phase = "WATCHING"  # WATCHING / PULLING_BACK / BOUNCE
        self._bars_above_vwap = 0
        self._pullback_low = float('inf')
        self._pullback_bar_count = 0
        self._touched_vwap = False
        self._bounce_count = 0
        self._entries_today = 0
        self._day_volume = 0
        self._below_vwap_count = 0       # for VWAP exit
        self._re_entry_cooldown = 0      # bars since last exit

        # Trailing stop state
        self._trailing_stop_price = 0.0
        self._bars_since_entry = 0
        self._highest_high = 0.0

        # Parse entry window end time
        h, m = ENTRY_WINDOW_END.split(":")
        self._entry_window_end = time(int(h), int(m))

    # ── Public API ───────────────────────────────────────

    def run(self) -> SimResult:
        """Run simulation across all symbols."""
        logger.info(
            f"Simulation starting: capital=${self._capital:,.2f}, "
            f"period={self._start_date} to {self._end_date}"
        )

        if not self._symbols:
            self._symbols = self._scan_for_candidates()

        if not self._symbols:
            logger.warning("No symbols to simulate")
            return self._result

        self._result.symbols_tested = list(self._symbols)

        for symbol in self._symbols:
            try:
                self._simulate_symbol(symbol)
            except Exception as e:
                logger.error(f"Simulation error for {symbol}: {e}", exc_info=True)

        self._write_live_state()

        wr = self._win_rate()
        logger.info(
            f"Simulation complete: {self._result.total_trades} trades, "
            f"gross P&L=${self._result.total_pnl_gross:+,.2f}, "
            f"net P&L=${self._result.total_pnl_net:+,.2f}, "
            f"commissions=${self._result.total_commissions:,.2f}, "
            f"win rate={wr:.1%}, max DD={self._result.max_drawdown:.2%}"
        )
        return self._result

    # ── Symbol Scanning ──────────────────────────────────

    def _scan_for_candidates(self) -> list[str]:
        """Find stocks that match scanner criteria via IBKR.

        Look for stocks under $20 with float <= 60M that moved up >= 20%
        intraday (high vs previous close) at some point in the last 30 days.

        Strategy:
          1. Loop through universe, qualify contract, request 30-day daily bars
          2. Filter by price and intraday move from the daily data
          3. Only fetch float (reqFundamentalData) for symbols that passed
             price+move filter, with a delay between each call
        """
        logger.info("Scanning for qualifying symbols via IBKR...")

        ib = _get_ib()
        if ib is None:
            logger.error("Cannot scan: IBKR not connected")
            return []

        # Small cap / momentum universe ($1-$20 range)
        universe = [
            # EV / Tech small caps
            "CLOV", "SOFI", "SENS", "GSAT", "VTNR",
            "RCAT", "AISP", "BFRG", "HOLO", "TPST",
            # Crypto-adjacent / blockchain
            "BTBT", "SOS", "MARA", "RIOT", "BBAI",
            "HIVE", "BITF", "CLSK", "WULF", "CIFR", "IREN",
            # Uranium / energy
            "DNN", "URG", "UEC", "LEU", "NXE",
            # Tech / EV / Quantum
            "OPEN", "SOUN", "JOBY", "LILM", "ARQQ", "QUBT",
            # Additional momentum candidates
            "GEVO", "PLUG", "BLDP", "FCEL", "BE",
            "LAZR", "AEVA", "OUST", "INVZ", "LIDR",
            "NKLA", "GOEV", "WKHS", "REE", "ARVL",
            "DNA", "IOVA", "CRSP", "BEAM", "NTLA",
        ]

        # Step 1+2: Download 30-day daily bars per symbol, filter by price & move
        logger.info(f"  Downloading daily data for {len(universe)} symbols via IBKR...")
        move_candidates = []
        for sym in universe:
            try:
                contract = Stock(sym, "SMART", "USD")
                ib.qualifyContracts(contract)
                _time.sleep(0.5)  # rate limiting

                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime="",
                    durationStr="30 D",
                    barSizeSetting="1 day",
                    whatToShow="TRADES",
                    useRTH=False,
                )
                if not bars or len(bars) < 2:
                    continue

                df_sym = ib_util.df(bars)
                if df_sym.empty or len(df_sym) < 2:
                    continue

                last_price = float(df_sym["close"].iloc[-1])
                if last_price < SCAN_PRICE_MIN or last_price > SCAN_PRICE_MAX:
                    continue

                # Check for intraday move >= 20% (high vs prev close)
                prev_closes = df_sym["close"].shift(1)
                moves = (df_sym["high"] - prev_closes) / prev_closes * 100
                max_move = float(moves.max())
                if max_move >= SCAN_MOVE_PCT_MIN:
                    move_candidates.append((sym, last_price, max_move))
                    logger.info(
                        f"  Price+Move OK: {sym} price=${last_price:.2f} "
                        f"max_move={max_move:.1f}%"
                    )
            except Exception as e:
                logger.debug(f"Daily data failed for {sym}: {e}")

        if not move_candidates:
            logger.info("No symbols passed price + move filter")
            return []

        logger.info(
            f"  {len(move_candidates)} symbols passed price+move filter, "
            f"checking float..."
        )

        # Step 3: Check float for candidates via reqFundamentalData
        passed = []
        for sym, price, max_move in move_candidates:
            try:
                _time.sleep(0.5)  # rate limiting
                contract = Stock(sym, "SMART", "USD")
                ib.qualifyContracts(contract)

                float_shares = 0
                try:
                    xml_data = ib.reqFundamentalData(contract, reportType="ReportSnapshot")
                    if xml_data:
                        root = ET.fromstring(xml_data)
                        # Look for SharesOut in the XML
                        for elem in root.iter("SharesOut"):
                            val = elem.text or elem.get("TotalFloat", "0")
                            float_shares = float(val) * 1e6  # typically in millions
                            break
                        if float_shares == 0:
                            for elem in root.iter("SharesOutstanding"):
                                float_shares = float(elem.text or "0")
                                break
                except Exception:
                    logger.debug(f"  {sym}: cannot fetch fundamental data, skipping")
                    continue

                if float_shares and float_shares <= SCAN_FLOAT_MAX:
                    passed.append(sym)
                    logger.info(
                        f"  PASS: {sym} price=${price:.2f} "
                        f"float={float_shares/1e6:.1f}M max_move={max_move:.1f}%"
                    )
                else:
                    logger.debug(
                        f"  {sym}: float={float_shares/1e6:.1f}M > "
                        f"{SCAN_FLOAT_MAX/1e6:.0f}M limit"
                    )
            except Exception as e:
                logger.debug(f"Float check failed for {sym}: {e}")

        logger.info(f"Scanner found {len(passed)} qualifying symbols: {passed}")
        return passed

    # ── Per-Symbol Simulation ────────────────────────────

    def _simulate_symbol(self, symbol: str) -> None:
        """Walk through all 2-min bars for one symbol using VWAP pullback strategy."""
        logger.info(f"--- Simulating {symbol} ---")

        # Reset per-symbol state
        self._position = None
        self._pending_buy = None
        self._pending_sell = None
        self._current_move_pct = 0.0
        self._trailing_stop_price = 0.0
        self._bars_since_entry = 0
        self._highest_high = 0.0
        self._reset_pullback_state()
        self._entries_today = 0
        self._re_entry_cooldown = 0

        # Download data
        end = self._end_date or datetime.now().strftime("%Y-%m-%d")
        start_dt = datetime.strptime(
            self._start_date or (datetime.now() - timedelta(days=25)).strftime("%Y-%m-%d"),
            "%Y-%m-%d",
        )

        # Daily data (60 days — no longer need 5 years for Fibonacci)
        daily_start = (start_dt - timedelta(days=60)).strftime("%Y-%m-%d")
        daily_df = self._download_daily(symbol, daily_start, end)
        if daily_df is None:
            return

        # 2-min intraday data with extended hours
        df_1m = self._download_intraday(symbol)
        if df_1m is None:
            return

        # VWAP warmup: need at least VWAP_WARMUP_BARS per day
        warmup = max(VWAP_WARMUP_BARS + 5, 20)
        if len(df_1m) <= warmup:
            logger.warning(f"{symbol}: only {len(df_1m)} bars, need {warmup}+ for warmup")
            return

        logger.info(
            f"{symbol}: {len(df_1m)} intraday bars, {len(daily_df)} daily bars"
        )

        # Pre-compute move days from daily data (high vs prev close >= 20%)
        move_days = {}
        if hasattr(daily_df.index, 'date'):
            prev_close_daily = daily_df["close"].shift(1)
            daily_moves = (daily_df["high"] - prev_close_daily) / prev_close_daily * 100
            for idx, move_val in daily_moves.items():
                if pd.notna(move_val) and move_val >= SCAN_MOVE_PCT_MIN:
                    move_days[idx.date()] = float(move_val)
                    logger.debug(f"  {symbol}: move day {idx.date()} = {move_val:+.1f}%")

        if move_days:
            logger.info(f"  {symbol}: {len(move_days)} move days found: {list(move_days.keys())}")
        else:
            logger.info(f"  {symbol}: no move days >= {SCAN_MOVE_PCT_MIN}% found")

        # Track previous day's close for intraday move detection
        prev_day_close = None
        current_day = None
        day_high = 0.0  # track intraday high for move detection

        for i in range(warmup, len(df_1m)):
            bar = df_1m.iloc[i]
            bar_time = df_1m.index[i]
            bar_open = float(bar["open"])
            bar_high = float(bar["high"])
            bar_low = float(bar["low"])
            bar_close = float(bar["close"])
            bar_volume = float(bar["volume"])

            # Detect day change → compute intraday move, reset daily state
            bar_date = bar_time.date() if hasattr(bar_time, 'date') else None
            if bar_date and bar_date != current_day:
                # Check pre-computed move days from daily data first
                if bar_date in move_days:
                    self._current_move_pct = move_days[bar_date]
                    logger.debug(
                        f"  {symbol} {bar_date}: move {self._current_move_pct:+.1f}% "
                        f"(from daily data)"
                    )
                    self._result.days_with_move += 1
                else:
                    self._current_move_pct = 0.0

                # Find previous day's close from the last bar of the previous day
                prev_day_bars = df_1m[df_1m.index.date < bar_date] if hasattr(df_1m.index, 'date') else None
                if prev_day_bars is not None and len(prev_day_bars) > 0:
                    prev_day_close = float(prev_day_bars["close"].iloc[-1])
                current_day = bar_date
                day_high = bar_high

                # Reset daily state
                self._day_volume = 0
                self._entries_today = 0
                self._reset_pullback_state()
            else:
                # Track intraday high
                day_high = max(day_high, bar_high)
                # Dynamically compute move if not from daily data
                if self._current_move_pct < SCAN_MOVE_PCT_MIN and prev_day_close and prev_day_close > 0:
                    intraday_move = ((day_high - prev_day_close) / prev_day_close) * 100
                    if intraday_move >= SCAN_MOVE_PCT_MIN:
                        self._current_move_pct = intraday_move
                        self._result.days_with_move += 1
                        logger.debug(
                            f"  {symbol} {bar_date}: intraday move {intraday_move:+.1f}% "
                            f"(prev close=${prev_day_close:.2f} → high=${day_high:.2f})"
                        )

            # Accumulate daily volume
            self._day_volume += bar_volume

            # Build window up to current bar (no look-ahead)
            window_1m = df_1m.iloc[:i + 1]

            # ── Step 1: Fill pending orders at this bar's open ──
            self._try_fill_pending_buy(symbol, bar_open, bar_time)
            self._try_fill_pending_sell(symbol, bar_open, bar_time)

            # ── Step 2: Check safety stop trigger on this bar's low ──
            if self._position and self._position.symbol == symbol:
                if bar_low <= self._position.stop_price:
                    self._execute_stop_hit(symbol, bar_low, bar_time)

            # ── Step 3: Update trailing stop and check trigger ──
            if (self._position and self._position.symbol == symbol
                    and self._pending_sell is None):
                self._update_trailing_stop(symbol, bar_low, bar_close, window_1m, bar_time)

            # ── Step 4: Check VWAP exit ──
            if (self._position and self._position.symbol == symbol
                    and self._pending_sell is None):
                self._check_vwap_exit(symbol, bar_close, window_1m, bar_time)

            # ── Step 5: Check entry (only if no position and no pending buy) ──
            if self._position is None and self._pending_buy is None:
                self._check_entry(
                    symbol, bar_close, bar_high, window_1m, bar_time,
                )

            # Decrement re-entry cooldown
            if self._re_entry_cooldown > 0:
                self._re_entry_cooldown -= 1

            # ── Step 6: Track equity ──
            equity = self._compute_equity(bar_close, symbol)
            if equity > self._peak_equity:
                self._peak_equity = equity
            dd = (self._peak_equity - equity) / self._peak_equity if self._peak_equity > 0 else 0
            if dd > self._result.max_drawdown:
                self._result.max_drawdown = dd

            # Sample equity curve every 60 bars (~2 hours at 2-min)
            if i % 60 == 0:
                self._result.equity_curve.append({
                    "time": str(bar_time),
                    "equity": round(equity, 2),
                })

        # If position is still open at end, force close at last bar's close
        if self._position and self._position.symbol == symbol:
            last_close = float(df_1m["close"].iloc[-1])
            last_time = df_1m.index[-1]
            self._force_close(symbol, last_close, last_time, "end_of_data")

    # ── Order Fills ──────────────────────────────────────

    def _try_fill_pending_buy(self, symbol: str, bar_open: float, bar_time) -> None:
        """Fill a pending buy order at this bar's open + slippage."""
        if self._pending_buy is None:
            return
        if self._pending_buy.status != OrderStatus.PENDING:
            return

        # Fill at open + slippage (buying pushes price up)
        slippage = bar_open * SLIPPAGE_PCT
        fill_price = round(bar_open + slippage, 4)
        qty = self._pending_buy.quantity
        commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
        cost = qty * fill_price + commission

        if cost > self._cash:
            # Not enough cash — reduce quantity
            qty = int((self._cash - MIN_COMMISSION) / fill_price)
            if qty <= 0:
                logger.warning(f"  {symbol}: can't afford buy, cancelling order")
                self._pending_buy.status = OrderStatus.CANCELLED
                self._pending_buy = None
                return
            commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
            cost = qty * fill_price + commission

        self._pending_buy.status = OrderStatus.FILLED
        self._pending_buy.fill_price = fill_price
        self._pending_buy.fill_time = bar_time
        self._pending_buy.slippage = slippage * qty
        self._pending_buy.commission = commission

        # Deduct cash
        self._cash -= cost

        # Dynamic stop: pullback low - buffer, clamped to 1.5-5% of entry
        pullback_low = self._pullback_low if self._pullback_low < float('inf') else fill_price * 0.97
        raw_stop = pullback_low * (1 - STOP_BUFFER_PCT)
        stop_floor = fill_price * (1 - STOP_MAX_DISTANCE_PCT)
        stop_ceil = fill_price * (1 - STOP_MIN_DISTANCE_PCT)
        stop_price = round(max(stop_floor, min(raw_stop, stop_ceil)), 4)

        # Initialize trailing stop at pullback low (phase 1)
        initial_trailing = stop_price
        self._trailing_stop_price = initial_trailing
        self._bars_since_entry = 0
        self._highest_high = bar_open
        self._below_vwap_count = 0

        # Open position
        self._position = SimPosition(
            symbol=symbol,
            quantity=qty,
            entry_price=fill_price,
            entry_time=bar_time,
            signal_price=self._pending_buy.signal_price,
            stop_price=stop_price,
            trailing_stop_price=initial_trailing,
            entry_commission=commission,
            fib_info={},
        )

        logger.info(
            f"  BUY FILLED: {symbol} {qty} shares @ ${fill_price:.4f} "
            f"(signal=${self._pending_buy.signal_price:.4f}, "
            f"slip=${slippage:.4f}, comm=${commission:.2f}, "
            f"stop=${stop_price:.4f})"
        )

        self._trade_logger.log_entry(
            symbol=symbol,
            quantity=qty,
            price=fill_price,
            stop_price=stop_price,
            notes=f"sim signal@${self._pending_buy.signal_price:.4f} slip=${slippage:.4f}",
        )

        self._pending_buy = None

    def _try_fill_pending_sell(self, symbol: str, bar_open: float, bar_time) -> None:
        """Fill a pending sell order at this bar's open - slippage."""
        if self._pending_sell is None:
            return
        if self._pending_sell.status != OrderStatus.PENDING:
            return
        if self._position is None:
            self._pending_sell = None
            return

        pos = self._position
        # Fill at open - slippage (selling pushes price down)
        slippage = bar_open * SLIPPAGE_PCT
        fill_price = round(bar_open - slippage, 4)
        fill_price = max(fill_price, 0.01)  # floor
        commission = max(pos.quantity * COMMISSION_PER_SHARE, MIN_COMMISSION)

        self._pending_sell.status = OrderStatus.FILLED
        self._pending_sell.fill_price = fill_price
        self._pending_sell.fill_time = bar_time
        self._pending_sell.slippage = slippage * pos.quantity
        self._pending_sell.commission = commission

        # Add proceeds to cash
        proceeds = pos.quantity * fill_price - commission
        self._cash += proceeds

        # Record trade
        self._record_trade(pos, fill_price, bar_time,
                          self._pending_sell.reason,
                          commission, slippage * pos.quantity,
                          self._pending_sell.signal_price)

        self._position = None
        self._pending_sell = None
        self._trailing_stop_price = 0.0
        self._bars_since_entry = 0
        self._highest_high = 0.0
        self._re_entry_cooldown = RE_ENTRY_COOLDOWN_BARS
        self._reset_pullback_state()

    def _execute_stop_hit(self, symbol: str, bar_low: float, bar_time) -> None:
        """Stop loss triggered — fill at stop price (or open if gap down)."""
        pos = self._position
        if pos is None:
            return

        # Cancel any pending sell (stop takes priority)
        if self._pending_sell:
            self._pending_sell.status = OrderStatus.CANCELLED
            self._pending_sell = None

        # Fill at stop price (worst case: at bar_low if it gapped through)
        fill_price = min(pos.stop_price, bar_low)
        fill_price = max(fill_price, 0.01)
        commission = max(pos.quantity * COMMISSION_PER_SHARE, MIN_COMMISSION)

        # Add proceeds to cash
        proceeds = pos.quantity * fill_price - commission
        self._cash += proceeds

        logger.info(
            f"  STOP HIT: {symbol} {pos.quantity} shares @ ${fill_price:.4f} "
            f"(stop was ${pos.stop_price:.4f}, bar low=${bar_low:.4f})"
        )

        self._record_trade(pos, fill_price, bar_time,
                          f"stop_loss (stop=${pos.stop_price:.4f})",
                          commission, 0, fill_price)

        self._position = None
        self._trailing_stop_price = 0.0
        self._bars_since_entry = 0
        self._highest_high = 0.0
        self._re_entry_cooldown = RE_ENTRY_COOLDOWN_BARS
        self._reset_pullback_state()

    def _force_close(self, symbol: str, price: float, bar_time, reason: str) -> None:
        """Force-close a position at end of simulation."""
        pos = self._position
        if pos is None:
            return

        commission = max(pos.quantity * COMMISSION_PER_SHARE, MIN_COMMISSION)
        proceeds = pos.quantity * price - commission
        self._cash += proceeds

        logger.info(f"  FORCE CLOSE: {symbol} @ ${price:.4f} ({reason})")
        self._record_trade(pos, price, bar_time, reason, commission, 0, price)
        self._position = None

    # ── Pullback State Machine ─────────────────────────────

    def _reset_pullback_state(self) -> None:
        """Reset the pullback state machine to WATCHING."""
        self._pullback_phase = "WATCHING"
        self._bars_above_vwap = 0
        self._pullback_low = float('inf')
        self._pullback_bar_count = 0
        self._touched_vwap = False
        self._bounce_count = 0
        self._below_vwap_count = 0

    # ── Signal Checks ────────────────────────────────────

    def _check_entry(
        self, symbol: str, current_price: float,
        bar_high: float,
        df_1m: pd.DataFrame,
        bar_time,
    ) -> None:
        """VWAP pullback state machine entry logic.

        3 phases:
        WATCHING → price above VWAP for 3+ bars, then starts dropping
        PULLING_BACK → price drops toward VWAP, track pullback low
          → touches VWAP (within 0.5%) or crosses below (max 1.5%)
        BOUNCE → 2 bars close above VWAP → ENTRY SIGNAL
        """

        # Gate 0: Must have moved up >= 20% intraday
        if self._current_move_pct < SCAN_MOVE_PCT_MIN:
            return

        # Gate 0b: Price must be within $1-$20
        if current_price < SCAN_PRICE_MIN or current_price > SCAN_PRICE_MAX:
            return

        # Gate 1: Re-entry cooldown
        if self._re_entry_cooldown > 0:
            return

        # Gate 2: Max entries per day
        if self._entries_today >= MAX_ENTRIES_PER_DAY:
            return

        # Gate 3: Entry window (no new entries after 11:30 AM ET)
        bar_t = bar_time.time() if hasattr(bar_time, 'time') else None
        if bar_t and bar_t >= self._entry_window_end:
            return

        # Gate 4: Minimum pre-market volume
        if self._day_volume < PM_VOLUME_MIN:
            return

        # Gate 5: VWAP warmup
        if len(df_1m) < VWAP_WARMUP_BARS:
            return

        current_vwap = vwap_value(df_1m)
        if current_vwap <= 0:
            return

        # ── State Machine ──
        vwap_distance_pct = (current_price - current_vwap) / current_vwap

        if self._pullback_phase == "WATCHING":
            # Count bars where price closes above VWAP
            if current_price > current_vwap:
                self._bars_above_vwap += 1
            else:
                self._bars_above_vwap = 0

            # Transition: have enough bars above VWAP and price starts pulling back
            if self._bars_above_vwap >= MIN_BARS_ABOVE_VWAP:
                # Detect start of pullback: price dropping toward VWAP
                if vwap_distance_pct < 0.02:  # within 2% of VWAP = starting to pull back
                    self._pullback_phase = "PULLING_BACK"
                    self._pullback_low = current_price
                    self._pullback_bar_count = 1
                    self._touched_vwap = False
                    logger.debug(
                        f"  {symbol}: WATCHING → PULLING_BACK "
                        f"(bars_above={self._bars_above_vwap}, "
                        f"vwap_dist={vwap_distance_pct:.3f})"
                    )

        elif self._pullback_phase == "PULLING_BACK":
            self._pullback_bar_count += 1
            self._pullback_low = min(self._pullback_low, current_price)

            # Compute pullback depth from recent high
            pullback_depth = (bar_high - self._pullback_low) / bar_high if bar_high > 0 else 0

            # Abort: too many bars (timeout)
            if self._pullback_bar_count > MAX_PULLBACK_BARS:
                logger.debug(f"  {symbol}: pullback timeout ({self._pullback_bar_count} bars)")
                self._reset_pullback_state()
                return

            # Abort: pullback too deep (gap filling)
            if pullback_depth > MAX_PULLBACK_DEPTH_PCT:
                logger.debug(f"  {symbol}: pullback too deep ({pullback_depth:.1%})")
                self._reset_pullback_state()
                return

            # Abort: price dropped too far below VWAP (overshoot)
            if vwap_distance_pct < -VWAP_OVERSHOOT_MAX_PCT:
                logger.debug(f"  {symbol}: VWAP overshoot ({vwap_distance_pct:.3f})")
                self._reset_pullback_state()
                return

            # Check if price touched VWAP (within proximity) or crossed below
            if abs(vwap_distance_pct) <= VWAP_PROXIMITY_PCT or vwap_distance_pct < 0:
                self._touched_vwap = True

            # Transition to BOUNCE: touched VWAP and now closing above it
            if self._touched_vwap and current_price > current_vwap:
                self._pullback_phase = "BOUNCE"
                self._bounce_count = 1
                logger.debug(
                    f"  {symbol}: PULLING_BACK → BOUNCE "
                    f"(pullback_low=${self._pullback_low:.4f}, "
                    f"bars={self._pullback_bar_count})"
                )

        elif self._pullback_phase == "BOUNCE":
            # Count consecutive bars closing above VWAP
            if current_price > current_vwap:
                self._bounce_count += 1
            else:
                # Failed bounce — go back to pulling back
                self._pullback_phase = "PULLING_BACK"
                self._bounce_count = 0
                return

            # ENTRY: enough bounce confirmation bars
            if self._bounce_count >= BOUNCE_CONFIRM_BARS:
                position_dollars = self._cash * POSITION_SIZE_PCT
                quantity = int(position_dollars / current_price)
                if quantity <= 0:
                    return

                self._pending_buy = SimOrder(
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=quantity,
                    signal_price=current_price,
                    signal_time=bar_time,
                    reason=(
                        f"vwap_pullback|move={self._current_move_pct:.1f}%"
                        f"|pullback_low=${self._pullback_low:.4f}"
                        f"|vwap=${current_vwap:.4f}"
                        f"|bounce_bars={self._bounce_count}"
                    ),
                )

                self._entries_today += 1
                session = self._get_session(bar_time)
                logger.info(
                    f"  ENTRY SIGNAL [{session}]: {symbol} @ ${current_price:.4f} "
                    f"(vwap_pullback, move={self._current_move_pct:+.1f}%, "
                    f"pullback_low=${self._pullback_low:.4f}, "
                    f"vwap=${current_vwap:.4f}, "
                    f"qty={quantity}) → pending buy"
                )

                # Reset state machine after entry signal
                self._reset_pullback_state()

    def _update_trailing_stop(
        self, symbol: str, bar_low: float, bar_close: float,
        df_1m: pd.DataFrame, bar_time,
    ) -> None:
        """Two-phase trailing stop.

        Phase 1 (first 15 bars): Stop anchored at pullback low, only moves up
            on higher swing lows.
        Phase 2 (after 15 bars): ATR x 1.5 trailing from highest high.
        """
        if len(df_1m) < 2:
            return

        bar_high = float(df_1m["high"].iloc[-1])
        self._bars_since_entry += 1
        self._highest_high = max(self._highest_high, bar_high)

        if self._bars_since_entry <= TRAILING_PHASE1_BARS:
            # Phase 1: conservative — only move stop up on higher swing lows
            # Use the 3-bar low as swing reference
            if len(df_1m) >= 3:
                recent_low = min(
                    float(df_1m["low"].iloc[-1]),
                    float(df_1m["low"].iloc[-2]),
                    float(df_1m["low"].iloc[-3]),
                )
                new_stop = recent_low * (1 - STOP_BUFFER_PCT)
                if new_stop > self._trailing_stop_price:
                    old_stop = self._trailing_stop_price
                    self._trailing_stop_price = new_stop
                    self._position.trailing_stop_price = new_stop
                    logger.debug(
                        f"  {symbol}: phase1 trail ${old_stop:.4f} → ${new_stop:.4f} "
                        f"(bar={self._bars_since_entry})"
                    )
        else:
            # Phase 2: ATR-based trailing from highest high
            current_atr = atr_value(df_1m, TRAILING_ATR_PERIOD)
            if current_atr > 0:
                new_stop = self._highest_high - current_atr * TRAILING_PHASE2_ATR_MULT
            else:
                # Fallback: 3% below highest high
                new_stop = self._highest_high * 0.97

            if new_stop > self._trailing_stop_price:
                old_stop = self._trailing_stop_price
                self._trailing_stop_price = new_stop
                self._position.trailing_stop_price = new_stop
                logger.debug(
                    f"  {symbol}: phase2 trail ${old_stop:.4f} → ${new_stop:.4f} "
                    f"(ATR=${current_atr:.4f}, highest=${self._highest_high:.4f}, "
                    f"bar={self._bars_since_entry})"
                )

        # Don't check trigger in first 3 bars (let trade breathe)
        if self._bars_since_entry <= 3:
            return

        # Check if trailing stop triggered
        if bar_low <= self._trailing_stop_price:
            fill_price = min(self._trailing_stop_price, bar_low)
            fill_price = max(fill_price, 0.01)

            pos = self._position
            commission = max(pos.quantity * COMMISSION_PER_SHARE, MIN_COMMISSION)
            proceeds = pos.quantity * fill_price - commission
            self._cash += proceeds

            phase = "phase1" if self._bars_since_entry <= TRAILING_PHASE1_BARS else "phase2"
            session = self._get_session(bar_time)
            logger.info(
                f"  TRAILING STOP [{session}]: {symbol} {pos.quantity} shares "
                f"@ ${fill_price:.4f} ({phase}, trail=${self._trailing_stop_price:.4f}, "
                f"bar_low=${bar_low:.4f}, bars_held={self._bars_since_entry})"
            )

            self._record_trade(
                pos, fill_price, bar_time,
                f"trailing_stop_{phase} (stop=${self._trailing_stop_price:.4f})",
                commission, 0, fill_price,
            )

            self._position = None
            self._trailing_stop_price = 0.0
            self._bars_since_entry = 0
            self._highest_high = 0.0
            self._re_entry_cooldown = RE_ENTRY_COOLDOWN_BARS
            self._reset_pullback_state()

    def _check_vwap_exit(
        self, symbol: str, bar_close: float,
        df_1m: pd.DataFrame, bar_time,
    ) -> None:
        """Exit if price closes below VWAP for N consecutive bars."""
        current_vwap = vwap_value(df_1m)
        if current_vwap <= 0:
            return

        if bar_close < current_vwap:
            self._below_vwap_count += 1
        else:
            self._below_vwap_count = 0

        if self._below_vwap_count >= VWAP_EXIT_BARS:
            pos = self._position
            if pos is None:
                return

            # Place pending sell for next bar
            self._pending_sell = SimOrder(
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=pos.quantity,
                signal_price=bar_close,
                signal_time=bar_time,
                reason=f"vwap_exit ({self._below_vwap_count} bars below VWAP)",
            )

            session = self._get_session(bar_time)
            logger.info(
                f"  VWAP EXIT [{session}]: {symbol} "
                f"({self._below_vwap_count} bars below VWAP=${current_vwap:.4f}, "
                f"close=${bar_close:.4f}) → pending sell"
            )

    # ── Data Download ────────────────────────────────────

    def _download_daily(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """Download daily OHLCV including extended hours via IBKR."""
        ib = _get_ib()
        if ib is None:
            logger.warning(f"{symbol}: IBKR not connected for daily download")
            return None

        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)

        # Calculate duration in days from start to end
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")
        duration_days = (end_dt - start_dt).days + 1
        duration_str = f"{duration_days} D"

        # Format endDateTime for IBKR (yyyyMMdd HH:mm:ss)
        end_datetime = end_dt.strftime("%Y%m%d 23:59:59")

        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end_datetime,
                durationStr=duration_str,
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=False,
            )
        except Exception as e:
            logger.warning(f"{symbol}: IBKR daily download failed: {e}")
            return None

        if not bars or len(bars) < 20:
            logger.warning(f"{symbol}: insufficient daily data ({len(bars) if bars else 0} rows)")
            return None

        df = ib_util.df(bars)
        df.columns = [c.lower() for c in df.columns]
        # Set date index
        if "date" in df.columns:
            df.set_index("date", inplace=True)
        return df

    def _download_intraday(self, symbol: str) -> Optional[pd.DataFrame]:
        """Download intraday data WITH extended hours via IBKR.

        Uses 2-min bars with 30-day duration (IBKR limit for 2-min bars).
        Includes outside regular trading hours data.
        """
        ib = _get_ib()
        if ib is None:
            logger.warning(f"{symbol}: IBKR not connected for intraday download")
            return None

        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)

        # Use 2-min data (IBKR allows up to ~30 days for 2-min bars)
        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr="30 D",
                barSizeSetting="2 mins",
                whatToShow="TRADES",
                useRTH=False,
            )
            if bars and len(bars) > VWAP_WARMUP_BARS + 20:
                df = ib_util.df(bars)
                df.columns = [c.lower() for c in df.columns]
                # Set date index
                if "date" in df.columns:
                    df.set_index("date", inplace=True)
                logger.info(f"{symbol}: 2-min data -- {len(df)} bars over 30 days")
                return df
        except Exception as e:
            logger.debug(f"{symbol}: 2-min download failed: {e}")

        # Fallback to 1-min (IBKR allows ~7 days for 1-min bars)
        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr="7 D",
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=False,
            )
            if bars and len(bars) > VWAP_WARMUP_BARS + 20:
                df = ib_util.df(bars)
                df.columns = [c.lower() for c in df.columns]
                if "date" in df.columns:
                    df.set_index("date", inplace=True)
                logger.info(f"{symbol}: falling back to 1-min data ({len(df)} bars, 7 days)")
                return df
        except Exception:
            pass

        logger.warning(f"{symbol}: no intraday data available")
        return None

    def _resample(self, df: pd.DataFrame, freq: str) -> pd.DataFrame:
        """Resample 1-min data to larger timeframe."""
        if df.empty:
            return df
        return df.resample(freq).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }).dropna()

    # ── Trade Recording ──────────────────────────────────

    def _record_trade(
        self, pos: SimPosition, exit_price: float, exit_time,
        exit_reason: str, exit_commission: float,
        exit_slippage: float, exit_signal_price: float,
    ) -> None:
        """Record a completed round-trip trade."""
        total_commission = pos.entry_commission + exit_commission
        pnl_gross = (exit_price - pos.entry_price) * pos.quantity
        pnl_net = pnl_gross - total_commission

        trade = SimTrade(
            symbol=pos.symbol,
            quantity=pos.quantity,
            entry_signal_price=pos.signal_price,
            entry_fill_price=pos.entry_price,
            entry_time=pos.entry_time,
            exit_signal_price=exit_signal_price,
            exit_fill_price=exit_price,
            exit_time=exit_time,
            exit_reason=exit_reason,
            pnl_gross=round(pnl_gross, 2),
            pnl_net=round(pnl_net, 2),
            pnl_pct=round((exit_price - pos.entry_price) / pos.entry_price, 4) if pos.entry_price > 0 else 0,
            total_commission=round(total_commission, 2),
            slippage_entry=round(pos.entry_price - pos.signal_price, 4),
            slippage_exit=round(exit_slippage, 4),
            move_pct=round(self._current_move_pct, 2),
            fib_info=pos.fib_info,
            session=self._get_session(exit_time),
        )

        self._result.trades.append({
            "symbol": trade.symbol,
            "quantity": trade.quantity,
            "entry_signal": trade.entry_signal_price,
            "entry_fill": trade.entry_fill_price,
            "entry_time": str(trade.entry_time),
            "exit_signal": trade.exit_signal_price,
            "exit_fill": trade.exit_fill_price,
            "exit_time": str(trade.exit_time),
            "exit_reason": trade.exit_reason,
            "pnl_gross": trade.pnl_gross,
            "pnl_net": trade.pnl_net,
            "pnl_pct": f"{trade.pnl_pct:.2%}",
            "commission": trade.total_commission,
            "slippage_entry": trade.slippage_entry,
            "move_pct": f"{trade.move_pct:+.1f}%",
            "session": trade.session,
        })

        self._result.total_trades += 1
        self._result.total_pnl_gross += pnl_gross
        self._result.total_pnl_net += pnl_net
        self._result.total_commissions += total_commission
        self._result.total_slippage += abs(trade.slippage_entry) + abs(trade.slippage_exit)

        if pnl_net >= 0:
            self._result.winning_trades += 1
        else:
            self._result.losing_trades += 1

        emoji = "+" if pnl_net >= 0 else ""
        logger.info(
            f"  TRADE CLOSED: {pos.symbol} {pos.quantity}sh "
            f"entry=${pos.entry_price:.4f} exit=${exit_price:.4f} "
            f"P&L={emoji}${pnl_net:.2f} ({exit_reason})"
        )

        self._trade_logger.log_exit(
            symbol=pos.symbol,
            quantity=pos.quantity,
            price=exit_price,
            entry_price=pos.entry_price,
            entry_time=str(pos.entry_time),
            notes=f"sim {exit_reason} net_pnl=${pnl_net:.2f} comm=${total_commission:.2f}",
        )

    # ── Helpers ───────────────────────────────────────────

    def _compute_equity(self, current_price: float, symbol: str) -> float:
        """Cash + unrealized position value."""
        equity = self._cash
        if self._position and self._position.symbol == symbol:
            equity += self._position.quantity * current_price
        return equity

    def _win_rate(self) -> float:
        if self._result.total_trades == 0:
            return 0.0
        return self._result.winning_trades / self._result.total_trades

    @staticmethod
    def _get_session(bar_time) -> str:
        """Determine trading session from bar timestamp."""
        try:
            t = bar_time.time() if hasattr(bar_time, 'time') else None
            if t is None:
                return "unknown"
            if t < time(9, 30):
                return "pre-market"
            elif t < time(16, 0):
                return "regular"
            else:
                return "after-hours"
        except Exception:
            return "unknown"

    def _write_live_state(self) -> None:
        """Write simulation results for dashboard."""
        try:
            equity = self._cash
            if self._position:
                equity += self._position.quantity * self._position.entry_price

            state = {
                "timestamp": now_utc().isoformat(),
                "simulation_mode": True,
                "market_open": False,
                "premarket": False,
                "afterhours": False,
                "positions": [],
                "risk_status": {
                    "portfolio_value": round(equity, 2),
                    "daily_pnl": round(self._result.total_pnl_net, 2),
                    "daily_trades": self._result.total_trades,
                    "day_trades_rolling": 0,
                    "pdt_limit": 3,
                    "daily_loss_halt": False,
                    "max_daily_loss": equity * 0.03,
                    "in_cooldown": False,
                    "cooldown_remaining": 0,
                    "position_size_pct": POSITION_SIZE_PCT,
                    "max_positions": 1,
                },
                "scanner_candidates": 0,
                "simulation_results": {
                    "total_trades": self._result.total_trades,
                    "winning_trades": self._result.winning_trades,
                    "losing_trades": self._result.losing_trades,
                    "total_pnl_gross": round(self._result.total_pnl_gross, 2),
                    "total_pnl": round(self._result.total_pnl_net, 2),
                    "total_commissions": round(self._result.total_commissions, 2),
                    "total_slippage": round(self._result.total_slippage, 2),
                    "win_rate": round(self._win_rate(), 4),
                    "max_drawdown": round(self._result.max_drawdown, 4),
                    "final_equity": round(equity, 2),
                    "symbols_tested": self._result.symbols_tested,
                    "days_with_move": self._result.days_with_move,
                },
                "equity_curve": self._result.equity_curve[-200:],
                "recent_trades": self._result.trades[-50:],
            }
            LIVE_STATE_PATH.write_text(json.dumps(state, default=str, indent=2))
        except Exception as e:
            logger.error(f"Failed to write simulation state: {e}")


# ── Entry Point ──────────────────────────────────────────

async def run_simulation(
    symbols: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> SimResult:
    """Entry point for running a simulation."""
    engine = SimulationEngine(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    return engine.run()
