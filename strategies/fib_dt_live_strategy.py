"""Live Fibonacci double-touch state machine — mirrors simulation/fib_double_touch_backtest.py.

Per-symbol state machines that persist across 15-second scan cycles:
  SCANNING → TRACKING → entry signal (DTEntryRequest)
  IN_POSITION → TRAILING (after target fills, no-new-high exit)

Uses 15-sec IBKR bars and dual-series recursive Fibonacci levels.
"""

import logging
from dataclasses import dataclass, field
from datetime import time
from enum import Enum
from typing import Optional

import numpy as np
import pandas as pd
from ib_insync import Contract

from broker.ibkr_connection import get_connection
from config.settings import (
    FIB_DT_LIVE_BAR_SIZE,
    FIB_DT_LIVE_BAR_DURATION,
    FIB_DT_LIVE_STOP_PCT,
    FIB_DT_LIVE_TARGET_LEVELS,
    FIB_DT_LIVE_MAX_ENTRIES_PER_DAY,
    FIB_DT_LIVE_ENTRY_START,
    FIB_DT_LIVE_ENTRY_END,
    FIB_DT_LIVE_PROXIMITY_PCT,
    FIB_DT_LIVE_MIN_BOUNCE_BARS,
    FIB_DT_LIVE_PREFERRED_RATIOS,
    FIB_DT_LIVE_TRAILING_BARS,
    FIB_DT_LIVE_MIN_TARGET_PCT,
    FIB_DT_LIVE_ATR_STOP_MULT,
    FIB_DT_LIVE_ATR_STOP_MIN_PCT,
    FIB_DT_LIVE_ATR_STOP_MAX_PCT,
    FIB_DT_LIVE_MAX_BAR_RANGE_PCT,
    FIB_LOOKBACK_YEARS,
)
from scanner.gap_scanner import GapSignal
from strategies.fibonacci_engine import (
    find_anchor_candle,
    build_dual_series,
    advance_series,
    DualFibSeries,
)
from strategies.indicators import bars_to_dataframe
from utils.time_utils import now_et, today_str

logger = logging.getLogger("trading_bot.fib_dt_live_strategy")


# ── Enums & Data Classes ──────────────────────────────────

class DTSymbolPhase(Enum):
    SCANNING = "SCANNING"
    IN_POSITION = "IN_POSITION"
    TRAILING = "TRAILING"


@dataclass
class DTTouchState:
    """Per-fib-level double-touch tracking."""
    phase: str = "IDLE"        # IDLE -> FIRST_TOUCH -> BOUNCED -> (trigger)
    first_touch_bar: int = -1  # bar index of first touch
    fib_price: float = 0.0
    fib_idx: int = -1


@dataclass
class DTEntryRequest:
    """Signal from strategy to entry executor."""
    symbol: str
    contract: Contract
    fib_level: float
    fib_idx: int
    fib_ratio: float
    entry_price: float  # latest bar close (estimated fill)
    stop_price: float
    target_price: float
    gap_pct: float


@dataclass
class DTTrailingExit:
    """Signal to sell remaining shares (no-new-high trailing exit)."""
    symbol: str
    contract: Contract
    reason: str


@dataclass
class DTSymbolState:
    """Per-symbol state persisting across scan cycles."""
    symbol: str
    contract: Contract
    phase: DTSymbolPhase = DTSymbolPhase.SCANNING
    prev_close: float = 0.0
    gap_high: float = 0.0
    fib_prices: list[float] = field(default_factory=list)
    fib_price_info: dict = field(default_factory=dict)
    dual_fib: Optional[DualFibSeries] = None
    touch_states: dict[int, DTTouchState] = field(default_factory=dict)
    # Tracking
    last_bar_count: int = 0
    date_key: str = ""
    # Trailing state
    prev_bar_high: float = 0.0
    trailing_no_high_count: int = 0  # consecutive bars without new high


# ── Fibonacci helpers ──────────────────────────────────────

def _get_all_fib_prices(dual: DualFibSeries, current_price: float) -> list[float]:
    """All unique prices from both series, sorted ascending. Auto-advances if needed."""
    while True:
        s1_top = dual.series1.levels[-1][1] if dual.series1.levels else float("inf")
        if current_price <= s1_top:
            break
        dual = advance_series(dual)
        if dual.advance_count > 20:
            break

    seen = set()
    prices = []
    for _ratio, price in dual.series1.levels:
        key = round(price, 4)
        if key not in seen:
            seen.add(key)
            prices.append(price)
    for _ratio, price in dual.series2.levels:
        key = round(price, 4)
        if key not in seen:
            seen.add(key)
            prices.append(price)
    prices.sort()
    return prices


def _get_fib_price_info(dual: DualFibSeries, current_price: float) -> dict[float, tuple[float, str]]:
    """Build mapping from rounded fib price -> (ratio, series_name)."""
    while True:
        s1_top = dual.series1.levels[-1][1] if dual.series1.levels else float("inf")
        if current_price <= s1_top:
            break
        dual = advance_series(dual)
        if dual.advance_count > 20:
            break

    info: dict[float, tuple[float, str]] = {}
    for ratio, price in dual.series1.levels:
        key = round(price, 4)
        if key not in info:
            info[key] = (ratio, "S1")
    for ratio, price in dual.series2.levels:
        key = round(price, 4)
        if key not in info:
            info[key] = (ratio, "S2")
    return info


_ATR_PERIOD = 14


def _compute_atr_from_15s(df: pd.DataFrame) -> float:
    """Resample 15-sec bars → 1-min, then compute ATR(14). Returns 0.0 if insufficient data."""
    if len(df) < 56:  # need ≥56 × 15s bars to get ≥14 × 1-min bars
        return 0.0

    ohlc = df.resample("1min").agg({"open": "first", "high": "max", "low": "min", "close": "last"}).dropna()
    if len(ohlc) < _ATR_PERIOD + 1:
        return 0.0

    highs = ohlc["high"].values
    lows = ohlc["low"].values
    closes = ohlc["close"].values

    prev_close = np.roll(closes, 1)
    prev_close[0] = closes[0]
    tr = np.maximum(highs - lows, np.maximum(np.abs(highs - prev_close), np.abs(lows - prev_close)))

    return float(np.mean(tr[-_ATR_PERIOD:]))


def _check_bar_range_filter(df: pd.DataFrame) -> bool:
    """Return True if stock passes bar-range filter (avg range ≤ threshold). False = reject."""
    tail = df.tail(10)
    if len(tail) < 5:
        return True  # not enough data, allow
    closes = tail["close"].values
    ranges = (tail["high"].values - tail["low"].values) / np.where(closes > 0, closes, 1.0)
    avg_range = float(np.mean(ranges))
    if avg_range > FIB_DT_LIVE_MAX_BAR_RANGE_PCT:
        return False
    return True


# ── Main Strategy ──────────────────────────────────────────

class FibDTLiveStrategy:
    """Per-symbol state machines for double-touch fib entries on gap stocks."""

    def __init__(self) -> None:
        self._conn = get_connection()
        self._states: dict[str, DTSymbolState] = {}
        self._entries_today = 0
        self._entries_date: str = ""
        h_start, m_start = FIB_DT_LIVE_ENTRY_START.split(":")
        h_end, m_end = FIB_DT_LIVE_ENTRY_END.split(":")
        self._entry_start = time(int(h_start), int(m_start))
        self._entry_cutoff = time(int(h_end), int(m_end))

    @property
    def entries_today(self) -> int:
        self._reset_daily_if_needed()
        return self._entries_today

    def record_entry(self) -> None:
        """Called by entry executor after a successful fill."""
        self._reset_daily_if_needed()
        self._entries_today += 1

    def mark_in_position(self, symbol: str) -> None:
        """Transition symbol to IN_POSITION (split orders handle exit)."""
        st = self._states.get(symbol)
        if st:
            st.phase = DTSymbolPhase.IN_POSITION
            logger.info(f"[{symbol}] -> IN_POSITION (split exit orders active)")

    def mark_trailing(self, symbol: str) -> None:
        """Transition symbol to TRAILING (target filled, monitoring no-new-high)."""
        st = self._states.get(symbol)
        if st:
            st.phase = DTSymbolPhase.TRAILING
            st.prev_bar_high = 0.0  # reset, will be set from next bar
            st.trailing_no_high_count = 0
            logger.info(f"[{symbol}] -> TRAILING (target filled, monitoring no-new-high)")

    def mark_position_closed(self, symbol: str) -> None:
        """Reset symbol back to SCANNING after position fully closes."""
        st = self._states.get(symbol)
        if st:
            st.phase = DTSymbolPhase.SCANNING
            # Reset touch states
            for ts in st.touch_states.values():
                ts.phase = "IDLE"
                ts.first_touch_bar = -1
            logger.info(f"[{symbol}] -> SCANNING (position closed)")

    async def process_cycle(
        self, gap_signals: list[GapSignal]
    ) -> tuple[list[DTEntryRequest], list[DTTrailingExit]]:
        """Run one strategy cycle across all tracked symbols.

        Returns (entry_requests, trailing_exits).
        """
        self._reset_daily_if_needed()
        entry_requests: list[DTEntryRequest] = []
        trailing_exits: list[DTTrailingExit] = []

        now = now_et()
        in_entry_window = self._entry_start <= now.time() < self._entry_cutoff

        # Process gap signals for entries
        if in_entry_window and self._entries_today < FIB_DT_LIVE_MAX_ENTRIES_PER_DAY:
            for signal in gap_signals:
                try:
                    req = await self._process_symbol(signal)
                    if req:
                        entry_requests.append(req)
                        if self._entries_today + len(entry_requests) >= FIB_DT_LIVE_MAX_ENTRIES_PER_DAY:
                            break
                except Exception as e:
                    logger.error(f"Strategy error for {signal.symbol}: {e}")

        # Check trailing exits for symbols in TRAILING phase
        for symbol, st in list(self._states.items()):
            if st.phase == DTSymbolPhase.TRAILING:
                try:
                    exit_signal = await self._check_trailing(st)
                    if exit_signal:
                        trailing_exits.append(exit_signal)
                except Exception as e:
                    logger.error(f"Trailing check error for {symbol}: {e}")

        return entry_requests, trailing_exits

    async def _process_symbol(self, signal: GapSignal) -> Optional[DTEntryRequest]:
        """Process one symbol through the double-touch state machine."""
        symbol = signal.symbol

        # Get or create state
        st = self._states.get(symbol)
        today = today_str()

        if st is None or st.date_key != today:
            st = await self._init_symbol_state(signal, today)
            if st is None:
                return None
            self._states[symbol] = st

        # Skip if already in position or trailing
        if st.phase in (DTSymbolPhase.IN_POSITION, DTSymbolPhase.TRAILING):
            return None

        # Fetch 15-sec bars
        bars = await self._conn.get_historical_data(
            st.contract,
            duration=FIB_DT_LIVE_BAR_DURATION,
            bar_size=FIB_DT_LIVE_BAR_SIZE,
            what_to_show="TRADES",
            use_rth=False,
        )
        df = bars_to_dataframe(bars)
        if len(df) < 10:
            return None

        # Bar-range filter — reject wide-spread / illiquid stocks early
        if not _check_bar_range_filter(df):
            logger.debug(f"[{symbol}] rejected: avg bar range > {FIB_DT_LIVE_MAX_BAR_RANGE_PCT:.0%}")
            return None

        # ATR for adaptive stop
        atr = _compute_atr_from_15s(df)

        # Only process NEW bars since last cycle
        new_bar_count = len(df)
        start_idx = max(0, st.last_bar_count)
        if new_bar_count <= st.last_bar_count:
            return None
        st.last_bar_count = new_bar_count

        # Update gap high
        day_high = float(df["high"].max())
        if day_high > st.gap_high:
            st.gap_high = day_high
            if st.dual_fib:
                st.fib_prices = _get_all_fib_prices(st.dual_fib, st.gap_high)
                st.fib_price_info = _get_fib_price_info(st.dual_fib, st.gap_high)
                range_low = st.prev_close * 0.3
                range_high = st.gap_high * 2.5
                st.fib_prices = [p for p in st.fib_prices if range_low <= p <= range_high]
                # Rebuild touch states for new fib prices
                st.touch_states = {}
                for idx, fp in enumerate(st.fib_prices):
                    st.touch_states[idx] = DTTouchState(fib_price=fp, fib_idx=idx)

        if len(st.fib_prices) < 3:
            return None

        # Walk new bars through double-touch state machine
        for i in range(start_idx, new_bar_count):
            bar = df.iloc[i]
            bar_low = float(bar["low"])
            bar_high = float(bar["high"])
            bar_close = float(bar["close"])

            for idx, ts in st.touch_states.items():
                fp = ts.fib_price

                # Only track support levels (below current price)
                if fp > bar_close:
                    continue

                proximity = fp * FIB_DT_LIVE_PROXIMITY_PCT if fp > 0 else 0.01

                if ts.phase == "IDLE":
                    if abs(bar_low - fp) <= proximity:
                        ts.phase = "FIRST_TOUCH"
                        ts.first_touch_bar = i
                        logger.debug(f"[{symbol}] FIRST_TOUCH fib ${fp:.4f} at bar {i}")

                elif ts.phase == "FIRST_TOUCH":
                    bounce_threshold = fp * 1.005
                    if bar_low > bounce_threshold:
                        ts.phase = "BOUNCED"
                        logger.debug(f"[{symbol}] BOUNCED from fib ${fp:.4f} at bar {i}")

                elif ts.phase == "BOUNCED":
                    bars_since_first = i - ts.first_touch_bar
                    if bars_since_first >= FIB_DT_LIVE_MIN_BOUNCE_BARS:
                        if abs(bar_low - fp) <= proximity:
                            # Fib ratio filter
                            fib_key = round(fp, 4)
                            ratio, _series = st.fib_price_info.get(fib_key, (None, "?"))
                            if ratio is not None and ratio not in FIB_DT_LIVE_PREFERRED_RATIOS:
                                logger.debug(
                                    f"[{symbol}] double-touch at fib ${fp:.4f} "
                                    f"ratio={ratio} NOT in preferred set, skipping"
                                )
                                ts.phase = "IDLE"
                                ts.first_touch_bar = -1
                                continue

                            # DOUBLE TOUCH — generate entry request
                            if atr > 0:
                                atr_stop = fp - atr * FIB_DT_LIVE_ATR_STOP_MULT
                                stop_floor = fp * (1 - FIB_DT_LIVE_ATR_STOP_MAX_PCT)
                                stop_ceil = fp * (1 - FIB_DT_LIVE_ATR_STOP_MIN_PCT)
                                stop_price = round(max(stop_floor, min(stop_ceil, atr_stop)), 4)
                                stop_tag = f"ATR({atr:.4f})"
                            else:
                                stop_price = round(fp * (1 - FIB_DT_LIVE_STOP_PCT), 4)
                                stop_tag = "fixed"
                            target_price = self._find_target(st.fib_prices, idx, bar_close)

                            logger.info(
                                f"[{symbol}] DOUBLE-TOUCH: fib=${fp:.4f} ratio={ratio} "
                                f"(touch1=bar{ts.first_touch_bar}, touch2=bar{i}, "
                                f"gap={bars_since_first} bars) -> "
                                f"stop=${stop_price:.4f} [{stop_tag}], target=${target_price:.4f}"
                            )

                            # Reset this level
                            ts.phase = "IDLE"
                            ts.first_touch_bar = -1

                            return DTEntryRequest(
                                symbol=symbol,
                                contract=st.contract,
                                fib_level=fp,
                                fib_idx=idx,
                                fib_ratio=ratio if ratio is not None else 0.0,
                                entry_price=bar_close,
                                stop_price=stop_price,
                                target_price=target_price,
                                gap_pct=signal.gap_pct,
                            )

        return None

    async def _check_trailing(self, st: DTSymbolState) -> Optional[DTTrailingExit]:
        """Check no-new-high trailing exit for symbols in TRAILING phase."""
        # Fetch latest 15-sec bars
        bars = await self._conn.get_historical_data(
            st.contract,
            duration=FIB_DT_LIVE_BAR_DURATION,
            bar_size=FIB_DT_LIVE_BAR_SIZE,
            what_to_show="TRADES",
            use_rth=False,
        )
        df = bars_to_dataframe(bars)
        if len(df) < 2:
            return None

        bar_high = float(df["high"].iloc[-1])

        if st.prev_bar_high > 0 and bar_high <= st.prev_bar_high:
            st.trailing_no_high_count += 1
            if st.trailing_no_high_count >= FIB_DT_LIVE_TRAILING_BARS:
                reason = (
                    f"no_new_high x{st.trailing_no_high_count} "
                    f"(prev_high=${st.prev_bar_high:.4f}, bar_high=${bar_high:.4f})"
                )
                logger.info(f"[{st.symbol}] TRAILING EXIT: {reason}")
                return DTTrailingExit(
                    symbol=st.symbol,
                    contract=st.contract,
                    reason=reason,
                )
            logger.debug(f"[{st.symbol}] trailing: no new high {st.trailing_no_high_count}/{FIB_DT_LIVE_TRAILING_BARS}")
        else:
            st.trailing_no_high_count = 0

        # Update prev_bar_high for next cycle
        st.prev_bar_high = bar_high
        return None

    def _find_target(self, fib_prices: list[float], fib_idx: int, entry_price: float) -> float:
        """Find the Nth fib level above entry price, enforcing minimum target distance."""
        min_target = round(entry_price * (1 + FIB_DT_LIVE_MIN_TARGET_PCT), 4)
        levels_above = 0
        for fp in fib_prices:
            if fp > entry_price:
                levels_above += 1
                if levels_above >= FIB_DT_LIVE_TARGET_LEVELS:
                    return max(fp, min_target)
        # Fallback: 5% above entry (or min target, whichever is higher)
        return max(round(entry_price * 1.05, 4), min_target)

    async def _init_symbol_state(
        self, signal: GapSignal, today: str
    ) -> Optional[DTSymbolState]:
        """Initialize state for a new symbol on a new day."""
        symbol = signal.symbol
        contract = signal.contract

        # Get 5-year daily data for fibonacci anchor
        daily_bars = await self._conn.get_historical_data(
            contract,
            duration=f"{FIB_LOOKBACK_YEARS} Y",
            bar_size="1 day",
            what_to_show="TRADES",
            use_rth=True,
        )
        daily_df = bars_to_dataframe(daily_bars)
        if len(daily_df) < 20:
            logger.warning(f"[{symbol}] Insufficient daily data ({len(daily_df)} bars)")
            return None

        anchor = find_anchor_candle(daily_df)
        if anchor is None:
            logger.warning(f"[{symbol}] No anchor candle found")
            return None

        anchor_low, anchor_high, _anchor_date = anchor
        dual = build_dual_series(anchor_low, anchor_high)

        # Compute fib prices
        gap_high = signal.current_price
        fib_prices = _get_all_fib_prices(dual, gap_high)
        fib_price_info = _get_fib_price_info(dual, gap_high)
        range_low = signal.prev_close * 0.3
        range_high = gap_high * 2.5
        fib_prices = [p for p in fib_prices if range_low <= p <= range_high]

        # Init touch states
        touch_states: dict[int, DTTouchState] = {}
        for idx, fp in enumerate(fib_prices):
            touch_states[idx] = DTTouchState(fib_price=fp, fib_idx=idx)

        logger.info(
            f"[{symbol}] Initialized: anchor=${anchor_low:.4f}-${anchor_high:.4f}, "
            f"{len(fib_prices)} fib levels, gap +{signal.gap_pct:.1f}%"
        )

        return DTSymbolState(
            symbol=symbol,
            contract=contract,
            prev_close=signal.prev_close,
            gap_high=gap_high,
            fib_prices=fib_prices,
            fib_price_info=fib_price_info,
            dual_fib=dual,
            touch_states=touch_states,
            date_key=today,
        )

    def _reset_daily_if_needed(self) -> None:
        """Reset daily entry count on new day."""
        today = today_str()
        if self._entries_date != today:
            self._entries_date = today
            self._entries_today = 0
            self._states.clear()

    def get_status(self) -> dict:
        """Status snapshot for logging/dashboard."""
        return {
            "tracked_symbols": len(self._states),
            "entries_today": self._entries_today,
            "symbol_phases": {
                s: st.phase.value for s, st in self._states.items()
            },
        }


# ── Synchronous Strategy (for monitor integration) ──────

class FibDTLiveStrategySync:
    """Synchronous version of FibDTLiveStrategy for use with the IBKR
    scanner monitor (which runs in a non-async thread).

    Accepts a raw ``ib_insync.IB`` instance and calls
    ``ib.reqHistoricalData(...)`` directly (blocking).
    """

    def __init__(self, ib_getter) -> None:
        """
        Parameters
        ----------
        ib_getter : callable
            A zero-arg function that returns an ``IB`` instance (or None).
            E.g. the monitor's ``_get_ibkr``.
        """
        self._ib_getter = ib_getter
        self._states: dict[str, DTSymbolState] = {}
        self._entries_today = 0
        self._entries_date: str = ""
        h_start, m_start = FIB_DT_LIVE_ENTRY_START.split(":")
        h_end, m_end = FIB_DT_LIVE_ENTRY_END.split(":")
        self._entry_start = time(int(h_start), int(m_start))
        self._entry_cutoff = time(int(h_end), int(m_end))

    # ── public helpers (same as async version) ──────────

    @property
    def entries_today(self) -> int:
        self._reset_daily_if_needed()
        return self._entries_today

    def record_entry(self) -> None:
        self._reset_daily_if_needed()
        self._entries_today += 1

    def mark_in_position(self, symbol: str) -> None:
        st = self._states.get(symbol)
        if st:
            st.phase = DTSymbolPhase.IN_POSITION
            logger.info(f"[{symbol}] -> IN_POSITION (split exit orders active)")

    def mark_trailing(self, symbol: str) -> None:
        st = self._states.get(symbol)
        if st:
            st.phase = DTSymbolPhase.TRAILING
            st.prev_bar_high = 0.0
            st.trailing_no_high_count = 0
            logger.info(f"[{symbol}] -> TRAILING (target filled, monitoring no-new-high)")

    def mark_position_closed(self, symbol: str) -> None:
        st = self._states.get(symbol)
        if st:
            st.phase = DTSymbolPhase.SCANNING
            for ts in st.touch_states.values():
                ts.phase = "IDLE"
                ts.first_touch_bar = -1
            logger.info(f"[{symbol}] -> SCANNING (position closed)")

    def sync_from_portfolio(self, held_positions: dict):
        """Sync strategy state from portfolio after restart.

        Args:
            held_positions: {sym: {'entry_price': float, 'phase': str, ...}}
        """
        for sym, pos in held_positions.items():
            st = self._states.get(sym)
            if not st:
                continue
            phase_str = pos.get('phase', 'IN_POSITION')
            if phase_str == 'TRAILING':
                st.phase = DTSymbolPhase.TRAILING
                st.prev_bar_high = 0.0
                st.trailing_no_high_count = 0
            else:
                st.phase = DTSymbolPhase.IN_POSITION
            entry_price = pos.get('entry_price', 0)
            logger.info(f"FIB DT strategy sync: {sym} marked {st.phase.name} @ ${entry_price:.4f}")

    # ── main cycle (synchronous) ────────────────────────

    def process_cycle(
        self, gap_signals: list[GapSignal]
    ) -> tuple[list[DTEntryRequest], list[DTTrailingExit]]:
        """Run one strategy cycle across all tracked symbols (sync).

        Returns (entry_requests, trailing_exits).
        """
        self._reset_daily_if_needed()
        entry_requests: list[DTEntryRequest] = []
        trailing_exits: list[DTTrailingExit] = []

        now = now_et()
        in_entry_window = self._entry_start <= now.time() < self._entry_cutoff

        if in_entry_window and self._entries_today < FIB_DT_LIVE_MAX_ENTRIES_PER_DAY:
            for signal in gap_signals:
                try:
                    req = self._process_symbol(signal)
                    if req:
                        entry_requests.append(req)
                        if self._entries_today + len(entry_requests) >= FIB_DT_LIVE_MAX_ENTRIES_PER_DAY:
                            break
                except Exception as e:
                    logger.error(f"Strategy error for {signal.symbol}: {e}")

        for symbol, st in list(self._states.items()):
            if st.phase == DTSymbolPhase.TRAILING:
                try:
                    exit_signal = self._check_trailing(st)
                    if exit_signal:
                        trailing_exits.append(exit_signal)
                except Exception as e:
                    logger.error(f"Trailing check error for {symbol}: {e}")

        return entry_requests, trailing_exits

    def _process_symbol(self, signal: GapSignal) -> Optional[DTEntryRequest]:
        """Process one symbol through the double-touch state machine (sync)."""
        symbol = signal.symbol
        ib = self._ib_getter()
        if not ib:
            return None

        st = self._states.get(symbol)
        today = today_str()

        if st is None or st.date_key != today:
            st = self._init_symbol_state(signal, today)
            if st is None:
                return None
            self._states[symbol] = st

        if st.phase in (DTSymbolPhase.IN_POSITION, DTSymbolPhase.TRAILING):
            return None

        # Fetch 15-sec bars (sync)
        try:
            bars = ib.reqHistoricalData(
                st.contract,
                endDateTime="",
                durationStr=FIB_DT_LIVE_BAR_DURATION,
                barSizeSetting=FIB_DT_LIVE_BAR_SIZE,
                whatToShow="TRADES",
                useRTH=False,
            )
        except Exception as e:
            logger.warning(f"[{symbol}] reqHistoricalData failed: {e}")
            return None

        df = bars_to_dataframe(bars)
        if len(df) < 10:
            return None

        # Bar-range filter — reject wide-spread / illiquid stocks early
        if not _check_bar_range_filter(df):
            logger.debug(f"[{symbol}] rejected: avg bar range > {FIB_DT_LIVE_MAX_BAR_RANGE_PCT:.0%}")
            return None

        # ATR for adaptive stop
        atr = _compute_atr_from_15s(df)

        new_bar_count = len(df)
        start_idx = max(0, st.last_bar_count)
        if new_bar_count <= st.last_bar_count:
            return None
        st.last_bar_count = new_bar_count

        # Update gap high
        day_high = float(df["high"].max())
        if day_high > st.gap_high:
            st.gap_high = day_high
            if st.dual_fib:
                st.fib_prices = _get_all_fib_prices(st.dual_fib, st.gap_high)
                st.fib_price_info = _get_fib_price_info(st.dual_fib, st.gap_high)
                range_low = st.prev_close * 0.3
                range_high = st.gap_high * 2.5
                st.fib_prices = [p for p in st.fib_prices if range_low <= p <= range_high]
                st.touch_states = {}
                for idx, fp in enumerate(st.fib_prices):
                    st.touch_states[idx] = DTTouchState(fib_price=fp, fib_idx=idx)

        if len(st.fib_prices) < 3:
            return None

        # Walk new bars through double-touch state machine
        for i in range(start_idx, new_bar_count):
            bar = df.iloc[i]
            bar_low = float(bar["low"])
            bar_high = float(bar["high"])
            bar_close = float(bar["close"])

            for idx, ts in st.touch_states.items():
                fp = ts.fib_price
                if fp > bar_close:
                    continue

                proximity = fp * FIB_DT_LIVE_PROXIMITY_PCT if fp > 0 else 0.01

                if ts.phase == "IDLE":
                    if abs(bar_low - fp) <= proximity:
                        ts.phase = "FIRST_TOUCH"
                        ts.first_touch_bar = i
                        logger.debug(f"[{symbol}] FIRST_TOUCH fib ${fp:.4f} at bar {i}")

                elif ts.phase == "FIRST_TOUCH":
                    bounce_threshold = fp * 1.005
                    if bar_low > bounce_threshold:
                        ts.phase = "BOUNCED"
                        logger.debug(f"[{symbol}] BOUNCED from fib ${fp:.4f} at bar {i}")

                elif ts.phase == "BOUNCED":
                    bars_since_first = i - ts.first_touch_bar
                    if bars_since_first >= FIB_DT_LIVE_MIN_BOUNCE_BARS:
                        if abs(bar_low - fp) <= proximity:
                            fib_key = round(fp, 4)
                            ratio, _series = st.fib_price_info.get(fib_key, (None, "?"))
                            if ratio is not None and ratio not in FIB_DT_LIVE_PREFERRED_RATIOS:
                                logger.debug(
                                    f"[{symbol}] double-touch at fib ${fp:.4f} "
                                    f"ratio={ratio} NOT in preferred set, skipping"
                                )
                                ts.phase = "IDLE"
                                ts.first_touch_bar = -1
                                continue

                            if atr > 0:
                                atr_stop = fp - atr * FIB_DT_LIVE_ATR_STOP_MULT
                                stop_floor = fp * (1 - FIB_DT_LIVE_ATR_STOP_MAX_PCT)
                                stop_ceil = fp * (1 - FIB_DT_LIVE_ATR_STOP_MIN_PCT)
                                stop_price = round(max(stop_floor, min(stop_ceil, atr_stop)), 4)
                                stop_tag = f"ATR({atr:.4f})"
                            else:
                                stop_price = round(fp * (1 - FIB_DT_LIVE_STOP_PCT), 4)
                                stop_tag = "fixed"
                            target_price = self._find_target(st.fib_prices, idx, bar_close)

                            logger.info(
                                f"[{symbol}] DOUBLE-TOUCH: fib=${fp:.4f} ratio={ratio} "
                                f"(touch1=bar{ts.first_touch_bar}, touch2=bar{i}, "
                                f"gap={bars_since_first} bars) -> "
                                f"stop=${stop_price:.4f} [{stop_tag}], target=${target_price:.4f}"
                            )

                            ts.phase = "IDLE"
                            ts.first_touch_bar = -1

                            return DTEntryRequest(
                                symbol=symbol,
                                contract=st.contract,
                                fib_level=fp,
                                fib_idx=idx,
                                fib_ratio=ratio if ratio is not None else 0.0,
                                entry_price=bar_close,
                                stop_price=stop_price,
                                target_price=target_price,
                                gap_pct=signal.gap_pct,
                            )

        return None

    def _check_trailing(self, st: DTSymbolState) -> Optional[DTTrailingExit]:
        """Check no-new-high trailing exit (sync)."""
        ib = self._ib_getter()
        if not ib:
            return None

        try:
            bars = ib.reqHistoricalData(
                st.contract,
                endDateTime="",
                durationStr=FIB_DT_LIVE_BAR_DURATION,
                barSizeSetting=FIB_DT_LIVE_BAR_SIZE,
                whatToShow="TRADES",
                useRTH=False,
            )
        except Exception as e:
            logger.warning(f"[{st.symbol}] trailing reqHistoricalData failed: {e}")
            return None

        df = bars_to_dataframe(bars)
        if len(df) < 2:
            return None

        bar_high = float(df["high"].iloc[-1])

        if st.prev_bar_high > 0 and bar_high <= st.prev_bar_high:
            st.trailing_no_high_count += 1
            if st.trailing_no_high_count >= FIB_DT_LIVE_TRAILING_BARS:
                reason = (
                    f"no_new_high x{st.trailing_no_high_count} "
                    f"(prev_high=${st.prev_bar_high:.4f}, bar_high=${bar_high:.4f})"
                )
                logger.info(f"[{st.symbol}] TRAILING EXIT: {reason}")
                return DTTrailingExit(
                    symbol=st.symbol,
                    contract=st.contract,
                    reason=reason,
                )
            logger.debug(f"[{st.symbol}] trailing: no new high {st.trailing_no_high_count}/{FIB_DT_LIVE_TRAILING_BARS}")
        else:
            st.trailing_no_high_count = 0

        st.prev_bar_high = bar_high
        return None

    def _find_target(self, fib_prices: list[float], fib_idx: int, entry_price: float) -> float:
        """Find the Nth fib level above entry price, enforcing minimum target distance."""
        min_target = round(entry_price * (1 + FIB_DT_LIVE_MIN_TARGET_PCT), 4)
        levels_above = 0
        for fp in fib_prices:
            if fp > entry_price:
                levels_above += 1
                if levels_above >= FIB_DT_LIVE_TARGET_LEVELS:
                    return max(fp, min_target)
        # Fallback: 5% above entry (or min target, whichever is higher)
        return max(round(entry_price * 1.05, 4), min_target)

    def _init_symbol_state(
        self, signal: GapSignal, today: str
    ) -> Optional[DTSymbolState]:
        """Initialize state for a new symbol on a new day (sync)."""
        symbol = signal.symbol
        contract = signal.contract
        ib = self._ib_getter()
        if not ib:
            return None

        try:
            daily_bars = ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=f"{FIB_LOOKBACK_YEARS} Y",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
            )
        except Exception as e:
            logger.warning(f"[{symbol}] daily reqHistoricalData failed: {e}")
            return None

        daily_df = bars_to_dataframe(daily_bars)
        if len(daily_df) < 20:
            logger.warning(f"[{symbol}] Insufficient daily data ({len(daily_df)} bars)")
            return None

        anchor = find_anchor_candle(daily_df)
        if anchor is None:
            logger.warning(f"[{symbol}] No anchor candle found")
            return None

        anchor_low, anchor_high, _anchor_date = anchor
        dual = build_dual_series(anchor_low, anchor_high)

        gap_high = signal.current_price
        fib_prices = _get_all_fib_prices(dual, gap_high)
        fib_price_info = _get_fib_price_info(dual, gap_high)
        range_low = signal.prev_close * 0.3
        range_high = gap_high * 2.5
        fib_prices = [p for p in fib_prices if range_low <= p <= range_high]

        touch_states: dict[int, DTTouchState] = {}
        for idx, fp in enumerate(fib_prices):
            touch_states[idx] = DTTouchState(fib_price=fp, fib_idx=idx)

        logger.info(
            f"[{symbol}] Initialized: anchor=${anchor_low:.4f}-${anchor_high:.4f}, "
            f"{len(fib_prices)} fib levels, gap +{signal.gap_pct:.1f}%"
        )

        return DTSymbolState(
            symbol=symbol,
            contract=contract,
            prev_close=signal.prev_close,
            gap_high=gap_high,
            fib_prices=fib_prices,
            fib_price_info=fib_price_info,
            dual_fib=dual,
            touch_states=touch_states,
            date_key=today,
        )

    def _reset_daily_if_needed(self) -> None:
        today = today_str()
        if self._entries_date != today:
            self._entries_date = today
            self._entries_today = 0
            self._states.clear()

    def get_status(self) -> dict:
        return {
            "tracked_symbols": len(self._states),
            "entries_today": self._entries_today,
            "symbol_phases": {
                s: st.phase.value for s, st in self._states.items()
            },
        }
