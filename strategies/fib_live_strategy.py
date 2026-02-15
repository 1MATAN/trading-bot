"""Live Fibonacci support state machine — mirrors simulation/fib_backtest.py.

Per-symbol state machines that persist across 30-second scan cycles:
  SCANNING → TESTING → entry signal (EntryRequest)

Uses 2-min IBKR bars and dual-series recursive Fibonacci levels.
"""

import logging
from dataclasses import dataclass, field
from datetime import time
from enum import Enum
from typing import Optional

import pandas as pd
from ib_insync import Contract

from broker.ibkr_connection import get_connection
from config.settings import (
    FIB_LIVE_BAR_SIZE,
    FIB_LIVE_BAR_DURATION,
    FIB_LIVE_STOP_PCT,
    FIB_LIVE_TARGET_LEVELS,
    FIB_LIVE_MAX_ENTRIES_PER_DAY,
    FIB_LIVE_ENTRY_WINDOW_END,
    FIB_LIVE_WARMUP_BARS,
    FIB_LOOKBACK_YEARS,
    SCAN_PRICE_MIN,
    SCAN_PRICE_MAX,
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

logger = logging.getLogger("trading_bot.fib_live_strategy")


class SymbolPhase(Enum):
    SCANNING = "SCANNING"
    TESTING = "TESTING"
    IN_POSITION = "IN_POSITION"


@dataclass
class EntryRequest:
    """Signal from strategy to entry executor."""
    symbol: str
    contract: Contract
    fib_level: float
    fib_idx: int
    entry_price: float  # latest bar open (estimated fill)
    stop_price: float
    target_price: float
    gap_pct: float


@dataclass
class SymbolState:
    """Per-symbol state persisting across scan cycles."""
    symbol: str
    contract: Contract
    phase: SymbolPhase = SymbolPhase.SCANNING
    prev_close: float = 0.0
    gap_high: float = 0.0
    fib_prices: list[float] = field(default_factory=list)
    dual_fib: Optional[DualFibSeries] = None
    # TESTING state
    test_fib_idx: int = -1
    test_bar_low: float = 0.0
    # Tracking
    last_bar_count: int = 0
    date_key: str = ""


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


class FibLiveStrategy:
    """Per-symbol state machines for fib support entries on gap stocks."""

    def __init__(self) -> None:
        self._conn = get_connection()
        self._states: dict[str, SymbolState] = {}
        self._entries_today = 0
        self._entries_date: str = ""
        h, m = FIB_LIVE_ENTRY_WINDOW_END.split(":")
        self._entry_cutoff = time(int(h), int(m))

    @property
    def entries_today(self) -> int:
        self._reset_daily_if_needed()
        return self._entries_today

    def record_entry(self) -> None:
        """Called by entry executor after a successful fill."""
        self._reset_daily_if_needed()
        self._entries_today += 1

    def mark_in_position(self, symbol: str) -> None:
        """Transition symbol to IN_POSITION (OCA bracket handles exit)."""
        st = self._states.get(symbol)
        if st:
            st.phase = SymbolPhase.IN_POSITION
            logger.info(f"[{symbol}] → IN_POSITION (OCA bracket active)")

    def mark_position_closed(self, symbol: str) -> None:
        """Reset symbol back to SCANNING after position closes."""
        st = self._states.get(symbol)
        if st:
            st.phase = SymbolPhase.SCANNING
            st.test_fib_idx = -1
            st.test_bar_low = 0.0
            logger.info(f"[{symbol}] → SCANNING (position closed)")

    async def process_cycle(self, gap_signals: list[GapSignal]) -> list[EntryRequest]:
        """Run one strategy cycle across all tracked symbols.

        Returns EntryRequest list for the entry executor to act on.
        """
        self._reset_daily_if_needed()
        requests: list[EntryRequest] = []

        # Check entry window
        now = now_et()
        if now.time() >= self._entry_cutoff:
            return []

        # Check daily entry limit
        if self._entries_today >= FIB_LIVE_MAX_ENTRIES_PER_DAY:
            return []

        for signal in gap_signals:
            try:
                req = await self._process_symbol(signal)
                if req:
                    requests.append(req)
                    # Stop if we've hit the limit
                    if self._entries_today + len(requests) >= FIB_LIVE_MAX_ENTRIES_PER_DAY:
                        break
            except Exception as e:
                logger.error(f"Strategy error for {signal.symbol}: {e}")

        return requests

    async def _process_symbol(self, signal: GapSignal) -> Optional[EntryRequest]:
        """Process one symbol through the state machine."""
        symbol = signal.symbol

        # Get or create state
        st = self._states.get(symbol)
        today = today_str()

        if st is None or st.date_key != today:
            st = await self._init_symbol_state(signal, today)
            if st is None:
                return None
            self._states[symbol] = st

        # Skip if already in position
        if st.phase == SymbolPhase.IN_POSITION:
            return None

        # Fetch 2-min bars
        bars = await self._conn.get_historical_data(
            st.contract,
            duration=FIB_LIVE_BAR_DURATION,
            bar_size=FIB_LIVE_BAR_SIZE,
            what_to_show="TRADES",
            use_rth=False,
        )
        df = bars_to_dataframe(bars)
        if len(df) < FIB_LIVE_WARMUP_BARS + 2:
            return None

        # Only process NEW bars since last cycle
        new_bar_count = len(df)
        start_idx = max(FIB_LIVE_WARMUP_BARS, st.last_bar_count)
        if new_bar_count <= st.last_bar_count:
            # No new bars yet — still run state machine on last bar for TESTING
            if st.phase == SymbolPhase.TESTING and new_bar_count > 0:
                start_idx = new_bar_count - 1
            else:
                return None
        st.last_bar_count = new_bar_count

        # Update gap high if needed (from warmup period)
        warmup_high = float(df["high"].iloc[:FIB_LIVE_WARMUP_BARS].max())
        if warmup_high > st.gap_high:
            st.gap_high = warmup_high
            # Recompute fib prices with new gap high
            if st.dual_fib:
                st.fib_prices = _get_all_fib_prices(st.dual_fib, st.gap_high)
                range_low = st.prev_close * 0.3
                range_high = st.gap_high * 2.5
                st.fib_prices = [p for p in st.fib_prices if range_low <= p <= range_high]

        if len(st.fib_prices) < 3:
            return None

        # Walk new bars through state machine
        for i in range(start_idx, new_bar_count):
            bar = df.iloc[i]
            bar_low = float(bar["low"])
            bar_high = float(bar["high"])
            bar_close = float(bar["close"])
            bar_open = float(bar["open"])

            if st.phase == SymbolPhase.SCANNING:
                # Look for bar that touches a fib from above
                # Condition: bar_low <= fib_level <= bar_close
                for fi, fp in enumerate(st.fib_prices):
                    if bar_low <= fp <= bar_close:
                        st.phase = SymbolPhase.TESTING
                        st.test_fib_idx = fi
                        st.test_bar_low = bar_low
                        logger.info(
                            f"[{symbol}] SCANNING → TESTING: "
                            f"bar touched fib ${fp:.4f} (idx={fi})"
                        )
                        break

            elif st.phase == SymbolPhase.TESTING:
                # Confirmation: bar must not make a new low
                if bar_low >= st.test_bar_low:
                    # CONFIRMED — generate entry request
                    fib_level = st.fib_prices[st.test_fib_idx]
                    stop_price = round(fib_level * (1 - FIB_LIVE_STOP_PCT), 4)

                    target_idx = st.test_fib_idx + FIB_LIVE_TARGET_LEVELS
                    if target_idx < len(st.fib_prices):
                        target_price = st.fib_prices[target_idx]
                    else:
                        target_price = st.fib_prices[-1]

                    logger.info(
                        f"[{symbol}] TESTING → CONFIRMED: "
                        f"fib=${fib_level:.4f}, stop=${stop_price:.4f}, "
                        f"target=${target_price:.4f}"
                    )

                    return EntryRequest(
                        symbol=symbol,
                        contract=st.contract,
                        fib_level=fib_level,
                        fib_idx=st.test_fib_idx,
                        entry_price=bar_open,
                        stop_price=stop_price,
                        target_price=target_price,
                        gap_pct=signal.gap_pct,
                    )
                else:
                    # Failed — new low made, back to scanning
                    logger.info(
                        f"[{symbol}] TESTING → SCANNING (new low "
                        f"${bar_low:.4f} < ${st.test_bar_low:.4f})"
                    )
                    st.phase = SymbolPhase.SCANNING
                    st.test_fib_idx = -1
                    st.test_bar_low = 0.0

        return None

    async def _init_symbol_state(
        self, signal: GapSignal, today: str
    ) -> Optional[SymbolState]:
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
        gap_high = signal.current_price  # initial estimate; updated from 2-min warmup
        fib_prices = _get_all_fib_prices(dual, gap_high)
        range_low = signal.prev_close * 0.3
        range_high = gap_high * 2.5
        fib_prices = [p for p in fib_prices if range_low <= p <= range_high]

        logger.info(
            f"[{symbol}] Initialized: anchor=${anchor_low:.4f}-${anchor_high:.4f}, "
            f"{len(fib_prices)} fib levels, gap +{signal.gap_pct:.1f}%"
        )

        return SymbolState(
            symbol=symbol,
            contract=contract,
            prev_close=signal.prev_close,
            gap_high=gap_high,
            fib_prices=fib_prices,
            dual_fib=dual,
            date_key=today,
        )

    def _reset_daily_if_needed(self) -> None:
        """Reset daily entry count on new day."""
        today = today_str()
        if self._entries_date != today:
            self._entries_date = today
            self._entries_today = 0
            # Clear old symbol states
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
