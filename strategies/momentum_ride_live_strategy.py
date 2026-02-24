"""Momentum Ride Live Strategy — VWAP cross / pullback + SMA 9 hourly.

Entry (VWAP cross):   price crosses above VWAP + above SMA 9 on 1-hour candles
Entry (Pullback):     low within 2% of VWAP + close above VWAP + above SMA 9 hourly
Exit:                 5% trailing stop from highest high since entry
Safety stop:          -5% from entry price
Tracking window:      90 minutes from first appearance in scan — force close at end
Gap filter:           20%-50%
Re-entries:           unlimited
"""

import logging
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from config.settings import (
    MR_GAP_MIN_PCT,
    MR_GAP_MAX_PCT,
    MR_TRACKING_MINUTES,
    MR_LIVE_MAX_TRACKED_SYMBOLS,
    MR_LIVE_TRAILING_STOP_PCT,
    MR_LIVE_SAFETY_STOP_PCT,
    MR_LIVE_PULLBACK_TOUCH_PCT,
)

logger = logging.getLogger("trading_bot.momentum_ride_live")
_ET = ZoneInfo("US/Eastern")

_EOD_CLOSE_TIME = time(19, 55)  # force close


# ── Data classes ───────────────────────────────────────────

@dataclass
class MRCandidate:
    symbol: str
    contract: object          # ib_insync Contract
    gap_pct: float
    prev_close: float
    current_price: float


@dataclass
class MREntrySignal:
    symbol: str
    price: float
    vwap: float
    sma9_hourly: float
    signal_type: str          # "vwap_cross" | "pullback_vwap"


@dataclass
class MRExitSignal:
    symbol: str
    price: float
    reason: str


@dataclass
class _MRSymbolState:
    in_position: bool = False
    prev_below_vwap: bool = True    # for VWAP cross detection
    prev_above_vwap: bool = False   # for pullback detection
    highest_high: float = 0.0       # trailing stop tracker
    entry_price: float = 0.0        # safety stop reference
    first_scan_time: Optional[datetime] = None  # 90-min tracking
    last_bar_count: int = 0         # detect new bars
    date_key: str = ""              # daily reset


# ── Helper functions ───────────────────────────────────────

def _compute_running_vwap(bars_1m: list[dict]) -> list[float]:
    """Cumulative VWAP from 1-min bars: sum(TP*Vol) / sum(Vol)."""
    if not bars_1m:
        return []
    tp = np.array([(b["high"] + b["low"] + b["close"]) / 3.0 for b in bars_1m])
    vol = np.array([float(b["volume"]) for b in bars_1m])
    cum_tp_vol = np.cumsum(tp * vol)
    cum_vol = np.cumsum(vol)
    with np.errstate(invalid='ignore', divide='ignore'):
        vwap = np.where(cum_vol > 0, cum_tp_vol / cum_vol, 0.0)
    return vwap.tolist()


def _compute_sma9_hourly(bars_1m: list[dict]) -> float:
    """Resample 1-min bars to 1-hour, compute SMA 9, return latest value."""
    if not bars_1m:
        return 0.0

    # Build DataFrame from 1-min bars
    df = pd.DataFrame(bars_1m)
    if "time" not in df.columns or df.empty:
        return 0.0

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.set_index("time")

    # Resample to 1-hour
    hourly = df["close"].resample("1h").last().dropna()
    if hourly.empty:
        return 0.0

    sma9 = hourly.rolling(9, min_periods=1).mean()
    return float(sma9.iloc[-1])


# ── Strategy class ─────────────────────────────────────────

class MomentumRideLiveStrategy:
    """Momentum Ride live strategy — processes one cycle at a time.

    Each cycle:
      1. Request 1-min bars from IBKR for each tracked symbol
      2. Compute running VWAP + SMA 9 hourly
      3. Check entry (VWAP cross / pullback) and exit (trailing/safety/timeout)
    """

    def __init__(self, ib_getter):
        self._ib_getter = ib_getter
        self._states: dict[str, _MRSymbolState] = {}
        self._last_raw_bars: dict[str, list] = {}  # sym -> raw ib bars from last cycle

    def _get_state(self, symbol: str) -> _MRSymbolState:
        today = datetime.now(_ET).strftime("%Y-%m-%d")
        state = self._states.get(symbol)
        if state is None or state.date_key != today:
            state = _MRSymbolState(date_key=today)
            self._states[symbol] = state
        return state

    def mark_in_position(self, symbol: str, entry_price: float, current_price: float):
        state = self._get_state(symbol)
        state.in_position = True
        state.entry_price = entry_price
        state.highest_high = current_price

    def mark_position_closed(self, symbol: str):
        state = self._get_state(symbol)
        state.in_position = False
        state.entry_price = 0.0
        state.highest_high = 0.0

    def sync_from_portfolio(self, held_positions: dict):
        """Sync strategy state from portfolio after restart.

        Args:
            held_positions: {sym: {'entry_price': float, ...}}
        """
        for sym, pos in held_positions.items():
            entry_price = pos.get('entry_price', 0)
            self.mark_in_position(sym, entry_price, entry_price)
            logger.info(f"MR strategy sync: {sym} marked in_position @ ${entry_price:.4f}")

    def process_cycle(
        self, candidates: list[MRCandidate]
    ) -> tuple[list[MREntrySignal], list[MRExitSignal]]:
        """Run one strategy cycle over all candidates.

        Returns (entries, exits).
        """
        entries: list[MREntrySignal] = []
        exits: list[MRExitSignal] = []
        self._last_raw_bars.clear()

        ib = self._ib_getter()
        if ib is None or not ib.isConnected():
            logger.warning("MR: IBKR not connected, skipping cycle")
            return entries, exits

        now_et = datetime.now(_ET)

        # Limit tracked symbols
        tracked = candidates[:MR_LIVE_MAX_TRACKED_SYMBOLS]

        for cand in tracked:
            try:
                entry, exit_sig = self._process_symbol(ib, cand, now_et)
                if entry:
                    entries.append(entry)
                if exit_sig:
                    exits.append(exit_sig)
            except Exception as e:
                logger.error(f"MR: Error processing {cand.symbol}: {e}")

        return entries, exits

    def _process_symbol(
        self, ib, cand: MRCandidate, now_et: datetime
    ) -> tuple[Optional[MREntrySignal], Optional[MRExitSignal]]:
        """Process a single symbol: fetch bars, compute indicators, check signals."""
        sym = cand.symbol
        state = self._get_state(sym)

        # ── Record first scan time for 90-min tracking ──
        if state.first_scan_time is None:
            state.first_scan_time = now_et

        # ── Check 90-min timeout ──
        tracking_deadline = state.first_scan_time + timedelta(minutes=MR_TRACKING_MINUTES)
        if now_et > tracking_deadline and state.in_position:
            return None, MRExitSignal(sym, cand.current_price, "90min_timeout")
        if now_et > tracking_deadline and not state.in_position:
            return None, None  # past tracking window, no action

        # ── EOD close ──
        if now_et.time() >= _EOD_CLOSE_TIME and state.in_position:
            return None, MRExitSignal(sym, cand.current_price, "eod_close")

        # ── Fetch 1-min bars from IBKR ──
        bars = ib.reqHistoricalData(
            cand.contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="1 min",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=2,
            timeout=10,
        )
        if not bars or len(bars) < 10:
            return None, None

        # Expose raw bars for external caching (e.g. Doji alert)
        self._last_raw_bars[sym] = bars

        # Convert to list of dicts
        bars_1m = []
        for b in bars:
            bars_1m.append({
                "time": b.date,
                "open": float(b.open),
                "high": float(b.high),
                "low": float(b.low),
                "close": float(b.close),
                "volume": float(b.volume),
            })

        # Skip if no new bars since last check
        if len(bars_1m) == state.last_bar_count:
            return None, None
        state.last_bar_count = len(bars_1m)

        # ── Compute indicators ──
        vwap_series = _compute_running_vwap(bars_1m)
        sma9_val = _compute_sma9_hourly(bars_1m)

        if not vwap_series:
            return None, None

        # Latest values
        current_vwap = vwap_series[-1]
        price = bars_1m[-1]["close"]
        latest_high = bars_1m[-1]["high"]
        latest_low = bars_1m[-1]["low"]

        if current_vwap <= 0:
            return None, None

        price_above_vwap = price > current_vwap

        # ── Check exit signals (position open) ──
        if state.in_position:
            # Update highest high
            if latest_high > state.highest_high:
                state.highest_high = latest_high

            # Trailing stop: 5% from peak
            trail_stop = state.highest_high * (1 - MR_LIVE_TRAILING_STOP_PCT)
            if price <= trail_stop:
                reason = (f"trailing_stop (peak=${state.highest_high:.4f}, "
                          f"-{MR_LIVE_TRAILING_STOP_PCT:.0%}=${trail_stop:.4f})")
                return None, MRExitSignal(sym, price, reason)

            # Safety stop: -5% from entry
            safety_stop = state.entry_price * (1 - MR_LIVE_SAFETY_STOP_PCT)
            if price <= safety_stop:
                reason = (f"safety_stop (entry=${state.entry_price:.4f}, "
                          f"-{MR_LIVE_SAFETY_STOP_PCT:.0%}=${safety_stop:.4f})")
                return None, MRExitSignal(sym, price, reason)

        # ── Check entry signals (no position) ──
        if not state.in_position:
            entry_signal = None

            # Signal 1: VWAP cross up (was below, now above)
            if state.prev_below_vwap and price_above_vwap:
                entry_signal = "vwap_cross"

            # Signal 2: Pullback to VWAP (was above, low touched VWAP, closed above)
            if entry_signal is None and state.prev_above_vwap and price_above_vwap:
                low_dist_to_vwap = (latest_low - current_vwap) / current_vwap
                if low_dist_to_vwap <= MR_LIVE_PULLBACK_TOUCH_PCT:
                    entry_signal = "pullback_vwap"

            if entry_signal and sma9_val > 0 and price > sma9_val:
                entry = MREntrySignal(
                    symbol=sym,
                    price=price,
                    vwap=current_vwap,
                    sma9_hourly=sma9_val,
                    signal_type=entry_signal,
                )
                # Mark position immediately to prevent duplicate entries
                state.in_position = True
                state.entry_price = price
                state.highest_high = price
                # Update VWAP tracking
                state.prev_below_vwap = not price_above_vwap
                state.prev_above_vwap = price_above_vwap
                return entry, None

        # Update VWAP position tracking for next iteration
        state.prev_below_vwap = not price_above_vwap
        state.prev_above_vwap = price_above_vwap

        return None, None
