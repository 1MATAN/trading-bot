"""VWAP Zone Live Strategy — price within VWAP band + SMA20 confirmation.

Entry:  price within +/-3% of VWAP + bar low >= prev bar low + price > SMA20 (1-min)
Exit:   ATR(14) x 1.5 trailing stop (min 3 bars hold)
Slots:  3 concurrent positions per symbol (SYM_0 / SYM_1 / SYM_2)
Sizing: $400 sub-$2, $300 otherwise
Dead zones: 11:00-11:30, 13:00-13:30, 14:00-15:00 ET
"""

import logging
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np

from config.settings import (
    VZ_LIVE_ATR_PERIOD,
    VZ_LIVE_ATR_TRAIL_MULT,
    VZ_LIVE_DEAD_ZONES,
    VZ_LIVE_LOSS_COOLDOWN_SEC,
    VZ_LIVE_MAX_ENTRIES_PER_SYM,
    VZ_LIVE_MAX_TRACKED_SYMBOLS,
    VZ_LIVE_MIN_HOLD_BARS,
    VZ_LIVE_SMA_PERIOD,
    VZ_LIVE_VWAP_BAND_PCT,
    VZ_LIVE_MAX_SLOTS_PER_SYM,
)

logger = logging.getLogger("trading_bot.vwap_zone_live")
_ET = ZoneInfo("US/Eastern")

_EOD_CLOSE_TIME = time(19, 55)

# Parse dead zones once
_DEAD_ZONES: list[tuple[time, time]] = []
for _start, _end in VZ_LIVE_DEAD_ZONES:
    h1, m1 = map(int, _start.split(":"))
    h2, m2 = map(int, _end.split(":"))
    _DEAD_ZONES.append((time(h1, m1), time(h2, m2)))


# ── Data classes ───────────────────────────────────────────

@dataclass
class VZCandidate:
    symbol: str
    contract: object  # ib_insync Contract
    gap_pct: float
    current_price: float


@dataclass
class VZEntrySignal:
    symbol: str
    slot_key: str     # "SYM_0", "SYM_1", "SYM_2"
    price: float
    vwap: float
    sma20: float
    atr: float


@dataclass
class VZExitSignal:
    symbol: str
    slot_key: str
    price: float
    reason: str


@dataclass
class _VZSlotState:
    in_position: bool = False
    entry_price: float = 0.0
    highest_high: float = 0.0
    entry_bar_idx: int = 0   # bar count at entry — for min-hold check


@dataclass
class _VZSymbolState:
    slots: list = field(default_factory=lambda: [_VZSlotState() for _ in range(VZ_LIVE_MAX_SLOTS_PER_SYM)])
    entries_today: int = 0
    cooldown_until: float = 0.0   # epoch timestamp
    last_bar_count: int = 0
    date_key: str = ""


# ── Helper functions ───────────────────────────────────────

def _compute_running_vwap(bars: list[dict]) -> list[float]:
    """Cumulative VWAP: sum(TP*Vol) / sum(Vol)."""
    if not bars:
        return []
    tp = np.array([(b["high"] + b["low"] + b["close"]) / 3.0 for b in bars])
    vol = np.array([float(b["volume"]) for b in bars])
    cum_tp_vol = np.cumsum(tp * vol)
    cum_vol = np.cumsum(vol)
    with np.errstate(invalid="ignore", divide="ignore"):
        vwap = np.where(cum_vol > 0, cum_tp_vol / cum_vol, 0.0)
    return vwap.tolist()


def _compute_sma(bars: list[dict], period: int) -> float:
    """SMA of close prices, latest value. Returns 0 if insufficient data."""
    if len(bars) < period:
        return 0.0
    closes = [b["close"] for b in bars[-period:]]
    return float(np.mean(closes))


def _compute_atr(bars: list[dict], period: int = VZ_LIVE_ATR_PERIOD) -> float:
    """ATR (Average True Range) from bars. Returns 0 if insufficient data."""
    if len(bars) < period + 1:
        return 0.0
    highs = np.array([b["high"] for b in bars])
    lows = np.array([b["low"] for b in bars])
    closes = np.array([b["close"] for b in bars])
    prev_close = np.roll(closes, 1)
    prev_close[0] = closes[0]
    tr = np.maximum(
        highs - lows,
        np.maximum(np.abs(highs - prev_close), np.abs(lows - prev_close)),
    )
    return float(np.mean(tr[-period:]))


def _in_dead_zone(t: time) -> bool:
    """Check if time falls in a dead zone."""
    for start, end in _DEAD_ZONES:
        if start <= t < end:
            return True
    return False


# ── Strategy class ─────────────────────────────────────────

class VWAPZoneLiveStrategy:
    """VWAP Zone live strategy — multi-slot, one cycle at a time.

    Each cycle:
      1. Fetch 1-min bars per symbol
      2. Compute VWAP / SMA20 / ATR
      3. Check exits on active slots (ATR trailing)
      4. Check entries on open slots (VWAP band + SMA20 + prev-bar-low)
    """

    def __init__(self, ib_getter):
        self._ib_getter = ib_getter
        self._states: dict[str, _VZSymbolState] = {}
        self._last_raw_bars: dict[str, list] = {}

    # ── State helpers ──────────────────────────────────────

    def _get_state(self, symbol: str) -> _VZSymbolState:
        today = datetime.now(_ET).strftime("%Y-%m-%d")
        state = self._states.get(symbol)
        if state is None or state.date_key != today:
            state = _VZSymbolState(date_key=today)
            self._states[symbol] = state
        return state

    def mark_slot_open(self, symbol: str, slot_idx: int, entry_price: float, bar_count: int = 0):
        state = self._get_state(symbol)
        sl = state.slots[slot_idx]
        sl.in_position = True
        sl.entry_price = entry_price
        sl.highest_high = entry_price
        sl.entry_bar_idx = bar_count

    def mark_slot_closed(self, symbol: str, slot_idx: int):
        state = self._get_state(symbol)
        sl = state.slots[slot_idx]
        sl.in_position = False
        sl.entry_price = 0.0
        sl.highest_high = 0.0
        sl.entry_bar_idx = 0

    def set_loss_cooldown(self, symbol: str):
        state = self._get_state(symbol)
        state.cooldown_until = _time.time() + VZ_LIVE_LOSS_COOLDOWN_SEC

    def sync_from_portfolio(self, held_positions: dict):
        """Restore state from portfolio after restart.

        Args:
            held_positions: {slot_key: {'entry_price': float, ...}}
                            slot_key format: "SYM_0", "SYM_1", "SYM_2"
        """
        for slot_key, pos in held_positions.items():
            sym, idx_s = slot_key.rsplit("_", 1)
            try:
                idx = int(idx_s)
            except ValueError:
                continue
            if idx >= VZ_LIVE_MAX_SLOTS_PER_SYM:
                continue
            entry_price = pos.get("entry_price", 0) if isinstance(pos, dict) else 0
            self.mark_slot_open(sym, idx, entry_price)
            logger.info(f"VZ sync: {slot_key} in_position @ ${entry_price:.4f}")

    # ── Main cycle ─────────────────────────────────────────

    def process_cycle(
        self, candidates: list[VZCandidate]
    ) -> tuple[list[VZEntrySignal], list[VZExitSignal]]:
        """Run one cycle. Returns (entries, exits)."""
        entries: list[VZEntrySignal] = []
        exits: list[VZExitSignal] = []
        self._last_raw_bars.clear()

        ib = self._ib_getter()
        if ib is None or not ib.isConnected():
            logger.warning("VZ: IBKR not connected, skipping cycle")
            return entries, exits

        now_et = datetime.now(_ET)
        tracked = candidates[:VZ_LIVE_MAX_TRACKED_SYMBOLS]

        for cand in tracked:
            try:
                ent_list, exit_list = self._process_symbol(ib, cand, now_et)
                entries.extend(ent_list)
                exits.extend(exit_list)
            except Exception as e:
                logger.error(f"VZ: Error processing {cand.symbol}: {e}")

        return entries, exits

    # ── Per-symbol processing ──────────────────────────────

    def _process_symbol(
        self, ib, cand: VZCandidate, now_et: datetime
    ) -> tuple[list[VZEntrySignal], list[VZExitSignal]]:
        """Fetch bars once, evaluate all slots."""
        sym = cand.symbol
        state = self._get_state(sym)
        entries: list[VZEntrySignal] = []
        exits: list[VZExitSignal] = []

        # ── EOD close all slots ──
        if now_et.time() >= _EOD_CLOSE_TIME:
            for i, sl in enumerate(state.slots):
                if sl.in_position:
                    key = f"{sym}_{i}"
                    exits.append(VZExitSignal(sym, key, cand.current_price, "eod_close"))
            return entries, exits

        # ── Fetch 1-min bars ──
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
        if not bars or len(bars) < VZ_LIVE_SMA_PERIOD + 2:
            return entries, exits

        self._last_raw_bars[sym] = bars

        bars_1m = [
            {
                "time": b.date,
                "open": float(b.open),
                "high": float(b.high),
                "low": float(b.low),
                "close": float(b.close),
                "volume": float(b.volume),
            }
            for b in bars
        ]

        # Skip if no new bars
        n_bars = len(bars_1m)
        if n_bars == state.last_bar_count:
            return entries, exits
        state.last_bar_count = n_bars

        # ── Compute indicators ──
        vwap_series = _compute_running_vwap(bars_1m)
        if not vwap_series:
            return entries, exits

        current_vwap = vwap_series[-1]
        sma20 = _compute_sma(bars_1m, VZ_LIVE_SMA_PERIOD)
        atr = _compute_atr(bars_1m, VZ_LIVE_ATR_PERIOD)
        price = bars_1m[-1]["close"]

        if current_vwap <= 0 or sma20 <= 0:
            return entries, exits

        # ── Check exits on active slots ──
        for i, sl in enumerate(state.slots):
            if not sl.in_position:
                continue
            key = f"{sym}_{i}"

            # Update highest high
            latest_high = bars_1m[-1]["high"]
            if latest_high > sl.highest_high:
                sl.highest_high = latest_high

            # Min hold check
            bars_held = n_bars - sl.entry_bar_idx
            if bars_held < VZ_LIVE_MIN_HOLD_BARS:
                continue

            # ATR trailing stop
            if atr > 0:
                trail_dist = VZ_LIVE_ATR_TRAIL_MULT * atr
                trail_stop = sl.highest_high - trail_dist
                if price <= trail_stop:
                    pct = (trail_dist / sl.highest_high * 100) if sl.highest_high > 0 else 0
                    reason = (
                        f"atr_trail (peak=${sl.highest_high:.4f}, "
                        f"ATR=${atr:.4f}, stop=${trail_stop:.4f} [{pct:.1f}%])"
                    )
                    exits.append(VZExitSignal(sym, key, price, reason))

        # ── Check entry on open slots ──
        # Dead zone filter
        if _in_dead_zone(now_et.time()):
            return entries, exits

        # Daily limit
        if state.entries_today >= VZ_LIVE_MAX_ENTRIES_PER_SYM:
            return entries, exits

        # Cooldown
        if _time.time() < state.cooldown_until:
            return entries, exits

        # VWAP band check: price within +/-3% of VWAP
        vwap_dist = abs(price - current_vwap) / current_vwap
        if vwap_dist > VZ_LIVE_VWAP_BAND_PCT:
            return entries, exits

        # Price > SMA20
        if price <= sma20:
            return entries, exits

        # Bar low >= prev bar low
        if n_bars < 2:
            return entries, exits
        if bars_1m[-1]["low"] < bars_1m[-2]["low"]:
            return entries, exits

        # ATR must be available for trailing stop
        if atr <= 0:
            return entries, exits

        # Find next open slot
        for i, sl in enumerate(state.slots):
            if sl.in_position:
                continue
            key = f"{sym}_{i}"
            state.entries_today += 1
            sl.in_position = True
            sl.entry_price = price
            sl.highest_high = price
            sl.entry_bar_idx = n_bars
            entries.append(VZEntrySignal(sym, key, price, current_vwap, sma20, atr))
            break  # one entry per symbol per cycle

        return entries, exits
