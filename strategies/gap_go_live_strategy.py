"""Gap and Go Live Strategy — Heikin Ashi + VWAP on 1-min bars.

Entry (first):  price above VWAP + within 2% of VWAP + HA green 1m + HA green 5m
Entry (re):     price above VWAP (no 2% limit) + HA green 1m + HA green 5m
Exit:           HA red 1m + HA red 5m
EOD close:      19:55 ET
Hours:          04:15 ET — 19:55 ET
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from config.settings import (
    GG_LIVE_GAP_MIN_PCT,
    GG_LIVE_VWAP_PROXIMITY_PCT,
    GG_LIVE_MAX_TRACKED_SYMBOLS,
)

logger = logging.getLogger("trading_bot.gap_go_live")
_ET = ZoneInfo("US/Eastern")

# ── Time boundaries ────────────────────────────────────────
_ENTRY_OK_TIME = time(4, 15)     # 04:15 ET — pre-market
_EOD_CLOSE_TIME = time(19, 55)   # 19:55 ET — force close


# ── Data classes ───────────────────────────────────────────

@dataclass
class GGCandidate:
    symbol: str
    contract: object          # ib_insync Contract
    gap_pct: float
    prev_close: float
    current_price: float


@dataclass
class GGEntrySignal:
    symbol: str
    price: float
    vwap: float
    is_first_entry: bool
    ha_1m_green: bool
    ha_5m_green: bool


@dataclass
class GGExitSignal:
    symbol: str
    price: float
    reason: str


@dataclass
class _GGSymbolState:
    first_entry_done: bool = False
    in_position: bool = False
    last_bar_count: int = 0
    date_key: str = ""


# ── Heikin Ashi ────────────────────────────────────────────

def _compute_heikin_ashi(candles: list[dict]) -> list[dict]:
    """Compute Heikin Ashi from regular OHLC candles.

    Returns list of dicts with keys: time, ha_open, ha_close, is_green.
    """
    if not candles:
        return []

    ha = []
    prev_ha_open = candles[0]["open"]
    prev_ha_close = (candles[0]["open"] + candles[0]["high"]
                     + candles[0]["low"] + candles[0]["close"]) / 4.0

    for c in candles:
        ha_close = (c["open"] + c["high"] + c["low"] + c["close"]) / 4.0
        ha_open = (prev_ha_open + prev_ha_close) / 2.0

        ha.append({
            "time": c["time"],
            "ha_open": ha_open,
            "ha_close": ha_close,
            "is_green": ha_close >= ha_open,
        })

        prev_ha_open = ha_open
        prev_ha_close = ha_close

    return ha


def _aggregate_to_5min(bars_1m: list[dict]) -> list[dict]:
    """Aggregate 1-min OHLCV candles into 5-min candles."""
    candles = []
    n = len(bars_1m)
    for start in range(0, n, 5):
        end = min(start + 5, n)
        chunk = bars_1m[start:end]
        candles.append({
            "time": chunk[0]["time"],
            "open": chunk[0]["open"],
            "high": max(c["high"] for c in chunk),
            "low": min(c["low"] for c in chunk),
            "close": chunk[-1]["close"],
            "volume": sum(c["volume"] for c in chunk),
        })
    return candles


def _compute_running_vwap(bars_1m: list[dict]) -> list[float]:
    """Cumulative VWAP from 1-min bars: sum(TP*Vol) / sum(Vol)."""
    if not bars_1m:
        return []
    tp = np.array([(b["high"] + b["low"] + b["close"]) / 3.0 for b in bars_1m])
    vol = np.array([float(b["volume"]) for b in bars_1m])
    cum_tp_vol = np.cumsum(tp * vol)
    cum_vol = np.cumsum(vol)
    vwap = np.where(cum_vol > 0, cum_tp_vol / cum_vol, 0.0)
    return vwap.tolist()


# ── Strategy class ─────────────────────────────────────────

class GapGoLiveStrategy:
    """Gap and Go live strategy — processes one cycle at a time.

    Each cycle:
      1. Request 1-min bars from IBKR for each tracked symbol
      2. Build HA 1m + 5m + running VWAP
      3. Check entry/exit conditions on the latest candle
    """

    def __init__(self, ib_getter):
        """
        Args:
            ib_getter: callable that returns a connected IB instance (or None).
        """
        self._ib_getter = ib_getter
        self._states: dict[str, _GGSymbolState] = {}

    def _get_state(self, symbol: str) -> _GGSymbolState:
        today = datetime.now(_ET).strftime("%Y-%m-%d")
        state = self._states.get(symbol)
        if state is None or state.date_key != today:
            state = _GGSymbolState(date_key=today)
            self._states[symbol] = state
        return state

    def mark_in_position(self, symbol: str):
        self._get_state(symbol).in_position = True

    def mark_position_closed(self, symbol: str):
        self._get_state(symbol).in_position = False

    def sync_from_portfolio(self, held_symbols: set):
        """Sync strategy state from portfolio after restart."""
        for sym in held_symbols:
            state = self._get_state(sym)
            state.in_position = True
            state.first_entry_done = True
            logger.info(f"GG strategy sync: {sym} marked in_position")

    def process_cycle(
        self, candidates: list[GGCandidate]
    ) -> tuple[list[GGEntrySignal], list[GGExitSignal]]:
        """Run one strategy cycle over all candidates.

        Returns (entries, exits).
        """
        entries: list[GGEntrySignal] = []
        exits: list[GGExitSignal] = []

        ib = self._ib_getter()
        if ib is None or not ib.isConnected():
            logger.warning("GG: IBKR not connected, skipping cycle")
            return entries, exits

        now_et = datetime.now(_ET).time()

        # Limit tracked symbols
        tracked = candidates[:GG_LIVE_MAX_TRACKED_SYMBOLS]

        for cand in tracked:
            try:
                entry, exit_sig = self._process_symbol(ib, cand, now_et)
                if entry:
                    entries.append(entry)
                if exit_sig:
                    exits.append(exit_sig)
            except Exception as e:
                logger.error(f"GG: Error processing {cand.symbol}: {e}")

        return entries, exits

    def _process_symbol(
        self, ib, cand: GGCandidate, now_et: time
    ) -> tuple[Optional[GGEntrySignal], Optional[GGExitSignal]]:
        """Process a single symbol: fetch bars, compute indicators, check signals."""
        sym = cand.symbol
        state = self._get_state(sym)

        # ── Fetch 1-min bars from IBKR ──
        bars = ib.reqHistoricalData(
            cand.contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="1 min",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=2,
        )
        if not bars or len(bars) < 10:
            return None, None

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
        ha_1m = _compute_heikin_ashi(bars_1m)
        bars_5m = _aggregate_to_5min(bars_1m)
        ha_5m = _compute_heikin_ashi(bars_5m)
        vwap_series = _compute_running_vwap(bars_1m)

        if not ha_1m or not ha_5m or not vwap_series:
            return None, None

        # Latest values
        latest_1m = ha_1m[-1]
        latest_5m = ha_5m[-1]
        current_vwap = vwap_series[-1]
        price = bars_1m[-1]["close"]

        if current_vwap <= 0:
            return None, None

        # ── EOD close ──
        if now_et >= _EOD_CLOSE_TIME and state.in_position:
            return None, GGExitSignal(sym, price, "eod_close")

        # ── Check exit (both HA red) ──
        if state.in_position:
            ha_1m_red = not latest_1m["is_green"]
            ha_5m_red = not latest_5m["is_green"]
            if ha_1m_red and ha_5m_red:
                return None, GGExitSignal(sym, price, "ha_exit (1m+5m red)")

        # ── Check entry ──
        if not state.in_position and _ENTRY_OK_TIME <= now_et < _EOD_CLOSE_TIME:
            # Price must be above VWAP
            if price <= current_vwap:
                return None, None

            # First entry: within 2% of VWAP
            vwap_dist = (price - current_vwap) / current_vwap
            if not state.first_entry_done and vwap_dist > GG_LIVE_VWAP_PROXIMITY_PCT:
                return None, None

            # HA must be green on both timeframes
            if not latest_1m["is_green"] or not latest_5m["is_green"]:
                return None, None

            # All conditions met — mark position immediately to prevent duplicate entries
            is_first = not state.first_entry_done
            state.in_position = True
            state.first_entry_done = True
            entry = GGEntrySignal(
                symbol=sym,
                price=price,
                vwap=current_vwap,
                is_first_entry=is_first,
                ha_1m_green=True,
                ha_5m_green=True,
            )
            return entry, None

        return None, None
