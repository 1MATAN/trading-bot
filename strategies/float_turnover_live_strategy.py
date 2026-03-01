"""Float Turnover Live Strategy — 30-sec bars, real-time entry/exit.

Entry (not in position): latest 30s bar low >= previous 30s bar low (held support)
Exit  (in position):     price <= prev bar low * 0.95  (stop = 5% below prev low)
Re-entry:                same as entry
EOD close at 19:55 ET.

Float turnover filter: ≥15% of float traded by volume.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo

from config.settings import FT_LIVE_MAX_TRACKED_SYMBOLS

logger = logging.getLogger("trading_bot.float_turnover_live")
_ET = ZoneInfo("US/Eastern")

_EOD_CLOSE_TIME = time(19, 55)  # force close
_FT_STOP_PCT = 0.05  # 5% below previous bar low (was 3%, too tight on pennies)
_FT_REENTRY_COOLDOWN_SEC = 60  # seconds before re-entry on same stock


# ── Data classes ───────────────────────────────────────────

@dataclass
class FTCandidate:
    symbol: str
    contract: object          # ib_insync Contract
    price: float
    float_shares: float
    turnover_pct: float
    vwap: float = 0.0


@dataclass
class FTEntrySignal:
    symbol: str
    price: float
    turnover_pct: float


@dataclass
class FTExitSignal:
    symbol: str
    price: float
    reason: str


@dataclass
class _FTSymbolState:
    in_position: bool = False
    entry_price: float = 0.0
    last_bar_count: int = 0         # detect new bars
    date_key: str = ""              # daily reset
    last_exit_time: float = 0.0     # timestamp of last exit (for cooldown)


# ── Strategy class ─────────────────────────────────────────

class FloatTurnoverLiveStrategy:
    """Float Turnover live strategy — 30-second bars, real-time.

    Each cycle:
      1. Request 30-sec bars from IBKR for each tracked symbol
      2. Entry: latest bar low >= previous bar low (support held)
      3. Exit: price drops to 5% below previous bar low (stop hit)
    """

    def __init__(self, ib_getter):
        self._ib_getter = ib_getter
        self._states: dict[str, _FTSymbolState] = {}

    def _get_state(self, symbol: str) -> _FTSymbolState:
        today = datetime.now(_ET).strftime("%Y-%m-%d")
        state = self._states.get(symbol)
        if state is None or state.date_key != today:
            state = _FTSymbolState(date_key=today)
            self._states[symbol] = state
        return state

    def mark_in_position(self, symbol: str, entry_price: float):
        state = self._get_state(symbol)
        state.in_position = True
        state.entry_price = entry_price

    def mark_position_closed(self, symbol: str):
        import time as time_mod
        state = self._get_state(symbol)
        state.in_position = False
        state.entry_price = 0.0
        state.last_exit_time = time_mod.time()

    def sync_from_portfolio(self, held_positions: dict):
        """Sync strategy state from portfolio after restart.

        Args:
            held_positions: {sym: {'entry_price': float, ...}}
        """
        for sym, pos in held_positions.items():
            entry_price = pos.get('entry_price', 0)
            self.mark_in_position(sym, entry_price)
            logger.info(f"FT strategy sync: {sym} marked in_position @ ${entry_price:.4f}")

    def process_cycle(
        self, candidates: list[FTCandidate]
    ) -> tuple[list[FTEntrySignal], list[FTExitSignal]]:
        """Run one strategy cycle over all candidates.

        Returns (entries, exits).
        """
        entries: list[FTEntrySignal] = []
        exits: list[FTExitSignal] = []

        ib = self._ib_getter()
        if ib is None or not ib.isConnected():
            logger.warning("FT: IBKR not connected, skipping cycle")
            return entries, exits

        now_et = datetime.now(_ET)

        # Limit tracked symbols
        tracked = candidates[:FT_LIVE_MAX_TRACKED_SYMBOLS]

        for cand in tracked:
            try:
                entry, exit_sig = self._process_symbol(ib, cand, now_et)
                if entry:
                    entries.append(entry)
                if exit_sig:
                    exits.append(exit_sig)
            except Exception as e:
                logger.error(f"FT: Error processing {cand.symbol}: {e}")

        return entries, exits

    def _process_symbol(
        self, ib, cand: FTCandidate, now_et: datetime
    ) -> tuple[Optional[FTEntrySignal], Optional[FTExitSignal]]:
        """Process a single symbol: fetch 30-sec bars, check entry/stop."""
        sym = cand.symbol
        state = self._get_state(sym)

        # ── EOD close ──
        if now_et.time() >= _EOD_CLOSE_TIME and state.in_position:
            return None, FTExitSignal(sym, cand.price, "eod_close")

        # ── Fetch 30-sec bars from IBKR ──
        bars = ib.reqHistoricalData(
            cand.contract,
            endDateTime="",
            durationStr="3600 S",
            barSizeSetting="30 secs",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=2,
            timeout=10,
        )
        if not bars or len(bars) < 3:
            return None, None

        # Skip if no new bars since last check
        if len(bars) == state.last_bar_count:
            return None, None
        state.last_bar_count = len(bars)

        # Latest bar and previous bar (30-sec each)
        latest = bars[-1]
        prev = bars[-2]

        latest_low = float(latest.low)
        prev_low = float(prev.low)
        price = float(latest.close)

        # Stop level: 5% below previous bar's low
        stop_price = prev_low * (1 - _FT_STOP_PCT)

        # ── Check exit signals (position open) ──
        if state.in_position:
            if price <= stop_price:
                reason = (f"stop_hit (price=${price:.4f} <= "
                          f"prev_low=${prev_low:.4f} -{_FT_STOP_PCT*100:.0f}% = ${stop_price:.4f})")
                return None, FTExitSignal(sym, price, reason)

        # ── Check entry signals (no position) ──
        if not state.in_position:
            import time as time_mod
            now_ts = time_mod.time()
            cooldown_ok = (now_ts - state.last_exit_time) >= _FT_REENTRY_COOLDOWN_SEC
            vwap_ok = cand.vwap <= 0 or price > cand.vwap  # above VWAP (or no VWAP data)
            # Volume confirmation: min 100 shares + green bar (close > open)
            latest_vol = float(latest.volume)
            latest_green = float(latest.close) > float(latest.open)
            # Close in upper half of range
            bar_range = float(latest.high) - float(latest.low)
            upper_half = bar_range <= 0 or (price - float(latest.low)) >= bar_range * 0.5
            vol_ok = latest_vol >= 100 and latest_green and upper_half
            # Entry: support held + above VWAP + cooldown passed + volume confirmed
            if latest_low >= prev_low and vwap_ok and cooldown_ok and vol_ok:
                state.in_position = True
                state.entry_price = price
                return FTEntrySignal(
                    symbol=sym,
                    price=price,
                    turnover_pct=cand.turnover_pct,
                ), None

        return None, None
