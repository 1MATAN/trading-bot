"""VWAP Pullback entry and two-phase trailing-stop exit signal logic.

Entry: VWAP pullback state machine (WATCHING → PULLING_BACK → BOUNCE).
Exit: Two-phase trailing stop + VWAP exit (3 bars below VWAP).
Re-entry: Cooldown-based (15 bars) + max entries per day.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from config.settings import (
    VWAP_PROXIMITY_PCT,
    VWAP_OVERSHOOT_MAX_PCT,
    MIN_BARS_ABOVE_VWAP,
    BOUNCE_CONFIRM_BARS,
    MAX_PULLBACK_BARS,
    MAX_PULLBACK_DEPTH_PCT,
    VWAP_WARMUP_BARS,
    MAX_ENTRIES_PER_DAY,
    STOP_BUFFER_PCT,
    STOP_MIN_DISTANCE_PCT,
    STOP_MAX_DISTANCE_PCT,
    TRAILING_PHASE1_BARS,
    TRAILING_PHASE2_ATR_MULT,
    TRAILING_ATR_PERIOD,
    VWAP_EXIT_BARS,
    RE_ENTRY_COOLDOWN_BARS,
)
from strategies.indicators import vwap_value, atr_value

logger = logging.getLogger("trading_bot.signal")


@dataclass
class EntrySignal:
    """Result of VWAP pullback entry check."""
    symbol: str
    passed: bool
    current_price: float
    pullback_low: float
    gap_pct: float
    bounce_price: float
    vwap: float
    details: dict = field(default_factory=dict)


class PullbackStateMachine:
    """Tracks per-symbol pullback state for live trading."""

    def __init__(self) -> None:
        self.phase = "WATCHING"  # WATCHING / PULLING_BACK / BOUNCE
        self.bars_above_vwap = 0
        self.pullback_low = float('inf')
        self.pullback_bar_count = 0
        self.touched_vwap = False
        self.bounce_count = 0
        self.entries_today = 0
        self.re_entry_cooldown = 0
        self.below_vwap_count = 0

    def reset(self) -> None:
        self.phase = "WATCHING"
        self.bars_above_vwap = 0
        self.pullback_low = float('inf')
        self.pullback_bar_count = 0
        self.touched_vwap = False
        self.bounce_count = 0
        self.below_vwap_count = 0

    def reset_daily(self) -> None:
        self.reset()
        self.entries_today = 0


def check_entry(
    symbol: str,
    df_1m: pd.DataFrame,
    state: PullbackStateMachine,
    gap_pct: float = 0.0,
) -> EntrySignal:
    """Run one tick of the VWAP pullback state machine.

    Args:
        symbol: Ticker symbol.
        df_1m: Intraday OHLCV with enough bars for VWAP.
        state: Per-symbol pullback state machine.
        gap_pct: Intraday move % (must be >= 20% to qualify).

    Returns:
        EntrySignal with passed=True when bounce is confirmed.
    """
    current_price = float(df_1m["close"].iloc[-1]) if len(df_1m) > 0 else 0.0
    current_vwap = vwap_value(df_1m)

    empty_signal = EntrySignal(
        symbol=symbol, passed=False, current_price=current_price,
        pullback_low=state.pullback_low, gap_pct=gap_pct,
        bounce_price=0.0, vwap=current_vwap,
    )

    # Guards
    if state.re_entry_cooldown > 0:
        return empty_signal
    if state.entries_today >= MAX_ENTRIES_PER_DAY:
        return empty_signal
    if len(df_1m) < VWAP_WARMUP_BARS:
        return empty_signal
    if current_vwap <= 0:
        return empty_signal

    vwap_distance_pct = (current_price - current_vwap) / current_vwap

    if state.phase == "WATCHING":
        if current_price > current_vwap:
            state.bars_above_vwap += 1
        else:
            state.bars_above_vwap = 0

        if state.bars_above_vwap >= MIN_BARS_ABOVE_VWAP:
            if vwap_distance_pct < 0.02:
                state.phase = "PULLING_BACK"
                state.pullback_low = current_price
                state.pullback_bar_count = 1
                state.touched_vwap = False
                logger.debug(f"{symbol}: WATCHING → PULLING_BACK")

    elif state.phase == "PULLING_BACK":
        state.pullback_bar_count += 1
        state.pullback_low = min(state.pullback_low, current_price)

        bar_high = float(df_1m["high"].iloc[-1])
        pullback_depth = (bar_high - state.pullback_low) / bar_high if bar_high > 0 else 0

        if (state.pullback_bar_count > MAX_PULLBACK_BARS
                or pullback_depth > MAX_PULLBACK_DEPTH_PCT
                or vwap_distance_pct < -VWAP_OVERSHOOT_MAX_PCT):
            state.reset()
            return empty_signal

        if abs(vwap_distance_pct) <= VWAP_PROXIMITY_PCT or vwap_distance_pct < 0:
            state.touched_vwap = True

        if state.touched_vwap and current_price > current_vwap:
            state.phase = "BOUNCE"
            state.bounce_count = 1
            logger.debug(
                f"{symbol}: PULLING_BACK → BOUNCE "
                f"(pullback_low=${state.pullback_low:.4f})"
            )

    elif state.phase == "BOUNCE":
        if current_price > current_vwap:
            state.bounce_count += 1
        else:
            state.phase = "PULLING_BACK"
            state.bounce_count = 0
            return empty_signal

        if state.bounce_count >= BOUNCE_CONFIRM_BARS:
            signal = EntrySignal(
                symbol=symbol,
                passed=True,
                current_price=current_price,
                pullback_low=state.pullback_low,
                gap_pct=gap_pct,
                bounce_price=current_price,
                vwap=current_vwap,
                details={
                    "pullback_bars": state.pullback_bar_count,
                    "bounce_bars": state.bounce_count,
                },
            )
            state.entries_today += 1
            state.reset()
            logger.info(
                f"ENTRY SIGNAL: {symbol} @ ${current_price:.4f} "
                f"(vwap_pullback, gap={gap_pct:+.1f}%, "
                f"pullback_low=${signal.pullback_low:.4f}, "
                f"VWAP=${current_vwap:.4f})"
            )
            return signal

    return empty_signal


def compute_stop_price(entry_price: float, pullback_low: float) -> float:
    """Calculate dynamic stop: pullback_low - buffer, clamped to 1.5-5%."""
    raw_stop = pullback_low * (1 - STOP_BUFFER_PCT)
    stop_floor = entry_price * (1 - STOP_MAX_DISTANCE_PCT)
    stop_ceil = entry_price * (1 - STOP_MIN_DISTANCE_PCT)
    return round(max(stop_floor, min(raw_stop, stop_ceil)), 4)


def get_trailing_stop_price(
    df_1m: pd.DataFrame,
    highest_high: float,
    bars_since_entry: int,
) -> float:
    """Two-phase trailing stop.

    Phase 1 (first 15 bars): 3-bar swing low - buffer.
    Phase 2 (after 15 bars): ATR x 1.5 from highest high.

    Returns 0.0 if insufficient data.
    """
    if len(df_1m) < 3:
        return 0.0

    if bars_since_entry <= TRAILING_PHASE1_BARS:
        recent_low = min(
            float(df_1m["low"].iloc[-1]),
            float(df_1m["low"].iloc[-2]),
            float(df_1m["low"].iloc[-3]),
        )
        return round(recent_low * (1 - STOP_BUFFER_PCT), 4)
    else:
        current_atr = atr_value(df_1m, TRAILING_ATR_PERIOD)
        if current_atr > 0 and highest_high > 0:
            return round(highest_high - current_atr * TRAILING_PHASE2_ATR_MULT, 4)
        return round(highest_high * 0.97, 4)


def check_vwap_exit(
    df_1m: pd.DataFrame,
    below_vwap_count: int,
) -> tuple[bool, int]:
    """Check if price has closed below VWAP for N consecutive bars.

    Returns (should_exit, updated_count).
    """
    if len(df_1m) < 1:
        return False, below_vwap_count

    current_price = float(df_1m["close"].iloc[-1])
    current_vwap = vwap_value(df_1m)

    if current_vwap <= 0:
        return False, below_vwap_count

    if current_price < current_vwap:
        below_vwap_count += 1
    else:
        below_vwap_count = 0

    return below_vwap_count >= VWAP_EXIT_BARS, below_vwap_count


def check_reentry(state: PullbackStateMachine) -> bool:
    """Check if cooldown has elapsed and entries remain.

    Returns True if re-entry is allowed.
    """
    return state.re_entry_cooldown <= 0 and state.entries_today < MAX_ENTRIES_PER_DAY
