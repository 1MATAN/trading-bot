"""Fibonacci Double-Touch Backtest (15-second bars, IBKR data).

Strategy:
  1. Stock makes a big daily move (gap from gappers.csv)
  2. Compute fibonacci retracement levels from 5y daily anchor candle
  3. Load 15-second intraday bars for the gap day
  4. Double-touch detection at fib support level:
     a. Price touches fib support (within 0.8%)  -> FIRST_TOUCH
     b. Price bounces away (> 0.5% above)        -> BOUNCED
     c. Price touches fib again (>= 3 bars later) -> BUY next bar open
  5. Stop loss: 3% below the fib support level
  6. Exit:
     - 50% at 3rd fib level above entry (target)
     - Remaining 50% trails with previous candle's low as stop
  7. No session restrictions, no 50% range filter, no entry window
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import time as dt_time
from typing import Optional

import numpy as np
import pandas as pd
import pytz

from config.settings import (
    STARTING_CAPITAL,
    POSITION_SIZE_PCT,
    SLIPPAGE_PCT,
    COMMISSION_PER_SHARE,
    MIN_COMMISSION,
    LIVE_STATE_PATH,
    DATA_DIR,
    BACKTEST_DATA_DIR,
    FIB_CHARTS_DIR,
    FIB_DT_STOP_PCT,
    FIB_DT_TARGET_LEVELS,
    FIB_DT_PROXIMITY_PCT,
    FIB_DT_MIN_BOUNCE_BARS,
    FIB_DT_MAX_ENTRIES_PER_DAY,
    FIB_DT_GAP_MIN_PCT,
    FIB_DT_GAP_MAX_PCT,
    FIB_DT_MIN_GAP_VOLUME,
    FIB_DT_ENTRY_WINDOW_START,
    FIB_DT_ENTRY_WINDOW_END,
    FIB_DT_PREFERRED_RATIOS,
    FIB_DT_USE_RATIO_FILTER,
    FIB_DT_S1_ONLY,
)
from simulation.sim_engine import SimPosition, SimResult
from strategies.fibonacci_engine import (
    find_anchor_candle,
    build_dual_series,
    advance_series,
    DualFibSeries,
)
from notifications.trade_logger import TradeLogger
from utils.time_utils import now_utc

logger = logging.getLogger("trading_bot.fib_double_touch")

GAPPERS_CSV = DATA_DIR / "gappers.csv"


# ── Fibonacci helpers (reused from fib_reversal_backtest) ─────────

def _get_all_fib_prices(dual: DualFibSeries, current_price: float) -> list[float]:
    """All unique prices from both series, sorted ascending. Auto-advances."""
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


# ── Double-Touch State Machine ──────────────────────────────────

@dataclass
class TouchState:
    """Per-fib-level double-touch tracking."""
    phase: str = "IDLE"        # IDLE -> FIRST_TOUCH -> BOUNCED -> (trigger)
    first_touch_bar: int = -1  # bar index of first touch
    fib_price: float = 0.0     # the fib level being tracked
    fib_idx: int = -1          # index in fib_prices list


# ── Main Engine ──────────────────────────────────────────────────

class FibDoubleTouchEngine:
    """Double-touch fib support backtest on 15-second bars."""

    def __init__(self, capital: float = STARTING_CAPITAL) -> None:
        self._capital = capital
        self._cash = capital
        self._position: Optional[SimPosition] = None
        self._result = SimResult()
        self._trade_logger = TradeLogger()
        self._peak_equity = capital
        self._trade_charts_data: list[dict] = []

        # Split position tracking
        self._half_sold = False
        self._remaining_qty = 0
        self._target_price = 0.0
        self._stop_price = 0.0
        self._fib_support_price = 0.0
        self._prev_bar_low = 0.0
        self._prev_bar_high = 0.0

    # ── Public API ────────────────────────────────────────

    def run(self) -> SimResult:
        """Run backtest across all gap events."""
        logger.info(f"Double-Touch Fib Backtest starting: capital=${self._capital:,.2f}")
        logger.info(
            f"  Stop: {FIB_DT_STOP_PCT:.1%} below fib | "
            f"Target: {FIB_DT_TARGET_LEVELS} fib levels above | "
            f"Proximity: {FIB_DT_PROXIMITY_PCT:.1%} | "
            f"Min bounce bars: {FIB_DT_MIN_BOUNCE_BARS} | "
            f"Gap: {FIB_DT_GAP_MIN_PCT}-{FIB_DT_GAP_MAX_PCT}% | "
            f"Min volume: {FIB_DT_MIN_GAP_VOLUME:,}"
        )

        gappers = self._load_gappers()
        if gappers.empty:
            logger.warning("No gap events found")
            return self._result

        # Group by symbol
        gap_by_symbol: dict[str, list[dict]] = {}
        for _, row in gappers.iterrows():
            sym = row["symbol"]
            gap_by_symbol.setdefault(sym, []).append(row.to_dict())

        symbols = sorted(gap_by_symbol.keys())
        self._result.symbols_tested = symbols
        logger.info(f"  {len(gappers)} gap events across {len(symbols)} symbols")

        for symbol in symbols:
            try:
                self._simulate_symbol(symbol, gap_by_symbol[symbol])
            except Exception as e:
                logger.error(f"Error for {symbol}: {e}", exc_info=True)

        self._write_live_state()
        self._print_summary()
        self._save_trades_csv()
        return self._result

    # ── Data Loading ─────────────────────────────────────

    def _load_gappers(self) -> pd.DataFrame:
        if not GAPPERS_CSV.exists():
            logger.error(f"Gappers CSV not found: {GAPPERS_CSV}")
            return pd.DataFrame()
        return pd.read_csv(GAPPERS_CSV)

    def _load_daily_5y(self, symbol: str) -> Optional[pd.DataFrame]:
        cache = BACKTEST_DATA_DIR / f"{symbol}_daily_5y.parquet"
        if not cache.exists():
            logger.debug(f"  {symbol}: no 5y daily cache")
            return None
        try:
            df = pd.read_parquet(cache)
            if len(df) >= 20:
                return df
        except Exception:
            pass
        return None

    def _load_15s_data(self, symbol: str, date_str: str) -> Optional[pd.DataFrame]:
        cache = BACKTEST_DATA_DIR / f"{symbol}_15s_{date_str}.parquet"
        if not cache.exists():
            return None
        try:
            df = pd.read_parquet(cache)
            if len(df) >= 50:
                return df
        except Exception:
            pass
        return None

    # ── Per-Symbol Simulation ────────────────────────────

    def _simulate_symbol(self, symbol: str, gap_events: list[dict]) -> None:
        logger.info(f"\n--- {symbol}: {len(gap_events)} gap events ---")

        daily_5y = self._load_daily_5y(symbol)
        if daily_5y is None:
            logger.warning(f"  {symbol}: no daily data")
            return

        anchor = find_anchor_candle(daily_5y)
        if anchor is None:
            logger.warning(f"  {symbol}: no anchor candle")
            return

        anchor_low, anchor_high, anchor_date = anchor
        dual = build_dual_series(anchor_low, anchor_high)
        anchor_info = {
            "anchor_date": anchor_date,
            "anchor_low": anchor_low,
            "anchor_high": anchor_high,
        }
        logger.info(
            f"  {symbol}: anchor {anchor_date} Low=${anchor_low:.4f} High=${anchor_high:.4f}"
        )

        for gap in gap_events:
            date_str = str(gap["date"])
            df_15s = self._load_15s_data(symbol, date_str)
            if df_15s is None:
                logger.debug(f"  {symbol} {date_str}: no 15-sec data")
                continue

            if len(df_15s) < 50:
                continue

            self._simulate_gap_day(symbol, gap, df_15s, dual, anchor_info)

    def _simulate_gap_day(
        self,
        symbol: str,
        gap: dict,
        df_15s: pd.DataFrame,
        dual: DualFibSeries,
        anchor_info: dict,
    ) -> None:
        """Walk 15-sec bars for one gap day with double-touch logic."""
        gap_pct = float(gap["gap_pct"])
        prev_close = float(gap["prev_close"])
        date_str = str(gap["date"])
        gap_volume = float(gap.get("gap_volume", 0))

        # ── Filter: minimum gap % (need real momentum) ──
        if gap_pct < FIB_DT_GAP_MIN_PCT:
            logger.debug(f"  {symbol} {date_str}: gap {gap_pct:.1f}% < min {FIB_DT_GAP_MIN_PCT}%, skipping")
            return

        # ── Filter: skip extreme gaps (poor win rate above 50%) ──
        if gap_pct > FIB_DT_GAP_MAX_PCT:
            logger.debug(f"  {symbol} {date_str}: gap {gap_pct:.1f}% > max {FIB_DT_GAP_MAX_PCT}%, skipping")
            return

        # ── Filter: minimum volume (filter illiquid stocks) ──
        if gap_volume < FIB_DT_MIN_GAP_VOLUME:
            logger.debug(f"  {symbol} {date_str}: volume {gap_volume:,.0f} < min {FIB_DT_MIN_GAP_VOLUME:,}, skipping")
            return

        # Find the high of the day for fib range
        day_high = float(df_15s["high"].max())
        if day_high <= prev_close * 1.05:
            return

        # Compute fib levels
        fib_prices = _get_all_fib_prices(dual, day_high)
        fib_price_info = _get_fib_price_info(dual, day_high)

        # Filter to reasonable range
        range_low = prev_close * 0.3
        range_high = day_high * 2.5
        fib_prices = [p for p in fib_prices if range_low <= p <= range_high]

        if len(fib_prices) < 3:
            return

        logger.info(
            f"  {symbol} {date_str} (gap +{gap_pct:.1f}%) "
            f"| {len(fib_prices)} fibs | {len(df_15s)} bars"
        )

        # Initialize double-touch state for each fib level
        touch_states: dict[int, TouchState] = {}
        for idx, fp in enumerate(fib_prices):
            touch_states[idx] = TouchState(fib_price=fp, fib_idx=idx)

        entries_today = 0
        signal_pending = False
        signal_fib_level = 0.0
        signal_fib_idx = -1
        signal_touch1_bar = -1
        signal_touch2_bar = -1

        # Reset position state for this day
        self._half_sold = False
        self._remaining_qty = 0
        self._target_price = 0.0
        self._stop_price = 0.0
        self._fib_support_price = 0.0
        self._prev_bar_low = 0.0
        self._prev_bar_high = 0.0

        # Pre-compute entry window (ET)
        _et_tz = pytz.timezone("US/Eastern")
        _sh, _sm = map(int, FIB_DT_ENTRY_WINDOW_START.split(":"))
        _entry_start = dt_time(_sh, _sm)
        _h, _m = map(int, FIB_DT_ENTRY_WINDOW_END.split(":"))
        _entry_cutoff = dt_time(_h, _m)

        for i in range(len(df_15s)):
            bar = df_15s.iloc[i]
            bar_time = df_15s.index[i]
            bar_open = float(bar["open"])
            bar_high = float(bar["high"])
            bar_low = float(bar["low"])
            bar_close = float(bar["close"])

            cur_bar = {
                "open": bar_open, "high": bar_high,
                "low": bar_low, "close": bar_close,
            }

            # ── Check exits for open position ──
            if self._position and self._position.symbol == symbol:
                self._check_exits(
                    symbol, bar_open, bar_high, bar_low, bar_close,
                    bar_time, cur_bar, gap, df_15s,
                    fib_prices, fib_price_info, anchor_info,
                )
                self._prev_bar_low = bar_low
                self._prev_bar_high = bar_high
                if self._position is None:
                    # Position closed — keep scanning
                    pass
                else:
                    continue

            # ── Fill pending entry signal ──
            if signal_pending:
                signal_pending = False

                if entries_today >= FIB_DT_MAX_ENTRIES_PER_DAY:
                    continue

                fill_price = round(bar_open * (1 + SLIPPAGE_PCT), 4)
                fib_level = signal_fib_level

                # Stop = 3% below fib support
                stop_price = round(fib_level * (1 - FIB_DT_STOP_PCT), 4)

                # Target = 3rd fib level above entry
                target_price = self._find_target(fib_prices, signal_fib_idx, fill_price)

                qty = int((self._cash * POSITION_SIZE_PCT) / fill_price)
                if qty <= 0:
                    continue

                commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                cost = qty * fill_price + commission
                if cost > self._cash:
                    qty = int((self._cash - MIN_COMMISSION) / fill_price)
                    if qty <= 0:
                        continue
                    commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                    cost = qty * fill_price + commission

                self._cash -= cost

                fib_key = round(fib_level, 4)
                fib_ratio, fib_series = fib_price_info.get(fib_key, (0, "?"))

                fib_info = {
                    "fib_level": round(fib_level, 4),
                    "fib_idx": signal_fib_idx,
                    "fib_ratio": fib_ratio,
                    "fib_series": fib_series,
                    "stop_price": stop_price,
                    "target_price": target_price,
                    "gap_pct": gap_pct,
                    "touch1_bar": signal_touch1_bar,
                    "touch2_bar": signal_touch2_bar,
                    "entry_bar": cur_bar,
                    **(anchor_info or {}),
                }

                self._position = SimPosition(
                    symbol=symbol, quantity=qty,
                    entry_price=fill_price, entry_time=bar_time,
                    signal_price=bar_open,
                    stop_price=stop_price,
                    trailing_stop_price=0,
                    entry_commission=commission,
                    fib_info=fib_info,
                )

                self._half_sold = False
                self._remaining_qty = qty
                self._target_price = target_price
                self._stop_price = stop_price
                self._fib_support_price = fib_level
                self._prev_bar_low = bar_low
                self._prev_bar_high = bar_high

                entries_today += 1

                logger.info(
                    f"  ENTRY: {symbol} {qty}sh @ ${fill_price:.4f} "
                    f"(double-touch @ fib ${fib_level:.4f}) -> "
                    f"stop ${stop_price:.4f}, target ${target_price:.4f}"
                )
                self._trade_logger.log_entry(
                    symbol=symbol, quantity=qty, price=fill_price,
                    stop_price=stop_price,
                    notes=(
                        f"fib_double_touch gap={gap_pct:+.1f}% "
                        f"fib=${fib_level:.4f} target=${target_price:.4f}"
                    ),
                )

                # Check immediate stop on entry bar
                if bar_low <= stop_price:
                    fill = min(bar_open, stop_price)
                    fill = max(fill, 0.01)
                    self._exit_full_position(
                        symbol, fill, bar_time,
                        f"stop_loss (immediate, stop=${stop_price:.4f})",
                        cur_bar, gap, df_15s, fib_prices, fib_price_info, anchor_info,
                    )
                continue

            # ── Double-touch scanning ──
            # Time window filter: no new entries after cutoff (edge disappears)
            try:
                _bar_et = bar_time.astimezone(_et_tz).time()
            except Exception:
                _bar_et = bar_time.time() if hasattr(bar_time, 'time') else dt_time(0, 0)
            if _bar_et < _entry_start or _bar_et >= _entry_cutoff:
                self._prev_bar_low = bar_low
                self._prev_bar_high = bar_high
                continue

            if entries_today >= FIB_DT_MAX_ENTRIES_PER_DAY:
                self._prev_bar_low = bar_low
                continue

            # Update touch states for each fib level
            for idx, ts in touch_states.items():
                fp = ts.fib_price

                # Only track support levels (below current price)
                if fp > bar_close:
                    continue

                proximity = fp * FIB_DT_PROXIMITY_PCT if fp > 0 else 0.01

                if ts.phase == "IDLE":
                    # Check if bar_low is within proximity of fib level
                    if abs(bar_low - fp) <= proximity:
                        ts.phase = "FIRST_TOUCH"
                        ts.first_touch_bar = i
                        logger.debug(
                            f"  {symbol}: FIRST_TOUCH fib ${fp:.4f} at bar {i}"
                        )

                elif ts.phase == "FIRST_TOUCH":
                    # Check if price has bounced away (low > fib + 0.5%)
                    bounce_threshold = fp * 1.005
                    if bar_low > bounce_threshold:
                        ts.phase = "BOUNCED"
                        logger.debug(
                            f"  {symbol}: BOUNCED from fib ${fp:.4f} at bar {i}"
                        )

                elif ts.phase == "BOUNCED":
                    # Check for second touch (within proximity, >= min bars since first)
                    bars_since_first = i - ts.first_touch_bar
                    if bars_since_first >= FIB_DT_MIN_BOUNCE_BARS:
                        if abs(bar_low - fp) <= proximity:
                            # Fib ratio + series filter
                            fib_key = round(fp, 4)
                            ratio, _series = fib_price_info.get(fib_key, (None, "?"))

                            # S1-only filter
                            if FIB_DT_S1_ONLY and _series == "S2":
                                logger.debug(
                                    f"  {symbol}: double-touch at fib ${fp:.4f} "
                                    f"series=S2, skipping (S1 only)"
                                )
                                ts.phase = "IDLE"
                                ts.first_touch_bar = -1
                                continue

                            # Ratio filter: only enter on high-WR ratios
                            if FIB_DT_USE_RATIO_FILTER:
                                if ratio is not None and ratio not in FIB_DT_PREFERRED_RATIOS:
                                    logger.debug(
                                        f"  {symbol}: double-touch at fib ${fp:.4f} "
                                        f"ratio={ratio} NOT in preferred set, skipping"
                                    )
                                    ts.phase = "IDLE"
                                    ts.first_touch_bar = -1
                                    continue

                            # DOUBLE TOUCH — signal entry on next bar
                            signal_pending = True
                            signal_fib_level = fp
                            signal_fib_idx = ts.fib_idx
                            signal_touch1_bar = ts.first_touch_bar
                            signal_touch2_bar = i

                            logger.info(
                                f"  DOUBLE-TOUCH: {symbol} fib ${fp:.4f} "
                                f"(touch1=bar{ts.first_touch_bar}, touch2=bar{i}, "
                                f"gap={bars_since_first} bars)"
                            )

                            # Reset this level's state
                            ts.phase = "IDLE"
                            ts.first_touch_bar = -1
                            break  # one signal per bar

            self._prev_bar_low = bar_low
            self._prev_bar_high = bar_high

        # End of day: force close any open position
        if self._position and self._position.symbol == symbol:
            last_bar = df_15s.iloc[-1]
            eod_bar = {
                "open": float(last_bar["open"]),
                "high": float(last_bar["high"]),
                "low": float(last_bar["low"]),
                "close": float(last_bar["close"]),
            }
            self._exit_full_position(
                symbol, eod_bar["close"], df_15s.index[-1], "eod_close",
                eod_bar, gap, df_15s, fib_prices, fib_price_info, anchor_info,
            )

    # ── Exit Logic (split position) ──────────────────────

    def _check_exits(
        self, symbol: str, bar_open: float, bar_high: float,
        bar_low: float, bar_close: float, bar_time,
        cur_bar: dict, gap: dict, df_15s: pd.DataFrame,
        fib_prices: list, fib_price_info: dict, anchor_info: dict,
    ) -> None:
        """Check stop loss, target hit (50%), and trailing stop (remaining 50%)."""
        pos = self._position
        if pos is None:
            return

        # ── Stop loss (full remaining position) ──
        if bar_low <= self._stop_price:
            fill = min(bar_open, self._stop_price)
            fill = max(fill, 0.01)
            self._exit_full_position(
                symbol, fill, bar_time,
                f"stop_loss (stop=${self._stop_price:.4f})",
                cur_bar, gap, df_15s, fib_prices, fib_price_info, anchor_info,
            )
            return

        if not self._half_sold:
            # ── Target hit: sell 50% ──
            if self._target_price > 0 and bar_high >= self._target_price:
                half_qty = self._remaining_qty // 2
                if half_qty > 0:
                    fill_price = self._target_price
                    commission = max(half_qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                    self._cash += half_qty * fill_price - commission

                    self._half_sold = True
                    self._remaining_qty -= half_qty

                    # Record partial exit info
                    pos.fib_info["partial_exit_price"] = fill_price
                    pos.fib_info["partial_exit_time"] = str(bar_time)
                    pos.fib_info["partial_exit_qty"] = half_qty
                    pos.fib_info["partial_commission"] = commission

                    logger.info(
                        f"  TARGET HIT: {symbol} sold {half_qty}sh @ ${fill_price:.4f} "
                        f"(50% at target), {self._remaining_qty}sh remaining"
                    )
        else:
            # ── No-new-high exit on remaining 50% ──
            # Exit if current bar's high doesn't exceed previous bar's high
            # Fill at bar close (≈ 4 sec before bar end on 15s bars)
            if self._prev_bar_high > 0 and bar_high <= self._prev_bar_high:
                fill = bar_close
                fill = max(fill, 0.01)
                self._exit_remaining_position(
                    symbol, fill, bar_time,
                    f"no_new_high (prev_high=${self._prev_bar_high:.4f}, bar_high=${bar_high:.4f})",
                    cur_bar, gap, df_15s, fib_prices, fib_price_info, anchor_info,
                )

    def _exit_full_position(
        self, symbol: str, price: float, bar_time, reason: str,
        exit_bar: dict, gap: dict, df_15s: pd.DataFrame,
        fib_prices: list, fib_price_info: dict, anchor_info: dict,
    ) -> None:
        """Exit full remaining position."""
        pos = self._position
        if pos is None:
            return

        qty = self._remaining_qty
        commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
        self._cash += qty * price - commission

        pos.fib_info["exit_bar"] = exit_bar
        trade_dict = self._record_trade(pos, price, bar_time, reason, commission, qty)

        if trade_dict and df_15s is not None:
            self._trade_charts_data.append({
                "trade": trade_dict,
                "day_df": df_15s.copy(),
                "fib_prices": fib_prices,
                "fib_price_info": fib_price_info or {},
                "anchor_info": anchor_info or {},
                "gap": gap,
            })

        self._position = None
        self._half_sold = False
        self._remaining_qty = 0

    def _exit_remaining_position(
        self, symbol: str, price: float, bar_time, reason: str,
        exit_bar: dict, gap: dict, df_15s: pd.DataFrame,
        fib_prices: list, fib_price_info: dict, anchor_info: dict,
    ) -> None:
        """Exit remaining position after 50% target was hit."""
        pos = self._position
        if pos is None:
            return

        qty = self._remaining_qty
        commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
        self._cash += qty * price - commission

        pos.fib_info["exit_bar"] = exit_bar
        trade_dict = self._record_trade(pos, price, bar_time, reason, commission, qty)

        if trade_dict and df_15s is not None:
            self._trade_charts_data.append({
                "trade": trade_dict,
                "day_df": df_15s.copy(),
                "fib_prices": fib_prices,
                "fib_price_info": fib_price_info or {},
                "anchor_info": anchor_info or {},
                "gap": gap,
            })

        self._position = None
        self._half_sold = False
        self._remaining_qty = 0

    # ── Target Finder ────────────────────────────────────

    def _find_target(self, fib_prices: list[float], fib_idx: int, entry_price: float) -> float:
        """Find the Nth fib level above entry price."""
        levels_above = 0
        for fp in fib_prices:
            if fp > entry_price:
                levels_above += 1
                if levels_above >= FIB_DT_TARGET_LEVELS:
                    return fp
        # Fallback: 5% above entry
        return round(entry_price * 1.05, 4)

    # ── Trade Recording ──────────────────────────────────

    def _record_trade(
        self, pos: SimPosition, exit_price: float, exit_time,
        exit_reason: str, exit_commission: float, exit_qty: int,
    ) -> dict:
        fi = pos.fib_info or {}
        entry_bar = fi.get("entry_bar", {})
        exit_bar = fi.get("exit_bar", {})

        # Calculate P&L accounting for partial exits
        partial_price = fi.get("partial_exit_price", 0)
        partial_qty = fi.get("partial_exit_qty", 0)
        partial_comm = fi.get("partial_commission", 0)

        total_exit_qty = exit_qty + partial_qty
        total_exit_comm = exit_commission + partial_comm

        if partial_qty > 0:
            # Blended exit: partial at target + remaining at final price
            total_proceeds = (partial_qty * partial_price) + (exit_qty * exit_price)
            avg_exit = total_proceeds / total_exit_qty if total_exit_qty > 0 else exit_price
        else:
            avg_exit = exit_price
            total_proceeds = exit_qty * exit_price

        total_commission = pos.entry_commission + total_exit_comm
        pnl_gross = (avg_exit - pos.entry_price) * pos.quantity
        pnl_net = pnl_gross - total_commission

        trade_dict = {
            "symbol": pos.symbol,
            "quantity": pos.quantity,
            "entry_fill": pos.entry_price,
            "entry_time": str(pos.entry_time),
            "exit_fill": round(avg_exit, 4),
            "exit_time": str(exit_time),
            "exit_reason": exit_reason,
            "pnl_gross": round(pnl_gross, 2),
            "pnl_net": round(pnl_net, 2),
            "pnl_pct": f"{(avg_exit - pos.entry_price) / pos.entry_price:.2%}" if pos.entry_price > 0 else "0.00%",
            "commission": round(total_commission, 2),
            "move_pct": f"{fi.get('gap_pct', 0):+.1f}%",
            "fib_level": fi.get("fib_level", ""),
            "fib_ratio": fi.get("fib_ratio", ""),
            "fib_series": fi.get("fib_series", ""),
            "stop_price": fi.get("stop_price", ""),
            "target_price": fi.get("target_price", ""),
            "partial_exit_price": fi.get("partial_exit_price", ""),
            "partial_exit_qty": fi.get("partial_exit_qty", ""),
            "touch1_bar": fi.get("touch1_bar", ""),
            "touch2_bar": fi.get("touch2_bar", ""),
            "anchor_date": fi.get("anchor_date", ""),
            "anchor_low": fi.get("anchor_low", ""),
            "anchor_high": fi.get("anchor_high", ""),
            "entry_bar_O": entry_bar.get("open", ""),
            "entry_bar_H": entry_bar.get("high", ""),
            "entry_bar_L": entry_bar.get("low", ""),
            "entry_bar_C": entry_bar.get("close", ""),
            "exit_bar_O": exit_bar.get("open", ""),
            "exit_bar_H": exit_bar.get("high", ""),
            "exit_bar_L": exit_bar.get("low", ""),
            "exit_bar_C": exit_bar.get("close", ""),
        }

        self._result.trades.append(trade_dict)
        self._result.total_trades += 1
        self._result.total_pnl_gross += pnl_gross
        self._result.total_pnl_net += pnl_net
        self._result.total_commissions += total_commission

        if pnl_net >= 0:
            self._result.winning_trades += 1
        else:
            self._result.losing_trades += 1

        equity = self._cash
        if equity > self._peak_equity:
            self._peak_equity = equity
        dd = (self._peak_equity - equity) / self._peak_equity if self._peak_equity > 0 else 0
        if dd > self._result.max_drawdown:
            self._result.max_drawdown = dd
        self._result.equity_curve.append({"time": str(exit_time), "equity": round(equity, 2)})

        sign = "+" if pnl_net >= 0 else ""
        logger.info(
            f"  TRADE CLOSED: {pos.symbol} {pos.quantity}sh "
            f"${pos.entry_price:.4f} -> ${avg_exit:.4f} "
            f"P&L={sign}${pnl_net:.2f} ({exit_reason})"
        )
        self._trade_logger.log_exit(
            symbol=pos.symbol, quantity=pos.quantity,
            price=avg_exit, entry_price=pos.entry_price,
            entry_time=str(pos.entry_time),
            notes=f"fib_double_touch {exit_reason} net=${pnl_net:.2f}",
        )
        return trade_dict

    # ── Summary & Output ─────────────────────────────────

    def _print_summary(self) -> None:
        r = self._result
        wr = r.winning_trades / r.total_trades * 100 if r.total_trades > 0 else 0
        equity = self._cash

        print(f"\n{'='*60}")
        print(f"Fibonacci Double-Touch Backtest (15-sec)")
        print(f"  Double-touch at fib support | {FIB_DT_STOP_PCT:.0%} stop | "
              f"{FIB_DT_TARGET_LEVELS} fib target (50%) + no-new-high exit (50%)")
        print(f"  Filters: gap {FIB_DT_GAP_MIN_PCT}-{FIB_DT_GAP_MAX_PCT}% | "
              f"min vol {FIB_DT_MIN_GAP_VOLUME:,} | "
              f"entries {FIB_DT_ENTRY_WINDOW_START}-{FIB_DT_ENTRY_WINDOW_END} ET | "
              f"ratio filter={'ON' if FIB_DT_USE_RATIO_FILTER else 'OFF'} | "
              f"S1 only={'ON' if FIB_DT_S1_ONLY else 'OFF'}")
        print(f"{'='*60}")
        print(f"  Gap events:    {len(self._load_gappers())}")
        print(f"  Symbols:       {len(r.symbols_tested)}")
        print(f"  Total trades:  {r.total_trades}")
        print(f"  Winning:       {r.winning_trades}")
        print(f"  Losing:        {r.losing_trades}")
        print(f"  Win rate:      {wr:.1f}%")
        print(f"  Gross P&L:     ${r.total_pnl_gross:+,.2f}")
        print(f"  Net P&L:       ${r.total_pnl_net:+,.2f}")
        print(f"  Commissions:   ${r.total_commissions:,.2f}")
        print(f"  Max Drawdown:  {r.max_drawdown:.2%}")
        print(f"  Final Equity:  ${equity:,.2f}")

        if r.trades:
            print(f"\n{'─'*100}")
            print(f"  DETAILED TRADES ({len(r.trades)} total)")
            print(f"{'─'*100}")
            for i, t in enumerate(r.trades, 1):
                pnl = t["pnl_net"]
                sign = "+" if pnl >= 0 else ""
                result_tag = "WIN " if pnl >= 0 else "LOSS"

                print(f"\n  #{i} {result_tag} {t['symbol']} | Gap {t['move_pct']}")
                print(f"    Entry: {t['entry_time'][:19]}  Fill=${t['entry_fill']:.4f}")
                print(f"    Exit:  {t['exit_time'][:19]}  Fill=${t['exit_fill']:.4f}")
                print(f"    Fib: level=${t.get('fib_level', '?')}  ratio={t.get('fib_ratio', '?')}  "
                      f"series={t.get('fib_series', '?')}")
                print(f"    Stop=${t.get('stop_price', '?')}  Target=${t.get('target_price', '?')}")
                if t.get("partial_exit_price"):
                    print(f"    Partial exit: {t['partial_exit_qty']}sh @ ${t['partial_exit_price']:.4f}")
                print(f"    P&L: {sign}${pnl:.2f} ({t['pnl_pct']})  Reason: {t['exit_reason']}")

        csv_path = DATA_DIR / "fib_double_touch_trades.csv"
        chart_path = FIB_CHARTS_DIR / "fib_double_touch_report.html"
        print(f"\nTrades:  {csv_path}")
        if chart_path.exists():
            print(f"Charts:  {chart_path}")

    def _save_trades_csv(self) -> None:
        if not self._result.trades:
            return
        csv_path = DATA_DIR / "fib_double_touch_trades.csv"
        columns = [
            "symbol", "entry_time", "exit_time", "quantity",
            "entry_fill", "exit_fill", "pnl_gross", "pnl_net", "pnl_pct",
            "exit_reason", "commission", "move_pct",
            "fib_level", "fib_ratio", "fib_series",
            "stop_price", "target_price",
            "partial_exit_price", "partial_exit_qty",
            "touch1_bar", "touch2_bar",
            "anchor_date", "anchor_low", "anchor_high",
            "entry_bar_O", "entry_bar_H", "entry_bar_L", "entry_bar_C",
            "exit_bar_O", "exit_bar_H", "exit_bar_L", "exit_bar_C",
        ]
        df = pd.DataFrame(self._result.trades)
        for col in columns:
            if col not in df.columns:
                df[col] = ""
        df[columns].to_csv(csv_path, index=False)
        logger.info(f"Saved trades to {csv_path}")

    def _write_live_state(self) -> None:
        try:
            equity = self._cash
            state = {
                "timestamp": now_utc().isoformat(),
                "simulation_mode": True,
                "strategy": "fib_double_touch",
                "market_open": False,
                "premarket": False,
                "afterhours": False,
                "positions": [],
                "risk_status": {
                    "portfolio_value": round(equity, 2),
                    "daily_pnl": round(self._result.total_pnl_net, 2),
                    "daily_trades": self._result.total_trades,
                },
                "simulation_results": {
                    "total_trades": self._result.total_trades,
                    "winning_trades": self._result.winning_trades,
                    "losing_trades": self._result.losing_trades,
                    "total_pnl_gross": round(self._result.total_pnl_gross, 2),
                    "total_pnl": round(self._result.total_pnl_net, 2),
                    "total_commissions": round(self._result.total_commissions, 2),
                    "win_rate": round(
                        self._result.winning_trades / self._result.total_trades, 4
                    ) if self._result.total_trades > 0 else 0,
                    "max_drawdown": round(self._result.max_drawdown, 4),
                    "final_equity": round(equity, 2),
                    "symbols_tested": self._result.symbols_tested,
                },
                "equity_curve": self._result.equity_curve[-200:],
                "recent_trades": self._result.trades[-50:],
            }
            LIVE_STATE_PATH.write_text(json.dumps(state, default=str, indent=2))
        except Exception as e:
            logger.error(f"Failed to write state: {e}")

    def get_chart_data(self) -> list[dict]:
        """Return collected trade chart data for the chart generator."""
        return self._trade_charts_data


async def run_fib_double_touch_backtest() -> SimResult:
    """Entry point for the double-touch fib backtest."""
    engine = FibDoubleTouchEngine()
    return engine.run()
