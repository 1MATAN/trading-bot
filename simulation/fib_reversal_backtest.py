"""Fibonacci Retracement + Reversal Candle Backtest.

Strategy (intraday pullback + reversal candle):
  1. Scan for hot gappers (>=20% gap)
  2. Build fibonacci levels from 5y daily anchor candle
  3. Same day only: resample 2-min -> 5-min bars
  4. Entry: reversal candle (hammer/bullish engulfing) at fib support,
     price above 50% of day's range -> enter NEXT bar open
  5. Exit: 3% trailing stop from highest high
  6. Force close at 15:00 ET
"""

import json
import logging
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

from config.settings import (
    STARTING_CAPITAL,
    POSITION_SIZE_PCT,
    SCAN_PRICE_MIN,
    SCAN_PRICE_MAX,
    SLIPPAGE_PCT,
    COMMISSION_PER_SHARE,
    MIN_COMMISSION,
    LIVE_STATE_PATH,
    DATA_DIR,
    BACKTEST_DATA_DIR,
    FIB_MAX_ENTRIES_PER_DAY,
    FIB_WARMUP_BARS,
    FIB_ENTRY_WINDOW_END,
    FIB_LOOKBACK_YEARS,
    FIB_GAP_LOOKBACK_DAYS,
    FIB_CHARTS_DIR,
)
from simulation.sim_engine import SimPosition, SimTrade, SimResult
from strategies.fibonacci_engine import (
    find_anchor_candle,
    build_dual_series,
    advance_series,
    DualFibSeries,
)
from notifications.trade_logger import TradeLogger
from utils.time_utils import now_utc

logger = logging.getLogger("trading_bot.fib_reversal")

# ── Strategy settings ──────────────────────────────────────
GAP_MIN_PCT = 20.0                # minimum gap to qualify (hot gappers)
TRAILING_STOP_PCT = 0.03          # 3% trailing stop


# ── Reversal Candle Detection ─────────────────────────────

def _is_hammer(row) -> bool:
    """Hammer: lower wick >= 2x body, bullish close, body <= 40% of range."""
    o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
    rng = h - l
    if rng <= 0:
        return False
    body = abs(c - o)
    lower_wick = min(o, c) - l
    return c >= o and lower_wick >= 2 * body and body <= 0.4 * rng


def _is_bullish_engulfing(curr, prev) -> bool:
    """Bullish engulfing: prev bearish, curr bullish, curr body engulfs prev body."""
    p_o, p_c = float(prev["open"]), float(prev["close"])
    c_o, c_c = float(curr["open"]), float(curr["close"])
    if p_c >= p_o:
        return False
    if c_c <= c_o:
        return False
    return c_o <= p_c and c_c >= p_o


def _detect_reversal_candle(df_5m: pd.DataFrame, i: int) -> tuple[bool, str]:
    """Check if bar at index i is a reversal candle."""
    row = df_5m.iloc[i]
    if _is_hammer(row):
        return True, "hammer"
    if i >= 1:
        prev = df_5m.iloc[i - 1]
        if _is_bullish_engulfing(row, prev):
            return True, "bullish_engulfing"
    return False, ""


def _resample_to_5min(df_2m: pd.DataFrame) -> pd.DataFrame:
    """Resample 2-min OHLCV to 5-min bars."""
    if df_2m.empty:
        return df_2m
    return df_2m.resample("5min").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna()


# ── Fibonacci helpers ──────────────────────────────────────

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


def _find_nearest_fib_below(fib_prices: list[float], price: float) -> int:
    """Find index of the nearest fib level AT or BELOW price. Returns -1 if none."""
    best_idx = -1
    for i, fp in enumerate(fib_prices):
        if fp <= price:
            best_idx = i
        else:
            break
    return best_idx


# ── Main Engine ──────────────────────────────────────────────

class FibReversalEngine:
    """Fib retracement + reversal candle at fib support, same-day 5-min bars."""

    def __init__(
        self,
        symbols: Optional[list[str]] = None,
        capital: float = STARTING_CAPITAL,
    ) -> None:
        self._symbols = symbols or []
        self._capital = capital
        self._cash = capital
        self._position: Optional[SimPosition] = None
        self._result = SimResult()
        self._trade_logger = TradeLogger()
        self._peak_equity = capital
        self._gappers: list[dict] = []
        self._trade_charts_data: list[dict] = []
        self._float_cache: dict[str, float] = {}
        self._warmup_bars = FIB_WARMUP_BARS

        h, m = FIB_ENTRY_WINDOW_END.split(":")
        self._entry_window_end = time(int(h), int(m))

    # ── Public API ────────────────────────────────────────

    def run(self) -> SimResult:
        logger.info(f"Fib Reversal Backtest starting: capital=${self._capital:,.2f}")
        logger.info(
            f"  Gap: >={GAP_MIN_PCT:.0f}% | Reversal candles (hammer/engulf) at fib support | "
            f"Same-day only | 50% filter | {TRAILING_STOP_PCT:.1%} trailing stop"
        )

        if not self._symbols:
            self._symbols = self._scan_for_candidates()
        if not self._symbols:
            logger.warning("No symbols to backtest")
            return self._result

        self._result.symbols_tested = list(self._symbols)
        self._find_gappers()

        if not self._gappers:
            logger.warning("No gap events found")
            return self._result

        gap_by_symbol: dict[str, list[dict]] = {}
        for g in self._gappers:
            gap_by_symbol.setdefault(g["symbol"], []).append(g)

        for symbol in self._symbols:
            if symbol not in gap_by_symbol:
                continue
            try:
                self._simulate_symbol(symbol, gap_by_symbol[symbol])
            except Exception as e:
                logger.error(f"Fib reversal error for {symbol}: {e}", exc_info=True)

        self._write_live_state()
        self._generate_charts()
        self._print_summary()
        return self._result

    # ── Gapper Detection (cache-first) ───────────────────

    def _find_gappers(self) -> None:
        # Load any cached gappers first
        csv_path = BACKTEST_DATA_DIR / "gappers_20pct.csv"
        cached_symbols = set()
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                if not df.empty:
                    sym_set = set(self._symbols)
                    for _, row in df.iterrows():
                        sym = row["symbol"]
                        if sym not in sym_set:
                            continue
                        gap_pct = float(row["gap_pct"])
                        if gap_pct < GAP_MIN_PCT:
                            continue
                        cached_symbols.add(sym)
                        self._gappers.append({
                            "symbol": sym,
                            "date": str(row["date"]),
                            "date_obj": pd.Timestamp(row["date"]),
                            "gap_pct": round(gap_pct, 1),
                            "prev_close": round(float(row["prev_close"]), 4),
                            "open_price": round(float(row["open_price"]), 4),
                            "high_price": round(float(row["high_price"]), 4),
                            "gap_volume": float(row.get("gap_volume", 0)),
                        })
                    if cached_symbols:
                        logger.info(
                            f"Loaded {len(self._gappers)} gappers from cache "
                            f"(>={GAP_MIN_PCT:.0f}%) across {len(cached_symbols)} symbols"
                        )
            except Exception as e:
                logger.debug(f"Failed to load cached gappers: {e}")

        # Scan remaining symbols via yfinance
        remaining = [s for s in self._symbols if s not in cached_symbols]
        if not remaining:
            return
        logger.info(f"Scanning {len(remaining)} symbols for gappers (>={GAP_MIN_PCT:.0f}%) via yfinance...")
        try:
            batch = yf.download(
                remaining, period=f"{FIB_GAP_LOOKBACK_DAYS}d", interval="1d",
                group_by="ticker", progress=False, threads=True,
            )
        except Exception as e:
            logger.error(f"Batch daily download failed: {e}")
            return
        if batch.empty:
            return

        single_symbol = len(remaining) == 1
        for sym in remaining:
            try:
                if single_symbol:
                    df_sym = batch.copy()
                elif isinstance(batch.columns, pd.MultiIndex):
                    if sym not in batch.columns.get_level_values(0):
                        continue
                    df_sym = batch[sym].dropna(how="all")
                else:
                    df_sym = batch.dropna(how="all")
                if len(df_sym) < 2:
                    continue
                if isinstance(df_sym.columns, pd.MultiIndex):
                    df_sym.columns = [c[0].lower() for c in df_sym.columns]
                else:
                    df_sym.columns = [c.lower() for c in df_sym.columns]

                prev_closes = df_sym["close"].shift(1)
                for idx in range(1, len(df_sym)):
                    row = df_sym.iloc[idx]
                    prev_close = float(prev_closes.iloc[idx])
                    if pd.isna(prev_close) or prev_close <= 0:
                        continue
                    if prev_close < SCAN_PRICE_MIN or prev_close > SCAN_PRICE_MAX:
                        continue
                    open_price = float(row["open"])
                    high_price = float(row["high"])
                    effective_gap = max(
                        (open_price - prev_close) / prev_close * 100,
                        (high_price - prev_close) / prev_close * 100,
                    )
                    if effective_gap >= GAP_MIN_PCT:
                        gap_vol = float(row["volume"]) if not pd.isna(row.get("volume", float("nan"))) else 0
                        date = df_sym.index[idx]
                        self._gappers.append({
                            "symbol": sym,
                            "date": date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date),
                            "date_obj": date,
                            "gap_pct": round(effective_gap, 1),
                            "prev_close": round(prev_close, 4),
                            "open_price": round(open_price, 4),
                            "high_price": round(high_price, 4),
                            "gap_volume": gap_vol,
                        })
            except Exception as e:
                logger.debug(f"Gapper scan failed for {sym}: {e}")

        logger.info(
            f"Gapper Scan: found {len(self._gappers)} events (>={GAP_MIN_PCT:.0f}%) "
            f"across {len(set(g['symbol'] for g in self._gappers))} symbols"
        )

    # ── Symbol Scanning ──────────────────────────────────

    def _scan_for_candidates(self) -> list[str]:
        """Pre-market / after-hours movers that gained 20%+ in last 30 days."""
        symbols = [
            "BZAI", "VIVS", "ANL", "LRHC", "MGLD", "RPGL", "VTIX", "SER",
            "XPON", "GOAI", "VENU", "BHAT", "DXF", "RUBI", "SOC", "VHUB",
            "CISS", "EVTV", "YJ", "CCHH", "ELAB", "MNTN", "STIM", "PRCH",
            "FSLY", "GDTC", "DHX", "CCTG", "MBOT", "ABP", "LXEH", "JZXN",
            "QNST", "LIMN", "CYCN", "AEVA", "PRFX", "ACCL", "VERO", "JAGX",
            "IBRX", "IMG", "ACRV", "ECDA", "DVLT", "SIDU", "KZIA", "AIMD",
            "KUST", "BKYI", "GCTS", "FATBB", "ISPC", "BARK", "LHSW",
        ]
        logger.info(f"Scanner universe: {len(symbols)} pre/after-market 20%+ movers")
        return symbols

    # ── Float Helper (cache-first) ───────────────────────

    def _get_float_shares(self, symbol: str) -> float:
        if symbol in self._float_cache:
            return self._float_cache[symbol]
        if not self._float_cache:
            json_path = BACKTEST_DATA_DIR / "float_cache.json"
            if json_path.exists():
                try:
                    data = json.loads(json_path.read_text())
                    for sym, flt in data.items():
                        self._float_cache[sym] = float(flt)
                    if symbol in self._float_cache:
                        return self._float_cache[symbol]
                except Exception:
                    pass
        try:
            info = yf.Ticker(symbol).info
            flt = float(info.get("floatShares", 0) or 0)
            self._float_cache[symbol] = flt
            return flt
        except Exception:
            self._float_cache[symbol] = 0
            return 0

    # ── Per-Symbol Simulation ────────────────────────────

    def _simulate_symbol(self, symbol: str, gap_events: list[dict]) -> None:
        logger.info(f"\n--- {symbol}: {len(gap_events)} gap events ---")

        daily_5y = self._download_daily_5y(symbol)
        if daily_5y is None:
            return
        anchor = find_anchor_candle(daily_5y)
        if anchor is None:
            logger.warning(f"  {symbol}: no anchor candle")
            return

        anchor_low, anchor_high, anchor_date = anchor
        dual = build_dual_series(anchor_low, anchor_high)
        anchor_info = {"anchor_date": anchor_date, "anchor_low": anchor_low, "anchor_high": anchor_high}
        logger.info(
            f"  {symbol}: anchor {anchor_date} Low=${anchor_low:.4f} High=${anchor_high:.4f}"
        )

        df_intra = self._download_intraday(symbol)
        if df_intra is None:
            return
        if not hasattr(df_intra.index, "date"):
            return

        # Group intraday bars by date
        bars_by_date: dict = {}
        for idx in df_intra.index:
            d = idx.date()
            bars_by_date.setdefault(d, []).append(idx)

        for gap in gap_events:
            gap_date = gap["date_obj"]
            gd = gap_date.date() if hasattr(gap_date, "date") else pd.Timestamp(gap_date).date()

            if gd not in bars_by_date:
                continue

            # Same-day only: just the gap day's bars
            day_indices = bars_by_date[gd]
            day_df = df_intra.loc[day_indices]

            if len(day_df) < self._warmup_bars + 5:
                continue

            self._simulate_window(symbol, gap, day_df, dual, anchor_info)

    def _simulate_window(
        self, symbol: str, gap: dict, window_df: pd.DataFrame,
        dual: DualFibSeries, anchor_info: dict,
    ) -> None:
        """Walk 5-min bars on gap day: reversal candle at fib support, trailing stop."""
        prev_close = gap["prev_close"]
        gap_pct = gap["gap_pct"]
        warmup = self._warmup_bars

        # Resample 2-min -> 5-min
        df_5m = _resample_to_5min(window_df)
        if len(df_5m) < warmup + 3:
            return

        day_open = float(df_5m["open"].iloc[0])
        gap_high = float(df_5m["high"].iloc[:warmup].max())

        if gap_high <= prev_close * 1.05:
            return

        # Fib levels
        fib_prices = _get_all_fib_prices(dual, gap_high)
        fib_price_info = _get_fib_price_info(dual, gap_high)
        range_low = prev_close * 0.3
        range_high = gap_high * 2.5
        fib_prices = [p for p in fib_prices if range_low <= p <= range_high]

        if len(fib_prices) < 3:
            return

        logger.info(
            f"  {symbol} {gap['date']} (gap +{gap_pct:.1f}%) "
            f"| {len(fib_prices)} fibs | {len(df_5m)} 5m bars"
        )

        entries_total = 0

        # Signal state: reversal detected on prev bar -> enter this bar
        signal_pending = False
        signal_fib_level = 0.0
        signal_fib_idx = -1
        signal_pattern = ""
        signal_half_range = 0.0

        running_high = day_open
        highest_since_entry = 0.0
        trailing_stop = 0.0

        for i in range(warmup, len(df_5m)):
            bar = df_5m.iloc[i]
            bar_time = df_5m.index[i]
            bar_open = float(bar["open"])
            bar_high = float(bar["high"])
            bar_low = float(bar["low"])
            bar_close = float(bar["close"])

            bar_t = bar_time.time() if hasattr(bar_time, "time") else None
            cur_bar = {"open": bar_open, "high": bar_high, "low": bar_low, "close": bar_close}

            # Update running high for 50% filter
            running_high = max(running_high, bar_high)
            half_range = (day_open + running_high) / 2

            # Force close at entry window end (15:00 ET)
            if bar_t and bar_t >= self._entry_window_end:
                if self._position and self._position.symbol == symbol:
                    self._position.fib_info["highest_high"] = round(highest_since_entry, 4)
                    self._force_close(symbol, bar_close, bar_time, "eod_close", exit_bar=cur_bar,
                                      gap=gap, window_df=window_df, fib_prices=fib_prices,
                                      fib_price_info=fib_price_info, anchor_info=anchor_info)
                    highest_since_entry = 0.0
                    trailing_stop = 0.0
                break

            # ── Check exits for open position ──
            if self._position and self._position.symbol == symbol:
                highest_since_entry = max(highest_since_entry, bar_high)
                new_trail = highest_since_entry * (1 - TRAILING_STOP_PCT)
                trailing_stop = max(trailing_stop, new_trail)

                if bar_low <= trailing_stop:
                    fill_price = min(bar_open, trailing_stop)
                    fill_price = max(fill_price, 0.01)
                    self._position.fib_info["highest_high"] = round(highest_since_entry, 4)
                    self._exit_position(
                        symbol, fill_price, bar_time,
                        f"trailing_stop (trail=${trailing_stop:.4f}, peak=${highest_since_entry:.4f})",
                        gap_pct, exit_bar=cur_bar,
                        gap=gap, window_df=window_df, fib_prices=fib_prices,
                        fib_price_info=fib_price_info, anchor_info=anchor_info,
                    )
                    highest_since_entry = 0.0
                    trailing_stop = 0.0
                continue

            # ── Fill pending entry signal ──
            if signal_pending:
                signal_pending = False

                if entries_total >= FIB_MAX_ENTRIES_PER_DAY:
                    continue

                fill_price = round(bar_open * (1 + SLIPPAGE_PCT), 4)
                stop_price = round(fill_price * (1 - TRAILING_STOP_PCT), 4)

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

                fib_key = round(signal_fib_level, 4)
                fib_ratio, fib_series = fib_price_info.get(fib_key, (0, "?"))

                fib_info = {
                    "fib_level": round(signal_fib_level, 4),
                    "fib_idx": signal_fib_idx,
                    "fib_ratio": fib_ratio,
                    "fib_series": fib_series,
                    "stop_price": stop_price,
                    "gap_pct": gap_pct,
                    "reversal_pattern": signal_pattern,
                    "half_range": round(signal_half_range, 4),
                    "highest_high": 0.0,
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

                # Initialize trailing stop
                highest_since_entry = bar_high
                trailing_stop = fill_price * (1 - TRAILING_STOP_PCT)

                entries_total += 1

                logger.info(
                    f"  ENTRY: {symbol} {qty}sh @ ${fill_price:.4f} "
                    f"({signal_pattern} @ fib ${signal_fib_level:.4f}) -> "
                    f"trail_stop ${trailing_stop:.4f}"
                )
                self._trade_logger.log_entry(
                    symbol=symbol, quantity=qty, price=fill_price,
                    stop_price=trailing_stop,
                    notes=f"fib_reversal gap={gap_pct:+.1f}% {signal_pattern} fib=${signal_fib_level:.4f}",
                )

                # Check immediate trailing stop on entry bar
                if bar_low <= trailing_stop:
                    self._position.fib_info["highest_high"] = round(highest_since_entry, 4)
                    self._exit_position(
                        symbol, trailing_stop, bar_time,
                        f"trailing_stop (trail=${trailing_stop:.4f})", gap_pct,
                        exit_bar=cur_bar,
                        gap=gap, window_df=window_df, fib_prices=fib_prices,
                        fib_price_info=fib_price_info, anchor_info=anchor_info,
                    )
                    highest_since_entry = 0.0
                    trailing_stop = 0.0
                continue

            # ── Scan for reversal candle at fib support ──
            if entries_total >= FIB_MAX_ENTRIES_PER_DAY:
                continue

            # Price must be above 50% of day's range (stock still strong)
            if bar_close < half_range:
                continue

            # Detect reversal candle
            is_reversal, pattern_name = _detect_reversal_candle(df_5m, i)
            if not is_reversal:
                continue

            # Find nearest fib level to bar_low (support the wick bounced from)
            best_fib_idx = -1
            best_dist = float("inf")
            for idx, fp in enumerate(fib_prices):
                dist = abs(bar_low - fp)
                if dist < best_dist:
                    best_dist = dist
                    best_fib_idx = idx

            if best_fib_idx < 0:
                continue

            fib_level = fib_prices[best_fib_idx]
            proximity = fib_level * 0.008 if fib_level > 0 else 0.01

            if best_dist > proximity:
                continue

            # Fib must be support (at or below close)
            if fib_level > bar_close:
                continue

            # Signal detected! Enter on NEXT bar's open
            signal_pending = True
            signal_fib_level = fib_level
            signal_fib_idx = best_fib_idx
            signal_pattern = pattern_name
            signal_half_range = half_range

            logger.info(
                f"  SIGNAL: {symbol} {pattern_name} @ fib ${fib_level:.4f} "
                f"(bar_low=${bar_low:.4f}, half_range=${half_range:.4f})"
            )

        # End of day cleanup
        if self._position and self._position.symbol == symbol:
            last_bar = df_5m.iloc[-1]
            eod_bar = {
                "open": float(last_bar["open"]), "high": float(last_bar["high"]),
                "low": float(last_bar["low"]), "close": float(last_bar["close"]),
            }
            self._position.fib_info["highest_high"] = round(highest_since_entry, 4)
            self._force_close(symbol, eod_bar["close"], df_5m.index[-1], "eod_close", exit_bar=eod_bar,
                              gap=gap, window_df=window_df, fib_prices=fib_prices,
                              fib_price_info=fib_price_info, anchor_info=anchor_info)

    # ── Exit helpers ──────────────────────────────────────

    def _exit_position(
        self, symbol: str, price: float, bar_time, reason: str,
        gap_pct: float = 0, exit_bar: dict = None,
        gap: dict = None, window_df: pd.DataFrame = None,
        fib_prices: list = None, fib_price_info: dict = None,
        anchor_info: dict = None,
    ) -> None:
        pos = self._position
        if pos is None:
            return
        commission = max(pos.quantity * COMMISSION_PER_SHARE, MIN_COMMISSION)
        fill_price = price
        self._cash += pos.quantity * fill_price - commission
        logger.info(f"  EXIT: {symbol} {pos.quantity}sh @ ${fill_price:.4f} ({reason})")
        if exit_bar and pos.fib_info:
            pos.fib_info["exit_bar"] = exit_bar
        trade_dict = self._record_trade(pos, fill_price, bar_time, reason, commission, 0, price)
        # Collect chart data
        if trade_dict and window_df is not None and fib_prices and gap:
            self._trade_charts_data.append({
                "trade": trade_dict,
                "day_df": window_df.copy(),
                "fib_prices": fib_prices,
                "fib_price_info": fib_price_info or {},
                "anchor_info": anchor_info or {},
                "gap": gap,
            })
        self._position = None

    def _force_close(
        self, symbol: str, price: float, bar_time, reason: str, exit_bar: dict = None,
        gap: dict = None, window_df: pd.DataFrame = None,
        fib_prices: list = None, fib_price_info: dict = None,
        anchor_info: dict = None,
    ) -> None:
        pos = self._position
        if pos is None:
            return
        commission = max(pos.quantity * COMMISSION_PER_SHARE, MIN_COMMISSION)
        self._cash += pos.quantity * price - commission
        logger.info(f"  EXIT: {symbol} {pos.quantity}sh @ ${price:.4f} ({reason})")
        if exit_bar and pos.fib_info:
            pos.fib_info["exit_bar"] = exit_bar
        trade_dict = self._record_trade(pos, price, bar_time, reason, commission, 0, price)
        if trade_dict and window_df is not None and fib_prices and gap:
            self._trade_charts_data.append({
                "trade": trade_dict,
                "day_df": window_df.copy(),
                "fib_prices": fib_prices,
                "fib_price_info": fib_price_info or {},
                "anchor_info": anchor_info or {},
                "gap": gap,
            })
        self._position = None

    # ── Trade Recording ──────────────────────────────────

    def _record_trade(
        self, pos: SimPosition, exit_price: float, exit_time,
        exit_reason: str, exit_commission: float,
        exit_slippage: float, exit_signal_price: float,
    ) -> dict:
        total_commission = pos.entry_commission + exit_commission
        pnl_gross = (exit_price - pos.entry_price) * pos.quantity
        pnl_net = pnl_gross - total_commission

        fi = pos.fib_info or {}
        entry_bar = fi.get("entry_bar", {})
        exit_bar = fi.get("exit_bar", {})

        trade_dict = {
            "symbol": pos.symbol, "quantity": pos.quantity,
            "entry_fill": pos.entry_price,
            "entry_time": str(pos.entry_time),
            "exit_fill": exit_price,
            "exit_time": str(exit_time),
            "exit_reason": exit_reason,
            "pnl_gross": round(pnl_gross, 2), "pnl_net": round(pnl_net, 2),
            "pnl_pct": f"{(exit_price - pos.entry_price) / pos.entry_price:.2%}" if pos.entry_price > 0 else "0.00%",
            "commission": round(total_commission, 2),
            "move_pct": f"{fi.get('gap_pct', 0):+.1f}%",
            "fib_level": fi.get("fib_level", ""),
            "fib_ratio": fi.get("fib_ratio", ""),
            "fib_series": fi.get("fib_series", ""),
            "reversal_pattern": fi.get("reversal_pattern", ""),
            "half_range": fi.get("half_range", ""),
            "highest_high": fi.get("highest_high", ""),
            "stop_price": fi.get("stop_price", ""),
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
            f"${pos.entry_price:.4f} -> ${exit_price:.4f} "
            f"P&L={sign}${pnl_net:.2f} ({exit_reason})"
        )
        self._trade_logger.log_exit(
            symbol=pos.symbol, quantity=pos.quantity,
            price=exit_price, entry_price=pos.entry_price,
            entry_time=str(pos.entry_time),
            notes=f"fib_reversal {exit_reason} net=${pnl_net:.2f}",
        )
        return trade_dict

    # ── Data Download (cache-first) ──────────────────────

    def _download_daily_5y(self, symbol: str) -> Optional[pd.DataFrame]:
        cache_path = BACKTEST_DATA_DIR / f"{symbol}_daily_5y.parquet"
        if cache_path.exists():
            try:
                df = pd.read_parquet(cache_path)
                if not df.empty and len(df) >= 20:
                    logger.info(f"  {symbol}: {len(df)} daily bars (cache)")
                    return df
            except Exception:
                pass

        try:
            df = yf.download(symbol, period=f"{FIB_LOOKBACK_YEARS}y", interval="1d", progress=False)
            if df.empty or len(df) < 20:
                return None
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]
            logger.info(f"  {symbol}: {len(df)} daily bars (yfinance)")
            return df
        except Exception as e:
            logger.error(f"  {symbol}: 5y download failed: {e}")
            return None

    def _download_intraday(self, symbol: str) -> Optional[pd.DataFrame]:
        cache_path = BACKTEST_DATA_DIR / f"{symbol}_intraday_2m.parquet"
        if cache_path.exists():
            try:
                df = pd.read_parquet(cache_path)
                if not df.empty and len(df) > 50:
                    logger.info(f"  {symbol}: {len(df)} 2m bars (cache)")
                    return df
            except Exception:
                pass

        ticker = yf.Ticker(symbol)
        try:
            df = ticker.history(period=f"{FIB_GAP_LOOKBACK_DAYS}d", interval="2m", prepost=True)
            if not df.empty and len(df) > 50:
                logger.info(f"  {symbol}: {len(df)} 2m bars (yfinance)")
                df.columns = [c.lower() for c in df.columns]
                for col in ["dividends", "stock splits", "capital gains"]:
                    if col in df.columns:
                        df.drop(columns=[col], inplace=True)
                return df
        except Exception as e:
            logger.debug(f"  {symbol}: 2m download failed: {e}")
        return None

    # ── Chart Generation ─────────────────────────────────

    def _generate_charts(self) -> None:
        if not self._trade_charts_data:
            logger.info("No trades to chart")
            return
        try:
            from simulation.fib_chart_generator import generate_fib_trade_charts
            html_path = generate_fib_trade_charts(
                self._trade_charts_data, self._result, self._capital,
            )
            # Copy to strategy-specific name
            target_path = FIB_CHARTS_DIR / "fib_reversal_report.html"
            if html_path.exists():
                import shutil
                shutil.copy(html_path, target_path)
                logger.info(f"Charts saved to {target_path}")
        except Exception as e:
            logger.error(f"Chart generation failed: {e}", exc_info=True)

    # ── Summary & Output ─────────────────────────────────

    def _print_summary(self) -> None:
        r = self._result
        wr = r.winning_trades / r.total_trades * 100 if r.total_trades > 0 else 0
        equity = self._cash

        print(f"\n{'='*60}")
        print(f"Fib Reversal Candle Backtest (5m)")
        print(f"  Reversal candles (hammer/engulf) at fib support")
        print(f"  Gap: >={GAP_MIN_PCT:.0f}% | Same-day only | 50% filter | {TRAILING_STOP_PCT:.1%} trailing stop")
        print(f"{'='*60}")
        print(f"  Gap events:    {len(self._gappers)}")
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
                print(f"    Entry: {t['entry_time'][:16]}  Fill=${t['entry_fill']:.4f}"
                      + (f"  Bar O={t['entry_bar_O']:.4f} H={t['entry_bar_H']:.4f} "
                         f"L={t['entry_bar_L']:.4f} C={t['entry_bar_C']:.4f}"
                         if isinstance(t.get('entry_bar_O'), (int, float)) else ""))
                print(f"    Exit:  {t['exit_time'][:16]}  Fill=${t['exit_fill']:.4f}"
                      + (f"  Bar O={t['exit_bar_O']:.4f} H={t['exit_bar_H']:.4f} "
                         f"L={t['exit_bar_L']:.4f} C={t['exit_bar_C']:.4f}"
                         if isinstance(t.get('exit_bar_O'), (int, float)) else ""))
                print(f"    Fib: level=${t.get('fib_level', '?')}  ratio={t.get('fib_ratio', '?')}  "
                      f"series={t.get('fib_series', '?')}  "
                      f"pattern={t.get('reversal_pattern', '?')}  "
                      f"stop=${t.get('stop_price', '?')}")
                print(f"    Half range=${t.get('half_range', '?')}  "
                      f"Highest=${t.get('highest_high', '?')}")
                print(f"    P&L: {sign}${pnl:.2f} ({t['pnl_pct']})  Reason: {t['exit_reason']}")

            self._save_trades_csv()

        chart_path = FIB_CHARTS_DIR / "fib_reversal_report.html"
        print(f"\nTrades:  data/fib_reversal_trades.csv")
        if chart_path.exists():
            print(f"Charts:  {chart_path}")

    def _save_trades_csv(self) -> None:
        if not self._result.trades:
            return
        csv_path = DATA_DIR / "fib_reversal_trades.csv"
        columns = [
            "symbol", "entry_time", "exit_time", "quantity",
            "entry_fill", "exit_fill", "pnl_gross", "pnl_net", "pnl_pct",
            "exit_reason", "commission", "move_pct",
            "fib_level", "fib_ratio", "fib_series", "reversal_pattern",
            "half_range", "highest_high", "stop_price",
            "anchor_date", "anchor_low", "anchor_high",
            "entry_bar_O", "entry_bar_H", "entry_bar_L", "entry_bar_C",
            "exit_bar_O", "exit_bar_H", "exit_bar_L", "exit_bar_C",
        ]
        df = pd.DataFrame(self._result.trades)
        for col in columns:
            if col not in df.columns:
                df[col] = ""
        df[columns].to_csv(csv_path, index=False)
        logger.info(f"Saved detailed trades to {csv_path}")

    def _write_live_state(self) -> None:
        try:
            equity = self._cash
            state = {
                "timestamp": now_utc().isoformat(),
                "simulation_mode": True,
                "strategy": "fib_reversal",
                "market_open": False, "premarket": False, "afterhours": False,
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
                    "gap_events": len(self._gappers),
                },
                "equity_curve": self._result.equity_curve[-200:],
                "recent_trades": self._result.trades[-50:],
            }
            LIVE_STATE_PATH.write_text(json.dumps(state, default=str, indent=2))
        except Exception as e:
            logger.error(f"Failed to write state: {e}")


async def run_fib_reversal_backtest() -> SimResult:
    """Entry point for the fib reversal backtest."""
    engine = FibReversalEngine()
    return engine.run()
