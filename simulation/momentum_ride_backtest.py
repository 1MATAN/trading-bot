"""Momentum Ride Backtest (15-second bars, IBKR data).

Strategy:
  1. Stock gapped ≥20% (from gappers CSV)
  2. Entry: price crosses VWAP upward + price above SMA 9 on 1-hour candles
  3. Exit: 5% trailing stop from highest high since entry
  4. Safety stop: -5% from entry price
  5. Track each stock for 90 minutes from first appearance
  6. Unlimited re-entries allowed
"""

import json
import logging
from dataclasses import dataclass, field
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
    MR_STOP_EXTRA_PCT,
    MR_VWAP_MAX_DISTANCE_PCT,
    MR_TRACKING_MINUTES,
    MR_GAP_MIN_PCT,
    MR_GAP_MAX_PCT,
    MR_MIN_GAP_VOLUME,
    MR_EXIT_BARS_IN_MINUTE,
    MR_TRAILING_STOP_PCT,
    MR_SAFETY_STOP_PCT,
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

logger = logging.getLogger("trading_bot.momentum_ride")

GAPPERS_CSV = DATA_DIR / "gappers_two_days.csv"


# ── Fibonacci helpers (reused from fib_double_touch) ─────────

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


# ── Main Engine ──────────────────────────────────────────────────

class MomentumRideEngine:
    """Momentum Ride backtest on 15-second bars."""

    def __init__(self, capital: float = STARTING_CAPITAL) -> None:
        self._capital = capital
        self._cash = capital
        self._position: Optional[SimPosition] = None
        self._result = SimResult()
        self._trade_logger = TradeLogger()
        self._peak_equity = capital
        self._trade_charts_data: list[dict] = []

    # ── Public API ────────────────────────────────────────

    def run(self) -> SimResult:
        """Run backtest across all gap events."""
        logger.info(f"Momentum Ride Backtest starting: capital=${self._capital:,.2f}")
        logger.info(
            f"  Entry: VWAP cross up + SMA 9 hourly | "
            f"Exit: 5% trailing stop | "
            f"Safety stop: -5% from entry | "
            f"Tracking: {MR_TRACKING_MINUTES} min | "
            f"Gap: {MR_GAP_MIN_PCT}-{MR_GAP_MAX_PCT}%"
        )

        gappers = self._load_gappers()
        if gappers.empty:
            logger.warning("No gap events found")
            return self._result

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
        """Walk 15-sec bars for one gap day — VWAP cross + SMA9 hourly + trailing stop."""
        gap_pct = float(gap["gap_pct"])
        prev_close = float(gap["prev_close"])
        date_str = str(gap["date"])
        gap_volume = float(gap.get("gap_volume", 0))

        # ── Filters ──
        if gap_pct < MR_GAP_MIN_PCT:
            return
        if gap_pct > MR_GAP_MAX_PCT:
            return
        if gap_volume < MR_MIN_GAP_VOLUME:
            return

        # Get day high for fib computation
        day_high = float(df_15s["high"].max())
        if day_high <= prev_close * 1.05:
            return

        # Compute fib levels
        fib_prices = _get_all_fib_prices(dual, day_high)
        fib_price_info = _get_fib_price_info(dual, day_high)
        range_low = prev_close * 0.3
        range_high = day_high * 2.5
        fib_prices = [p for p in fib_prices if range_low <= p <= range_high]
        if len(fib_prices) < 3:
            return

        logger.info(
            f"  {symbol} {date_str} (gap +{gap_pct:.1f}%) "
            f"| {len(fib_prices)} fibs | {len(df_15s)} bars"
        )

        # ── Build 1-min candles from 15-sec bars ──
        min_candles = self._build_1min_candles(df_15s)
        if len(min_candles) < 3:
            return

        # ── Build 1-hour DataFrame for SMA 9 ──
        hour_df = df_15s.resample("1h").agg({
            "open": "first", "high": "max", "low": "min",
            "close": "last", "volume": "sum",
        }).dropna()
        sma9_1h = hour_df["close"].rolling(9, min_periods=1).mean()

        # ── Compute running VWAP from 15-sec bars ──
        vwap_series = self._compute_running_vwap(df_15s)

        # ── Track 90 min from first bar ──
        first_bar_time = df_15s.index[0]
        tracking_end = first_bar_time + pd.Timedelta(minutes=MR_TRACKING_MINUTES)

        # State
        position = None
        prev_below_vwap = True  # assume starts below to detect first cross
        prev_above_vwap = False  # track for pullback detection
        trades_this_day = []

        PULLBACK_TOUCH_PCT = 0.02  # low within 2% of VWAP = "touched"

        for mi in range(1, len(min_candles)):
            mc = min_candles[mi]
            mc_prev = min_candles[mi - 1]
            mc_time = mc["time"]

            # Skip past tracking window
            if mc_time > tracking_end:
                break

            # Get VWAP at this minute's last 15-sec bar
            mc_end_idx = mc["end_15s_idx"]
            if mc_end_idx >= len(vwap_series):
                mc_end_idx = len(vwap_series) - 1
            current_vwap = vwap_series[mc_end_idx]
            if current_vwap <= 0:
                continue

            price = mc["close"]
            price_above_vwap = price > current_vwap

            # ── Check trailing stop on open position (bar-by-bar within minute) ──
            if position is not None:
                exit_result = self._check_trailing_stop(position, mc, df_15s)
                if exit_result is not None:
                    exit_price, exit_time_ts, exit_reason = exit_result
                    trade_dict = self._close_position(
                        symbol, position, exit_price, exit_time_ts,
                        exit_reason, gap, df_15s, fib_prices, fib_price_info, anchor_info,
                    )
                    if trade_dict:
                        trades_this_day.append(trade_dict)
                    position = None

                # Safety stop
                if position is not None and mc["low"] <= position["stop_price"]:
                    fill = min(mc["open"], position["stop_price"])
                    fill = max(fill, 0.01)
                    trade_dict = self._close_position(
                        symbol, position, fill, mc_time,
                        f"safety_stop (${position['stop_price']:.4f})",
                        gap, df_15s, fib_prices, fib_price_info, anchor_info,
                    )
                    if trade_dict:
                        trades_this_day.append(trade_dict)
                    position = None

            # ── Check entry signals (no position) ──
            if position is None:
                entry_signal = None

                # Signal 1: VWAP cross up (was below, now above)
                if prev_below_vwap and price_above_vwap:
                    entry_signal = "vwap_cross"

                # Signal 2: Pullback to VWAP (was above, low touched VWAP, closed above)
                if entry_signal is None and prev_above_vwap and price_above_vwap:
                    low_dist_to_vwap = (mc["low"] - current_vwap) / current_vwap
                    if low_dist_to_vwap <= PULLBACK_TOUCH_PCT:
                        entry_signal = "pullback_vwap"

                if entry_signal:
                    # Check SMA 9 on 1-hour: price must be above it
                    above_sma9_1h = self._check_above_sma9_hourly(mc_time, price, sma9_1h)

                    if above_sma9_1h:
                        fill_price = round(mc["close"] * (1 + SLIPPAGE_PCT), 4)
                        vwap_dist = (fill_price - current_vwap) / current_vwap

                        # Safety stop
                        stop_price = round(fill_price * (1 - MR_SAFETY_STOP_PCT), 4)

                        qty = int((self._cash * POSITION_SIZE_PCT) / fill_price)
                        if qty <= 0:
                            prev_below_vwap = not price_above_vwap
                            prev_above_vwap = price_above_vwap
                            continue

                        commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                        cost = qty * fill_price + commission
                        if cost > self._cash:
                            qty = int((self._cash - MIN_COMMISSION) / fill_price)
                            if qty <= 0:
                                prev_below_vwap = not price_above_vwap
                                prev_above_vwap = price_above_vwap
                                continue
                            commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
                            cost = qty * fill_price + commission

                        self._cash -= cost

                        fib_level_below = self._find_nearest_fib_below(fib_prices, fill_price)
                        fib_key = round(fib_level_below, 4) if fib_level_below else 0
                        fib_ratio, fib_series = fib_price_info.get(fib_key, (0, "?"))

                        position = {
                            "symbol": symbol,
                            "quantity": qty,
                            "entry_price": fill_price,
                            "entry_time": mc_time,
                            "entry_commission": commission,
                            "stop_price": stop_price,
                            "highest_high": mc["high"],  # trailing stop tracker
                            "fib_level": fib_level_below or 0,
                            "fib_ratio": fib_ratio,
                            "fib_series": fib_series,
                            "vwap_at_entry": current_vwap,
                            "vwap_dist_pct": round(vwap_dist * 100, 1),
                            "entry_signal": entry_signal,
                        }

                        logger.info(
                            f"  ENTRY: {symbol} {qty}sh @ ${fill_price:.4f} "
                            f"({entry_signal}, VWAP dist +{vwap_dist:.1%}, stop ${stop_price:.4f})"
                        )

            # Update VWAP position tracking for next iteration
            prev_below_vwap = not price_above_vwap
            prev_above_vwap = price_above_vwap

        # End of day / tracking window: force close open position
        if position is not None:
            last_mc = min_candles[-1]
            last_price = last_mc["close"]
            last_time = last_mc["time"]
            trade_dict = self._close_position(
                symbol, position, last_price, last_time,
                "eod_close",
                gap, df_15s, fib_prices, fib_price_info, anchor_info,
            )
            if trade_dict:
                trades_this_day.append(trade_dict)
            position = None

    # ── 1-Min Candle Builder ──────────────────────────────

    @staticmethod
    def _build_1min_candles(df_15s: pd.DataFrame) -> list[dict]:
        """Group 15-sec bars into 1-min candles (every 4 bars)."""
        candles = []
        n = len(df_15s)
        for start in range(0, n, 4):
            end = min(start + 4, n)
            chunk = df_15s.iloc[start:end]
            candles.append({
                "time": df_15s.index[start],
                "open": float(chunk["open"].iloc[0]),
                "high": float(chunk["high"].max()),
                "low": float(chunk["low"].min()),
                "close": float(chunk["close"].iloc[-1]),
                "volume": float(chunk["volume"].sum()),
                "start_15s_idx": start,
                "end_15s_idx": end - 1,
            })
        return candles

    # ── Running VWAP ──────────────────────────────────────

    @staticmethod
    def _compute_running_vwap(df_15s: pd.DataFrame) -> list[float]:
        """Cumulative VWAP from 15-sec bars: Σ(TP×Vol) / Σ(Vol)."""
        tp = (df_15s["high"].values + df_15s["low"].values + df_15s["close"].values) / 3.0
        vol = df_15s["volume"].values.astype(float)
        cum_tp_vol = np.cumsum(tp * vol)
        cum_vol = np.cumsum(vol)
        vwap = np.where(cum_vol > 0, cum_tp_vol / cum_vol, 0.0)
        return vwap.tolist()

    # ── SMA 9 Hourly Filter ──────────────────────────────────

    @staticmethod
    def _check_above_sma9_hourly(
        mc_time: pd.Timestamp,
        price: float,
        sma9_1h: pd.Series,
    ) -> bool:
        """Check if price is above SMA 9 on 1-hour candles."""
        mask = sma9_1h.index <= mc_time
        if mask.any():
            sma_val = sma9_1h.loc[mask].iloc[-1]
            return price > sma_val
        return True  # no data yet, allow

    # ── Trailing Stop Exit ────────────────────────────────

    def _check_trailing_stop(
        self,
        position: dict,
        mc: dict,
        df_15s: pd.DataFrame,
    ) -> Optional[tuple[float, pd.Timestamp, str]]:
        """5% trailing stop: track highest high, exit if price drops 5% from peak.

        Walks 15-sec bars within the minute for precision.
        """
        start_idx = mc["start_15s_idx"]
        end_idx = mc["end_15s_idx"]
        highest = position["highest_high"]
        trail_pct = MR_TRAILING_STOP_PCT

        for bi in range(start_idx, end_idx + 1):
            bar = df_15s.iloc[bi]
            bar_high = float(bar["high"])
            bar_low = float(bar["low"])

            # Update highest high
            if bar_high > highest:
                highest = bar_high

            # Check trailing stop: price dropped from peak
            trail_stop = highest * (1 - trail_pct)
            if bar_low <= trail_stop:
                position["highest_high"] = highest
                exit_price = max(trail_stop, 0.01)
                exit_time = df_15s.index[bi]
                return (
                    exit_price,
                    exit_time,
                    f"trailing_stop (peak=${highest:.4f}, -{MR_TRAILING_STOP_PCT:.0%}=${trail_stop:.4f})",
                )

        # Update highest for next minute
        position["highest_high"] = highest
        return None

    # ── Fib-Based Stop ────────────────────────────────────

    @staticmethod
    def _find_nearest_fib_below(fib_prices: list[float], price: float) -> Optional[float]:
        """Find the closest fib level below the given price."""
        below = [fp for fp in fib_prices if fp < price]
        return below[-1] if below else None

    def _find_stop(self, fib_prices: list[float], entry_price: float) -> float:
        """Stop = MR_SAFETY_STOP_PCT below entry price."""
        return round(entry_price * (1 - MR_SAFETY_STOP_PCT), 4)

    # ── Position Close ────────────────────────────────────

    def _close_position(
        self,
        symbol: str,
        position: dict,
        exit_price: float,
        exit_time,
        exit_reason: str,
        gap: dict,
        df_15s: pd.DataFrame,
        fib_prices: list,
        fib_price_info: dict,
        anchor_info: dict,
    ) -> Optional[dict]:
        """Close position, record trade, update cash."""
        qty = position["quantity"]
        entry_price = position["entry_price"]
        entry_commission = position["entry_commission"]

        exit_commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
        proceeds = qty * exit_price - exit_commission
        self._cash += proceeds

        total_commission = entry_commission + exit_commission
        pnl_gross = (exit_price - entry_price) * qty
        pnl_net = pnl_gross - total_commission

        trade_dict = {
            "symbol": symbol,
            "date": str(gap["date"]),
            "quantity": qty,
            "entry_fill": entry_price,
            "entry_time": str(position["entry_time"]),
            "exit_fill": round(exit_price, 4),
            "exit_time": str(exit_time),
            "exit_reason": exit_reason,
            "pnl_gross": round(pnl_gross, 2),
            "pnl_net": round(pnl_net, 2),
            "pnl_pct": f"{(exit_price - entry_price) / entry_price:.2%}" if entry_price > 0 else "0.00%",
            "commission": round(total_commission, 2),
            "move_pct": f"{float(gap.get('gap_pct', 0)):+.1f}%",
            "fib_level": position.get("fib_level", ""),
            "fib_ratio": position.get("fib_ratio", ""),
            "fib_series": position.get("fib_series", ""),
            "stop_price": position.get("stop_price", ""),
            "vwap_at_entry": position.get("vwap_at_entry", ""),
            "vwap_dist_pct": position.get("vwap_dist_pct", ""),
            "entry_signal": position.get("entry_signal", ""),
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
            f"  TRADE CLOSED: {symbol} {qty}sh "
            f"${entry_price:.4f} -> ${exit_price:.4f} "
            f"P&L={sign}${pnl_net:.2f} ({exit_reason})"
        )

        # Store chart data
        self._trade_charts_data.append({
            "trade": trade_dict,
            "day_df": df_15s.copy(),
            "fib_prices": fib_prices,
            "fib_price_info": fib_price_info or {},
            "anchor_info": anchor_info or {},
            "gap": gap,
        })

        return trade_dict

    # ── Summary & Output ─────────────────────────────────

    def _print_summary(self) -> None:
        r = self._result
        wr = r.winning_trades / r.total_trades * 100 if r.total_trades > 0 else 0
        equity = self._cash

        print(f"\n{'='*60}")
        print(f"Momentum Ride Backtest (15-sec)")
        print(f"  Entry: VWAP cross up OR pullback to VWAP + above SMA 9 hourly")
        print(f"  Exit: {MR_TRAILING_STOP_PCT:.0%} trailing stop | Safety stop: -{MR_SAFETY_STOP_PCT:.0%} from entry")
        print(f"  Gap filter: {MR_GAP_MIN_PCT}-{MR_GAP_MAX_PCT}% | "
              f"Min vol: {MR_MIN_GAP_VOLUME:,} | "
              f"Tracking: {MR_TRACKING_MINUTES} min")
        print(f"{'='*60}")
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
            # Time segment analysis
            print(f"\n{'─'*60}")
            print("  TIME SEGMENT ANALYSIS")
            print(f"{'─'*60}")
            hour_stats: dict[int, dict] = {}
            for t in r.trades:
                try:
                    entry_str = t["entry_time"]
                    ts = pd.Timestamp(entry_str)
                    hour = ts.hour
                except Exception:
                    hour = -1
                if hour not in hour_stats:
                    hour_stats[hour] = {"wins": 0, "losses": 0, "pnl": 0.0}
                if t["pnl_net"] >= 0:
                    hour_stats[hour]["wins"] += 1
                else:
                    hour_stats[hour]["losses"] += 1
                hour_stats[hour]["pnl"] += t["pnl_net"]

            for hour in sorted(hour_stats.keys()):
                s = hour_stats[hour]
                total_h = s["wins"] + s["losses"]
                wr_h = s["wins"] / total_h * 100 if total_h > 0 else 0
                print(
                    f"    {hour:02d}:00  {total_h} trades  "
                    f"({s['wins']}W/{s['losses']}L)  "
                    f"WR={wr_h:.0f}%  P&L=${s['pnl']:+,.2f}"
                )

            print(f"\n{'─'*100}")
            print(f"  DETAILED TRADES ({len(r.trades)} total)")
            print(f"{'─'*100}")
            for i, t in enumerate(r.trades, 1):
                pnl = t["pnl_net"]
                sign = "+" if pnl >= 0 else ""
                result_tag = "WIN " if pnl >= 0 else "LOSS"
                print(f"\n  #{i} {result_tag} {t['symbol']} ({t['date']}) | Gap {t['move_pct']}")
                print(f"    Entry: {t['entry_time'][:19]}  Fill=${t['entry_fill']:.4f}  "
                      f"VWAP dist: +{t.get('vwap_dist_pct', '?')}%  Signal: {t.get('entry_signal', '?')}")
                print(f"    Exit:  {t['exit_time'][:19]}  Fill=${t['exit_fill']:.4f}")
                print(f"    Fib: ${t.get('fib_level', '?')} ({t.get('fib_series', '?')} {t.get('fib_ratio', '?')})  "
                      f"Stop=${t.get('stop_price', '?')}")
                print(f"    P&L: {sign}${pnl:.2f} ({t['pnl_pct']})  Reason: {t['exit_reason']}")

    def _save_trades_csv(self) -> None:
        if not self._result.trades:
            return
        csv_path = DATA_DIR / "momentum_ride_trades.csv"
        df = pd.DataFrame(self._result.trades)
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved trades to {csv_path}")

    def get_chart_data(self) -> list[dict]:
        """Return collected trade chart data for the chart generator."""
        return self._trade_charts_data
