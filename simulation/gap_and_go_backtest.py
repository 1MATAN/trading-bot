"""Gap and Go Backtest (15-second bars, IBKR data).

Strategy:
  1. Stock gapped >= 15% (from gappers list)
  2. Running VWAP from pre-market bars
  3. Entry conditions (ALL must be true):
     a. Price above VWAP
     b. 1-min Heikin Ashi is green (positive)
     c. 5-min Heikin Ashi is green (positive)
  4. Exit conditions (ALL must be true):
     a. 1-min Heikin Ashi is red (negative)
     b. 5-min Heikin Ashi is red (negative)
  5. Also exit if price drops below VWAP
  6. EOD: close everything at 15:55 ET
"""

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import pytz

from config.settings import (
    STARTING_CAPITAL,
    SLIPPAGE_PCT,
    COMMISSION_PER_SHARE,
    MIN_COMMISSION,
    GG_ENTRY_AFTER_MINUTES,
)
from simulation.sim_engine import SimResult

logger = logging.getLogger("trading_bot.gap_and_go")

ET = pytz.timezone("US/Eastern")


@dataclass
class GGPosition:
    """A Gap-and-Go position."""
    symbol: str
    quantity: int
    entry_price: float
    entry_time: pd.Timestamp
    entry_commission: float
    vwap_at_entry: float


# ── Heikin Ashi helpers ───────────────────────────────────

def _compute_heikin_ashi(candles: list[dict]) -> list[dict]:
    """Compute Heikin Ashi from regular OHLC candles.

    Returns list of dicts with keys: time, ha_open, ha_high, ha_low, ha_close, is_green.
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
        ha_high = max(c["high"], ha_open, ha_close)
        ha_low = min(c["low"], ha_open, ha_close)

        ha.append({
            "time": c["time"],
            "ha_open": ha_open,
            "ha_high": ha_high,
            "ha_low": ha_low,
            "ha_close": ha_close,
            "is_green": ha_close >= ha_open,
        })

        prev_ha_open = ha_open
        prev_ha_close = ha_close

    return ha


class GapAndGoEngine:
    """Gap and Go backtest on 15-second bars."""

    def __init__(self, capital: float = STARTING_CAPITAL) -> None:
        self._capital = capital
        self._cash = capital
        self._result = SimResult()
        self._peak_equity = capital
        self._trade_charts_data: list[dict] = []

    # ── Public API ────────────────────────────────────────

    def run(self, gappers_data: list[dict]) -> SimResult:
        """Run backtest across all gap events."""
        logger.info(f"Gap and Go Backtest starting: capital=${self._capital:,.2f}")
        logger.info(
            f"  Entry: above VWAP + HA green 1m + HA green 5m | "
            f"Exit: HA red 1m + HA red 5m (or below VWAP)"
        )

        symbols_seen = set()
        for g in gappers_data:
            sym = g["symbol"]
            symbols_seen.add(sym)
            try:
                self._simulate_gap_day(
                    symbol=sym,
                    gap=g,
                    df_15s=g["df_15s"],
                )
            except Exception as e:
                logger.error(f"Error for {sym}: {e}", exc_info=True)

        self._result.symbols_tested = sorted(symbols_seen)
        self._print_summary()
        return self._result

    # ── Per-Day Simulation ────────────────────────────────

    def _simulate_gap_day(
        self,
        symbol: str,
        gap: dict,
        df_15s: pd.DataFrame,
    ) -> None:
        """Walk 15-sec bars for one gap day."""
        gap_pct = float(gap["gap_pct"])
        date_str = str(gap["date"])

        if len(df_15s) < 50:
            return

        logger.info(
            f"\n--- {symbol} {date_str} (gap +{gap_pct:.1f}%) | {len(df_15s)} bars ---"
        )

        # ── Localize index to ET ──
        idx = df_15s.index
        if idx.tz is None:
            idx = idx.tz_localize("UTC").tz_convert(ET)
        elif str(idx.tz) != str(ET):
            idx = idx.tz_convert(ET)
        df_15s = df_15s.copy()
        df_15s.index = idx

        # ── Time boundaries ──
        # Allow entries from 04:15 (pre-market, after HA warmup) until 19:55 (after-hours)
        entry_ok_time = idx[0].normalize() + pd.Timedelta(hours=4, minutes=15)
        eod_close_time = idx[0].normalize() + pd.Timedelta(hours=19, minutes=55)

        # ── Build running VWAP ──
        vwap_series = self._compute_running_vwap(df_15s)

        # ── Build 1-min candles + Heikin Ashi ──
        min_candles = self._build_1min_candles(df_15s)
        if len(min_candles) < 10:
            return
        ha_1m = _compute_heikin_ashi(min_candles)

        # ── Build 5-min candles + Heikin Ashi ──
        min5_candles = self._build_5min_candles(df_15s)
        ha_5m = _compute_heikin_ashi(min5_candles)

        # ── State ──
        position: Optional[GGPosition] = None
        first_entry_done = False  # 2% VWAP proximity only on first entry

        for mi in range(1, len(min_candles)):
            mc = min_candles[mi]
            mc_time = mc["time"]
            ha = ha_1m[mi]

            # Get running VWAP at this candle's end
            mc_end_idx = mc["end_15s_idx"]
            if mc_end_idx >= len(vwap_series):
                mc_end_idx = len(vwap_series) - 1
            current_vwap = vwap_series[mc_end_idx]
            if current_vwap <= 0:
                continue

            price = mc["close"]

            # Find current 5-min HA candle
            ha_5m_current = self._get_5m_ha_at_time(ha_5m, min5_candles, mc_time)

            # ── Check exits on open position ──
            if position is not None:
                closed = self._check_exits(
                    position, mc, ha, ha_5m_current,
                    eod_close_time,
                    gap, date_str, df_15s,
                )
                if closed:
                    position = None

            # ── Check entry (after 9:35, no position) ──
            if position is None and mc_time >= entry_ok_time and mc_time < eod_close_time:
                position = self._check_entry(
                    symbol, mc, price,
                    current_vwap, ha, ha_5m_current,
                    gap, date_str,
                    require_vwap_proximity=not first_entry_done,
                )
                if position is not None:
                    first_entry_done = True

        # EOD: force close
        if position is not None:
            last_mc = min_candles[-1]
            self._close_position(
                position, last_mc["close"], last_mc["time"],
                "eod_close", gap, date_str, df_15s,
            )

    # ── Entry Detection ────────────────────────────────────

    def _check_entry(
        self,
        symbol: str,
        mc: dict,
        price: float,
        current_vwap: float,
        ha_1m: dict,
        ha_5m: Optional[dict],
        gap: dict,
        date_str: str,
        require_vwap_proximity: bool = True,
    ) -> Optional[GGPosition]:
        """Check entry conditions.

        First entry:  above VWAP + within 2% + HA green 1m + HA green 5m
        Re-entry:     above VWAP + HA green 1m + HA green 5m (no 2% limit)
        """
        # Condition 1: price above VWAP
        if price <= current_vwap:
            return None

        # Condition 1b: first entry must be within 2% of VWAP
        vwap_distance_pct = (price - current_vwap) / current_vwap
        if require_vwap_proximity and vwap_distance_pct > 0.02:
            return None

        # Condition 2: HA green on 1m
        if not ha_1m["is_green"]:
            return None

        # Condition 3: HA green on 5m (always required)
        if ha_5m is None or not ha_5m["is_green"]:
            return None

        # ── Entry ──
        fill_price = round(price * (1 + SLIPPAGE_PCT), 4)

        # Size: 95% of cash
        total_qty = int((self._cash * 0.95) / fill_price)
        if total_qty <= 0:
            return None

        commission = max(total_qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
        cost = total_qty * fill_price + commission
        if cost > self._cash:
            total_qty = int((self._cash - MIN_COMMISSION) / fill_price)
            if total_qty <= 0:
                return None
            commission = max(total_qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
            cost = total_qty * fill_price + commission

        self._cash -= cost

        bar_time = mc["time"]
        position = GGPosition(
            symbol=symbol,
            quantity=total_qty,
            entry_price=fill_price,
            entry_time=bar_time,
            entry_commission=commission,
            vwap_at_entry=current_vwap,
        )

        entry_type = "1st" if require_vwap_proximity else "re"
        logger.info(
            f"  ENTRY({entry_type}): {symbol} {total_qty}sh @ ${fill_price:.4f} "
            f"(VWAP=${current_vwap:.4f}, dist={vwap_distance_pct:.1%}, HA 1m+5m green)"
        )
        return position

    # ── Exit Logic ─────────────────────────────────────────

    def _check_exits(
        self,
        position: GGPosition,
        mc: dict,
        ha_1m: dict,
        ha_5m: Optional[dict],
        eod_close_time: pd.Timestamp,
        gap: dict,
        date_str: str,
        df_15s: pd.DataFrame,
    ) -> bool:
        """Check exit conditions. Returns True if position closed."""
        mc_time = mc["time"]
        price = mc["close"]

        # ── HA exit: both 1m AND 5m are red ──
        ha_1m_red = not ha_1m["is_green"]
        ha_5m_red = ha_5m is not None and not ha_5m["is_green"]

        if ha_1m_red and ha_5m_red:
            self._close_position(
                position, price, mc_time,
                "ha_exit (1m+5m red)",
                gap, date_str, df_15s,
            )
            return True

        # ── EOD close ──
        if mc_time >= eod_close_time:
            self._close_position(
                position, price, mc_time,
                "eod_close",
                gap, date_str, df_15s,
            )
            return True

        return False

    # ── Position Close ─────────────────────────────────────

    def _close_position(
        self,
        position: GGPosition,
        exit_price: float,
        exit_time,
        exit_reason: str,
        gap: dict,
        date_str: str,
        df_15s: pd.DataFrame,
    ) -> None:
        """Close entire position and record trade."""
        qty = position.quantity
        entry_price = position.entry_price

        exit_commission = max(qty * COMMISSION_PER_SHARE, MIN_COMMISSION)
        proceeds = qty * exit_price - exit_commission
        self._cash += proceeds

        total_commission = position.entry_commission + exit_commission
        pnl_gross = (exit_price - entry_price) * qty
        pnl_net = pnl_gross - total_commission

        trade_dict = {
            "symbol": position.symbol,
            "date": date_str,
            "quantity": qty,
            "entry_fill": entry_price,
            "entry_time": str(position.entry_time),
            "exit_fill": round(exit_price, 4),
            "exit_time": str(exit_time),
            "exit_reason": exit_reason,
            "pnl_gross": round(pnl_gross, 2),
            "pnl_net": round(pnl_net, 2),
            "pnl_pct": f"{(exit_price - entry_price) / entry_price:.2%}" if entry_price > 0 else "0.00%",
            "commission": round(total_commission, 2),
            "move_pct": f"+{float(gap.get('gap_pct', 0)):.1f}%",
            "vwap_at_entry": position.vwap_at_entry,
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

        # Equity tracking
        equity = self._cash
        if equity > self._peak_equity:
            self._peak_equity = equity
        dd = (self._peak_equity - equity) / self._peak_equity if self._peak_equity > 0 else 0
        if dd > self._result.max_drawdown:
            self._result.max_drawdown = dd
        self._result.equity_curve.append({"time": str(exit_time), "equity": round(equity, 2)})

        sign = "+" if pnl_net >= 0 else ""
        logger.info(
            f"  EXIT: {position.symbol} {qty}sh "
            f"${entry_price:.4f} -> ${exit_price:.4f} "
            f"P&L={sign}${pnl_net:.2f} ({exit_reason})"
        )

        # Store chart data
        self._trade_charts_data.append({
            "position": {
                "symbol": position.symbol,
                "entry_price": position.entry_price,
                "entry_time": str(position.entry_time),
                "vwap_at_entry": position.vwap_at_entry,
            },
            "trade": trade_dict,
            "day_df": df_15s.copy(),
            "gap": gap,
        })

    # ── Candle Builders ───────────────────────────────────

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

    @staticmethod
    def _build_5min_candles(df_15s: pd.DataFrame) -> list[dict]:
        """Group 15-sec bars into 5-min candles (every 20 bars)."""
        candles = []
        n = len(df_15s)
        for start in range(0, n, 20):
            end = min(start + 20, n)
            chunk = df_15s.iloc[start:end]
            candles.append({
                "time": df_15s.index[start],
                "open": float(chunk["open"].iloc[0]),
                "high": float(chunk["high"].max()),
                "low": float(chunk["low"].min()),
                "close": float(chunk["close"].iloc[-1]),
                "volume": float(chunk["volume"].sum()),
            })
        return candles

    @staticmethod
    def _get_5m_ha_at_time(
        ha_5m: list[dict],
        min5_candles: list[dict],
        mc_time: pd.Timestamp,
    ) -> Optional[dict]:
        """Find the 5-min HA candle that covers mc_time."""
        for i in range(len(min5_candles) - 1, -1, -1):
            if min5_candles[i]["time"] <= mc_time:
                return ha_5m[i] if i < len(ha_5m) else None
        return None

    # ── Running VWAP ──────────────────────────────────────

    @staticmethod
    def _compute_running_vwap(df_15s: pd.DataFrame) -> list[float]:
        """Cumulative VWAP from 15-sec bars: sum(TP*Vol) / sum(Vol)."""
        tp = (df_15s["high"].values + df_15s["low"].values + df_15s["close"].values) / 3.0
        vol = df_15s["volume"].values.astype(float)
        cum_tp_vol = np.cumsum(tp * vol)
        cum_vol = np.cumsum(vol)
        vwap = np.where(cum_vol > 0, cum_tp_vol / cum_vol, 0.0)
        return vwap.tolist()

    # ── Summary ────────────────────────────────────────────

    def _print_summary(self) -> None:
        r = self._result
        wr = r.winning_trades / r.total_trades * 100 if r.total_trades > 0 else 0
        equity = self._cash

        print(f"\n{'='*60}")
        print(f"Gap and Go Backtest (15-sec)")
        print(f"  1st entry: within 2% VWAP + HA green 1m + 5m")
        print(f"  Re-entry: above VWAP + HA green 1m + 5m")
        print(f"  Exit: HA red 1m + HA red 5m")
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
            print(f"\n{'─'*90}")
            print(f"  DETAILED TRADES ({len(r.trades)})")
            print(f"{'─'*90}")
            for i, t in enumerate(r.trades, 1):
                pnl = t["pnl_net"]
                sign = "+" if pnl >= 0 else ""
                result_tag = "WIN " if pnl >= 0 else "LOSS"
                print(
                    f"\n  #{i} {result_tag} {t['symbol']} "
                    f"({t['date']}) | Gap {t['move_pct']}"
                )
                print(
                    f"    Entry: {t['entry_time'][:19]}  ${t['entry_fill']:.4f}  "
                    f"Qty={t['quantity']}  VWAP=${t['vwap_at_entry']:.4f}"
                )
                print(
                    f"    Exit:  {t['exit_time'][:19]}  ${t['exit_fill']:.4f}  "
                    f"{t['exit_reason']}"
                )
                print(
                    f"    P&L: {sign}${pnl:.2f} ({t['pnl_pct']})"
                )

    def get_chart_data(self) -> list[dict]:
        """Return collected chart data for the chart generator."""
        return self._trade_charts_data
