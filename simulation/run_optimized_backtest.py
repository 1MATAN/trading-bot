"""Optimized Double Touch Backtest — all winning parameters.

Settings:
  - Entry window: 08:00 - 12:00 ET (best WR hours)
  - Gap: 10-25% (sweet spot)
  - Fib ratios: exclude 0.618 and 1.414 (losers)
  - Stop: 3% below fib (proven)
  - Target: 50% at 3rd fib level + 50% no-new-high trailing
  - 15-second bars
  - Float filter: <= 500M (via yfinance, post-filter)
"""
import sys
sys.path.insert(0, "/home/matan-shaar/trading-bot")

import config.settings as settings

# ── Patch settings ──
settings.FIB_DT_GAP_MAX_PCT = 25.0                    # was 50.0
settings.FIB_DT_ENTRY_WINDOW_END = "12:00"             # was 13:00
settings.FIB_DT_PREFERRED_RATIOS = {
    0.382, 0.5, 0.764, 0.88,                           # removed 0.618, 1.414
    2.272, 2.414, 3.272, 3.414, 3.618
}
settings.FIB_DT_USE_RATIO_FILTER = True

# Now import the engine (after patching)
from simulation.fib_double_touch_backtest import FibDoubleTouchEngine
import pandas as pd
import pytz
import yfinance as yf

print("=" * 70)
print("OPTIMIZED BACKTEST: Double Touch (15-sec)")
print("=" * 70)
print(f"  Gap max:        {settings.FIB_DT_GAP_MAX_PCT}%")
print(f"  Entry window:   08:00 - {settings.FIB_DT_ENTRY_WINDOW_END} ET")
print(f"  Fib ratios:     {sorted(settings.FIB_DT_PREFERRED_RATIOS)}")
print(f"  Excluded:       0.618, 1.414")
print(f"  Stop:           {settings.FIB_DT_STOP_PCT:.0%} below fib")
print(f"  Target:         {settings.FIB_DT_TARGET_LEVELS} fib levels (50%) + trailing (50%)")
print("=" * 70)

# ── Monkey-patch: add 08:00 start time filter ──
# We need to override _simulate_gap_day to add start time
import types
from datetime import time as dt_time

_original_simulate = FibDoubleTouchEngine._simulate_gap_day

def _patched_simulate(self, symbol, gap, df_15s, dual, anchor_info):
    """Filter out bars before 08:00 ET before running."""
    et_tz = pytz.timezone("US/Eastern")
    start_time = dt_time(8, 0)  # 08:00 ET

    # Filter dataframe to only bars >= 08:00 ET
    mask = []
    for t in df_15s.index:
        try:
            t_et = t.astimezone(et_tz).time()
            mask.append(t_et >= start_time)
        except:
            mask.append(True)

    df_filtered = df_15s[mask]
    if len(df_filtered) < 50:
        return

    _original_simulate(self, symbol, gap, df_filtered, dual, anchor_info)

# Apply patch
FibDoubleTouchEngine._simulate_gap_day = _patched_simulate

# ── Run backtest ──
engine = FibDoubleTouchEngine()
result = engine.run()

# ── Float post-filter analysis ──
if result.trades:
    print(f"\n{'=' * 70}")
    print("FLOAT POST-FILTER ANALYSIS")
    print(f"{'=' * 70}")

    symbols = list(set(t["symbol"] for t in result.trades))
    float_data = {}
    for sym in symbols:
        try:
            fs = yf.Ticker(sym).info.get("floatShares") or 0
            float_data[sym] = fs
        except:
            float_data[sym] = 0

    # Show all trades with float
    et = pytz.timezone("US/Eastern")
    trades_with_float = []
    for t in result.trades:
        fs = float_data.get(t["symbol"], 0)
        t["float_shares"] = fs
        trades_with_float.append(t)

    # Filter by float <= 500M
    under_500m = [t for t in trades_with_float if t["float_shares"] <= 500_000_000]
    over_500m = [t for t in trades_with_float if t["float_shares"] > 500_000_000]

    def stats(trades, label):
        if not trades:
            print(f"  {label}: 0 trades")
            return
        wins = sum(1 for t in trades if t["pnl_net"] >= 0)
        wr = wins / len(trades) * 100
        total = sum(t["pnl_net"] for t in trades)
        avg = total / len(trades)
        syms = len(set(t["symbol"] for t in trades))
        print(f"  {label}: {len(trades)} trades | {syms} symbols | WR {wr:.1f}% | "
              f"Total ${total:+,.0f} | Avg ${avg:+,.0f}")

    stats(trades_with_float, "ALL")
    stats(under_500m, "Float <= 500M")
    stats(over_500m, "Float > 500M")

    # Detailed trades
    print(f"\n{'=' * 70}")
    print("ALL TRADES (detailed)")
    print(f"{'=' * 70}")

    for i, t in enumerate(result.trades, 1):
        pnl = t["pnl_net"]
        sign = "+" if pnl >= 0 else ""
        fs = float_data.get(t["symbol"], 0)
        float_str = f"{fs/1e6:.0f}M" if fs >= 1e6 else f"{fs/1e3:.0f}K" if fs > 0 else "?"
        tag = "WIN " if pnl >= 0 else "LOSS"

        try:
            entry_dt = pd.Timestamp(t["entry_time"])
            if entry_dt.tzinfo is None:
                entry_dt = entry_dt.tz_localize("UTC")
            entry_et = entry_dt.astimezone(et)
            time_str = entry_et.strftime("%H:%M")
        except:
            time_str = "??:??"

        print(f"  #{i:3d} {tag} {t['symbol']:6s} | {time_str}ET | gap{t['move_pct']:>7s} | "
              f"fib={t.get('fib_ratio','?'):>6} | float={float_str:>5s} | "
              f"{sign}${pnl:,.0f} | {t['exit_reason'][:30]}")

    # Hourly breakdown
    print(f"\n{'=' * 70}")
    print("HOURLY BREAKDOWN")
    print(f"{'=' * 70}")

    hourly = {}
    for t in result.trades:
        try:
            entry_dt = pd.Timestamp(t["entry_time"])
            if entry_dt.tzinfo is None:
                entry_dt = entry_dt.tz_localize("UTC")
            h = entry_dt.astimezone(et).hour
            hourly.setdefault(h, []).append(t)
        except:
            pass

    for h in sorted(hourly.keys()):
        trades = hourly[h]
        wins = sum(1 for t in trades if t["pnl_net"] >= 0)
        total = sum(t["pnl_net"] for t in trades)
        avg = total / len(trades)
        wr = wins / len(trades) * 100
        print(f"  {h:02d}:00 ET: {len(trades):2d} trades | WR {wr:5.1f}% | "
              f"Total ${total:+10,.0f} | Avg ${avg:+8,.0f}")

    # Fib ratio breakdown
    print(f"\n{'=' * 70}")
    print("FIB RATIO BREAKDOWN")
    print(f"{'=' * 70}")

    by_ratio = {}
    for t in result.trades:
        r = t.get("fib_ratio", "?")
        by_ratio.setdefault(r, []).append(t)

    for r in sorted(by_ratio.keys(), key=lambda x: float(x) if x != "?" else 0):
        trades = by_ratio[r]
        wins = sum(1 for t in trades if t["pnl_net"] >= 0)
        total = sum(t["pnl_net"] for t in trades)
        avg = total / len(trades)
        wr = wins / len(trades) * 100
        print(f"  Ratio {r:>6}: {len(trades):2d} trades | WR {wr:5.1f}% | "
              f"Total ${total:+10,.0f} | Avg ${avg:+8,.0f}")

    # Exit reason breakdown
    print(f"\n{'=' * 70}")
    print("EXIT REASON BREAKDOWN")
    print(f"{'=' * 70}")

    by_exit = {}
    for t in result.trades:
        reason = t["exit_reason"].split(" ")[0]
        by_exit.setdefault(reason, []).append(t)

    for reason in sorted(by_exit.keys()):
        trades = by_exit[reason]
        wins = sum(1 for t in trades if t["pnl_net"] >= 0)
        total = sum(t["pnl_net"] for t in trades)
        avg = total / len(trades)
        wr = wins / len(trades) * 100
        print(f"  {reason:>20}: {len(trades):2d} trades | WR {wr:5.1f}% | "
              f"Total ${total:+10,.0f} | Avg ${avg:+8,.0f}")

    # Summary
    equity = 3000 + sum(t["pnl_net"] for t in result.trades)
    print(f"\n{'=' * 70}")
    print("FINAL SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Starting capital:  $3,000")
    print(f"  Final equity:      ${equity:,.0f}")
    print(f"  Total return:      {(equity - 3000) / 3000 * 100:,.0f}%")
    print(f"  Total trades:      {result.total_trades}")
    print(f"  Win rate:          {result.winning_trades/result.total_trades*100:.1f}%" if result.total_trades > 0 else "  No trades")
    print(f"  Max drawdown:      {result.max_drawdown:.2%}")
