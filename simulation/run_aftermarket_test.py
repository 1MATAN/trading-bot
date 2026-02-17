"""Quick test: run Double Touch strategy with after-market entries allowed.

Changes from baseline:
  - Entry window extended to 20:00 ET (was 13:00)
  - Gap max set to 10% (only small gaps)
"""
import sys
sys.path.insert(0, "/home/matan-shaar/trading-bot")

import config.settings as settings

# ── Patch settings for this test ──
settings.FIB_DT_ENTRY_WINDOW_END = "20:00"   # allow entries through after-market

# Save original gap max, set to large to not filter (gappers.csv already min 10%)
_orig_gap_max = settings.FIB_DT_GAP_MAX_PCT

# Reload the backtest module AFTER patching settings
from simulation.fib_double_touch_backtest import FibDoubleTouchEngine
import pandas as pd
import pytz

print("=" * 70)
print("AFTER-MARKET TEST: Double Touch with entry window 04:00-20:00 ET")
print(f"  Gap max: {settings.FIB_DT_GAP_MAX_PCT}%")
print(f"  Entry window: until {settings.FIB_DT_ENTRY_WINDOW_END} ET")
print(f"  Ratio filter: {settings.FIB_DT_USE_RATIO_FILTER}")
print(f"  Preferred ratios: {settings.FIB_DT_PREFERRED_RATIOS}")
print("=" * 70)

engine = FibDoubleTouchEngine()
result = engine.run()

# ── Analyze trades by session ──
if result.trades:
    et = pytz.timezone("US/Eastern")

    sessions = {
        "pre_market": [],      # 04:00 - 09:29
        "morning": [],         # 09:30 - 12:59
        "afternoon": [],       # 13:00 - 15:59
        "after_market": [],    # 16:00 - 19:59
    }

    for t in result.trades:
        entry_str = t["entry_time"]
        try:
            entry_dt = pd.Timestamp(entry_str)
            if entry_dt.tzinfo is None:
                entry_dt = entry_dt.tz_localize("UTC")
            entry_et = entry_dt.astimezone(et)
            hour = entry_et.hour
            minute = entry_et.minute

            if hour < 9 or (hour == 9 and minute < 30):
                sessions["pre_market"].append(t)
            elif hour < 13:
                sessions["morning"].append(t)
            elif hour < 16:
                sessions["afternoon"].append(t)
            else:
                sessions["after_market"].append(t)
        except Exception as e:
            print(f"  Error parsing time {entry_str}: {e}")

    print(f"\n{'='*70}")
    print("BREAKDOWN BY SESSION")
    print(f"{'='*70}")

    for session_name, trades in sessions.items():
        if not trades:
            print(f"\n  {session_name}: 0 trades")
            continue

        wins = sum(1 for t in trades if t["pnl_net"] >= 0)
        losses = len(trades) - wins
        total_pnl = sum(t["pnl_net"] for t in trades)
        avg_pnl = total_pnl / len(trades) if trades else 0
        wr = wins / len(trades) * 100 if trades else 0

        print(f"\n  {session_name.upper()}: {len(trades)} trades")
        print(f"    Win rate: {wr:.1f}%  ({wins}W / {losses}L)")
        print(f"    Total P&L: ${total_pnl:+,.2f}")
        print(f"    Avg P&L: ${avg_pnl:+,.2f}")

        # Show each trade
        for i, t in enumerate(trades, 1):
            pnl = t["pnl_net"]
            sign = "+" if pnl >= 0 else ""
            entry_dt = pd.Timestamp(t["entry_time"])
            if entry_dt.tzinfo is None:
                entry_dt = entry_dt.tz_localize("UTC")
            entry_et = entry_dt.astimezone(et)
            print(f"      #{i} {t['symbol']} {entry_et.strftime('%H:%M')}ET "
                  f"gap{t['move_pct']} fib={t.get('fib_ratio','')} "
                  f"{sign}${pnl:.2f} ({t['exit_reason'][:25]})")

    # Hourly breakdown
    print(f"\n{'='*70}")
    print("HOURLY BREAKDOWN")
    print(f"{'='*70}")

    hourly = {}
    for t in result.trades:
        try:
            entry_dt = pd.Timestamp(t["entry_time"])
            if entry_dt.tzinfo is None:
                entry_dt = entry_dt.tz_localize("UTC")
            entry_et = entry_dt.astimezone(et)
            h = entry_et.hour
            hourly.setdefault(h, []).append(t)
        except Exception:
            pass

    for h in sorted(hourly.keys()):
        trades = hourly[h]
        wins = sum(1 for t in trades if t["pnl_net"] >= 0)
        total = sum(t["pnl_net"] for t in trades)
        avg = total / len(trades)
        wr = wins / len(trades) * 100
        print(f"  {h:02d}:00 ET: {len(trades):2d} trades | WR {wr:5.1f}% | "
              f"Total ${total:+8,.2f} | Avg ${avg:+7,.2f}")

print(f"\n{'='*70}")
print("SUMMARY")
print(f"{'='*70}")
print(f"  Total trades: {result.total_trades}")
print(f"  Win rate: {result.winning_trades/result.total_trades*100:.1f}%" if result.total_trades > 0 else "  No trades")
print(f"  Net P&L: ${result.total_pnl_net:+,.2f}")
print(f"  Max DD: {result.max_drawdown:.2%}")
