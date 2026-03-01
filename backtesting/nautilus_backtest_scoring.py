"""A/B Comparison Backtest — Old Scoring vs New Scoring.

Computes RVOL and float turnover from ACTUAL 15s bar data (total daily
volume), not from gap_volume. This matches how the live system works.

Runs two VWAPZoneSimulators on the same data:
  A) Baseline  — ALL qualifying candidates per day (old behavior)
  B) New Score — TOP 3 candidates per day, ranked by new composite score
     (20% gap, 35% RVOL, 45% turnover + convergence bonus)

Usage:
    python backtesting/nautilus_backtest_scoring.py [--min-date 2026-02-14]
"""

import json
import sys
from datetime import time
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

# Reuse loader + simulator from main backtest
from nautilus_backtest import (
    VWAPZoneSimulator,
    _CACHE_DIR,
    load_15s_bars,
)

_FLOAT_DATA_PATH = _CACHE_DIR / "float_data.json"
_ET = ZoneInfo("US/Eastern")


# ═══════════════════════════════════════════════════════════════
#  Float + RVOL from Finviz cache + 15s bars
# ═══════════════════════════════════════════════════════════════

def _load_float_data() -> dict:
    """Load Finviz float + avg volume data from JSON cache."""
    if not _FLOAT_DATA_PATH.exists():
        print(f"WARNING: {_FLOAT_DATA_PATH} not found. Run fetch_float_data.py first.")
        return {}
    return json.loads(_FLOAT_DATA_PATH.read_text())


def _compute_volumes(parquet_path: str) -> tuple[float, float]:
    """Compute total daily volume AND first-hour volume (9:30-10:30 ET) from 15s bars.

    Returns (total_vol, first_hour_vol).
    """
    try:
        df = pd.read_parquet(parquet_path)
        if "volume" not in df.columns:
            return 0.0, 0.0
        total = float(df["volume"].sum())

        # First hour: 9:30 - 10:30 ET
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        et_idx = df.index.tz_convert(_ET)
        mask = (et_idx.time >= time(9, 30)) & (et_idx.time < time(10, 30))
        first_hour = float(df.loc[mask, "volume"].sum())
        return total, first_hour
    except Exception:
        return 0.0, 0.0


# ═══════════════════════════════════════════════════════════════
#  Composite Score (mirrors screen_monitor.py logic)
# ═══════════════════════════════════════════════════════════════

def composite_score_old(gap_pct: float, rvol: float, turnover: float) -> float:
    """OLD weights: 40% gap, 40% RVOL, 20% turnover."""
    gap_score = min(gap_pct / 100, 1.5) * 100
    rvol_score = min(rvol / 10, 1.0) * 100
    turn_score = min(turnover / 100, 1.5) * 100
    return 0.4 * gap_score + 0.4 * rvol_score + 0.2 * turn_score


def composite_score_new(gap_pct: float, rvol: float, turnover: float) -> float:
    """NEW weights: 20% gap, 35% RVOL, 45% turnover + convergence bonus."""
    gap_score = min(gap_pct / 100, 1.5) * 100
    rvol_score = min(rvol / 10, 1.0) * 100
    turn_score = min(turnover / 100, 1.5) * 100
    score = 0.2 * gap_score + 0.35 * rvol_score + 0.45 * turn_score

    # Convergence bonus
    strong = 0
    if gap_pct >= 40:
        strong += 1
    if rvol >= 4.0:
        strong += 1
    if turnover >= 20:
        strong += 1
    if strong >= 2:
        score *= 1.3
    if strong >= 3:
        score *= 1.2

    return score


# ═══════════════════════════════════════════════════════════════
#  Main Backtest Runner
# ═══════════════════════════════════════════════════════════════

def run_comparison():
    # Parse CLI
    min_date = "2026-02-14"
    for i, arg in enumerate(sys.argv):
        if arg == "--min-date" and i + 1 < len(sys.argv):
            min_date = sys.argv[i + 1]

    csv_path = _CACHE_DIR / "gappers_bot_stocks.csv"
    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found")
        sys.exit(1)

    gappers = pd.read_csv(csv_path)

    # Filter: min date, min gap, has parquet
    results = []
    for _, row in gappers.iterrows():
        sym = row["symbol"]
        date = row["date"]
        if date < min_date:
            continue
        parquet = _CACHE_DIR / f"{sym}_15s_{date}.parquet"
        if not parquet.exists():
            continue
        if row["gap_pct"] < 20.0:
            continue
        if row["gap_volume"] < 5000:
            continue
        results.append(row.to_dict() | {"parquet": str(parquet)})

    if not results:
        print("No qualifying gappers found")
        sys.exit(1)

    df = pd.DataFrame(results)
    dates = sorted(df["date"].unique())

    print("=" * 70)
    print("  A/B Scoring Comparison Backtest (with real intraday volume)")
    print(f"  Date range: {dates[0]} → {dates[-1]} ({len(dates)} days)")
    print(f"  Candidates: {len(df)} symbol-days")
    print("=" * 70)
    print()

    # Load Finviz float + avg volume data
    print("Loading Finviz float + avg volume data...")
    float_data = _load_float_data()
    sym_set = set(df["symbol"].unique())
    has_float = sum(1 for s in sym_set if float_data.get(s, {}).get("float_shares", 0) > 0)
    has_avg_vol = sum(1 for s in sym_set if float_data.get(s, {}).get("avg_vol", 0) > 0)
    print(f"  {has_float}/{len(sym_set)} symbols with float data")
    print(f"  {has_avg_vol}/{len(sym_set)} symbols with avg volume (for RVOL)")
    print()

    # Pre-compute volumes from 15s bar data
    print("Computing intraday volumes from 15s bar data...")
    vol_data: dict[str, tuple[float, float]] = {}  # key: "SYM_DATE" → (total, first_hour)
    for _, row in df.iterrows():
        key = f"{row['symbol']}_{row['date']}"
        vol_data[key] = _compute_volumes(row["parquet"])
    total_vol = sum(v[0] for v in vol_data.values())
    print(f"  {len(vol_data)} symbol-days, total volume: {total_vol:,.0f} shares")
    print()

    # Three simulators
    sim_all = VWAPZoneSimulator()
    sim_all.result.name = "VZ All (Baseline)"
    sim_top3_old = VWAPZoneSimulator()
    sim_top3_old.result.name = "VZ Top-3 (Old Wt)"
    sim_top3 = VWAPZoneSimulator()
    sim_top3.result.name = "VZ Top-3 (New)"

    total_bars = 0

    for date in dates:
        day_df = df[df["date"] == date]

        # Compute scores for each candidate using REAL intraday volume
        scored = []
        for _, row in day_df.iterrows():
            sym = row["symbol"]
            gap_pct = row["gap_pct"]
            parquet = row["parquet"]

            fdata = float_data.get(sym, {})
            float_shares = fdata.get("float_shares", 0)
            avg_vol = fdata.get("avg_vol", 0)

            # Min float filter: skip micro-float traps (< 500K shares)
            if 0 < float_shares < 500_000:
                continue

            # Volumes from 15s bars
            total_vol_day, first_hour_vol = vol_data.get(f"{sym}_{date}", (0, 0))

            # RVOL from first hour: extrapolate to full day, compare to avg
            # 6.5 trading hours → first hour ≈ 1/6.5 of day (but typically 30-40%)
            # Use first_hour × 3.5 as conservative daily estimate (first hour = ~30% of vol)
            projected_vol = first_hour_vol * 3.5 if first_hour_vol > 0 else total_vol_day
            rvol = (projected_vol / avg_vol) if avg_vol > 0 else 1.0

            # Float turnover = total_day_volume / float_shares * 100
            turnover = (total_vol_day / float_shares * 100) if float_shares > 0 else 0.0

            old_score = composite_score_old(gap_pct, rvol, turnover)
            new_score = composite_score_new(gap_pct, rvol, turnover)

            scored.append({
                "sym": sym,
                "parquet": parquet,
                "gap_pct": gap_pct,
                "rvol": round(rvol, 1),
                "turnover": round(turnover, 1),
                "total_vol": total_vol_day,
                "first_hr": first_hour_vol,
                "float_k": round(float_shares / 1000) if float_shares > 0 else 0,
                "old_score": old_score,
                "new_score": new_score,
            })

        # Sort by new score for top-3 selection
        scored_new = sorted(scored, key=lambda x: x["new_score"], reverse=True)
        top3_new = set(s["sym"] for s in scored_new[:3])

        # Sort by old score for top-3 with old weights
        scored_old = sorted(scored, key=lambda x: x["old_score"], reverse=True)
        top3_old = set(s["sym"] for s in scored_old[:3])

        # Print day summary
        print(f"─── {date} ({len(scored)} candidates) ───")
        for s in scored_new[:6]:
            in_new = "★" if s["sym"] in top3_new else " "
            in_old = "●" if s["sym"] in top3_old else " "
            flt_str = f"{s['float_k']:>6,}K" if s['float_k'] > 0 else "    n/a"
            print(f"  {in_new}{in_old} {s['sym']:8s} gap={s['gap_pct']:5.0f}%  "
                  f"RVOL={s['rvol']:5.1f}x  FT={s['turnover']:5.0f}%  "
                  f"float={flt_str}  1h={s['first_hr']:>10,.0f}  "
                  f"old={s['old_score']:5.1f}  new={s['new_score']:5.1f}")
        if len(scored) > 6:
            print(f"  ... +{len(scored)-6} more")
        diff = top3_new != top3_old
        print(f"  TOP 3 (new): {sorted(top3_new)}{'  ← DIFFERENT' if diff else ''}")
        if diff:
            print(f"  TOP 3 (old): {sorted(top3_old)}")
        print()

        # Reset daily state
        sim_all.reset_day()
        sim_top3.reset_day()
        sim_top3_old.reset_day()

        # Process bars
        for s in scored:
            sym = s["sym"]
            try:
                bars_df = load_15s_bars(s["parquet"])
            except Exception as e:
                print(f"  SKIP {sym} {date}: {e}")
                continue
            if len(bars_df) < 20:
                continue

            bar_count = 0
            for ts, bar_row in bars_df.iterrows():
                bar = {
                    "open": float(bar_row["open"]),
                    "high": float(bar_row["high"]),
                    "low": float(bar_row["low"]),
                    "close": float(bar_row["close"]),
                    "volume": float(bar_row["volume"]),
                }
                if bar["close"] <= 0 or bar["open"] <= 0:
                    continue

                # Baseline: processes ALL candidates
                sim_all.process_bar(sym, bar, ts)

                # New scoring: only processes top 3
                if sym in top3_new:
                    sim_top3.process_bar(sym, bar, ts)

                # Old scoring top 3
                if sym in top3_old:
                    sim_top3_old.process_bar(sym, bar, ts)

                bar_count += 1

            total_bars += bar_count

        # Force close remaining at EOD
        for sim in [sim_all, sim_top3, sim_top3_old]:
            for pos_key in list(sim.result.positions.keys()):
                pos = sim.result.positions[pos_key]
                sim.result.sell(pos_key, pos["entry_price"],
                                pd.Timestamp.now(tz="UTC"), "eod_force_close")

    print(f"\nProcessed {total_bars:,} bars total")
    print()

    # ── Results ──
    print("=" * 70)
    print("  RESULTS COMPARISON")
    print("=" * 70)
    print()
    print(f"  {'Strategy':25s} {'P&L':>10s} {'%':>8s} {'Trades':>7s} "
          f"{'WR':>5s} {'Avg W':>8s} {'Avg L':>8s} {'Best':>8s} {'Worst':>8s}")
    print("  " + "─" * 90)

    for sim in [sim_all, sim_top3_old, sim_top3]:
        s = sim.result.summary()
        emoji = "🟢" if s["total_pnl"] >= 0 else "🔴"
        print(f"  {emoji} {s['name']:23s} ${s['total_pnl']:+8.2f} "
              f"{s['pnl_pct']:+6.1f}%  {s['total_trades']:5d}  "
              f"{s['win_rate']:4.0f}% ${s['avg_win']:+7.2f} ${s['avg_loss']:+7.2f} "
              f"${s['largest_win']:+7.2f} ${s['largest_loss']:+7.2f}")
    print()

    # ── Per-day breakdown ──
    print("=" * 70)
    print("  PER-DAY P&L BREAKDOWN")
    print("=" * 70)
    print()
    print(f"  {'Date':12s} {'All (Baseline)':>16s} {'Top-3 (Old Wt)':>16s} {'Top-3 (New Wt)':>16s}")
    print("  " + "─" * 62)

    for sim in [sim_all, sim_top3_old, sim_top3]:
        sim._daily_pnl = {}
        for t in sim.result.trades:
            if t["side"] != "SELL":
                continue
            ts_str = str(t.get("ts", ""))[:10]
            sim._daily_pnl[ts_str] = sim._daily_pnl.get(ts_str, 0) + t.get("pnl", 0)

    all_trade_dates = sorted(set(
        list(sim_all._daily_pnl.keys()) +
        list(sim_top3_old._daily_pnl.keys()) +
        list(sim_top3._daily_pnl.keys())
    ))
    for d in all_trade_dates:
        p_all = sim_all._daily_pnl.get(d, 0)
        p_old = sim_top3_old._daily_pnl.get(d, 0)
        p_new = sim_top3._daily_pnl.get(d, 0)
        e_all = "🟢" if p_all >= 0 else "🔴"
        e_old = "🟢" if p_old >= 0 else "🔴"
        e_new = "🟢" if p_new >= 0 else "🔴"
        print(f"  {d:12s} {e_all} ${p_all:+10.2f}   {e_old} ${p_old:+10.2f}   {e_new} ${p_new:+10.2f}")

    # Totals
    t_all = sum(sim_all._daily_pnl.values())
    t_old = sum(sim_top3_old._daily_pnl.values())
    t_new = sum(sim_top3._daily_pnl.values())
    print("  " + "─" * 62)
    e_all = "🟢" if t_all >= 0 else "🔴"
    e_old = "🟢" if t_old >= 0 else "🔴"
    e_new = "🟢" if t_new >= 0 else "🔴"
    print(f"  {'TOTAL':12s} {e_all} ${t_all:+10.2f}   {e_old} ${t_old:+10.2f}   {e_new} ${t_new:+10.2f}")
    print()
    print("Done.")


if __name__ == "__main__":
    run_comparison()
