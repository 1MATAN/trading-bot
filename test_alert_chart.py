#!/usr/bin/env python3
"""Demo: 4H chart stretched to full Fibonacci range.

Usage:
    python test_alert_chart.py CURX
"""

import sys
import os
from pathlib import Path

_ROOT = Path(__file__).parent.resolve()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from ib_insync import IB, Stock, util as ib_util
from matplotlib.patches import Rectangle

from config.settings import FIB_LEVEL_COLORS
from strategies.fibonacci_engine import find_anchor_candle, build_dual_series, advance_series

load_dotenv(_ROOT / '.env')
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def connect_ibkr() -> IB:
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=25, timeout=10)
    print(f"Connected: {ib.isConnected()}")
    return ib


def download_bars(ib: IB, symbol: str, bar_size: str, duration: str) -> pd.DataFrame | None:
    contract = Stock(symbol, 'SMART', 'USD')
    ib.qualifyContracts(contract)
    if not contract.conId:
        return None
    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr=duration,
        barSizeSetting=bar_size, whatToShow='TRADES', useRTH=False,
        timeout=30,
    )
    if not bars:
        return None
    df = ib_util.df(bars)
    print(f"  {len(df)} bars ({bar_size}, {duration})")
    return df


def calc_fib_levels(daily_df: pd.DataFrame, current_price: float):
    anchor = find_anchor_candle(daily_df)
    if anchor is None:
        return [], {}
    anchor_low, anchor_high, _ = anchor
    dual = build_dual_series(anchor_low, anchor_high)
    all_levels = set()
    ratio_map: dict[float, tuple[float, str]] = {}
    for _ in range(25):
        for ratio, price in dual.series1.levels:
            all_levels.add(price)
            ratio_map[round(price, 4)] = (ratio, "S1")
        for ratio, price in dual.series2.levels:
            all_levels.add(price)
            pk = round(price, 4)
            if pk not in ratio_map:
                ratio_map[pk] = (ratio, "S2")
        s1_4236 = dual.series1.low + 4.236 * (dual.series1.high - dual.series1.low)
        if current_price <= s1_4236:
            break
        dual = advance_series(dual)
    all_levels = sorted(all_levels)
    deduped = []
    for lv in all_levels:
        if not deduped or abs(lv - deduped[-1]) / max(deduped[-1], 0.001) > 0.001:
            deduped.append(lv)
    return deduped, ratio_map


def generate_chart(sym: str, df: pd.DataFrame, all_levels: list[float],
                   current_price: float, ratio_map: dict,
                   title: str, max_bars: int = 1500) -> Path | None:
    try:
        if len(df) < 5:
            return None

        df = df.tail(max_bars).copy()
        df.reset_index(drop=True, inplace=True)
        n = len(df)
        rpad = max(15, n // 10)

        fig, ax = plt.subplots(figsize=(18, 10), facecolor='#0e1117')
        ax.set_facecolor('#0e1117')

        dates = pd.to_datetime(df['date']) if 'date' in df.columns else df.index

        # Adaptive candle sizing
        if n > 1200:
            bw, ww = 0.9, 0.4
        elif n > 600:
            bw, ww = 0.8, 0.5
        elif n > 300:
            bw, ww = 0.7, 0.6
        else:
            bw, ww = 0.6, 0.7

        # ── Japanese candlesticks ──
        for i, (_, row) in enumerate(df.iterrows()):
            o, h, l, c = row['open'], row['high'], row['low'], row['close']
            clr = '#26a69a' if c >= o else '#ef5350'
            ax.plot([i, i], [l, h], color=clr, linewidth=ww, solid_capstyle='round')
            bb = min(o, c)
            bh = abs(c - o)
            if bh < (h - l) * 0.01:
                bh = max(abs(h - l) * 0.02, 0.0001)
                bb = (o + c) / 2 - bh / 2
            ax.add_patch(Rectangle((i - bw / 2, bb), bw, bh,
                                   facecolor=clr, edgecolor=clr, linewidth=0.3))

        # ── Y-axis: stretch to full Fibonacci range ──
        if all_levels:
            vmin = max(0, min(all_levels) * 0.95)
            vmax = max(all_levels) * 1.05
        else:
            plo = df['low'].min()
            phi = df['high'].max()
            prng = phi - plo
            vmin = max(0, plo - prng * 0.08)
            vmax = phi + prng * 0.08

        visible_lvls = [lv for lv in all_levels if vmin <= lv <= vmax]
        pspan = vmax - vmin

        # ── Fib levels ──
        gap = pspan * 0.018
        yr, yl = -999.0, -999.0
        for lv in visible_lvls:
            info = ratio_map.get(round(lv, 4))
            if isinstance(info, tuple):
                ratio, series = info
            elif info is not None:
                ratio, series = info, "S1"
            else:
                ratio, series = None, "S1"

            c = FIB_LEVEL_COLORS.get(ratio, '#888') if ratio is not None else '#888'
            ax.axhline(y=lv, color=c, linewidth=0.6, alpha=0.55,
                       xmin=0, xmax=n / (n + rpad))

            lb = f'{ratio}  ${lv:.4f}' if ratio is not None else f'${lv:.4f}'

            if series == "S2":
                if abs(lv - yl) < gap:
                    continue
                ax.text(-0.5, lv, f'{lb} ', color=c, fontsize=7,
                        va='center', ha='right', fontweight='bold')
                yl = lv
            else:
                if abs(lv - yr) < gap:
                    continue
                ax.text(n + 1, lv, f' {lb}', color=c, fontsize=7,
                        va='center', ha='left', fontweight='bold')
                yr = lv

        # ── Current price ──
        ax.axhline(y=current_price, color='#FFD700', linewidth=1.8,
                    linestyle='--', alpha=0.9)
        ax.text(n + rpad - 1, current_price, f'  ${current_price:.2f}  ',
                color='#0e1117', fontsize=10, va='center', ha='right',
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3',
                          facecolor='#FFD700', edgecolor='#FFD700', alpha=0.95))

        # ── X-axis ──
        step = max(1, n // 12)
        tpos = list(range(0, n, step))
        dlst = list(dates)
        tlbl = [dlst[p].strftime('%m/%d %H:%M') if hasattr(dlst[p], 'strftime')
                else str(dlst[p])[:11] for p in tpos]
        ax.set_xticks(tpos)
        ax.set_xticklabels(tlbl, color='#888', fontsize=7, rotation=30, ha='right')

        # ── Style ──
        ax.set_ylim(vmin, vmax)
        ax.set_xlim(-2, n + rpad)
        ax.tick_params(colors='#888', labelsize=8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('#333')
        ax.spines['left'].set_color('#333')
        ax.set_title(title, color='white', fontsize=15, fontweight='bold', pad=12)
        ax.grid(axis='y', color='#222', linewidth=0.3, alpha=0.5)

        out = Path(f'/tmp/alert_chart_{sym}_4h.png')
        fig.savefig(out, dpi=130, bbox_inches='tight',
                    facecolor='#0e1117', edgecolor='none')
        plt.close(fig)
        print(f"  Chart saved: {out}")
        return out

    except Exception as e:
        print(f"  Chart error: {e}")
        import traceback; traceback.print_exc()
        plt.close('all')
        return None


def send_photo(path: Path, caption: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        return False
    data = {'chat_id': CHAT_ID, 'caption': caption, 'parse_mode': 'HTML'}
    with open(path, 'rb') as f:
        resp = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                             data=data, files={'photo': f}, timeout=15)
    print(f"  Telegram: {resp.status_code}")
    return resp.ok


def main():
    sym = sys.argv[1] if len(sys.argv) > 1 else 'CURX'
    print(f"=== 4H Chart (Full Fib Range): {sym} ===\n")

    ib = connect_ibkr()
    try:
        # Daily data for Fib
        print("Daily data for Fib...")
        df_daily = download_bars(ib, sym, '1 day', '5 Y')

        # 4H bars, 6 months
        print("4H bars, 6 months...")
        df = download_bars(ib, sym, '4 hours', '6 M')
        if df is None or len(df) < 5:
            print("No data")
            return

        price = df['close'].iloc[-1]
        all_levels, ratio_map = [], {}
        if df_daily is not None:
            all_levels, ratio_map = calc_fib_levels(df_daily, price)
            print(f"  {len(all_levels)} fib levels")

        title = f'{sym} — 4H (6M) + Fibonacci  ${price:.2f}'
        p = generate_chart(sym, df, all_levels, price, ratio_map, title)
        if p:
            send_photo(p, f"📐 <b>{sym}</b> — 4 שעות (חצי שנה) + פיבו  ${price:.2f}")
            print(f"\nDone! ${price:.2f}")

    finally:
        ib.disconnect()


if __name__ == '__main__':
    main()
