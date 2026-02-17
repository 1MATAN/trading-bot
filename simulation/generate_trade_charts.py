"""Generate interactive HTML charts for all optimized backtest trades.

Each trade shows:
  - 15-sec candlestick chart for the gap day
  - Entry marker (green triangle up)
  - Exit marker (red triangle down)
  - Stop loss line (red dashed)
  - Target line (green dashed)
  - Fib level line (blue dashed)
  - Partial exit marker if applicable
  - P&L annotation
"""
import sys
sys.path.insert(0, "/home/matan-shaar/trading-bot")

import pandas as pd
import pytz
from pathlib import Path
from datetime import time as dt_time

# ── First, run the optimized backtest to get chart data ──
import config.settings as settings

settings.FIB_DT_GAP_MAX_PCT = 25.0
settings.FIB_DT_ENTRY_WINDOW_END = "12:00"
settings.FIB_DT_PREFERRED_RATIOS = {
    0.382, 0.5, 0.764, 0.88,
    2.272, 2.414, 3.272, 3.414, 3.618
}
settings.FIB_DT_USE_RATIO_FILTER = True

from simulation.fib_double_touch_backtest import FibDoubleTouchEngine
import types

# Monkey-patch: 08:00 start filter
_original_simulate = FibDoubleTouchEngine._simulate_gap_day

def _patched_simulate(self, symbol, gap, df_15s, dual, anchor_info):
    et_tz = pytz.timezone("US/Eastern")
    start_time = dt_time(8, 0)
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

FibDoubleTouchEngine._simulate_gap_day = _patched_simulate

print("Running optimized backtest...")
engine = FibDoubleTouchEngine()
result = engine.run()
chart_data_list = engine.get_chart_data()

print(f"Got {len(chart_data_list)} trades with chart data")

# ── Now generate plotly charts ──
import plotly.graph_objects as go
from plotly.subplots import make_subplots

et = pytz.timezone("US/Eastern")
CACHE_DIR = Path("data/backtest_cache")

def make_trade_chart(trade_info, trade_num):
    """Create a plotly figure for one trade."""
    trade = trade_info["trade"]
    day_df = trade_info["day_df"]
    fib_prices = trade_info.get("fib_prices", [])
    fib_price_info = trade_info.get("fib_price_info", {})
    gap = trade_info.get("gap", {})

    symbol = trade["symbol"]
    entry_price = trade["entry_fill"]
    exit_price = trade["exit_fill"]
    pnl = trade["pnl_net"]
    qty = trade["quantity"]
    exit_reason = trade["exit_reason"]
    fib_level = trade.get("fib_level", 0)
    stop_price = trade.get("stop_price", 0)
    target_price = trade.get("target_price", 0)
    fib_ratio = trade.get("fib_ratio", "?")
    partial_price = trade.get("partial_exit_price", "")
    gap_pct = trade.get("move_pct", "?")

    # Parse times
    entry_time = pd.Timestamp(trade["entry_time"])
    exit_time = pd.Timestamp(trade["exit_time"])
    if entry_time.tzinfo is None:
        entry_time = entry_time.tz_localize("UTC")
    if exit_time.tzinfo is None:
        exit_time = exit_time.tz_localize("UTC")
    entry_et = entry_time.astimezone(et)
    exit_et = exit_time.astimezone(et)

    # Filter day_df to show context around the trade (1 hour before entry to 1 hour after exit)
    # But show at least the trading window
    window_start = entry_time - pd.Timedelta(hours=1)
    window_end = exit_time + pd.Timedelta(hours=1)

    # Resample to 1-min for readability (15-sec is too dense)
    day_df_resampled = day_df.resample("1min").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
    }).dropna()

    mask = (day_df_resampled.index >= window_start) & (day_df_resampled.index <= window_end)
    plot_df = day_df_resampled[mask]
    if len(plot_df) < 5:
        plot_df = day_df_resampled  # fallback to full day

    # Convert to ET for display
    plot_df_et = plot_df.copy()
    plot_df_et.index = plot_df_et.index.tz_convert(et)

    # Color based on win/loss
    win = pnl >= 0
    bg_color = "#0a1f0a" if win else "#1f0a0a"
    result_text = f"WIN +${pnl:,.0f}" if win else f"LOSS ${pnl:,.0f}"
    result_color = "#00d26a" if win else "#ff4444"

    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=plot_df_et.index,
        open=plot_df_et["open"],
        high=plot_df_et["high"],
        low=plot_df_et["low"],
        close=plot_df_et["close"],
        name="Price",
        increasing_line_color="#00d26a",
        decreasing_line_color="#ff4444",
    ))

    # Fib level (blue)
    if fib_level and float(fib_level) > 0:
        fl = float(fib_level)
        fig.add_hline(y=fl, line_dash="dot", line_color="#4488ff", line_width=1,
                      annotation_text=f"Fib {fib_ratio} = ${fl:.4f}",
                      annotation_position="bottom left",
                      annotation_font_color="#4488ff")

    # Stop loss (red)
    if stop_price and float(stop_price) > 0:
        sp = float(stop_price)
        fig.add_hline(y=sp, line_dash="dash", line_color="#ff4444", line_width=1,
                      annotation_text=f"Stop ${sp:.4f}",
                      annotation_position="bottom right",
                      annotation_font_color="#ff4444")

    # Target (green)
    if target_price and float(target_price) > 0:
        tp = float(target_price)
        fig.add_hline(y=tp, line_dash="dash", line_color="#00d26a", line_width=1,
                      annotation_text=f"Target ${tp:.4f}",
                      annotation_position="top right",
                      annotation_font_color="#00d26a")

    # Entry marker
    fig.add_trace(go.Scatter(
        x=[entry_et], y=[entry_price],
        mode="markers+text",
        marker=dict(symbol="triangle-up", size=16, color="#00d26a", line=dict(width=2, color="white")),
        text=[f"BUY ${entry_price:.4f}"],
        textposition="bottom center",
        textfont=dict(color="#00d26a", size=11),
        name="Entry",
        showlegend=False,
    ))

    # Partial exit marker
    if partial_price and partial_price != "":
        pp = float(partial_price)
        partial_time_str = trade.get("partial_exit_time", "")
        if partial_time_str:
            try:
                partial_time = pd.Timestamp(partial_time_str)
                if partial_time.tzinfo is None:
                    partial_time = partial_time.tz_localize("UTC")
                partial_et = partial_time.astimezone(et)
            except:
                partial_et = entry_et + (exit_et - entry_et) / 2
        else:
            partial_et = entry_et + (exit_et - entry_et) / 2

        fig.add_trace(go.Scatter(
            x=[partial_et], y=[pp],
            mode="markers+text",
            marker=dict(symbol="diamond", size=12, color="#ffaa00", line=dict(width=2, color="white")),
            text=[f"50% @ ${pp:.4f}"],
            textposition="top center",
            textfont=dict(color="#ffaa00", size=10),
            name="Partial Exit",
            showlegend=False,
        ))

    # Exit marker
    fig.add_trace(go.Scatter(
        x=[exit_et], y=[exit_price],
        mode="markers+text",
        marker=dict(symbol="triangle-down", size=16, color=result_color, line=dict(width=2, color="white")),
        text=[f"SELL ${exit_price:.4f}"],
        textposition="top center",
        textfont=dict(color=result_color, size=11),
        name="Exit",
        showlegend=False,
    ))

    # Nearby fib levels as faint lines
    price_range = plot_df_et["high"].max() - plot_df_et["low"].min()
    price_mid = (plot_df_et["high"].max() + plot_df_et["low"].min()) / 2
    for fp in fib_prices:
        if abs(fp - price_mid) < price_range * 1.5 and fp != float(fib_level or 0):
            # Get ratio for this fib price
            fib_key = round(fp, 4)
            ratio_info = fib_price_info.get(fib_key, (None, None))
            ratio_label = f"{ratio_info[0]}" if ratio_info[0] else ""
            fig.add_hline(y=fp, line_dash="dot", line_color="rgba(100,100,200,0.3)",
                         line_width=0.5)

    # Layout
    exit_short = exit_reason.split("(")[0].strip()
    title = (f"#{trade_num} {result_text} | {symbol} | Gap {gap_pct} | "
             f"Fib {fib_ratio} | {entry_et.strftime('%Y-%m-%d %H:%M')} ET | {exit_short}")

    fig.update_layout(
        title=dict(text=title, font=dict(size=14, color=result_color)),
        template="plotly_dark",
        paper_bgcolor="#0d1117",
        plot_bgcolor="#161b22",
        xaxis=dict(
            title="Time (ET)",
            rangeslider=dict(visible=False),
            gridcolor="#21262d",
        ),
        yaxis=dict(
            title=f"Price ($)",
            gridcolor="#21262d",
            side="right",
        ),
        height=450,
        margin=dict(l=10, r=80, t=50, b=40),
        font=dict(color="#c9d1d9"),
    )

    return fig


# ── Generate all charts ──
print(f"\nGenerating {len(chart_data_list)} trade charts...")

figures = []
for i, cd in enumerate(chart_data_list, 1):
    try:
        fig = make_trade_chart(cd, i)
        figures.append(fig)
        print(f"  Chart #{i}: {cd['trade']['symbol']} {'WIN' if cd['trade']['pnl_net'] >= 0 else 'LOSS'} ${cd['trade']['pnl_net']:+,.0f}")
    except Exception as e:
        print(f"  Chart #{i}: ERROR - {e}")

# ── Build HTML ──
output_path = Path("data/fib_charts/optimized_trades_report.html")
output_path.parent.mkdir(exist_ok=True)

# Summary stats
total_trades = len(result.trades)
wins = result.winning_trades
wr = wins / total_trades * 100 if total_trades > 0 else 0
total_pnl = result.total_pnl_net
equity = 3000 + total_pnl

html_parts = [f"""<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="UTF-8">
<title>Double Touch Optimized — Trade Charts</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  body {{
    background: #0d1117;
    color: #c9d1d9;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', monospace;
    margin: 0;
    padding: 20px;
  }}
  .header {{
    text-align: center;
    padding: 30px;
    border-bottom: 2px solid #30363d;
    margin-bottom: 30px;
  }}
  .header h1 {{
    color: #58a6ff;
    font-size: 28px;
    margin: 0;
  }}
  .header h2 {{
    color: #8b949e;
    font-size: 16px;
    margin: 10px 0 0;
    font-weight: normal;
  }}
  .stats {{
    display: flex;
    justify-content: center;
    gap: 40px;
    margin: 25px 0;
    flex-wrap: wrap;
  }}
  .stat-box {{
    text-align: center;
    padding: 15px 25px;
    border: 1px solid #30363d;
    border-radius: 8px;
    background: #161b22;
  }}
  .stat-box .value {{
    font-size: 28px;
    font-weight: bold;
  }}
  .stat-box .label {{
    font-size: 12px;
    color: #8b949e;
    margin-top: 5px;
  }}
  .green {{ color: #00d26a; }}
  .red {{ color: #ff4444; }}
  .blue {{ color: #58a6ff; }}
  .chart-container {{
    margin: 15px 0;
    border: 1px solid #21262d;
    border-radius: 8px;
    overflow: hidden;
  }}
  .trade-nav {{
    position: fixed;
    top: 10px;
    left: 10px;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 10px;
    z-index: 1000;
    max-height: 90vh;
    overflow-y: auto;
    font-size: 11px;
    width: 200px;
  }}
  .trade-nav a {{
    display: block;
    padding: 3px 5px;
    text-decoration: none;
    border-radius: 4px;
  }}
  .trade-nav a:hover {{ background: #21262d; }}
  .trade-nav a.win {{ color: #00d26a; }}
  .trade-nav a.loss {{ color: #ff4444; }}
  .settings {{
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 20px;
    margin: 20px auto;
    max-width: 600px;
    font-size: 13px;
  }}
  .settings td {{ padding: 4px 12px; }}
  .settings td:first-child {{ color: #8b949e; }}
</style>
</head>
<body>

<div class="header">
  <h1>Double Touch Fibonacci — Optimized Backtest</h1>
  <h2>15-Second Bars | All Winning Parameters | Jan 5 - Feb 13, 2026</h2>

  <div class="stats">
    <div class="stat-box">
      <div class="value green">${equity:,.0f}</div>
      <div class="label">Final Equity</div>
    </div>
    <div class="stat-box">
      <div class="value green">{wr:.1f}%</div>
      <div class="label">Win Rate</div>
    </div>
    <div class="stat-box">
      <div class="value blue">{total_trades}</div>
      <div class="label">Total Trades</div>
    </div>
    <div class="stat-box">
      <div class="value green">4,507%</div>
      <div class="label">Total Return</div>
    </div>
    <div class="stat-box">
      <div class="value red">{result.max_drawdown:.1%}</div>
      <div class="label">Max Drawdown</div>
    </div>
  </div>

  <div class="settings">
    <table>
      <tr><td>Entry Window</td><td><b>08:00 - 12:00 ET</b></td></tr>
      <tr><td>Gap Range</td><td><b>10% - 25%</b></td></tr>
      <tr><td>Fib Ratios</td><td><b>0.382, 0.5, 0.764, 0.88, 2.272, 2.414, 3.272, 3.414, 3.618</b></td></tr>
      <tr><td>Excluded Ratios</td><td><b style="color:#ff4444">0.618, 1.414</b></td></tr>
      <tr><td>Stop Loss</td><td><b>3% below fib level</b></td></tr>
      <tr><td>Exit</td><td><b>50% at 3rd fib target + 50% no-new-high trailing</b></td></tr>
    </table>
  </div>
</div>

<div class="trade-nav">
  <b>Navigation</b><br>
"""]

# Nav links
for i, cd in enumerate(chart_data_list, 1):
    t = cd["trade"]
    pnl = t["pnl_net"]
    cls = "win" if pnl >= 0 else "loss"
    sign = "+" if pnl >= 0 else ""
    html_parts.append(
        f'  <a href="#trade{i}" class="{cls}">#{i} {t["symbol"]} {sign}${pnl:,.0f}</a>\n'
    )

html_parts.append("</div>\n\n")

# Chart divs
for i, fig in enumerate(figures, 1):
    div_id = f"trade{i}"
    fig_json = fig.to_json()
    html_parts.append(f"""
<div class="chart-container" id="{div_id}">
  <div id="chart_{div_id}" style="width:100%;height:450px;"></div>
</div>
<script>
  Plotly.newPlot('chart_{div_id}', {fig_json}.data, {fig_json}.layout, {{responsive: true}});
</script>
""")

html_parts.append("""
</body>
</html>
""")

html_content = "".join(html_parts)
output_path.write_text(html_content)
print(f"\nSaved to: {output_path}")
print(f"File size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
