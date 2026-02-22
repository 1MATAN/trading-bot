"""Momentum Ride Backtest — Interactive HTML Dashboard + Per-Trade Charts.

Generates a single self-contained HTML with:
  1. Summary metrics header (trades, win rate, P&L, equity, drawdown)
  2. Equity curve + drawdown charts
  3. Time analysis: win rate by hour (bar chart)
  4. Per-trade 1-min candlestick charts showing:
     - OHLC candles (green/red)
     - Fib levels (S1 blue / S2 orange)
     - VWAP line (yellow)
     - Entry marker (green triangle-up)
     - Exit marker (red/green triangle-down)
     - Stop loss line (red dashed)
  5. Full trade log table with time segment column

Output: data/fib_charts/momentum_ride_report.html
"""

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from config.settings import (
    FIB_CHARTS_DIR, DATA_DIR, STARTING_CAPITAL,
    MR_STOP_EXTRA_PCT, MR_VWAP_MAX_DISTANCE_PCT,
    MR_EXIT_BARS_IN_MINUTE, MR_TRACKING_MINUTES,
)

logger = logging.getLogger("trading_bot.mr_charts")

# Theme colors (same as V1 for consistency)
BG = "#0e1117"
CARD_BG = "#1a1f2e"
BORDER = "#2a3040"
GRID = "#1e2530"
TEXT = "#fafafa"
TEXT_DIM = "#8b949e"
WIN = "#00d26a"
LOSS = "#f23645"
BLUE = "#2962ff"
ORANGE = "#ff9800"
YELLOW = "#ffd600"
CANDLE_UP = "#26a69a"
CANDLE_DOWN = "#ef5350"


def generate_momentum_ride_report(
    trade_charts_data: list[dict],
    result,
    starting_capital: float = STARTING_CAPITAL,
    output_path: Path | None = None,
) -> Path:
    """Generate the full HTML dashboard with per-trade charts.

    Args:
        trade_charts_data: From MomentumRideEngine.get_chart_data()
        result: SimResult with aggregate stats
        starting_capital: Initial capital
        output_path: Override output location

    Returns:
        Path to generated HTML file.
    """
    output_path = output_path or FIB_CHARTS_DIR / "momentum_ride_report.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total = result.total_trades
    wins = result.winning_trades
    losses = result.losing_trades
    wr = (wins / total * 100) if total > 0 else 0
    net_pnl = result.total_pnl_net
    max_dd = result.max_drawdown

    # Equity curve
    equity_data = [starting_capital]
    for t in result.trades:
        equity_data.append(equity_data[-1] + t["pnl_net"])

    # Drawdown
    peak = starting_capital
    dd_pcts = []
    for eq in equity_data[1:]:
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak * 100 if peak > 0 else 0
        dd_pcts.append(round(-dd, 2))

    # Time analysis
    hour_stats: dict[int, dict] = {}
    for t in result.trades:
        try:
            ts = pd.Timestamp(t["entry_time"])
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

    hours_sorted = sorted(hour_stats.keys())
    hour_labels = [f"{h:02d}:00" for h in hours_sorted]
    hour_win_rates = []
    hour_pnls = []
    hour_totals = []
    for h in hours_sorted:
        s = hour_stats[h]
        t_h = s["wins"] + s["losses"]
        hour_totals.append(t_h)
        hour_win_rates.append(round(s["wins"] / t_h * 100, 1) if t_h > 0 else 0)
        hour_pnls.append(round(s["pnl"], 2))

    # Per-trade chart fragments
    chart_fragments = []
    for idx, chart_data in enumerate(trade_charts_data, 1):
        try:
            fig = _build_trade_chart(idx, chart_data)
            chart_html = fig.to_html(
                full_html=False,
                include_plotlyjs=False,
                config={"displayModeBar": True, "scrollZoom": True},
            )
            chart_fragments.append(f'<div class="card">{chart_html}</div>')
        except Exception as e:
            logger.error(f"Failed chart #{idx}: {e}", exc_info=True)
            chart_fragments.append(
                f'<div class="card"><p>#{idx} — chart error: {e}</p></div>'
            )

    table_html = _build_trade_table(result.trades)

    trade_nums = list(range(1, total + 1))
    pnl_color = WIN if net_pnl >= 0 else LOSS
    pnl_sign = "+" if net_pnl >= 0 else ""
    final_eq = equity_data[-1] if equity_data else starting_capital

    gross_wins = sum(t["pnl_net"] for t in result.trades if t["pnl_net"] >= 0)
    gross_losses = abs(sum(t["pnl_net"] for t in result.trades if t["pnl_net"] < 0))
    pf = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    win_trades = [t["pnl_net"] for t in result.trades if t["pnl_net"] >= 0]
    loss_trades = [t["pnl_net"] for t in result.trades if t["pnl_net"] < 0]
    avg_win = sum(win_trades) / len(win_trades) if win_trades else 0
    avg_loss = sum(loss_trades) / len(loss_trades) if loss_trades else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Momentum Ride Backtest Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  background: {BG}; color: {TEXT};
  font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  padding: 24px; line-height: 1.5;
}}
h1 {{ font-size: 26px; font-weight: 700; margin-bottom: 6px; }}
.subtitle {{ color: {TEXT_DIM}; font-size: 14px; margin-bottom: 24px; }}
.metrics {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
  gap: 12px; margin-bottom: 28px;
}}
.metric {{
  background: {CARD_BG}; border: 1px solid {BORDER};
  border-radius: 8px; padding: 16px; text-align: center;
}}
.metric .val {{ font-size: 24px; font-weight: 700; margin-bottom: 2px; }}
.metric .lbl {{ font-size: 11px; color: {TEXT_DIM}; text-transform: uppercase; letter-spacing: 0.5px; }}
.card {{
  background: {CARD_BG}; border: 1px solid {BORDER};
  border-radius: 8px; padding: 18px; margin-bottom: 20px;
}}
.card h2 {{
  font-size: 16px; font-weight: 600; margin-bottom: 12px;
  padding-bottom: 8px; border-bottom: 1px solid {BORDER};
}}
.plot {{ width: 100%; }}
.row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
@media (max-width: 900px) {{ .row {{ grid-template-columns: 1fr; }} }}
.trade-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
.trade-table th {{
  background: #161b26; color: {TEXT_DIM}; font-size: 11px;
  text-transform: uppercase; letter-spacing: 0.5px;
  padding: 10px 8px; text-align: left; position: sticky; top: 0;
  border-bottom: 2px solid {BORDER};
}}
.trade-table td {{ padding: 8px; border-bottom: 1px solid {GRID}; }}
.trade-table tr.win {{ background: rgba(0, 210, 106, 0.05); }}
.trade-table tr.loss {{ background: rgba(242, 54, 69, 0.05); }}
.trade-table tr:hover {{ background: rgba(255,255,255,0.04); }}
.pos {{ color: {WIN}; }} .neg {{ color: {LOSS}; }}
.table-wrap {{ max-height: 600px; overflow-y: auto; border: 1px solid {BORDER}; border-radius: 6px; }}
</style>
</head>
<body>

<h1>Momentum Ride Backtest</h1>
<p class="subtitle">
  15-sec bars | Entry: 1-min low &gt; prev low + VWAP 0-{MR_VWAP_MAX_DISTANCE_PCT:.0%} |
  Stop: fib below - {MR_STOP_EXTRA_PCT:.0%} |
  Exit: {MR_EXIT_BARS_IN_MINUTE}/4 bars no new high |
  ${starting_capital:,.0f} starting capital | {total} trades
</p>

<div class="metrics">
  <div class="metric"><div class="val">{total}</div><div class="lbl">Total Trades</div></div>
  <div class="metric"><div class="val" style="color:{WIN}">{wins}</div><div class="lbl">Wins</div></div>
  <div class="metric"><div class="val" style="color:{LOSS}">{losses}</div><div class="lbl">Losses</div></div>
  <div class="metric"><div class="val" style="color:{WIN if wr >= 50 else LOSS}">{wr:.1f}%</div><div class="lbl">Win Rate</div></div>
  <div class="metric"><div class="val" style="color:{pnl_color}">{pnl_sign}${net_pnl:,.2f}</div><div class="lbl">Net P&amp;L</div></div>
  <div class="metric"><div class="val">${starting_capital:,.0f} &rarr; ${final_eq:,.0f}</div><div class="lbl">Capital</div></div>
  <div class="metric"><div class="val" style="color:{LOSS}">{max_dd:.1%}</div><div class="lbl">Max Drawdown</div></div>
  <div class="metric"><div class="val">{pf:.2f}</div><div class="lbl">Profit Factor</div></div>
  <div class="metric"><div class="val" style="color:{WIN}">${avg_win:,.2f}</div><div class="lbl">Avg Win</div></div>
  <div class="metric"><div class="val" style="color:{LOSS}">${avg_loss:,.2f}</div><div class="lbl">Avg Loss</div></div>
</div>

<div class="row">
  <div class="card">
    <h2>Equity Curve</h2>
    <div id="equity" class="plot" style="height:350px"></div>
  </div>
  <div class="card">
    <h2>Drawdown from Peak</h2>
    <div id="drawdown" class="plot" style="height:350px"></div>
  </div>
</div>

<div class="row">
  <div class="card">
    <h2>Win Rate by Hour</h2>
    <div id="hour_wr" class="plot" style="height:350px"></div>
  </div>
  <div class="card">
    <h2>P&amp;L by Hour</h2>
    <div id="hour_pnl" class="plot" style="height:350px"></div>
  </div>
</div>

<h2 style="margin: 30px 0 16px 0; font-size: 22px; border-bottom: 1px solid {BORDER}; padding-bottom: 10px;">
  Per-Trade Charts ({total} trades)
</h2>
{''.join(chart_fragments)}

<div class="card">
  <h2>Trade Log</h2>
  {table_html}
</div>

<script>
var C = {{
  bg: '{BG}', grid: '{GRID}', text: '{TEXT}', dim: '{TEXT_DIM}',
  win: '{WIN}', loss: '{LOSS}', blue: '{BLUE}', orange: '{ORANGE}'
}};
var cfg = {{displayModeBar: true, scrollZoom: true, responsive: true}};

function makeLayout(extra) {{
  var base = {{
    paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
    font: {{color: C.text, size: 12}},
    margin: {{l: 60, r: 20, t: 20, b: 50}},
    xaxis: {{gridcolor: C.grid, zerolinecolor: C.grid}},
    yaxis: {{gridcolor: C.grid, zerolinecolor: C.grid}},
  }};
  for (var k in extra) {{
    if (k === 'xaxis' || k === 'yaxis') {{
      for (var kk in extra[k]) base[k][kk] = extra[k][kk];
    }} else {{ base[k] = extra[k]; }}
  }}
  return base;
}}

// Equity curve
var eq = {json.dumps(equity_data)};
var nums = {json.dumps(list(range(len(equity_data))))};
Plotly.newPlot('equity', [
  {{x: nums, y: eq, type: 'scatter', mode: 'lines',
    fill: 'tozeroy', line: {{color: C.win, width: 2}},
    fillcolor: 'rgba(0,210,106,0.08)', hovertemplate: 'Trade #%{{x}}<br>$%{{y:,.2f}}<extra></extra>'}}
], makeLayout({{
  xaxis: {{title: 'Trade #'}}, yaxis: {{title: 'Equity ($)', tickprefix: '$'}},
  shapes: [{{type:'line', x0:0, x1:{total}, y0:{starting_capital}, y1:{starting_capital},
    line:{{color:C.dim, width:1, dash:'dot'}}}}]
}}), cfg);

// Drawdown
var dd = {json.dumps(dd_pcts)};
var ddNums = {json.dumps(trade_nums)};
Plotly.newPlot('drawdown', [{{
  x: ddNums, y: dd, type: 'scatter', mode: 'lines',
  fill: 'tozeroy', line: {{color: C.loss, width: 2}},
  fillcolor: 'rgba(242,54,69,0.15)',
  hovertemplate: 'Trade #%{{x}}<br>DD: %{{y:.2f}}%<extra></extra>'
}}], makeLayout({{
  xaxis: {{title: 'Trade #'}}, yaxis: {{title: 'Drawdown (%)', ticksuffix: '%'}}
}}), cfg);

// Win rate by hour
var hrLabels = {json.dumps(hour_labels)};
var hrWR = {json.dumps(hour_win_rates)};
var hrTotals = {json.dumps(hour_totals)};
Plotly.newPlot('hour_wr', [{{
  x: hrLabels, y: hrWR, type: 'bar',
  marker: {{color: hrWR.map(function(v) {{ return v >= 50 ? C.win : C.loss; }})}},
  text: hrTotals.map(function(n, i) {{ return hrWR[i] + '% (' + n + ')'; }}),
  textposition: 'outside', textfont: {{color: C.text, size: 11}},
  hovertemplate: '%{{x}}<br>WR: %{{y:.1f}}%<br>Trades: %{{text}}<extra></extra>'
}}], makeLayout({{
  xaxis: {{title: 'Hour (ET)'}}, yaxis: {{title: 'Win Rate (%)', ticksuffix: '%', range: [0, 100]}},
  shapes: [{{type:'line', x0:-0.5, x1:{len(hours_sorted)-0.5}, y0:50, y1:50,
    line:{{color:C.dim, width:1, dash:'dot'}}}}]
}}), cfg);

// P&L by hour
var hrPnL = {json.dumps(hour_pnls)};
Plotly.newPlot('hour_pnl', [{{
  x: hrLabels, y: hrPnL, type: 'bar',
  marker: {{color: hrPnL.map(function(v) {{ return v >= 0 ? C.win : C.loss; }})}},
  text: hrPnL.map(function(v) {{ return (v >= 0 ? '+' : '') + '$' + v.toFixed(2); }}),
  textposition: 'outside', textfont: {{color: C.text, size: 11}},
  hovertemplate: '%{{x}}<br>P&L: $%{{y:,.2f}}<extra></extra>'
}}], makeLayout({{
  xaxis: {{title: 'Hour (ET)'}}, yaxis: {{title: 'P&L ($)', tickprefix: '$'}}
}}), cfg);
</script>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    logger.info(f"Dashboard generated -> {output_path}")
    print(f"Dashboard saved to: {output_path}")
    return output_path


def _build_trade_chart(trade_num: int, chart_data: dict) -> go.Figure:
    """Build a Plotly chart for one trade — 1-min candles with fibs + VWAP."""
    trade = chart_data["trade"]
    day_df = chart_data["day_df"]
    fib_prices = chart_data["fib_prices"]
    fib_price_info = chart_data["fib_price_info"]
    anchor_info = chart_data["anchor_info"]
    gap = chart_data["gap"]

    symbol = trade["symbol"]
    entry_price = trade["entry_fill"]
    exit_price = trade["exit_fill"]
    entry_time = pd.Timestamp(trade["entry_time"])
    exit_time = pd.Timestamp(trade["exit_time"])
    pnl_net = trade["pnl_net"]
    exit_reason = trade["exit_reason"]
    stop_price = trade.get("stop_price", 0)
    fib_level = trade.get("fib_level", 0)
    fib_ratio = trade.get("fib_ratio", "")
    fib_series_name = trade.get("fib_series", "")
    vwap_dist = trade.get("vwap_dist_pct", "")

    is_win = pnl_net >= 0
    result_tag = "WIN" if is_win else "LOSS"
    pnl_sign = "+" if is_win else ""

    # Build 1-min candles for charting
    min_df = day_df.resample("1min").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna()

    # Compute VWAP for the line
    tp = (day_df["high"].values + day_df["low"].values + day_df["close"].values) / 3.0
    vol = day_df["volume"].values.astype(float)
    cum_tp_vol = np.cumsum(tp * vol)
    cum_vol = np.cumsum(vol)
    vwap_vals = np.where(cum_vol > 0, cum_tp_vol / cum_vol, 0.0)

    # Resample VWAP to 1-min (take last value per minute)
    vwap_series = pd.Series(vwap_vals, index=day_df.index)
    vwap_1min = vwap_series.resample("1min").last().dropna()

    fig = go.Figure()

    # Candlestick (1-min)
    fig.add_trace(go.Candlestick(
        x=min_df.index,
        open=min_df["open"],
        high=min_df["high"],
        low=min_df["low"],
        close=min_df["close"],
        increasing_line_color=CANDLE_UP,
        decreasing_line_color=CANDLE_DOWN,
        increasing_fillcolor=CANDLE_UP,
        decreasing_fillcolor=CANDLE_DOWN,
        name="Price",
        showlegend=False,
    ))

    # VWAP line
    fig.add_trace(go.Scatter(
        x=vwap_1min.index,
        y=vwap_1min.values,
        mode="lines",
        line=dict(color=YELLOW, width=1.5),
        name="VWAP",
        hovertemplate="VWAP: $%{y:.4f}<extra></extra>",
    ))

    # Price range for fib level visibility
    price_low = float(min_df["low"].min())
    price_high = float(min_df["high"].max())
    price_margin = (price_high - price_low) * 0.15
    visible_low = price_low - price_margin
    visible_high = price_high + price_margin

    x_start = min_df.index[0]
    x_end = min_df.index[-1]

    # Fib levels
    fib_labels_added = set()
    for fp in fib_prices:
        if fp < visible_low or fp > visible_high:
            continue
        fp_key = round(fp, 4)
        ratio, series_name = fib_price_info.get(fp_key, (0, "?"))
        color = BLUE if series_name == "S1" else ORANGE
        show_legend = series_name not in fib_labels_added
        if show_legend:
            fib_labels_added.add(series_name)

        fig.add_trace(go.Scatter(
            x=[x_start, x_end], y=[fp, fp],
            mode="lines",
            line=dict(color=color, width=0.8, dash="dot"),
            name=f"Fib {series_name}" if show_legend else "",
            showlegend=show_legend,
            legendgroup=f"fib_{series_name}",
            hovertemplate=f"{series_name} {ratio:.3f}: ${fp:.4f}<extra></extra>",
        ))

    # Stop loss line
    if stop_price and isinstance(stop_price, (int, float)) and stop_price > 0:
        fig.add_trace(go.Scatter(
            x=[x_start, x_end], y=[stop_price, stop_price],
            mode="lines",
            line=dict(color=LOSS, width=1.5, dash="dash"),
            name=f"Stop ${stop_price:.4f}",
            hovertemplate=f"Stop: ${stop_price:.4f}<extra></extra>",
        ))

    # Entry marker
    fig.add_trace(go.Scatter(
        x=[entry_time], y=[entry_price],
        mode="markers+text",
        marker=dict(symbol="triangle-up", size=14, color=WIN,
                    line=dict(width=1, color="white")),
        text=[f"BUY ${entry_price:.4f}"],
        textposition="top center",
        textfont=dict(color=WIN, size=10),
        name=f"Entry ${entry_price:.4f}",
        hovertemplate=(
            f"ENTRY<br>${entry_price:.4f}<br>"
            f"VWAP dist: +{vwap_dist}%<br>"
            f"%{{x}}<extra></extra>"
        ),
    ))

    # Exit marker
    exit_color = WIN if is_win else LOSS
    fig.add_trace(go.Scatter(
        x=[exit_time], y=[exit_price],
        mode="markers+text",
        marker=dict(symbol="triangle-down", size=14, color=exit_color,
                    line=dict(width=1, color="white")),
        text=[f"EXIT ${exit_price:.4f}"],
        textposition="bottom center",
        textfont=dict(color=exit_color, size=10),
        name=f"Exit ${exit_price:.4f}",
        hovertemplate=(
            f"EXIT ({exit_reason})<br>"
            f"${exit_price:.4f}<br>"
            f"P&L: {pnl_sign}${pnl_net:.2f}<br>"
            f"%{{x}}<extra></extra>"
        ),
    ))

    # Title
    gap_pct = gap.get("gap_pct", 0)
    title_text = (
        f"<b>#{trade_num} {result_tag}</b> {symbol} | "
        f"Gap +{gap_pct:.1f}% | "
        f"P&L: <span style='color:{exit_color}'>{pnl_sign}${pnl_net:.2f}</span> | "
        f"{exit_reason}"
    )
    subtitle_text = (
        f"VWAP dist: +{vwap_dist}% | "
        f"Fib below: ${fib_level:.4f} ({fib_series_name} {fib_ratio}) | "
        f"Stop: ${stop_price:.4f}"
        if isinstance(stop_price, (int, float)) and stop_price > 0
        else f"VWAP dist: +{vwap_dist}%"
    )

    fig.update_layout(
        title=dict(
            text=f"{title_text}<br><span style='font-size:11px;color:{TEXT_DIM}'>{subtitle_text}</span>",
            font=dict(size=14, color=TEXT),
            x=0.01, xanchor="left",
        ),
        xaxis=dict(
            gridcolor=GRID, showgrid=True,
            rangeslider=dict(visible=False),
            type="date",
        ),
        yaxis=dict(
            gridcolor=GRID, showgrid=True,
            title="Price ($)",
            tickformat=".4f",
        ),
        plot_bgcolor=BG,
        paper_bgcolor=BG,
        font=dict(color=TEXT, size=11),
        legend=dict(
            bgcolor="rgba(0,0,0,0.3)",
            bordercolor=GRID, borderwidth=1,
            font=dict(size=10),
            x=1.01, y=1,
        ),
        height=500,
        margin=dict(l=60, r=160, t=80, b=40),
        hovermode="x unified",
    )

    return fig


def _build_trade_table(trades: list[dict]) -> str:
    """Build an HTML trade log table."""
    if not trades:
        return "<p>No trades</p>"

    headers = [
        "#", "Symbol", "Date", "Hour", "Entry Time", "Exit Time",
        "Entry $", "Exit $", "Qty", "P&L",
        "VWAP Dist", "Exit Reason",
    ]
    th_cells = "".join(f"<th>{h}</th>" for h in headers)

    rows = []
    for i, t in enumerate(trades, 1):
        pnl = t["pnl_net"]
        cls = "win" if pnl >= 0 else "loss"
        pnl_cls = "pos" if pnl >= 0 else "neg"
        ps = "+" if pnl >= 0 else ""

        reason = str(t["exit_reason"])
        if "stop_loss" in reason.lower():
            reason_short = "Stop Loss"
        elif "no_new_high" in reason.lower():
            reason_short = "No New High"
        elif "eod" in reason.lower():
            reason_short = "EOD Close"
        else:
            reason_short = reason[:25]

        entry_t = str(t["entry_time"])[:19]
        exit_t = str(t["exit_time"])[:19]
        try:
            hour = pd.Timestamp(t["entry_time"]).strftime("%H:%M")
        except Exception:
            hour = "?"
        vwap_dist = t.get("vwap_dist_pct", "?")

        rows.append(
            f'<tr class="{cls}">'
            f"<td>{i}</td>"
            f"<td><b>{t['symbol']}</b></td>"
            f"<td>{t.get('date', '')}</td>"
            f"<td>{hour}</td>"
            f"<td>{entry_t}</td><td>{exit_t}</td>"
            f"<td>${t['entry_fill']:.4f}</td>"
            f"<td>${t['exit_fill']:.4f}</td>"
            f"<td>{t['quantity']}</td>"
            f'<td class="{pnl_cls}">{ps}${pnl:.2f}</td>'
            f"<td>+{vwap_dist}%</td>"
            f"<td>{reason_short}</td>"
            f"</tr>"
        )

    return f"""<div class="table-wrap">
<table class="trade-table">
<thead><tr>{th_cells}</tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>
</div>"""
