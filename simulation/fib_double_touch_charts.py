"""Double-Touch Fib Backtest — Interactive HTML Dashboard + Per-Trade Charts.

Generates a single self-contained HTML with:
  1. Summary metrics header (trades, win rate, P&L, equity, drawdown)
  2. Equity curve + drawdown charts
  3. Per-trade 15-sec candlestick charts showing:
     - OHLC candles (green/red)
     - Fib support levels (blue S1 / orange S2)
     - Entry marker (green triangle-up)
     - First exit marker (blue star — 50% target hit)
     - Final exit marker (red triangle-down — trailing/stop)
     - Stop loss line (red dashed), target line (blue dashed)
     - Double-touch annotations (touch #1 and touch #2)
  4. Full trade log table

Output: data/fib_charts/fib_double_touch_report.html
"""

import json
import logging
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from config.settings import (
    FIB_CHARTS_DIR, DATA_DIR, STARTING_CAPITAL,
    FIB_DT_STOP_PCT, FIB_DT_TARGET_LEVELS,
)

logger = logging.getLogger("trading_bot.fib_dt_charts")

# Theme colors
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
CANDLE_UP = "#26a69a"
CANDLE_DOWN = "#ef5350"
TARGET_STAR_COLOR = "#00bcd4"  # cyan for partial exit


def generate_double_touch_report(
    trade_charts_data: list[dict],
    result,
    starting_capital: float = STARTING_CAPITAL,
    output_path: Path | None = None,
) -> Path:
    """Generate the full HTML dashboard with per-trade charts.

    Args:
        trade_charts_data: From FibDoubleTouchEngine.get_chart_data()
        result: SimResult with aggregate stats
        starting_capital: Initial capital
        output_path: Override output location

    Returns:
        Path to generated HTML file.
    """
    output_path = output_path or FIB_CHARTS_DIR / "fib_double_touch_report.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Metrics
    total = result.total_trades
    wins = result.winning_trades
    losses = result.losing_trades
    wr = (wins / total * 100) if total > 0 else 0
    net_pnl = result.total_pnl_net
    max_dd = result.max_drawdown

    # Build equity curve data
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

    # Trade table
    table_html = _build_trade_table(result.trades)

    # Equity + drawdown JS
    trade_nums = list(range(1, total + 1))
    pnl_color = WIN if net_pnl >= 0 else LOSS
    pnl_sign = "+" if net_pnl >= 0 else ""

    # Final equity
    final_eq = equity_data[-1] if equity_data else starting_capital

    # Profit factor
    gross_wins = sum(t["pnl_net"] for t in result.trades if t["pnl_net"] >= 0)
    gross_losses = abs(sum(t["pnl_net"] for t in result.trades if t["pnl_net"] < 0))
    pf = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    # Avg win/loss
    win_trades = [t["pnl_net"] for t in result.trades if t["pnl_net"] >= 0]
    loss_trades = [t["pnl_net"] for t in result.trades if t["pnl_net"] < 0]
    avg_win = sum(win_trades) / len(win_trades) if win_trades else 0
    avg_loss = sum(loss_trades) / len(loss_trades) if loss_trades else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Double-Touch Fib Backtest Dashboard</title>
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

<h1>Double-Touch Fibonacci Support Backtest</h1>
<p class="subtitle">
  15-sec bars | {FIB_DT_STOP_PCT:.0%} stop below fib |
  {FIB_DT_TARGET_LEVELS} fib levels target (50%) + trailing (50%) |
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

<div class="card">
  <h2>Equity Curve</h2>
  <div id="equity" class="plot" style="height:400px"></div>
</div>

<div class="card">
  <h2>Drawdown from Peak</h2>
  <div id="drawdown" class="plot" style="height:280px"></div>
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
</script>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    logger.info(f"Dashboard generated -> {output_path}")
    print(f"Dashboard saved to: {output_path}")
    return output_path


def _build_trade_chart(trade_num: int, chart_data: dict) -> go.Figure:
    """Build a single Plotly candlestick chart for one trade."""
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
    target_price = trade.get("target_price", 0)
    fib_level = trade.get("fib_level", 0)
    fib_ratio = trade.get("fib_ratio", "")
    fib_series_name = trade.get("fib_series", "")
    touch1_bar = trade.get("touch1_bar", "")
    touch2_bar = trade.get("touch2_bar", "")
    partial_exit_price = trade.get("partial_exit_price", "")
    partial_exit_qty = trade.get("partial_exit_qty", "")

    is_win = pnl_net >= 0
    result_tag = "WIN" if is_win else "LOSS"
    pnl_sign = "+" if is_win else ""

    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=day_df.index,
        open=day_df["open"],
        high=day_df["high"],
        low=day_df["low"],
        close=day_df["close"],
        increasing_line_color=CANDLE_UP,
        decreasing_line_color=CANDLE_DOWN,
        increasing_fillcolor=CANDLE_UP,
        decreasing_fillcolor=CANDLE_DOWN,
        name="Price",
        showlegend=False,
    ))

    # Price range for fib level visibility
    price_low = float(day_df["low"].min())
    price_high = float(day_df["high"].max())
    price_margin = (price_high - price_low) * 0.15
    visible_low = price_low - price_margin
    visible_high = price_high + price_margin

    x_start = day_df.index[0]
    x_end = day_df.index[-1]

    # Fibonacci levels
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

    # Target line
    if target_price and isinstance(target_price, (int, float)) and target_price > 0:
        fig.add_trace(go.Scatter(
            x=[x_start, x_end], y=[target_price, target_price],
            mode="lines",
            line=dict(color=BLUE, width=1.5, dash="dash"),
            name=f"Target ${target_price:.4f}",
            hovertemplate=f"Target: ${target_price:.4f}<extra></extra>",
        ))

    # Double-touch annotations
    if isinstance(touch1_bar, (int, float)) and touch1_bar >= 0 and int(touch1_bar) < len(day_df):
        t1_idx = int(touch1_bar)
        t1_time = day_df.index[t1_idx]
        t1_low = float(day_df["low"].iloc[t1_idx])
        fig.add_trace(go.Scatter(
            x=[t1_time], y=[t1_low],
            mode="markers+text",
            marker=dict(symbol="circle", size=10, color=ORANGE,
                        line=dict(width=1, color="white")),
            text=["Touch #1"],
            textposition="bottom center",
            textfont=dict(color=ORANGE, size=9),
            name="Touch #1",
            hovertemplate=f"Touch #1<br>${t1_low:.4f}<br>%{{x}}<extra></extra>",
        ))

    if isinstance(touch2_bar, (int, float)) and touch2_bar >= 0 and int(touch2_bar) < len(day_df):
        t2_idx = int(touch2_bar)
        t2_time = day_df.index[t2_idx]
        t2_low = float(day_df["low"].iloc[t2_idx])
        fig.add_trace(go.Scatter(
            x=[t2_time], y=[t2_low],
            mode="markers+text",
            marker=dict(symbol="circle", size=10, color=ORANGE,
                        line=dict(width=1, color="white")),
            text=["Touch #2"],
            textposition="bottom center",
            textfont=dict(color=ORANGE, size=9),
            name="Touch #2",
            hovertemplate=f"Touch #2<br>${t2_low:.4f}<br>%{{x}}<extra></extra>",
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
            f"Fib: ${fib_level:.4f} ({fib_series_name} {fib_ratio})<br>"
            f"%{{x}}<extra></extra>"
        ),
    ))

    # Partial exit marker (50% target hit)
    if partial_exit_price and isinstance(partial_exit_price, (int, float)) and partial_exit_price > 0:
        partial_time_str = trade.get("partial_exit_time", "")
        if partial_time_str:
            partial_time = pd.Timestamp(partial_time_str)
        else:
            # Approximate: find bar where high >= target
            partial_time = entry_time  # fallback
            for idx in range(len(day_df)):
                if float(day_df["high"].iloc[idx]) >= partial_exit_price:
                    if day_df.index[idx] >= entry_time:
                        partial_time = day_df.index[idx]
                        break

        fig.add_trace(go.Scatter(
            x=[partial_time], y=[partial_exit_price],
            mode="markers+text",
            marker=dict(symbol="star", size=14, color=TARGET_STAR_COLOR,
                        line=dict(width=1, color="white")),
            text=[f"50% @ ${partial_exit_price:.4f}"],
            textposition="top center",
            textfont=dict(color=TARGET_STAR_COLOR, size=9),
            name=f"50% Exit ${partial_exit_price:.4f}",
            hovertemplate=(
                f"50% TARGET HIT<br>${partial_exit_price:.4f}<br>"
                f"Qty: {partial_exit_qty}<br>%{{x}}<extra></extra>"
            ),
        ))

    # Final exit marker
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
    anchor_date = anchor_info.get("anchor_date", "?")
    anchor_low = anchor_info.get("anchor_low", 0)
    anchor_high = anchor_info.get("anchor_high", 0)

    title_text = (
        f"<b>#{trade_num} {result_tag}</b> {symbol} | "
        f"Gap +{gap_pct:.1f}% | "
        f"P&L: <span style='color:{exit_color}'>{pnl_sign}${pnl_net:.2f}</span> | "
        f"{exit_reason}"
    )
    subtitle_text = (
        f"Anchor: {anchor_date} Low=${anchor_low:.4f} High=${anchor_high:.4f} | "
        f"Fib: ${fib_level:.4f} ({fib_series_name} {fib_ratio}) | "
        f"Stop: ${stop_price:.4f} | Target: ${target_price:.4f}"
        if isinstance(stop_price, (int, float)) and stop_price > 0
        else f"Anchor: {anchor_date} | Fib: ${fib_level:.4f}"
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
        "#", "Symbol", "Entry Time", "Exit Time",
        "Entry $", "Exit $", "Qty", "P&L",
        "Fib Level", "Exit Reason",
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
        elif "target" in reason.lower():
            reason_short = "Target Hit"
        elif "trailing" in reason.lower():
            reason_short = "Trailing Stop"
        elif "eod" in reason.lower():
            reason_short = "EOD Close"
        else:
            reason_short = reason[:25]

        entry_t = str(t["entry_time"])[:19]
        exit_t = str(t["exit_time"])[:19]

        rows.append(
            f'<tr class="{cls}">'
            f"<td>{i}</td>"
            f"<td><b>{t['symbol']}</b></td>"
            f"<td>{entry_t}</td><td>{exit_t}</td>"
            f"<td>${t['entry_fill']:.4f}</td>"
            f"<td>${t['exit_fill']:.4f}</td>"
            f"<td>{t['quantity']}</td>"
            f'<td class="{pnl_cls}">{ps}${pnl:.2f}</td>'
            f"<td>${t.get('fib_level', '')}</td>"
            f"<td>{reason_short}</td>"
            f"</tr>"
        )

    return f"""<div class="table-wrap">
<table class="trade-table">
<thead><tr>{th_cells}</tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>
</div>"""
