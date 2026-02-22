"""Gap and Go Backtest — Interactive HTML Dashboard + Per-Trade Charts.

Generates a single self-contained HTML with:
  1. Summary metrics header (trades, win rate, P&L, equity, drawdown, profit factor)
  2. Equity curve + drawdown charts
  3. Per-trade candlestick charts showing:
     - OHLC candles (green/red)
     - VWAP line (yellow)
     - Fib levels (blue/orange + cyan for entry fib)
     - Entry marker (green triangle-up)
     - Exit marker (red/green triangle-down)
     - Stop loss line (red dashed)
  4. Full trade log table

Output: data/fib_charts/gap_and_go_report.html
"""

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config.settings import FIB_CHARTS_DIR, STARTING_CAPITAL

logger = logging.getLogger("trading_bot.gg_charts")

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
YELLOW = "#ffd600"
CYAN = "#00bcd4"
CANDLE_UP = "#26a69a"
CANDLE_DOWN = "#ef5350"


def generate_gap_and_go_report(
    trade_charts_data: list[dict],
    result,
    starting_capital: float = STARTING_CAPITAL,
    output_path: Path | None = None,
) -> Path:
    """Generate the full HTML dashboard.

    Args:
        trade_charts_data: From GapAndGoEngine.get_chart_data()
        result: SimResult with aggregate stats
        starting_capital: Initial capital
        output_path: Override output location

    Returns:
        Path to generated HTML file.
    """
    output_path = output_path or FIB_CHARTS_DIR / "gap_and_go_report.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total = result.total_trades
    wins = result.winning_trades
    losses = result.losing_trades
    wr = (wins / total * 100) if total > 0 else 0
    net_pnl = result.total_pnl_net
    max_dd = result.max_drawdown

    # Equity curve from trade P&Ls
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
<title>Gap and Go Backtest Dashboard</title>
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

<h1>Gap and Go Backtest</h1>
<p class="subtitle">
  15-sec bars | Entry: Fib pullback + above VWAP + HA green 1m |
  Stop: below fib level |
  Exit: HA red 1m + 5m |
  ${starting_capital:,.0f} capital | {total} trades
</p>

<div class="metrics">
  <div class="metric"><div class="val">{total}</div><div class="lbl">Trades</div></div>
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

<h2 style="margin: 30px 0 16px 0; font-size: 22px; border-bottom: 1px solid {BORDER}; padding-bottom: 10px;">
  Per-Trade Charts ({total} trades)
</h2>
{''.join(chart_fragments)}

<div class="card">
  <h2>Trade Log ({total} trades)</h2>
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


def _compute_ha(df: pd.DataFrame) -> pd.DataFrame:
    """Compute Heikin Ashi from OHLC DataFrame."""
    ha = pd.DataFrame(index=df.index)
    ha["close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4.0

    ha_open = [df["open"].iloc[0]]
    for i in range(1, len(df)):
        ha_open.append((ha_open[-1] + ha["close"].iloc[i - 1]) / 2.0)
    ha["open"] = ha_open

    ha["high"] = pd.concat([df["high"], ha["open"], ha["close"]], axis=1).max(axis=1)
    ha["low"] = pd.concat([df["low"], ha["open"], ha["close"]], axis=1).min(axis=1)
    return ha


def _build_trade_chart(trade_num: int, chart_data: dict) -> go.Figure:
    """Build a Plotly chart: price + VWAP (top), HA 1m (mid), HA 5m (bottom)."""
    pos_info = chart_data["position"]
    trade = chart_data["trade"]
    day_df = chart_data["day_df"]
    gap = chart_data["gap"]

    symbol = pos_info["symbol"]
    entry_price = pos_info["entry_price"]
    entry_time = pd.Timestamp(pos_info["entry_time"])
    vwap_at_entry = pos_info["vwap_at_entry"]

    pnl = trade["pnl_net"]
    is_win = pnl >= 0
    result_tag = "WIN" if is_win else "LOSS"
    pnl_sign = "+" if is_win else ""

    # Build 1-min and 5-min candles
    min_df = day_df.resample("1min").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna()

    min5_df = day_df.resample("5min").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna()

    # Compute VWAP
    tp = (day_df["high"].values + day_df["low"].values + day_df["close"].values) / 3.0
    vol = day_df["volume"].values.astype(float)
    cum_tp_vol = np.cumsum(tp * vol)
    cum_vol = np.cumsum(vol)
    vwap_vals = np.where(cum_vol > 0, cum_tp_vol / cum_vol, 0.0)
    vwap_series = pd.Series(vwap_vals, index=day_df.index)
    vwap_1min = vwap_series.resample("1min").last().dropna()

    # Compute Heikin Ashi
    ha_1m = _compute_ha(min_df)
    ha_5m = _compute_ha(min5_df)

    # 3-row subplot: Price, HA 1m, HA 5m
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.5, 0.25, 0.25],
        subplot_titles=["Price + VWAP", "Heikin Ashi 1min", "Heikin Ashi 5min"],
    )

    # ── Row 1: Regular candles + VWAP ──
    fig.add_trace(go.Candlestick(
        x=min_df.index,
        open=min_df["open"], high=min_df["high"],
        low=min_df["low"], close=min_df["close"],
        increasing_line_color=CANDLE_UP, decreasing_line_color=CANDLE_DOWN,
        increasing_fillcolor=CANDLE_UP, decreasing_fillcolor=CANDLE_DOWN,
        name="Price", showlegend=False,
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=vwap_1min.index, y=vwap_1min.values,
        mode="lines", line=dict(color=YELLOW, width=1.5),
        name="VWAP", hovertemplate="VWAP: $%{y:.4f}<extra></extra>",
    ), row=1, col=1)

    # Entry marker
    fig.add_trace(go.Scatter(
        x=[entry_time], y=[entry_price],
        mode="markers+text",
        marker=dict(symbol="triangle-up", size=14, color=WIN,
                    line=dict(width=1, color="white")),
        text=[f"BUY ${entry_price:.4f}"],
        textposition="top center", textfont=dict(color=WIN, size=10),
        name=f"Entry ${entry_price:.4f}",
    ), row=1, col=1)

    # Exit marker
    exit_time = pd.Timestamp(trade["exit_time"])
    exit_price = trade["exit_fill"]
    exit_color = WIN if pnl >= 0 else LOSS

    fig.add_trace(go.Scatter(
        x=[exit_time], y=[exit_price],
        mode="markers+text",
        marker=dict(symbol="triangle-down", size=12, color=exit_color,
                    line=dict(width=1, color="white")),
        text=[f"SELL ${exit_price:.4f}"],
        textposition="bottom center", textfont=dict(color=exit_color, size=9),
        name=f"Exit ${exit_price:.4f}",
        hovertemplate=(
            f"EXIT ${exit_price:.4f}<br>"
            f"P&L: {pnl_sign}${pnl:.2f}<br>"
            f"{trade['exit_reason']}<extra></extra>"
        ),
    ), row=1, col=1)

    # ── Row 2: HA 1-min candles ──
    fig.add_trace(go.Candlestick(
        x=ha_1m.index,
        open=ha_1m["open"], high=ha_1m["high"],
        low=ha_1m["low"], close=ha_1m["close"],
        increasing_line_color=CANDLE_UP, decreasing_line_color=CANDLE_DOWN,
        increasing_fillcolor=CANDLE_UP, decreasing_fillcolor=CANDLE_DOWN,
        name="HA 1m", showlegend=False,
    ), row=2, col=1)

    # Entry/exit lines on HA 1m
    fig.add_trace(go.Scatter(
        x=[entry_time, entry_time], y=[ha_1m["low"].min(), ha_1m["high"].max()],
        mode="lines", line=dict(color=WIN, width=1, dash="dot"),
        showlegend=False,
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=[exit_time, exit_time], y=[ha_1m["low"].min(), ha_1m["high"].max()],
        mode="lines", line=dict(color=LOSS, width=1, dash="dot"),
        showlegend=False,
    ), row=2, col=1)

    # ── Row 3: HA 5-min candles ──
    fig.add_trace(go.Candlestick(
        x=ha_5m.index,
        open=ha_5m["open"], high=ha_5m["high"],
        low=ha_5m["low"], close=ha_5m["close"],
        increasing_line_color=CANDLE_UP, decreasing_line_color=CANDLE_DOWN,
        increasing_fillcolor=CANDLE_UP, decreasing_fillcolor=CANDLE_DOWN,
        name="HA 5m", showlegend=False,
    ), row=3, col=1)

    # Entry/exit lines on HA 5m
    fig.add_trace(go.Scatter(
        x=[entry_time, entry_time], y=[ha_5m["low"].min(), ha_5m["high"].max()],
        mode="lines", line=dict(color=WIN, width=1, dash="dot"),
        showlegend=False,
    ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=[exit_time, exit_time], y=[ha_5m["low"].min(), ha_5m["high"].max()],
        mode="lines", line=dict(color=LOSS, width=1, dash="dot"),
        showlegend=False,
    ), row=3, col=1)

    # Title
    gap_pct = gap.get("gap_pct", 0)
    title_text = (
        f"<b>#{trade_num} {result_tag}</b> {symbol} | "
        f"Gap +{gap_pct:.1f}% | "
        f"P&L: <span style='color:{WIN if is_win else LOSS}'>"
        f"{pnl_sign}${pnl:.2f}</span>"
    )
    subtitle_text = (
        f"Entry: ${entry_price:.4f} | "
        f"VWAP: ${vwap_at_entry:.4f} | "
        f"{trade['exit_reason']}"
    )

    fig.update_layout(
        title=dict(
            text=f"{title_text}<br><span style='font-size:11px;color:{TEXT_DIM}'>{subtitle_text}</span>",
            font=dict(size=14, color=TEXT),
            x=0.01, xanchor="left",
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
        height=800,
        margin=dict(l=60, r=160, t=80, b=40),
        hovermode="x unified",
    )

    # Style all axes
    for i in range(1, 4):
        yaxis = f"yaxis{i}" if i > 1 else "yaxis"
        xaxis = f"xaxis{i}" if i > 1 else "xaxis"
        fig.update_layout(**{
            yaxis: dict(gridcolor=GRID, showgrid=True, tickformat=".4f"),
            xaxis: dict(gridcolor=GRID, showgrid=True, rangeslider=dict(visible=False)),
        })

    # Subtitle colors
    for ann in fig.layout.annotations:
        ann.font = dict(color=TEXT_DIM, size=11)

    return fig


def _build_trade_table(trades: list[dict]) -> str:
    """Build an HTML trade log table."""
    if not trades:
        return "<p>No trades</p>"

    headers = [
        "#", "Symbol", "Date", "Entry Time", "Exit Time",
        "Entry $", "Exit $", "Qty", "P&L", "P&L %", "Exit Reason",
    ]
    th_cells = "".join(f"<th>{h}</th>" for h in headers)

    rows = []
    for i, t in enumerate(trades, 1):
        pnl = t["pnl_net"]
        cls = "win" if pnl >= 0 else "loss"
        pnl_cls = "pos" if pnl >= 0 else "neg"
        ps = "+" if pnl >= 0 else ""

        reason = str(t["exit_reason"])
        if "below_vwap" in reason.lower():
            reason_short = "Below VWAP"
        elif "ha_exit" in reason.lower():
            reason_short = "HA Exit (1m+5m)"
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
            f"<td>{t.get('date', '')}</td>"
            f"<td>{entry_t}</td><td>{exit_t}</td>"
            f"<td>${t['entry_fill']:.4f}</td>"
            f"<td>${t['exit_fill']:.4f}</td>"
            f"<td>{t['quantity']}</td>"
            f'<td class="{pnl_cls}">{ps}${pnl:.2f}</td>'
            f"<td>{t.get('pnl_pct', '')}</td>"
            f"<td>{reason_short}</td>"
            f"</tr>"
        )

    return f"""<div class="table-wrap">
<table class="trade-table">
<thead><tr>{th_cells}</tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>
</div>"""
