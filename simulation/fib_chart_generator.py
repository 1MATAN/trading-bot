"""Per-trade Plotly candlestick charts for Fibonacci backtest.

Generates a single HTML file with:
  - Summary metrics header (trades, win rate, P&L, drawdown)
  - One candlestick chart per trade showing:
    - 2-min OHLC bars for the gap day
    - Fibonacci support levels (blue=S1, orange=S2)
    - Entry marker (green triangle-up) + Exit marker (red triangle-down)
    - Stop loss (red dashed) + Target (blue dashed) horizontal lines
    - Anchor candle info annotation
  - Dark theme matching existing reports (#0e1117)
"""

import logging
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config.settings import FIB_CHARTS_DIR, FIB_STOP_BUFFER_PCT, FIB_TARGET_LEVELS_ABOVE

logger = logging.getLogger("trading_bot.fib_charts")

# Dark theme colors
BG_COLOR = "#0e1117"
PAPER_COLOR = "#0e1117"
GRID_COLOR = "#1e2530"
TEXT_COLOR = "#fafafa"
WIN_COLOR = "#00d26a"
LOSS_COLOR = "#f23645"
FIB_S1_COLOR = "#2962ff"   # blue for Series 1
FIB_S2_COLOR = "#ff9800"   # orange for Series 2
STOP_COLOR = "#f23645"     # red
TARGET_COLOR = "#2962ff"   # blue
ENTRY_COLOR = "#00d26a"    # green
EXIT_WIN_COLOR = "#00d26a"
EXIT_LOSS_COLOR = "#f23645"


def generate_fib_trade_charts(
    trade_charts_data: list[dict],
    result,
    starting_capital: float,
) -> Path:
    """Generate a single HTML file with per-trade candlestick charts.

    Args:
        trade_charts_data: List of dicts with keys:
            trade, day_df, fib_prices, fib_price_info, anchor_info, gap
        result: SimResult with aggregate stats
        starting_capital: Initial capital for display

    Returns:
        Path to the generated HTML file.
    """
    html_path = FIB_CHARTS_DIR / "fib_trades_report.html"

    # Build summary metrics
    total_trades = result.total_trades
    win_rate = (result.winning_trades / total_trades * 100) if total_trades > 0 else 0
    net_pnl = result.total_pnl_net
    max_dd = result.max_drawdown

    # Build individual chart HTML fragments
    chart_divs = []
    for idx, chart_data in enumerate(trade_charts_data, 1):
        try:
            fig = _build_trade_chart(idx, chart_data)
            chart_html = fig.to_html(
                full_html=False, include_plotlyjs=False,
                config={"displayModeBar": True, "scrollZoom": True},
            )
            chart_divs.append(chart_html)
        except Exception as e:
            logger.error(f"Failed to build chart #{idx}: {e}", exc_info=True)

    # Assemble full HTML
    pnl_color = WIN_COLOR if net_pnl >= 0 else LOSS_COLOR
    pnl_sign = "+" if net_pnl >= 0 else ""

    html_content = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Fibonacci Backtest — Per-Trade Charts</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  body {{
    background: {BG_COLOR};
    color: {TEXT_COLOR};
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    margin: 0; padding: 20px;
  }}
  .summary {{
    display: flex; gap: 30px; flex-wrap: wrap;
    padding: 20px; margin-bottom: 20px;
    background: #1a1f2e; border-radius: 8px;
    border: 1px solid #2a3040;
  }}
  .metric {{
    text-align: center;
  }}
  .metric .value {{
    font-size: 28px; font-weight: 700;
  }}
  .metric .label {{
    font-size: 12px; color: #8b949e; text-transform: uppercase;
    margin-top: 4px;
  }}
  .chart-container {{
    margin-bottom: 30px;
    background: #1a1f2e; border-radius: 8px;
    padding: 10px; border: 1px solid #2a3040;
  }}
  h1 {{
    margin: 0 0 20px 0; font-size: 24px;
    border-bottom: 1px solid #2a3040; padding-bottom: 10px;
  }}
</style>
</head>
<body>
<h1>Fibonacci Support Backtest — Per-Trade Charts</h1>
<div class="summary">
  <div class="metric">
    <div class="value">{total_trades}</div>
    <div class="label">Total Trades</div>
  </div>
  <div class="metric">
    <div class="value">{result.winning_trades}</div>
    <div class="label">Wins</div>
  </div>
  <div class="metric">
    <div class="value">{result.losing_trades}</div>
    <div class="label">Losses</div>
  </div>
  <div class="metric">
    <div class="value" style="color: {WIN_COLOR if win_rate >= 50 else LOSS_COLOR}">{win_rate:.1f}%</div>
    <div class="label">Win Rate</div>
  </div>
  <div class="metric">
    <div class="value" style="color: {pnl_color}">{pnl_sign}${net_pnl:,.2f}</div>
    <div class="label">Net P&amp;L</div>
  </div>
  <div class="metric">
    <div class="value">${starting_capital:,.0f}</div>
    <div class="label">Starting Capital</div>
  </div>
  <div class="metric">
    <div class="value" style="color: {LOSS_COLOR}">{max_dd:.2%}</div>
    <div class="label">Max Drawdown</div>
  </div>
  <div class="metric">
    <div class="value">{FIB_STOP_BUFFER_PCT:.1%}</div>
    <div class="label">Stop Buffer</div>
  </div>
  <div class="metric">
    <div class="value">{FIB_TARGET_LEVELS_ABOVE}</div>
    <div class="label">Target Levels</div>
  </div>
</div>
"""

    for div in chart_divs:
        html_content += f'<div class="chart-container">{div}</div>\n'

    html_content += """
</body>
</html>
"""

    html_path.write_text(html_content)
    logger.info(f"Generated {len(chart_divs)} trade charts → {html_path}")
    return html_path


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
    target_price = trade.get("target_level", 0)
    fib_level = trade.get("fib_level", 0)
    fib_ratio = trade.get("fib_ratio", "")
    fib_series_name = trade.get("fib_series", "")

    is_win = pnl_net >= 0
    result_tag = "WIN" if is_win else "LOSS"
    pnl_sign = "+" if is_win else ""

    # Build figure
    fig = go.Figure()

    # Candlestick chart
    fig.add_trace(go.Candlestick(
        x=day_df.index,
        open=day_df["open"],
        high=day_df["high"],
        low=day_df["low"],
        close=day_df["close"],
        increasing_line_color="#26a69a",
        decreasing_line_color="#ef5350",
        increasing_fillcolor="#26a69a",
        decreasing_fillcolor="#ef5350",
        name="Price",
        showlegend=False,
    ))

    # Price range for determining which fib levels to show
    price_low = float(day_df["low"].min())
    price_high = float(day_df["high"].max())
    price_margin = (price_high - price_low) * 0.15
    visible_low = price_low - price_margin
    visible_high = price_high + price_margin

    x_start = day_df.index[0]
    x_end = day_df.index[-1]

    # Fibonacci levels — horizontal lines across the chart
    fib_labels_added = set()
    for fp in fib_prices:
        if fp < visible_low or fp > visible_high:
            continue
        fp_key = round(fp, 4)
        ratio, series_name = fib_price_info.get(fp_key, (0, "?"))
        color = FIB_S1_COLOR if series_name == "S1" else FIB_S2_COLOR
        label = f"{series_name} {ratio:.3f}" if ratio else f"${fp:.4f}"

        # Only show legend for first S1 and first S2
        show_legend = series_name not in fib_labels_added
        if show_legend:
            fib_labels_added.add(series_name)

        fig.add_trace(go.Scatter(
            x=[x_start, x_end],
            y=[fp, fp],
            mode="lines",
            line=dict(color=color, width=0.8, dash="dot"),
            name=f"Fib {series_name}" if show_legend else "",
            showlegend=show_legend,
            legendgroup=f"fib_{series_name}",
            hovertemplate=f"{label}: ${fp:.4f}<extra></extra>",
        ))

    # Stop loss line
    if stop_price and isinstance(stop_price, (int, float)) and stop_price > 0:
        fig.add_trace(go.Scatter(
            x=[x_start, x_end],
            y=[stop_price, stop_price],
            mode="lines",
            line=dict(color=STOP_COLOR, width=1.5, dash="dash"),
            name=f"Stop ${stop_price:.4f}",
            hovertemplate=f"Stop: ${stop_price:.4f}<extra></extra>",
        ))

    # Target line
    if target_price and isinstance(target_price, (int, float)) and target_price > 0:
        fig.add_trace(go.Scatter(
            x=[x_start, x_end],
            y=[target_price, target_price],
            mode="lines",
            line=dict(color=TARGET_COLOR, width=1.5, dash="dash"),
            name=f"Target ${target_price:.4f}",
            hovertemplate=f"Target: ${target_price:.4f}<extra></extra>",
        ))

    # Entry marker
    fig.add_trace(go.Scatter(
        x=[entry_time],
        y=[entry_price],
        mode="markers+text",
        marker=dict(
            symbol="triangle-up", size=14, color=ENTRY_COLOR,
            line=dict(width=1, color="white"),
        ),
        text=[f"BUY ${entry_price:.4f}"],
        textposition="top center",
        textfont=dict(color=ENTRY_COLOR, size=10),
        name=f"Entry ${entry_price:.4f}",
        hovertemplate=(
            f"ENTRY<br>"
            f"Price: ${entry_price:.4f}<br>"
            f"Fib: ${fib_level:.4f} ({fib_series_name} {fib_ratio})<br>"
            f"Time: %{{x}}<extra></extra>"
        ),
    ))

    # Exit marker
    exit_color = EXIT_WIN_COLOR if is_win else EXIT_LOSS_COLOR
    fig.add_trace(go.Scatter(
        x=[exit_time],
        y=[exit_price],
        mode="markers+text",
        marker=dict(
            symbol="triangle-down", size=14, color=exit_color,
            line=dict(width=1, color="white"),
        ),
        text=[f"SELL ${exit_price:.4f}"],
        textposition="bottom center",
        textfont=dict(color=exit_color, size=10),
        name=f"Exit ${exit_price:.4f}",
        hovertemplate=(
            f"EXIT ({exit_reason})<br>"
            f"Price: ${exit_price:.4f}<br>"
            f"P&L: {pnl_sign}${pnl_net:.2f}<br>"
            f"Time: %{{x}}<extra></extra>"
        ),
    ))

    # Title with trade info
    anchor_date = anchor_info.get("anchor_date", "?")
    anchor_low = anchor_info.get("anchor_low", 0)
    anchor_high = anchor_info.get("anchor_high", 0)

    title_text = (
        f"<b>#{trade_num} {result_tag}</b> {symbol} | "
        f"Gap +{gap['gap_pct']:.1f}% | "
        f"P&L: <span style='color:{exit_color}'>{pnl_sign}${pnl_net:.2f}</span> | "
        f"{exit_reason}"
    )
    if isinstance(stop_price, (int, float)) and stop_price > 0:
        subtitle_text = (
            f"Anchor: {anchor_date} Low=${anchor_low:.4f} High=${anchor_high:.4f} | "
            f"Fib entry: ${fib_level:.4f} ({fib_series_name} {fib_ratio}) | "
            f"Stop: ${stop_price:.4f}"
        )
        if target_price and isinstance(target_price, (int, float)) and target_price > 0:
            subtitle_text += f" | Target: ${target_price:.4f}"
    else:
        subtitle_text = f"Anchor: {anchor_date} Low=${anchor_low:.4f} High=${anchor_high:.4f}"

    fig.update_layout(
        title=dict(
            text=f"{title_text}<br><span style='font-size:11px;color:#8b949e'>{subtitle_text}</span>",
            font=dict(size=14, color=TEXT_COLOR),
            x=0.01, xanchor="left",
        ),
        xaxis=dict(
            gridcolor=GRID_COLOR, showgrid=True,
            rangeslider=dict(visible=False),
            type="date",
        ),
        yaxis=dict(
            gridcolor=GRID_COLOR, showgrid=True,
            title="Price ($)",
            tickformat=".4f",
        ),
        plot_bgcolor=BG_COLOR,
        paper_bgcolor=PAPER_COLOR,
        font=dict(color=TEXT_COLOR, size=11),
        legend=dict(
            bgcolor="rgba(0,0,0,0.3)",
            bordercolor=GRID_COLOR,
            borderwidth=1,
            font=dict(size=10),
            x=1.01, y=1,
        ),
        height=500,
        margin=dict(l=60, r=160, t=80, b=40),
        hovermode="x unified",
    )

    return fig
