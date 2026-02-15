"""Multi-timeframe Fibonacci analysis charts.

Generates per-symbol HTML pages with:
  - Daily candlestick (5y) with fib levels
  - 5-min candlestick (30d) with fib levels
  - 1-min candlestick (7d) with fib levels
  - Index page linking all symbols

Usage:
    python -c "from simulation.fib_analysis_charts import generate_analysis; generate_analysis()"
"""

import logging
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config.settings import FIB_CHARTS_DIR, BACKTEST_DATA_DIR, FIB_LOOKBACK_YEARS
from strategies.fibonacci_engine import find_anchor_candle, build_dual_series, advance_series

logger = logging.getLogger("trading_bot.fib_analysis")

# Dark theme
BG = "#0e1117"
GRID = "#1e2530"
TEXT = "#fafafa"
S1_COLOR = "#2962ff"
S2_COLOR = "#ff9800"
UP_COLOR = "#26a69a"
DN_COLOR = "#ef5350"

OUT_DIR = FIB_CHARTS_DIR / "analysis"
OUT_DIR.mkdir(exist_ok=True)


def _get_fib_data(daily_df):
    """Build fib levels from daily data. Returns (fib_prices, fib_info, anchor_info) or None."""
    anchor = find_anchor_candle(daily_df)
    if anchor is None:
        return None
    anchor_low, anchor_high, anchor_date = anchor
    dual = build_dual_series(anchor_low, anchor_high)
    day_high = float(daily_df["high"].max())

    # Auto-advance to cover price range
    for _ in range(20):
        s1_top = dual.series1.levels[-1][1] if dual.series1.levels else float("inf")
        if day_high <= s1_top:
            break
        dual = advance_series(dual)

    fib_prices = []
    fib_info = {}
    seen = set()
    for ratio, price in dual.series1.levels:
        key = round(price, 4)
        if key not in seen:
            seen.add(key)
            fib_prices.append(price)
            fib_info[key] = (ratio, "S1")
    for ratio, price in dual.series2.levels:
        key = round(price, 4)
        if key not in seen:
            seen.add(key)
            fib_prices.append(price)
            fib_info[key] = (ratio, "S2")
    fib_prices.sort()

    anchor_info = {"date": anchor_date, "low": anchor_low, "high": anchor_high}
    return fib_prices, fib_info, anchor_info


def _build_candlestick(df, title, fib_prices, fib_info, height=500):
    """Build a plotly candlestick chart with fib levels."""
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df.index, open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        increasing_line_color=UP_COLOR, decreasing_line_color=DN_COLOR,
        increasing_fillcolor=UP_COLOR, decreasing_fillcolor=DN_COLOR,
        name="Price", showlegend=False,
    ))

    # Visible price range
    price_low = float(df["low"].min())
    price_high = float(df["high"].max())
    margin = (price_high - price_low) * 0.1
    vis_low = price_low - margin
    vis_high = price_high + margin

    x_start, x_end = df.index[0], df.index[-1]
    labels_added = set()

    for fp in fib_prices:
        if fp < vis_low or fp > vis_high:
            continue
        fp_key = round(fp, 4)
        ratio, series = fib_info.get(fp_key, (0, "?"))
        color = S1_COLOR if series == "S1" else S2_COLOR
        show_legend = series not in labels_added
        if show_legend:
            labels_added.add(series)

        fig.add_trace(go.Scatter(
            x=[x_start, x_end], y=[fp, fp],
            mode="lines",
            line=dict(color=color, width=0.8, dash="dot"),
            name=f"Fib {series}" if show_legend else "",
            showlegend=show_legend,
            legendgroup=f"fib_{series}",
            hovertemplate=f"{series} {ratio:.3f}: ${fp:.4f}<extra></extra>",
        ))

    fig.update_layout(
        title=dict(text=title, font=dict(size=14, color=TEXT), x=0.01, xanchor="left"),
        xaxis=dict(gridcolor=GRID, rangeslider=dict(visible=False), type="date"),
        yaxis=dict(gridcolor=GRID, title="Price ($)", tickformat=".4f"),
        plot_bgcolor=BG, paper_bgcolor=BG,
        font=dict(color=TEXT, size=11),
        legend=dict(bgcolor="rgba(0,0,0,0.3)", bordercolor=GRID, borderwidth=1,
                    font=dict(size=10), x=1.01, y=1),
        height=height,
        margin=dict(l=60, r=160, t=60, b=40),
        hovermode="x unified",
    )
    return fig


def _resample(df, freq):
    """Resample OHLCV."""
    if df.empty:
        return df
    return df.resample(freq).agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna()


def generate_symbol_page(symbol, daily_df, intraday_2m_df, fib_prices, fib_info, anchor_info):
    """Generate a single HTML page for one symbol with 3 timeframes."""
    charts_html = []

    # 1. Daily chart (full 5y)
    fig_daily = _build_candlestick(
        daily_df,
        f"<b>{symbol}</b> — Daily (5y) | Anchor: {anchor_info['date']} "
        f"L=${anchor_info['low']:.4f} H=${anchor_info['high']:.4f}",
        fib_prices, fib_info, height=600,
    )
    charts_html.append(fig_daily.to_html(full_html=False, include_plotlyjs=False,
                                          config={"displayModeBar": True, "scrollZoom": True}))

    if intraday_2m_df is not None and not intraday_2m_df.empty:
        # 2. 5-min chart
        df_5m = _resample(intraday_2m_df, "5min")
        if len(df_5m) > 10:
            fig_5m = _build_candlestick(
                df_5m, f"<b>{symbol}</b> — 5-min (30d)", fib_prices, fib_info, height=500,
            )
            charts_html.append(fig_5m.to_html(full_html=False, include_plotlyjs=False,
                                               config={"displayModeBar": True, "scrollZoom": True}))

        # 3. 1-min chart (last 7 days of 2m data — closest we have)
        cutoff = intraday_2m_df.index[-1] - pd.Timedelta(days=7)
        last_7d = intraday_2m_df.loc[intraday_2m_df.index >= cutoff]
        if len(last_7d) > 20:
            fig_1m = _build_candlestick(
                last_7d, f"<b>{symbol}</b> — 2-min (last 7d)", fib_prices, fib_info, height=500,
            )
            charts_html.append(fig_1m.to_html(full_html=False, include_plotlyjs=False,
                                               config={"displayModeBar": True, "scrollZoom": True}))

    # Assemble HTML
    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><title>{symbol} — Fib Analysis</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  body {{ background: {BG}; color: {TEXT}; font-family: 'Segoe UI', system-ui, sans-serif; margin: 0; padding: 20px; }}
  h1 {{ margin: 0 0 10px; font-size: 22px; border-bottom: 1px solid #2a3040; padding-bottom: 8px; }}
  .chart {{ margin-bottom: 20px; background: #1a1f2e; border-radius: 8px; padding: 10px; border: 1px solid #2a3040; }}
  a {{ color: #2962ff; }}
</style>
</head><body>
<h1>{symbol} — Fibonacci Multi-Timeframe Analysis</h1>
<p><a href="index.html">Back to Index</a></p>
"""
    for ch in charts_html:
        html += f'<div class="chart">{ch}</div>\n'
    html += "</body></html>"

    out_path = OUT_DIR / f"{symbol}.html"
    out_path.write_text(html)
    return out_path


def generate_index(symbols_data):
    """Generate index.html linking all symbol pages."""
    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><title>Fib Analysis — All Symbols</title>
<style>
  body {{ background: {BG}; color: {TEXT}; font-family: 'Segoe UI', system-ui, sans-serif; margin: 0; padding: 20px; }}
  h1 {{ font-size: 24px; border-bottom: 1px solid #2a3040; padding-bottom: 10px; }}
  .grid {{ display: flex; flex-wrap: wrap; gap: 12px; }}
  .card {{ background: #1a1f2e; border: 1px solid #2a3040; border-radius: 8px; padding: 14px 20px;
           text-decoration: none; color: {TEXT}; min-width: 140px; }}
  .card:hover {{ border-color: #2962ff; }}
  .card .sym {{ font-size: 18px; font-weight: 700; color: #2962ff; }}
  .card .info {{ font-size: 12px; color: #8b949e; margin-top: 4px; }}
</style>
</head><body>
<h1>Fibonacci Analysis — {len(symbols_data)} Symbols</h1>
<p style="color:#8b949e">Pre/after-market 20%+ movers — Daily (5y) + 5-min + 2-min charts with fib levels</p>
<div class="grid">
"""
    for sym, info in sorted(symbols_data.items()):
        anchor = info.get("anchor", {})
        html += f"""<a class="card" href="{sym}.html">
  <div class="sym">{sym}</div>
  <div class="info">Anchor: {anchor.get('date', '?')}<br>L=${anchor.get('low', 0):.4f} H=${anchor.get('high', 0):.4f}</div>
</a>\n"""

    html += "</div></body></html>"
    idx_path = OUT_DIR / "index.html"
    idx_path.write_text(html)
    return idx_path


def generate_analysis(symbols=None):
    """Generate multi-timeframe fib charts for all symbols."""
    if symbols is None:
        from simulation.fib_reversal_backtest import FibReversalEngine
        engine = FibReversalEngine()
        symbols = engine._scan_for_candidates()

    logger.info(f"Generating fib analysis charts for {len(symbols)} symbols...")
    symbols_data = {}
    generated = 0

    for sym in symbols:
        # Load daily 5y
        daily_path = BACKTEST_DATA_DIR / f"{sym}_daily_5y.parquet"
        if not daily_path.exists():
            logger.debug(f"  {sym}: no daily data, skipping")
            continue
        try:
            daily_df = pd.read_parquet(daily_path)
            if daily_df.empty or len(daily_df) < 20:
                continue
        except Exception:
            continue

        # Build fib levels
        fib_data = _get_fib_data(daily_df)
        if fib_data is None:
            logger.debug(f"  {sym}: no anchor candle")
            continue
        fib_prices, fib_info, anchor_info = fib_data

        # Load intraday 2m
        intra_path = BACKTEST_DATA_DIR / f"{sym}_intraday_2m.parquet"
        intraday_df = None
        if intra_path.exists():
            try:
                intraday_df = pd.read_parquet(intra_path)
            except Exception:
                pass

        # Generate page
        try:
            generate_symbol_page(sym, daily_df, intraday_df, fib_prices, fib_info, anchor_info)
            symbols_data[sym] = {"anchor": anchor_info}
            generated += 1
            logger.info(f"  {sym}: chart generated")
        except Exception as e:
            logger.error(f"  {sym}: chart failed: {e}", exc_info=True)

    # Generate index
    idx_path = generate_index(symbols_data)
    logger.info(f"Generated {generated} symbol charts -> {OUT_DIR}")
    print(f"\nFib Analysis Charts: {generated} symbols")
    print(f"Open: {idx_path}")
    return idx_path
