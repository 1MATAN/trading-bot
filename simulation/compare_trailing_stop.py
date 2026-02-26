"""Compare Momentum Ride trailing stop: 5% vs 8%.

Runs the backtest twice with different settings and generates
a side-by-side comparison HTML report.
"""

import json
import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import DATA_DIR, FIB_CHARTS_DIR, STARTING_CAPITAL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("mr_compare")

GAPPERS_CSV = DATA_DIR / "gappers_two_days.csv"
OUTPUT_HTML = FIB_CHARTS_DIR / "mr_trailing_comparison.html"


def run_with_trailing(trail_pct: float, safety_pct: float) -> tuple:
    """Run MR backtest with specific trailing/safety stop values."""
    import config.settings as cfg
    import simulation.momentum_ride_backtest as mr_mod
    from simulation.momentum_ride_backtest import MomentumRideEngine

    # Patch both the config module AND the local bindings in the backtest module
    old_trail = cfg.MR_TRAILING_STOP_PCT
    old_safety = cfg.MR_SAFETY_STOP_PCT

    cfg.MR_TRAILING_STOP_PCT = trail_pct
    cfg.MR_SAFETY_STOP_PCT = safety_pct
    cfg.MR_LIVE_TRAILING_STOP_PCT = trail_pct
    cfg.MR_LIVE_SAFETY_STOP_PCT = safety_pct
    # Patch the module-level bindings (from ... import creates local copies)
    mr_mod.MR_TRAILING_STOP_PCT = trail_pct
    mr_mod.MR_SAFETY_STOP_PCT = safety_pct

    engine = MomentumRideEngine(capital=STARTING_CAPITAL)
    result = engine.run()
    chart_data = engine.get_chart_data()

    # Restore
    cfg.MR_TRAILING_STOP_PCT = old_trail
    cfg.MR_SAFETY_STOP_PCT = old_safety
    cfg.MR_LIVE_TRAILING_STOP_PCT = old_trail
    cfg.MR_LIVE_SAFETY_STOP_PCT = old_safety
    mr_mod.MR_TRAILING_STOP_PCT = old_trail
    mr_mod.MR_SAFETY_STOP_PCT = old_safety

    return result, chart_data


def build_equity_curve(result, capital: float) -> list[float]:
    eq = [capital]
    for t in result.trades:
        eq.append(eq[-1] + t["pnl_net"])
    return eq


def build_drawdown(equity: list[float]) -> list[float]:
    peak = equity[0]
    dd = []
    for e in equity[1:]:
        if e > peak:
            peak = e
        dd.append(round(-(peak - e) / peak * 100, 2) if peak > 0 else 0)
    return dd


def build_trade_rows(result, label: str) -> list[dict]:
    rows = []
    for i, t in enumerate(result.trades, 1):
        rows.append({
            "config": label,
            "num": i,
            "symbol": t["symbol"],
            "date": t.get("date", ""),
            "entry_time": str(t["entry_time"])[:16],
            "exit_time": str(t["exit_time"])[:16],
            "entry_fill": t["entry_fill"],
            "exit_fill": t["exit_fill"],
            "qty": t["quantity"],
            "pnl": t["pnl_net"],
            "pnl_pct": t["pnl_pct"],
            "exit_reason": t["exit_reason"],
            "signal": t.get("entry_signal", ""),
        })
    return rows


BG = "#0e1117"
CARD = "#1a1f2e"
BORDER = "#2a3040"
GRID = "#1e2530"
TEXT = "#fafafa"
DIM = "#8b949e"
WIN_C = "#00d26a"
LOSS_C = "#f23645"
BLUE5 = "#ff6b6b"   # red-ish for 5%
BLUE8 = "#4ecdc4"   # teal for 8%
CANDLE_UP = "#26a69a"
CANDLE_DN = "#ef5350"
YELLOW = "#ffd600"
FBLUE = "#2962ff"
FORANGE = "#ff9800"


def _build_combined_chart(
    symbol: str,
    date_str: str,
    day_df: pd.DataFrame,
    fib_prices: list,
    fib_price_info: dict,
    trades_5: list[dict],
    trades_8: list[dict],
) -> go.Figure:
    """Build a 1-min candlestick chart with entries/exits from both configs overlaid."""

    # Resample to 1-min candles
    min_df = day_df.resample("1min").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna()

    # Running VWAP
    tp = (day_df["high"].values + day_df["low"].values + day_df["close"].values) / 3.0
    vol = day_df["volume"].values.astype(float)
    cum_tp_vol = np.cumsum(tp * vol)
    cum_vol = np.cumsum(vol)
    vwap_vals = np.where(cum_vol > 0, cum_tp_vol / cum_vol, 0.0)
    vwap_s = pd.Series(vwap_vals, index=day_df.index).resample("1min").last().dropna()

    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=min_df.index, open=min_df["open"], high=min_df["high"],
        low=min_df["low"], close=min_df["close"],
        increasing_line_color=CANDLE_UP, decreasing_line_color=CANDLE_DN,
        increasing_fillcolor=CANDLE_UP, decreasing_fillcolor=CANDLE_DN,
        name="Price", showlegend=False,
    ))

    # VWAP
    fig.add_trace(go.Scatter(
        x=vwap_s.index, y=vwap_s.values,
        mode="lines", line=dict(color=YELLOW, width=1.5),
        name="VWAP",
    ))

    # Fib levels (visible range only)
    p_lo = float(min_df["low"].min())
    p_hi = float(min_df["high"].max())
    margin = (p_hi - p_lo) * 0.15
    vis_lo, vis_hi = p_lo - margin, p_hi + margin
    x0, x1 = min_df.index[0], min_df.index[-1]

    fib_shown = set()
    for fp in fib_prices:
        if fp < vis_lo or fp > vis_hi:
            continue
        fp_key = round(fp, 4)
        ratio, sname = fib_price_info.get(fp_key, (0, "?"))
        color = FBLUE if sname == "S1" else FORANGE
        show_leg = sname not in fib_shown
        if show_leg:
            fib_shown.add(sname)
        fig.add_trace(go.Scatter(
            x=[x0, x1], y=[fp, fp],
            mode="lines", line=dict(color=color, width=0.7, dash="dot"),
            name=f"Fib {sname}" if show_leg else "",
            showlegend=show_leg, legendgroup=f"fib_{sname}",
            hovertemplate=f"{sname} {ratio:.3f}: ${fp:.4f}<extra></extra>",
        ))

    # ─── 5% markers ───
    for t in trades_5:
        entry_t = pd.Timestamp(t["entry_time"])
        exit_t = pd.Timestamp(t["exit_time"])
        entry_p = t["entry_fill"]
        exit_p = t["exit_fill"]
        pnl = t["pnl_net"]
        stop = t.get("stop_price", 0)

        # Entry
        fig.add_trace(go.Scatter(
            x=[entry_t], y=[entry_p],
            mode="markers+text",
            marker=dict(symbol="triangle-up", size=14, color=BLUE5,
                        line=dict(width=1.5, color="white")),
            text=[f"5% BUY ${entry_p:.4f}"],
            textposition="top center",
            textfont=dict(color=BLUE5, size=10),
            name=f"5% Entry ${entry_p:.4f}",
            legendgroup="5pct",
            showlegend=(t == trades_5[0]),
            hovertemplate=f"5% ENTRY<br>${entry_p:.4f}<br>%{{x}}<extra></extra>",
        ))
        # Exit
        fig.add_trace(go.Scatter(
            x=[exit_t], y=[exit_p],
            mode="markers+text",
            marker=dict(symbol="triangle-down", size=14, color=BLUE5,
                        line=dict(width=1.5, color="white")),
            text=[f"5% EXIT ${exit_p:.4f}"],
            textposition="bottom center",
            textfont=dict(color=BLUE5, size=10),
            name="",
            legendgroup="5pct", showlegend=False,
            hovertemplate=(
                f"5% EXIT ({t['exit_reason'][:25]})<br>"
                f"${exit_p:.4f} P&L=${pnl:+.2f}<br>%{{x}}<extra></extra>"
            ),
        ))
        # Stop line
        if stop and isinstance(stop, (int, float)) and stop > 0:
            fig.add_trace(go.Scatter(
                x=[entry_t, exit_t], y=[stop, stop],
                mode="lines", line=dict(color=BLUE5, width=1, dash="dash"),
                name="", legendgroup="5pct", showlegend=False,
                hovertemplate=f"5% Stop ${stop:.4f}<extra></extra>",
            ))

    # ─── 8% markers ───
    for t in trades_8:
        entry_t = pd.Timestamp(t["entry_time"])
        exit_t = pd.Timestamp(t["exit_time"])
        entry_p = t["entry_fill"]
        exit_p = t["exit_fill"]
        pnl = t["pnl_net"]
        stop = t.get("stop_price", 0)

        fig.add_trace(go.Scatter(
            x=[entry_t], y=[entry_p],
            mode="markers+text",
            marker=dict(symbol="diamond", size=12, color=BLUE8,
                        line=dict(width=1.5, color="white")),
            text=[f"8% BUY ${entry_p:.4f}"],
            textposition="top right",
            textfont=dict(color=BLUE8, size=10),
            name=f"8% Entry ${entry_p:.4f}",
            legendgroup="8pct",
            showlegend=(t == trades_8[0]),
            hovertemplate=f"8% ENTRY<br>${entry_p:.4f}<br>%{{x}}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=[exit_t], y=[exit_p],
            mode="markers+text",
            marker=dict(symbol="diamond", size=12, color=BLUE8,
                        line=dict(width=1.5, color="white")),
            text=[f"8% EXIT ${exit_p:.4f}"],
            textposition="bottom right",
            textfont=dict(color=BLUE8, size=10),
            name="",
            legendgroup="8pct", showlegend=False,
            hovertemplate=(
                f"8% EXIT ({t['exit_reason'][:25]})<br>"
                f"${exit_p:.4f} P&L=${pnl:+.2f}<br>%{{x}}<extra></extra>"
            ),
        ))
        if stop and isinstance(stop, (int, float)) and stop > 0:
            fig.add_trace(go.Scatter(
                x=[entry_t, exit_t], y=[stop, stop],
                mode="lines", line=dict(color=BLUE8, width=1, dash="dash"),
                name="", legendgroup="8pct", showlegend=False,
                hovertemplate=f"8% Stop ${stop:.4f}<extra></extra>",
            ))

    # ─── PnL summary for title ───
    pnl5_total = sum(t["pnl_net"] for t in trades_5)
    pnl8_total = sum(t["pnl_net"] for t in trades_8)
    n5 = len(trades_5)
    n8 = len(trades_8)

    title = (
        f"<b>{symbol}</b> ({date_str}) | "
        f"<span style='color:{BLUE5}'>5%: {n5} trades P&L=${pnl5_total:+,.2f}</span> | "
        f"<span style='color:{BLUE8}'>8%: {n8} trades P&L=${pnl8_total:+,.2f}</span>"
    )

    fig.update_layout(
        title=dict(text=title, font=dict(size=14, color=TEXT), x=0.01, xanchor="left"),
        xaxis=dict(gridcolor=GRID, showgrid=True, rangeslider=dict(visible=False), type="date"),
        yaxis=dict(gridcolor=GRID, showgrid=True, title="Price ($)", tickformat=".4f"),
        plot_bgcolor=BG, paper_bgcolor=BG,
        font=dict(color=TEXT, size=11),
        legend=dict(bgcolor="rgba(0,0,0,0.3)", bordercolor=GRID, borderwidth=1,
                    font=dict(size=10), x=1.01, y=1),
        height=550, margin=dict(l=60, r=180, t=80, b=40),
        hovermode="x unified",
    )
    return fig


def build_per_trade_charts(cd5: list[dict], cd8: list[dict]) -> list[str]:
    """Build combined candlestick chart HTML fragments for each (symbol, date)."""
    # Index trades by (symbol, date)
    def index_by_key(chart_data_list):
        by_key = {}
        for cd in chart_data_list:
            t = cd["trade"]
            key = (t["symbol"], t.get("date", ""))
            by_key.setdefault(key, []).append(cd)
        return by_key

    idx5 = index_by_key(cd5)
    idx8 = index_by_key(cd8)
    all_keys = sorted(set(list(idx5.keys()) + list(idx8.keys())))

    fragments = []
    for sym, date_str in all_keys:
        # Get day_df and fib data from whichever config has it
        ref_list = idx5.get((sym, date_str), []) or idx8.get((sym, date_str), [])
        if not ref_list:
            continue
        ref = ref_list[0]
        day_df = ref["day_df"]
        fib_prices = ref["fib_prices"]
        fib_price_info = ref["fib_price_info"]

        trades5 = [cd["trade"] for cd in idx5.get((sym, date_str), [])]
        trades8 = [cd["trade"] for cd in idx8.get((sym, date_str), [])]

        try:
            fig = _build_combined_chart(
                sym, date_str, day_df, fib_prices, fib_price_info,
                trades5, trades8,
            )
            chart_html = fig.to_html(
                full_html=False, include_plotlyjs=False,
                config={"displayModeBar": True, "scrollZoom": True},
            )
            fragments.append(chart_html)
        except Exception as e:
            fragments.append(f'<p>Chart error for {sym} {date_str}: {e}</p>')

    return fragments


def generate_comparison_html(
    r5, cd5, r8, cd8, capital: float,
) -> Path:
    """Generate comparison HTML dashboard."""

    eq5 = build_equity_curve(r5, capital)
    eq8 = build_equity_curve(r8, capital)
    dd5 = build_drawdown(eq5)
    dd8 = build_drawdown(eq8)

    wr5 = r5.winning_trades / r5.total_trades * 100 if r5.total_trades > 0 else 0
    wr8 = r8.winning_trades / r8.total_trades * 100 if r8.total_trades > 0 else 0

    # Per-trade P&L bars
    pnl5 = [t["pnl_net"] for t in r5.trades]
    sym5 = [f"{t['symbol']} ({t.get('date', '')[:5]})" for t in r5.trades]
    pnl8 = [t["pnl_net"] for t in r8.trades]
    sym8 = [f"{t['symbol']} ({t.get('date', '')[:5]})" for t in r8.trades]

    # Exit reason breakdown
    def reason_counts(result):
        counts = {}
        for t in result.trades:
            r = t["exit_reason"]
            if "trailing" in r:
                key = "Trailing Stop"
            elif "safety" in r:
                key = "Safety Stop"
            elif "eod" in r:
                key = "EOD Close"
            elif "90min" in r:
                key = "90min Timeout"
            else:
                key = r[:20]
            counts[key] = counts.get(key, 0) + 1
        return counts

    rc5 = reason_counts(r5)
    rc8 = reason_counts(r8)
    all_reasons = sorted(set(list(rc5.keys()) + list(rc8.keys())))
    rc5_vals = [rc5.get(r, 0) for r in all_reasons]
    rc8_vals = [rc8.get(r, 0) for r in all_reasons]

    # Build per-trade candlestick charts
    trade_chart_fragments = build_per_trade_charts(cd5, cd8)
    charts_section = ""
    for i, frag in enumerate(trade_chart_fragments):
        charts_section += f'<div class="card">{frag}</div>\n'

    # Trade table
    trades5 = build_trade_rows(r5, "5%")
    trades8 = build_trade_rows(r8, "8%")

    def make_table(trades, label, color):
        if not trades:
            return f"<p>No trades ({label})</p>"
        rows_html = ""
        for t in trades:
            pnl = t["pnl"]
            cls = "pos" if pnl >= 0 else "neg"
            ps = "+" if pnl >= 0 else ""
            reason = t["exit_reason"]
            if "trailing" in reason:
                reason_short = "Trailing"
            elif "safety" in reason:
                reason_short = "Safety"
            elif "eod" in reason:
                reason_short = "EOD"
            else:
                reason_short = reason[:18]
            rows_html += (
                f'<tr>'
                f'<td>{t["num"]}</td>'
                f'<td><b>{t["symbol"]}</b></td>'
                f'<td>{t["date"]}</td>'
                f'<td>{t["entry_time"][11:]}</td>'
                f'<td>{t["exit_time"][11:]}</td>'
                f'<td>${t["entry_fill"]:.4f}</td>'
                f'<td>${t["exit_fill"]:.4f}</td>'
                f'<td>{t["qty"]}</td>'
                f'<td class="{cls}">{ps}${pnl:.2f}</td>'
                f'<td>{t["pnl_pct"]}</td>'
                f'<td>{reason_short}</td>'
                f'<td>{t["signal"]}</td>'
                f'</tr>'
            )
        headers = "#,Symbol,Date,Entry,Exit,Entry$,Exit$,Qty,P&L,P&L%,Reason,Signal".split(",")
        th = "".join(f"<th>{h}</th>" for h in headers)
        return (
            f'<div class="table-wrap">'
            f'<table class="trade-table"><thead><tr>{th}</tr></thead>'
            f'<tbody>{rows_html}</tbody></table></div>'
        )

    tbl5_html = make_table(trades5, "5%", BLUE5)
    tbl8_html = make_table(trades8, "8%", BLUE8)

    # Avg win/loss
    wins5 = [t["pnl_net"] for t in r5.trades if t["pnl_net"] >= 0]
    loss5 = [t["pnl_net"] for t in r5.trades if t["pnl_net"] < 0]
    wins8 = [t["pnl_net"] for t in r8.trades if t["pnl_net"] >= 0]
    loss8 = [t["pnl_net"] for t in r8.trades if t["pnl_net"] < 0]
    avg_w5 = sum(wins5) / len(wins5) if wins5 else 0
    avg_l5 = sum(loss5) / len(loss5) if loss5 else 0
    avg_w8 = sum(wins8) / len(wins8) if wins8 else 0
    avg_l8 = sum(loss8) / len(loss8) if loss8 else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>MR Trailing Stop Comparison: 5% vs 8%</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  background: {BG}; color: {TEXT};
  font-family: 'Segoe UI', system-ui, sans-serif;
  padding: 24px; line-height: 1.5;
}}
h1 {{ font-size: 28px; font-weight: 700; margin-bottom: 4px; }}
h2.section {{ margin: 30px 0 16px 0; font-size: 22px; border-bottom: 1px solid {BORDER}; padding-bottom: 10px; }}
.subtitle {{ color: {DIM}; font-size: 14px; margin-bottom: 24px; }}
.metrics {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px; margin-bottom: 28px;
}}
.metric {{
  background: {CARD}; border: 1px solid {BORDER};
  border-radius: 8px; padding: 14px; text-align: center;
}}
.metric .val {{ font-size: 22px; font-weight: 700; margin-bottom: 2px; }}
.metric .lbl {{ font-size: 10px; color: {DIM}; text-transform: uppercase; letter-spacing: 0.5px; }}
.metric .sub {{ font-size: 12px; margin-top: 4px; }}
.card {{
  background: {CARD}; border: 1px solid {BORDER};
  border-radius: 8px; padding: 18px; margin-bottom: 20px;
}}
.card h2 {{
  font-size: 16px; font-weight: 600; margin-bottom: 12px;
  padding-bottom: 8px; border-bottom: 1px solid {BORDER};
}}
.row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
@media (max-width: 900px) {{ .row {{ grid-template-columns: 1fr; }} }}
.legend-box {{
  display: inline-flex; align-items: center; gap: 6px;
  margin-right: 20px; font-size: 13px;
}}
.legend-dot {{
  width: 12px; height: 12px; border-radius: 50%; display: inline-block;
}}
.trade-table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
.trade-table th {{
  background: #161b26; color: {DIM}; font-size: 10px;
  text-transform: uppercase; letter-spacing: 0.5px;
  padding: 8px 6px; text-align: left; position: sticky; top: 0;
  border-bottom: 2px solid {BORDER};
}}
.trade-table td {{ padding: 6px; border-bottom: 1px solid {GRID}; }}
.trade-table tr:hover {{ background: rgba(255,255,255,0.04); }}
.pos {{ color: {WIN_C}; }} .neg {{ color: {LOSS_C}; }}
.table-wrap {{ max-height: 400px; overflow-y: auto; border: 1px solid {BORDER}; border-radius: 6px; }}
.badge5 {{ background: {BLUE5}; color: #fff; padding: 2px 8px; border-radius: 4px; font-weight: 700; font-size: 13px; }}
.badge8 {{ background: {BLUE8}; color: #000; padding: 2px 8px; border-radius: 4px; font-weight: 700; font-size: 13px; }}
</style>
</head>
<body>

<h1>Trailing Stop Comparison: 5% vs 8%</h1>
<p class="subtitle">
  Momentum Ride Backtest | 15-sec bars | Feb 19-20, 2026 | ${capital:,.0f} starting capital
  <br>
  <span class="legend-box"><span class="legend-dot" style="background:{BLUE5}"></span> 5% trailing (triangle)</span>
  <span class="legend-box"><span class="legend-dot" style="background:{BLUE8}"></span> 8% trailing (diamond)</span>
</p>

<div class="metrics">
  <div class="metric">
    <div class="lbl">Trades</div>
    <div class="val">{r5.total_trades} vs {r8.total_trades}</div>
    <div class="sub"><span class="badge5">5%</span> <span class="badge8">8%</span></div>
  </div>
  <div class="metric">
    <div class="lbl">Win Rate</div>
    <div class="val" style="color:{WIN_C}">{wr5:.0f}% vs {wr8:.0f}%</div>
    <div class="sub"><span class="badge5">{r5.winning_trades}W/{r5.losing_trades}L</span> <span class="badge8">{r8.winning_trades}W/{r8.losing_trades}L</span></div>
  </div>
  <div class="metric">
    <div class="lbl">Net P&L</div>
    <div class="sub"><span class="badge5">${r5.total_pnl_net:+,.2f}</span></div>
    <div class="sub"><span class="badge8">${r8.total_pnl_net:+,.2f}</span></div>
  </div>
  <div class="metric">
    <div class="lbl">Final Equity</div>
    <div class="sub"><span class="badge5">${capital + r5.total_pnl_net:,.0f}</span></div>
    <div class="sub"><span class="badge8">${capital + r8.total_pnl_net:,.0f}</span></div>
  </div>
  <div class="metric">
    <div class="lbl">Max Drawdown</div>
    <div class="sub"><span class="badge5">{r5.max_drawdown:.2%}</span></div>
    <div class="sub"><span class="badge8">{r8.max_drawdown:.2%}</span></div>
  </div>
  <div class="metric">
    <div class="lbl">Avg Win</div>
    <div class="sub"><span class="badge5">${avg_w5:,.2f}</span></div>
    <div class="sub"><span class="badge8">${avg_w8:,.2f}</span></div>
  </div>
  <div class="metric">
    <div class="lbl">Avg Loss</div>
    <div class="sub"><span class="badge5">${avg_l5:,.2f}</span></div>
    <div class="sub"><span class="badge8">${avg_l8:,.2f}</span></div>
  </div>
  <div class="metric">
    <div class="lbl">Commissions</div>
    <div class="sub"><span class="badge5">${r5.total_commissions:,.2f}</span></div>
    <div class="sub"><span class="badge8">${r8.total_commissions:,.2f}</span></div>
  </div>
</div>

<div class="row">
  <div class="card">
    <h2>Equity Curve</h2>
    <div id="equity" style="height:400px"></div>
  </div>
  <div class="card">
    <h2>Drawdown from Peak</h2>
    <div id="drawdown" style="height:400px"></div>
  </div>
</div>

<h2 class="section">Per-Trade 1-Min Charts ({len(trade_chart_fragments)} stocks)</h2>
<p style="color:{DIM}; margin-bottom:16px; font-size:13px;">
  <span class="badge5">5%</span> = triangle markers + dashed stop &nbsp;&nbsp;
  <span class="badge8">8%</span> = diamond markers + dashed stop &nbsp;&nbsp;
  Yellow = VWAP &nbsp; Blue dotted = Fib S1 &nbsp; Orange dotted = Fib S2
</p>
{charts_section}

<div class="row">
  <div class="card">
    <h2>Per-Trade P&L — <span class="badge5">5%</span></h2>
    <div id="pnl5" style="height:350px"></div>
  </div>
  <div class="card">
    <h2>Per-Trade P&L — <span class="badge8">8%</span></h2>
    <div id="pnl8" style="height:350px"></div>
  </div>
</div>

<div class="card">
  <h2>Exit Reason Breakdown</h2>
  <div id="reasons" style="height:350px"></div>
</div>

<div class="row">
  <div class="card">
    <h2>Trade Log — <span class="badge5">5% Trailing</span> ({r5.total_trades} trades)</h2>
    {tbl5_html}
  </div>
  <div class="card">
    <h2>Trade Log — <span class="badge8">8% Trailing</span> ({r8.total_trades} trades)</h2>
    {tbl8_html}
  </div>
</div>

<script>
var C5 = '{BLUE5}', C8 = '{BLUE8}';
var _BG = '{BG}', _GRID = '{GRID}', _TEXT = '{TEXT}', _DIM = '{DIM}';
var _WIN = '{WIN_C}', _LOSS = '{LOSS_C}';
var cfg = {{displayModeBar: true, scrollZoom: true, responsive: true}};

function layout(extra) {{
  var base = {{
    paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
    font: {{color: _TEXT, size: 12}},
    margin: {{l: 60, r: 20, t: 30, b: 50}},
    xaxis: {{gridcolor: _GRID, zerolinecolor: _GRID}},
    yaxis: {{gridcolor: _GRID, zerolinecolor: _GRID}},
    legend: {{bgcolor: 'rgba(0,0,0,0.3)', bordercolor: _GRID, font: {{size:11}}}},
  }};
  for (var k in extra) {{
    if (k === 'xaxis' || k === 'yaxis') {{
      for (var kk in extra[k]) base[k][kk] = extra[k][kk];
    }} else base[k] = extra[k];
  }}
  return base;
}}

// ─── Equity ───
var eq5 = {json.dumps(eq5)};
var eq8 = {json.dumps(eq8)};
Plotly.newPlot('equity', [
  {{x: [...Array(eq5.length).keys()], y: eq5, name: '5% trailing',
    line: {{color: C5, width: 2.5}}, mode: 'lines'}},
  {{x: [...Array(eq8.length).keys()], y: eq8, name: '8% trailing',
    line: {{color: C8, width: 2.5}}, mode: 'lines'}},
], layout({{
  xaxis: {{title: 'Trade #'}},
  yaxis: {{title: 'Equity ($)', tickprefix: '$'}},
  shapes: [{{type:'line', x0:0, x1:Math.max(eq5.length, eq8.length)-1,
    y0:{capital}, y1:{capital}, line:{{color:_DIM, width:1, dash:'dot'}}}}],
}}), cfg);

// ─── Drawdown ───
var dd5 = {json.dumps(dd5)};
var dd8 = {json.dumps(dd8)};
Plotly.newPlot('drawdown', [
  {{x: [...Array(dd5.length).keys()].map(i=>i+1), y: dd5, name: '5%',
    line: {{color: C5, width: 2}}, fill: 'tozeroy', fillcolor: 'rgba(255,107,107,0.1)'}},
  {{x: [...Array(dd8.length).keys()].map(i=>i+1), y: dd8, name: '8%',
    line: {{color: C8, width: 2}}, fill: 'tozeroy', fillcolor: 'rgba(78,205,196,0.1)'}},
], layout({{
  xaxis: {{title: 'Trade #'}},
  yaxis: {{title: 'Drawdown (%)', ticksuffix: '%'}},
}}), cfg);

// ─── Per-Trade P&L bars ───
var pnl5 = {json.dumps(pnl5)};
var sym5 = {json.dumps(sym5)};
Plotly.newPlot('pnl5', [{{
  x: sym5, y: pnl5, type: 'bar',
  marker: {{color: pnl5.map(v => v >= 0 ? _WIN : _LOSS)}},
  text: pnl5.map(v => (v >= 0 ? '+' : '') + '$' + v.toFixed(2)),
  textposition: 'outside', textfont: {{size: 10}},
}}], layout({{yaxis: {{title: 'P&L ($)', tickprefix: '$'}}}}), cfg);

var pnl8 = {json.dumps(pnl8)};
var sym8 = {json.dumps(sym8)};
Plotly.newPlot('pnl8', [{{
  x: sym8, y: pnl8, type: 'bar',
  marker: {{color: pnl8.map(v => v >= 0 ? _WIN : _LOSS)}},
  text: pnl8.map(v => (v >= 0 ? '+' : '') + '$' + v.toFixed(2)),
  textposition: 'outside', textfont: {{size: 10}},
}}], layout({{yaxis: {{title: 'P&L ($)', tickprefix: '$'}}}}), cfg);

// ─── Exit Reasons ───
var reasons = {json.dumps(all_reasons)};
var rc5v = {json.dumps(rc5_vals)};
var rc8v = {json.dumps(rc8_vals)};
Plotly.newPlot('reasons', [
  {{x: reasons, y: rc5v, name: '5%', type: 'bar', marker: {{color: C5}}}},
  {{x: reasons, y: rc8v, name: '8%', type: 'bar', marker: {{color: C8}}}},
], layout({{
  barmode: 'group',
  yaxis: {{title: 'Count'}},
}}), cfg);
</script>
</body>
</html>"""

    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    logger.info(f"Comparison report: {OUTPUT_HTML}")
    return OUTPUT_HTML


def main():
    if not GAPPERS_CSV.exists():
        logger.error(f"Gappers CSV not found: {GAPPERS_CSV}")
        logger.error("Run run_two_day_backtest.py first")
        return

    logger.info("=" * 60)
    logger.info("Trailing Stop Comparison: 5% vs 8%")
    logger.info("=" * 60)

    # Run with 5%
    logger.info("\n>>> Running with 5% trailing / 5% safety <<<")
    r5, cd5 = run_with_trailing(0.05, 0.05)

    # Run with 8%
    logger.info("\n>>> Running with 8% trailing / 8% safety <<<")
    r8, cd8 = run_with_trailing(0.08, 0.08)

    # Print quick comparison
    wr5 = r5.winning_trades / r5.total_trades * 100 if r5.total_trades > 0 else 0
    wr8 = r8.winning_trades / r8.total_trades * 100 if r8.total_trades > 0 else 0

    print(f"\n{'='*60}")
    print(f"  COMPARISON: 5% vs 8% Trailing Stop")
    print(f"{'='*60}")
    print(f"  {'Metric':<20} {'5% Trail':>12} {'8% Trail':>12} {'Delta':>12}")
    print(f"  {'-'*56}")
    print(f"  {'Trades':<20} {r5.total_trades:>12} {r8.total_trades:>12} {r8.total_trades - r5.total_trades:>+12}")
    print(f"  {'Win Rate':<20} {wr5:>11.1f}% {wr8:>11.1f}% {wr8-wr5:>+11.1f}%")
    print(f"  {'Net P&L':<20} ${r5.total_pnl_net:>+11,.2f} ${r8.total_pnl_net:>+11,.2f} ${r8.total_pnl_net-r5.total_pnl_net:>+11,.2f}")
    print(f"  {'Final Equity':<20} ${STARTING_CAPITAL+r5.total_pnl_net:>11,.0f} ${STARTING_CAPITAL+r8.total_pnl_net:>11,.0f}")
    print(f"  {'Max Drawdown':<20} {r5.max_drawdown:>11.2%} {r8.max_drawdown:>11.2%}")
    print(f"  {'Commissions':<20} ${r5.total_commissions:>11,.2f} ${r8.total_commissions:>11,.2f}")
    print(f"{'='*60}")

    # Generate HTML
    path = generate_comparison_html(r5, cd5, r8, cd8, STARTING_CAPITAL)
    print(f"\nReport: {path}")
    os.system(f"xdg-open {path} &")


if __name__ == "__main__":
    main()
