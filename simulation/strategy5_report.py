"""Strategy 5 (Pure Fib Support) — Interactive HTML Dashboard.

Reads data/fib_trades_detail.csv and generates a self-contained interactive
HTML dashboard with Plotly charts:
  1. Summary metrics bar
  2. Equity curve with trade markers
  3. Drawdown chart
  4. P&L distribution histogram
  5. P&L by symbol (horizontal bar)
  6. Exit reason breakdown (donut)
  7. Session analysis (bar)
  8. Win rate by fib level (bar)
  9. Per-trade candlestick charts with entry/exit markers
  10. Full trade log table
"""

import json
import logging
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from config.settings import FIB_CHARTS_DIR, DATA_DIR

logger = logging.getLogger("trading_bot.strategy5_report")

# ---------------------------------------------------------------------------
# Theme colors (matching fib_chart_generator.py)
# ---------------------------------------------------------------------------
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

STARTING_CAPITAL = 3000.0
CACHE_DIR = DATA_DIR / "backtest_cache"

# Candlestick colors
CANDLE_UP = "#26a69a"
CANDLE_DOWN = "#ef5350"


def generate_strategy5_report(
    csv_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> Path:
    csv_path = Path(csv_path) if csv_path else DATA_DIR / "fib_trades_detail.csv"
    output_path = Path(output_path) if output_path else FIB_CHARTS_DIR / "strategy5_dashboard.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)

    # --- Clean data ---------------------------------------------------------
    df["pnl_net"] = pd.to_numeric(df["pnl_net"], errors="coerce").fillna(0)
    df["pnl_pct"] = df["pnl_pct"].astype(str).str.replace("%", "").astype(float) / 100
    df["entry_time"] = pd.to_datetime(df["entry_time"], utc=True)
    df["exit_time"] = pd.to_datetime(df["exit_time"], utc=True)
    df["entry_fill"] = pd.to_numeric(df["entry_fill"], errors="coerce")
    df["exit_fill"] = pd.to_numeric(df["exit_fill"], errors="coerce")
    df["fib_ratio"] = pd.to_numeric(df["fib_ratio"], errors="coerce")
    df["is_win"] = df["pnl_net"] >= 0

    # Sort by entry time
    df = df.sort_values("entry_time").reset_index(drop=True)

    # --- Metrics ------------------------------------------------------------
    metrics = _compute_metrics(df)

    # --- Build all JS chart calls -------------------------------------------
    charts_js = "\n".join([
        _build_equity_curve(df),
        _build_drawdown(df),
        _build_pnl_distribution(df),
        _build_pnl_by_symbol(df),
        _build_exit_reasons(df),
        _build_session_analysis(df),
        _build_fib_level_analysis(df),
    ])

    summary_html = _build_summary(metrics)
    table_html = _build_trade_table(df)
    sym_chart_height = max(350, df["symbol"].nunique() * 30)

    # --- Per-trade candlestick charts ---------------------------------------
    trade_charts_html = _build_all_trade_charts(df)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Strategy 5 — Pure Fib Support Dashboard</title>
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
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
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
  border-bottom: 2px solid {BORDER}; cursor: pointer; user-select: none;
}}
.trade-table th:hover {{ color: {TEXT}; }}
.trade-table th .arrow {{ font-size: 10px; margin-left: 4px; }}
.trade-table td {{ padding: 8px; border-bottom: 1px solid {GRID}; }}
.trade-table tr.win {{ background: rgba(0, 210, 106, 0.05); }}
.trade-table tr.loss {{ background: rgba(242, 54, 69, 0.05); }}
.trade-table tr:hover {{ background: rgba(255,255,255,0.04); }}
.pos {{ color: {WIN}; }} .neg {{ color: {LOSS}; }}
.table-wrap {{ max-height: 600px; overflow-y: auto; border: 1px solid {BORDER}; border-radius: 6px; }}
</style>
</head>
<body>

<h1>Strategy 5 — Pure Fibonacci Support</h1>
<p class="subtitle">1.5% stop-loss &middot; 3 fib levels target &middot; $3,000 starting capital &middot; {len(df)} trades</p>

{summary_html}

<div class="card">
  <h2>Equity Curve — Trade by Trade</h2>
  <div id="equity" class="plot" style="height:450px"></div>
</div>

<div class="card">
  <h2>Drawdown from Peak</h2>
  <div id="drawdown" class="plot" style="height:300px"></div>
</div>

<div class="row">
  <div class="card">
    <h2>P&amp;L Distribution</h2>
    <div id="pnl-dist" class="plot" style="height:350px"></div>
  </div>
  <div class="card">
    <h2>P&amp;L by Symbol</h2>
    <div id="pnl-symbol" class="plot" style="height:{sym_chart_height}px"></div>
  </div>
</div>

<div class="row">
  <div class="card">
    <h2>Exit Reason Breakdown</h2>
    <div id="exit-reason" class="plot" style="height:350px"></div>
  </div>
  <div class="card">
    <h2>Session Analysis</h2>
    <div id="session" class="plot" style="height:350px"></div>
  </div>
</div>

<div class="card">
  <h2>Win Rate by Fib Ratio</h2>
  <div id="fib-level" class="plot" style="height:380px"></div>
</div>

<h2 style="margin: 30px 0 16px 0; font-size: 22px; border-bottom: 1px solid {BORDER}; padding-bottom: 10px;">
  Per-Trade Charts ({len(df)} trades)
</h2>
{trade_charts_html}

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
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: {{color: C.text, size: 12}},
    margin: {{l: 60, r: 20, t: 20, b: 50}},
    xaxis: {{gridcolor: C.grid, zerolinecolor: C.grid}},
    yaxis: {{gridcolor: C.grid, zerolinecolor: C.grid}},
  }};
  for (var k in extra) {{
    if (k === 'xaxis' || k === 'yaxis') {{
      for (var kk in extra[k]) base[k][kk] = extra[k][kk];
    }} else {{
      base[k] = extra[k];
    }}
  }}
  return base;
}}

{charts_js}

// --- Table sorting ---
document.querySelectorAll('.trade-table th[data-col]').forEach(function(th) {{
  th.addEventListener('click', function() {{
    var table = th.closest('table');
    var tbody = table.querySelector('tbody');
    var rows = Array.from(tbody.querySelectorAll('tr'));
    var col = parseInt(th.dataset.col);
    var numeric = th.dataset.type === 'num';
    var asc = th.dataset.dir !== 'asc';
    th.dataset.dir = asc ? 'asc' : 'desc';
    table.querySelectorAll('th .arrow').forEach(function(a) {{ a.textContent = ''; }});
    th.querySelector('.arrow').textContent = asc ? ' \\u25B2' : ' \\u25BC';
    rows.sort(function(a, b) {{
      var va = a.children[col].textContent.replace(/[$,%+]/g, '').trim();
      var vb = b.children[col].textContent.replace(/[$,%+]/g, '').trim();
      if (numeric) {{ va = parseFloat(va) || 0; vb = parseFloat(vb) || 0; }}
      if (va < vb) return asc ? -1 : 1;
      if (va > vb) return asc ? 1 : -1;
      return 0;
    }});
    rows.forEach(function(r) {{ tbody.appendChild(r); }});
  }});
}});
</script>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    logger.info(f"Strategy 5 dashboard generated -> {output_path}")
    print(f"Dashboard saved to: {output_path}")
    return output_path


# ===========================================================================
# Metrics
# ===========================================================================

def _compute_metrics(df: pd.DataFrame) -> dict:
    total = len(df)
    wins = int(df["is_win"].sum())
    losses = total - wins
    win_rate = wins / total if total else 0
    net_pnl = df["pnl_net"].sum()
    ret_pct = net_pnl / STARTING_CAPITAL * 100

    equity = STARTING_CAPITAL + df["pnl_net"].cumsum()
    peak = equity.cummax()
    max_dd = ((equity - peak) / peak).min()
    final_equity = equity.iloc[-1] if len(equity) else STARTING_CAPITAL

    gross_wins = df.loc[df["is_win"], "pnl_net"].sum()
    gross_losses = abs(df.loc[~df["is_win"], "pnl_net"].sum())
    profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    avg_win = df.loc[df["is_win"], "pnl_net"].mean() if wins > 0 else 0
    avg_loss = df.loc[~df["is_win"], "pnl_net"].mean() if losses > 0 else 0

    return dict(
        total=total, wins=wins, losses=losses, win_rate=win_rate,
        net_pnl=net_pnl, return_pct=ret_pct, max_dd=max_dd,
        final_equity=final_equity, profit_factor=profit_factor,
        avg_win=avg_win, avg_loss=avg_loss,
    )


# ===========================================================================
# Summary HTML
# ===========================================================================

def _build_summary(m: dict) -> str:
    pnl_c = WIN if m["net_pnl"] >= 0 else LOSS
    ret_c = WIN if m["return_pct"] >= 0 else LOSS
    wr_c = WIN if m["win_rate"] >= 0.5 else LOSS
    ps = "+" if m["net_pnl"] >= 0 else ""
    rs = "+" if m["return_pct"] >= 0 else ""

    return f"""<div class="metrics">
  <div class="metric"><div class="val">{m['total']}</div><div class="lbl">Total Trades</div></div>
  <div class="metric"><div class="val" style="color:{WIN}">{m['wins']}</div><div class="lbl">Wins</div></div>
  <div class="metric"><div class="val" style="color:{LOSS}">{m['losses']}</div><div class="lbl">Losses</div></div>
  <div class="metric"><div class="val" style="color:{wr_c}">{m['win_rate']:.1%}</div><div class="lbl">Win Rate</div></div>
  <div class="metric"><div class="val" style="color:{pnl_c}">{ps}${m['net_pnl']:,.2f}</div><div class="lbl">Net P&amp;L</div></div>
  <div class="metric"><div class="val" style="color:{ret_c}">{rs}{m['return_pct']:.1f}%</div><div class="lbl">Return</div></div>
  <div class="metric"><div class="val" style="color:{LOSS}">{m['max_dd']:.1%}</div><div class="lbl">Max Drawdown</div></div>
  <div class="metric"><div class="val">${STARTING_CAPITAL:,.0f} &rarr; ${m['final_equity']:,.0f}</div><div class="lbl">Capital</div></div>
  <div class="metric"><div class="val" style="color:{WIN}">${m['avg_win']:,.2f}</div><div class="lbl">Avg Win</div></div>
  <div class="metric"><div class="val" style="color:{LOSS}">${m['avg_loss']:,.2f}</div><div class="lbl">Avg Loss</div></div>
  <div class="metric"><div class="val">{m['profit_factor']:.2f}</div><div class="lbl">Profit Factor</div></div>
</div>"""


# ===========================================================================
# JS helpers — use json.dumps for all data to avoid Python repr issues
# ===========================================================================

def _j(obj) -> str:
    """JSON-encode for safe JS embedding."""
    return json.dumps(obj)


def _fmt_dates(series: pd.Series) -> list[str]:
    return [d.strftime("%Y-%m-%d %H:%M") for d in series]


# ===========================================================================
# Plotly chart builders
# ===========================================================================

def _build_equity_curve(df: pd.DataFrame) -> str:
    equity = (STARTING_CAPITAL + df["pnl_net"].cumsum()).tolist()
    trade_nums = list(range(1, len(df) + 1))
    symbols = df["symbol"].tolist()
    pnls = df["pnl_net"].tolist()
    is_wins = df["is_win"].tolist()
    dates = _fmt_dates(df["entry_time"])

    # Hover text per trade
    hover = [
        f"#{i} {sym}<br>{dt}<br>P&L: ${pnl:+,.2f}<br>Equity: ${eq:,.2f}"
        for i, sym, dt, pnl, eq in zip(trade_nums, symbols, dates, pnls, equity)
    ]

    # Marker colors: green for win, red for loss
    colors = [WIN if w else LOSS for w in is_wins]

    return f"""
Plotly.newPlot('equity', [
  {{
    x: {_j(trade_nums)},
    y: {_j(equity)},
    type: 'scatter', mode: 'lines',
    fill: 'tozeroy',
    line: {{color: C.win, width: 2}},
    fillcolor: 'rgba(0,210,106,0.08)',
    hoverinfo: 'skip',
    showlegend: false
  }},
  {{
    x: {_j(trade_nums)},
    y: {_j(equity)},
    type: 'scatter', mode: 'markers',
    marker: {{color: {_j(colors)}, size: 9, line: {{color: '#fff', width: 1}}}},
    text: {_j(hover)},
    hoverinfo: 'text',
    name: 'Trades'
  }}
], makeLayout({{
  xaxis: {{title: 'Trade #', dtick: 5}},
  yaxis: {{title: 'Equity ($)', tickprefix: '$'}},
  margin: {{l: 70, r: 20, t: 20, b: 50}},
  shapes: [{{
    type: 'line', x0: 0, x1: {len(df) + 1}, y0: {STARTING_CAPITAL}, y1: {STARTING_CAPITAL},
    line: {{color: C.dim, width: 1, dash: 'dot'}}
  }}]
}}), cfg);
"""


def _build_drawdown(df: pd.DataFrame) -> str:
    equity = STARTING_CAPITAL + df["pnl_net"].cumsum()
    peak = equity.cummax()
    dd_pct = ((equity - peak) / peak * 100).round(2).tolist()
    trade_nums = list(range(1, len(df) + 1))

    return f"""
Plotly.newPlot('drawdown', [{{
  x: {_j(trade_nums)},
  y: {_j(dd_pct)},
  type: 'scatter', mode: 'lines',
  fill: 'tozeroy',
  line: {{color: C.loss, width: 2}},
  fillcolor: 'rgba(242,54,69,0.15)',
  hovertemplate: 'Trade #%{{x}}<br>Drawdown: %{{y:.2f}}%<extra></extra>'
}}], makeLayout({{
  xaxis: {{title: 'Trade #', dtick: 5}},
  yaxis: {{title: 'Drawdown (%)', ticksuffix: '%'}},
  margin: {{l: 70, r: 20, t: 10, b: 50}}
}}), cfg);
"""


def _build_pnl_distribution(df: pd.DataFrame) -> str:
    wins = df.loc[df["is_win"], "pnl_net"].round(2).tolist()
    losses = df.loc[~df["is_win"], "pnl_net"].round(2).tolist()
    return f"""
Plotly.newPlot('pnl-dist', [
  {{x: {_j(wins)}, type: 'histogram', name: 'Wins', marker: {{color: C.win}}, opacity: 0.85, nbinsx: 15}},
  {{x: {_j(losses)}, type: 'histogram', name: 'Losses', marker: {{color: C.loss}}, opacity: 0.85, nbinsx: 15}}
], makeLayout({{
  barmode: 'overlay',
  xaxis: {{tickprefix: '$', title: 'P&L per Trade'}},
  yaxis: {{title: 'Count'}},
  legend: {{font: {{color: C.text}}, bgcolor: 'rgba(0,0,0,0.3)'}}
}}), cfg);
"""


def _build_pnl_by_symbol(df: pd.DataFrame) -> str:
    by_sym = df.groupby("symbol")["pnl_net"].sum().sort_values()
    symbols = by_sym.index.tolist()
    values = by_sym.round(2).values.tolist()
    colors = [WIN if v >= 0 else LOSS for v in values]
    text = [f"${v:+,.0f}" for v in values]

    return f"""
Plotly.newPlot('pnl-symbol', [{{
  y: {_j(symbols)},
  x: {_j(values)},
  type: 'bar', orientation: 'h',
  marker: {{color: {_j(colors)}}},
  text: {_j(text)},
  textposition: 'outside',
  textfont: {{color: C.text, size: 11}},
  hovertemplate: '%{{y}}: $%{{x:,.2f}}<extra></extra>'
}}], makeLayout({{
  xaxis: {{tickprefix: '$', title: 'Net P&L'}},
  margin: {{l: 70, r: 60, t: 10, b: 50}}
}}), cfg);
"""


def _build_exit_reasons(df: pd.DataFrame) -> str:
    def _short(r: str) -> str:
        r = str(r).lower()
        if "stop_loss" in r:
            return "Stop Loss"
        if "target_hit" in r:
            return "Target Hit"
        if "entry_window" in r:
            return "Window End"
        return r.title()

    reasons = df["exit_reason"].apply(_short)
    grouped = df.assign(reason=reasons).groupby("reason").agg(
        count=("pnl_net", "size"),
        avg_pnl=("pnl_net", "mean"),
        total_pnl=("pnl_net", "sum"),
    ).reset_index()

    labels = grouped["reason"].tolist()
    counts = grouped["count"].tolist()
    custom = [
        f"Avg P&L: ${r['avg_pnl']:.2f}<br>Total: ${r['total_pnl']:.2f}"
        for _, r in grouped.iterrows()
    ]
    # Map colors to reason types
    color_map = {"Stop Loss": LOSS, "Target Hit": WIN, "Window End": ORANGE}
    colors = [color_map.get(l, TEXT_DIM) for l in labels]

    return f"""
Plotly.newPlot('exit-reason', [{{
  labels: {_j(labels)},
  values: {_j(counts)},
  type: 'pie', hole: 0.45,
  marker: {{colors: {_j(colors)}}},
  textinfo: 'label+percent',
  textfont: {{color: C.text, size: 13}},
  customdata: {_j(custom)},
  hovertemplate: '%{{label}}<br>Count: %{{value}}<br>%{{customdata}}<extra></extra>'
}}], makeLayout({{
  showlegend: true,
  legend: {{font: {{color: C.text}}, bgcolor: 'rgba(0,0,0,0.3)'}},
  margin: {{l: 10, r: 10, t: 20, b: 10}}
}}), cfg);
"""


def _build_session_analysis(df: pd.DataFrame) -> str:
    sessions = df.groupby("session").agg(
        trades=("pnl_net", "size"),
        total_pnl=("pnl_net", "sum"),
        avg_pnl=("pnl_net", "mean"),
        win_rate=("is_win", "mean"),
    ).reset_index()

    labels = sessions["session"].tolist()
    pnl_vals = sessions["total_pnl"].round(2).tolist()
    colors = [WIN if v >= 0 else LOSS for v in pnl_vals]
    text = [f"${v:+,.0f}" for v in pnl_vals]
    custom = [
        f"Trades: {int(r['trades'])}<br>Avg P&L: ${r['avg_pnl']:.2f}<br>Win Rate: {r['win_rate']:.0%}"
        for _, r in sessions.iterrows()
    ]

    return f"""
Plotly.newPlot('session', [{{
  x: {_j(labels)},
  y: {_j(pnl_vals)},
  type: 'bar',
  marker: {{color: {_j(colors)}}},
  text: {_j(text)},
  textposition: 'outside',
  textfont: {{color: C.text, size: 12}},
  customdata: {_j(custom)},
  hovertemplate: '%{{x}}<br>Total P&L: $%{{y:,.2f}}<br>%{{customdata}}<extra></extra>'
}}], makeLayout({{
  yaxis: {{tickprefix: '$', title: 'Total P&L'}},
  xaxis: {{title: 'Session'}},
  margin: {{l: 70, r: 20, t: 30, b: 50}}
}}), cfg);
"""


def _build_fib_level_analysis(df: pd.DataFrame) -> str:
    fib_stats = df.groupby("fib_ratio").agg(
        trades=("pnl_net", "size"),
        win_rate=("is_win", "mean"),
        total_pnl=("pnl_net", "sum"),
        avg_pnl=("pnl_net", "mean"),
    ).reset_index().sort_values("fib_ratio")

    ratios = [str(r) for r in fib_stats["fib_ratio"].tolist()]
    wr_vals = (fib_stats["win_rate"] * 100).round(1).tolist()
    colors = [WIN if v >= 50 else LOSS for v in wr_vals]
    text = [f"{v:.0f}%" for v in wr_vals]
    custom = [
        f"Trades: {int(r['trades'])}<br>Total P&L: ${r['total_pnl']:.2f}<br>Avg P&L: ${r['avg_pnl']:.2f}"
        for _, r in fib_stats.iterrows()
    ]

    return f"""
Plotly.newPlot('fib-level', [{{
  x: {_j(ratios)},
  y: {_j(wr_vals)},
  type: 'bar',
  marker: {{color: {_j(colors)}}},
  text: {_j(text)},
  textposition: 'outside',
  textfont: {{color: C.text, size: 11}},
  customdata: {_j(custom)},
  hovertemplate: 'Fib %{{x}}<br>Win Rate: %{{y:.1f}}%<br>%{{customdata}}<extra></extra>'
}}], makeLayout({{
  yaxis: {{ticksuffix: '%', title: 'Win Rate', range: [0, 115]}},
  xaxis: {{title: 'Fibonacci Ratio', type: 'category'}},
  margin: {{l: 60, r: 20, t: 30, b: 50}},
  shapes: [{{
    type: 'line', x0: 0, x1: 1, xref: 'paper',
    y0: 50, y1: 50,
    line: {{color: C.dim, width: 1, dash: 'dot'}}
  }}]
}}), cfg);
"""


# ===========================================================================
# Trade table
# ===========================================================================

def _build_trade_table(df: pd.DataFrame) -> str:
    headers = [
        ("#", "num"), ("Symbol", "str"), ("Entry Time", "str"), ("Exit Time", "str"),
        ("Entry $", "num"), ("Exit $", "num"), ("Qty", "num"),
        ("P&L", "num"), ("P&L %", "num"), ("Exit Reason", "str"),
        ("Fib Ratio", "num"), ("Session", "str"),
    ]
    th_cells = "".join(
        f'<th data-col="{i}" data-type="{t}">{name}<span class="arrow"></span></th>'
        for i, (name, t) in enumerate(headers)
    )

    rows = []
    for idx, r in df.iterrows():
        cls = "win" if r["is_win"] else "loss"
        pnl_cls = "pos" if r["is_win"] else "neg"
        ps = "+" if r["pnl_net"] >= 0 else ""

        reason = str(r["exit_reason"]).lower()
        if "stop_loss" in reason:
            reason_short = "Stop Loss"
        elif "target_hit" in reason:
            reason_short = "Target Hit"
        elif "entry_window" in reason:
            reason_short = "Window End"
        else:
            reason_short = r["exit_reason"]

        et = r["entry_time"].strftime("%m/%d %H:%M") if pd.notna(r["entry_time"]) else ""
        xt = r["exit_time"].strftime("%m/%d %H:%M") if pd.notna(r["exit_time"]) else ""

        rows.append(
            f'<tr class="{cls}">'
            f"<td>{idx + 1}</td>"
            f"<td><b>{r['symbol']}</b></td>"
            f"<td>{et}</td><td>{xt}</td>"
            f"<td>${r['entry_fill']:.4f}</td>"
            f"<td>${r['exit_fill']:.4f}</td>"
            f"<td>{r['quantity']}</td>"
            f'<td class="{pnl_cls}">{ps}${r["pnl_net"]:.2f}</td>'
            f'<td class="{pnl_cls}">{ps}{r["pnl_pct"]:.2%}</td>'
            f"<td>{reason_short}</td>"
            f"<td>{r['fib_ratio']}</td>"
            f"<td>{r['session']}</td>"
            f"</tr>"
        )

    return f"""<div class="table-wrap">
<table class="trade-table">
<thead><tr>{th_cells}</tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>
</div>"""


# ===========================================================================
# Per-trade candlestick charts
# ===========================================================================

def _load_intraday(symbol: str) -> pd.DataFrame | None:
    """Load cached 2-min intraday data for a symbol."""
    path = CACHE_DIR / f"{symbol}_intraday_2m.parquet"
    if not path.exists():
        logger.warning(f"No cached data for {symbol}")
        return None
    df = pd.read_parquet(path)
    df.columns = [c.lower() for c in df.columns]
    return df


def _build_all_trade_charts(df: pd.DataFrame) -> str:
    """Build per-trade candlestick chart HTML fragments."""
    # Pre-load all symbol data
    intraday_cache: dict[str, pd.DataFrame | None] = {}
    for sym in df["symbol"].unique():
        intraday_cache[sym] = _load_intraday(sym)

    fragments = []
    for idx, row in df.iterrows():
        trade_num = idx + 1
        sym = row["symbol"]
        intraday = intraday_cache.get(sym)
        if intraday is None:
            fragments.append(
                f'<div class="card"><p>#{trade_num} {sym} — no intraday data</p></div>'
            )
            continue

        try:
            fig = _build_single_trade_chart(trade_num, row, intraday)
            chart_html = fig.to_html(
                full_html=False,
                include_plotlyjs=False,
                config={"displayModeBar": True, "scrollZoom": True},
            )
            fragments.append(f'<div class="card">{chart_html}</div>')
        except Exception as e:
            logger.error(f"Failed chart #{trade_num} {sym}: {e}", exc_info=True)
            fragments.append(
                f'<div class="card"><p>#{trade_num} {sym} — chart error: {e}</p></div>'
            )

    return "\n".join(fragments)


def _build_single_trade_chart(
    trade_num: int, row: pd.Series, intraday: pd.DataFrame
) -> go.Figure:
    """Build a Plotly candlestick chart for one trade."""
    symbol = row["symbol"]
    entry_time = row["entry_time"]
    exit_time = row["exit_time"]
    entry_price = row["entry_fill"]
    exit_price = row["exit_fill"]
    pnl = row["pnl_net"]
    pnl_pct = row["pnl_pct"]
    is_win = row["is_win"]
    stop_price = pd.to_numeric(row.get("stop_price"), errors="coerce")
    target_price = pd.to_numeric(row.get("target_level"), errors="coerce")
    fib_level = pd.to_numeric(row.get("fib_level"), errors="coerce")
    fib_ratio = row.get("fib_ratio", "")
    fib_series = row.get("fib_series", "S1")
    exit_reason = str(row.get("exit_reason", ""))
    session = row.get("session", "")

    # Short exit reason
    if "stop_loss" in exit_reason.lower():
        reason_short = "Stop Loss"
    elif "target_hit" in exit_reason.lower():
        reason_short = "Target Hit"
    elif "entry_window" in exit_reason.lower():
        reason_short = "Window End"
    else:
        reason_short = exit_reason

    # Filter intraday data to trade date
    trade_date = entry_time.date()
    day_df = intraday[intraday.index.date == trade_date].copy()
    if day_df.empty:
        raise ValueError(f"No bars for {symbol} on {trade_date}")

    # Ensure timezone-aware for matching
    if day_df.index.tz is None:
        day_df.index = day_df.index.tz_localize("UTC")

    fig = go.Figure()

    # --- Candlestick ---
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

    x_start = day_df.index[0]
    x_end = day_df.index[-1]

    # --- Fib entry level line ---
    if pd.notna(fib_level) and fib_level > 0:
        fib_color = BLUE if fib_series == "S1" else ORANGE
        fig.add_trace(go.Scatter(
            x=[x_start, x_end], y=[fib_level, fib_level],
            mode="lines",
            line=dict(color=fib_color, width=1, dash="dot"),
            name=f"Fib {fib_series} {fib_ratio}",
            hovertemplate=f"Fib {fib_series} {fib_ratio}: ${fib_level:.4f}<extra></extra>",
        ))

    # --- Stop loss line ---
    if pd.notna(stop_price) and stop_price > 0:
        fig.add_trace(go.Scatter(
            x=[x_start, x_end], y=[stop_price, stop_price],
            mode="lines",
            line=dict(color=LOSS, width=1.5, dash="dash"),
            name=f"Stop ${stop_price:.4f}",
            hovertemplate=f"Stop: ${stop_price:.4f}<extra></extra>",
        ))

    # --- Target level line ---
    if pd.notna(target_price) and target_price > 0:
        fig.add_trace(go.Scatter(
            x=[x_start, x_end], y=[target_price, target_price],
            mode="lines",
            line=dict(color=BLUE, width=1.5, dash="dash"),
            name=f"Target ${target_price:.4f}",
            hovertemplate=f"Target: ${target_price:.4f}<extra></extra>",
        ))

    # --- Entry marker ---
    fig.add_trace(go.Scatter(
        x=[entry_time], y=[entry_price],
        mode="markers+text",
        marker=dict(symbol="triangle-up", size=14, color=WIN,
                    line=dict(width=1, color="white")),
        text=[f"BUY ${entry_price:.2f}"],
        textposition="top center",
        textfont=dict(color=WIN, size=10),
        name=f"Entry ${entry_price:.4f}",
        hovertemplate=f"ENTRY<br>${entry_price:.4f}<br>%{{x}}<extra></extra>",
    ))

    # --- Exit marker ---
    exit_color = WIN if is_win else LOSS
    fig.add_trace(go.Scatter(
        x=[exit_time], y=[exit_price],
        mode="markers+text",
        marker=dict(symbol="triangle-down", size=14, color=exit_color,
                    line=dict(width=1, color="white")),
        text=[f"SELL ${exit_price:.2f}"],
        textposition="bottom center",
        textfont=dict(color=exit_color, size=10),
        name=f"Exit ${exit_price:.4f}",
        hovertemplate=(
            f"EXIT ({reason_short})<br>"
            f"${exit_price:.4f}<br>"
            f"P&L: ${pnl:+,.2f} ({pnl_pct:+.2%})<br>"
            f"%{{x}}<extra></extra>"
        ),
    ))

    # --- Title ---
    tag = "WIN" if is_win else "LOSS"
    tag_color = WIN if is_win else LOSS
    pnl_sign = "+" if pnl >= 0 else ""
    title_text = (
        f"<b>#{trade_num} {tag}</b> {symbol} | "
        f"<span style='color:{tag_color}'>{pnl_sign}${pnl:.2f} ({pnl_pct:+.2%})</span> | "
        f"{reason_short} | {session}"
    )
    subtitle = (
        f"Fib: ${fib_level:.4f} ({fib_series} {fib_ratio}) | "
        f"Stop: ${stop_price:.4f} | Target: ${target_price:.4f}"
        if pd.notna(stop_price) and pd.notna(target_price) else ""
    )

    fig.update_layout(
        title=dict(
            text=f"{title_text}<br><span style='font-size:11px;color:{TEXT_DIM}'>{subtitle}</span>",
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
            tickformat=".2f",
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
        height=450,
        margin=dict(l=60, r=150, t=70, b=40),
        hovermode="x unified",
    )

    return fig


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    csv = sys.argv[1] if len(sys.argv) > 1 else None
    generate_strategy5_report(csv_path=csv)
