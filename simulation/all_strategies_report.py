"""All Strategies Comparison Dashboard.

Generates a single HTML dashboard comparing:
  - Strategy 5 (Pure Fib Support) from fib_trades_detail.csv
  - Fib Reversal from fib_reversal_trades.csv
  - Fib Strength from fib_strength_trades.csv
  - 72 optimizer combinations from optimizer_results.csv

Sections:
  1. Head-to-head summary cards (3 trade-level strategies)
  2. Overlaid equity curves
  3. Optimizer heatmap (entry × stop × target)
  4. Optimizer scatter (win rate vs return)
  5. Best strategy per dimension (entry, stop, target, filter)
  6. Exit reason comparison
  7. Full optimizer ranking table
"""

import json
import logging
from pathlib import Path

import pandas as pd

from config.settings import FIB_CHARTS_DIR, DATA_DIR

logger = logging.getLogger("trading_bot.all_strategies")

BG = "#0e1117"
CARD_BG = "#1a1f2e"
BORDER = "#2a3040"
GRID = "#1e2530"
TEXT = "#fafafa"
DIM = "#8b949e"
WIN = "#00d26a"
LOSS = "#f23645"
BLUE = "#2962ff"
ORANGE = "#ff9800"
PURPLE = "#6c5ce7"
CYAN = "#00bcd4"

CAPITAL = 3000.0

# Strategy colors for charts
S5_COLOR = WIN       # green
REV_COLOR = ORANGE   # orange
STR_COLOR = CYAN     # cyan


def _j(obj) -> str:
    return json.dumps(obj)


def generate_all_strategies_report(output_path: str | Path | None = None) -> Path:
    output_path = Path(output_path) if output_path else FIB_CHARTS_DIR / "all_strategies_dashboard.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # --- Load data ----------------------------------------------------------
    s5 = _load_trades(DATA_DIR / "fib_trades_detail.csv", "Strategy 5 (Pure Fib Support)")
    rev = _load_trades(DATA_DIR / "fib_reversal_trades.csv", "Fib Reversal")
    stg = _load_trades(DATA_DIR / "fib_strength_trades.csv", "Fib Strength")
    opt = pd.read_csv(DATA_DIR / "optimizer_results.csv")

    strategies = [s5, rev, stg]

    # --- Build sections -----------------------------------------------------
    summary_html = _build_summary_cards(strategies)
    equity_js = _build_equity_curves(strategies)
    drawdown_js = _build_drawdown_curves(strategies)
    optimizer_bar_js = _build_optimizer_top_bar(opt)
    scatter_js = _build_optimizer_scatter(opt)
    heatmap_js = _build_heatmap(opt)
    dimension_js = _build_dimension_analysis(opt)
    exit_js = _build_exit_comparison(strategies)
    table_html = _build_optimizer_table(opt)

    charts_js = "\n".join([
        equity_js, drawdown_js, optimizer_bar_js,
        scatter_js, heatmap_js, dimension_js, exit_js,
    ])

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>All Strategies — Comparison Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  background: {BG}; color: {TEXT};
  font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  padding: 24px; line-height: 1.5;
}}
h1 {{ font-size: 28px; font-weight: 700; margin-bottom: 6px; }}
h2.section {{ font-size: 20px; margin: 30px 0 14px; padding-bottom: 8px; border-bottom: 1px solid {BORDER}; }}
.subtitle {{ color: {DIM}; font-size: 14px; margin-bottom: 28px; }}

.strat-grid {{
  display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 16px; margin-bottom: 28px;
}}
.strat-card {{
  background: {CARD_BG}; border: 1px solid {BORDER}; border-radius: 10px;
  padding: 20px; border-top: 4px solid {DIM};
}}
.strat-card h3 {{ font-size: 16px; margin-bottom: 12px; }}
.strat-card .big {{ font-size: 32px; font-weight: 700; margin: 4px 0; }}
.strat-card .row {{ display: flex; justify-content: space-between; padding: 4px 0; font-size: 13px; }}
.strat-card .row .label {{ color: {DIM}; }}

.card {{
  background: {CARD_BG}; border: 1px solid {BORDER};
  border-radius: 8px; padding: 18px; margin-bottom: 20px;
}}
.card h2 {{
  font-size: 16px; font-weight: 600; margin-bottom: 12px;
  padding-bottom: 8px; border-bottom: 1px solid {BORDER};
}}
.plot {{ width: 100%; }}
.grid2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
@media (max-width: 900px) {{ .grid2 {{ grid-template-columns: 1fr; }} }}

.opt-table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
.opt-table th {{
  background: #161b26; color: {DIM}; font-size: 10px;
  text-transform: uppercase; padding: 8px 6px; text-align: left;
  border-bottom: 2px solid {BORDER}; position: sticky; top: 0;
  cursor: pointer; user-select: none;
}}
.opt-table th:hover {{ color: {TEXT}; }}
.opt-table th .arrow {{ font-size: 9px; margin-left: 3px; }}
.opt-table td {{ padding: 7px 6px; border-bottom: 1px solid {GRID}; }}
.opt-table tr:hover {{ background: rgba(255,255,255,0.03); }}
.opt-table .pos {{ color: {WIN}; }} .opt-table .neg {{ color: {LOSS}; }}
.opt-table .top3 {{ background: rgba(0,210,106,0.08); }}
.table-wrap {{ max-height: 600px; overflow-y: auto; border: 1px solid {BORDER}; border-radius: 6px; }}
</style>
</head>
<body>

<h1>All Strategies — Comparison Dashboard</h1>
<p class="subtitle">3 live-tested strategies + 72 optimizer combinations &middot; $3,000 starting capital</p>

{summary_html}

<div class="card">
  <h2>Equity Curves — Head to Head</h2>
  <div id="equity" class="plot" style="height:450px"></div>
</div>

<div class="card">
  <h2>Drawdown Comparison</h2>
  <div id="drawdown" class="plot" style="height:320px"></div>
</div>

<h2 class="section">Optimizer — 72 Strategy Combinations</h2>

<div class="card">
  <h2>Top 15 Strategies by Net P&amp;L</h2>
  <div id="opt-bar" class="plot" style="height:420px"></div>
</div>

<div class="grid2">
  <div class="card">
    <h2>Win Rate vs Return</h2>
    <div id="scatter" class="plot" style="height:400px"></div>
  </div>
  <div class="card">
    <h2>Avg Return by Entry &times; Stop</h2>
    <div id="heatmap" class="plot" style="height:400px"></div>
  </div>
</div>

<div class="grid2">
  <div class="card">
    <h2>Best by Dimension</h2>
    <div id="dimension" class="plot" style="height:400px"></div>
  </div>
  <div class="card">
    <h2>Exit Reason Comparison</h2>
    <div id="exit-compare" class="plot" style="height:400px"></div>
  </div>
</div>

<div class="card">
  <h2>Full Optimizer Rankings</h2>
  {table_html}
</div>

<script>
var C = {{
  bg: '{BG}', grid: '{GRID}', text: '{TEXT}', dim: '{DIM}',
  win: '{WIN}', loss: '{LOSS}', blue: '{BLUE}',
  orange: '{ORANGE}', purple: '{PURPLE}', cyan: '{CYAN}'
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

// Table sorting
document.querySelectorAll('.opt-table th[data-col]').forEach(function(th) {{
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
      var va = a.children[col].textContent.replace(/[$,%+#]/g, '').trim();
      var vb = b.children[col].textContent.replace(/[$,%+#]/g, '').trim();
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
    print(f"Dashboard saved to: {output_path}")
    return output_path


# ===========================================================================
# Data loading
# ===========================================================================

def _load_trades(path: Path, name: str) -> dict:
    df = pd.read_csv(path)
    df["pnl_net"] = pd.to_numeric(df["pnl_net"], errors="coerce").fillna(0)
    pct_col = df["pnl_pct"].astype(str).str.replace("%", "")
    df["pnl_pct"] = pd.to_numeric(pct_col, errors="coerce").fillna(0) / 100
    df["entry_time"] = pd.to_datetime(df["entry_time"], utc=True)
    df["is_win"] = df["pnl_net"] >= 0
    df = df.sort_values("entry_time").reset_index(drop=True)

    total = len(df)
    wins = int(df["is_win"].sum())
    losses = total - wins
    net = df["pnl_net"].sum()
    equity = CAPITAL + df["pnl_net"].cumsum()
    peak = equity.cummax()
    max_dd = ((equity - peak) / peak).min()
    gross_w = df.loc[df["is_win"], "pnl_net"].sum()
    gross_l = abs(df.loc[~df["is_win"], "pnl_net"].sum())
    pf = gross_w / gross_l if gross_l > 0 else float("inf")
    avg_win = df.loc[df["is_win"], "pnl_net"].mean() if wins else 0
    avg_loss = df.loc[~df["is_win"], "pnl_net"].mean() if losses else 0

    return {
        "name": name, "df": df,
        "total": total, "wins": wins, "losses": losses,
        "win_rate": wins / total if total else 0,
        "net_pnl": net,
        "return_pct": net / CAPITAL * 100,
        "max_dd": max_dd,
        "final_equity": equity.iloc[-1] if len(equity) else CAPITAL,
        "profit_factor": pf,
        "avg_win": avg_win, "avg_loss": avg_loss,
        "equity": equity.tolist(),
    }


# ===========================================================================
# Summary cards
# ===========================================================================

def _build_summary_cards(strategies: list[dict]) -> str:
    colors = [S5_COLOR, REV_COLOR, STR_COLOR]
    cards = []
    for s, color in zip(strategies, colors):
        pnl_c = WIN if s["net_pnl"] >= 0 else LOSS
        ps = "+" if s["net_pnl"] >= 0 else ""
        rs = "+" if s["return_pct"] >= 0 else ""
        cards.append(f"""<div class="strat-card" style="border-top-color: {color}">
  <h3 style="color: {color}">{s['name']}</h3>
  <div class="big" style="color: {pnl_c}">{rs}{s['return_pct']:.1f}%</div>
  <div class="big" style="color: {pnl_c}; font-size: 22px">{ps}${s['net_pnl']:,.2f}</div>
  <div class="row"><span class="label">Trades</span><span>{s['total']}</span></div>
  <div class="row"><span class="label">Wins / Losses</span><span style="color:{WIN}">{s['wins']}</span> / <span style="color:{LOSS}">{s['losses']}</span></div>
  <div class="row"><span class="label">Win Rate</span><span>{s['win_rate']:.1%}</span></div>
  <div class="row"><span class="label">Profit Factor</span><span>{s['profit_factor']:.2f}</span></div>
  <div class="row"><span class="label">Max Drawdown</span><span style="color:{LOSS}">{s['max_dd']:.1%}</span></div>
  <div class="row"><span class="label">Avg Win</span><span style="color:{WIN}">${s['avg_win']:,.2f}</span></div>
  <div class="row"><span class="label">Avg Loss</span><span style="color:{LOSS}">${s['avg_loss']:,.2f}</span></div>
  <div class="row"><span class="label">Capital</span><span>${CAPITAL:,.0f} &rarr; ${s['final_equity']:,.0f}</span></div>
</div>""")
    return f'<div class="strat-grid">{"".join(cards)}</div>'


# ===========================================================================
# Equity curves
# ===========================================================================

def _build_equity_curves(strategies: list[dict]) -> str:
    colors = [S5_COLOR, REV_COLOR, STR_COLOR]
    traces = []
    for s, color in zip(strategies, colors):
        eq = [CAPITAL] + s["equity"]
        nums = list(range(len(eq)))
        traces.append(f"""{{
  x: {_j(nums)}, y: {_j(eq)},
  type: 'scatter', mode: 'lines',
  line: {{color: '{color}', width: 2}},
  name: '{s["name"]} ({s["return_pct"]:+.0f}%)',
  hovertemplate: '{s["name"]}<br>Trade #%{{x}}<br>Equity: $%{{y:,.2f}}<extra></extra>'
}}""")

    return f"""
Plotly.newPlot('equity', [{','.join(traces)}], makeLayout({{
  xaxis: {{title: 'Trade #'}},
  yaxis: {{title: 'Equity ($)', tickprefix: '$'}},
  margin: {{l: 70, r: 20, t: 20, b: 50}},
  legend: {{font: {{color: C.text, size: 12}}, bgcolor: 'rgba(0,0,0,0.4)', x: 0.02, y: 0.98}},
  shapes: [{{
    type: 'line', x0: 0, x1: 1, xref: 'paper',
    y0: {CAPITAL}, y1: {CAPITAL},
    line: {{color: C.dim, width: 1, dash: 'dot'}}
  }}]
}}), cfg);
"""


def _build_drawdown_curves(strategies: list[dict]) -> str:
    colors = [S5_COLOR, REV_COLOR, STR_COLOR]
    traces = []
    for s, color in zip(strategies, colors):
        equity = pd.Series([CAPITAL] + s["equity"])
        peak = equity.cummax()
        dd = ((equity - peak) / peak * 100).round(2).tolist()
        nums = list(range(len(dd)))
        traces.append(f"""{{
  x: {_j(nums)}, y: {_j(dd)},
  type: 'scatter', mode: 'lines',
  fill: 'tozeroy',
  line: {{color: '{color}', width: 1.5}},
  fillcolor: '{color}11',
  name: '{s["name"]}',
  hovertemplate: '{s["name"]}<br>Trade #%{{x}}<br>DD: %{{y:.2f}}%<extra></extra>'
}}""")

    return f"""
Plotly.newPlot('drawdown', [{','.join(traces)}], makeLayout({{
  xaxis: {{title: 'Trade #'}},
  yaxis: {{title: 'Drawdown (%)', ticksuffix: '%'}},
  margin: {{l: 70, r: 20, t: 10, b: 50}},
  legend: {{font: {{color: C.text}}, bgcolor: 'rgba(0,0,0,0.4)', x: 0.02, y: -0.15, orientation: 'h'}}
}}), cfg);
"""


# ===========================================================================
# Optimizer charts
# ===========================================================================

def _build_optimizer_top_bar(opt: pd.DataFrame) -> str:
    top = opt.sort_values("net_pnl", ascending=False).head(15)
    names = top["strategy"].tolist()
    pnls = top["net_pnl"].round(2).tolist()
    colors = [WIN if v >= 0 else LOSS for v in pnls]
    text = [f"${v:+,.0f}" for v in pnls]
    wr = [f"WR: {r:.0f}%" for r in top["win_rate"]]

    return f"""
Plotly.newPlot('opt-bar', [{{
  y: {_j(names[::-1])},
  x: {_j(pnls[::-1])},
  type: 'bar', orientation: 'h',
  marker: {{color: {_j(colors[::-1])}}},
  text: {_j(text[::-1])},
  textposition: 'outside',
  textfont: {{color: C.text, size: 11}},
  customdata: {_j(wr[::-1])},
  hovertemplate: '%{{y}}<br>P&L: $%{{x:,.2f}}<br>%{{customdata}}<extra></extra>'
}}], makeLayout({{
  xaxis: {{tickprefix: '$', title: 'Net P&L'}},
  margin: {{l: 240, r: 70, t: 10, b: 50}}
}}), cfg);
"""


def _build_optimizer_scatter(opt: pd.DataFrame) -> str:
    x = opt["win_rate"].tolist()
    y = (opt["net_pnl"] / CAPITAL * 100).round(1).tolist()
    names = opt["strategy"].tolist()
    sizes = [max(6, min(20, t / 5)) for t in opt["total_trades"]]
    colors = [WIN if v >= 0 else LOSS for v in y]

    return f"""
Plotly.newPlot('scatter', [{{
  x: {_j(x)}, y: {_j(y)},
  mode: 'markers',
  marker: {{color: {_j(colors)}, size: {_j(sizes)}, line: {{color: '#fff', width: 0.5}}, opacity: 0.8}},
  text: {_j(names)},
  hovertemplate: '%{{text}}<br>Win Rate: %{{x:.1f}}%<br>Return: %{{y:.1f}}%<extra></extra>'
}}], makeLayout({{
  xaxis: {{title: 'Win Rate (%)', ticksuffix: '%'}},
  yaxis: {{title: 'Return (%)', ticksuffix: '%'}},
  margin: {{l: 60, r: 20, t: 10, b: 50}},
  shapes: [
    {{type:'line', x0:0, x1:100, y0:0, y1:0, line:{{color:C.dim, width:1, dash:'dot'}}}},
    {{type:'line', x0:50, x1:50, y0:-350, y1:50, line:{{color:C.dim, width:1, dash:'dot'}}}}
  ]
}}), cfg);
"""


def _build_heatmap(opt: pd.DataFrame) -> str:
    pivot = opt.pivot_table(
        values="net_pnl", index="entry_type", columns="stop_type", aggfunc="mean"
    ).round(0)
    z = pivot.values.tolist()
    x = pivot.columns.tolist()
    y = pivot.index.tolist()
    # Custom text showing dollar amounts
    text = [[f"${v:+,.0f}" for v in row] for row in z]

    return f"""
Plotly.newPlot('heatmap', [{{
  z: {_j(z)}, x: {_j(x)}, y: {_j(y)},
  type: 'heatmap',
  colorscale: [[0, '{LOSS}'], [0.5, '{BG}'], [1, '{WIN}']],
  zmid: 0,
  text: {_j(text)},
  texttemplate: '%{{text}}',
  textfont: {{color: C.text, size: 12}},
  hovertemplate: '%{{y}} + %{{x}}<br>Avg P&L: $%{{z:,.0f}}<extra></extra>',
  colorbar: {{tickprefix: '$', title: 'Avg P&L', font: {{color: C.text}}}}
}}], makeLayout({{
  xaxis: {{title: 'Stop Type'}},
  yaxis: {{title: 'Entry Type'}},
  margin: {{l: 130, r: 80, t: 10, b: 70}}
}}), cfg);
"""


def _build_dimension_analysis(opt: pd.DataFrame) -> str:
    """Grouped bar chart: avg return by each dimension."""
    dims = [
        ("entry_type", "Entry"),
        ("stop_type", "Stop"),
        ("target_type", "Target"),
    ]

    traces = []
    bar_colors = [BLUE, ORANGE, PURPLE]
    for (col, label), color in zip(dims, bar_colors):
        grouped = opt.groupby(col)["net_pnl"].mean().sort_values(ascending=False)
        x_vals = grouped.index.tolist()
        y_vals = (grouped / CAPITAL * 100).round(1).tolist()
        colors = [WIN if v >= 0 else LOSS for v in y_vals]
        text = [f"{v:+.0f}%" for v in y_vals]
        traces.append(f"""{{
  x: {_j(x_vals)}, y: {_j(y_vals)},
  type: 'bar', name: '{label}',
  marker: {{color: {_j(colors)}}},
  text: {_j(text)},
  textposition: 'outside',
  textfont: {{color: C.text, size: 10}},
  hovertemplate: '{label}: %{{x}}<br>Avg Return: %{{y:.1f}}%<extra></extra>'
}}""")

    return f"""
Plotly.newPlot('dimension', [{','.join(traces)}], makeLayout({{
  yaxis: {{title: 'Avg Return (%)', ticksuffix: '%'}},
  xaxis: {{title: ''}},
  margin: {{l: 60, r: 20, t: 20, b: 80}},
  barmode: 'group',
  legend: {{font: {{color: C.text}}, bgcolor: 'rgba(0,0,0,0.4)'}},
  shapes: [{{type:'line', x0:0, x1:1, xref:'paper', y0:0, y1:0, line:{{color:C.dim, width:1, dash:'dot'}}}}]
}}), cfg);
"""


def _build_exit_comparison(strategies: list[dict]) -> str:
    """Grouped bar: exit reason breakdown per strategy."""
    colors = [S5_COLOR, REV_COLOR, STR_COLOR]

    def _short(r: str) -> str:
        r = str(r).lower()
        if "stop_loss" in r:
            return "Stop Loss"
        if "target_hit" in r:
            return "Target Hit"
        if "entry_window" in r or "eod" in r:
            return "Time Exit"
        if "trailing" in r:
            return "Trailing Stop"
        return "Other"

    all_reasons = set()
    data = []
    for s in strategies:
        reasons = s["df"]["exit_reason"].apply(_short)
        counts = reasons.value_counts()
        data.append(counts)
        all_reasons.update(counts.index)

    all_reasons = sorted(all_reasons)
    traces = []
    for s, d, color in zip(strategies, data, colors):
        vals = [int(d.get(r, 0)) for r in all_reasons]
        traces.append(f"""{{
  x: {_j(all_reasons)}, y: {_j(vals)},
  type: 'bar', name: '{s["name"]}',
  marker: {{color: '{color}'}},
  hovertemplate: '{s["name"]}<br>%{{x}}: %{{y}}<extra></extra>'
}}""")

    return f"""
Plotly.newPlot('exit-compare', [{','.join(traces)}], makeLayout({{
  yaxis: {{title: 'Count'}},
  barmode: 'group',
  legend: {{font: {{color: C.text}}, bgcolor: 'rgba(0,0,0,0.4)'}},
  margin: {{l: 50, r: 20, t: 20, b: 80}}
}}), cfg);
"""


# ===========================================================================
# Optimizer table
# ===========================================================================

def _build_optimizer_table(opt: pd.DataFrame) -> str:
    headers = [
        ("#", "num"), ("Strategy", "str"), ("Entry", "str"), ("Stop", "str"),
        ("Target", "str"), ("Filter", "str"), ("Trades", "num"),
        ("Win Rate", "num"), ("Net P&L", "num"), ("Return %", "num"),
        ("Max DD", "num"), ("PF", "num"), ("Avg Win", "num"), ("Avg Loss", "num"),
    ]
    th = "".join(
        f'<th data-col="{i}" data-type="{t}">{n}<span class="arrow"></span></th>'
        for i, (n, t) in enumerate(headers)
    )

    sorted_opt = opt.sort_values("net_pnl", ascending=False).reset_index(drop=True)
    rows = []
    for idx, r in sorted_opt.iterrows():
        rank = idx + 1
        pnl = r["net_pnl"]
        ret = pnl / CAPITAL * 100
        pnl_cls = "pos" if pnl >= 0 else "neg"
        top_cls = " top3" if rank <= 3 else ""
        ps = "+" if pnl >= 0 else ""
        filt = "Yes" if r.get("half_range_filter") else "No"

        rows.append(
            f'<tr class="{top_cls}">'
            f"<td>#{rank}</td>"
            f"<td><b>{r['strategy']}</b></td>"
            f"<td>{r['entry_type']}</td>"
            f"<td>{r['stop_type']}</td>"
            f"<td>{r['target_type']}</td>"
            f"<td>{filt}</td>"
            f"<td>{r['total_trades']}</td>"
            f"<td>{r['win_rate']:.1f}%</td>"
            f'<td class="{pnl_cls}">{ps}${pnl:,.2f}</td>'
            f'<td class="{pnl_cls}">{ps}{ret:.1f}%</td>'
            f'<td class="neg">{r["max_drawdown"]:.1%}</td>'
            f"<td>{r['profit_factor']:.2f}</td>"
            f'<td class="pos">${r["avg_win"]:,.2f}</td>'
            f'<td class="neg">${r["avg_loss"]:,.2f}</td>'
            f"</tr>"
        )

    return f"""<div class="table-wrap">
<table class="opt-table">
<thead><tr>{th}</tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>
</div>"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_all_strategies_report()
