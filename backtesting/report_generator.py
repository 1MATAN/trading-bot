"""HTML reports with Plotly charts for backtest results."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config.settings import PROJECT_ROOT

logger = logging.getLogger("trading_bot.report")


def generate_report(
    result,
    output_dir: str = "backtesting/reports",
    title: str = "Backtest Report",
) -> str:
    """Generate an HTML report from backtest results.

    Returns the path to the generated HTML file.
    """
    output_path = PROJECT_ROOT / output_dir
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output_path / f"backtest_{timestamp}.html"

    # Build charts
    equity_chart = _build_equity_chart(result)
    pnl_chart = _build_pnl_distribution(result)
    trades_table = _build_trades_table(result)
    drawdown_chart = _build_drawdown_chart(result)

    # Build HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ font-family: -apple-system, sans-serif; margin: 40px; background: #0e1117; color: #fafafa; }}
        .header {{ text-align: center; margin-bottom: 40px; }}
        .metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin: 30px 0; }}
        .metric {{ background: #1e2130; border-radius: 8px; padding: 20px; text-align: center; }}
        .metric .value {{ font-size: 28px; font-weight: bold; margin: 8px 0; }}
        .metric .label {{ font-size: 12px; color: #888; text-transform: uppercase; }}
        .positive {{ color: #00d26a; }}
        .negative {{ color: #f45b69; }}
        .chart {{ margin: 30px 0; background: #1e2130; border-radius: 8px; padding: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #2a2d3e; }}
        th {{ background: #1e2130; color: #888; font-size: 12px; text-transform: uppercase; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{title}</h1>
        <p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    </div>

    <div class="metrics">
        <div class="metric">
            <div class="label">Total P&L</div>
            <div class="value {'positive' if result.total_pnl >= 0 else 'negative'}">
                ${result.total_pnl:+,.2f}
            </div>
        </div>
        <div class="metric">
            <div class="label">Win Rate</div>
            <div class="value">{result.win_rate:.1%}</div>
        </div>
        <div class="metric">
            <div class="label">Sharpe Ratio</div>
            <div class="value">{result.sharpe_ratio:.2f}</div>
        </div>
        <div class="metric">
            <div class="label">Max Drawdown</div>
            <div class="value negative">{result.max_drawdown_pct:.1%}</div>
        </div>
        <div class="metric">
            <div class="label">Total Trades</div>
            <div class="value">{result.total_trades}</div>
        </div>
        <div class="metric">
            <div class="label">Winning / Losing</div>
            <div class="value">{result.winning_trades} / {result.losing_trades}</div>
        </div>
        <div class="metric">
            <div class="label">Profit Factor</div>
            <div class="value">{result.profit_factor:.2f}</div>
        </div>
        <div class="metric">
            <div class="label">Capital</div>
            <div class="value">${result.start_capital:,.0f} → ${result.end_capital:,.0f}</div>
        </div>
    </div>

    <div class="chart">
        <h3>Equity Curve</h3>
        <div id="equity-chart"></div>
    </div>

    <div class="chart">
        <h3>Drawdown</h3>
        <div id="drawdown-chart"></div>
    </div>

    <div class="chart">
        <h3>P&L Distribution</h3>
        <div id="pnl-chart"></div>
    </div>

    <div class="chart">
        <h3>Trade Log</h3>
        {trades_table}
    </div>

    <script>
        {equity_chart}
        {drawdown_chart}
        {pnl_chart}
    </script>
</body>
</html>"""

    filename.write_text(html)
    logger.info(f"Report generated: {filename}")
    return str(filename)


def _build_equity_chart(result) -> str:
    """Build equity curve Plotly chart as JS."""
    if not result.equity_curve:
        return ""
    dates = [str(e[0]) for e in result.equity_curve]
    values = [e[1] for e in result.equity_curve]
    return f"""
    Plotly.newPlot('equity-chart', [{{
        x: {dates},
        y: {values},
        type: 'scatter',
        mode: 'lines',
        fill: 'tozeroy',
        line: {{color: '#00d26a', width: 2}},
        fillcolor: 'rgba(0, 210, 106, 0.1)'
    }}], {{
        paper_bgcolor: 'transparent',
        plot_bgcolor: 'transparent',
        font: {{color: '#fafafa'}},
        xaxis: {{gridcolor: '#2a2d3e'}},
        yaxis: {{gridcolor: '#2a2d3e', tickprefix: '$'}},
        margin: {{l: 60, r: 20, t: 10, b: 40}}
    }});
    """


def _build_drawdown_chart(result) -> str:
    """Build drawdown chart."""
    if not result.equity_curve:
        return ""
    values = [e[1] for e in result.equity_curve]
    dates = [str(e[0]) for e in result.equity_curve]
    peak = values[0]
    drawdowns = []
    for v in values:
        if v > peak:
            peak = v
        dd = (v - peak) / peak * 100
        drawdowns.append(dd)
    return f"""
    Plotly.newPlot('drawdown-chart', [{{
        x: {dates},
        y: {drawdowns},
        type: 'scatter',
        mode: 'lines',
        fill: 'tozeroy',
        line: {{color: '#f45b69', width: 2}},
        fillcolor: 'rgba(244, 91, 105, 0.1)'
    }}], {{
        paper_bgcolor: 'transparent',
        plot_bgcolor: 'transparent',
        font: {{color: '#fafafa'}},
        xaxis: {{gridcolor: '#2a2d3e'}},
        yaxis: {{gridcolor: '#2a2d3e', ticksuffix: '%'}},
        margin: {{l: 60, r: 20, t: 10, b: 40}}
    }});
    """


def _build_pnl_distribution(result) -> str:
    """Build P&L histogram."""
    if not result.trades:
        return ""
    pnls = [t.pnl for t in result.trades]
    return f"""
    Plotly.newPlot('pnl-chart', [{{
        x: {pnls},
        type: 'histogram',
        marker: {{color: '#6c5ce7'}},
        nbinsx: 30
    }}], {{
        paper_bgcolor: 'transparent',
        plot_bgcolor: 'transparent',
        font: {{color: '#fafafa'}},
        xaxis: {{gridcolor: '#2a2d3e', tickprefix: '$', title: 'P&L per Trade'}},
        yaxis: {{gridcolor: '#2a2d3e', title: 'Count'}},
        margin: {{l: 60, r: 20, t: 10, b: 50}}
    }});
    """


def _build_trades_table(result) -> str:
    """Build HTML table of trades."""
    if not result.trades:
        return "<p>No trades</p>"

    rows = []
    for t in result.trades:
        pnl_class = "positive" if t.pnl >= 0 else "negative"
        rows.append(f"""
        <tr>
            <td>{t.symbol}</td>
            <td>{str(t.entry_date)[:10]}</td>
            <td>${t.entry_price:.4f}</td>
            <td>{str(t.exit_date)[:10] if t.exit_date else '-'}</td>
            <td>${t.exit_price:.4f if t.exit_price else 0}</td>
            <td>{t.quantity}</td>
            <td class="{pnl_class}">${t.pnl:+.2f}</td>
            <td class="{pnl_class}">{t.pnl_pct:+.2%}</td>
            <td>{t.exit_reason}</td>
        </tr>""")

    return f"""
    <table>
        <thead>
            <tr>
                <th>Symbol</th><th>Entry Date</th><th>Entry Price</th>
                <th>Exit Date</th><th>Exit Price</th><th>Qty</th>
                <th>P&L</th><th>P&L %</th><th>Exit Reason</th>
            </tr>
        </thead>
        <tbody>{''.join(rows)}</tbody>
    </table>"""
