"""Streamlit dashboard: P&L, positions, Fibonacci levels, simulation results.

Reads from shared SQLite DB and live_state.json written by main/simulation process.
Writes control commands to control.json.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Paths (replicate from settings to avoid import issues in subprocess)
_PROJECT_ROOT = Path(__file__).parent.parent
_DATA_DIR = _PROJECT_ROOT / "data"
_TRADES_DB = _DATA_DIR / "trades.db"
_LIVE_STATE = _DATA_DIR / "live_state.json"
_CONTROL = _DATA_DIR / "control.json"

st.set_page_config(
    page_title="Momentum Trading Bot",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    state = _load_live_state()
    is_sim = state.get("simulation_mode", False)

    if is_sim:
        st.title("📊 Momentum Bot — Simulation Mode")
        st.info("Running in SIMULATION mode — no real trades executed")
    else:
        st.title("📈 Momentum Trading Bot")

    # Sidebar
    _render_sidebar(state)

    # Top metrics
    _render_metrics(state)

    # Main content
    col1, col2 = st.columns([2, 1])

    with col1:
        if is_sim:
            _render_simulation_results(state)
        _render_positions(state)
        _render_equity_curve(state)
        _render_recent_trades(state)

    with col2:
        _render_risk_status(state)
        _render_scanner_status(state)
        if is_sim:
            _render_simulation_trades(state)
        else:
            _render_daily_summary()

    # Auto-refresh
    st.empty()
    import time
    time.sleep(5)
    st.rerun()


def _render_sidebar(state: dict):
    """Render sidebar with controls."""
    st.sidebar.header("Controls")

    if not state.get("simulation_mode"):
        if st.sidebar.button("Stop Bot", use_container_width=True):
            _send_command("stop")
            st.sidebar.success("Stop command sent")

    st.sidebar.divider()
    st.sidebar.header("Status")

    if state:
        if state.get("simulation_mode"):
            st.sidebar.info("SIMULATION MODE")
        else:
            session = state.get("session", "closed")
            if session == "regular":
                st.sidebar.success("Market: OPEN")
            elif session == "pre-market":
                st.sidebar.info("Session: Pre-Market (4:00-9:30)")
            elif session == "after-hours":
                st.sidebar.info("Session: After-Hours (16:00-20:00)")
            else:
                st.sidebar.warning("Market: CLOSED")

        ts = state.get("timestamp", "")
        if ts:
            st.sidebar.caption(f"Last update: {ts[:19]}")
    else:
        st.sidebar.error("No live data — bot may not be running")

    # Strategy info
    st.sidebar.divider()
    st.sidebar.header("Strategy")
    risk = state.get("risk_status", {})
    st.sidebar.metric("Position Size", f"{risk.get('position_size_pct', 0.9):.0%}")
    st.sidebar.metric("Max Positions", risk.get("max_positions", 1))
    st.sidebar.caption("Entry: Fib 0.5 + SMA9(5m) + SMA200(1m/5m/30m)")
    st.sidebar.caption("Exit: 1% below SMA20(1m)")


def _render_metrics(state: dict):
    """Render top metrics row."""
    risk = state.get("risk_status", {})
    sim = state.get("simulation_results", {})

    cols = st.columns(5)
    cols[0].metric("Portfolio Value", f"${risk.get('portfolio_value', 0):,.2f}")

    if state.get("simulation_mode") and sim:
        cols[1].metric("Total P&L", f"${sim.get('total_pnl', 0):+,.2f}")
        cols[2].metric("Total Trades", sim.get("total_trades", 0))
        win_rate = sim.get("win_rate", 0)
        cols[3].metric("Win Rate", f"{win_rate * 100:.1f}%")
        cols[4].metric("Max Drawdown", f"{sim.get('max_drawdown', 0) * 100:.1f}%")
    else:
        cols[1].metric(
            "Daily P&L",
            f"${risk.get('daily_pnl', 0):+,.2f}",
            delta=f"{risk.get('daily_pnl', 0):+.2f}",
        )
        cols[2].metric("Open Positions", len(state.get("positions", [])))
        cols[3].metric("Daily Trades", risk.get("daily_trades", 0))
        cols[4].metric(
            "Day Trades",
            f"{risk.get('day_trades_rolling', 0)}/{risk.get('pdt_limit', 3)}",
        )


def _render_simulation_results(state: dict):
    """Render simulation-specific results with full detail."""
    sim = state.get("simulation_results", {})
    if not sim:
        return

    st.subheader("Simulation Summary")
    symbols = sim.get("symbols_tested", [])
    if symbols:
        st.caption(f"Symbols tested: {', '.join(symbols)}")

    # Row 1: Core results
    cols = st.columns(4)
    cols[0].metric("Winning", sim.get("winning_trades", 0))
    cols[1].metric("Losing", sim.get("losing_trades", 0))
    final_eq = sim.get("final_equity", 3000)
    cols[2].metric("Final Equity", f"${final_eq:,.2f}")
    pnl_pct = ((final_eq / 3000) - 1) * 100 if final_eq else 0
    cols[3].metric("Return", f"{pnl_pct:+.1f}%")

    # Row 2: Costs breakdown
    cols2 = st.columns(4)
    cols2[0].metric("Gross P&L", f"${sim.get('total_pnl_gross', 0):+,.2f}")
    cols2[1].metric("Net P&L", f"${sim.get('total_pnl', 0):+,.2f}")
    cols2[2].metric("Total Commissions", f"${sim.get('total_commissions', 0):,.2f}")
    cols2[3].metric("Total Slippage", f"${sim.get('total_slippage', 0):,.2f}")

    # Row 3: Additional stats
    cols3 = st.columns(3)
    cols3[0].metric("Days w/ Gap >= 7%", sim.get("days_with_gap", 0))
    cols3[1].metric("Max Drawdown", f"{sim.get('max_drawdown', 0) * 100:.1f}%")
    wr = sim.get("win_rate", 0)
    cols3[2].metric("Win Rate", f"{wr * 100:.1f}%")


def _render_positions(state: dict):
    """Render open positions table."""
    st.subheader("Open Positions")
    positions = state.get("positions", [])

    if not positions:
        st.info("No open positions")
        return

    df = pd.DataFrame(positions)
    display_cols = [
        "symbol", "quantity", "entry_price", "stop_loss_price",
        "highest_price",
    ]
    existing = [c for c in display_cols if c in df.columns]
    st.dataframe(df[existing], use_container_width=True, hide_index=True)


def _render_equity_curve(state: dict):
    """Render equity curve."""
    st.subheader("Equity Curve")

    # Try simulation equity curve first
    eq_data = state.get("equity_curve", [])
    if eq_data:
        df = pd.DataFrame(eq_data)
        if "equity" in df.columns and "time" in df.columns:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df["time"],
                y=df["equity"],
                mode="lines",
                fill="tozeroy",
                line=dict(color="#00d26a", width=2),
                fillcolor="rgba(0, 210, 106, 0.1)",
            ))
            fig.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=0, b=0),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(gridcolor="#2a2d3e"),
                yaxis=dict(gridcolor="#2a2d3e", tickprefix="$"),
            )
            st.plotly_chart(fig, use_container_width=True)
            return

    # Fall back to daily summaries from DB
    summaries = _get_daily_summaries()
    if not summaries:
        st.info("No historical data yet")
        return

    df = pd.DataFrame(summaries)
    if "portfolio_value" not in df.columns or "date" not in df.columns:
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["portfolio_value"],
        mode="lines",
        fill="tozeroy",
        line=dict(color="#00d26a", width=2),
        fillcolor="rgba(0, 210, 106, 0.1)",
    ))
    fig.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(gridcolor="#2a2d3e"),
        yaxis=dict(gridcolor="#2a2d3e", tickprefix="$"),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_recent_trades(state: dict):
    """Render recent trades table."""
    st.subheader("Recent Trades")
    trades = _get_recent_trades()

    if not trades:
        st.info("No trades yet")
        return

    df = pd.DataFrame(trades)
    display_cols = [
        "created_at", "symbol", "action", "quantity", "price", "pnl", "notes",
    ]
    existing = [c for c in display_cols if c in df.columns]
    st.dataframe(df[existing], use_container_width=True, hide_index=True)


def _render_simulation_trades(state: dict):
    """Render simulation trade details with full order information."""
    st.subheader("Simulation Trades")
    trades = state.get("recent_trades", [])
    if not trades:
        st.info("No simulation trades")
        return

    df = pd.DataFrame(trades)

    # Full detail table
    display_cols = [
        "symbol", "session", "gap_pct",
        "entry_signal", "entry_fill", "slippage_entry",
        "exit_signal", "exit_fill", "exit_reason",
        "quantity", "pnl_gross", "pnl_net", "pnl_pct",
        "commission",
    ]
    existing = [c for c in display_cols if c in df.columns]

    # Rename columns for readability
    rename_map = {
        "entry_signal": "Signal $",
        "entry_fill": "Fill $",
        "slippage_entry": "Slip $",
        "exit_signal": "Exit Sig $",
        "exit_fill": "Exit Fill $",
        "exit_reason": "Exit Reason",
        "gap_pct": "Gap",
        "pnl_gross": "Gross P&L",
        "pnl_net": "Net P&L",
        "pnl_pct": "P&L %",
        "commission": "Comm",
    }
    display_df = df[existing].rename(columns=rename_map)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Per-trade expandable details
    for i, trade in enumerate(trades):
        pnl = trade.get("pnl_net", 0)
        color = "green" if pnl >= 0 else "red"
        label = (
            f"{trade.get('symbol', '?')} | "
            f"${pnl:+.2f} | {trade.get('session', '')} | "
            f"{trade.get('exit_reason', '')}"
        )
        with st.expander(label):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown("**Entry**")
                st.text(f"Signal:  ${trade.get('entry_signal', 0):.4f}")
                st.text(f"Fill:    ${trade.get('entry_fill', 0):.4f}")
                st.text(f"Slippage: ${trade.get('slippage_entry', 0):.4f}")
                st.text(f"Time:    {trade.get('entry_time', '')}")
            with c2:
                st.markdown("**Exit**")
                st.text(f"Signal:  ${trade.get('exit_signal', 0):.4f}")
                st.text(f"Fill:    ${trade.get('exit_fill', 0):.4f}")
                st.text(f"Reason:  {trade.get('exit_reason', '')}")
                st.text(f"Time:    {trade.get('exit_time', '')}")
            with c3:
                st.markdown("**P&L**")
                st.text(f"Gross:   ${trade.get('pnl_gross', 0):+.2f}")
                st.text(f"Net:     ${trade.get('pnl_net', 0):+.2f}")
                st.text(f"Comm:    ${trade.get('commission', 0):.2f}")
                st.text(f"Gap:     {trade.get('gap_pct', '0%')}")
                st.text(f"Session: {trade.get('session', '')}")


def _render_risk_status(state: dict):
    """Render risk management status."""
    st.subheader("Risk Status")
    risk = state.get("risk_status", {})

    if risk.get("daily_loss_halt"):
        st.error("DAILY LOSS LIMIT — Trading halted")
    elif risk.get("in_cooldown"):
        remaining = risk.get("cooldown_remaining", 0)
        st.warning(f"Cooldown: {remaining:.0f}s remaining")
    else:
        st.success("Risk checks passing")

    st.metric("Max Daily Loss", f"${risk.get('max_daily_loss', 0):,.2f}")


def _render_scanner_status(state: dict):
    """Render scanner status."""
    st.subheader("Scanner")
    st.metric("Last Scan Candidates", state.get("scanner_candidates", 0))


def _render_daily_summary():
    """Render today's summary."""
    st.subheader("Today's Summary")
    summary = _get_today_summary()
    if not summary:
        st.info("No trades today")
        return

    col1, col2 = st.columns(2)
    col1.metric("Wins", summary.get("winning_trades", 0))
    col2.metric("Losses", summary.get("losing_trades", 0))
    st.metric("Total P&L", f"${summary.get('total_pnl', 0):+,.2f}")


# ── Data Loading ───────────────────────────────────────

def _load_live_state() -> dict:
    """Load live state from JSON file."""
    try:
        if _LIVE_STATE.exists():
            return json.loads(_LIVE_STATE.read_text())
    except Exception:
        pass
    return {}


def _send_command(command: str) -> None:
    """Write a control command for the bot."""
    try:
        _DATA_DIR.mkdir(exist_ok=True)
        _CONTROL.write_text(json.dumps({"command": command}))
    except Exception:
        pass


def _get_recent_trades(limit: int = 50) -> list[dict]:
    """Read recent trades from SQLite."""
    try:
        if not _TRADES_DB.exists():
            return []
        conn = sqlite3.connect(_TRADES_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM trades ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


def _get_daily_summaries() -> list[dict]:
    """Read daily summaries from SQLite."""
    try:
        if not _TRADES_DB.exists():
            return []
        conn = sqlite3.connect(_TRADES_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM daily_summary ORDER BY date ASC")
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


def _get_today_summary() -> dict:
    """Get today's summary."""
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        if not _TRADES_DB.exists():
            return {}
        conn = sqlite3.connect(_TRADES_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM daily_summary WHERE date = ?", (today,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        return {}


if __name__ == "__main__":
    main()
