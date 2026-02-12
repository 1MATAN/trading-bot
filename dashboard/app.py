"""Streamlit dashboard: P&L, positions, scanner, charts, controls.

Reads from shared SQLite DB and live_state.json written by main process.
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
    page_title="Penny Stock Bot",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    st.title("📈 Penny Stock Trading Bot")

    # Sidebar controls
    _render_sidebar()

    # Load live state
    state = _load_live_state()

    # Top metrics row
    _render_metrics(state)

    # Main content
    col1, col2 = st.columns([2, 1])

    with col1:
        _render_positions(state)
        _render_equity_curve()
        _render_recent_trades()

    with col2:
        _render_risk_status(state)
        _render_scanner_status(state)
        _render_daily_summary()

    # Auto-refresh
    st.empty()
    import time
    time.sleep(5)
    st.rerun()


def _render_sidebar():
    """Render sidebar with controls."""
    st.sidebar.header("Controls")

    if st.sidebar.button("🛑 Stop Bot", use_container_width=True):
        _send_command("stop")
        st.sidebar.success("Stop command sent")

    st.sidebar.divider()
    st.sidebar.header("Status")

    state = _load_live_state()
    if state:
        if state.get("market_open"):
            st.sidebar.success("Market: OPEN")
        elif state.get("premarket"):
            st.sidebar.info("Pre-Market")
        elif state.get("afterhours"):
            st.sidebar.info("After-Hours")
        else:
            st.sidebar.warning("Market: CLOSED")

        ts = state.get("timestamp", "")
        if ts:
            st.sidebar.caption(f"Last update: {ts[:19]}")
    else:
        st.sidebar.error("No live data — bot may not be running")


def _render_metrics(state: dict):
    """Render top metrics row."""
    risk = state.get("risk_status", {})

    cols = st.columns(5)
    cols[0].metric("Portfolio Value", f"${risk.get('portfolio_value', 0):,.2f}")
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
        "highest_price", "partial_exits_done",
    ]
    existing = [c for c in display_cols if c in df.columns]
    st.dataframe(df[existing], use_container_width=True, hide_index=True)


def _render_equity_curve():
    """Render equity curve from daily summaries."""
    st.subheader("Equity Curve")
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


def _render_recent_trades():
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


def _render_risk_status(state: dict):
    """Render risk management status."""
    st.subheader("Risk Status")
    risk = state.get("risk_status", {})

    if risk.get("daily_loss_halt"):
        st.error("⚠️ DAILY LOSS LIMIT — Trading halted")
    elif risk.get("in_cooldown"):
        remaining = risk.get("cooldown_remaining", 0)
        st.warning(f"Cooldown: {remaining:.0f}s remaining")
    else:
        st.success("✅ Risk checks passing")

    st.metric("Max Daily Loss", f"${risk.get('max_daily_loss', 0):,.2f}")


def _render_scanner_status(state: dict):
    """Render scanner status."""
    st.subheader("Scanner")
    st.metric("Last Scan Candidates", state.get("scanner_candidates", 0))
    st.metric("Pre-market Watchlist", state.get("premarket_watchlist", 0))
    st.metric("After-hours Watchlist", state.get("afterhours_watchlist", 0))


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
