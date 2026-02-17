# CLAUDE.md — Project Conventions

## Project Overview
Penny stock trading bot using IBKR TWS API (`ib_insync`), recursive Fibonacci momentum strategy, paper trading mode with $3,000 budget.

## Tech Stack
- Python 3.11+, asyncio (single event loop)
- `ib_insync` for IBKR connection
- `ta` library for technical indicators (not ta-lib)
- `pandas` / `numpy` for data
- `streamlit` for dashboard (separate subprocess)
- `plotly` for charts
- IBKR TWS for all market data (live + backtest)
- SQLite for trade storage, JSON for IPC

## Key Conventions
- All timestamps stored as UTC internally, displayed as US/Eastern
- Singleton pattern for IBKR connection (`IBKRConnection`)
- Rate limiting via asyncio semaphores (45 msg/sec, 3 concurrent historical)
- GTC server-side stop-losses (survive bot crashes)
- All-or-nothing signal gate (5 conditions must pass)
- Settings in `config/settings.py` as named constants

## File Organization
- `config/` — settings and environment
- `broker/` — IBKR connection, orders, positions
- `risk/` — risk manager, trailing stops, partial exits
- `strategies/` — indicators, fibonacci, signals, entry logic
- `scanner/` — real-time, pre-market, after-hours scanners
- `notifications/` — trade logger (SQLite+CSV), telegram alerts
- `backtesting/` — data download, engine, report generation
- `dashboard/` — Streamlit app (reads SQLite + JSON, writes control.json)
- `data/` — runtime data (gitignored)
- `logs/` — log files (gitignored)

## Running
- `python main.py` — live/paper trading
- `python main.py --backtest` — run backtester
- `python main.py --dashboard` — dashboard only

## Testing Approach
- Test IBKR connection: `python -c "from broker.ibkr_connection import get_connection; ..."`
- Test fibonacci: feed known price data and verify levels
- Test scanner: run standalone scan cycle
- Test backtest: `python main.py --backtest`

## Important Notes
- IBKR TWS must be running with API enabled on port 7497 (paper)
- Never deploy to live trading without extensive paper testing
- PDT tracking is independent (don't rely solely on IBKR enforcement)
- Float filter via IBKR fundamental data (reqFundamentalData ReportSnapshot)
- Fibonacci cache: 24h TTL, invalidated when price exceeds grid top
