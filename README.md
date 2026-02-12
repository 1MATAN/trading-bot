# Penny Stock Trading Bot

Automated penny stock trading bot using Interactive Brokers TWS API with a recursive Fibonacci-based momentum strategy.

## Features

- **Recursive Fibonacci Engine**: Multi-depth Fibonacci retracement grids for precise support/resistance identification
- **3-Layer Scanner**: IBKR scanner → technical analysis → Fibonacci confirmation
- **Full Risk Management**: PDT tracking, daily loss limits, position sizing, cooldown periods
- **Staged Exits**: Partial profit-taking at Fibonacci resistance levels (25% each stage)
- **Trailing Stops**: GTC server-side stops that survive bot crashes, automatically tightened
- **Pre/After-Market**: Builds watchlists for upcoming sessions
- **Telegram Alerts**: Real-time notifications for entries, exits, and system events
- **Backtesting**: Event-driven backtester with Plotly HTML reports
- **Web Dashboard**: Streamlit dashboard with live P&L, positions, scanner, and controls

## Architecture

```
IBKR TWS → IBKRConnection (singleton)
  → Scanner (reqScannerSubscription)
    → Layer 1: IBKR filter (price $0.50–$5, volume 500k+, 5%+ change)
    → Layer 2: Technical analysis (SMA200, volume spike, Bollinger, VWAP)
    → Layer 3: Fibonacci confirmation (recursive sub-grids)
  → Signal Scorer (all 5 conditions must pass)
  → Momentum Entry (risk check → position size → order → stop-loss)
  → Trailing Stop + Partial Exits at Fibonacci levels
  → Trade Logger (SQLite + CSV) + Telegram
```

## Setup

### Prerequisites

- Python 3.11+
- Interactive Brokers TWS or IB Gateway (paper trading recommended)
- TWS API enabled (Edit → Global Configuration → API → Settings)

### Installation

```bash
git clone <repo-url>
cd trading-bot
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### Configuration

```bash
cp config/.env.template config/.env
```

Edit `config/.env`:
```
IBKR_HOST=127.0.0.1
IBKR_PORT=7497          # 7497=paper, 7496=live
IBKR_CLIENT_ID=1
TELEGRAM_BOT_TOKEN=     # optional
TELEGRAM_CHAT_ID=       # optional
PAPER_TRADING=true
```

### TWS Settings

1. Open TWS → Edit → Global Configuration → API → Settings
2. Enable "Enable ActiveX and Socket Clients"
3. Set socket port to `7497` (paper) or `7496` (live)
4. Add `127.0.0.1` to trusted IPs
5. Uncheck "Read-Only API"

## Usage

### Live/Paper Trading

```bash
python main.py
```

The bot will:
1. Connect to IBKR TWS
2. Sync existing positions
3. Launch the Streamlit dashboard
4. Run the scanner/trading loop during market hours
5. Build watchlists during pre/after-market

### Backtesting

```bash
python main.py --backtest
```

Generates an HTML report in `backtesting/reports/`.

### Dashboard Only

```bash
python main.py --dashboard
```

Opens the Streamlit dashboard on port 8501.

## Strategy

### Signal Conditions (all must pass)

1. **Above SMA200**: Long-term uptrend confirmation
2. **Volume Spike**: 2x+ average volume (institutional interest)
3. **Above VWAP**: Intraday demand confirmation
4. **Near Fibonacci Support**: Price near a recursive Fibonacci level
5. **Below Upper Bollinger**: Not overbought

### Risk Management

| Parameter | Value |
|-----------|-------|
| Starting Capital | $3,000 |
| Max Position Size | 15% of portfolio |
| Max Risk Per Trade | 1% of portfolio |
| Max Open Positions | 5 |
| Max Daily Loss | 3% of portfolio |
| PDT Limit | 3 day trades / 5 business days |
| Cooldown After Loss | 5 minutes |

### Exit Strategy

- **Trailing Stop**: Starts at 5% below entry, tightens to 3% after first partial exit
- **Partial Exits**: 25% sold at each Fibonacci resistance level (4 stages)
- **GTC Stops**: Server-side, enforced by IBKR even if bot is offline

## Project Structure

```
trading-bot/
├── main.py                    # Main orchestrator
├── config/
│   ├── settings.py            # All tunable parameters
│   └── .env.template          # Environment template
├── broker/
│   ├── ibkr_connection.py     # Singleton IB() with auto-reconnect
│   ├── order_manager.py       # Order lifecycle
│   └── position_manager.py    # Position tracking
├── risk/
│   ├── risk_manager.py        # Central risk gate
│   ├── trailing_stop.py       # Trailing stop monitor
│   └── partial_exits.py       # Staged Fibonacci exits
├── strategies/
│   ├── indicators.py          # SMA, Bollinger, VWAP, volume
│   ├── fibonacci_engine.py    # Recursive Fibonacci levels
│   ├── signal_scorer.py       # All-or-nothing gate
│   └── momentum_entry.py      # Entry pipeline
├── scanner/
│   ├── realtime_scanner.py    # 3-layer market scanner
│   ├── premarket_scanner.py   # Pre-market watchlist
│   └── afterhours_scanner.py  # After-hours watchlist
├── notifications/
│   ├── trade_logger.py        # SQLite + CSV logging
│   └── telegram_bot.py        # Telegram alerts
├── backtesting/
│   ├── data_downloader.py     # yfinance + parquet caching
│   ├── backtest_engine.py     # Event-driven backtester
│   └── report_generator.py    # Plotly HTML reports
├── dashboard/
│   └── app.py                 # Streamlit dashboard
├── data/                      # SQLite DB, CSV, state files
└── logs/                      # Daily log files
```

## Disclaimer

This software is for educational purposes only. Trading penny stocks involves significant risk of loss. Past performance does not guarantee future results. Use paper trading mode to test before risking real capital. The authors are not responsible for any financial losses.
