"""Event-driven backtester reusing the same strategy logic."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd
import numpy as np

from config.settings import (
    STARTING_CAPITAL,
    MAX_POSITION_SIZE_PCT,
    MAX_LOSS_PER_TRADE_PCT,
    MAX_OPEN_POSITIONS,
    TRAILING_STOP_INITIAL_PCT,
    TRAILING_STOP_TIGHTEN_PCT,
    PARTIAL_EXIT_STAGES,
    FIB_LEVELS,
)
from strategies.indicators import (
    is_above_sma200,
    is_below_upper_bollinger,
    volume_spike,
    is_above_vwap,
    volume_ratio,
)
from strategies.fibonacci_engine import (
    compute_fibonacci_levels,
    is_near_fib_support,
    find_nearest_support,
)
from backtesting.data_downloader import download_historical_data

logger = logging.getLogger("trading_bot.backtest")


@dataclass
class BacktestTrade:
    symbol: str
    entry_date: datetime
    entry_price: float
    quantity: int
    stop_price: float
    exit_date: Optional[datetime] = None
    exit_price: Optional[float] = None
    pnl: float = 0.0
    pnl_pct: float = 0.0
    exit_reason: str = ""
    partial_exits: list = field(default_factory=list)


@dataclass
class BacktestResult:
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    start_capital: float = STARTING_CAPITAL
    end_capital: float = STARTING_CAPITAL
    trades: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)


class BacktestEngine:
    """Event-driven backtester that simulates the live strategy."""

    def __init__(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        starting_capital: float = STARTING_CAPITAL,
    ) -> None:
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.capital = starting_capital
        self.starting_capital = starting_capital

        # State
        self._positions: dict[str, BacktestTrade] = {}
        self._highest_prices: dict[str, float] = {}
        self._trades: list[BacktestTrade] = []
        self._equity_curve: list[tuple[datetime, float]] = []
        self._peak_equity = starting_capital
        self._max_drawdown = 0.0

    def run(self) -> BacktestResult:
        """Run the full backtest."""
        logger.info(
            f"Backtest: {len(self.symbols)} symbols, "
            f"{self.start_date} to {self.end_date}, "
            f"capital=${self.starting_capital:,.2f}"
        )

        # Download data
        data = {}
        for symbol in self.symbols:
            df = download_historical_data(symbol, self.start_date, self.end_date)
            if len(df) >= 50:
                data[symbol] = df

        if not data:
            logger.error("No valid data for any symbol")
            return BacktestResult()

        logger.info(f"Loaded data for {len(data)} symbols")

        # Get all unique dates
        all_dates = set()
        for df in data.values():
            all_dates.update(df.index)
        all_dates = sorted(all_dates)

        # Simulate day by day
        for date in all_dates:
            self._process_day(date, data)

        # Close remaining positions at last price
        for symbol in list(self._positions.keys()):
            if symbol in data:
                last_price = data[symbol]["close"].iloc[-1]
                self._close_position(symbol, last_price, all_dates[-1], "end_of_backtest")

        return self._compile_results()

    def _process_day(self, date: datetime, data: dict[str, pd.DataFrame]) -> None:
        """Process one trading day."""
        # Check existing positions (stops, partial exits)
        for symbol in list(self._positions.keys()):
            if symbol not in data or date not in data[symbol].index:
                continue
            row = data[symbol].loc[date]
            self._check_stop_and_exits(symbol, row, date)

        # Look for new entries
        if len(self._positions) < MAX_OPEN_POSITIONS:
            for symbol, df in data.items():
                if symbol in self._positions:
                    continue
                if date not in df.index:
                    continue
                # Need enough history before this date
                idx = df.index.get_loc(date)
                if idx < 200:
                    continue
                history = df.iloc[:idx + 1]
                self._evaluate_entry(symbol, history, date)

        # Record equity
        equity = self.capital
        for symbol, trade in self._positions.items():
            if symbol in data and date in data[symbol].index:
                current = data[symbol].loc[date]["close"]
                equity += (current - trade.entry_price) * trade.quantity
        self._equity_curve.append((date, equity))

        # Track drawdown
        if equity > self._peak_equity:
            self._peak_equity = equity
        drawdown = self._peak_equity - equity
        if drawdown > self._max_drawdown:
            self._max_drawdown = drawdown

    def _evaluate_entry(
        self, symbol: str, df: pd.DataFrame, date: datetime
    ) -> None:
        """Evaluate entry signal for a symbol."""
        current_price = df["close"].iloc[-1]

        # Run all 5 signal conditions
        cond1 = is_above_sma200(df)
        cond2 = volume_spike(df)
        cond3 = True  # VWAP needs intraday data, approximate as pass
        fib_levels = compute_fibonacci_levels(df, symbol=symbol)
        cond4 = is_near_fib_support(fib_levels, current_price)
        cond5 = is_below_upper_bollinger(df)

        if not all([cond1, cond2, cond4, cond5]):
            return

        # Calculate stop and position size
        support = find_nearest_support(fib_levels, current_price)
        if support:
            stop_price = support[1] * 0.99
            max_stop = current_price * (1 - TRAILING_STOP_INITIAL_PCT)
            stop_price = max(stop_price, max_stop)
        else:
            stop_price = current_price * (1 - TRAILING_STOP_INITIAL_PCT)

        risk_per_share = current_price - stop_price
        if risk_per_share <= 0:
            return

        max_risk = self.capital * MAX_LOSS_PER_TRADE_PCT
        qty_by_risk = int(max_risk / risk_per_share)
        max_position = self.capital * MAX_POSITION_SIZE_PCT
        qty_by_cap = int(max_position / current_price)
        quantity = min(qty_by_risk, qty_by_cap)

        if quantity <= 0:
            return

        # Enter position
        cost = quantity * current_price
        if cost > self.capital:
            quantity = int(self.capital / current_price)
            if quantity <= 0:
                return
            cost = quantity * current_price

        self.capital -= cost
        trade = BacktestTrade(
            symbol=symbol,
            entry_date=date,
            entry_price=current_price,
            quantity=quantity,
            stop_price=stop_price,
        )
        self._positions[symbol] = trade
        self._highest_prices[symbol] = current_price

    def _check_stop_and_exits(
        self, symbol: str, row: pd.Series, date: datetime
    ) -> None:
        """Check stop-loss and partial exits for an open position."""
        trade = self._positions[symbol]
        low = row["low"]
        high = row["high"]
        close = row["close"]

        # Update highest price
        if high > self._highest_prices.get(symbol, 0):
            self._highest_prices[symbol] = high

        # Check stop-loss (using low of day)
        if low <= trade.stop_price:
            self._close_position(symbol, trade.stop_price, date, "stop_loss")
            return

        # Update trailing stop
        highest = self._highest_prices[symbol]
        partial_done = len(trade.partial_exits)
        trail_pct = (
            TRAILING_STOP_TIGHTEN_PCT if partial_done > 0
            else TRAILING_STOP_INITIAL_PCT
        )
        new_stop = highest * (1 - trail_pct)
        if new_stop > trade.stop_price:
            trade.stop_price = new_stop

        # Check partial exits (simplified: use close price)
        if partial_done < len(PARTIAL_EXIT_STAGES):
            stage = PARTIAL_EXIT_STAGES[partial_done]
            # Simple target: entry + (fib_level * risk)
            risk = trade.entry_price - trade.stop_price
            target = trade.entry_price + (risk * stage["fib_level"] / 0.236)
            if close >= target:
                exit_qty = max(1, int(trade.quantity * stage["exit_pct"]))
                exit_qty = min(exit_qty, trade.quantity)
                trade.partial_exits.append({
                    "date": date,
                    "price": close,
                    "quantity": exit_qty,
                    "stage": partial_done + 1,
                })
                trade.quantity -= exit_qty
                self.capital += exit_qty * close
                if trade.quantity <= 0:
                    self._finalize_trade(trade, close, date, "all_partial_exits")
                    self._positions.pop(symbol, None)
                    self._highest_prices.pop(symbol, None)

    def _close_position(
        self, symbol: str, exit_price: float, date: datetime, reason: str
    ) -> None:
        """Close a full position."""
        trade = self._positions.pop(symbol, None)
        self._highest_prices.pop(symbol, None)
        if trade is None:
            return
        self.capital += trade.quantity * exit_price
        self._finalize_trade(trade, exit_price, date, reason)

    def _finalize_trade(
        self, trade: BacktestTrade, exit_price: float,
        date: datetime, reason: str,
    ) -> None:
        """Finalize trade record."""
        # Include partial exit P&L
        partial_pnl = sum(
            (p["price"] - trade.entry_price) * p["quantity"]
            for p in trade.partial_exits
        )
        remaining_pnl = (exit_price - trade.entry_price) * trade.quantity
        trade.pnl = partial_pnl + remaining_pnl
        trade.pnl_pct = trade.pnl / (trade.entry_price * trade.quantity) if trade.quantity > 0 else 0
        trade.exit_price = exit_price
        trade.exit_date = date
        trade.exit_reason = reason
        self._trades.append(trade)

    def _compile_results(self) -> BacktestResult:
        """Compile final backtest results."""
        result = BacktestResult()
        result.trades = self._trades
        result.equity_curve = self._equity_curve
        result.start_capital = self.starting_capital
        result.end_capital = self.capital
        result.total_trades = len(self._trades)
        result.max_drawdown = self._max_drawdown
        result.max_drawdown_pct = (
            self._max_drawdown / self._peak_equity if self._peak_equity > 0 else 0
        )

        if not self._trades:
            return result

        pnls = [t.pnl for t in self._trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        result.total_pnl = sum(pnls)
        result.winning_trades = len(wins)
        result.losing_trades = len(losses)
        result.win_rate = len(wins) / len(pnls) if pnls else 0
        result.avg_win = np.mean(wins) if wins else 0
        result.avg_loss = np.mean(losses) if losses else 0
        result.profit_factor = (
            abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else float("inf")
        )

        # Sharpe ratio (annualized, daily returns)
        if len(self._equity_curve) > 1:
            equities = [e[1] for e in self._equity_curve]
            returns = pd.Series(equities).pct_change().dropna()
            if returns.std() > 0:
                result.sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252)

        logger.info(
            f"Backtest complete: {result.total_trades} trades, "
            f"P&L=${result.total_pnl:+.2f}, "
            f"Win rate={result.win_rate:.1%}, "
            f"Sharpe={result.sharpe_ratio:.2f}, "
            f"Max DD={result.max_drawdown_pct:.1%}"
        )
        return result


async def run_backtest() -> None:
    """Entry point for CLI backtest."""
    from backtesting.data_downloader import get_penny_stock_universe
    from backtesting.report_generator import generate_report

    symbols = get_penny_stock_universe("2024-01-01")
    engine = BacktestEngine(
        symbols=symbols,
        start_date="2024-01-01",
        end_date="2024-12-31",
    )
    result = engine.run()
    generate_report(result, output_dir="backtesting/reports")
