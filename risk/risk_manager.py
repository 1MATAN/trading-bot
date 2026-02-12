"""Central risk gate: PDT, daily loss, position sizing, cooldown."""

import logging
from collections import deque
from datetime import datetime, timedelta
from typing import Optional

from config.settings import (
    STARTING_CAPITAL,
    MAX_POSITION_SIZE_PCT,
    MAX_OPEN_POSITIONS,
    MAX_DAILY_LOSS_PCT,
    MAX_LOSS_PER_TRADE_PCT,
    PDT_MAX_DAY_TRADES,
    PDT_ROLLING_WINDOW_DAYS,
    COOLDOWN_AFTER_LOSS_SECONDS,
    COOLDOWN_AFTER_MAX_DAILY_LOSS,
)
from utils.helpers import format_currency, format_pct
from utils.time_utils import now_utc

logger = logging.getLogger("trading_bot.risk")


class RiskManager:
    """Central risk gate — all trades must pass through here."""

    def __init__(self, portfolio_value: Optional[float] = None) -> None:
        self._portfolio_value = portfolio_value or STARTING_CAPITAL
        self._daily_pnl = 0.0
        self._daily_trades = 0
        self._day_trade_timestamps: deque[datetime] = deque()
        self._last_loss_time: Optional[datetime] = None
        self._daily_loss_halt = False
        self._current_date: Optional[str] = None

    def update_portfolio_value(self, value: float) -> None:
        """Update current portfolio value from IBKR account."""
        self._portfolio_value = value

    def _reset_daily_if_needed(self) -> None:
        """Reset daily counters on new trading day."""
        today = now_utc().strftime("%Y-%m-%d")
        if self._current_date != today:
            self._current_date = today
            self._daily_pnl = 0.0
            self._daily_trades = 0
            self._daily_loss_halt = False
            logger.info(f"Daily risk counters reset for {today}")

    def record_trade_pnl(self, pnl: float, is_day_trade: bool = False) -> None:
        """Record a completed trade's P&L."""
        self._reset_daily_if_needed()
        self._daily_pnl += pnl
        self._daily_trades += 1

        if is_day_trade:
            self._day_trade_timestamps.append(now_utc())

        if pnl < 0:
            self._last_loss_time = now_utc()

        # Check daily loss limit
        max_daily_loss = self._portfolio_value * MAX_DAILY_LOSS_PCT
        if self._daily_pnl <= -max_daily_loss:
            self._daily_loss_halt = True
            logger.critical(
                f"DAILY LOSS LIMIT HIT: {format_currency(self._daily_pnl)} "
                f"(limit: {format_currency(-max_daily_loss)}). Trading halted."
            )

    def can_trade(self, open_position_count: int) -> tuple[bool, str]:
        """Master gate: check all risk conditions before entering a trade."""
        self._reset_daily_if_needed()

        # Daily loss halt
        if self._daily_loss_halt:
            return False, "Daily loss limit reached — trading halted for today"

        # Max positions
        if open_position_count >= MAX_OPEN_POSITIONS:
            return False, f"Max open positions ({MAX_OPEN_POSITIONS}) reached"

        # PDT check
        if self._is_pdt_restricted():
            return False, (
                f"PDT limit: {self._count_recent_day_trades()}/"
                f"{PDT_MAX_DAY_TRADES} day trades in rolling "
                f"{PDT_ROLLING_WINDOW_DAYS} days"
            )

        # Cooldown after loss
        if self._is_in_cooldown():
            remaining = self._cooldown_remaining()
            return False, f"Cooldown active — {remaining:.0f}s remaining after loss"

        return True, "OK"

    def calculate_position_size(
        self, entry_price: float, stop_price: float
    ) -> int:
        """Calculate position size based on risk per trade and price."""
        if entry_price <= 0 or stop_price <= 0 or stop_price >= entry_price:
            return 0

        risk_per_share = entry_price - stop_price
        max_risk_dollars = self._portfolio_value * MAX_LOSS_PER_TRADE_PCT
        risk_based_qty = int(max_risk_dollars / risk_per_share)

        # Cap by max position size
        max_position_dollars = self._portfolio_value * MAX_POSITION_SIZE_PCT
        cap_based_qty = int(max_position_dollars / entry_price)

        quantity = min(risk_based_qty, cap_based_qty)

        # Ensure at least 1 share if within limits
        quantity = max(quantity, 1) if quantity > 0 else 0

        logger.debug(
            f"Position sizing: entry=${entry_price:.4f} stop=${stop_price:.4f} "
            f"risk/share=${risk_per_share:.4f} → {quantity} shares "
            f"(risk={format_currency(quantity * risk_per_share)}, "
            f"value={format_currency(quantity * entry_price)})"
        )
        return quantity

    def _is_pdt_restricted(self) -> bool:
        """Check if we'd exceed PDT day trade limit."""
        return self._count_recent_day_trades() >= PDT_MAX_DAY_TRADES

    def _count_recent_day_trades(self) -> int:
        """Count day trades in the rolling window."""
        cutoff = now_utc() - timedelta(days=PDT_ROLLING_WINDOW_DAYS)
        # Prune old entries
        while self._day_trade_timestamps and self._day_trade_timestamps[0] < cutoff:
            self._day_trade_timestamps.popleft()
        return len(self._day_trade_timestamps)

    def _is_in_cooldown(self) -> bool:
        """Check if cooldown period is active after a loss."""
        if self._last_loss_time is None:
            return False

        elapsed = (now_utc() - self._last_loss_time).total_seconds()

        if self._daily_loss_halt:
            return elapsed < COOLDOWN_AFTER_MAX_DAILY_LOSS

        return elapsed < COOLDOWN_AFTER_LOSS_SECONDS

    def _cooldown_remaining(self) -> float:
        """Seconds remaining in cooldown."""
        if self._last_loss_time is None:
            return 0.0
        elapsed = (now_utc() - self._last_loss_time).total_seconds()
        limit = (
            COOLDOWN_AFTER_MAX_DAILY_LOSS
            if self._daily_loss_halt
            else COOLDOWN_AFTER_LOSS_SECONDS
        )
        return max(0.0, limit - elapsed)

    def get_status(self) -> dict:
        """Status snapshot for dashboard."""
        self._reset_daily_if_needed()
        return {
            "portfolio_value": self._portfolio_value,
            "daily_pnl": self._daily_pnl,
            "daily_trades": self._daily_trades,
            "day_trades_rolling": self._count_recent_day_trades(),
            "pdt_limit": PDT_MAX_DAY_TRADES,
            "daily_loss_halt": self._daily_loss_halt,
            "max_daily_loss": self._portfolio_value * MAX_DAILY_LOSS_PCT,
            "in_cooldown": self._is_in_cooldown(),
            "cooldown_remaining": self._cooldown_remaining(),
        }
