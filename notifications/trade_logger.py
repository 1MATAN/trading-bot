"""SQLite + CSV dual logging, daily summaries."""

import csv
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from config.settings import TRADES_DB_PATH, TRADES_CSV_PATH
from utils.helpers import format_currency
from utils.time_utils import now_utc, today_str

logger = logging.getLogger("trading_bot.trade_logger")

_CREATE_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    action TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price REAL NOT NULL,
    total_value REAL NOT NULL,
    stop_price REAL,
    pnl REAL,
    pnl_pct REAL,
    strategy TEXT DEFAULT 'fibonacci_momentum',
    entry_time TEXT,
    exit_time TEXT,
    trade_type TEXT,
    notes TEXT,
    created_at TEXT NOT NULL
);
"""

_CREATE_DAILY_SUMMARY_TABLE = """
CREATE TABLE IF NOT EXISTS daily_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,
    total_pnl REAL DEFAULT 0,
    portfolio_value REAL,
    max_drawdown REAL,
    created_at TEXT NOT NULL
);
"""

_CSV_HEADERS = [
    "timestamp", "symbol", "action", "quantity", "price",
    "total_value", "stop_price", "pnl", "pnl_pct", "notes",
]


class TradeLogger:
    """Dual-writes trades to SQLite and CSV."""

    def __init__(self) -> None:
        self._db_path = TRADES_DB_PATH
        self._csv_path = TRADES_CSV_PATH
        self._init_db()
        self._init_csv()

    def _init_db(self) -> None:
        """Initialize SQLite database."""
        conn = sqlite3.connect(self._db_path)
        conn.execute(_CREATE_TRADES_TABLE)
        conn.execute(_CREATE_DAILY_SUMMARY_TABLE)
        conn.commit()
        conn.close()
        logger.debug(f"Trade database initialized: {self._db_path}")

    def _init_csv(self) -> None:
        """Initialize CSV file with headers if needed."""
        if not self._csv_path.exists():
            with open(self._csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(_CSV_HEADERS)

    def log_entry(
        self,
        symbol: str,
        quantity: int,
        price: float,
        stop_price: float = 0.0,
        notes: str = "",
    ) -> int:
        """Log a trade entry (buy)."""
        return self._log_trade(
            symbol=symbol,
            action="BUY",
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            trade_type="entry",
            entry_time=now_utc().isoformat(),
            notes=notes,
        )

    def log_exit(
        self,
        symbol: str,
        quantity: int,
        price: float,
        entry_price: float,
        entry_time: Optional[str] = None,
        notes: str = "",
    ) -> int:
        """Log a trade exit (sell) with P&L calculation."""
        pnl = (price - entry_price) * quantity
        pnl_pct = (price - entry_price) / entry_price if entry_price > 0 else 0

        trade_id = self._log_trade(
            symbol=symbol,
            action="SELL",
            quantity=quantity,
            price=price,
            pnl=pnl,
            pnl_pct=pnl_pct,
            trade_type="exit",
            entry_time=entry_time,
            exit_time=now_utc().isoformat(),
            notes=notes,
        )

        logger.info(
            f"Trade logged: SELL {quantity} {symbol} @ ${price:.4f}, "
            f"P&L: {format_currency(pnl)} ({pnl_pct:.2%})"
        )
        return trade_id

    def log_partial_exit(
        self,
        symbol: str,
        quantity: int,
        price: float,
        entry_price: float,
        stage: int,
        notes: str = "",
    ) -> int:
        """Log a partial exit."""
        return self.log_exit(
            symbol=symbol,
            quantity=quantity,
            price=price,
            entry_price=entry_price,
            notes=f"partial_exit_stage_{stage} {notes}".strip(),
        )

    def update_daily_summary(self, portfolio_value: float) -> None:
        """Update today's daily summary."""
        date = today_str()
        conn = sqlite3.connect(self._db_path)
        cursor = conn.cursor()

        # Calculate today's stats from trades
        cursor.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END), "
            "COALESCE(SUM(pnl), 0) "
            "FROM trades WHERE DATE(created_at) = ? AND action = 'SELL'",
            (date,),
        )
        row = cursor.fetchone()
        total_trades = row[0] or 0
        winning = row[1] or 0
        losing = row[2] or 0
        total_pnl = row[3] or 0.0

        cursor.execute(
            """INSERT INTO daily_summary (date, total_trades, winning_trades,
               losing_trades, total_pnl, portfolio_value, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET
               total_trades=?, winning_trades=?, losing_trades=?,
               total_pnl=?, portfolio_value=?""",
            (
                date, total_trades, winning, losing, total_pnl,
                portfolio_value, now_utc().isoformat(),
                total_trades, winning, losing, total_pnl, portfolio_value,
            ),
        )
        conn.commit()
        conn.close()

    def get_daily_summary(self, date: Optional[str] = None) -> Optional[dict]:
        """Get summary for a given date."""
        date = date or today_str()
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM daily_summary WHERE date = ?", (date,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_recent_trades(self, limit: int = 50) -> list[dict]:
        """Get recent trades for dashboard."""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM trades ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_all_summaries(self) -> list[dict]:
        """Get all daily summaries."""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM daily_summary ORDER BY date DESC")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _log_trade(self, **kwargs) -> int:
        """Write trade to both SQLite and CSV."""
        now = now_utc().isoformat()
        kwargs.setdefault("created_at", now)
        kwargs.setdefault("total_value", kwargs.get("quantity", 0) * kwargs.get("price", 0))

        # SQLite
        conn = sqlite3.connect(self._db_path)
        cursor = conn.cursor()
        columns = ", ".join(kwargs.keys())
        placeholders = ", ".join("?" for _ in kwargs)
        cursor.execute(
            f"INSERT INTO trades ({columns}) VALUES ({placeholders})",
            tuple(kwargs.values()),
        )
        trade_id = cursor.lastrowid
        conn.commit()
        conn.close()

        # CSV
        csv_row = [
            kwargs.get("created_at", now),
            kwargs.get("symbol", ""),
            kwargs.get("action", ""),
            kwargs.get("quantity", 0),
            kwargs.get("price", 0),
            kwargs.get("total_value", 0),
            kwargs.get("stop_price", ""),
            kwargs.get("pnl", ""),
            kwargs.get("pnl_pct", ""),
            kwargs.get("notes", ""),
        ]
        with open(self._csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(csv_row)

        return trade_id
