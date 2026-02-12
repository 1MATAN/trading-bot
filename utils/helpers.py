"""Utility functions and logging setup."""

import logging
import sys
from datetime import datetime
from pathlib import Path

from config.settings import LOG_DIR, LOG_LEVEL, LOG_FORMAT, LOG_DATE_FORMAT


def setup_logging(name: str = "trading_bot") -> logging.Logger:
    """Configure and return the root logger for the application."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler — rotates daily by filename
    today = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(
        LOG_DIR / f"{name}_{today}.log", encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def round_price(price: float, tick_size: float = 0.01) -> float:
    """Round price to nearest tick size."""
    return round(round(price / tick_size) * tick_size, 4)


def format_currency(amount: float) -> str:
    """Format number as USD currency string."""
    return f"${amount:,.2f}"


def format_pct(value: float) -> str:
    """Format decimal as percentage string (0.05 → '5.00%')."""
    return f"{value * 100:.2f}%"


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Divide with zero-safety."""
    if denominator == 0:
        return default
    return numerator / denominator


def clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp value between min and max."""
    return max(min_val, min(value, max_val))


def ensure_dir(path: Path) -> Path:
    """Ensure directory exists, create if needed, return path."""
    path.mkdir(parents=True, exist_ok=True)
    return path
