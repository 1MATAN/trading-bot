"""Async Telegram alerts for entry, exit, scanner, and system events."""

import asyncio
import logging
from typing import Optional

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_ENABLED

logger = logging.getLogger("trading_bot.telegram")

# Conditional import — don't fail if telegram not configured
_bot = None


async def _get_bot():
    """Lazy-initialize the Telegram bot."""
    global _bot
    if _bot is not None:
        return _bot
    if not TELEGRAM_ENABLED:
        return None
    try:
        from telegram import Bot
        _bot = Bot(token=TELEGRAM_BOT_TOKEN)
        return _bot
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot: {e}")
        return None


async def send_message(text: str, parse_mode: str = "HTML") -> bool:
    """Send a message to the configured Telegram chat."""
    if not TELEGRAM_ENABLED:
        return False
    bot = await _get_bot()
    if bot is None:
        return False
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode=parse_mode,
        )
        return True
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


async def notify_entry(
    symbol: str,
    quantity: int,
    price: float,
    stop_price: float,
    risk_dollars: float,
) -> None:
    """Notify on trade entry."""
    text = (
        f"🟢 <b>ENTRY</b>\n"
        f"<b>{symbol}</b> — {quantity} shares\n"
        f"Price: ${price:.4f}\n"
        f"Stop: ${stop_price:.4f}\n"
        f"Risk: ${risk_dollars:.2f}\n"
    )
    await send_message(text)


async def notify_exit(
    symbol: str,
    quantity: int,
    price: float,
    pnl: float,
    pnl_pct: float,
    reason: str = "signal",
) -> None:
    """Notify on trade exit."""
    emoji = "🟢" if pnl >= 0 else "🔴"
    text = (
        f"{emoji} <b>EXIT</b> ({reason})\n"
        f"<b>{symbol}</b> — {quantity} shares\n"
        f"Price: ${price:.4f}\n"
        f"P&L: ${pnl:+.2f} ({pnl_pct:+.2%})\n"
    )
    await send_message(text)


async def notify_partial_exit(
    symbol: str,
    quantity: int,
    price: float,
    stage: int,
    total_stages: int,
) -> None:
    """Notify on partial exit."""
    text = (
        f"📊 <b>PARTIAL EXIT</b> ({stage}/{total_stages})\n"
        f"<b>{symbol}</b> — sold {quantity} shares\n"
        f"Price: ${price:.4f}\n"
    )
    await send_message(text)


async def notify_scanner(candidates: list[dict]) -> None:
    """Notify on scanner results."""
    if not candidates:
        return
    lines = [f"🔍 <b>Scanner: {len(candidates)} candidates</b>"]
    for c in candidates[:10]:  # limit to 10
        lines.append(f"  • {c.get('symbol', '?')} — {c.get('details', '')}")
    await send_message("\n".join(lines))


async def notify_daily_summary(summary: dict) -> None:
    """Notify daily summary."""
    text = (
        f"📈 <b>Daily Summary</b>\n"
        f"Trades: {summary.get('total_trades', 0)}\n"
        f"Wins: {summary.get('winning_trades', 0)} | "
        f"Losses: {summary.get('losing_trades', 0)}\n"
        f"P&L: ${summary.get('total_pnl', 0):+.2f}\n"
        f"Portfolio: ${summary.get('portfolio_value', 0):,.2f}\n"
    )
    await send_message(text)


async def notify_system(message: str, level: str = "info") -> None:
    """Notify system events (start, stop, errors)."""
    emoji_map = {"info": "ℹ️", "warning": "⚠️", "error": "🚨", "critical": "🔥"}
    emoji = emoji_map.get(level, "ℹ️")
    text = f"{emoji} <b>System</b>\n{message}"
    await send_message(text)


async def notify_risk_alert(message: str) -> None:
    """Notify risk management alerts."""
    text = f"⚠️ <b>Risk Alert</b>\n{message}"
    await send_message(text)
