"""Market hours and timezone handling (US/Eastern)."""

from datetime import datetime, time, timedelta
import pytz

from config.settings import (
    TIMEZONE,
    PREMARKET_START,
    MARKET_OPEN,
    MARKET_CLOSE,
    AFTERHOURS_END,
)

ET = pytz.timezone(TIMEZONE)
UTC = pytz.UTC

_PREMARKET_START = time(*map(int, PREMARKET_START.split(":")))
_MARKET_OPEN = time(*map(int, MARKET_OPEN.split(":")))
_MARKET_CLOSE = time(*map(int, MARKET_CLOSE.split(":")))
_AFTERHOURS_END = time(*map(int, AFTERHOURS_END.split(":")))


def now_et() -> datetime:
    """Current time in US/Eastern."""
    return datetime.now(ET)


def now_utc() -> datetime:
    """Current time in UTC."""
    return datetime.now(UTC)


def to_et(dt: datetime) -> datetime:
    """Convert any datetime to US/Eastern."""
    if dt.tzinfo is None:
        dt = UTC.localize(dt)
    return dt.astimezone(ET)


def to_utc(dt: datetime) -> datetime:
    """Convert any datetime to UTC."""
    if dt.tzinfo is None:
        dt = ET.localize(dt)
    return dt.astimezone(UTC)


def is_market_open() -> bool:
    """Check if regular market hours are active (9:30–16:00 ET, weekdays)."""
    now = now_et()
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    return _MARKET_OPEN <= now.time() < _MARKET_CLOSE


def is_premarket() -> bool:
    """Check if pre-market session is active (4:00–9:30 ET, weekdays)."""
    now = now_et()
    if now.weekday() >= 5:
        return False
    return _PREMARKET_START <= now.time() < _MARKET_OPEN


def is_afterhours() -> bool:
    """Check if after-hours session is active (16:00–20:00 ET, weekdays)."""
    now = now_et()
    if now.weekday() >= 5:
        return False
    return _MARKET_CLOSE <= now.time() < _AFTERHOURS_END


def is_any_session_active() -> bool:
    """Check if any trading session (pre/regular/after) is active."""
    return is_premarket() or is_market_open() or is_afterhours()


def seconds_until_market_open() -> float:
    """Seconds until next market open. Returns 0 if market is open."""
    if is_market_open():
        return 0.0
    now = now_et()
    target = now.replace(
        hour=int(MARKET_OPEN.split(":")[0]),
        minute=int(MARKET_OPEN.split(":")[1]),
        second=0,
        microsecond=0,
    )
    # If we've passed market open today, target next weekday
    if now.time() >= _MARKET_OPEN or now.weekday() >= 5:
        days_ahead = 1
        while True:
            candidate = target + timedelta(days=days_ahead)
            if candidate.weekday() < 5:
                target = candidate
                break
            days_ahead += 1
    return max(0.0, (target - now).total_seconds())


def seconds_until_market_close() -> float:
    """Seconds until market close. Returns 0 if market is closed."""
    if not is_market_open():
        return 0.0
    now = now_et()
    close = now.replace(
        hour=int(MARKET_CLOSE.split(":")[0]),
        minute=int(MARKET_CLOSE.split(":")[1]),
        second=0,
        microsecond=0,
    )
    return max(0.0, (close - now).total_seconds())


def today_str() -> str:
    """Today's date as YYYY-MM-DD in Eastern time."""
    return now_et().strftime("%Y-%m-%d")


def seconds_until_next_session() -> float:
    """Seconds until the next trading session starts (pre-market at 4:00 ET).

    Returns 0 if any session is currently active.
    """
    if is_any_session_active():
        return 0.0
    now = now_et()
    target = now.replace(
        hour=int(PREMARKET_START.split(":")[0]),
        minute=int(PREMARKET_START.split(":")[1]),
        second=0,
        microsecond=0,
    )
    # If past after-hours today or it's weekend, target next weekday
    if now.time() >= _AFTERHOURS_END or now.weekday() >= 5:
        days_ahead = 1
        while True:
            candidate = target + timedelta(days=days_ahead)
            if candidate.weekday() < 5:
                target = candidate
                break
            days_ahead += 1
    return max(0.0, (target - now).total_seconds())


def current_session_name() -> str:
    """Return the name of the current trading session."""
    if is_premarket():
        return "pre-market"
    if is_market_open():
        return "regular"
    if is_afterhours():
        return "after-hours"
    return "closed"


def format_timestamp(dt: datetime) -> str:
    """Format datetime for display (Eastern time)."""
    return to_et(dt).strftime("%Y-%m-%d %H:%M:%S ET")
