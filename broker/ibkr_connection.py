"""Singleton IB() wrapper with auto-reconnect."""

import asyncio
import logging
from typing import Optional

from ib_insync import IB, Contract, util

from config.settings import (
    IBKR_HOST,
    IBKR_PORT,
    IBKR_CLIENT_ID,
    IBKR_TIMEOUT,
    IBKR_RECONNECT_DELAY,
    IBKR_MAX_RECONNECT_ATTEMPTS,
    IBKR_MSG_RATE_LIMIT,
    IBKR_MAX_CONCURRENT_HIST,
)

logger = logging.getLogger("trading_bot.ibkr")

# Rate limiting primitives
_msg_semaphore: Optional[asyncio.Semaphore] = None
_hist_semaphore: Optional[asyncio.Semaphore] = None


def _get_msg_semaphore() -> asyncio.Semaphore:
    global _msg_semaphore
    if _msg_semaphore is None:
        _msg_semaphore = asyncio.Semaphore(IBKR_MSG_RATE_LIMIT)
    return _msg_semaphore


def _get_hist_semaphore() -> asyncio.Semaphore:
    global _hist_semaphore
    if _hist_semaphore is None:
        _hist_semaphore = asyncio.Semaphore(IBKR_MAX_CONCURRENT_HIST)
    return _hist_semaphore


class IBKRConnection:
    """Singleton wrapper around ib_insync.IB with auto-reconnect."""

    _instance: Optional["IBKRConnection"] = None
    _ib: Optional[IB] = None

    def __new__(cls) -> "IBKRConnection":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self._ib is None:
            self._ib = IB()
            self._ib.errorEvent += self._on_error
            self._ib.disconnectedEvent += self._on_disconnect
            self._reconnect_attempts = 0
            self._reconnecting = False

    @property
    def ib(self) -> IB:
        return self._ib

    @property
    def connected(self) -> bool:
        return self._ib.isConnected()

    async def connect(self) -> bool:
        """Connect to IBKR TWS/Gateway."""
        if self.connected:
            logger.info("Already connected to IBKR")
            return True
        try:
            await self._ib.connectAsync(
                host=IBKR_HOST,
                port=IBKR_PORT,
                clientId=IBKR_CLIENT_ID,
                timeout=IBKR_TIMEOUT,
                readonly=False,
            )
            self._reconnect_attempts = 0
            account = self._ib.managedAccounts()
            logger.info(f"Connected to IBKR — accounts: {account}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IBKR: {e}")
            return False

    async def disconnect(self) -> None:
        """Gracefully disconnect."""
        if self.connected:
            self._ib.disconnect()
            logger.info("Disconnected from IBKR")

    async def _reconnect(self) -> None:
        """Auto-reconnect with exponential backoff."""
        if self._reconnecting:
            return
        self._reconnecting = True
        try:
            while self._reconnect_attempts < IBKR_MAX_RECONNECT_ATTEMPTS:
                self._reconnect_attempts += 1
                delay = IBKR_RECONNECT_DELAY * self._reconnect_attempts
                logger.info(
                    f"Reconnect attempt {self._reconnect_attempts}/"
                    f"{IBKR_MAX_RECONNECT_ATTEMPTS} in {delay}s..."
                )
                await asyncio.sleep(delay)
                if await self.connect():
                    return
            logger.critical("Max reconnect attempts reached — giving up")
        finally:
            self._reconnecting = False

    def _on_error(self, reqId: int, errorCode: int, errorString: str, contract) -> None:
        """Handle IBKR error events."""
        # Ignore non-critical info messages
        if errorCode in (2104, 2106, 2158, 2119):  # market data farm messages
            logger.debug(f"IBKR info [{errorCode}]: {errorString}")
            return
        logger.warning(f"IBKR error [{errorCode}] reqId={reqId}: {errorString}")

    def _on_disconnect(self) -> None:
        """Handle unexpected disconnections."""
        logger.warning("Disconnected from IBKR — attempting reconnect")
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(self._reconnect())

    async def qualify_contract(self, contract: Contract) -> Optional[Contract]:
        """Resolve contract details with IBKR."""
        async with _get_msg_semaphore():
            try:
                contracts = await self._ib.qualifyContractsAsync(contract)
                return contracts[0] if contracts else None
            except Exception as e:
                logger.error(f"Failed to qualify {contract}: {e}")
                return None

    async def get_account_summary(self) -> dict:
        """Get key account values."""
        async with _get_msg_semaphore():
            values = self._ib.accountSummary()
        summary = {}
        for v in values:
            if v.tag in (
                "NetLiquidation", "TotalCashValue", "BuyingPower",
                "GrossPositionValue", "UnrealizedPnL", "RealizedPnL",
            ):
                summary[v.tag] = float(v.value)
        return summary

    async def get_historical_data(
        self,
        contract: Contract,
        duration: str = "30 D",
        bar_size: str = "1 day",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> list:
        """Fetch historical bars with rate limiting."""
        async with _get_hist_semaphore():
            async with _get_msg_semaphore():
                try:
                    bars = await self._ib.reqHistoricalDataAsync(
                        contract,
                        endDateTime="",
                        durationStr=duration,
                        barSizeSetting=bar_size,
                        whatToShow=what_to_show,
                        useRTH=use_rth,
                        formatDate=1,
                    )
                    return bars if bars else []
                except Exception as e:
                    logger.error(f"Historical data error for {contract.symbol}: {e}")
                    return []


# Module-level convenience
def get_connection() -> IBKRConnection:
    """Get the singleton IBKR connection instance."""
    return IBKRConnection()
