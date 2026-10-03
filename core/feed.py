import asyncio
import json
import logging
from typing import Callable, Coroutine, Any, List
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DerivFeed")


def extract_last_digit(quote: Any) -> int:
    """Extracts the exact last digit of a price quote avoiding float precision issues."""
    quote_str = str(quote).strip()
    digits_only = [c for c in quote_str if c.isdigit()]
    if not digits_only:
        raise ValueError(f"Cannot extract digit from quote: {quote}")
    return int(digits_only[-1])


class DerivFeed:
    """Maintains an async WebSocket stream to Deriv with cluster failover."""

    CLUSTERS = ["red", "blue", "green", "ws"]

    def __init__(self, symbol: str, app_id: int = 1089):
        self.symbol = symbol
        self.app_id = app_id
        self._cluster_idx = 0
        self._running = False

    @property
    def current_endpoint(self) -> str:
        cluster = self.CLUSTERS[self._cluster_idx]
        return f"wss://{cluster}.derivws.com/websockets/v3?app_id={self.app_id}"

    def _rotate_cluster(self) -> None:
        self._cluster_idx = (self._cluster_idx + 1) % len(self.CLUSTERS)
        logger.info(f"Switching gateway to: {self.current_endpoint}")

    async def subscribe_ticks(
        self,
        on_tick: Callable[[int, dict], Coroutine[Any, Any, None]]
    ) -> None:
        """Connects, subscribes to tick stream, and runs the listener loop."""
        self._running = True
        subscription_payload = {
            "ticks": self.symbol,
            "subscribe": 1
        }

        while self._running:
            endpoint = self.current_endpoint
            try:
                logger.info(f"Connecting to {endpoint} for {self.symbol}...")
                async with websockets.connect(
                    endpoint,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=10,
                    origin="https://smarttrader.deriv.com",
                    user_agent_header="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
                ) as ws:
                    await ws.send(json.dumps(subscription_payload))
                    logger.info(f"Subscribed to tick stream for {self.symbol}")

                    async for message in ws:
                        if not self._running:
                            break

                        data = json.loads(message)

                        if "error" in data:
                            logger.error(f"Deriv API error: {data['error'].get('message')}")
                            continue

                        if data.get("msg_type") == "tick" and "tick" in data:
                            tick_info = data["tick"]
                            raw_quote = tick_info.get("quote")
                            digit = extract_last_digit(raw_quote)

                            await on_tick(digit, tick_info)

            except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK) as e:
                logger.warning(f"Connection lost ({e}). Reconnecting...")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Gateway failed ({e}). Rotating cluster...")
                self._rotate_cluster()
                await asyncio.sleep(2)

    def stop(self) -> None:
        """Stops the streaming loop cleanly."""
        self._running = False
