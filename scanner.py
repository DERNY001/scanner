import asyncio
import json
import logging
import os
from collections import deque, Counter
from typing import Optional, Dict, List
import aiohttp
import websockets

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("DerivScanner")

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
SYMBOL = "R_100"
WINDOW_SIZE = 1000
PIP_SIZE = 2


def load_env() -> Dict[str, str]:
    env_vars = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    env_vars[key.strip()] = val.strip()
    return env_vars


class TelegramDispatcher:
    def __init__(self, token: Optional[str], chat_id: Optional[str]):
        self.token = token
        self.chat_id = chat_id
        self.queue: asyncio.Queue = asyncio.Queue()
        self.enabled = bool(token and chat_id)

    async def start(self):
        if not self.enabled:
            logger.warning("Telegram alerts disabled (credentials missing).")
            return
        asyncio.create_task(self._worker())
        # Dispatch a startup test ping to verify Telegram connectivity
        await self.send("🟢 <b>Deriv R_100 Scanner Online</b>\nMonitoring 1,000-tick statistical conditions...")

    async def send(self, message: str):
        if self.enabled:
            await self.queue.put(message)

    async def _worker(self):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        async with aiohttp.ClientSession() as session:
            while True:
                text = await self.queue.get()
                payload = {
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "HTML"
                }
                try:
                    async with session.post(url, json=payload, timeout=5) as resp:
                        if resp.status != 200:
                            # Fallback without the -100 prefix if rejected
                            payload["chat_id"] = "-5066183237"
                            async with session.post(url, json=payload, timeout=5) as retry_resp:
                                if retry_resp.status != 200:
                                    err_text = await retry_resp.text()
                                    logger.error(f"Telegram dispatch error ({retry_resp.status}): {err_text}")
                except Exception as e:
                    logger.error(f"Telegram network error: {e}")
                finally:
                    self.queue.task_done()


class StatisticalDigitEngine:
    def __init__(self, window_size: int = WINDOW_SIZE):
        self.window_size = window_size
        self.buffer: deque[int] = deque(maxlen=window_size)

    def prime_history(self, prices: List[float], pip_size: int = PIP_SIZE) -> int:
        self.buffer.clear()
        for price in prices:
            formatted = f"{float(price):.{pip_size}f}"
            self.buffer.append(int(formatted[-1]))
        return len(self.buffer)

    def push(self, digit: int) -> Optional[Dict[str, any]]:
        self.buffer.append(digit)
        if len(self.buffer) < self.window_size:
            return None

        counts = Counter(self.buffer)
        pcts = {d: counts[d] / 10.0 for d in range(10)}

        under_alert = self._evaluate_category(
            category_name="UNDER 5",
            target_set=[0, 1, 2, 3, 4],
            opposite_set=[5, 6, 7, 8, 9],
            counts=counts,
            pcts=pcts
        )
        if under_alert:
            return under_alert

        over_alert = self._evaluate_category(
            category_name="OVER 4",
            target_set=[5, 6, 7, 8, 9],
            opposite_set=[0, 1, 2, 3, 4],
            counts=counts,
            pcts=pcts
        )
        if over_alert:
            return over_alert

        return None

    def _evaluate_category(
        self,
        category_name: str,
        target_set: List[int],
        opposite_set: List[int],
        counts: Counter,
        pcts: Dict[int, float]
    ) -> Optional[Dict[str, any]]:
        ranked = sorted(
            [(d, counts[d], pcts[d]) for d in target_set],
            key=lambda item: item[1],
            reverse=True
        )

        top1, top2, top3 = ranked[0], ranked[1], ranked[2]

        c1 = top1[1] >= 119
        c2 = top2[1] >= 117
        c3 = top3[1] >= 113
        c_opposites = all(counts[d] < 100 for d in opposite_set)

        if c1 and c2 and c3 and c_opposites:
            return {
                "category": category_name,
                "top1": top1,
                "top2": top2,
                "top3": top3,
                "all_ranks": ranked,
                "opposite_max": max(pcts[d] for d in opposite_set),
                "pcts": pcts
            }
        return None

    def get_status_overview(self) -> str:
        if not self.buffer:
            return "Buffer empty"
        counts = Counter(self.buffer)
        row_under = " ".join([f"{d}:{counts[d]/10.0:.1f}%" for d in range(5)])
        row_over = " ".join([f"{d}:{counts[d]/10.0:.1f}%" for d in range(5, 10)])
        return f"[0-4] {row_under} | [5-9] {row_over}"


async def heartbeat(ws: websockets.WebSocketClientProtocol) -> None:
    try:
        while True:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"ping": 1}))
    except (asyncio.CancelledError, websockets.ConnectionClosed):
        pass


async def scan() -> None:
    headers = {
        "Origin": "https://app.deriv.com",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
    }

    env = load_env()
    dispatcher = TelegramDispatcher(
        token=env.get("TELEGRAM_BOT_TOKEN"),
        chat_id=env.get("TELEGRAM_CHAT_ID")
    )
    await dispatcher.start()

    engine = StatisticalDigitEngine(window_size=WINDOW_SIZE)

    while True:
        try:
            logger.info("Connecting to Deriv WebSocket gateway...")
            async with websockets.connect(
                WS_URL,
                additional_headers=headers,
                open_timeout=10,
                ping_interval=None
            ) as ws:
                logger.info("Connected! Priming 1,000 historical ticks...")
                await ws.send(json.dumps({
                    "ticks_history": SYMBOL,
                    "count": WINDOW_SIZE,
                    "end": "latest",
                    "style": "ticks"
                }))

                primed = False
                hb_task = asyncio.create_task(heartbeat(ws))

                try:
                    async for message in ws:
                        data = json.loads(message)

                        if "history" in data and not primed:
                            prices = data["history"].get("prices", [])
                            count = engine.prime_history(prices, pip_size=PIP_SIZE)
                            logger.info(f"Loaded {count} historical ticks! Buffer primed.")
                            await ws.send(json.dumps({"ticks": SYMBOL}))
                            primed = True
                            continue

                        if "tick" in data:
                            tick = data["tick"]
                            pip_size = tick.get("pip_size", PIP_SIZE)
                            formatted_quote = f"{float(tick['quote']):.{pip_size}f}"
                            last_digit = int(formatted_quote[-1])

                            alert = engine.push(last_digit)

                            logger.info(
                                f"{tick['symbol']} | "
                                f"Quote: {formatted_quote:>8} | "
                                f"Digit: {last_digit} | "
                                f"{engine.get_status_overview()}"
                            )

                            if alert:
                                alert_msg = (
                                    f"🚨 <b>DERIV R_100 ALERT: {alert['category']}</b>\n"
                                    f"<b>Quote</b>: {formatted_quote} (Digit: {last_digit})\n"
                                    f"<b>Top 3</b>: #{alert['top1'][0]} ({alert['top1'][2]:.1f}%), "
                                    f"#{alert['top2'][0]} ({alert['top2'][2]:.1f}%), "
                                    f"#{alert['top3'][0]} ({alert['top3'][2]:.1f}%)\n"
                                    f"<b>Opposite Max</b>: {alert['opposite_max']:.1f}%\n"
                                    f"<b>Window</b>: 1,000 ticks"
                                )
                                logger.warning(f">>> {alert['category']} CONDITION TRIGGERED")
                                await dispatcher.send(alert_msg)

                        elif "error" in data:
                            logger.error(f"Deriv error returned: {data['error']}")
                            break

                finally:
                    hb_task.cancel()

        except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
            logger.warning(f"Connection lost ({e}). Reconnecting in 3 seconds...")
            await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(scan())
    except KeyboardInterrupt:
        logger.info("Scanner stopped by user.")
