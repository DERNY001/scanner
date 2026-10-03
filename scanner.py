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
logger = logging.getLogger("MultiScanner")

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
WINDOW_SIZE = 1000

# Complete target universe (All 1s, Standard Volatilities, and Jumps except Jump 100)
SYMBOLS = [
    "1HZ10V", "1HZ25V", "1HZ50V", "1HZ75V", "1HZ100V",
    "1HZ150V", "1HZ200V", "1HZ250V", "1HZ300V",
    "R_10", "R_25", "R_50", "R_75", "R_100",
    "JD10", "JD25", "JD50", "JD75"
]

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
            logger.warning("Telegram alerts disabled (missing credentials).")
            return
        asyncio.create_task(self._worker())
        await self.send(f"🟢 <b>Multi-Market Scanner Online</b>\nMonitoring {len(SYMBOLS)} Deriv indices (1,000-tick baseline).")

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
                            err_data = await resp.text()
                            logger.error(f"Telegram dispatch error ({resp.status}): {err_data}")
                except Exception as e:
                    logger.error(f"Telegram network error: {e}")
                finally:
                    self.queue.task_done()


class StatisticalDigitEngine:
    def __init__(self, symbol: str, window_size: int = WINDOW_SIZE):
        self.symbol = symbol
        self.window_size = window_size
        self.buffer: deque[int] = deque(maxlen=window_size)

    def prime_history(self, prices: List[float], pip_size: int) -> int:
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

        # 1. UNDER 5
        under = self._evaluate_category("UNDER 5", [0, 1, 2, 3, 4], [5, 6, 7, 8, 9], counts, pcts)
        if under:
            return under

        # 2. OVER 4
        over = self._evaluate_category("OVER 4", [5, 6, 7, 8, 9], [0, 1, 2, 3, 4], counts, pcts)
        if over:
            return over

        return None

    def _evaluate_category(self, category_name: str, target_set: List[int], opposite_set: List[int], counts: Counter, pcts: Dict[int, float]) -> Optional[Dict[str, any]]:
        ranked = sorted(
            [(d, counts[d], pcts[d]) for d in target_set],
            key=lambda item: item[1],
            reverse=True
        )

        top1, top2, top3 = ranked[0], ranked[1], ranked[2]

        if top1[1] >= 119 and top2[1] >= 117 and top3[1] >= 113:
            if all(counts[d] < 100 for d in opposite_set):
                return {
                    "category": category_name,
                    "symbol": self.symbol,
                    "top1": top1,
                    "top2": top2,
                    "top3": top3,
                    "opposite_max": max(pcts[d] for d in opposite_set)
                }
        return None


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

    engines: Dict[str, StatisticalDigitEngine] = {s: StatisticalDigitEngine(s) for s in SYMBOLS}
    primed_symbols = set()

    while True:
        try:
            logger.info("Connecting to Deriv Gateway...")
            async with websockets.connect(WS_URL, additional_headers=headers, open_timeout=15, ping_interval=None) as ws:
                logger.info(f"Connected! Priming 1,000 ticks for {len(SYMBOLS)} symbols...")
                
                # Request history for all symbols
                for sym in SYMBOLS:
                    await ws.send(json.dumps({
                        "ticks_history": sym,
                        "count": WINDOW_SIZE,
                        "end": "latest",
                        "style": "ticks",
                        "req_id": SYMBOLS.index(sym) + 100
                    }))
                    await asyncio.sleep(0.05)

                hb_task = asyncio.create_task(heartbeat(ws))

                try:
                    async for message in ws:
                        data = json.loads(message)

                        # Priming stage
                        if "history" in data:
                            req_id = data.get("req_id")
                            if req_id is not None and (req_id - 100) < len(SYMBOLS):
                                sym = SYMBOLS[req_id - 100]
                                pip_size = data.get("pip_size", 2)
                                prices = data["history"].get("prices", [])
                                engines[sym].prime_history(prices, pip_size)
                                primed_symbols.add(sym)
                                logger.info(f"[{len(primed_symbols)}/{len(SYMBOLS)}] Primed baseline for {sym}")

                                # Subscribe once all are primed
                                if len(primed_symbols) == len(SYMBOLS):
                                    logger.info("All baselines loaded! Subscribing to live tick streams...")
                                    for s in SYMBOLS:
                                        await ws.send(json.dumps({"ticks": s}))
                            continue

                        # Live tick stream
                        if "tick" in data:
                            tick = data["tick"]
                            sym = tick["symbol"]
                            pip_size = tick.get("pip_size", 2)
                            formatted_quote = f"{float(tick['quote']):.{pip_size}f}"
                            last_digit = int(formatted_quote[-1])

                            engine = engines.get(sym)
                            if engine:
                                alert = engine.push(last_digit)
                                if alert:
                                    alert_msg = (
                                        f"🚨 <b>DERIV ALERT: {alert['category']}</b>\n"
                                        f"<b>Market</b>: {sym}\n"
                                        f"<b>Quote</b>: {formatted_quote} (Digit: {last_digit})\n"
                                        f"<b>Top 3</b>: #{alert['top1'][0]} ({alert['top1'][2]:.1f}%), "
                                        f"#{alert['top2'][0]} ({alert['top2'][2]:.1f}%), "
                                        f"#{alert['top3'][0]} ({alert['top3'][2]:.1f}%)\n"
                                        f"<b>Opposite Max</b>: {alert['opposite_max']:.1f}%\n"
                                        f"<b>Window</b>: 1,000 ticks"
                                    )
                                    logger.warning(f"ALERT: {sym} -> {alert['category']}")
                                    await dispatcher.send(alert_msg)

                        elif "error" in data:
                            logger.error(f"Deriv API error: {data['error']}")

                finally:
                    hb_task.cancel()

        except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
            logger.warning(f"Connection lost ({e}). Reconnecting in 5 seconds...")
            primed_symbols.clear()
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(scan())
    except KeyboardInterrupt:
        logger.info("Scanner stopped by user.")
