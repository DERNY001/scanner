
async def _start_health_server():
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    async def _handle_health(request):
        return web.Response(text="OK - Scanner & Bot Active")
    app.router.add_get("/", _handle_health)
    app.router.add_get("/healthz", _handle_handle := _handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

from trader import ExecutionEngine

trader_engine = ExecutionEngine()
import asyncio
import json
import logging
import os
import time
from collections import deque, Counter
from typing import Optional, Dict, List, Tuple
import aiohttp
from aiohttp import web
import websockets

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("MultiScanner")

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
WINDOW_SIZE = 1000

MARKET_NAMES = {
    # 1s Continuous Volatility (Fast)
    "1HZ10V": "Volatility 10 (1s) Index",
    "1HZ25V": "Volatility 25 (1s) Index",
    "1HZ50V": "Volatility 50 (1s) Index",
    "1HZ75V": "Volatility 75 (1s) Index",
    "1HZ100V": "Volatility 100 (1s) Index",
    
    # Standard Volatility (Standard)
    "R_10": "Volatility 10 Index",
    "R_25": "Volatility 25 Index",
    "R_50": "Volatility 50 Index",
    "R_75": "Volatility 75 Index",
    "R_100": "Volatility 100 Index",
    
    # Jump Indices (Fast)
    "JD10": "Jump 10 Index",
    "JD25": "Jump 25 Index",
    "JD50": "Jump 50 Index",
    "JD75": "Jump 75 Index",
}

SYMBOLS = list(MARKET_NAMES.keys())

def get_thresholds_for_symbol(symbol: str) -> Tuple[int, int, int]:
    return 119, 117, 113

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
        await self.send(
            f"🟢 <b>Multi-Market Scanner Updated (Render)</b>\n"
            f"Monitoring {len(SYMBOLS)} indices.\n"
            f"• Unified Threshold (All 14 Indices): 11.9% | 11.7% | 11.3%"
        )

    async def send(self, message: str):
        if self.enabled:
            await self.queue.put(message)

    async def _worker(self):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                text = await self.queue.get()
                payload = {
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "HTML"
                }
                for attempt in range(1, 4):
                    try:
                        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            if resp.status == 200:
                                break
                            else:
                                err_data = await resp.text()
                                logger.error(f"Telegram dispatch error ({resp.status}): {err_data}")
                    except Exception as e:
                        if attempt < 3:
                            logger.warning(f"Telegram connection dropped ({e}). Retrying in 2s (Attempt {attempt}/3)...")
                            await asyncio.sleep(2)
                        else:
                            logger.error(f"Telegram network failure after 3 attempts: {e}")
                self.queue.task_done()


class StatisticalDigitEngine:
    def __init__(self, symbol: str, window_size: int = WINDOW_SIZE):
        self.symbol = symbol
        self.display_name = MARKET_NAMES.get(symbol, symbol)
        self.window_size = window_size
        self.buffer: deque[int] = deque(maxlen=window_size)
        self.processed_ticks = 0
        self.currently_qualifying = False
        self.t1_limit, self.t2_limit, self.t3_limit = get_thresholds_for_symbol(symbol)

    def prime_history(self, prices: List[float], pip_size: int) -> int:
        self.buffer.clear()
        for price in prices:
            formatted = f"{float(price):.{pip_size}f}"
            self.buffer.append(int(formatted[-1]))
        return len(self.buffer)

    def push(self, digit: int) -> Optional[Dict[str, any]]:
        self.buffer.append(digit)
        self.processed_ticks += 1
        if len(self.buffer) < self.window_size:
            return None

        counts = Counter(self.buffer)
        pcts = {d: counts[d] / 10.0 for d in range(10)}

        under = self._evaluate_category("UNDER 5", [0, 1, 2, 3, 4], [5, 6, 7, 8, 9], counts, pcts)
        if under:
            self.currently_qualifying = True
            return under

        over = self._evaluate_category("OVER 4", [5, 6, 7, 8, 9], [0, 1, 2, 3, 4], counts, pcts)
        if over:
            self.currently_qualifying = True
            return over

        self.currently_qualifying = False
        return None

    def _evaluate_category(self, category_name: str, target_set: List[int], opposite_set: List[int], counts: Counter, pcts: Dict[int, float]) -> Optional[Dict[str, any]]:
        ranked = sorted(
            [(d, counts[d], pcts[d]) for d in target_set],
            key=lambda item: item[1],
            reverse=True
        )

        top1, top2, top3 = ranked[0], ranked[1], ranked[2]

        if top1[1] >= self.t1_limit and top2[1] >= self.t2_limit and top3[1] >= self.t3_limit:
            if all(counts[d] < 100 for d in opposite_set):
                return {
                    "category": category_name,
                    "symbol": self.symbol,
                    "display_name": self.display_name,
                    "top1": top1,
                    "top2": top2,
                    "top3": top3,
                    "target_reqs": (self.t1_limit / 10.0, self.t2_limit / 10.0, self.t3_limit / 10.0),
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


async def monitor_heartbeat(engines: Dict[str, StatisticalDigitEngine]) -> None:
    while True:
        await asyncio.sleep(15)
        total_ticks = sum(e.processed_ticks for e in engines.values())
        active_qualifiers = [e.display_name for e in engines.values() if e.currently_qualifying]
        status_text = f"Active Setups: {len(active_qualifiers)}" if active_qualifiers else "No active skews"
        logger.info(f"📊 Live ticks ingested: {total_ticks} | {status_text}")


async def start_web_server(engines: Dict[str, StatisticalDigitEngine]):
    async def handle_ping(request):
        total_ticks = sum(e.processed_ticks for e in engines.values())
        return web.Response(text=f"OK - Deriv Scanner Running. Ingested ticks: {total_ticks}")

    app = web.Application()
    app.router.add_get("/", handle_ping)
    app.router.add_get("/health", handle_ping)

    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"🌐 Keep-alive web server listening on port {port}")


async def scan() -> None:
    headers = {
        "Origin": "https://app.deriv.com",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
    }

    env = load_env()
    token = os.environ.get("TELEGRAM_BOT_TOKEN") or env.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID") or env.get("TELEGRAM_CHAT_ID")

    dispatcher = TelegramDispatcher(token=token, chat_id=chat_id)
    await dispatcher.start()
    await _start_health_server()
    trader_engine.ensure_background_monitors()

    engines: Dict[str, StatisticalDigitEngine] = {s: StatisticalDigitEngine(s) for s in SYMBOLS}
    primed_symbols = set()
    subscribed_live = False
    last_alert_time: Dict[str, float] = {}

    await start_web_server(engines)

    while True:
        try:
            logger.info("Connecting to Deriv Gateway...")
            async with websockets.connect(WS_URL, additional_headers=headers, open_timeout=15, ping_interval=None) as ws:
                logger.info(f"Connected! Loading 1,000 baseline ticks for {len(SYMBOLS)} markets...")
                
                for idx, sym in enumerate(SYMBOLS):
                    await ws.send(json.dumps({
                        "ticks_history": sym,
                        "count": WINDOW_SIZE,
                        "end": "latest",
                        "style": "ticks",
                        "req_id": idx + 100
                    }))
                    await asyncio.sleep(0.04)

                hb_task = asyncio.create_task(heartbeat(ws))
                stats_task = asyncio.create_task(monitor_heartbeat(engines))

                try:
                    async for message in ws:
                        data = json.loads(message)

                        if "history" in data:
                            req_id = data.get("req_id")
                            if req_id is not None and 0 <= (req_id - 100) < len(SYMBOLS):
                                sym = SYMBOLS[req_id - 100]
                                pip_size = data.get("pip_size", 2)
                                prices = data["history"].get("prices", [])
                                engines[sym].prime_history(prices, pip_size)
                                primed_symbols.add(sym)
                                display = MARKET_NAMES.get(sym, sym)
                                logger.info(f"[{len(primed_symbols)}/{len(SYMBOLS)}] Baseline loaded for {display}")

                                if len(primed_symbols) == len(SYMBOLS) and not subscribed_live:
                                    subscribed_live = True
                                    logger.info("🟢 ALL 14 BASELINES LOADED. Subscribing to live continuous tick streams...")
                                    for s in SYMBOLS:
                                        await ws.send(json.dumps({"ticks": s, "subscribe": 1}))
                            continue

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
                                    qualifying_count = sum(1 for e in engines.values() if e.currently_qualifying)
                                    cooldown_limit = 300 if qualifying_count > 1 else 120

                                    now = time.time()
                                    last_sent = last_alert_time.get(sym, 0)

                                    if now - last_sent >= cooldown_limit:
                                        last_alert_time[sym] = now
                                        pacing_label = f"5m Shared Cooldown ({qualifying_count} Active Markets)" if qualifying_count > 1 else "2m Single-Market Pacing"
                                        req1, req2, req3 = alert["target_reqs"]
                                        
                                        alert_msg = (
                                            f"🚨 <b>DERIV ALERT: {alert['category']}</b>\n"
                                            f"<b>Market</b>: {alert['display_name']}\n"
                                            f"<b>Quote</b>: {formatted_quote} (Digit: {last_digit})\n"
                                            f"<b>Top 3</b>: #{alert['top1'][0]} ({alert['top1'][2]:.1f}%), "
                                            f"#{alert['top2'][0]} ({alert['top2'][2]:.1f}%), "
                                            f"#{alert['top3'][0]} ({alert['top3'][2]:.1f}%)\n"
                                            f"<b>Min Req</b>: {req1}% | {req2}% | {req3}%\n"
                                            f"<b>Opposite Max</b>: {alert['opposite_max']:.1f}%\n"
                                            f"<b>Pacing</b>: {pacing_label}\n"
                                            f"<b>Window</b>: 1,000 ticks"
                                        )
                                        logger.warning(f"ALERT DISPATCHED: {alert['display_name']} -> {alert['category']} ({pacing_label})")
                                        await dispatcher.send(alert_msg)

                                        # --- AUTOMATED TRADER HOOK ---
                                        target_dir = "DIGITUNDER" if alert["category"] == "UNDER 5" else "DIGITOVER"
                                        barrier_val = 5 if alert["category"] == "UNDER 5" else 4
                                        asyncio.create_task(
                                            trader_engine.execute_trade(
                                                symbol=sym,
                                                target_type=target_dir,
                                                barrier=barrier_val,
                                                quote=float(tick["quote"]),
                                                digits_1000=list(engine.buffer)
                                            )
                                        )

                        elif "error" in data:
                            logger.error(f"Deriv API error: {data['error']}")

                finally:
                    hb_task.cancel()
                    stats_task.cancel()

        except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
            logger.warning(f"Connection lost ({e}). Reconnecting in 5 seconds...")
            primed_symbols.clear()
            subscribed_live = False
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(scan())
    except KeyboardInterrupt:
        logger.info("Scanner stopped by user.")
