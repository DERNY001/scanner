import os
import csv
import json
import time
import asyncio
import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional
import aiohttp
import websockets

DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"
DERIV_TOKEN = os.environ.get("DERIV_DEMO_TOKEN", "").strip()
SCANNER_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
SCANNER_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TRADER_BOT_TOKEN = os.environ.get("TRADER_TELEGRAM_BOT_TOKEN") or SCANNER_BOT_TOKEN
TRADER_CHAT_ID = os.environ.get("TRADER_TELEGRAM_CHAT_ID") or SCANNER_CHAT_ID

BASE_STAKE = 0.35
CYCLE_TARGET_PROFIT = 0.70
MARTINGALE_MULTIPLIER = 2.0
MAX_RECOVERY_STEPS = 3
MARKET_COOLDOWN_SECONDS = 900
LEDGER_FILE = "multi_tracker_ledger.csv"

market_last_traded: Dict[str, float] = {}

@dataclass
class PaperPortfolio:
    name: str
    balance: float
    max_cycles: int
    stop_loss_balance: float
    current_cycle: int = 1
    cycle_profit: float = 0.0
    is_halted: bool = False
    is_cycle_paused: bool = False

    def process_result(self, profit_loss: float, market_remains_stable: bool):
        if self.is_halted or self.current_cycle > self.max_cycles or self.is_cycle_paused:
            return

        self.balance += profit_loss
        self.cycle_profit += profit_loss

        if self.balance <= self.stop_loss_balance:
            self.is_halted = True
            return

        if self.name == "D_UltraCautious" and profit_loss > 0:
            if not market_remains_stable:
                self.is_cycle_paused = True
                return

        if self.cycle_profit >= CYCLE_TARGET_PROFIT:
            self.current_cycle += 1
            self.cycle_profit = 0.0
            self.is_cycle_paused = False

portfolios: Dict[str, PaperPortfolio] = {
    "A": PaperPortfolio("A_LiveExec_5Cycles", balance=10.0, max_cycles=5, stop_loss_balance=0.0),
    "B": PaperPortfolio("B_Moderate_3Cycles", balance=10.0, max_cycles=3, stop_loss_balance=0.0),
    "C": PaperPortfolio("C_Strict_StopAt5", balance=10.0, max_cycles=5, stop_loss_balance=5.0),
    "D": PaperPortfolio("D_UltraCautious", balance=10.0, max_cycles=5, stop_loss_balance=0.0)
}

def check_market_stability(symbol: str, target_direction: str, digits_1000: List[int]) -> bool:
    if len(digits_1000) < 1000:
        return False

    total_ticks = len(digits_1000)
    counts = {d: digits_1000.count(d) for d in range(10)}
    percentages = {d: (cnt / total_ticks) * 100.0 for d, cnt in counts.items()}

    req_r1, req_r2, req_r3 = 11.9, 11.7, 11.3

    if target_direction == "DIGITUNDER":
        favored_pcts = sorted([percentages[d] for d in range(5)], reverse=True)
        opposite_pcts = [percentages[d] for d in range(5, 10)]
    elif target_direction == "DIGITOVER":
        favored_pcts = sorted([percentages[d] for d in range(5, 10)], reverse=True)
        opposite_pcts = [percentages[d] for d in range(5)]
    else:
        return False

    if favored_pcts[0] < req_r1 or favored_pcts[1] < req_r2 or favored_pcts[2] < req_r3:
        return False

    if any(pct >= 10.0 for pct in opposite_pcts):
        return False

    return True

def check_pullback_entry(target_direction: str, recent_digits: List[int]) -> bool:
    if len(recent_digits) < 4:
        return False

    latest_tick = recent_digits[-1]
    
    # 1. Latest tick must be on the winning/favored side
    if target_direction == "DIGITUNDER":
        if latest_tick >= 5:
            return False
        # Count consecutive opposite ticks (>= 5) right before the latest tick
        opposite_count = 0
        for digit in reversed(recent_digits[:-1]):
            if digit >= 5:
                opposite_count += 1
            else:
                break
        return opposite_count >= 3

    elif target_direction == "DIGITOVER":
        if latest_tick <= 4:
            return False
        # Count consecutive opposite ticks (<= 4) right before the latest tick
        opposite_count = 0
        for digit in reversed(recent_digits[:-1]):
            if digit <= 4:
                opposite_count += 1
            else:
                break
        return opposite_count >= 3

    return False

def is_market_on_cooldown(symbol: str) -> bool:
    now = time.time()
    return (now - market_last_traded.get(symbol, 0.0)) < MARKET_COOLDOWN_SECONDS

async def send_telegram(token: str, chat_id: str, message: str):
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                pass
    except Exception:
        pass

class ExecutionEngine:
    def __init__(self):
        self.current_stake = BASE_STAKE
        self.recovery_step = 0
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.session_pnl = 0.0
        self.live_balance = 0.0
        self._init_csv()
        self._bg_started = False

    def ensure_background_monitors(self):
        if not self._bg_started:
            self._bg_started = True
            asyncio.create_task(self._send_startup_message())
            asyncio.create_task(self._daily_monitor_loop())

    async def _send_startup_message(self):
        msg = (
            "🚀 <b>DERIV TRADER: ACTIVE & RUNNING</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "• <b>Base Stake</b>: $0.35 (Martingale 2.0x)\n"
            "• <b>Thresholds</b>: 11.9% / 11.7% / 11.3% (All Markets)\n"
            "• <b>Opposite Cap</b>: Strict &lt; 10.0%\n"
            "• <b>Entry Pattern</b>: ≥3 Opposite Pullback Ticks + 1 Winning Reversal Tick\n"
            "• <b>Cooldown</b>: 15m per Market\n"
            "• <b>Daily Heartbeat & Report</b>: Clock-Aligned at 12:00 UTC\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "<i>Execution engine standing by.</i>"
        )
        await send_telegram(TRADER_BOT_TOKEN, TRADER_CHAT_ID, msg)

    async def _daily_monitor_loop(self):
        while True:
            now = datetime.datetime.now(datetime.timezone.utc)
            target = now.replace(hour=12, minute=0, second=0, microsecond=0)
            if target <= now:
                target += datetime.timedelta(days=1)
            sleep_seconds = (target - now).total_seconds()
            await asyncio.sleep(sleep_seconds)

            # Scanner Channel Heartbeat
            scanner_msg = "🌅 <b>New day. Successful trades.</b>\nScanner active across all 14 indices."
            asyncio.create_task(send_telegram(SCANNER_BOT_TOKEN, SCANNER_CHAT_ID, scanner_msg))

            # Trader Channel Heartbeat & Comprehensive Audit
            win_rate = (self.total_wins / self.total_trades * 100.0) if self.total_trades > 0 else 0.0
            audit_msg = (
                "🌅 <b>New day. Successful trades.</b>\n\n"
                "📊 <b>DAILY TRADING AUDIT (12:00 UTC)</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"• <b>Total Trades</b>: {self.total_trades} (W: {self.total_wins} | L: {self.total_losses})\n"
                f"• <b>Win Rate</b>: {win_rate:.1f}%\n"
                f"• <b>Session P/L</b>: <code>${self.session_pnl:+.2f}</code>\n"
                f"• <b>Deriv Balance</b>: <code>${self.live_balance:.2f}</code>\n\n"
                "💼 <b>LEDGER COMPARISON MATRIX:</b>\n"
                f"├ <b>A (Live/5-Cycles)</b>: ${portfolios['A'].balance:.2f} (Cycle {portfolios['A'].current_cycle}/5)\n"
                f"├ <b>B (Moderate/3-Cycles)</b>: ${portfolios['B'].balance:.2f} (Cycle {portfolios['B'].current_cycle}/3)\n"
                f"├ <b>C (Strict Stop $5)</b>: ${portfolios['C'].balance:.2f} (Halted: {portfolios['C'].is_halted})\n"
                f"└ <b>D (Ultra-Cautious)</b>: ${portfolios['D'].balance:.2f} (Paused: {portfolios['D'].is_cycle_paused})\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "<i>Trading bot operational.</i>"
            )
            asyncio.create_task(send_telegram(TRADER_BOT_TOKEN, TRADER_CHAT_ID, audit_msg))
            await asyncio.sleep(60)

    def _init_csv(self):
        if not os.path.exists(LEDGER_FILE):
            with open(LEDGER_FILE, mode="w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Timestamp_UTC", "Symbol", "Contract_Type", "Barrier",
                    "Stake", "Result", "Exit_Digit", "Profit_Loss",
                    "Live_Deriv_Balance", "Bal_A_5Cycle", "Bal_B_3Cycle",
                    "Bal_C_StopAt5", "Bal_D_UltraCautious"
                ])

    def log_trade(self, symbol, contract_type, barrier, stake, result, exit_digit, profit_loss, live_balance, market_stable):
        for p in portfolios.values():
            p.process_result(profit_loss, market_stable)

        with open(LEDGER_FILE, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                symbol, contract_type, barrier, stake, result, exit_digit,
                round(profit_loss, 4), round(live_balance, 2),
                round(portfolios["A"].balance, 2),
                round(portfolios["B"].balance, 2),
                round(portfolios["C"].balance, 2),
                round(portfolios["D"].balance, 2)
            ])

    async def execute_trade(self, symbol: str, target_type: str, barrier: int, quote: float, digits_1000: List[int]):
        logger.info(f"[TRADER] Alert received for {symbol} ({target_type} barrier {barrier}). Evaluating setup...")

        if not DERIV_TOKEN:
            logger.error("[TRADER GATE] Aborted: DERIV_TOKEN is not configured.")
            return None

        if is_market_on_cooldown(symbol):
            logger.warning(f"[TRADER GATE] Aborted: {symbol} is currently on a 15-minute cooldown.")
            return None

        if not check_market_stability(symbol, target_type, digits_1000):
            logger.warning(f"[TRADER GATE] Aborted: {symbol} failed market stability check (opposite digit >= 10.0% or threshold unmet).")
            return None

        logger.info(f"[TRADER] {symbol} passed initial stability. Arming pullback watcher (waiting for >=3 opposite ticks + 1 reversal)...")

        # Connect WebSocket to watch live ticks and execute order when pattern triggers
        async with websockets.connect(DERIV_WS_URL) as ws:
            await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
            auth_res = json.loads(await ws.recv())

            if "error" in auth_res:
                logger.error(f"[TRADER AUTH ERROR] Deriv authorization failed: {auth_res["error"]}")
                return None

            if not auth_res["authorize"].get("is_virtual"):
                raise SystemExit("SAFETY STOP: Real Account Token Detected!")

            self.live_balance = float(auth_res["authorize"]["balance"])
            logger.info(f"[TRADER AUTH] Authorized successfully on Demo. Balance: ${self.live_balance:.2f}")

            # Subscribe to live ticks for this symbol to await the pullback sequence
            await ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))
            sub_res = json.loads(await ws.recv())
            tick_sub_id = sub_res.get("subscription", {}).get("id")

            recent_buffer = list(digits_1000)
            pattern_confirmed = False
            watch_limit = 60  # Wait up to 60 ticks for the pullback + reversal

            for _ in range(watch_limit):
                msg = json.loads(await ws.recv())
                if "tick" not in msg:
                    continue

                new_quote = str(msg["tick"]["quote"])
                last_digit = int(new_quote[-1])
                recent_buffer.append(last_digit)
                if len(recent_buffer) > 1000:
                    recent_buffer.pop(0)

                # Re-verify market stability preserves strict < 10.0% opposite rule
                if not check_market_stability(symbol, target_type, recent_buffer):
                    logger.warning(f"[TRADER GATE] Stability broken during watcher for {symbol}. Disarming.")
                    break

                # Check if >= 3 opposite ticks followed by 1 reversal tick formed
                if check_pullback_entry(target_type, recent_buffer):
                    logger.info(f"[TRADER CONFIRMED] Pullback entry triggered on {symbol}! Executing order now.")
                    pattern_confirmed = True
                    break

            # Unsubscribe from ticks
            if tick_sub_id:
                try:
                    await ws.send(json.dumps({"forget": tick_sub_id}))
                    await ws.recv()
                except Exception:
                    pass

            if not pattern_confirmed:
                logger.info(f"[TRADER TIMEOUT] Pullback pattern did not complete within {watch_limit} ticks on {symbol}. Setup expired.")
                return None

            market_last_traded[symbol] = time.time()

            proposal_req = {
                "proposal": 1,
                "amount": round(self.current_stake, 2),
                "basis": "stake",
                "contract_type": target_type,
                "currency": "USD",
                "duration": 1,
                "duration_unit": "t",
                "symbol": symbol,
                "barrier": str(barrier)
            }
            await ws.send(json.dumps(proposal_req))
            prop_res = json.loads(await ws.recv())
            if "error" in prop_res:
                logger.error(f"[TRADER PROPOSAL ERROR] {prop_res["error"]}")
                return None

            buy_req = {"buy": prop_res["proposal"]["id"], "price": round(self.current_stake, 2)}
            await ws.send(json.dumps(buy_req))
            buy_res = json.loads(await ws.recv())
            if "error" in buy_res:
                logger.error(f"[TRADER BUY ERROR] {buy_res["error"]}")
                return None

            contract_id = buy_res["buy"]["contract_id"]
            logger.info(f"[TRADER ORDER PLACED] Contract #{contract_id} bought at ${self.current_stake:.2f} on {symbol}")
            await ws.send(json.dumps({"proposal_open_contract": 1, "contract_id": contract_id, "subscribe": 1}))

            profit_loss = 0.0
            exit_digit = ""
            status = "LOST"

            while True:
                poc_res = json.loads(await ws.recv())
                contract = poc_res.get("proposal_open_contract", {})
                if contract.get("is_expired") or contract.get("status") in ["won", "lost"]:
                    profit_loss = float(contract.get("profit", 0.0))
                    status = "WON" if profit_loss > 0 else "LOST"
                    exit_digit = str(contract.get("exit_tick_display_value", ""))[-1]
                    break

            self.total_trades += 1
            self.session_pnl += profit_loss
            if status == "WON":
                self.total_wins += 1
                self.current_stake = BASE_STAKE
                self.recovery_step = 0
            else:
                self.total_losses += 1
                self.recovery_step += 1
                if self.recovery_step > MAX_RECOVERY_STEPS:
                    self.current_stake = BASE_STAKE
                    self.recovery_step = 0
                else:
                    self.current_stake = round(self.current_stake * MARTINGALE_MULTIPLIER, 2)

            market_stable_now = check_market_stability(symbol, target_type, digits_1000)
            self.live_balance += profit_loss
            logger.info(f"[TRADER COMPLETED] Result: {status} | PnL: ${profit_loss:+.2f} | Balance: ${self.live_balance:.2f}")
            self.log_trade(symbol, target_type, barrier, self.current_stake, status, exit_digit, profit_loss, self.live_balance, market_stable_now)