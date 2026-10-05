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

# ==========================================
# 1. CORE PARAMETERS & CONFIGURATION
# ==========================================
DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"
DERIV_TOKEN = os.environ.get("DERIV_DEMO_TOKEN", "pat_5d445d2fb269576d6660b5006db40520be36fc0cc1319cede5db09ea19af39b8").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TRADER_TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TRADER_TELEGRAM_CHAT_ID", "").strip()

BASE_STAKE = 0.35
CYCLE_TARGET_PROFIT = 0.70       # Target profit per completed cycle ($0.70)
MARTINGALE_MULTIPLIER = 2.0      # Fixed strictly at 2.0x
MAX_RECOVERY_STEPS = 3           # Max recovery attempts before reset to protect capital
MARKET_COOLDOWN_SECONDS = 900    # 15 minutes cooldown per market (900s)
LEDGER_FILE = "multi_tracker_ledger.csv"

# Global cooldown tracker per symbol
market_last_traded: Dict[str, float] = {}


# ==========================================
# 2. MULTI-LEDGER SHADOW TRACKING MODELS
# ==========================================
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
        if self.is_halted or self.current_cycle > self.max_cycles:
            return

        if self.is_cycle_paused:
            return

        self.balance += profit_loss
        self.cycle_profit += profit_loss

        # Ledger C: Stop Loss Floor ($5.00)
        if self.balance <= self.stop_loss_balance:
            self.is_halted = True
            return

        # Ledger D: Cautious Market Check after ANY win
        if self.name == "D_UltraCautious" and profit_loss > 0:
            if not market_remains_stable:
                self.is_cycle_paused = True
                return

        # Check cycle completion
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


# ==========================================
# 3. STATISTICAL FILTERS & PATTERN LOGIC
# ==========================================
FAST_SYMBOLS = {
    "1HZ10V", "1HZ25V", "1HZ50V", "1HZ75V", "1HZ100V",
    "JD10", "JD25", "JD50", "JD75", "JD100"
}

def check_market_stability(symbol: str, target_direction: str, digits_1000: List[int]) -> bool:
    if len(digits_1000) < 1000:
        return False

    total_ticks = len(digits_1000)
    counts = {d: digits_1000.count(d) for d in range(10)}
    percentages = {d: (cnt / total_ticks) * 100.0 for d, cnt in counts.items()}

    # Stricter +0.1% rule
    if symbol in FAST_SYMBOLS:
        req_r1, req_r2, req_r3 = 12.1, 11.9, 11.5
    else:
        req_r1, req_r2, req_r3 = 12.0, 11.8, 11.4

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


def check_exhaustion_sequence(target_direction: str, recent_digits: List[int]) -> bool:
    if len(recent_digits) < 5:
        return False

    last_5 = recent_digits[-5:]
    d_m4, d_m3, d_m2, d_m1, d_current = last_5

    if target_direction == "DIGITUNDER":
        return all(d >= 5 for d in [d_m4, d_m3, d_m2, d_m1]) and (d_current < 5)
    elif target_direction == "DIGITOVER":
        return all(d <= 4 for d in [d_m4, d_m3, d_m2, d_m1]) and (d_current > 4)
    return False


def is_market_on_cooldown(symbol: str) -> bool:
    now = time.time()
    last_run = market_last_traded.get(symbol, 0.0)
    return (now - last_run) < MARKET_COOLDOWN_SECONDS


# ==========================================
# 4. TELEGRAM NOTIFICATIONS
# ==========================================
async def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                pass
    except Exception:
        pass


# ==========================================
# 5. EXECUTION & LOGGING ENGINE
# ==========================================
class ExecutionEngine:
    def __init__(self):
        self.current_stake = BASE_STAKE
        self.recovery_step = 0
        self._init_csv()

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
        if not DERIV_TOKEN:
            print("[TRADER WARNING]: DERIV_DEMO_TOKEN not set.")
            return None

        if is_market_on_cooldown(symbol):
            return None

        if not check_market_stability(symbol, target_type, digits_1000):
            return None

        if not check_exhaustion_sequence(target_type, digits_1000):
            return None

        # Set 15-minute market cooldown
        market_last_traded[symbol] = time.time()

        async with websockets.connect(DERIV_WS_URL) as ws:
            # 1. Authorize
            await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
            auth_res = json.loads(await ws.recv())

            if "error" in auth_res:
                print(f"[AUTH ERROR]: {auth_res["error"]["message"]}")
                return None

            if not auth_res["authorize"].get("is_virtual"):
                raise SystemExit("SAFETY STOP: Token is linked to a Real Account! Terminating.")

            live_balance = float(auth_res["authorize"]["balance"])

            # 2. Dynamic Price Proposal
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
                print(f"[PROPOSAL ERROR]: {prop_res["error"]["message"]}")
                return None

            # 3. Buy Contract
            buy_req = {"buy": prop_res["proposal"]["id"], "price": round(self.current_stake, 2)}
            await ws.send(json.dumps(buy_req))
            buy_res = json.loads(await ws.recv())

            if "error" in buy_res:
                print(f"[BUY ERROR]: {buy_res["error"]["message"]}")
                return None

            contract_id = buy_res["buy"]["contract_id"]

            # 4. Wait for Contract Settlement
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

            # 5. Recovery Calculation (2.0x Martingale strictly)
            if status == "WON":
                self.current_stake = BASE_STAKE
                self.recovery_step = 0
            else:
                self.recovery_step += 1
                if self.recovery_step > MAX_RECOVERY_STEPS:
                    self.current_stake = BASE_STAKE
                    self.recovery_step = 0
                else:
                    self.current_stake = round(self.current_stake * MARTINGALE_MULTIPLIER, 2)

            # 6. Post-Trade Verification for Ledger D
            market_stable_now = check_market_stability(symbol, target_type, digits_1000)

            # 7. Non-blocking Async Ledger Update & Telegram Notification
            updated_balance = live_balance + profit_loss
            self.log_trade(symbol, target_type, barrier, self.current_stake, status, exit_digit, profit_loss, updated_balance, market_stable_now)

            msg = (
                f"📊 *Trade Executed: {symbol}*\n"
                f"Result: *{status}* (Exit Digit: `{exit_digit}`)\n"
                f"P/L: `${profit_loss:+.2f}` | Next Stake: `${self.current_stake:.2f}`\n"
                f"Live Balance: `${updated_balance:.2f}`\n"
                f"Ledgers: A=${portfolios["A"].balance:.2f} | B=${portfolios["B"].balance:.2f} | C=${portfolios["C"].balance:.2f} | D=${portfolios["D"].balance:.2f}"
            )
            asyncio.create_task(send_telegram_alert(msg))

            return {"status": status, "pnl": profit_loss, "exit_digit": exit_digit}
