import asyncio
import json
import logging
import urllib.request
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Mobile Algo Hub")

state = {
    "mode": "PAPER",
    "symbol": "^NSEI",
    "display_symbol": "NIFTY 50",
    "paper_balance": 100000.0,
    "last_price": 23810.0,
    "positions": [],
    "logs": ["Engine ready. Multi-asset selector active."],
    "strategy": {
        "active": False,
        "buy_above": 0.0,
        "sell_below": 0.0,
        "qty": 1,
        "target_pts": 50.0,
        "trailing_sl_pts": 25.0
    }
}

active_connections: List[WebSocket] = []

async def broadcast_state():
    if active_connections:
        payload = json.dumps(state)
        for connection in list(active_connections):
            try:
                await connection.send_text(payload)
            except Exception:
                pass

def log_event(message: str):
    state["logs"].insert(0, message)
    if len(state["logs"]) > 25:
        state["logs"].pop()

def fetch_price(symbol: str) -> float:
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1m&range=1d"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read().decode())
            result = data.get("chart", {}).get("result")
            if result and len(result) > 0:
                meta = result[0].get("meta", {})
                price = meta.get("regularMarketPrice")
                if price:
                    return round(float(price), 2)
    except Exception as e:
        logging.warning(f"Price fetch failed for {symbol}: {e}")
    return 0.0

def execute_paper_order(side: str, qty: int, price: float):
    sym = state["display_symbol"]
    if side == "BUY":
        cost = price * qty
        state["paper_balance"] -= cost
        state["positions"].append({
            "symbol": sym, "side": "BUY", "qty": qty, 
            "entry_price": price, "ltp": price, "pnl": 0.0,
            "peak_price": price
        })
        log_event(f"[AUTO-BUY] {qty}x {sym} @ ₹{price}")
    elif side == "SELL" and state["positions"]:
        p = state["positions"].pop(0)
        pnl = round((price - p["entry_price"]) * p["qty"], 2)
        state["paper_balance"] += (p["entry_price"] * p["qty"]) + pnl
        log_event(f"[AUTO-SELL] {p['qty']}x {sym} @ ₹{price} | P&L: ₹{pnl}")

def evaluate_strategy(price: float):
    strat = state["strategy"]
    if not strat["active"] or price <= 0:
        return

    for pos in list(state["positions"]):
        if pos["side"] == "BUY":
            if price > pos.get("peak_price", pos["entry_price"]):
                pos["peak_price"] = price

            entry = pos["entry_price"]
            peak = pos["peak_price"]
            target = entry + strat["target_pts"]
            trailing_stop = peak - strat["trailing_sl_pts"]

            if price >= target:
                execute_paper_order("SELL", pos["qty"], price)
                log_event(f"🎯 Target hit at ₹{price} (+{strat['target_pts']} pts)!")
                strat["active"] = False
                return

            if price <= trailing_stop:
                execute_paper_order("SELL", pos["qty"], price)
                log_event(f"🛑 Trailing SL hit at ₹{price} (Peak: ₹{peak})")
                strat["active"] = False
                return

    if len(state["positions"]) == 0:
        if strat["buy_above"] >
