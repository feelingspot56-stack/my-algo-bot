import asyncio
import json
import logging
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import yfinance as yf

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Mobile Algo Hub")

# State management
state = {
    "mode": "PAPER",
    "symbol": "^NSEI",          # Yahoo ticker for NIFTY 50
    "display_symbol": "NIFTY 50",
    "paper_balance": 100000.0,
    "last_price": 0.0,
    "positions": [],
    "logs": []
}

active_connections: List[WebSocket] = []

async def broadcast_state():
    if active_connections:
        payload = json.dumps(state)
        for connection in active_connections:
            try:
                await connection.send_text(payload)
            except Exception:
                pass

def log_event(message: str):
    state["logs"].insert(0, message)
    if len(state["logs"]) > 25:
        state["logs"].pop()

# Background live price fetcher
async def live_feed_worker():
    while True:
        try:
            ticker = yf.Ticker(state["symbol"])
            # Fast price check from fast_info
            price = ticker.fast_info.get("last_price")
            if not price:
                data = ticker.history(period="1d", interval="1m")
                if not data.empty:
                    price = float(data["Close"].iloc[-1])

            if price and price > 0:
                state["last_price"] = round(float(price), 2)
                # Recalculate paper positions PnL
                for pos in state["positions"]:
                    pos["ltp"] = state["last_price"]
                    pos["pnl"] = round((state["last_price"] - pos["entry_price"]) * pos["qty"], 2)

                await broadcast_state()
        except Exception as e:
            logging.error(f"Data fetch error: {e}")

        # Polls live price every 3 seconds
        await asyncio.sleep(3)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(live_feed_worker())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    await websocket.send_text(json.dumps(state))
    try:
        while True:
            data = await websocket.receive_text()
            cmd = json.loads(data)
            action = cmd.get("action")
            
            if action == "TOGGLE_MODE":
                state["mode"] = "LIVE" if state["mode"] == "PAPER" else "PAPER"
                log_event(f"Switched mode to {state['mode']}")
            
            elif action == "CHANGE_SYMBOL":
                sym = cmd.get("symbol", "^NSEI")
                disp = cmd.get("display", "NIFTY 50")
                state["symbol"] = sym
                state["display_symbol"] = disp
                log_event(f"Changed ticker to {disp}")

            elif action == "ORDER":
                side = cmd.get("side")
                qty = int(cmd.get("qty", 1))
                sym = state["display_symbol"]
                price = state["last_price"]

                if price <= 0:
                    log_event("Cannot place order: Price feed not ready.")
                else:
                    if state["mode"] == "PAPER":
                        cost = price * qty
                        if side == "BUY":
                            state["paper_balance"] -= cost
                            state["positions"].append({
                                "symbol": sym, "side": "BUY", "qty": qty, 
                                "entry_price": price, "ltp": price, "pnl": 0.0
                            })
                            log_event(f"[PAPER] Bought {qty} {sym} @ ₹{price}")
                        elif side == "SELL" and state["positions"]:
                            p = state["positions"].pop(0)
                            pnl = (price - p["entry_price"]) * p["qty"]
                            state["paper_balance"] += (p["entry_price"] * p["qty"]) + pnl
                            log_event(f"[PAPER] Sold {qty} {sym} @ ₹{price} | P&L: ₹{pnl:.2f}")

            await broadcast_state()
    except WebSocketDisconnect:
        active_connections.remove(websocket)

@app.get("/", response_class=HTMLResponse)
def index():
    return HTML_CONTENT

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Algo Terminal</title>
  <style>
    :root { --bg: #090d16; --card: #131b2e; --border: #1f2b45; --up: #10b981; --down: #ef4444; --text: #f1f5f9; }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, system-ui, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 14px; }
    .header { display: flex; justify-content: space-between; align-items: center; padding-bottom: 12px; border-bottom: 1px solid var(--border); }
    .badge { padding: 4px 10px; border-radius: 999px; font-size: 11px; font-weight: 700; }
    .badge-paper { background: #3b82f633; color: #60a5fa; }
    .badge-live { background: #ef444433; color: #f87171; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; margin-top: 12px; }
    .price-lg { font-size: 32px; font-weight: 800; font-variant-numeric: tabular-nums; margin-top: 4px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 10px; }
    button { padding: 12px; border-radius: 8px; font-weight: bold; border: none; cursor: pointer; color: white; width: 100%; font-size: 14px; }
    .btn-buy { background: var(--up); }
    .btn-sell { background: var(--down); }
    .btn-mode { background: #1e293b; border: 1px solid var(--border); margin-top: 10px; }
    .log-box { font-size: 12px; max-height: 140px; overflow-y: auto; background: #00000040; border-radius: 6px; padding: 8px; margin-top: 10px; line-height: 1.6; }
    select { width: 100%; padding: 8px; background: #0b0f19; color: #fff; border: 1px solid var(--border); border-radius: 6px; margin-top: 6px; }
  </style>
</head>
<body>
  <div class="header">
    <h3>Algo Terminal</h3>
    <span id="modeBadge" class="badge badge-paper">PAPER</span>
  </div>

  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <span style="font-size: 12px; color: #94a3b8;" id="symLabel">NIFTY 50 (Live)</span>
      <select id="tickerSelect" onchange="changeTicker(this)">
        <option value="^NSEI|NIFTY 50">NIFTY 50</option>
        <option value="^NSEBANK|BANKNIFTY">BANKNIFTY</option>
        <option value="RELIANCE.NS|RELIANCE">RELIANCE</option>
        <option value="TATAMOTORS.NS|TATAMOTORS">TATAMOTORS</option>
        <option value="TCS.NS|TCS">TCS</option>
      </select>
    </div>
    <div class="price-lg" id="ltp">Connecting...</div>
    <div style="font-size: 13px; margin-top: 6px;">Virtual Margin: <b id="bal">₹1,00,000</b></div>
  </div>

  <div class="card">
    <div style="font-size: 13px; font-weight: 600;">Manual / Instant Signal</div>
    <div class="grid">
      <button class="btn-buy" onclick="order('BUY')">BUY (MIS)</button>
      <button class="btn-sell" onclick="order('SELL')">SELL (MIS)</button>
    </div>
    <button class="btn-mode" onclick="toggle()">Switch Paper / Live</button>
  </div>

  <div class="card">
    <div style="font-size: 13px; font-weight: 600;">Open Positions (<span id="count">0</span>)</div>
    <div id="positions" style="margin-top: 8px; font-size: 13px; color: #94a3b8;">No open trades.</div>
  </div>

  <div class="card">
    <div style="font-size: 13px; font-weight: 600;">Execution Activity</div>
    <div class="log-box" id="logs">Connecting to server...</div>
  </div>

  <script>
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    let ws = new WebSocket(`${proto}//${location.host}/ws`);

    ws.onmessage = (e) => {
      const d = JSON.parse(e.data);
      document.getElementById('symLabel').innerText = d.display_symbol + ' (Live)';
      document.getElementById('ltp').innerText = d.last_price > 0 ? '₹' + d.last_price.toFixed(2) : 'Fetching live price...';
      document.getElementById('bal').innerText = '₹' + d.paper_balance.toLocaleString('en-IN');
      
      const badge = document.getElementById('modeBadge');
      badge.innerText = d.mode;
      badge.className = d.mode === 'LIVE' ? 'badge badge-live' : 'badge badge-paper';

      document.getElementById('count').innerText = d.positions.length;
      document.getElementById('positions').innerHTML = d.positions.length ? d.positions.map(p => 
        `<div style="display:flex; justify-content:space-between; margin-bottom:4px; color:#fff;">
           <span>${p.qty}x ${p.symbol} @ ₹${p.entry_price.toFixed(1)}</span>
           <span style="color:${p.pnl >= 0 ? 'var(--up)':'var(--down)'}">₹${p.pnl.toFixed(2)}</span>
         </div>`
      ).join('') : 'No open trades.';

      document.getElementById('logs').innerHTML = d.logs.map(l => `<div>• ${l}</div>`).join('');
    };

    function order(side) {
      ws.send(JSON.stringify({ action: 'ORDER', side, qty: 1 }));
    }
    function toggle() {
      ws.send(JSON.stringify({ action: 'TOGGLE_MODE' }));
    }
    function changeTicker(el) {
      const [sym, disp] = el.value.split('|');
      ws.send(JSON.stringify({ action: 'CHANGE_SYMBOL', symbol: sym, display: disp }));
    }
  </script>
</body>
</html>
