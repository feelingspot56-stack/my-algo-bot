import asyncio
import json
import logging
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from kiteconnect import KiteConnect, KiteTicker

logging.basicConfig(level=logging.INFO)

# ================= CONFIGURATION =================
API_KEY = "YOUR_KITE_API_KEY"
API_SECRET = "YOUR_KITE_API_SECRET"
REDIRECT_URL = "http://localhost:8000/callback"
INSTRUMENT_TOKENS = [256265]  # NIFTY 50
# =================================================

app = FastAPI(title="Mobile Algo Hub")
kite = KiteConnect(api_key=API_KEY)
kws = None

state = {
    "authenticated": False,
    "mode": "PAPER",  # "PAPER" or "LIVE"
    "paper_balance": 100000.0,
    "last_price": 0.0,
    "symbol": "NIFTY",
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

def on_ticks(ws, ticks):
    for tick in ticks:
        ltp = tick.get("last_price", 0.0)
        state["last_price"] = ltp
        for pos in state["positions"]:
            pos["ltp"] = ltp
            pos["pnl"] = round((ltp - pos["entry_price"]) * pos["qty"], 2)
    asyncio.run(broadcast_state())

def on_connect(ws, response):
    logging.info("Connected to KiteTicker WebSockets.")
    ws.subscribe(INSTRUMENT_TOKENS)
    ws.set_mode(ws.MODE_LTP, INSTRUMENT_TOKENS)

@app.get("/login")
def login():
    return RedirectResponse(kite.login_url())

@app.get("/callback")
def callback(request_token: str):
    global kws
    try:
        session = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = session["access_token"]
        kite.set_access_token(access_token)
        state["authenticated"] = True
        log_event("Kite Connect linked successfully.")

        kws = KiteTicker(API_KEY, access_token)
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.connect(threaded=True)

        return RedirectResponse("/")
    except Exception as e:
        return HTMLResponse(f"<h3>Login error: {e}</h3>")

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
                log_event(f"Mode switched to {state['mode']}")
            
            elif action == "ORDER":
                side = cmd.get("side")
                qty = int(cmd.get("qty", 1))
                sym = cmd.get("symbol", state["symbol"])
                price = state["last_price"]

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

                elif state["mode"] == "LIVE":
                    try:
                        tx = kite.TRANSACTION_TYPE_BUY if side == "BUY" else kite.TRANSACTION_TYPE_SELL
                        order_id = kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NSE,
                            tradingsymbol=sym,
                            transaction_type=tx,
                            quantity=qty,
                            product=kite.PRODUCT_MIS,
                            order_type=kite.ORDER_TYPE_MARKET
                        )
                        log_event(f"[LIVE] Order #{order_id} ({side} {qty} {sym})")
                    except Exception as err:
                        log_event(f"[LIVE ERROR] {err}")

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
  </style>
</head>
<body>
  <div class="header">
    <h3>Algo Terminal</h3>
    <span id="modeBadge" class="badge badge-paper">PAPER</span>
  </div>

  <div class="card">
    <div style="font-size: 12px; color: #94a3b8;">NSE Live Feed (LTP)</div>
    <div class="price-lg" id="ltp">₹0.00</div>
    <div style="font-size: 13px; margin-top: 6px;">Virtual Margin: <b id="bal">₹1,00,000</b></div>
  </div>

  <div class="card">
    <div style="font-size: 13px; font-weight: 600;">Manual / Instant Signal</div>
    <div class="grid">
      <button class="btn-buy" onclick="order('BUY')">BUY (MIS)</button>
      <button class="btn-sell" onclick="order('SELL')">SELL (MIS)</button>
    </div>
    <button class="btn-mode" onclick="toggle()">Switch Paper / Live</button>
    <a href="/login" style="display:block; text-align:center; color:#60a5fa; margin-top:12px; font-size:12px; text-decoration:none;">
      🔑 Re-authenticate Kite Session
    </a>
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
      document.getElementById('ltp').innerText = '₹' + (d.last_price || 0).toFixed(2);
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
  </script>
</body>
</html>
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
