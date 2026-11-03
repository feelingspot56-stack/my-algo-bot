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
    "logs": ["Engine ready. Multi-asset selector loaded."],
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
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            meta = data["chart"]["result"][0]["meta"]
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
        if strat["buy_above"] > 0 and price >= strat["buy_above"]:
            execute_paper_order("BUY", strat["qty"], price)
            log_event(f"⚡ Breakout triggered above ₹{strat['buy_above']}!")
            strat["buy_above"] = 0.0
        elif strat["sell_below"] > 0 and price <= strat["sell_below"]:
            log_event(f"⚠️ Sell level hit below ₹{strat['sell_below']}")
            strat["sell_below"] = 0.0

async def feed_worker():
    while True:
        try:
            p = await asyncio.to_thread(fetch_price, state["symbol"])
            if p > 0:
                state["last_price"] = p
                for pos in state["positions"]:
                    pos["ltp"] = state["last_price"]
                    pos["pnl"] = round((state["last_price"] - pos["entry_price"]) * pos["qty"], 2)
                evaluate_strategy(p)
            await broadcast_state()
        except Exception as e:
            logging.error(f"Worker loop: {e}")
        await asyncio.sleep(4)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(feed_worker())

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
            
            elif action == "CHANGE_SYMBOL":
                sym = cmd.get("symbol", "").strip()
                disp = cmd.get("display", "").strip()
                if sym:
                    state["symbol"] = sym
                    state["display_symbol"] = disp if disp else sym
                    log_event(f"Switched asset to {state['display_symbol']}")
                    p = await asyncio.to_thread(fetch_price, sym)
                    if p > 0:
                        state["last_price"] = p
                        evaluate_strategy(p)

            elif action == "SAVE_STRATEGY":
                strat = state["strategy"]
                strat["buy_above"] = float(cmd.get("buy_above", 0.0))
                strat["sell_below"] = float(cmd.get("sell_below", 0.0))
                strat["target_pts"] = float(cmd.get("target_pts", 50.0))
                strat["trailing_sl_pts"] = float(cmd.get("trailing_sl_pts", 25.0))
                strat["qty"] = int(cmd.get("qty", 1))
                strat["active"] = bool(cmd.get("active", False))
                status_txt = "ARMED" if strat["active"] else "PAUSED"
                log_event(f"Rules updated & {status_txt}")

            elif action == "ORDER":
                side = cmd.get("side")
                qty = int(cmd.get("qty", 1))
                price = state["last_price"]
                execute_paper_order(side, qty, price)

            await broadcast_state()
    except WebSocketDisconnect:
        if websocket in active_connections:
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
    :root { --bg: #090d16; --card: #131b2e; --border: #1f2b45; --up: #10b981; --down: #ef4444; --text: #f1f5f9; --accent: #3b82f6; }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, system-ui, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 14px; }
    .header { display: flex; justify-content: space-between; align-items: center; padding-bottom: 12px; border-bottom: 1px solid var(--border); }
    .badge { padding: 4px 10px; border-radius: 999px; font-size: 11px; font-weight: 700; }
    .badge-paper { background: #3b82f633; color: #60a5fa; }
    .badge-live { background: #ef444433; color: #f87171; }
    .badge-active { background: #10b98133; color: #34d399; }
    .badge-idle { background: #64748b33; color: #94a3b8; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; margin-top: 12px; }
    .price-lg { font-size: 32px; font-weight: 800; font-variant-numeric: tabular-nums; margin-top: 4px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 10px; }
    button { padding: 12px; border-radius: 8px; font-weight: bold; border: none; cursor: pointer; color: white; width: 100%; font-size: 14px; }
    .btn-buy { background: var(--up); }
    .btn-sell { background: var(--down); }
    .btn-mode { background: #1e293b; border: 1px solid var(--border); margin-top: 10px; }
    .btn-save { background: var(--accent); margin-top: 12px; }
    .btn-search { background: #334155; padding: 10px 14px; font-size: 13px; width: auto; border-radius: 6px; }
    .log-box { font-size: 12px; max-height: 140px; overflow-y: auto; background: #00000040; border-radius: 6px; padding: 8px; margin-top: 10px; line-height: 1.6; }
    input, select { width: 100%; padding: 10px; background: #0b0f19; color: #fff; border: 1px solid var(--border); border-radius: 6px; font-size: 14px; }
    label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; }
    .toggle-row { display: flex; justify-content: space-between; align-items: center; margin-top: 6px; }
    .quick-chips { display: flex; gap: 6px; overflow-x: auto; padding-bottom: 4px; margin-top: 8px; }
    .chip { background: #1e293b; padding: 5px 10px; border-radius: 6px; font-size: 12px; white-space: nowrap; cursor: pointer; border: 1px solid var(--border); }
  </style>
</head>
<body>
  <div class="header">
    <h3>Algo Terminal</h3>
    <span id="modeBadge" class="badge badge-paper">PAPER</span>
  </div>

  <!-- ASSET SEARCH / SELECTOR -->
  <div class="card">
    <label>Select Asset / Stock / Commodity</label>
    <div style="display: flex; gap: 8px; margin-top: 6px;">
      <input id="customSymbol" type="text" placeholder="e.g. INFY, SBIN, GOLD, CRUDEOIL" style="text-transform:uppercase;">
      <button class="btn-search" onclick="loadCustomAsset()">Load</button>
    </div>

    <!-- Quick select chips -->
    <div class="quick-chips">
      <div class="chip" onclick="setPreset('^NSEI', 'NIFTY 50')">NIFTY 50</div>
      <div class="chip" onclick="setPreset('^NSEBANK', 'BANKNIFTY')">BANKNIFTY</div>
      <div class="chip" onclick="setPreset('RELIANCE.NS', 'RELIANCE')">Reliance</div>
      <div class="chip" onclick="setPreset('TATAMOTORS.NS', 'TATAMOTORS')">Tata Motors</div>
      <div class="chip" onclick="setPreset('GC=F', 'GOLD')">Gold (GC)</div>
      <div class="chip" onclick="setPreset('CL=F', 'CRUDE OIL')">Crude Oil (CL)</div>
      <div class="chip" onclick="setPreset('SI=F', 'SILVER')">Silver (SI)</div>
    </div>
  </div>

  <!-- LIVE PRICE CARD -->
  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <span style="font-size: 12px; color: #94a3b8;" id="symLabel">NIFTY 50 (Live)</span>
    </div>
    <div class="price-lg" id="ltp">Connecting...</div>
    <div style="font-size: 13px; margin-top: 6px;">Virtual Margin: <b id="bal">₹1,00,000</b></div>
  </div>

  <!-- BREAKOUT AUTOMATION CARD -->
  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <span style="font-size: 14px; font-weight: 700;">⚡ Breakout Automation</span>
      <span id="stratStatusBadge" class="badge badge-idle">PAUSED</span>
    </div>
    <p style="font-size: 11px; color: #94a3b8; margin-top: 4px;">Executes automatically on price trigger.</p>

    <div class="grid" style="margin-top: 10px;">
      <div>
        <label>Buy Above (₹)</label>
        <input id="inBuyAbove" type="number" step="0.05" placeholder="e.g. 23850">
      </div>
      <div>
        <label>Sell Below (₹)</label>
        <input id="inSellBelow" type="number" step="0.05" placeholder="e.g. 23750">
      </div>
    </div>

    <div class="grid" style="margin-top: 8px;">
      <div>
        <label>Target (Points)</label>
        <input id="inTarget" type="number" value="40">
      </div>
      <div>
        <label>Trailing SL (Points)</label>
        <input id="inSL" type="number" value="20">
      </div>
    </div>

    <div class="toggle-row" style="margin-top: 12px;">
      <label style="font-size: 13px; color: #fff;">Activate Strategy</label>
      <input id="stratActive" type="checkbox" style="width: 22px; height: 22px;">
    </div>

    <button class="btn-save" onclick="saveStrategy()">Save & Arm Rules</button>
  </div>

  <!-- MANUAL BUTTONS -->
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
    let stratLoaded = false;

    ws.onmessage = (e) => {
      const d = JSON.parse(e.data);
      document.getElementById('symLabel').innerText = d.display_symbol + ' (Live)';
      document.getElementById('ltp').innerText = '₹' + (d.last_price || 0).toLocaleString('en-IN', {minimumFractionDigits: 2});
      document.getElementById('bal').innerText = '₹' + d.paper_balance.toLocaleString('en-IN');
      
      const badge = document.getElementById('modeBadge');
      badge.innerText = d.mode;
      badge.className = d.mode === 'LIVE' ? 'badge badge-live' : 'badge badge-paper';

      const sBadge = document.getElementById('stratStatusBadge');
      if (d.strategy.active) {
        sBadge.innerText = 'ARMED';
        sBadge.className = 'badge badge-active';
      } else {
        sBadge.innerText = 'PAUSED';
        sBadge.className = 'badge badge-idle';
      }

      if (!stratLoaded && d.strategy) {
        document.getElementById('inBuyAbove').value = d.strategy.buy_above || '';
        document.getElementById('inSellBelow').value = d.strategy.sell_below || '';
        document.getElementById('inTarget').value = d.strategy.target_pts || 40;
        document.getElementById('inSL').value = d.strategy.trailing_sl_pts || 20;
        document.getElementById('stratActive').checked = d.strategy.active;
        stratLoaded = true;
      }

      document.getElementById('count').innerText = d.positions.length;
      document.getElementById('positions').innerHTML = d.positions.length ? d.positions.map(p => 
        `<div style="display:flex; justify-content:space-between; margin-bottom:4px; color:#fff;">
           <span>${p.qty}x ${p.symbol} @ ₹${p.entry_price.toFixed(1)} (Peak ₹${(p.peak_price||p.entry_price).toFixed(1)})</span>
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
    function setPreset(sym, disp) {
      ws.send(JSON.stringify({ action: 'CHANGE_SYMBOL', symbol: sym, display: disp }));
    }
    function loadCustomAsset() {
      let input = document.getElementById('customSymbol').value.trim().toUpperCase();
      if (!input) return;

      let sym = input;
      let disp = input;

      // Automatically append .NS for Indian stocks if not provided
      if (!input.includes('.') && !input.startsWith('^') && !input.includes('=')) {
        sym = input + '.NS';
      }
      ws.send(JSON.stringify({ action: 'CHANGE_SYMBOL', symbol: sym, display: disp }));
      document.getElementById('customSymbol').value = '';
    }
    function saveStrategy() {
      const payload = {
        action: 'SAVE_STRATEGY',
        buy_above: document.getElementById('inBuyAbove').value,
        sell_below: document.getElementById('inSellBelow').value,
        target_pts: document.getElementById('inTarget').value,
        trailing_sl_pts: document.getElementById('inSL').value,
        active: document.getElementById('stratActive').checked,
        qty: 1
      };
      ws.send(JSON.stringify(payload));
    }
  </script>
</body>
</html>
