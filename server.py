import asyncio
from datetime import datetime
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
    "symbol": "SUZLON.NS",
    "display_symbol": "SUZLON",
    "paper_balance": 100000.0,
    "last_price": 35.75,
    "positions": [],
    "logs": ["Engine ready. Multi-asset selector active."],
    "strategy": {
        "active": False,
        "buy_above": 0.0,
        "sell_below": 0.0,
        "qty": 10,
        "target_pts": 5.0,
        "trailing_sl_pts": 2.0,
        "armed_time": None
    },
    "trigger_history": []
}

active_connections: List[WebSocket] = []

COMMODITY_ALIASES = {
    "NATURAL GAS": ("NG=F", "NATURAL GAS"),
    "NATURALGAS": ("NG=F", "NATURAL GAS"),
    "NATGAS": ("NG=F", "NATURAL GAS"),
    "NATURAL GAS MINI": ("NG=F", "NATURAL GAS"),
    "CRUDE OIL": ("CL=F", "CRUDE OIL"),
    "CRUDE": ("CL=F", "CRUDE OIL"),
    "CRUDEOIL": ("CL=F", "CRUDE OIL"),
    "GOLD": ("GC=F", "GOLD"),
    "GOLD MINI": ("GC=F", "GOLD"),
    "SILVER": ("SI=F", "SILVER"),
    "SILVER MINI": ("SI=F", "SILVER"),
    "COPPER": ("HG=F", "COPPER"),
    "NIFTY": ("^NSEI", "NIFTY 50"),
    "BANKNIFTY": ("^NSEBANK", "BANKNIFTY"),
    "BANK NIFTY": ("^NSEBANK", "BANKNIFTY"),
}

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
        req = urllib.request.Request(
            url, 
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read().decode())
            res = data.get("chart", {}).get("result")
            if res and len(res) > 0:
                meta = res[0].get("meta", {})
                price = meta.get("regularMarketPrice")
                if price:
                    return round(float(price), 2)
    except Exception as e:
        logging.warning(f"Price fetch failed for {symbol}: {e}")
    return 0.0

def execute_paper_order(side: str, qty: int, price: float, reason: str = "MANUAL"):
    sym = state["display_symbol"]
    timestamp = datetime.now().strftime("%H:%M:%S")

    if side == "BUY":
        cost = price * qty
        state["paper_balance"] -= cost
        state["positions"].append({
            "symbol": sym, "side": "BUY", "qty": qty, 
            "entry_price": price, "ltp": price, "pnl": 0.0,
            "peak_price": price, "time": timestamp
        })
        log_event(f"[{reason}-BUY] {qty}x {sym} @ ₹{price}")
        state["trigger_history"].insert(0, {
            "time": timestamp, "symbol": sym, "action": "BUY",
            "qty": qty, "price": price, "reason": reason, "pnl": "-"
        })
    elif side == "SELL" and state["positions"]:
        p = state["positions"].pop(0)
        pnl = round((price - p["entry_price"]) * p["qty"], 2)
        state["paper_balance"] += (p["entry_price"] * p["qty"]) + pnl
        log_event(f"[{reason}-SELL] {p['qty']}x {sym} @ ₹{price} | P&L: ₹{pnl}")
        state["trigger_history"].insert(0, {
            "time": timestamp, "symbol": sym, "action": "SELL",
            "qty": p["qty"], "price": price, "reason": reason, "pnl": f"₹{pnl:+.2f}"
        })

    if len(state["trigger_history"]) > 20:
        state["trigger_history"].pop()

def evaluate_strategy(price: float):
    strat = state["strategy"]
    if not strat.get("active", False) or price <= 0:
        return

    # 1. Manage Exits (Target & Trailing SL)
    for pos in list(state["positions"]):
        if pos["side"] == "BUY":
            if price > pos.get("peak_price", pos["entry_price"]):
                pos["peak_price"] = price

            entry = pos["entry_price"]
            peak = pos["peak_price"]
            target = entry + strat["target_pts"]
            trailing_stop = peak - strat["trailing_sl_pts"]

            if price >= target:
                execute_paper_order("SELL", pos["qty"], price, reason="TARGET")
                log_event(f"🎯 Target reached at ₹{price} (+{strat['target_pts']} pts)!")
                strat["active"] = False
                return

            if price <= trailing_stop:
                execute_paper_order("SELL", pos["qty"], price, reason="TRAIL-SL")
                log_event(f"🛑 Trailing SL hit at ₹{price} (Peak: ₹{peak})")
                strat["active"] = False
                return

    # 2. Manage Entries
    if len(state["positions"]) == 0:
        qty = int(strat.get("qty", 1))
        if strat["buy_above"] > 0 and price >= strat["buy_above"]:
            execute_paper_order("BUY", qty, price, reason="BREAKOUT")
            log_event(f"⚡ Breakout triggered: Bought {qty}x {state['display_symbol']} @ ₹{price}")
            strat["buy_above"] = 0.0
        elif strat["sell_below"] > 0 and price <= strat["sell_below"]:
            log_event(f"⚠️ Price fell below ₹{strat['sell_below']}")
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
            logging.error(f"Worker loop error: {e}")
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
                log_event(f"Mode set to {state['mode']}")
            
            elif action == "CHANGE_SYMBOL":
                raw = cmd.get("symbol", "").strip().upper()
                disp = cmd.get("display", "").strip()

                if raw in COMMODITY_ALIASES:
                    sym, disp = COMMODITY_ALIASES[raw]
                elif raw.replace(" ", "") in COMMODITY_ALIASES:
                    sym, disp = COMMODITY_ALIASES[raw.replace(" ", "")]
                else:
                    if "." not in raw and not raw.startswith("^") and not raw.endswith("=F"):
                        sym = raw + ".NS"
                        disp = raw
                    else:
                        sym = raw
                        disp = disp or raw

                log_event(f"Requesting feed for {disp} ({sym})...")
                p = await asyncio.to_thread(fetch_price, sym)
                if p > 0:
                    state["symbol"] = sym
                    state["display_symbol"] = disp
                    state["last_price"] = p
                    log_event(f"Tracking {disp} @ ₹{p}")
                    evaluate_strategy(p)
                else:
                    log_event(f"⚠️ Cannot fetch price for {disp}. Ticker: {sym}")

            elif action == "SAVE_STRATEGY":
                strat = state["strategy"]
                strat["buy_above"] = float(cmd.get("buy_above") or 0.0)
                strat["sell_below"] = float(cmd.get("sell_below") or 0.0)
                strat["target_pts"] = float(cmd.get("target_pts") or 5.0)
                strat["trailing_sl_pts"] = float(cmd.get("trailing_sl_pts") or 2.0)
                strat["qty"] = max(1, int(cmd.get("qty") or 1))
                strat["active"] = bool(cmd.get("active", True))
                strat["armed_time"] = datetime.now().strftime("%d-%b %H:%M:%S") if strat["active"] else None
                status_txt = "ARMED" if strat["active"] else "PAUSED"
                log_event(f"Rules armed for {strat['qty']}x {state['display_symbol']} & {status_txt}")
                evaluate_strategy(state["last_price"])

            elif action == "ORDER":
                side = cmd.get("side")
                qty = max(1, int(cmd.get("qty", 1)))
                price = state["last_price"]
                execute_paper_order(side, qty, price, reason="MANUAL")

            await broadcast_state()
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)

HTML_CONTENT = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Algo Terminal</title>
  <style>
    :root { --bg: #090d16; --card: #131b2e; --border: #1f2b45; --up: #10b981; --down: #ef4444; --text: #f1f5f9; --accent: #2563eb; }
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
    .btn-save { background: var(--accent); margin-top: 12px; transition: background 0.2s; }
    .btn-search { background: #3b82f6; padding: 10px 18px; font-size: 14px; width: auto; border-radius: 6px; }
    .log-box { font-size: 12px; max-height: 140px; overflow-y: auto; background: #00000040; border-radius: 6px; padding: 8px; margin-top: 10px; line-height: 1.6; }
    input, select { width: 100%; padding: 10px; background: #0b0f19; color: #fff; border: 1px solid var(--border); border-radius: 6px; font-size: 14px; }
    label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; }
    .toggle-row { display: flex; justify-content: space-between; align-items: center; margin-top: 10px; }
    .quick-chips { display: flex; gap: 6px; overflow-x: auto; padding-bottom: 4px; margin-top: 8px; }
    .chip { background: #1e293b; padding: 6px 12px; border-radius: 6px; font-size: 12px; white-space: nowrap; cursor: pointer; border: 1px solid var(--border); }
    table { width: 100%; border-collapse: collapse; font-size: 12px; margin-top: 8px; }
    th { text-align: left; padding: 6px 4px; color: #94a3b8; font-weight: 600; border-bottom: 1px solid var(--border); }
    td { padding: 8px 4px; border-bottom: 1px solid #1f2b4533; }
  </style>
</head>
<body>
  <div class="header">
    <h3>Algo Terminal</h3>
    <span id="modeBadge" class="badge badge-paper">PAPER</span>
  </div>

  <!-- ASSET SELECTOR -->
  <div class="card">
    <label>Search Stock / Commodity</label>
    <div style="display: flex; gap: 8px; margin-top: 6px;">
      <input id="customSymbol" type="text" placeholder="e.g. SUZLON, NATGAS, GOLD, INFY" style="text-transform:uppercase;">
      <button id="loadBtn" class="btn-search" onclick="loadCustomAsset()">Load</button>
    </div>
    <div class="quick-chips">
      <div class="chip" onclick="setPreset('SUZLON.NS', 'SUZLON')">⚡ Suzlon</div>
      <div class="chip" onclick="setPreset('NG=F', 'NATURAL GAS')">🔥 NatGas</div>
      <div class="chip" onclick="setPreset('CL=F', 'CRUDE OIL')">🛢️ Crude</div>
      <div class="chip" onclick="setPreset('GC=F', 'GOLD')">🪙 Gold</div>
      <div class="chip" onclick="setPreset('^NSEI', 'NIFTY 50')">NIFTY 50</div>
      <div class="chip" onclick="setPreset('TATAMOTORS.NS', 'TATAMOTORS')">Tata Motors</div>
      <div class="chip" onclick="setPreset('RELIANCE.NS', 'RELIANCE')">Reliance</div>
    </div>
  </div>

  <!-- LIVE PRICE -->
  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <span style="font-size: 12px; color: #94a3b8;" id="symLabel">SUZLON (Live)</span>
    </div>
    <div class="price-lg" id="ltp">Connecting...</div>
    <div style="font-size: 13px; margin-top: 6px;">Virtual Margin: <b id="bal">₹1,00,000</b></div>
  </div>

  <!-- BREAKOUT AUTOMATION -->
  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <span style="font-size: 14px; font-weight: 700;">⚡ Breakout Automation</span>
      <span id="stratStatusBadge" class="badge badge-idle">PAUSED</span>
    </div>
    <p style="font-size: 11px; color: #94a3b8; margin-top: 4px;">Executes automatically on price trigger.</p>

    <div class="grid" style="margin-top: 10px;">
      <div>
        <label>Buy Above (₹)</label>
        <input id="inBuyAbove" type="number" step="0.05" placeholder="e.g. 36.00">
      </div>
      <div>
        <label>Sell Below (₹)</label>
        <input id="inSellBelow" type="number" step="0.05" placeholder="e.g. 35.00">
      </div>
    </div>

    <div class="grid" style="margin-top: 8px;">
      <div>
        <label>Target (Points / ₹)</label>
        <input id="inTarget" type="number" step="0.05" value="5">
      </div>
      <div>
        <label>Trailing SL (Points / ₹)</label>
        <input id="inSL" type="number" step="0.05" value="2">
      </div>
    </div>

    <div style="margin-top: 8px;">
      <label>Order Quantity (Shares / Lots)</label>
      <input id="inQty" type="number" min="1" step="1" value="10" placeholder="e.g. 10 or 100">
    </div>

    <div class="toggle-row">
      <label style="font-size: 13px; color: #fff;">Activate Strategy</label>
      <input id="stratActive" type="checkbox" style="width: 24px; height: 24px; cursor: pointer;">
    </div>

    <button id="btnSave" class="btn-save" onclick="saveStrategy()">Save & Arm Rules</button>
  </div>

  <!-- ACTIVE RULE & TRIGGER HISTORY CARD -->
  <div class="card">
    <div style="font-size: 13px; font-weight: 600;">Active Armed Rule</div>
    <div id="activeRuleSummary" style="margin-top: 6px; font-size: 12px; color: #cbd5e1; background: #0b0f19; padding: 8px; border-radius: 6px;">
      No active strategy rules armed.
    </div>

    <div style="font-size: 13px; font-weight: 600; margin-top: 14px;">Trigger & Order History</div>
    <div style="max-height: 180px; overflow-y: auto;">
      <table id="historyTable">
        <thead>
          <tr>
            <th>Time</th>
            <th>Asset</th>
            <th>Type</th>
            <th>Qty</th>
            <th>Price</th>
            <th>P&L</th>
          </tr>
        </thead>
        <tbody id="historyBody">
          <tr><td colspan="6" style="text-align:center; color:#94a3b8;">No trigger executions recorded yet.</td></tr>
        </tbody>
      </table>
    </div>
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

  <!-- OPEN POSITIONS -->
  <div class="card">
    <div style="font-size: 13px; font-weight: 600;">Open Positions (<span id="count">0</span>)</div>
    <div id="positions" style="margin-top: 8px; font-size: 13px; color: #94a3b8;">No open trades.</div>
  </div>

  <!-- LOGS -->
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
      if (d.strategy && d.strategy.active) {
        sBadge.innerText = 'ARMED';
        sBadge.className = 'badge badge-active';
      } else {
        sBadge.innerText = 'PAUSED';
        sBadge.className = 'badge badge-idle';
      }

      // Populate input values once initially
      if (!stratLoaded && d.strategy) {
        if (d.strategy.buy_above) document.getElementById('inBuyAbove').value = d.strategy.buy_above;
        if (d.strategy.sell_below) document.getElementById('inSellBelow').value = d.strategy.sell_below;
        document.getElementById('inTarget').value = d.strategy.target_pts || 5;
        document.getElementById('inSL').value = d.strategy.trailing_sl_pts || 2;
        document.getElementById('inQty').value = d.strategy.qty || 10;
        document.getElementById('stratActive').checked = Boolean(d.strategy.active);
        stratLoaded = true;
      }

      // Render Active Rule Summary
      const summaryDiv = document.getElementById('activeRuleSummary');
      if (d.strategy && d.strategy.active) {
        summaryDiv.innerHTML = `
          <div style="display:flex; justify-content:space-between; margin-bottom:4px;">
            <b>${d.strategy.qty || 1}x ${d.display_symbol}</b>
            <span style="color:#34d399; font-weight:bold;">ARMED</span>
          </div>
          <div>• Buy Above: <b>₹${d.strategy.buy_above || 'N/A'}</b> | Sell Below: <b>₹${d.strategy.sell_below || 'N/A'}</b></div>
          <div>• Target: <b>+${d.strategy.target_pts} pts</b> | Trailing SL: <b>-${d.strategy.trailing_sl_pts} pts</b></div>
          <div style="color:#64748b; font-size:11px; margin-top:4px;">Armed at: ${d.strategy.armed_time || 'Active session'}</div>
        `;
      } else {
        summaryDiv.innerHTML = '<span style="color:#94a3b8;">No active breakout rules armed. Enter figures and tap Save & Arm.</span>';
      }

      // Render Trigger History Table
      const historyBody = document.getElementById('historyBody');
      if (d.trigger_history && d.trigger_history.length > 0) {
        historyBody.innerHTML = d.trigger_history.map(h => `
          <tr>
            <td>${h.time}</td>
            <td><b>${h.symbol}</b></td>
            <td style="color:${h.action === 'BUY' ? 'var(--up)' : 'var(--down)'}; font-weight:bold;">${h.action} (${h.reason})</td>
            <td>${h.qty}</td>
            <td>₹${h.price.toFixed(2)}</td>
            <td style="color:${h.pnl.includes('-') && h.pnl !== '-' ? 'var(--down)' : 'var(--up)'}">${h.pnl}</td>
          </tr>
        `).join('');
      } else {
        historyBody.innerHTML = '<tr><td colspan="6" style="text-align:center; color:#94a3b8;">No trigger executions recorded yet.</td></tr>';
      }

      // Render Open Positions
      document.getElementById('count').innerText = d.positions.length;
      document.getElementById('positions').innerHTML = d.positions.length ? d.positions.map(p => 
        `<div style="display:flex; justify-content:space-between; margin-bottom:4px; color:#fff;">
           <span>${p.qty}x ${p.symbol} @ ₹${p.entry_price.toFixed(2)}</span>
           <span style="color:${p.pnl >= 0 ? 'var(--up)':'var(--down)'}">₹${p.pnl.toFixed(2)}</span>
         </div>`
      ).join('') : 'No open trades.';

      document.getElementById('logs').innerHTML = d.logs.map(l => `<div>• ${l}</div>`).join('');

      const loadBtn = document.getElementById('loadBtn');
      if (loadBtn) loadBtn.innerText = 'Load';
    };

    function order(side) {
      const q = parseInt(document.getElementById('inQty').value) || 1;
      ws.send(JSON.stringify({ action: 'ORDER', side, qty: q }));
    }
    function toggle() {
      ws.send(JSON.stringify({ action: 'TOGGLE_MODE' }));
    }
    function setPreset(sym, disp) {
      const loadBtn = document.getElementById('loadBtn');
      if (loadBtn) loadBtn.innerText = '...';
      ws.send(JSON.stringify({ action: 'CHANGE_SYMBOL', symbol: sym, display: disp }));
    }
    function loadCustomAsset() {
      let raw = document.getElementById('customSymbol').value.trim();
      if (!raw) return;

      const loadBtn = document.getElementById('loadBtn');
      if (loadBtn) loadBtn.innerText = 'Loading...';

      ws.send(JSON.stringify({ action: 'CHANGE_SYMBOL', symbol: raw, display: raw.toUpperCase() }));
      document.getElementById('customSymbol').value = '';
    }
    function saveStrategy() {
      const chk = document.getElementById('stratActive');
      chk.checked = true;

      const qtyVal = parseInt(document.getElementById('inQty').value) || 1;

      const btn = document.getElementById('btnSave');
      btn.innerText = 'Armed & Saved!';
      btn.style.background = '#10b981';
      setTimeout(() => { 
        btn.innerText = 'Save & Arm Rules'; 
        btn.style.background = 'var(--accent)';
      }, 1200);

      const sBadge = document.getElementById('stratStatusBadge');
      sBadge.innerText = 'ARMED';
      sBadge.className = 'badge badge-active';

      const payload = {
        action: 'SAVE_STRATEGY',
        buy_above: parseFloat(document.getElementById('inBuyAbove').value) || 0.0,
        sell_below: parseFloat(document.getElementById('inSellBelow').value) || 0.0,
        target_pts: parseFloat(document.getElementById('inTarget').value) || 5.0,
        trailing_sl_pts: parseFloat(document.getElementById('inSL').value) || 2.0,
        qty: qtyVal,
        active: true
      };
      ws.send(JSON.stringify(payload));
    }
  </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def index():
    return HTML_CONTENT

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
