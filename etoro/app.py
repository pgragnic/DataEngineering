from flask import Flask, jsonify, request
import requests, uuid

app = Flask(__name__)

AGENT_KEY  = "eyJjaSI6IjYwY2FiYjBiLTU1OTctNDQ4NS04ZjYzLTdlOWUwNTZlMGJiOCIsImVhbiI6IlVucmVnaXN0ZXJlZEFwcGxpY2F0aW9uIiwiZWsiOiI1VmJNekM2WEozUTk2elVZRG9ScTZCbjE4LjdVenAzZ2xkVWsuWUhQUW5RSzNjVEkwSUZMWTVxeS1MVkpOeU9FUmdvQVg0WXZ0YXBoNm95Z0FtTDFJSEJjSm1MZkU2ZjJKSC5BalRGMFFSOF8ifQ__"
API_KEY    = "sdgdskldFPLGfjHn1421dgnlxdGTbngdflg6290bRjslfihsjhSDsdgGHH25hjf"
BASE       = "https://public-api.etoro.com/api/v1"
INVESTMENT = 14892
RATIO      = INVESTMENT / 10000.0

def hdrs():
    return {
        "x-api-key": API_KEY,
        "x-user-key": AGENT_KEY,
        "x-request-id": str(uuid.uuid4()),
        "Content-Type": "application/json"
    }

def resolve_names(ids):
    names = {}
    if not ids:
        return names
    try:
        r = requests.get(
            f"{BASE}/market-data/instruments?instrumentIds={','.join(map(str, ids))}",
            headers=hdrs(), timeout=10)
        for inst in r.json().get("instrumentDisplayDatas", []):
            iid = inst.get("instrumentID")
            if iid:
                names[iid] = inst.get("symbolFull") or inst.get("instrumentDisplayName") or f"ID:{iid}"
    except:
        pass
    return names

@app.route("/")
def index():
    return HTML

@app.route("/api/portfolio")
def portfolio():
    try:
        r = requests.get(f"{BASE}/trading/info/real/pnl", headers=hdrs(), timeout=15)
        data = r.json()
        cp = data.get("clientPortfolio", {})

        credit = float(cp.get("credit", 0))
        positions     = cp.get("positions", []) or []
        orders_open   = cp.get("ordersForOpen", []) or []
        orders_close  = cp.get("orders", []) or []

        all_ids = list(set(
            [p["instrumentID"] for p in positions if p.get("instrumentID")] +
            [o["instrumentID"] for o in orders_open if o.get("instrumentID")]
        ))
        names = resolve_names(all_ids)

        pending_open_amt  = sum(float(o.get("amount", 0)) for o in orders_open  if o.get("mirrorID", 1) == 0)
        pending_close_amt = sum(float(o.get("amount", 0)) for o in orders_close)
        available_cash    = credit - pending_open_amt - pending_close_amt

        total_invested  = sum(float(p.get("amount", 0)) for p in positions)
        total_invested += pending_open_amt + pending_close_amt

        unrealized_pnl  = sum(float((p.get("unrealizedPnL") or {}).get("pnL", 0)) for p in positions)
        equity_virtual  = available_cash + total_invested + unrealized_pnl

        result_positions = []
        for p in positions:
            iid = p.get("instrumentID")
            result_positions.append({
                "positionID":   p.get("positionID"),
                "instrumentID": iid,
                "name":         names.get(iid, f"ID:{iid}"),
                "amount":       round(float(p.get("amount", 0)) * RATIO, 2),
                "pnl":          round(float((p.get("unrealizedPnL") or {}).get("pnL", 0)) * RATIO, 2),
                "isBuy":        p.get("isBuy", True)
            })

        pending_list = []
        for o in orders_open:
            if o.get("mirrorID", 1) == 0:
                iid = o.get("instrumentID")
                pending_list.append({
                    "orderId":      o.get("orderId"),
                    "instrumentID": iid,
                    "name":         names.get(iid, f"ID:{iid}"),
                    "amount":       round(float(o.get("amount", 0)) * RATIO, 2)
                })

        return jsonify({
            "equity":       round(equity_virtual * RATIO, 2),
            "cash":         round(available_cash  * RATIO, 2),
            "positions":    result_positions,
            "pending":      pending_list,
            "count":        len(result_positions),
            "pendingCount": len(pending_list)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/search/<symbol>")
def search(symbol):
    try:
        r = requests.get(
            f"{BASE}/market-data/search?internalSymbolFull={symbol}",
            headers=hdrs(), timeout=10)
        items = r.json().get("items", []) or []
        match = next((i for i in items if i.get("internalSymbolFull") == symbol), None)
        if match:
            return jsonify({"instrumentId": match["instrumentId"],
                            "name": match.get("instrumentDisplayName", symbol)})
        return jsonify({"error": "Symbole non trouvé"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/buy", methods=["POST"])
def buy():
    try:
        body = request.json or {}
        symbol = body.get("symbol", "").upper()
        pct    = float(body.get("pct", 0))

        r = requests.get(f"{BASE}/trading/info/real/pnl", headers=hdrs(), timeout=15)
        cp = r.json().get("clientPortfolio", {})
        credit       = float(cp.get("credit", 0))
        pos          = cp.get("positions", []) or []
        oopen        = cp.get("ordersForOpen", []) or []
        oclose       = cp.get("orders", []) or []
        pend_open    = sum(float(o.get("amount", 0)) for o in oopen  if o.get("mirrorID", 1) == 0)
        pend_close   = sum(float(o.get("amount", 0)) for o in oclose)
        avail        = credit - pend_open - pend_close
        invested     = sum(float(p.get("amount", 0)) for p in pos) + pend_open + pend_close
        upnl         = sum(float((p.get("unrealizedPnL") or {}).get("pnL", 0)) for p in pos)
        equity_virt  = avail + invested + upnl
        amount       = round(equity_virt * pct / 100, 2)

        sr = requests.get(
            f"{BASE}/market-data/search?internalSymbolFull={symbol}",
            headers=hdrs(), timeout=10)
        items = sr.json().get("items", []) or []
        match = next((i for i in items if i.get("internalSymbolFull") == symbol), None)
        if not match:
            return jsonify({"error": f"Symbole {symbol} non trouvé"}), 404

        payload = {"InstrumentID": match["instrumentId"], "IsBuy": True, "Leverage": 1, "Amount": amount}
        r2 = requests.post(
            f"{BASE}/trading/execution/market-open-orders/by-amount",
            headers=hdrs(), json=payload, timeout=15)
        return jsonify(r2.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sell/<int:pid>", methods=["POST"])
def sell(pid):
    try:
        r = requests.get(f"{BASE}/trading/info/real/pnl", headers=hdrs(), timeout=15)
        positions = r.json().get("clientPortfolio", {}).get("positions", []) or []
        pos = next((p for p in positions if p.get("positionID") == pid), None)
        if not pos:
            return jsonify({"error": "Position non trouvée"}), 404
        payload = {"InstrumentId": pos.get("instrumentID"), "UnitsToDeduct": None}
        r2 = requests.post(
            f"{BASE}/trading/execution/market-close-orders/positions/{pid}",
            headers=hdrs(), json=payload, timeout=15)
        return jsonify(r2.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/cancel/<oid>", methods=["DELETE"])
def cancel(oid):
    try:
        r = requests.delete(
            f"{BASE}/trading/execution/orders/{oid}",
            headers=hdrs(), timeout=15)
        return jsonify(r.json() if r.content else {"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
<title>eToro Portfolio</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #0d1117; color: #e6edf3; min-height: 100vh; }
header { background: #161b22; padding: 20px 16px 12px; text-align: center; }
header h1 { font-size: 1.1rem; color: #58a6ff; font-weight: 600; }
#equity { font-size: 2rem; font-weight: 700; margin: 4px 0; }
#subtitle { font-size: 0.75rem; color: #8b949e; }
nav { display: flex; background: #161b22; border-bottom: 1px solid #30363d; }
nav button { flex: 1; padding: 12px 4px; background: none; border: none; color: #8b949e; font-size: 0.85rem; cursor: pointer; border-bottom: 2px solid transparent; }
nav button.active { color: #58a6ff; border-bottom-color: #58a6ff; }
.tab { display: none; padding: 12px; }
.tab.active { display: block; }
.card { background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 14px; margin-bottom: 10px; }
.card-row { display: flex; justify-content: space-between; align-items: center; }
.card-name { font-weight: 600; font-size: 0.95rem; }
.card-pct { font-size: 1rem; font-weight: 700; color: #3fb950; }
.card-amount { font-size: 0.8rem; color: #8b949e; margin-top: 2px; }
.card-pnl { font-size: 0.8rem; margin-top: 4px; }
.pnl-pos { color: #3fb950; }
.pnl-neg { color: #f85149; }
.bar-bg { height: 4px; background: #30363d; border-radius: 2px; margin-top: 8px; }
.bar-fg { height: 4px; border-radius: 2px; background: #3fb950; }
input[type=text], input[type=number] { width: 100%; padding: 12px; background: #0d1117; border: 1px solid #30363d; border-radius: 8px; color: #e6edf3; font-size: 0.95rem; margin-bottom: 10px; }
button.btn { width: 100%; padding: 13px; background: #238636; border: none; border-radius: 8px; color: white; font-size: 1rem; font-weight: 600; cursor: pointer; margin-top: 4px; }
button.btn:active { background: #1a6e2d; }
button.btn-small { width: auto; padding: 7px 14px; background: #b91c1c; border: none; border-radius: 6px; color: white; font-size: 0.8rem; cursor: pointer; }
#msg, #sell-msg, #pending-msg { padding: 10px; border-radius: 8px; margin-bottom: 10px; font-size: 0.85rem; display: none; }
.msg-ok  { background: #0e3a1f; color: #3fb950; }
.msg-err { background: #3a0e0e; color: #f85149; }
.spinner { text-align: center; padding: 40px; color: #8b949e; }
</style>
</head>
<body>
<header>
  <h1>eToro Agent Portfolio</h1>
  <div id="equity">—</div>
  <div id="subtitle">Chargement...</div>
</header>
<nav>
  <button class="active" onclick="showTab('portfolio',this)">Portfolio</button>
  <button onclick="showTab('acheter',this)">Acheter</button>
  <button onclick="showTab('vendre',this)">Vendre</button>
  <button onclick="showTab('pending',this)">Pending</button>
</nav>

<div id="tab-portfolio" class="tab active">
  <div id="portfolio-content"><div class="spinner">Chargement...</div></div>
</div>

<div id="tab-acheter" class="tab">
  <div id="msg"></div>
  <input type="text" id="symbol-input" placeholder="Symbole (ex: AAPL, GLD)" oninput="this.value=this.value.toUpperCase()">
  <button class="btn" onclick="searchSymbol()">Rechercher</button>
  <div id="search-result" style="margin:10px 0;color:#8b949e;font-size:0.85rem;min-height:20px;"></div>
  <input type="number" id="pct-input" placeholder="% du portfolio à investir" min="0.1" max="100" step="0.1">
  <button class="btn" onclick="buyStock()">Acheter</button>
</div>

<div id="tab-vendre" class="tab">
  <div id="sell-msg"></div>
  <div id="sell-content"><div class="spinner">Chargement...</div></div>
</div>

<div id="tab-pending" class="tab">
  <div id="pending-msg"></div>
  <div id="pending-content"><div class="spinner">Chargement...</div></div>
</div>

<script>
let portfolioData = null;

function showTab(name, btn) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  btn.classList.add('active');
  if (name === 'vendre' || name === 'pending') renderSellPending();
}

async function loadPortfolio() {
  try {
    const r = await fetch('/api/portfolio');
    portfolioData = await r.json();
    if (portfolioData.error) {
      document.getElementById('subtitle').textContent = 'Erreur: ' + portfolioData.error;
      return;
    }
    renderPortfolio();
  } catch(e) {
    document.getElementById('subtitle').textContent = 'Erreur réseau';
  }
}

function fmt(n) {
  return '$' + n.toLocaleString('fr-FR', {minimumFractionDigits:2, maximumFractionDigits:2});
}

function renderPortfolio() {
  const d = portfolioData;
  document.getElementById('equity').textContent = fmt(d.equity);
  document.getElementById('subtitle').textContent =
    d.count + ' positions · ' + d.pendingCount + ' pending · v2.1 · 12 mai 2026';

  const equity = d.equity;
  let html = '';

  const cashPct = equity > 0 ? (d.cash / equity * 100) : 0;
  html += `<div class="card">
    <div class="card-row">
      <span class="card-name">Cash</span>
      <span class="card-pct" style="color:#e3b341">${cashPct.toFixed(1)}%</span>
    </div>
    <div class="card-amount">${fmt(d.cash)}</div>
  </div>`;

  const sorted = [...d.positions].sort((a, b) => b.amount - a.amount);
  for (const p of sorted) {
    const pct    = equity > 0 ? (p.amount / equity * 100) : 0;
    const barW   = Math.min(pct, 100).toFixed(1);
    const pnlCls = p.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
    const pnlStr = (p.pnl >= 0 ? '+' : '') + fmt(p.pnl);
    html += `<div class="card">
      <div class="card-row">
        <span class="card-name">${p.name}</span>
        <span class="card-pct">${pct.toFixed(1)}%</span>
      </div>
      <div class="card-amount">${fmt(p.amount)}</div>
      <div class="card-pnl ${pnlCls}">${pnlStr}</div>
      <div class="bar-bg"><div class="bar-fg" style="width:${barW}%"></div></div>
    </div>`;
  }

  document.getElementById('portfolio-content').innerHTML = html;
}

function renderSellPending() {
  if (!portfolioData) { loadPortfolio().then(() => renderSellPending()); return; }
  const d = portfolioData;

  let sellHtml = '';
  for (const p of d.positions) {
    sellHtml += `<div class="card">
      <div class="card-row">
        <span class="card-name">${p.name}</span>
        <button class="btn-small" onclick="sellPos(${p.positionID})">Vendre</button>
      </div>
      <div class="card-amount">${fmt(p.amount)}</div>
    </div>`;
  }
  if (!sellHtml) sellHtml = '<p style="color:#8b949e;padding:20px;text-align:center">Aucune position ouverte</p>';
  document.getElementById('sell-content').innerHTML = sellHtml;

  let pendHtml = '';
  for (const o of d.pending) {
    pendHtml += `<div class="card">
      <div class="card-row">
        <span class="card-name">${o.name}</span>
        <button class="btn-small" onclick="cancelOrder('${o.orderId}')">Annuler</button>
      </div>
      <div class="card-amount">${fmt(o.amount)}</div>
    </div>`;
  }
  if (!pendHtml) pendHtml = '<p style="color:#8b949e;padding:20px;text-align:center">Aucun ordre en attente</p>';
  document.getElementById('pending-content').innerHTML = pendHtml;
}

async function searchSymbol() {
  const sym = document.getElementById('symbol-input').value.trim();
  if (!sym) return;
  const el = document.getElementById('search-result');
  el.style.color = '#8b949e';
  el.textContent = 'Recherche...';
  try {
    const r = await fetch('/api/search/' + sym);
    const d = await r.json();
    if (d.error) { el.style.color = '#f85149'; el.textContent = d.error; return; }
    el.style.color = '#3fb950';
    el.textContent = '✓ ' + (d.name || sym) + '  (ID: ' + d.instrumentId + ')';
  } catch(e) {
    el.style.color = '#f85149';
    el.textContent = 'Erreur réseau';
  }
}

async function buyStock() {
  const sym  = document.getElementById('symbol-input').value.trim();
  const pct  = parseFloat(document.getElementById('pct-input').value);
  const msgEl = document.getElementById('msg');
  if (!sym || !pct) { showMsg(msgEl, 'Symbole et pourcentage requis', false); return; }
  showMsg(msgEl, 'Envoi en cours...', null);
  try {
    const r = await fetch('/api/buy', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({symbol: sym, pct: pct})
    });
    const d = await r.json();
    if (d.error || d.errorCode) {
      showMsg(msgEl, d.error || d.errorMessage || JSON.stringify(d), false);
    } else {
      showMsg(msgEl, 'Ordre envoyé !', true);
      loadPortfolio();
    }
  } catch(e) {
    showMsg(msgEl, 'Erreur réseau', false);
  }
}

async function sellPos(pid) {
  const msgEl = document.getElementById('sell-msg');
  if (!confirm('Fermer cette position ?')) return;
  try {
    const r = await fetch('/api/sell/' + pid, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}'
    });
    const d = await r.json();
    if (d.error || d.errorCode) {
      showMsg(msgEl, d.error || d.errorMessage || JSON.stringify(d), false);
    } else {
      showMsg(msgEl, 'Position fermée', true);
      await loadPortfolio();
      renderSellPending();
    }
  } catch(e) {
    showMsg(msgEl, 'Erreur réseau', false);
  }
}

async function cancelOrder(oid) {
  const msgEl = document.getElementById('pending-msg');
  if (!confirm('Annuler cet ordre ?')) return;
  try {
    const r = await fetch('/api/cancel/' + oid, {method: 'DELETE'});
    const d = await r.json();
    if (d.error || d.errorCode) {
      showMsg(msgEl, d.error || d.errorMessage || JSON.stringify(d), false);
    } else {
      showMsg(msgEl, 'Ordre annulé', true);
      await loadPortfolio();
      renderSellPending();
    }
  } catch(e) {
    showMsg(msgEl, 'Erreur réseau', false);
  }
}

function showMsg(el, text, ok) {
  el.textContent = text;
  el.className   = ok === true ? 'msg-ok' : ok === false ? 'msg-err' : '';
  el.style.background = ok === null ? '#1c2333' : '';
  el.style.color      = ok === null ? '#8b949e' : '';
  el.style.display    = 'block';
  if (ok !== null) setTimeout(() => { el.style.display = 'none'; }, 4000);
}

loadPortfolio();
</script>
</body>
</html>"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
