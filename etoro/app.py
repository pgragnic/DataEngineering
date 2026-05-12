from flask import Flask, jsonify, request
import requests
import uuid

app = Flask(__name__)

API_KEY = "sdgdskldFPLGfjHn1421dgnlxdGTbngdflg6290bRjslfihsjhSDsdgGHH25hjf"
BASE = "https://public-api.etoro.com/api/v1"

NAMES = {
    1467: "GS", 3025: "GLD", 1011: "BAC", 1023: "JPM",
    4430: "SLV", 6688: "EQNR", 8684: "TTE", 14358: "RR", 1486: "SHEL"
}

def etoro_headers(user_key):
    return {
        "x-api-key": API_KEY,
        "x-user-key": user_key,
        "x-request-id": str(uuid.uuid4()),
        "Content-Type": "application/json"
    }

def user_key():
    return request.headers.get("X-User-Key", "")

@app.route("/")
def index():
    return HTML

@app.route("/api/portfolio")
def portfolio():
    r = requests.get(f"{BASE}/trading/info/real/pnl", headers=etoro_headers(user_key()))
    p = r.json().get("clientPortfolio", {})
    cash = p.get("credit", 0)
    positions = p.get("positions", [])
    pending = p.get("ordersForOpen", [])
    invested = sum(x.get("amount", 0) for x in positions)
    pnl = sum(x.get("unrealizedPnL", {}).get("pnL", 0) for x in positions)
    equity = cash + invested + pnl or 1
    return jsonify({
        "equity": round(equity, 2),
        "cash_pct": round(cash / equity * 100, 1),
        "positions": [{
            "positionID": x.get("positionID"),
            "instrumentID": x["instrumentID"],
            "name": NAMES.get(x["instrumentID"], f"ID:{x['instrumentID']}"),
            "amount": x["amount"],
            "pct": round(x["amount"] / equity * 100, 1)
        } for x in positions],
        "pending": [{
            "orderID": x.get("orderID"),
            "instrumentID": x.get("instrumentID"),
            "name": NAMES.get(x.get("instrumentID"), f"ID:{x.get('instrumentID')}"),
            "amount": x.get("amount")
        } for x in pending]
    })

@app.route("/api/search/<symbol>")
def search(symbol):
    r = requests.get(f"{BASE}/market-data/search?internalSymbolFull={symbol}", headers=etoro_headers(user_key()))
    items = r.json().get("items", [])
    match = next((i for i in items if i.get("internalSymbolFull") == symbol), None)
    if match:
        return jsonify({"instrumentId": match["instrumentId"], "name": match.get("instrumentName", symbol)})
    return jsonify({"error": f"Symbole '{symbol}' introuvable"}), 404

@app.route("/api/buy", methods=["POST"])
def buy():
    body = request.json
    r = requests.post(
        f"{BASE}/trading/execution/market-open-orders/by-amount",
        headers=etoro_headers(user_key()),
        json={"InstrumentID": body["instrumentId"], "IsBuy": True, "Leverage": 1, "Amount": body["amount"]}
    )
    return jsonify(r.json()), r.status_code

@app.route("/api/sell/<int:position_id>", methods=["POST"])
def sell(position_id):
    body = request.json
    r = requests.post(
        f"{BASE}/trading/execution/market-close-orders/positions/{position_id}",
        headers=etoro_headers(user_key()),
        json={"InstrumentId": body["instrumentId"], "UnitsToDeduct": None}
    )
    return jsonify(r.json()), r.status_code

@app.route("/api/cancel/<order_id>", methods=["DELETE"])
def cancel(order_id):
    r = requests.delete(
        f"{BASE}/trading/execution/market-open-orders/{order_id}",
        headers=etoro_headers(user_key())
    )
    return jsonify(r.json() if r.text else {"status": "cancelled"}), r.status_code

HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>eToro Agent Portfolio</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, sans-serif; background: #0f0f0f; color: #e0e0e0; min-height: 100vh; }

  /* Header */
  .header { background: #1a1a2e; padding: 16px; text-align: center; border-bottom: 1px solid #333; position: sticky; top: 0; z-index: 10; }
  .header h1 { font-size: 18px; color: #00d4aa; }
  .header .equity { font-size: 28px; font-weight: bold; margin-top: 4px; }
  .header .sub { font-size: 12px; color: #888; margin-top: 2px; }

  /* Key input */
  .key-bar { background: #1a1a1a; padding: 10px 16px; border-bottom: 1px solid #333; display: flex; gap: 8px; }
  .key-bar input { flex: 1; background: #2a2a2a; border: 1px solid #444; color: #e0e0e0; padding: 8px; border-radius: 8px; font-size: 11px; }
  .key-bar button { background: #00d4aa; color: #000; border: none; padding: 8px 14px; border-radius: 8px; font-weight: bold; font-size: 13px; white-space: nowrap; }

  /* Tabs */
  .tabs { display: flex; background: #1a1a1a; border-bottom: 1px solid #333; position: sticky; top: 73px; z-index: 9; }
  .tab { flex: 1; padding: 12px 4px; text-align: center; font-size: 12px; color: #888; cursor: pointer; border-bottom: 2px solid transparent; }
  .tab.active { color: #00d4aa; border-bottom-color: #00d4aa; }

  /* Sections */
  .section { display: none; padding: 16px; }
  .section.active { display: block; }

  /* Cards */
  .card { background: #1e1e1e; border-radius: 12px; padding: 14px; margin-bottom: 10px; border: 1px solid #2a2a2a; }
  .card-row { display: flex; justify-content: space-between; align-items: center; }
  .card-name { font-size: 16px; font-weight: 600; }
  .card-pct { font-size: 18px; font-weight: bold; color: #00d4aa; }
  .card-amount { font-size: 12px; color: #666; margin-top: 4px; }
  .bar { background: #2a2a2a; border-radius: 4px; height: 4px; margin-top: 8px; }
  .bar-fill { background: #00d4aa; height: 4px; border-radius: 4px; }

  /* Buttons */
  .btn { width: 100%; padding: 14px; border: none; border-radius: 10px; font-size: 16px; font-weight: bold; cursor: pointer; margin-top: 8px; }
  .btn-buy { background: #00d4aa; color: #000; }
  .btn-sell { background: #e74c3c; color: #fff; font-size: 13px; padding: 8px 14px; width: auto; margin-top: 0; border-radius: 8px; }
  .btn-cancel { background: #e67e22; color: #fff; font-size: 13px; padding: 8px 14px; width: auto; margin-top: 0; border-radius: 8px; }

  /* Form */
  .form-group { margin-bottom: 14px; }
  .form-group label { display: block; font-size: 12px; color: #888; margin-bottom: 6px; }
  .form-group input { width: 100%; background: #2a2a2a; border: 1px solid #444; color: #e0e0e0; padding: 12px; border-radius: 10px; font-size: 16px; }
  .symbol-result { background: #1e2e2e; border: 1px solid #00d4aa33; border-radius: 8px; padding: 10px; margin-top: 8px; font-size: 13px; color: #00d4aa; display: none; }

  /* Cash row */
  .cash-row { background: #1e1e1e; border-radius: 12px; padding: 14px; margin-bottom: 10px; border: 1px solid #2a2a2a; display: flex; justify-content: space-between; align-items: center; }
  .cash-label { color: #888; font-size: 14px; }
  .cash-pct { font-size: 18px; font-weight: bold; color: #f0c040; }

  /* Status */
  .status { padding: 12px; border-radius: 8px; margin-top: 12px; font-size: 14px; display: none; }
  .status.ok { background: #1e3a2a; color: #00d4aa; border: 1px solid #00d4aa44; }
  .status.err { background: #3a1e1e; color: #e74c3c; border: 1px solid #e74c3c44; }

  .pending-badge { font-size: 11px; background: #e67e2233; color: #e67e22; padding: 2px 7px; border-radius: 10px; }
  .loading { text-align: center; color: #555; padding: 40px; font-size: 14px; }
</style>
</head>
<body>

<div class="header">
  <h1>eToro Agent Portfolio</h1>
  <div class="equity" id="equity">—</div>
  <div class="sub" id="equity-sub">Chargement...</div>
</div>

<div class="key-bar">
  <input type="password" id="user-key" placeholder="Clé agent-portfolio..." />
  <button onclick="loadPortfolio()">OK</button>
</div>

<div class="tabs">
  <div class="tab active" onclick="showTab('portfolio')">Portfolio</div>
  <div class="tab" onclick="showTab('buy')">Acheter</div>
  <div class="tab" onclick="showTab('sell')">Vendre</div>
  <div class="tab" onclick="showTab('pending')">Pending</div>
</div>

<!-- PORTFOLIO -->
<div class="section active" id="tab-portfolio">
  <div class="loading" id="portfolio-loading">Entrez votre clé pour charger le portfolio.</div>
  <div id="portfolio-content"></div>
</div>

<!-- BUY -->
<div class="section" id="tab-buy">
  <div class="form-group">
    <label>Symbole (ex: GLD, SLV, AAPL)</label>
    <input type="text" id="buy-symbol" placeholder="Symbole..." oninput="this.value=this.value.toUpperCase()" />
    <div class="symbol-result" id="symbol-result"></div>
  </div>
  <div class="form-group">
    <label>Montant en USD</label>
    <input type="number" id="buy-amount" placeholder="Ex: 500" />
  </div>
  <button class="btn btn-buy" onclick="searchAndBuy()">Rechercher & Acheter</button>
  <div class="status" id="buy-status"></div>
</div>

<!-- SELL -->
<div class="section" id="tab-sell">
  <div class="loading" id="sell-loading">Chargez le portfolio d'abord.</div>
  <div id="sell-content"></div>
</div>

<!-- PENDING -->
<div class="section" id="tab-pending">
  <div class="loading" id="pending-loading">Chargez le portfolio d'abord.</div>
  <div id="pending-content"></div>
</div>

<script>
let portfolioData = null;
let resolvedId = null;

function getKey() {
  const k = localStorage.getItem('etoro_key') || document.getElementById('user-key').value;
  document.getElementById('user-key').value = k;
  return k;
}

function saveKey(k) { localStorage.setItem('etoro_key', k); }

function apiHeaders() {
  return { 'Content-Type': 'application/json', 'X-User-Key': getKey() };
}

function showTab(name) {
  document.querySelectorAll('.tab').forEach((t, i) => {
    t.classList.toggle('active', ['portfolio','buy','sell','pending'][i] === name);
  });
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
}

async function loadPortfolio() {
  const key = document.getElementById('user-key').value.trim();
  if (!key) return;
  saveKey(key);
  document.getElementById('portfolio-loading').textContent = 'Chargement...';
  document.getElementById('portfolio-content').innerHTML = '';
  try {
    const r = await fetch('/api/portfolio', { headers: apiHeaders() });
    const text = await r.text();
    if (!r.ok) {
      document.getElementById('portfolio-loading').textContent = 'Erreur HTTP ' + r.status + ': ' + text;
      return;
    }
    portfolioData = JSON.parse(text);
    renderPortfolio();
    renderSell();
    renderPending();
  } catch(e) {
    document.getElementById('portfolio-loading').textContent = 'Erreur: ' + e.message + ' — Vérifiez que Flask tourne dans Termux.';
  }
}

function renderPortfolio() {
  document.getElementById('portfolio-loading').style.display = 'none';
  document.getElementById('equity').textContent = '$' + portfolioData.equity.toLocaleString();
  document.getElementById('equity-sub').textContent = portfolioData.positions.length + ' positions · ' + portfolioData.pending.length + ' pending';

  let html = `<div class="cash-row"><span class="cash-label">Cash disponible</span><span class="cash-pct">${portfolioData.cash_pct}%</span></div>`;
  for (const p of portfolioData.positions) {
    html += `<div class="card">
      <div class="card-row">
        <span class="card-name">${p.name}</span>
        <span class="card-pct">${p.pct}%</span>
      </div>
      <div class="card-amount">$${p.amount.toLocaleString()}</div>
      <div class="bar"><div class="bar-fill" style="width:${Math.min(p.pct,100)}%"></div></div>
    </div>`;
  }
  document.getElementById('portfolio-content').innerHTML = html;
}

function renderSell() {
  document.getElementById('sell-loading').style.display = 'none';
  if (!portfolioData.positions.length) {
    document.getElementById('sell-content').innerHTML = '<div class="loading">Aucune position ouverte.</div>';
    return;
  }
  let html = '';
  for (const p of portfolioData.positions) {
    html += `<div class="card">
      <div class="card-row">
        <div>
          <div class="card-name">${p.name}</div>
          <div class="card-amount">${p.pct}% · $${p.amount.toLocaleString()}</div>
        </div>
        <button class="btn-sell" onclick="closePo(${p.positionID},${p.instrumentID},'${p.name}')">Vendre</button>
      </div>
    </div>`;
  }
  document.getElementById('sell-content').innerHTML = html;
}

function renderPending() {
  document.getElementById('pending-loading').style.display = 'none';
  if (!portfolioData.pending.length) {
    document.getElementById('pending-content').innerHTML = '<div class="loading">Aucun ordre en attente.</div>';
    return;
  }
  let html = '';
  for (const o of portfolioData.pending) {
    html += `<div class="card">
      <div class="card-row">
        <div>
          <div class="card-name">${o.name} <span class="pending-badge">PENDING</span></div>
          <div class="card-amount">$${o.amount}</div>
        </div>
        <button class="btn-cancel" onclick="cancelOrder('${o.orderID}','${o.name}')">Annuler</button>
      </div>
    </div>`;
  }
  document.getElementById('pending-content').innerHTML = html;
}

async function searchAndBuy() {
  const symbol = document.getElementById('buy-symbol').value.trim();
  const amount = parseFloat(document.getElementById('buy-amount').value);
  const statusEl = document.getElementById('buy-status');
  const resultEl = document.getElementById('symbol-result');

  if (!symbol || !amount) { showStatus(statusEl, 'Remplissez le symbole et le montant.', false); return; }

  // Search
  resultEl.style.display = 'block';
  resultEl.textContent = 'Recherche de ' + symbol + '...';
  try {
    const sr = await fetch('/api/search/' + symbol, { headers: apiHeaders() });
    const sd = await sr.json();
    if (!sr.ok) { resultEl.textContent = sd.error; showStatus(statusEl, sd.error, false); return; }
    resolvedId = sd.instrumentId;
    resultEl.textContent = '✓ Trouvé: ' + (sd.name || symbol) + ' (ID: ' + sd.instrumentId + ')';

    // Buy
    showStatus(statusEl, 'Envoi de l\'ordre...', true);
    const br = await fetch('/api/buy', {
      method: 'POST', headers: apiHeaders(),
      body: JSON.stringify({ instrumentId: resolvedId, amount })
    });
    const bd = await br.json();
    if (br.ok) {
      showStatus(statusEl, '✓ Ordre envoyé pour ' + symbol + ' — $' + amount, true);
      loadPortfolio();
    } else {
      showStatus(statusEl, 'Erreur: ' + JSON.stringify(bd), false);
    }
  } catch(e) {
    showStatus(statusEl, 'Erreur: ' + e.message, false);
  }
}

async function closePo(positionId, instrumentId, name) {
  if (!confirm('Fermer la position ' + name + ' ?')) return;
  const r = await fetch('/api/sell/' + positionId, {
    method: 'POST', headers: apiHeaders(),
    body: JSON.stringify({ instrumentId })
  });
  const d = await r.json();
  alert(r.ok ? '✓ Position ' + name + ' fermée.' : 'Erreur: ' + JSON.stringify(d));
  if (r.ok) loadPortfolio();
}

async function cancelOrder(orderId, name) {
  if (!confirm('Annuler l\'ordre ' + name + ' ?')) return;
  const r = await fetch('/api/cancel/' + orderId, { method: 'DELETE', headers: apiHeaders() });
  const d = await r.json();
  alert(r.ok ? '✓ Ordre ' + name + ' annulé.' : 'Erreur: ' + JSON.stringify(d));
  if (r.ok) loadPortfolio();
}

function showStatus(el, msg, ok) {
  el.textContent = msg;
  el.className = 'status ' + (ok ? 'ok' : 'err');
  el.style.display = 'block';
}

// Restore key from localStorage
window.onload = () => {
  const k = localStorage.getItem('etoro_key');
  if (k) { document.getElementById('user-key').value = k; loadPortfolio(); }
};
</script>
</body>
</html>"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
