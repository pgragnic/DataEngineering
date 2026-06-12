#!/usr/bin/env python3
"""
Run from Termux: python3 ~/DataEngineering/etoro-react/batches/patch_transactions.py
Ajoute dans ~/app.py :
  - un after_request qui logue chaque buy/sell dans
    ~/DataEngineering/etoro-react/data/etoro_transactions.json
  - GET /api/transactions pour lire ce fichier
"""
import re, shutil, sys
from pathlib import Path

APP = Path.home() / "app.py"
if not APP.exists():
    print(f"Erreur : {APP} introuvable"); sys.exit(1)

shutil.copy(APP, APP.with_suffix(".py.bak"))
print(f"Backup : {APP}.bak")

src = APP.read_text()

if 'log_transaction' in src:
    print("Hook log_transaction déjà présent — rien à faire.")
    sys.exit(0)

TX_CODE = '''
# ── Transaction logger ───────────────────────────────────────────────────────
import json as _txjson
from pathlib import Path as _TXPath
from datetime import datetime as _txdt

_TX_FILE = _TXPath.home() / "DataEngineering/etoro-react/data/etoro_transactions.json"

def _tx_append(entry):
    try:
        _TX_FILE.parent.mkdir(parents=True, exist_ok=True)
        existing = _txjson.loads(_TX_FILE.read_text()) if _TX_FILE.exists() else []
    except Exception:
        existing = []
    existing.insert(0, entry)
    _TX_FILE.write_text(_txjson.dumps(existing, indent=2))

@app.after_request
def log_transaction(response):
    path   = request.path
    method = request.method
    try:
        if path == "/api/buy" and method == "POST":
            req  = request.get_json(force=True, silent=True) or {}
            resp = _txjson.loads(response.get_data(as_text=True))
            order = resp.get("orderForOpen") or {}
            _tx_append({
                "date":    _txdt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "type":    "buy",
                "symbol":  req.get("symbol", "?"),
                "pct":     req.get("pct"),
                "amount":  order.get("amount"),
                "orderId": order.get("orderID") or order.get("orderId"),
                "status":  "error" if resp.get("error") else "ok",
                "error":   resp.get("error"),
            })
        elif path.startswith("/api/sell/") and method == "POST":
            resp = _txjson.loads(response.get_data(as_text=True))
            _tx_append({
                "date":       _txdt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "type":       "sell",
                "positionId": path.split("/")[-1],
                "status":     "error" if resp.get("error") else "ok",
                "error":      resp.get("error"),
            })
    except Exception:
        pass
    return response

@app.route("/api/transactions", methods=["GET"])
def get_transactions():
    try:
        txs = _txjson.loads(_TX_FILE.read_text()) if _TX_FILE.exists() else []
    except Exception:
        txs = []
    limit = request.args.get("limit", 200, type=int)
    return jsonify(txs[:limit])
# ─────────────────────────────────────────────────────────────────────────────
'''

insert_before = '\nif __name__'
if insert_before in src:
    src = src.replace(insert_before, '\n' + TX_CODE + insert_before, 1)
else:
    matches = list(re.finditer(r'\n@app\.route', src))
    if matches:
        pos = matches[-1].start()
        src = src[:pos] + '\n' + TX_CODE + src[pos:]
    else:
        src += '\n' + TX_CODE

APP.write_text(src)
print("Patch log_transaction appliqué.")
print("Redémarre Flask : pkill -f 'python.*app.py' && python3 ~/app.py &")
