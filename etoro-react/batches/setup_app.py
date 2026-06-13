#!/usr/bin/env python3
"""
Run from Termux: python3 ~/DataEngineering/etoro-react/batches/setup_app.py
Applies all patches to ~/app.py in one shot:
  1. /api/track route (patch_app.py)
  2. /api/config routes (patch_config.py)
  3. apply_portfolio_ratio after_request (patch_portfolio_ratio.py)
  4. log_transaction after_request + /api/transactions (patch_transactions.py)
"""
import re, shutil, sys
from pathlib import Path

APP = Path.home() / "app.py"

def main():
    if not APP.exists():
        print(f"Erreur : {APP} introuvable"); sys.exit(1)

    shutil.copy(APP, APP.with_suffix(".py.bak"))
    print(f"Backup : {APP}.bak")

    src = APP.read_text()
    changed = False

    # ── Patch 1 : /api/track/<username> ──────────────────────────────────────
    print("\n[1/4] Patch /api/track/<username>...")
    NEW_TRACK_ROUTE = '''@app.route("/api/track/<username>")
def track_user(username):
    try:
        import json as _json
        from datetime import datetime
        from pathlib import Path

        EVENTS_FILE = Path.home() / "etoro_track_events.json"

        def load_events():
            try: return _json.loads(EVENTS_FILE.read_text())
            except: return {}

        def save_events(data):
            EVENTS_FILE.write_text(_json.dumps(data, ensure_ascii=False))

        # 1. CID
        r = requests.get(
            f"https://www.etoro.com/api/logininfo/v1.1/users/{username}",
            timeout=12
        )
        r.raise_for_status()
        cid = r.json()["realCID"]

        # 2. Positions ouvertes
        r = requests.get(
            f"https://www.etoro.com/sapi/trade-data-real/live/public/portfolios?cid={cid}",
            timeout=12
        )
        r.raise_for_status()
        data = r.json()
        agg = data.get("AggregatedPositions", [])

        # 3. Noms des instruments
        ids = [p["InstrumentID"] for p in agg]
        r = requests.get(
            f"https://api.etorostatic.com/sapi/instrumentsmetadata/V1.1/instruments?ids={','.join(map(str,ids))}",
            timeout=12
        )
        meta = {m["InstrumentID"]: m["SymbolFull"]
                for m in r.json().get("InstrumentDisplayDatas", [])}

        # 4. Positions actuelles
        positions = []
        current_map = {}
        for p in agg:
            iid = p["InstrumentID"]
            entry = {
                "iid":    iid,
                "name":   meta.get(iid, f"#{iid}"),
                "amount": round(p.get("Value", 0), 2),
                "pnl":    round(p.get("NetProfit", 0), 2),
                "isBuy":  p.get("Direction") != "Sell",
            }
            current_map[iid] = entry
            positions.append({k: v for k, v in entry.items() if k != "iid"})
        positions.sort(key=lambda x: -x["amount"])

        # 5. Diff avec le snapshot précédent
        events   = load_events()
        key      = username.lower()
        user     = events.get(key, {"last": {}, "history": []})
        last_map = user.get("last", {})
        today    = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

        new_events = []

        # Instruments ouverts depuis la dernière visite
        for iid, p in current_map.items():
            if str(iid) not in last_map:
                new_events.append({
                    "name":   p["name"],
                    "action": "open",
                    "isBuy":  p["isBuy"],
                    "amount": p["amount"],
                    "date":   today,
                })

        # Instruments fermés depuis la dernière visite
        for siid, p in last_map.items():
            if siid not in {str(k) for k in current_map}:
                new_events.append({
                    "name":   p["name"],
                    "action": "close",
                    "isBuy":  p["isBuy"],
                    "date":   today,
                })

        history = user.get("history", [])
        if new_events:
            history = new_events + history

        # Sauvegarde snapshot
        user["last"]    = {str(iid): {"name": p["name"], "amount": p["amount"], "isBuy": p["isBuy"]}
                           for iid, p in current_map.items()}
        user["history"] = history[:100]
        events[key] = user
        save_events(events)

        return jsonify({"positions": positions, "history": history[:30]})

    except Exception as e:
        return jsonify({"error": str(e)}), 500'''

    pattern = r'@app\.route\("/api/track/<username>"\)\s*\ndef track_user\(username\):.*?(?=\n@app\.route|\nif __name__|\Z)'
    if re.search(pattern, src, re.DOTALL):
        src = re.sub(pattern, NEW_TRACK_ROUTE.strip(), src, flags=re.DOTALL)
        print("  -> Route /api/track remplacée.")
        changed = True
    else:
        print("  -> Route /api/track introuvable, patch ignoré.")

    # ── Patch 2 : /api/config ────────────────────────────────────────────────
    print("\n[2/4] Patch /api/config...")
    if '/api/config' in src:
        print("  -> Route /api/config déjà présente — rien à faire.")
    else:
        CONFIG_ROUTES = '''
@app.route("/api/config", methods=["GET"])
def get_config():
    import json as _json
    from pathlib import Path
    cfg_file = Path.home() / "etoro_config.json"
    try:
        cfg = _json.loads(cfg_file.read_text())
    except:
        cfg = {"ratio": 1.0}
    return jsonify(cfg)

@app.route("/api/config", methods=["POST"])
def set_config():
    import json as _json
    from pathlib import Path
    cfg_file = Path.home() / "etoro_config.json"
    data = request.get_json(force=True)
    ratio = float(data.get("ratio", 1.0))
    if ratio <= 0 or ratio > 100:
        return jsonify({"error": "ratio must be between 0 and 100"}), 400
    cfg_file.write_text(_json.dumps({"ratio": ratio}))
    return jsonify({"ok": True, "ratio": ratio})
'''
        insert_before = '\nif __name__'
        if insert_before in src:
            src = src.replace(insert_before, '\n' + CONFIG_ROUTES + insert_before, 1)
        else:
            matches = list(re.finditer(r'\n@app\.route', src))
            if matches:
                pos = matches[-1].start()
                src = src[:pos] + '\n' + CONFIG_ROUTES + src[pos:]
            else:
                src = src + '\n' + CONFIG_ROUTES
        print("  -> Routes /api/config ajoutées.")
        changed = True

    # ── Patch 3 : apply_portfolio_ratio ──────────────────────────────────────
    print("\n[3/4] Patch apply_portfolio_ratio...")
    if 'apply_portfolio_ratio' in src:
        print("  -> Hook apply_portfolio_ratio déjà présent — rien à faire.")
    else:
        RATIO_HOOK = '''
@app.after_request
def apply_portfolio_ratio(response):
    import json as _json
    from pathlib import Path as _Path
    if request.path != "/api/portfolio":
        return response
    if "application/json" not in response.content_type:
        return response
    try:
        cfg_file = _Path.home() / "etoro_config.json"
        ratio = float(_json.loads(cfg_file.read_text()).get("ratio", 1.0))
    except Exception:
        ratio = 1.0
    if ratio == 1.0:
        return response
    try:
        data = _json.loads(response.get_data(as_text=True))
        _fields = {"equity", "cash", "amount", "pnl"}

        def _mul(obj):
            if isinstance(obj, dict):
                return {
                    k: round(v * ratio, 2) if k in _fields and isinstance(v, (int, float)) else _mul(v)
                    for k, v in obj.items()
                }
            if isinstance(obj, list):
                return [_mul(item) for item in obj]
            return obj

        response.set_data(_json.dumps(_mul(data)))
    except Exception:
        pass
    return response
'''
        insert_before = '\nif __name__'
        if insert_before in src:
            src = src.replace(insert_before, '\n' + RATIO_HOOK + insert_before, 1)
        else:
            matches = list(re.finditer(r'\n@app\.route', src))
            if matches:
                pos = matches[-1].start()
                src = src[:pos] + '\n' + RATIO_HOOK + src[pos:]
            else:
                src = src + '\n' + RATIO_HOOK
        print("  -> Hook apply_portfolio_ratio ajouté.")
        changed = True

    # ── Patch 4 : log_transaction + /api/transactions ────────────────────────
    print("\n[4/4] Patch log_transaction + /api/transactions...")
    if 'log_transaction' in src:
        print("  -> Hook log_transaction déjà présent — rien à faire.")
    else:
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
                "detail":  order,
            })
        elif path.startswith("/api/sell/") and method == "POST":
            resp = _txjson.loads(response.get_data(as_text=True))
            _tx_append({
                "date":       _txdt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "type":       "sell",
                "positionId": path.split("/")[-1],
                "status":     "error" if resp.get("error") else "ok",
                "error":      resp.get("error"),
                "detail":     resp,
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
        print("  -> Patch log_transaction appliqué.")
        changed = True

    # ── Write result ──────────────────────────────────────────────────────────
    if changed:
        APP.write_text(src)
        print(f"\nTous les patches appliqués à {APP}")
    else:
        print("\nAucun patch nécessaire — app.py est déjà à jour.")

    print("Redémarre Flask : pkill -f 'python.*app.py' && python3 ~/app.py &")

if __name__ == "__main__":
    main()
