#!/usr/bin/env python3
"""
Run from Termux: python3 ~/DataEngineering/etoro-react/patch_app.py
Patches ~/app.py — track_user route with snapshot-diff history
"""
import re, shutil, sys
from pathlib import Path

APP = Path.home() / "app.py"

if not APP.exists():
    print(f"Erreur : {APP} introuvable"); sys.exit(1)

shutil.copy(APP, APP.with_suffix(".py.bak"))
print(f"Backup : {APP}.bak")

src = APP.read_text()

NEW_ROUTE = '''@app.route("/api/track/<username>")
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
        events = load_events()
        key = username.lower()
        user = events.get(key, {"last": {}, "history": []})
        last_map = user.get("last", {})
        today = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

        new_events = []
        # Positions ouvertes depuis la dernière visite
        for iid, p in current_map.items():
            siid = str(iid)
            if siid not in last_map:
                new_events.append({
                    "name":   p["name"],
                    "action": "open",
                    "isBuy":  p["isBuy"],
                    "amount": p["amount"],
                    "date":   today,
                })
        # Positions fermées depuis la dernière visite
        for siid, p in last_map.items():
            if int(siid) not in current_map:
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
        user["last"]    = {str(iid): p for iid, p in current_map.items()}
        user["history"] = history[:50]
        events[key] = user
        save_events(events)

        return jsonify({"positions": positions, "history": history[:20]})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
'''

pattern = r'@app\.route\("/api/track/<username>"\)\s*\ndef track_user\(username\):.*?(?=\n@app\.route|\nif __name__|\Z)'
if not re.search(pattern, src, re.DOTALL):
    print("Route /api/track/<username> introuvable dans app.py")
    sys.exit(1)

patched = re.sub(pattern, NEW_ROUTE.strip(), src, flags=re.DOTALL)
APP.write_text(patched)
print("Patch appliqué.")
print("Redémarre Flask : pkill -f 'python.*app.py' && python3 ~/app.py &")
