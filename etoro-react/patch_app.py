#!/usr/bin/env python3
"""
Run from Termux: python3 ~/DataEngineering/etoro-react/patch_app.py
Patches ~/app.py — track_user route with recent trades history
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
        from datetime import datetime, timedelta

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
        agg_positions = data.get("AggregatedPositions", [])

        # 3. Historique récent (90 derniers jours)
        start = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%S")
        end   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        rh = requests.get(
            "https://www.etoro.com/sapi/trade-data-real/history/public/credit/flat",
            params={"cid": cid, "startTime": start, "endTime": end,
                    "pageNumber": 1, "pageSize": 40},
            timeout=12
        )
        history_items = []
        if rh.status_code == 200:
            hdata = rh.json()
            # response peut être une liste ou {"items": [...]}
            if isinstance(hdata, list):
                history_items = hdata
            elif isinstance(hdata, dict):
                for key in ("items", "Items", "PositionActions", "data", "Data"):
                    if key in hdata and isinstance(hdata[key], list):
                        history_items = hdata[key]
                        break
                if not history_items:
                    history_items = [hdata]  # debug fallback

        # 4. Noms des instruments
        hist_ids = list({p.get("InstrumentID") or p.get("instrumentId") or 0
                         for p in history_items} - {0})
        all_ids  = list({p["InstrumentID"] for p in agg_positions} | set(hist_ids))
        r = requests.get(
            f"https://api.etorostatic.com/sapi/instrumentsmetadata/V1.1/instruments?ids={','.join(map(str, all_ids))}",
            timeout=12
        )
        meta = {
            m["InstrumentID"]: m["SymbolFull"]
            for m in r.json().get("InstrumentDisplayDatas", [])
        }

        # 5. Positions ouvertes
        positions = []
        for p in agg_positions:
            iid = p["InstrumentID"]
            positions.append({
                "name":   meta.get(iid, f"#{iid}"),
                "amount": round(p.get("Value", 0), 2),
                "pnl":    round(p.get("NetProfit", 0), 2),
                "isBuy":  p.get("Direction") != "Sell",
            })
        positions.sort(key=lambda x: -x["amount"])

        # 6. Historique
        history = []
        for h in history_items[:30]:
            iid    = h.get("InstrumentID") or h.get("instrumentId")
            action = h.get("Action") or h.get("action") or ""
            dt     = h.get("CloseDateTime") or h.get("OpenDateTime") or h.get("Date") or h.get("date") or ""
            pnl    = h.get("NetProfit") or h.get("netProfit") or h.get("PNL") or 0
            history.append({
                "name":   meta.get(iid, f"#{iid}") if iid else "?",
                "action": action,
                "date":   dt[:10] if dt else "",
                "pnl":    round(float(pnl), 2),
                "raw":    h,  # debug — à retirer après validation
            })

        return jsonify({"positions": positions, "history": history, "_histKeys": list(history_items[0].keys()) if history_items else []})

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
