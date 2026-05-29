#!/usr/bin/env python3
"""
Run from Termux: python3 ~/DataEngineering/etoro-react/patch_app.py
Patches ~/app.py to add sub-positions support in /api/track/<username>
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
        r = requests.get(
            f"https://www.etoro.com/api/logininfo/v1.1/users/{username}",
            timeout=12
        )
        r.raise_for_status()
        cid = r.json()["realCID"]

        r = requests.get(
            f"https://www.etoro.com/sapi/trade-data-real/live/public/portfolios?cid={cid}",
            timeout=12
        )
        r.raise_for_status()
        data = r.json()

        agg_positions = data.get("AggregatedPositions", [])
        ind_positions = data.get("Positions", [])

        all_ids = list({p["InstrumentID"] for p in agg_positions + ind_positions})
        r = requests.get(
            f"https://api.etorostatic.com/sapi/instrumentsmetadata/V1.1/instruments?ids={','.join(map(str, all_ids))}",
            timeout=12
        )
        meta = {
            m["InstrumentID"]: m["SymbolFull"]
            for m in r.json().get("InstrumentDisplayDatas", [])
        }

        from collections import defaultdict
        grouped = defaultdict(list)
        for p in ind_positions:
            iid = p.get("InstrumentID")
            open_dt = p.get("OpenDateTime", "")
            grouped[iid].append({
                "positionID": p.get("PositionID"),
                "amount":     round(p.get("Value", 0), 2),
                "pnl":        round(p.get("NetProfit", 0), 2),
                "invested":   round(p.get("Invested", 0), 2),
                "isBuy":      p.get("IsBuy", True),
                "openDate":   open_dt[:10] if open_dt else "",
                "openRate":   p.get("OpenRate", 0),
                "closeRate":  p.get("CurrentRate", 0),
                "leverage":   p.get("Leverage", 1),
            })

        positions = []
        for p in agg_positions:
            iid = p["InstrumentID"]
            subs = grouped.get(iid, [])
            positions.append({
                "name":   meta.get(iid, f"#{iid}"),
                "amount": round(p.get("Value", 0), 2),
                "pnl":    round(p.get("NetProfit", 0), 2),
                "isBuy":  p.get("Direction") != "Sell",
                "sub":    subs,
            })

        positions.sort(key=lambda x: -x["amount"])
        return jsonify({"positions": positions})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
'''

# Replace the entire track_user function
pattern = r'@app\.route\("/api/track/<username>"\)\s*\ndef track_user\(username\):.*?(?=\n@app\.route|\nif __name__|\Z)'
if not re.search(pattern, src, re.DOTALL):
    print("Route /api/track/<username> introuvable dans app.py")
    sys.exit(1)

patched = re.sub(pattern, NEW_ROUTE.strip(), src, flags=re.DOTALL)
APP.write_text(patched)
print("Patch appliqué avec succès.")
print("Redémarre Flask : pkill -f 'python.*app.py' && python3 ~/app.py &")
