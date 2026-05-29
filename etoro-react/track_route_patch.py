# Remplacer la route /api/track/<username> dans ~/app.py par ce code :

@app.route("/api/track/<username>")
def track_user(username):
    try:
        # 1. Récupérer le CID
        r = requests.get(
            f"https://www.etoro.com/api/logininfo/v1.1/users/{username}",
            timeout=12
        )
        r.raise_for_status()
        cid = r.json()["realCID"]

        # 2. Récupérer le portfolio public
        r = requests.get(
            f"https://www.etoro.com/sapi/trade-data-real/live/public/portfolios?cid={cid}",
            timeout=12
        )
        r.raise_for_status()
        data = r.json()

        agg_positions = data.get("AggregatedPositions", [])
        ind_positions = data.get("Positions", [])

        # 3. Récupérer les noms des instruments
        all_ids = list({p["InstrumentID"] for p in agg_positions + ind_positions})
        r = requests.get(
            f"https://api.etorostatic.com/sapi/instrumentsmetadata/V1.1/instruments?ids={','.join(map(str, all_ids))}",
            timeout=12
        )
        meta = {
            m["InstrumentID"]: m["SymbolFull"]
            for m in r.json().get("InstrumentDisplayDatas", [])
        }

        # 4. Grouper les positions individuelles par instrument
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

        # 5. Construire la liste finale
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
