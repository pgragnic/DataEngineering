import os
import uuid
import requests
from flask import Flask, jsonify, request
from mangum import Mangum

app = Flask(__name__)

AGENT_KEY  = os.environ["ETORO_AGENT_KEY"]
API_KEY    = os.environ["ETORO_API_KEY"]
INVESTMENT = float(os.environ.get("ETORO_INVESTMENT", "14892"))
BASE       = "https://public-api.etoro.com/api/v1"
RATIO      = INVESTMENT / 10000.0


def hdrs():
    return {
        "x-api-key":     API_KEY,
        "x-user-key":    AGENT_KEY,
        "x-request-id":  str(uuid.uuid4()),
        "Content-Type":  "application/json",
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
                names[iid] = (
                    inst.get("symbolFull")
                    or inst.get("instrumentDisplayName")
                    or f"ID:{iid}"
                )
    except Exception:
        pass
    return names


@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"]  = os.environ.get("ALLOWED_ORIGIN", "*")
    r.headers["Access-Control-Allow-Methods"] = "GET,POST,DELETE,OPTIONS"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return r


@app.route("/api/portfolio")
def portfolio():
    try:
        r  = requests.get(f"{BASE}/trading/info/real/pnl", headers=hdrs(), timeout=15)
        cp = r.json().get("clientPortfolio", {})

        credit       = float(cp.get("credit", 0))
        positions    = cp.get("positions", []) or []
        orders_open  = cp.get("ordersForOpen", []) or []
        orders_close = cp.get("orders", []) or []

        all_ids = list(set(
            [p["instrumentID"] for p in positions    if p.get("instrumentID")] +
            [o["instrumentID"] for o in orders_open if o.get("instrumentID")]
        ))
        names = resolve_names(all_ids)

        pend_open  = sum(float(o.get("amount", 0)) for o in orders_open  if o.get("mirrorID", 1) == 0)
        pend_close = sum(float(o.get("amount", 0)) for o in orders_close)
        cash       = credit - pend_open - pend_close

        total_inv  = sum(float(p.get("amount", 0)) for p in positions) + pend_open + pend_close
        upnl       = sum(float((p.get("unrealizedPnL") or {}).get("pnL", 0)) for p in positions)
        equity     = cash + total_inv + upnl

        result_positions = sorted([{
            "positionID":  p.get("positionID"),
            "instrumentID": p.get("instrumentID"),
            "name":        names.get(p.get("instrumentID"), f"ID:{p.get('instrumentID')}"),
            "amount":      round(float(p.get("amount", 0)) * RATIO, 2),
            "pnl":         round(float((p.get("unrealizedPnL") or {}).get("pnL", 0)) * RATIO, 2),
            "units":       round(float(p.get("units", 0)), 4),
            "openRate":    float(p.get("openRate", 0)),
            "closeRate":   float((p.get("unrealizedPnL") or {}).get("closeRate", 0)),
            "openDate":    str(p.get("openDateTime", ""))[:10],
            "isBuy":       p.get("isBuy", True),
        } for p in positions], key=lambda x: -x["amount"])

        pending_list = [{
            "orderId":     o.get("orderId"),
            "instrumentID": o.get("instrumentID"),
            "name":        names.get(o.get("instrumentID"), f"ID:{o.get('instrumentID')}"),
            "amount":      round(float(o.get("amount", 0)) * RATIO, 2),
        } for o in orders_open if o.get("mirrorID", 1) == 0]

        return jsonify({
            "equity":       round(equity   * RATIO, 2),
            "cash":         round(cash     * RATIO, 2),
            "positions":    result_positions,
            "pending":      pending_list,
            "count":        len(result_positions),
            "pendingCount": len(pending_list),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/search/<symbol>")
def search(symbol):
    try:
        r     = requests.get(
            f"{BASE}/market-data/search?internalSymbolFull={symbol}",
            headers=hdrs(), timeout=10)
        items = r.json().get("items", []) or []
        match = next((i for i in items if i.get("internalSymbolFull") == symbol), None)
        if match:
            return jsonify({
                "instrumentId": match["instrumentId"],
                "name":         match.get("instrumentDisplayName", symbol),
                "symbol":       symbol,
            })
        return jsonify({"error": "Symbole non trouvé"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/buy", methods=["POST", "OPTIONS"])
def buy():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    try:
        body   = request.json or {}
        symbol = body.get("symbol", "").upper()
        pct    = float(body.get("pct", 0))

        r  = requests.get(f"{BASE}/trading/info/real/pnl", headers=hdrs(), timeout=15)
        cp = r.json().get("clientPortfolio", {})

        credit     = float(cp.get("credit", 0))
        pos        = cp.get("positions", []) or []
        oopen      = cp.get("ordersForOpen", []) or []
        oclose     = cp.get("orders", []) or []
        pend_open  = sum(float(o.get("amount", 0)) for o in oopen  if o.get("mirrorID", 1) == 0)
        pend_close = sum(float(o.get("amount", 0)) for o in oclose)
        avail      = credit - pend_open - pend_close
        inv        = sum(float(p.get("amount", 0)) for p in pos) + pend_open + pend_close
        upnl       = sum(float((p.get("unrealizedPnL") or {}).get("pnL", 0)) for p in pos)
        equity_v   = avail + inv + upnl
        amount     = round(equity_v * pct / 100, 2)

        sr    = requests.get(
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


@app.route("/api/sell/<int:pid>", methods=["POST", "OPTIONS"])
def sell(pid):
    if request.method == "OPTIONS":
        return jsonify({}), 200
    try:
        r         = requests.get(f"{BASE}/trading/info/real/pnl", headers=hdrs(), timeout=15)
        positions = r.json().get("clientPortfolio", {}).get("positions", []) or []
        pos       = next((p for p in positions if p.get("positionID") == pid), None)
        if not pos:
            return jsonify({"error": "Position non trouvée"}), 404
        payload = {"InstrumentId": pos.get("instrumentID"), "UnitsToDeduct": None}
        r2 = requests.post(
            f"{BASE}/trading/execution/market-close-orders/positions/{pid}",
            headers=hdrs(), json=payload, timeout=15)
        return jsonify(r2.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cancel/<oid>", methods=["DELETE", "OPTIONS"])
def cancel(oid):
    if request.method == "OPTIONS":
        return jsonify({}), 200
    try:
        r = requests.delete(
            f"{BASE}/trading/execution/market-open-orders/{oid}",
            headers=hdrs(), timeout=15)
        return jsonify(r.json() if r.content else {"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Point d'entrée Lambda
handler = Mangum(app, lifespan="off")

# Dev local uniquement
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
