#!/usr/bin/env python3
"""
Envoie une série d'ordres d'achat à l'API Flask locale.
Usage : python3 ~/DataEngineering/etoro-react/buy_batch.py [--dry-run]

Modifie la liste ORDERS ci-dessous avant d'exécuter.
"""
import json, sys, time, urllib.request, urllib.error

API = "http://127.0.0.1:5000"
DRY_RUN = "--dry-run" in sys.argv

# ── Ordres à passer ─────────────────────────────────────────────────────────
# Chaque ordre : { "symbol": "TICKER", "pct": <% du portefeuille> }
# Source : portfolio ThomasPJ au 07/06/2026
ORDERS = [
    {"symbol": "BAC", "pct": 0.50},
    {"symbol": "BAC", "pct": 0.63},
    {"symbol": "BAC", "pct": 0.25},
    {"symbol": "BAC", "pct": 0.50},
    {"symbol": "BAC", "pct": 0.50},
    {"symbol": "BAC", "pct": 0.50},
    {"symbol": "BAC", "pct": 0.50},
    {"symbol": "BAC", "pct": 0.50},
    {"symbol": "BAC", "pct": 0.50},
    {"symbol": "BAC", "pct": 0.62},
]

DELAY_SECONDS = 2   # délai entre chaque ordre (évite le rate-limit)
# ────────────────────────────────────────────────────────────────────────────

def buy(symbol, pct):
    url  = f"{API}/api/buy"
    body = json.dumps({"symbol": symbol, "pct": pct}).encode()
    req  = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())

total = len(ORDERS)
print(f"{'[DRY-RUN] ' if DRY_RUN else ''}{total} ordres à passer — délai {DELAY_SECONDS}s entre chaque\n")

ok_count  = 0
err_count = 0

for i, order in enumerate(ORDERS, 1):
    sym = order["symbol"]
    pct = order["pct"]
    print(f"[{i}/{total}] BUY {sym}  {pct}%", end="  →  ", flush=True)

    if DRY_RUN:
        print("skipped (dry-run)")
        continue

    try:
        result = buy(sym, pct)
        if result.get("error"):
            print(f"ERREUR : {result['error']}")
            err_count += 1
        else:
            print(f"OK  {result}")
            ok_count += 1
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"HTTP {e.code} : {body}")
        err_count += 1
    except Exception as e:
        print(f"Exception : {e}")
        err_count += 1

    if i < total:
        time.sleep(DELAY_SECONDS)

print(f"\nTerminé — {ok_count} OK, {err_count} erreurs")
