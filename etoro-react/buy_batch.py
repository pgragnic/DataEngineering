#!/usr/bin/env python3
"""
Passe des ordres d'achat en série et sauvegarde un snapshot du portefeuille.

Usage :
  python3 buy_batch.py              — passe les ordres + sauvegarde snapshot
  python3 buy_batch.py --dry-run    — simule sans passer d'ordres
  python3 buy_batch.py --compare    — compare snapshot sauvegardé vs portefeuille actuel
  python3 buy_batch.py --snapshots  — liste tous les snapshots enregistrés
"""
import json, sys, time, urllib.request, urllib.error
from datetime import datetime
from pathlib import Path

API           = "http://127.0.0.1:5000"
SNAPSHOT_FILE = Path.home() / "etoro_batch_snapshots.json"
DRY_RUN       = "--dry-run"   in sys.argv
DO_COMPARE    = "--compare"   in sys.argv
LIST_SNAPS    = "--snapshots" in sys.argv

# ── Ordres à passer ─────────────────────────────────────────────────────────
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

LABEL         = "Copie ThomasPJ — BAC x10"
DELAY_SECONDS = 2
# ────────────────────────────────────────────────────────────────────────────

def api_get(path):
    with urllib.request.urlopen(f"{API}{path}", timeout=15) as r:
        return json.loads(r.read())

def api_post(path, body):
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
        f"{API}{path}", data=data,
        headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

def load_snapshots():
    try:
        return json.loads(SNAPSHOT_FILE.read_text())
    except Exception:
        return []

def save_snapshot(entry):
    snaps = load_snapshots()
    snaps.append(entry)
    SNAPSHOT_FILE.write_text(json.dumps(snaps, indent=2))
    print(f"\nSnapshot sauvegardé : {SNAPSHOT_FILE}")

# ── Mode liste ───────────────────────────────────────────────────────────────
if LIST_SNAPS:
    snaps = load_snapshots()
    if not snaps:
        print("Aucun snapshot enregistré.")
    else:
        for i, s in enumerate(snaps):
            nb   = len(s.get("orders", []))
            eq   = s.get("portfolio", {}).get("equity", "?")
            print(f"[{i}] {s['date']}  —  {s['label']}  ({nb} ordres, equity={eq})")
    sys.exit(0)

# ── Mode comparaison ─────────────────────────────────────────────────────────
if DO_COMPARE:
    snaps = load_snapshots()
    if not snaps:
        print("Aucun snapshot trouvé. Lance d'abord le script sans --compare.")
        sys.exit(1)

    snap = snaps[-1]
    print(f"Comparaison vs snapshot du {snap['date']} — {snap['label']}\n")

    try:
        current = api_get("/api/portfolio")
    except Exception as e:
        print(f"Impossible de joindre l'API : {e}"); sys.exit(1)

    snap_positions  = snap.get("portfolio", {}).get("positions", [])
    cur_positions   = current.get("positions", [])
    snap_equity     = snap.get("portfolio", {}).get("equity", 0)
    cur_equity      = current.get("equity", 0)

    # Regroupe par symbole
    def by_symbol(positions):
        acc = {}
        for p in positions:
            sym = p.get("name", "?")
            if sym not in acc:
                acc[sym] = {"count": 0, "amount": 0.0, "pnl": 0.0}
            acc[sym]["count"]  += 1
            acc[sym]["amount"] += p.get("amount", 0)
            acc[sym]["pnl"]    += p.get("pnl", 0)
        return acc

    snap_map = by_symbol(snap_positions)
    cur_map  = by_symbol(cur_positions)
    all_syms = sorted(set(snap_map) | set(cur_map))

    fmt = lambda n: f"{n:+.2f}" if n else "0.00"

    print(f"{'Symbole':<12} {'Snapshot':>10} {'Actuel':>10} {'Δ montant':>12} {'P&L actuel':>12} {'Positions':>10}")
    print("─" * 72)
    for sym in all_syms:
        s = snap_map.get(sym, {})
        c = cur_map.get(sym, {})
        s_amt  = s.get("amount", 0)
        c_amt  = c.get("amount", 0)
        c_pnl  = c.get("pnl", 0)
        delta  = c_amt - s_amt
        count  = c.get("count", 0)
        status = "" if c else "  ✗ fermé"
        print(f"{sym:<12} ${s_amt:>9.2f} ${c_amt:>9.2f} {fmt(delta):>12}  {fmt(c_pnl):>11}  {count:>4}{status}")

    print("─" * 72)
    d_eq = cur_equity - snap_equity
    print(f"{'EQUITY':<12} ${snap_equity:>9.2f} ${cur_equity:>9.2f} {fmt(d_eq):>12}")
    sys.exit(0)

# ── Mode passage d'ordres ────────────────────────────────────────────────────
total     = len(ORDERS)
ok_count  = 0
err_count = 0
results   = []

print(f"{'[DRY-RUN] ' if DRY_RUN else ''}{total} ordres — {LABEL}\n")

for i, order in enumerate(ORDERS, 1):
    sym = order["symbol"]
    pct = order["pct"]
    print(f"[{i}/{total}] BUY {sym}  {pct}%", end="  →  ", flush=True)

    if DRY_RUN:
        print("skipped")
        results.append({**order, "status": "dry-run"})
        continue

    try:
        res = api_post("/api/buy", {"symbol": sym, "pct": pct})
        if res.get("error"):
            print(f"ERREUR : {res['error']}")
            err_count += 1
            results.append({**order, "status": "error", "detail": res["error"]})
        else:
            print(f"OK  {res}")
            ok_count += 1
            results.append({**order, "status": "ok", "detail": res})
    except urllib.error.HTTPError as e:
        msg = e.read().decode(errors="replace")
        print(f"HTTP {e.code} : {msg}")
        err_count += 1
        results.append({**order, "status": "error", "detail": msg})
    except Exception as e:
        print(f"Exception : {e}")
        err_count += 1
        results.append({**order, "status": "error", "detail": str(e)})

    if i < total:
        time.sleep(DELAY_SECONDS)

print(f"\nTerminé — {ok_count} OK, {err_count} erreurs")

if not DRY_RUN:
    try:
        portfolio = api_get("/api/portfolio")
    except Exception:
        portfolio = {}

    save_snapshot({
        "date":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "label":     LABEL,
        "orders":    results,
        "portfolio": portfolio,
    })
    print(f"Pour comparer plus tard : python3 {__file__} --compare")
