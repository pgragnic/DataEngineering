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
SNAPSHOT_FILE = Path.home() / "DataEngineering/etoro-react/data/etoro_batch_snapshots.json"
DRY_RUN       = "--dry-run"   in sys.argv
DO_COMPARE    = "--compare"   in sys.argv
LIST_SNAPS    = "--snapshots" in sys.argv

# --from N  : commence à l'ordre N (1-indexé), utile pour reprendre après une erreur
FROM_IDX = 1
for _a in sys.argv:
    if _a.startswith("--from="):
        FROM_IDX = int(_a.split("=")[1])

# ── Ordres à passer ─────────────────────────────────────────────────────────
# Source : portfolio ThomasPJ au 07/06/2026
ORDERS = [
    # C x34 — 09/04, 10/04, 13/04, 05/06/2026
    {"symbol": "C", "pct": 0.25},  # 09/04 21:53
    {"symbol": "C", "pct": 0.13},  # 13/04 21:42
    {"symbol": "C", "pct": 0.13},  # 05/06 21:30
    {"symbol": "C", "pct": 0.13},  # 13/04 21:42
    {"symbol": "C", "pct": 0.12},  # 05/06 21:28
    {"symbol": "C", "pct": 0.13},  # 05/06 21:21
    {"symbol": "C", "pct": 0.25},  # 10/04 20:00
    {"symbol": "C", "pct": 0.25},  # 10/04 19:59
    {"symbol": "C", "pct": 0.13},  # 05/06 21:16
    {"symbol": "C", "pct": 0.12},  # 05/06 21:16
    {"symbol": "C", "pct": 0.12},  # 05/06 21:17
    {"symbol": "C", "pct": 0.12},  # 05/06 21:25
    {"symbol": "C", "pct": 0.13},  # 05/06 21:24
    {"symbol": "C", "pct": 0.13},  # 05/06 21:27
    {"symbol": "C", "pct": 0.12},  # 05/06 21:20
    {"symbol": "C", "pct": 0.13},  # 05/06 21:26
    {"symbol": "C", "pct": 0.13},  # 05/06 21:15
    {"symbol": "C", "pct": 0.13},  # 05/06 21:20
    {"symbol": "C", "pct": 0.12},  # 05/06 21:18
    {"symbol": "C", "pct": 0.25},  # 13/04 21:42
    {"symbol": "C", "pct": 0.13},  # 05/06 21:25
    {"symbol": "C", "pct": 0.13},  # 05/06 21:28
    {"symbol": "C", "pct": 0.13},  # 05/06 21:28
    {"symbol": "C", "pct": 0.25},  # 09/04 21:53
    {"symbol": "C", "pct": 0.13},  # 10/04 19:59
    {"symbol": "C", "pct": 0.13},  # 05/06 21:16
    {"symbol": "C", "pct": 0.25},  # 10/04 20:00
    {"symbol": "C", "pct": 0.13},  # 05/06 21:21
    {"symbol": "C", "pct": 0.12},  # 05/06 21:28
    {"symbol": "C", "pct": 0.12},  # 10/04 20:00
    {"symbol": "C", "pct": 0.13},  # 10/04 19:59
    {"symbol": "C", "pct": 0.13},  # 10/04 20:00
    {"symbol": "C", "pct": 0.12},  # 13/04 21:42
    {"symbol": "C", "pct": 0.12},  # 10/04 19:59
]

LABEL         = "Copie ThomasPJ — C x34"
DELAY_SECONDS = 1
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

    snap_positions = snap.get("portfolio", {}).get("positions", [])
    cur_positions  = current.get("positions", [])
    snap_equity    = snap.get("portfolio", {}).get("equity", 0)
    cur_equity     = current.get("equity", 0)

    fm  = lambda n: f"${n:>9.2f}"
    fd  = lambda n: f"{n:+.2f}"
    fpct= lambda n: f"{n:+.2f}%"

    # Index courant par positionID
    cur_by_id = {p["positionID"]: p for p in cur_positions if "positionID" in p}

    # ── Détail position par position ────────────────────────────────────────
    COL = f"{'#':<3} {'Symbole':<14} {'Dir':<6} {'Ouverture':<12} {'Prix ouv':>9} {'Prix act':>9} {'Mnt snap':>10} {'Mnt act':>10} {'Δ mnt':>8} {'P&L act':>9} {'P&L%':>7}  Statut"
    SEP = "─" * len(COL)
    print(COL)
    print(SEP)

    total_snap_amt = 0.0
    total_cur_amt  = 0.0
    total_cur_pnl  = 0.0

    for i, sp in enumerate(snap_positions, 1):
        pid        = sp.get("positionID")
        name       = sp.get("name", "?")
        direction  = "Long" if sp.get("isBuy") else "Short"
        open_date  = sp.get("openDate", "")[:10]
        open_rate  = sp.get("openRate", 0)
        snap_amt   = sp.get("amount", 0)
        total_snap_amt += snap_amt

        cp = cur_by_id.get(pid)
        if cp:
            cur_amt  = cp.get("amount", 0)
            cur_pnl  = cp.get("pnl", 0)
            close_rt = cp.get("closeRate", 0)
            delta    = cur_amt - snap_amt
            cost     = snap_amt - cur_pnl if (snap_amt - cur_pnl) != 0 else 1
            pnl_pct  = (cur_pnl / (cur_amt - cur_pnl)) * 100 if (cur_amt - cur_pnl) != 0 else 0
            status   = "✓ ouvert"
            total_cur_amt += cur_amt
            total_cur_pnl += cur_pnl
        else:
            cur_amt  = 0.0
            cur_pnl  = 0.0
            close_rt = 0.0
            delta    = -snap_amt
            pnl_pct  = 0.0
            status   = "✗ fermé"

        dir_arrow = "▲" if direction == "Long" else "▼"
        print(
            f"{i:<3} {name:<14} {dir_arrow+' '+direction:<6} {open_date:<12} "
            f"{open_rate:>9.4f} {close_rt:>9.4f} "
            f"{fm(snap_amt)} {fm(cur_amt)} {fd(delta):>8} "
            f"{fm(cur_pnl)} {fpct(pnl_pct):>7}  {status}"
        )

    # ── Nouvelles positions (absentes du snapshot) ──────────────────────────
    snap_ids  = {p.get("positionID") for p in snap_positions}
    snap_syms = {p.get("name") for p in snap_positions}
    new_pos   = [p for p in cur_positions if p.get("positionID") not in snap_ids
                 and p.get("name") in snap_syms]

    if new_pos:
        print(f"\n  + Nouvelles positions sur les mêmes symboles :")
        for p in new_pos:
            direction = "▲ Long" if p.get("isBuy") else "▼ Short"
            print(f"    #{p.get('positionID')}  {p.get('name'):<14} {direction}  "
                  f"ouv {p.get('openRate',0):.4f}  "
                  f"{fm(p.get('amount',0))}  P&L {fm(p.get('pnl',0))}")
            total_cur_amt += p.get("amount", 0)
            total_cur_pnl += p.get("pnl", 0)

    # ── Résumé ───────────────────────────────────────────────────────────────
    print(SEP)
    d_amt = total_cur_amt - total_snap_amt
    d_eq  = cur_equity    - snap_equity
    pnl_pct_tot = (total_cur_pnl / (total_cur_amt - total_cur_pnl) * 100
                   if (total_cur_amt - total_cur_pnl) != 0 else 0)
    print(f"{'TOTAL positions':<34} {fm(total_snap_amt)} {fm(total_cur_amt)} {fd(d_amt):>8} "
          f"{fm(total_cur_pnl)} {fpct(pnl_pct_tot):>7}")
    print(f"{'EQUITY portefeuille':<34} {fm(snap_equity)} {fm(cur_equity)} {fd(d_eq):>8}")
    sys.exit(0)

# ── Mode passage d'ordres ────────────────────────────────────────────────────
total     = len(ORDERS)
ok_count  = 0
err_count = 0
results   = []

start = FROM_IDX - 1  # 0-indexed
if start > 0:
    print(f"Reprise à partir de l'ordre {FROM_IDX}/{total}\n")
else:
    print(f"{'[DRY-RUN] ' if DRY_RUN else ''}{total} ordres — {LABEL}\n")

for i, order in enumerate(ORDERS, 1):
    if i < FROM_IDX:
        continue
    sym = order["symbol"]
    pct = order["pct"]
    print(f"[{i}/{total}] BUY {sym}  {pct}%", end="  →  ", flush=True)

    if pct <= 0:
        print("SKIP (pct <= 0)")
        results.append({**order, "status": "skipped"})
        continue

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
