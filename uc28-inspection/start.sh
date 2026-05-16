#!/usr/bin/env bash
# Démarre Flask (backend :8000) + Next.js (frontend :3000) en parallèle
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$ROOT/backend"
FRONTEND_DIR="$ROOT/frontend"
BACKEND_LOG="/tmp/uc28-backend.log"
FRONTEND_LOG="/tmp/uc28-frontend.log"
BACKEND_PID=""
FRONTEND_PID=""

# ── couleurs ────────────────────────────────────────────────────────────────
C_GREEN='\033[0;32m'; C_BLUE='\033[0;34m'
C_YELLOW='\033[1;33m'; C_RED='\033[0;31m'; C_RESET='\033[0m'
ok()   { echo -e "${C_GREEN}✓${C_RESET} $*"; }
info() { echo -e "${C_BLUE}▶${C_RESET} $*"; }
warn() { echo -e "${C_YELLOW}⚠${C_RESET} $*"; }
err()  { echo -e "${C_RED}✗${C_RESET} $*" >&2; }

# ── nettoyage à l'arrêt ──────────────────────────────────────────────────────
cleanup() {
    echo ""
    warn "Arrêt des services..."
    [ -n "$BACKEND_PID" ]  && kill "$BACKEND_PID"  2>/dev/null && ok "Backend arrêté"
    [ -n "$FRONTEND_PID" ] && kill "$FRONTEND_PID" 2>/dev/null && ok "Frontend arrêté"
    # tuer les tail -f fils
    jobs -p | xargs -r kill 2>/dev/null
    exit 0
}
trap cleanup INT TERM

echo ""
echo -e "${C_GREEN}╔══════════════════════════════════════════╗${C_RESET}"
echo -e "${C_GREEN}║  UC 28 — Inspection Augmentée            ║${C_RESET}"
echo -e "${C_GREEN}╚══════════════════════════════════════════╝${C_RESET}"
echo ""

# ── vérifier .env ────────────────────────────────────────────────────────────
if [ ! -f "$BACKEND_DIR/.env" ]; then
    warn "backend/.env introuvable — création depuis .env.example"
    cp "$ROOT/.env.example" "$BACKEND_DIR/.env"
    err "ANTHROPIC_API_KEY manquante. Édite $BACKEND_DIR/.env avant de continuer."
    exit 1
fi
ok ".env présent"

# ── vérifier node_modules ────────────────────────────────────────────────────
if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
    info "node_modules absent — npm install en cours..."
    (cd "$FRONTEND_DIR" && npm install --silent)
    ok "npm install terminé"
fi

# ── démarrer le backend ──────────────────────────────────────────────────────
info "Backend Flask → http://localhost:8000"
(cd "$BACKEND_DIR" && python flask_app.py) > "$BACKEND_LOG" 2>&1 &
BACKEND_PID=$!

# attendre que Flask réponde (max 10s)
for i in $(seq 1 10); do
    sleep 1
    if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
        err "Le backend a planté. Derniers logs :"
        tail -20 "$BACKEND_LOG"
        exit 1
    fi
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        ok "Backend prêt (${i}s)"
        break
    fi
    [ "$i" -eq 10 ] && warn "Backend lent à démarrer — voir $BACKEND_LOG"
done

# ── démarrer le frontend ─────────────────────────────────────────────────────
info "Frontend Next.js → http://localhost:3000"
(cd "$FRONTEND_DIR" && npm run dev) > "$FRONTEND_LOG" 2>&1 &
FRONTEND_PID=$!
sleep 3

if ! kill -0 "$FRONTEND_PID" 2>/dev/null; then
    err "Le frontend a planté. Derniers logs :"
    tail -20 "$FRONTEND_LOG"
    cleanup
fi
ok "Frontend démarré"

# ── résumé ───────────────────────────────────────────────────────────────────
echo ""
echo -e "  ${C_GREEN}Backend ${C_RESET} : http://localhost:8000/health"
echo -e "  ${C_GREEN}Frontend${C_RESET} : http://localhost:3000"
echo ""
echo -e "${C_YELLOW}Logs en direct — Ctrl+C pour tout arrêter${C_RESET}"
echo "──────────────────────────────────────────"

# ── afficher les logs des deux services ──────────────────────────────────────
tail -f "$BACKEND_LOG"  | sed "s/^/$(printf '\033[0;34m')[BE]$(printf '\033[0m') /" &
tail -f "$FRONTEND_LOG" | sed "s/^/$(printf '\033[0;32m')[FE]$(printf '\033[0m') /" &

# rester vivant tant que les deux processus tournent
while kill -0 "$BACKEND_PID" 2>/dev/null && kill -0 "$FRONTEND_PID" 2>/dev/null; do
    sleep 2
done

err "Un service s'est arrêté de façon inattendue."
cleanup
