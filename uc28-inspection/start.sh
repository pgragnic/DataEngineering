#!/usr/bin/env bash
# Démarre PostgreSQL + Flask (backend :8000) + Next.js (frontend :3000)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$ROOT/backend"
FRONTEND_DIR="$ROOT/frontend"
BACKEND_LOG="/tmp/uc28-backend.log"
FRONTEND_LOG="/tmp/uc28-frontend.log"
BACKEND_PID=""
FRONTEND_PID=""

# Répertoire de données PostgreSQL (Termux : $PREFIX/var/lib/postgresql)
PG_PREFIX="${PREFIX:-/data/data/com.termux/files/usr}"
PG_DATA="${PGDATA:-$PG_PREFIX/var/lib/postgresql}"

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
    # PostgreSQL : ne l'arrêter que si c'est nous qui l'avons démarré
    if [ "${PG_STARTED_BY_US:-0}" = "1" ]; then
        pg_ctl -D "$PG_DATA" stop -m fast > /dev/null 2>&1 && ok "PostgreSQL arrêté"
    fi
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

# ── PostgreSQL ───────────────────────────────────────────────────────────────
if command -v pg_ctl > /dev/null 2>&1; then
    if pg_isready -q 2>/dev/null; then
        ok "PostgreSQL déjà en cours d'exécution"
    else
        info "Démarrage de PostgreSQL ($PG_DATA)..."
        # Initialiser le cluster si besoin
        if [ ! -f "$PG_DATA/PG_VERSION" ]; then
            warn "Cluster non initialisé — initdb en cours..."
            pg_ctl -D "$PG_DATA" initdb -o "--locale=C --encoding=UTF8" > /tmp/uc28-pgctl.log 2>&1
            ok "Cluster initialisé"
        fi
        pg_ctl -D "$PG_DATA" start -l /tmp/uc28-postgres.log > /dev/null 2>&1
        # Attendre que PostgreSQL soit prêt (max 15s)
        for i in $(seq 1 15); do
            sleep 1
            pg_isready -q 2>/dev/null && break
            [ "$i" -eq 15 ] && { err "PostgreSQL n'a pas démarré — voir /tmp/uc28-postgres.log"; exit 1; }
        done
        PG_STARTED_BY_US=1
        ok "PostgreSQL prêt"
        # Créer la base uc28 si elle n'existe pas
        if ! psql -lqt 2>/dev/null | cut -d'|' -f1 | grep -qw uc28; then
            info "Création de la base uc28..."
            createuser --superuser uc28 2>/dev/null || true
            createdb -O uc28 uc28 2>/dev/null && ok "Base uc28 créée" || warn "Base uc28 déjà existante"
        fi
    fi
else
    warn "pg_ctl introuvable — PostgreSQL ignoré (le backend Flask n'en a pas besoin)"
fi

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
echo -e "  ${C_GREEN}PostgreSQL${C_RESET}: localhost:5432"
echo -e "  ${C_GREEN}Backend   ${C_RESET}: http://localhost:8000/health"
echo -e "  ${C_GREEN}Frontend  ${C_RESET}: http://localhost:3000"
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
