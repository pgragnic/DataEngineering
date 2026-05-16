#!/usr/bin/env bash
# Démarre PostgreSQL + Flask (backend :8000) + Next.js (frontend :3000)

ROOT="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$ROOT/backend"
FRONTEND_DIR="$ROOT/frontend"
# Sur Termux, /tmp n'existe pas — utiliser $TMPDIR (ou $HOME en fallback)
_TMP="${TMPDIR:-${HOME:-/tmp}}"
BACKEND_LOG="$_TMP/uc28-backend.log"
FRONTEND_LOG="$_TMP/uc28-frontend.log"
BACKEND_PID=""
FRONTEND_PID=""
PG_STARTED_BY_US=0

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
    [ -n "$BACKEND_PID" ]  && kill "$BACKEND_PID"  2>/dev/null; ok "Backend arrêté"
    [ -n "$FRONTEND_PID" ] && kill "$FRONTEND_PID" 2>/dev/null; ok "Frontend arrêté"
    if [ "$PG_STARTED_BY_US" = "1" ]; then
        pg_ctl -D "$PG_DATA" stop -m fast > /dev/null 2>&1 && ok "PostgreSQL arrêté"
    fi
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

        # Supprimer un fichier PID obsolète (processus mort)
        if [ -f "$PG_DATA/postmaster.pid" ]; then
            DEAD_PID=$(head -1 "$PG_DATA/postmaster.pid" 2>/dev/null || true)
            if [ -n "$DEAD_PID" ] && ! kill -0 "$DEAD_PID" 2>/dev/null; then
                warn "PID obsolète ($DEAD_PID) — suppression du lock..."
                rm -f "$PG_DATA/postmaster.pid"
            fi
        fi

        # Initialiser le cluster si besoin
        if [ ! -f "$PG_DATA/PG_VERSION" ]; then
            warn "Cluster non initialisé — initdb..."
            initdb -D "$PG_DATA" --locale=C --encoding=UTF8 \
                > $_TMP/uc28-pgctl.log 2>&1 || true
        fi

        # Démarrer PostgreSQL (ne pas planter le script si ça échoue)
        pg_ctl -D "$PG_DATA" start -l $_TMP/uc28-postgres.log > /dev/null 2>&1 || true

        # Attendre jusqu'à 15s
        PG_READY=0
        for i in $(seq 1 15); do
            sleep 1
            if pg_isready -q 2>/dev/null; then
                PG_READY=1; break
            fi
        done

        if [ "$PG_READY" = "1" ]; then
            PG_STARTED_BY_US=1
            ok "PostgreSQL prêt"
            # Créer user + base uc28 si absents
            if ! psql -lqt 2>/dev/null | cut -d'|' -f1 | grep -qw uc28; then
                createuser --superuser uc28 2>/dev/null || true
                createdb -O uc28 uc28 2>/dev/null || true
                ok "Base uc28 créée"
            fi
        else
            warn "PostgreSQL n'a pas démarré (voir $_TMP/uc28-postgres.log) — mode JSON utilisé"
        fi
    fi
else
    warn "pg_ctl introuvable — PostgreSQL ignoré (Flask JSON mode)"
fi

# ── vérifier node_modules ────────────────────────────────────────────────────
# Forcer npm install si node_modules absent OU si @next/swc-android-arm64
# manque (cas où node_modules vient d'un next@14 sans binaire ARM64)
_SWC_DIR="$FRONTEND_DIR/node_modules/@next/swc-android-arm64"
if [ ! -d "$FRONTEND_DIR/node_modules" ] || [ ! -d "$_SWC_DIR" ]; then
    info "Installation des dépendances npm (next@13 + SWC android-arm64)..."
    (cd "$FRONTEND_DIR" && npm install)
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

if ! kill -0 "$FRONTEND_PID" 2>/dev/null; then
    err "Frontend arrêté — derniers logs :"
    tail -30 "$FRONTEND_LOG"
else
    err "Backend arrêté — derniers logs :"
    tail -20 "$BACKEND_LOG"
fi
cleanup
