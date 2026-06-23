# CLAUDE.md — Mémoire projet Code Resonance

## Identité du projet

- **Nom** : UC 28 — Inspection Augmentée
- **Contexte** : Hackathon Capgemini × Anthropic 2026
- **Équipe** : Code Resonance
- **Membres** :
  - Habib KOFFI — Agent IA
  - Véronique POILANE ZHANG — UI/UX
  - Philippe GRAGNIC — Data & démo

## Architecture

```
backend/    → FastAPI, logique métier, intégration Claude API
  main.py       Routes FastAPI (GET /sites, GET /sites/{id}, POST /analyser, POST /synthetiser, POST /suggestions, POST /questions_oui_non, POST /analyser_document_fournisseur)
  agent.py      Analyse ISO (rules + Claude), synthèse observation, suggestions terrain, questions oui/non
  rag.py        RAG ISO 9001 — sentence-transformers préchargé au démarrage (mode offline HuggingFace)
  database.py   SQLite — tables sites & audits_historiques
frontend/   → React + Vite, SPA bilingue FR/EN
  src/App.jsx               Routeur d'état (login → clients → dashboard → brief → inspection → report / portail)
  src/mockData.js           Données de démo RATP (10 missions, checklist FR/EN, KPIs, RAG articles, trajets)
  src/i18n.js               Dictionnaire FR/EN ~350 chaînes (9 namespaces)
  src/useT.js               Hook useT(lang) — interpolation {var}, aucune dépendance externe
  src/components/
    LoginScreen.jsx         Écran 0.1 — connexion mock, sélecteur rôle (Marc Lefèvre / Mei Lin Zhang), sélecteur langue FR/EN
    ClientList.jsx          Écran 0.2 — sélection client (RATP / Apave / BV)
    Dashboard.jsx           Écran 1 — planning temps réel + filtres + carte OSRM
    MapCard.jsx             Carte Leaflet/OSM avec routing OSRM réel, prop transport
    PlanningOverlay.jsx     Overlay génération planning
    SelectionView.jsx       Sélection mission (statut dynamique basé sur heure courante)
    Brief.jsx               Écran 2 — mission brief, checklist éditable, historique, docs portail RATP
    InspectionCapture.jsx   Écran 3/4 — 3 colonnes, saisie vocale, analyse, questions oui/non, récidive
    ReportView.jsx          Écran 5 — rapport, grille conformité SVG, transmission portail BV→RATP, RGPD
    SupplierPortal.jsx      Portail Mei Lin Zhang (RATP) — docs fournisseur, filtres catégorie, badge BV
    Header.jsx              Sticky, fil d'ariane cliquable, menu profil, salutation, mission subtitle
    PageLayout.jsx          Shell app 3 slots (left/center/right)
    Card.jsx                Primitif carte
data/       → audit.db (SQLite versionné, seedé) ; fixtures de test
docs/       → Schémas architecture, scripts démo, pitch decks
frontend/public/documents/  → Docs fournisseur téléchargeables (DOCX + PDF générés)
start.bat   → Lance backend (uv) + frontend (npm) en un clic
```

## Conventions

- **Branches** : `feature/<prenom>/<sujet>` (ex. `feature/habib/agent-ia`)
- **Commits** : préfixe conventionnel (`feat:`, `fix:`, `docs:`, `chore:`)
- **Langue** : code en anglais, commentaires et docs en français
- **Secrets** : jamais dans le dépôt — utiliser `.env` (ignoré par git)

## Stack

| Composant | Technologie | Version cible |
|---|---|---|
| Runtime Python | CPython | 3.12 |
| API Backend | FastAPI | ≥ 0.110 |
| Modèle IA | Claude Sonnet 4.6 | `claude-sonnet-4-6` |
| SDK Anthropic | anthropic | dernière stable |
| Frontend | React + Vite | React 18 / Vite 5 |
| Carte | Leaflet + react-leaflet + OSRM | — |
| Gestionnaire deps Python | uv / pip | — |
| Gestionnaire deps Node | npm | — |

## Points d'attention

- Le **prompt caching** est activé sur le system prompt dans `agent.py` (`cache_control: ephemeral`).
- Le modèle sentence-transformers du RAG est **préchargé au démarrage** (`lifespan`, mode `HF_HUB_OFFLINE=1`) pour éviter 30-60 s de latence et contourner le proxy Capgemini.
- La clé `ANTHROPIC_API_KEY` dans `backend/.env` doit être la clé **complète** (format `sk-ant-api03-…`, ~100 caractères). Une clé tronquée provoque une `AuthenticationError 401`.
- La base SQLite **`data/audit.db`** est **versionnée** dans le dépôt (déjà seedée : tables `clauses_iso`, `sites`, `audits_historiques`). Le seed `data/iso_9001_clauses.json` n'est utilisé qu'au premier `init_db()` si la table est vide ; il n'est pas versionné. Les clauses en base sont stockées **sans le symbole `§`** (ex. `7.1.5`, `8.7`) — d'où le repli tolérant de `articlesForClause()` côté front.
- Vérifier que `.env` n'est jamais commité.
- **Thème** : Agile verrouillé (`useState("agile")` dans App.jsx) — le sélecteur de thème a été supprimé.
- **Boucle fermée BV→RATP** : `savedReports` persisté dans `localStorage("bv_saved_reports")` ; ReportView → "⬆ Portail RATP" → SupplierPortal (Mei Lin Zhang).
- **i18n** : `typeDoc` dans SupplierPortal et items CHECKLIST restent en FR pour ne pas casser `getGroupe()`.
- **Statut missions** : calculé dynamiquement dans `SelectionView` via `new Date()`, pas depuis `item.statut` figé.
- **Modificateurs d'opacité Tailwind** (`/30`, `/5`) ne fonctionnent pas avec `var(--...)` en Tailwind v3 — utiliser `bg-red-50` etc. pour les fonds, les tokens UC28 pour les badges.

## Commandes utiles

```powershell
# Lancement en un clic (Windows)
.\start.bat

# Backend (manuel)
cd backend
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
# ou avec la clé si pas de .env :
$env:ANTHROPIC_API_KEY = "sk-ant-api03-..."
python -m uvicorn main:app --reload

# Frontend (manuel)
cd frontend
npm install  # première fois
npm run dev  # → http://localhost:5173

# API docs interactives
# http://localhost:8000/docs
```
