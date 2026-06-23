# UC 28 — Inspection Augmentée

> **Hackathon Capgemini × Anthropic 2026 — Équipe Code Resonance**

## Équipe

| Membre | Périmètre |
|---|---|
| Habib KOFFI | Agent IA (Claude API) |
| Véronique POILANE ZHANG | Interface utilisateur (React/Vite) |
| Philippe GRAGNIC | Data & démo |

## Description

Solution d'inspection augmentée assistée par IA pour les auditeurs qualité terrain (démo : client RATP).
L'agent IA s'appuie sur Claude (Anthropic) pour analyser des observations ISO 9001, détecter des non-conformités, générer des synthèses professionnelles et proposer des suggestions contextuelles.

## Fonctionnalités principales

### Parcours auditeur (8 écrans)
- **Connexion** (écran 0.1) — formulaire mock pré-rempli (Marc Lefèvre)
- **Sélection client** (écran 0.2) — RATP / Apave / Bureau Veritas
- **Dashboard** (écran 1) — planning journée avec trait temps réel, filtres, temps de trajet, carte interactive Leaflet
- **Brief** (écran 2) — contexte site, checklist pré-audit éditable (ajout/suppression d'items), ingestion CR externe
- **Capture** (écrans 3/4) — saisie vocale, photo, analyse ISO, récidive, suggestions IA, questions oui/non
- **Rapport** (écran 5) — rapport structuré, suivi actions correctives, signature, anonymisation RGPD

### Intelligence artificielle (Claude API)
| Fonctionnalité | Endpoint | Modèle |
|---|---|---|
| Analyse ISO 9001 (criticité, clause, score) | `POST /analyser` | Claude Sonnet 4.6 + RAG |
| Synthèse de l'observation brute | `POST /synthetiser` | Claude Sonnet 4.6 |
| Suggestions d'observations terrain | `POST /suggestions` | Claude Sonnet 4.6 |
| Questions de vérification oui/non | `POST /questions_oui_non` | Claude Sonnet 4.6 |

### UX avancée
- 3 thèmes visuels : Classique / Agile Diagrams / Aria (Navy)
- Carte interactive (Leaflet + OpenStreetMap) avec tracé de parcours, `flyTo` animé, filtre missions
- Checklist éditable : suppression d'items de base, ajout de points auditeur, propagation à l'écran Capture
- Barre d'action en haut de chaque écran (retour + action principale)
- Badge hors-ligne réactif (`navigator.onLine`)
- Animation "moment fort" sur NC MAJEURE

## Stack technique

| Couche | Technologie | Version |
|---|---|---|
| Backend | Python 3.12, FastAPI | ≥ 0.110 |
| Frontend | React + Vite | React 18 / Vite 5 |
| IA | Claude Sonnet 4.6 (Anthropic) | `claude-sonnet-4-6` |
| RAG | sentence-transformers + SQLite | — |
| Carte | Leaflet + OpenStreetMap | react-leaflet |
| Styles | Tailwind CSS + Google Fonts Inter | — |

## Structure

```
.
├── backend/
│   ├── main.py        # Routes FastAPI
│   ├── agent.py       # Analyse ISO, synthèse, suggestions, questions oui/non
│   ├── rag.py         # RAG ISO 9001 (sentence-transformers)
│   └── database.py    # SQLite — sites & historique audits
├── frontend/
│   └── src/
│       ├── App.jsx                  # Routeur d'état principal
│       ├── mockData.js              # Données démo RATP
│       └── components/
│           ├── LoginScreen.jsx
│           ├── ClientList.jsx
│           ├── Dashboard.jsx
│           ├── MapCard.jsx
│           ├── Brief.jsx
│           ├── InspectionCapture.jsx
│           ├── ReportView.jsx
│           └── Header.jsx
├── data/              # Données de démo (exclues du dépôt)
├── docs/              # Spécifications et supports de démo
├── CLAUDE.md          # Mémoire projet pour Claude Code
└── start.bat          # Lancement en un clic (Windows)
```

## Démarrage rapide

```powershell
# Lancement en un clic (Windows)
.\start.bat
```

```powershell
# Backend (manuel)
cd backend
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Frontend (manuel)
cd frontend
npm install   # première fois
npm run dev   # → http://localhost:5173
```

La clé `ANTHROPIC_API_KEY` doit être définie dans `backend/.env` (format `sk-ant-api03-…`).

API docs interactives : [http://localhost:8000/docs](http://localhost:8000/docs)
