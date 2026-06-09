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
  main.py       Routes FastAPI (GET /sites, GET /sites/{id}, POST /analyser, POST /synthetiser, POST /suggestions)
  agent.py      Analyse ISO (rules + GEP/Claude), synthèse observation, suggestions terrain (Claude Opus 4.8)
  rag.py        RAG ISO 9001 — sentence-transformers préchargé au démarrage
  database.py   SQLite — tables sites & audits_historiques
frontend/   → React + Vite, SPA 8 écrans
  src/App.jsx               Routeur d'état (login → clients → dashboard → brief → inspection → report)
  src/mockData.js           Données de démo RATP (sites, checklist, KPIs, RAG articles, trajets, questions)
  src/components/
    LoginScreen.jsx         Écran 0.1 — connexion mock (Marc Lefèvre pré-rempli)
    ClientList.jsx          Écran 0.2 — sélection client (RATP / Apave / BV)
    Dashboard.jsx           Écran 1 — timeline + planning (trait temps réel) + filtres + itinéraire
    Brief.jsx               Écran 2 — contexte site + checklist pré-audit + ingestion CR externe
    InspectionCapture.jsx   Écran 3/4 — saisie vocale, photo, analyse, récidive, moment fort
    ReportView.jsx          Écran 5 — rapport, suivi actions, signature, anonymisation RGPD
    Header.jsx              En-tête contextuel (timer, mode jury, badge hors-ligne)
data/       → Jeux de données de démo et fixtures de test
docs/       → Spécifications, ADR, documentation API
start.bat   → Lance backend (uv) + frontend (npm) en un clic

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
| Gestionnaire deps Python | uv / pip | — |
| Gestionnaire deps Node | npm | — |

## Points d'attention

- Le **prompt caching** est activé sur le system prompt dans `agent.py` (`cache_control: ephemeral`).
- Le modèle sentence-transformers du RAG est **préchargé au démarrage** (`lifespan`) pour éviter 30-60 s de latence sur le premier `/analyser`.
- La clé `ANTHROPIC_API_KEY` dans `backend/.env` doit être la clé **complète** (format `sk-ant-api03-…`, ~100 caractères). Une clé tronquée provoque une `AuthenticationError 401`.
- La base SQLite **`data/audit.db`** est **versionnée** dans le dépôt (déjà seedée : tables `clauses_iso`, `sites`, `audits_historiques`). Le seed `data/iso_9001_clauses.json` n'est utilisé qu'au premier `init_db()` si la table est vide ; il n'est pas versionné. Les clauses en base sont stockées **sans le symbole `§`** (ex. `7.1.5`, `8.7`) — d'où le repli tolérant de `articlesForClause()` côté front.
- Vérifier que `.env` n'est jamais commité. Le fichier `Lancer la démo (deux terminaux).txt` contient une clé en clair — **ne pas commiter**.

## Historique des sessions de développement

### Session du 2026-06-01 — 14h→16h (approx.)

Implémentation des fonctionnalités v2 issues de `Fonctionnalites_Inspection_Augmentee_v2.xlsx` et du parcours héros (`Parcours_Heros_Inspection_Augmentee.pptx`).

**Bloc A — Nouveaux écrans login et sélection client**
- Créé `LoginScreen.jsx` (écran 0.1) — formulaire mock pré-rempli, aucun backend
- Créé `ClientList.jsx` (écran 0.2) — RATP / Apave / Bureau Veritas avec nb de missions
- Modifié `App.jsx` — navigation démarre sur "login", Header masqué sur écrans 0.1/0.2

**Bloc B — Dashboard enrichi (écran 1)**
- Toggle "Liste | Planning" avec vue horizontale des missions sur la journée
- Trait rouge temps réel indiquant l'heure actuelle (via `setInterval`)
- Filtres : Mes missions / Toutes, par statut (Tous / TERMINÉ / PROCHAIN / PLANIFIÉ)
- Temps de trajet 🚗 🚲 🚶 dans chaque carte et dans la vue Planning
- Données `heureMin`, `dureeMin`, `trajet` ajoutées dans `AUDITS_TIMELINE` (mockData.js)

**Bloc C — Déjà implémentés (session précédente)**
- Saisie vocale Web Speech API (`fr-FR`, bouton micro toggle rouge)
- Capture photo avec thumbnail (input file `capture="environment"`)
- Questions suggérées par clause ISO (QUESTIONS_SUGGEREES dans mockData.js)
- Animation "moment fort" NC MAJEURE (ring rouge pulsant 3,5 s)

**Bloc D — Moteur d'analyse enrichi**
- `agent.py` : champ `score_criticite` ajouté (MAJEURE=3, MINEURE=2, OBSERVATION=1, CONFORME=0)
- `InspectionCapture.jsx` : badge "score X/3" affiché à côté du badge criticité
- `InspectionCapture.jsx` : alerte récidive 🔁 si clause détectée ∈ RECURRENCES du site courant

**Bloc E — Restitution enrichie (écran 5)**
- `ReportView.jsx` : tableau "Suivi actions correctives" (NC, Responsable, Délai, Statut mock)
- `ReportView.jsx` : bloc "Validation contradictoire / Signature" avec modal inline

**Bloc F — Features transverses**
- `Header.jsx` : badge ⚡ "Mode hors-ligne" réactif à `navigator.onLine` / `window online/offline`
- `ReportView.jsx` : toggle 🔒 Anonymiser (RGPD) — remplace noms dans l'aperçu rapport
- `Brief.jsx` : import CR externe (PDF/DOCX) avec animation "Analyse IA…" (mock 2,2 s) + confirmation

### Session du 2026-06-01 — 16h→18h (approx.)

Refonte visuelle complète inspirée du PPTX `Agile Diagrams.pptx` — aucune modification de logique ou de fonctionnalité.

**Design system Agile Diagrams appliqué**
- Palette extraite du PPTX : `#5D93C1` (bleu brand), `#1AAED2` (cyan), `#00CACC` (teal), `#4CDFB2` (mint), `#E8E96D` (jaune alerte), `#494949` (dark), `#999999` (mid gray)
- Police Inter (Google Fonts) en remplacement du sans-serif par défaut
- Style : flat, géométrique, minimal — cards blanches, fond dark `#494949` sur headers/footers

**Toggle de thème sur l'écran Login**
- `App.jsx` : nouvel état `theme` (`"classic"` | `"agile"`, défaut `"classic"`) transmis en prop à tous les composants
- `LoginScreen.jsx` : sélecteur deux boutons "Classique / Agile Diagrams" en bas du formulaire
- Chaque composant utilise `const ag = theme === "agile"` pour les classes conditionnelles

**Fichiers modifiés**
- `frontend/tailwind.config.js` — palette `brand.*` ajoutée dans `theme.extend.colors`
- `frontend/src/index.css` — import Google Fonts Inter
- `frontend/src/App.jsx` — état `theme` + propagation props
- `frontend/src/components/LoginScreen.jsx` — toggle + gradient bleu→teal en mode agile
- `frontend/src/components/ClientList.jsx` — header dark, couleurs cartes clients
- `frontend/src/components/Header.jsx` — fond `#494949`, badges statut palette
- `frontend/src/components/Dashboard.jsx` — réécriture complète (KPI colorés, planning teal, bouton mint)
- `frontend/src/components/Brief.jsx` — accents bleu brand, spinner/badge mint
- `frontend/src/components/InspectionCapture.jsx` — CRITICITE_STYLE_AGILE (MINEURE jaune, OBSERVATION mint, CONFORME lime)
- `frontend/src/components/ReportView.jsx` — titre RATP bleu brand, boutons mint/cyan, suivi actions palette

### Session du 2026-06-03 — Synthèse IA de l'observation auditeur

Ajout d'une fonctionnalité de reformulation de l'observation brute avant déclenchement de l'analyse ISO.

**Fonctionnalité**
- Bouton **✨ Synthétiser** visible dans l'écran Capture (écran 3/4) dès que l'observation est non vide et avant tout résultat d'analyse
- Appel Claude (120 tokens, même client GEP) pour reformuler l'observation en une phrase d'audit nominale, concise et professionnelle
- Bloc aperçu avec deux actions : **Utiliser ce texte** (remplace la textarea) / **Ignorer**
- Fallback transparent si `GEP_API_KEY` absent : retourne l'observation originale inchangée
- États `synthese` / `syntheseLoading` réinitialisés à chaque Valider / Refaire

**Fichiers modifiés**
- `backend/agent.py` — nouvelle fonction `synthetiser_observation(observation: str) -> str`
- `backend/main.py` — nouvelle route `POST /synthetiser` + modèle `SyntheseRequest`
- `frontend/src/api.js` — nouvelle fonction exportée `synthetiser(observation)`
- `frontend/src/components/InspectionCapture.jsx` — import, états, handler, UI bouton + bloc aperçu

### Session du 2026-06-03 — UX navigation + sélection checklist + suggestions Opus

Améliorations UX et nouvelle fonctionnalité de suggestions contextuelles par IA.

**Barre d'action en haut de chaque écran**
- Les boutons d'action (Démarrer l'inspection, Générer le pré-rapport, Envoyer au client, Nouvel audit) ont été déplacés du bas vers le haut de l'écran pour éviter le scroll
- Les liens de navigation (← Retour au dashboard, ← Retour, ← Retour à l'inspection) ont été intégrés dans cette même barre, à gauche
- Structure finale : `[← Retour] — [info contexte] — [bouton action]` avec `justify-between`
- `border-t` → `border-b` sur chaque barre ; liens redondants dans le contenu supprimés
- Timer dupliqué supprimé de `InspectionCapture.jsx` (déjà présent dans la barre)

**Fichiers modifiés**
- `frontend/src/components/Brief.jsx` — barre haut avec ← Retour au dashboard
- `frontend/src/components/InspectionCapture.jsx` — barre haut avec ← Retour
- `frontend/src/components/ReportView.jsx` — barre haut avec ← Retour à l'inspection

**Sélection d'un point de checklist (écran 3/4)**
- Clic sur un item de la check-list dynamique → surlignage coloré (fond + ring selon thème) + texte en gras
- Header central `CAPTURE EN COURS` remplacé par : label `POINT EN COURS` + nom du point en bleu + identifiant section/clause
- Sans sélection : `CAPTURE EN COURS — sélectionner un point à gauche`
- Correction de bug : `handleValider()` cochait toujours le premier item `a-venir` au lieu de l'item sélectionné — corrigé pour marquer `selectedItem.id` en priorité, puis fallback sur le premier `a-venir`
- `selectedItem` réinitialisé à `null` après validation

**Fichiers modifiés**
- `frontend/src/components/InspectionCapture.jsx` — état `selectedItem`, onClick sur items, header dynamique, fix handleValider

**Suggestions d'observations générées par Claude Opus (écran 3/4)**
- Quand un item de checklist est sélectionné, appel automatique à `POST /suggestions` pour générer 3 exemples d'observations terrain contextualisés
- Modèle utilisé : `anthropic.claude-opus-4-8` (via variable `GEP_MODEL_OPUS`, fallback sur `GEP_MODEL`)
- Spinner "Génération des suggestions…" pendant le chargement
- Fallback silencieux sur les `EXEMPLES` statiques si GEP indisponible ou erreur
- Sans item sélectionné : retour aux 3 exemples statiques

**Fichiers modifiés**
- `backend/agent.py` — nouvelle fonction `generer_suggestions(item_texte, clause, section_titre) -> list[str]`
- `backend/main.py` — nouvelle route `POST /suggestions` + modèle `SuggestionsRequest`
- `frontend/src/api.js` — nouvelle fonction exportée `getSuggestions(itemTexte, clause, sectionTitre)`
- `frontend/src/components/InspectionCapture.jsx` — import, états `suggestions`/`suggestionsLoading`/`selectionCount`, `useEffect` sur `selectionCount`, rendu conditionnel des boutons

**Fix suggestions statiques (même session)**
- Bug : `generer_suggestions` utilisait `GEP_MODEL_OPUS` (variable inexistante) → fallback sur `anthropic.claude-opus-4-8` inconnu de GEP → exception silencieuse → `[]` → EXEMPLES statiques toujours affichées
- Correction `backend/agent.py` : utilise `GEP_MODEL` (= `anthropic.claude-opus-4-7` dans `.env`)
- Correction `frontend/src/components/InspectionCapture.jsx` : `useEffect` dépend de `selectionCount` (compteur incrémenté à chaque clic item) plutôt que `selectedItem?.id` → chaque sélection déclenche un nouvel appel LLM, même item répété inclus

### Session du 2026-06-03 — Carte des trajets dans le Dashboard

Ajout d'une carte interactive style Google Maps affichant le parcours de l'auditeur entre ses sites d'audit.

**Choix technique**
- **Leaflet + OpenStreetMap** via `react-leaflet` — zéro clé API, zéro coût, compatible démo offline
- `npm install react-leaflet leaflet` dans `frontend/`

**Fonctionnalités de la carte**
- Marqueur de départ `M` (auditeur, Paris) relié aux sites par une polyligne pointillée dans l'ordre chronologique (`heureMin`)
- Marqueurs colorés par statut : TERMINÉ (vert), PROCHAIN (bleu), PLANIFIÉ (gris) — palette adaptée aux 3 thèmes
- Popup sur chaque marqueur : site, heure, durée audit, temps de trajet voiture
- Légende des statuts dans l'en-tête de la carte
- Centre automatique sur le barycentre des points, zoom 11
- Hauteur fixe 240px, pleine largeur, entre les KPIs et les onglets Liste/Planning

**Fichiers modifiés**
- `frontend/src/index.css` — import `leaflet/dist/leaflet.css`
- `frontend/src/mockData.js` — ajout `coords: { lat, lng }` sur `AUDITEUR` et les 4 entrées `AUDITS_TIMELINE`
- `frontend/src/components/MapCard.jsx` — nouveau composant (création) avec `DivIcon` personnalisés (fix icônes Leaflet sous Vite)
- `frontend/src/components/Dashboard.jsx` — import + `<MapCard audits={AUDITS_TIMELINE} theme={theme} />`

**Coordonnées ajoutées**
| Point | lat | lng |
|---|---|---|
| Auditeur (Paris) | 48.8566 | 2.3522 |
| Atelier Châtillon | 48.8017 | 2.2792 |
| Fontenay-sous-Bois | 48.8505 | 2.4785 |
| Sucy-en-Brie | 48.7707 | 2.5126 |
| Siège RATP | 48.8724 | 2.3375 |

### Session du 2026-06-03 — Améliorations carte + checklist auditeur + RAG dynamique

**Refonte carte (MapCard.jsx + Dashboard.jsx)**
- Récurrences à vérifier supprimées du Dashboard ; carte déplacée en sidebar de la vue Liste (`w-72`, hauteur 340px, mode `compact`)
- Tracé rendu visible : ligne orange `#F97316` épaisse (weight 5, lineCap round) avec halo blanc (weight 9) derrière — contraste maximal sur tuiles OSM
- Zoom activé : `zoomControl={true}`, `scrollWheelZoom={true}`
- Clic sur une carte d'audit → `flyTo` animé (1,2 s) au zoom 15 (niveau quartier) via composant `FlyTo` interne utilisant `useMap()`
- Composant `SaveMapRef` pour capturer l'instance de la carte via `useRef`
- Bouton **⊖ Tout voir** dans la légende bas → `flyToBounds` sur tous les points du parcours
- Filtre "Mes missions / Toutes" appliqué à la carte : `audits={auditsFiltres}` au lieu de `AUDITS_TIMELINE`
- État `focusCoords` dans Dashboard.jsx transmis en prop `focusCoords` à MapCard

**Fichiers modifiés**
- `frontend/src/components/MapCard.jsx` — FlyTo, SaveMapRef, route orange, halo, zoom, bouton reset, prop compact/focusCoords
- `frontend/src/components/Dashboard.jsx` — état `focusCoords`, onClick sur cartes audit, `audits={auditsFiltres}`, suppression Récurrences

**Ajout de points personnalisés dans la checklist (Brief → InspectionCapture)**
- Écran Brief (écran 2) : bouton `+ Ajouter un point` en bas de la checklist IA (visible après génération)
- Formulaire inline : saisie texte, confirmation Entrée/bouton, annulation Échap ; chaque point ajouté affiché avec badge "Auditeur" + bouton ×
- Les points sont transmis à l'écran suivant via `onDemarrer(checklistExtra)` → `handleDemarrerInspection(points)` → prop `extraPoints` sur InspectionCapture
- Dans InspectionCapture : si `extraPoints` non vide, une section `Sx — Points auditeur (ajouté)` est injectée en bas de la checklist avec items cliquables comme les autres

**Fichiers modifiés**
- `frontend/src/components/Brief.jsx` — états `checklistExtra`/`addingPoint`/`newPointTexte`, UI formulaire, `onDemarrer(checklistExtra)`
- `frontend/src/App.jsx` — état `extraPoints`, `handleDemarrerInspection(points)`, prop `extraPoints` sur InspectionCapture
- `frontend/src/components/InspectionCapture.jsx` — prop `extraPoints`, injection section `Sx` dans `baseChecklist`, titre vert + badge "(ajouté)"

**Articles RAG dynamiques selon le point sélectionné (écran 3/4)**
- Cliquer sur un item de la checklist met à jour simultanément : suggestions Opus, header "Point en cours", ET articles RAG
- `useEffect` sur `selectionCount` : si `selectedItem.clause` contient "7.1.5" → `RAG_ARTICLES["§7.1.5"]`, "8.7" → `RAG_ARTICLES["§8.7"]`, sinon → `RAG_ARTICLES.default`
- Cohérent avec la logique existante dans `handleAnalyser`

**Fichiers modifiés**
- `frontend/src/components/InspectionCapture.jsx` — `useEffect` RAG sur `selectionCount`

### Session du 2026-06-04 — Checklist éditable + colonne réponses suggérées oui/non IA

**Checklist éditable dans Brief (écran 2)**
- Les items de base de chaque section sont maintenant affichés individuellement (○ + texte)
- Bouton × au survol de chaque item de base → suppression via state `removedItemIds: Set<string>`
- Items extra ajoutés par l'auditeur conservent leur × permanent (comportement inchangé)
- Compteur dynamique : `section.items.filter(i => !removedItemIds.has(i.id)).length + extraItems.length`
- `removedItemIds` transmis à `onDemarrer(customSections, extraItemsBySectionId, removedItemIds)` puis propagé à `InspectionCapture` via `App.jsx`
- Dans `InspectionCapture` : filtre `.filter(it => !removedItemIds.has(it.id))` dans `baseChecklist`

**Fichiers modifiés**
- `frontend/src/components/Brief.jsx` — state `removedItemIds`, rendu items individuels, × par item, compteur dynamique
- `frontend/src/App.jsx` — state `removedItemIds`, `handleDemarrerInspection` étendu, prop transmise à `InspectionCapture`
- `frontend/src/components/InspectionCapture.jsx` — prop `removedItemIds`, filtre dans `baseChecklist`

**Colonne "Réponses suggérées" avec questions oui/non (écran 3/4)**
- Nouvelle 4ème colonne entre capture et constats : `col-span-3` (grid 3+4+3+2=12)
- Au clic sur un item de checklist : appel `POST /questions_oui_non` → 3 questions de vérification contextuelles générées par Claude
- Spinner "Génération des questions…" pendant le chargement
- Fallback silencieux sur 3 questions statiques si backend indisponible
- Boutons Oui/Non toggleables : vert si Oui sélectionné, rouge si Non, gris si non répondu
- Réponses réinitialisées à chaque changement d'item (`selectionCount`)
- Suppression du bloc "Question suggérée au responsable" redondant dans la colonne capture

**Fichiers modifiés**
- `backend/agent.py` — nouvelle fonction `generer_questions_oui_non(item_texte, clause, section_titre) -> list[str]`
- `backend/main.py` — nouvelle route `POST /questions_oui_non` + modèle `QuestionsOuiNonRequest`
- `frontend/src/api.js` — nouvelle fonction `getQuestionsOuiNon(itemTexte, clause, sectionTitre)`
- `frontend/src/components/InspectionCapture.jsx` — import, states `reponses`/`questionsOuiNon`/`questionsLoading`, `useEffect` sur `selectionCount`, colonne JSX, ajustement grid

### Session du 2026-06-04 — Grille de conformité + Plateforme fournisseur RATP

**Analyse des écarts cahier des charges UC28**
- Comparaison du CDC officiel (BU SPS, clients Apave / BV / RATP) avec l'état de l'application
- Manques identifiés : portail fournisseur, multi-normes, capteurs, grilles d'évaluation, capitalisation expertise BV

**Grille de conformité (ReportView — écran 5)**
- Nouvelle carte "Grille de conformité" dans la colonne gauche du rapport
- Score global en % calculé sur tous les constats (CONFORME=3pts, OBSERVATION=2pts, MINEURE=1pt, MAJEURE=0pt)
- 4 barres de progression par section ISO (§7.5, §7.1.5, §8.7, §7.2) colorées : vert ≥80%, orange 50-79%, rouge <50%
- Sections non auditées affichent `—`
- Import `CHECKLIST` ajouté dans `ReportView.jsx`

**Fichiers modifiés**
- `frontend/src/components/ReportView.jsx` — import CHECKLIST, calcul `sectionScores`/`globalScore`, carte JSX

**Plateforme fournisseur (architecture biface BV ↔ RATP)**
- Toggle "👤 Marc Lefèvre / 🏢 Karim Belkacem (RATP)" dans le Dashboard
- Nouveau composant `SupplierPortal.jsx` : identité fournisseur, prochain audit BV, 3 docs pré-analysés, dépôt document → appel Claude réel
- Nouveau endpoint `POST /analyser_document_fournisseur` : analyse Claude du contenu documentaire, retourne `{ resume, sections_a_risque, points_controle, nc_historique }`
- `Brief.jsx` : section CR remplacée par "Documents portail RATP" (3 docs pré-chargés) + badge `⚠ RATP` sur §7.1.5 et §8.7

**Fichiers modifiés**
- `frontend/src/mockData.js` — ajout `SUPPLIER_DOCUMENTS`, `SUPPLIER_DOC_CONTENT`, `SUPPLIER_ALERTS`
- `backend/agent.py` — nouvelle fonction `analyser_document_fournisseur(nom, contenu) -> dict`
- `backend/main.py` — nouvelle route `POST /analyser_document_fournisseur` + modèle `DocumentFournisseurRequest`
- `frontend/src/api.js` — nouvelle fonction `analyserDocumentFournisseur(nom, contenu)`
- `frontend/src/components/SupplierPortal.jsx` — nouveau composant (création)
- `frontend/src/components/Dashboard.jsx` — import SupplierPortal, état `fournisseurMode`, toggle, rendu conditionnel
- `frontend/src/components/Brief.jsx` — import SUPPLIER_DOCUMENTS/SUPPLIER_ALERTS, section docs portail, badges ⚠

**Supports de pitch créés**
- `docs/Pitch_Jury_UC28.pptx` — deck jury 7 slides (hook → problème → architecture → démo → IA → impact → équipe)
- `docs/UC28_Demo_Deck.pptx` — deck démo mis à jour (BV client, RATP client final, Marc Lefèvre persona)
- `docs/FAQ_Pitch.md` — 14 questions/réponses jury en 4 catégories + checklist pré-scène
- `scripts/gen_pitch_jury.py` — script python-pptx pour Pitch_Jury_UC28.pptx
- `scripts/gen_demo_deck.py` — script python-pptx pour UC28_Demo_Deck.pptx

### Session du 2026-06-04 (suite) — Portail fournisseur au login + polish UI

**Sélecteur de rôle sur l'écran de connexion**
- Deux cartes cliquables sur LoginScreen : "👤 Marc Lefèvre (Auditeur BV)" / "🏢 Karim Belkacem (Responsable Qualité RATP)"
- Sélectionner "Karim Belkacem" → connexion → SupplierPortal directement (view "portail")
- Sélectionner "Marc Lefèvre" → connexion → ClientList (parcours normal)
- Email pré-rempli change selon le rôle sélectionné
- Toggle supprimé du Dashboard (plus propre, rôle choisi en amont)

**Fichiers modifiés**
- `frontend/src/components/LoginScreen.jsx` — state `selectedRole`, cartes rôle, `onLogin(selectedRole)`
- `frontend/src/App.jsx` — `onLogin` reçoit le rôle, view "portail", import SupplierPortal
- `frontend/src/components/SupplierPortal.jsx` — prop `onBack`, barre haut avec "← Changer de compte"
- `frontend/src/components/Dashboard.jsx` — suppression toggle `fournisseurMode` et SupplierPortal conditionnel

**Fix SSL RAG (proxy Capgemini)**
- `backend/rag.py` : ajout `os.environ.setdefault("HF_HUB_OFFLINE", "1")` et `TRANSFORMERS_OFFLINE` avant l'import sentence-transformers
- Modèle déjà en cache (`~/.cache/huggingface/hub/`) → plus d'appel réseau au démarrage

**Sélecteur de référentiel (Brief — écran 2)**
- 3 pills cliquables : ISO 9001:2015 ● ISO 14001:2015 ● ISO 45001:2018
- Défaut ISO 9001, sélection d'un autre référentiel affiche "✓ Checklist adaptée…"

**Feedback expertise BV (Inspection — écrans 3/4)**
- Après chaque résultat Claude : boutons "👍 Confirmer" / "✏️ Corriger"
- Corriger → textarea → "Envoyer la correction" → message de confirmation
- États `feedbackDonne` / `correctionTexte` réinitialisés à chaque Valider / Refaire

**Fix KPIs**
- `KPIS.audits_jour` corrigé de 3 → 4 pour correspondre aux 4 entrées de `AUDITS_TIMELINE`

**Équilibrage colonnes écran Inspection**
- Grid 3+4+3+2 → 3+3+3+3 (4 colonnes égales)
- `max-h-[calc(100vh-240px)]` → `min-h-0` + wrapper `flex flex-col` → hauteur uniforme
- Suppression de `max-w-screen-xl` → colonnes pleine largeur dynamique

**Fichiers modifiés**
- `frontend/src/components/InspectionCapture.jsx` — grid, hauteurs, largeur pleine page
- `frontend/src/components/Brief.jsx` — sélecteur référentiel
- `frontend/src/mockData.js` — `audits_jour: 4`

**Supports et documentation**
- `docs/PROMPTS_CLAUDE_DESKTOP.md` — 5 prompts prêts à l'emploi pour Claude Desktop (doc technique, Mermaid, spécifications, script démo, pitch deck)
- `docs/UC28_Claude_Desktop_Context.zip` — archive des 16 fichiers clés à fournir à Claude Desktop

### Session du 2026-06-05 — Harmonisation visuelle via système de tokens (ÉTAPE 1)

**Contexte**
- Direction design retenue : thème clair unique "dashboard SaaS", accents cyan Capgemini + émeraude UC28
- Approche par étapes validée avec le dev avant implémentation

**Système de tokens — source de vérité unique**
- `frontend/src/index.css` : variables CSS `:root` déjà correctes — conservées inchangées comme référence maîtresse
- `frontend/tailwind.config.js` : refonte complète
  - Suppression de la palette legacy `brand: { blue, cyan, teal, mint, lime, yellow, dark, mid, aria-* }` (21 lignes)
  - Les 14 tokens UC28 pointent maintenant vers les variables CSS (`'brand': 'var(--brand)'`, etc.) au lieu de hex en dur
  - Résultat : `bg-brand` → `background-color: var(--brand)` — modifier `index.css` suffit pour changer toute l'app

**Note technique — modificateurs d'opacité Tailwind avec CSS variables**
- Les modificateurs `/30`, `/5` (ex. `bg-brand/30`) ne fonctionnent PAS avec `var(--...)` en Tailwind v3 : le navigateur reçoit `rgb(var(--nc-majeure) / 0.3)` ce qui est du CSS invalide
- Solution retenue : fonds de cartes avec les couleurs sémantiques statiques Tailwind (`bg-red-50`, `bg-amber-50`, etc.) ; les badges (élément le plus visible) utilisent les tokens UC28

**Migration InspectionCapture.jsx (composant sample ÉTAPE 1)**
- 3 dictionnaires thème-dépendants (`CRITICITE_STYLE_CLASSIC`, `_AGILE`, `_ARIA`) → 1 seul dictionnaire `CRITICITE_STYLE`
- Ligne de sélection ternaire `const CRITICITE_STYLE = ag ? ... : ar ? ... : ...` supprimée
- Badges : `bg-nc-majeure`, `bg-nc-mineure`, `bg-observation`, `bg-conforme` (tokens)
- Fonds de cartes : `bg-red-50 border-red-200`, `bg-amber-50 border-amber-200`, etc. (statiques)
- Bordures latérales : `border-l-4 border-nc-majeure`, etc. (tokens)

**Fichiers modifiés**
- `frontend/tailwind.config.js` — suppression palette legacy, liaison tokens → variables CSS
- `frontend/src/components/InspectionCapture.jsx` — dictionnaire criticité unifié (3 → 1)

**Périmètre restant (à valider étape par étape)**
- ÉTAPE 2 : Header.jsx, Dashboard.jsx, Brief.jsx, ClientList.jsx — cartes + navigation
- ÉTAPE 3 : ReportView.jsx criticité + barres de conformité tokenisées
- ÉTAPE 4 (optionnelle) : LoginScreen.jsx — variante sombre écran d'accueil uniquement

### Session du 2026-06-05 — Harmonisation visuelle UC28 via tokens de design

**Système de tokens — source de vérité unique**
- `frontend/src/index.css` : `--bg` → `#EEF2F7`, ajout `--surface-sunk`, `--shadow-sm/md/lg/inset`, `--wash-cyan/emerald`, canaux RGB `--brand-rgb` etc.
- `frontend/tailwind.config.js` : suppression palette legacy, tous les tokens pointent vers CSS variables avec support opacité `rgb(var(--*-rgb) / <alpha-value>)`, ajout `boxShadow` custom, tokens `surface-sunk`, `wash-cyan`, `wash-emerald`

**Migration visuelle complète (5 étapes)**
- ÉTAPE 1 : InspectionCapture — 3 `CRITICITE_STYLE_*` (classic/agile/aria) → 1 dictionnaire unifié tokens
- ÉTAPE 2 : ClientList, Dashboard, Brief, Header — cartes `shadow-md` sans bordure, hover `shadow-lg -translate-y-px`, KPI `rounded-xl`
- ÉTAPE 3 : Panneaux internes → `bg-surface-sunk shadow-inset` (checklist items, articles RAG, questions Oui/Non, suivi actions, aperçu rapport)
- ÉTAPE 4 : En-têtes de section → `bg-wash-cyan rounded-md` + icône Lucide colorée ; ternaires `ag ?/ar ?` entièrement nettoyés dans InspectionCapture
- ÉTAPE 5 (ReportView) : anneau SVG de progression pour score global, pastilles pleines pour compteurs synthèse, barres conformité `h-2` + animation 700ms

**Fichiers modifiés**
- `frontend/src/index.css` — nouvelles variables CSS (profondeur + washes + canaux RGB)
- `frontend/tailwind.config.js` — tokens CSS variables + boxShadow custom
- `frontend/src/components/Dashboard.jsx` — réécriture complète
- `frontend/src/components/Brief.jsx` — réécriture complète
- `frontend/src/components/InspectionCapture.jsx` — réécriture complète (ternaires thème nettoyés)
- `frontend/src/components/ReportView.jsx` — réécriture complète + SVG ring `ScoreRing`
- `frontend/src/components/ClientList.jsx` — hover upgrade
- `frontend/src/components/LoginScreen.jsx` — suppression blocs logo BV/IA

**Bug fixes**
- Photo constat : `photoUrl` manquait dans `onAddConstat` ; passage blob URL → data URL (base64) pour éviter révocation après `setPhoto(null)`
- Suggestions sous observation : bloc JSX supprimé
- Réponses Oui/Non : transmises à Claude via `observationAvecReponses` dans `handleAnalyser`

### Session du 2026-06-05 — Portail fournisseur : sélecteur de nature, filtres, pictogrammes

**Sélecteur de nature de document**
- `TYPES_DOCUMENT` : 6 catégories, 37 types en `<select>` + `<optgroup>`
- Bouton "Déposer" désactivé tant que nature non choisie ; cas "Autres" → champ texte libre
- `typeDoc` stocké dans chaque document à l'upload et conservé après l'analyse

**Filtres catégorie + tri date**
- `CATEGORIE_META` : icône Lucide + couleurs par catégorie (ClipboardList/orange, BookOpen/brand, ListChecks/teal, Wrench/slate, ShieldAlert/amber, FileText/gris)
- `getGroupe(typeDoc)` : retrouve la catégorie parente d'un type
- Pills dynamiques (visibles si ≥1 doc dans la catégorie), tri Récent/Ancien via `triDate`
- `docsFiltres` = `docs.filter(...).sort(...)` — liste rendue à la place de `docs`

**Catégorisation des documents mockés**
- `CR_Audit_nov_2024.docx` → `typeDoc: "Compte-rendu d'inspection"` (Audit & Inspection)
- `Procedures_etalonnage_v3.pdf` → `typeDoc: "Procédure interne"` (Procédures & Règles internes)
- `Plan_qualite_2025.pdf` → `typeDoc: "Plan de prévention"` (Sécurité & Risques)

**Fichiers modifiés**
- `frontend/src/components/SupplierPortal.jsx` — TYPES_DOCUMENT, CATEGORIE_META, getGroupe, filtreCategorie/triDate, barre filtres, pictogrammes catégorie
- `frontend/src/mockData.js` — `typeDoc` ajouté sur les 3 entrées `SUPPLIER_DOCUMENTS`

### Session du 2026-06-05 — UX polish : tooltips, profil utilisateur, design cohérent

**Tooltips catégories portail fournisseur (SupplierPortal)**
- Champ `tooltip` ajouté dans `CATEGORIE_META` pour les 5 catégories métier
- Pills de filtre : wrapper `<div className="relative group">` + `<span>` absolu opacity-0 → opacity-100 au survol
- Dropdown custom remplace le `<select>` natif (impossible d'insérer du HTML dans `<optgroup>`)
  - Bouton déclencheur + panneau absolu avec en-têtes catégorie + icône `Info` bleue avec tooltip `top-full`
  - Fermeture au clic extérieur via `useEffect` + `dropdownRef`
- Bouton × suppression sur les documents uploadés (masqué si `doc.mock === true`)
- Nom `deposePar: "Eve Dupont"` + flag `mock: true` sur les 3 documents simulés
- Remplacement complet Karim Belkacem → **Mei Lin Zhang** (`meilin.zhang@ratp.fr`) dans 5 fichiers + photo `mei-lin-zhang.png`

**Menu profil utilisateur (Header — style Microsoft 365)**
- `LoginScreen.jsx` : `onLogin(roleId)` → `onLogin({ id, label, sublabel, email, avatar })`
- `App.jsx` : états `user` + `lang`, propagation `user/lang/onLangChange/onLogout` au Header
- `Header.jsx` : avatar cliquable (photo ou initiales), panneau dropdown :
  - Ligne "Capgemini | Se déconnecter"
  - Carte utilisateur (photo/initiales, nom, titre, email)
  - "Se connecter avec un autre compte"
  - Sélecteur langue FR/EN
- Photos ajoutées dans `frontend/public/` : `marc-lefevre.png`, `mei-lin-zhang.png`
- `showHeader` étendu à toutes les pages post-login (`view !== "login"`)

**Titres conjugués (2e personne du pluriel)**
- `ClientList.jsx` : "Sélectionner un client" → "Sélectionnez un client"
- `PlanningOverlay.jsx` : "📅 Générer mon planning" → "📅 Générez votre planning"

**Fichiers modifiés**
- `frontend/src/components/LoginScreen.jsx` — émission objet utilisateur complet, photo dans cartes rôle
- `frontend/src/components/Header.jsx` — menu profil complet
- `frontend/src/components/SupplierPortal.jsx` — dropdown custom, tooltips, ×, Eve Dupont, Mei Lin Zhang
- `frontend/src/components/Brief.jsx`, `ReportView.jsx` — Mei Lin Zhang
- `frontend/src/mockData.js` — deposePar, mock, Mei Lin Zhang
- `frontend/src/App.jsx` — user/lang states, showHeader étendu
- `frontend/public/marc-lefevre.png`, `frontend/public/mei-lin-zhang.png` — avatars

### Session du 2026-06-05 — Fond dégradé mint + badges icônes colorés

**Fond dégradé uniforme**
- `App.jsx` : `bg-gradient-to-br from-teal-50 to-cyan-100` sur le div racine
- Suppression `bg-canvas` / `bg-gray-50` sur les wrappers racines de 8 composants (Dashboard, Brief, ClientList, InspectionCapture, ReportView, PlanningOverlay, SelectionView, SupplierPortal)
- LoginScreen conserve son `bg-dark-teal`

**Badges icônes colorés (style SupplierPortal étendu à toutes les pages)**
- **Transport** : emojis 🚗🚇🚲 → Lucide `Car/Train/Bike` dans badge `w-7/8 h-7/8 rounded-lg` coloré (bleu/violet/vert)
  - `PlanningOverlay.jsx` : `TRANSPORT_OPTIONS` avec champ `Icon/bg/text`
  - `SelectionView.jsx` : `TRANSPORT_ICON` dict → `TRANSPORT_META` ; fix référence résiduelle (page blanche)
- **Dashboard cartes de mission** : badge statut `w-10 h-10 rounded-lg` en 1ère colonne
  - `STATUT_ICON_BADGE` : CheckCircle2 vert (TERMINÉ), PlayCircle brand (PROCHAIN), Building2 gris (PLANIFIÉ)
  - Trajets 🚗🚲🚶 → `Car/Bike/User` inline `size={10}`
- **En-têtes de section** : mini-badge `w-5 h-5 rounded bg-brand/15` sur tous les h2/h3 (9 headers)
  - Headers sans icône reçoivent : `MessageSquare` (Réponses suggérées), `LayoutList` (Synthèse), `Wrench` (Plan d'action)
- **Checklist items** : `✓●○` → `CheckCircle2/Circle/Circle` Lucide (emerald/brand/divider)

**Fichiers modifiés**
- `frontend/src/App.jsx` — gradient racine
- `frontend/src/components/Dashboard.jsx` — STATUT_ICON_BADGE, badge carte, icônes trajet
- `frontend/src/components/PlanningOverlay.jsx` — TRANSPORT_OPTIONS refactorisé
- `frontend/src/components/SelectionView.jsx` — TRANSPORT_META, fix référence
- `frontend/src/components/Brief.jsx` — mini-badges h2
- `frontend/src/components/InspectionCapture.jsx` — mini-badges h3, CheckCircle2/Circle items
- `frontend/src/components/ReportView.jsx` — mini-badges h3, imports LayoutList/Wrench

### Session du 2026-06-05 — Refonte layout : fond neutre, shell app, grille 12 col, primitives

**Contexte**
Demande formelle de cohérence visuelle via un prompt structuré en 4 étapes (fond, shell, grille, primitives). Thème agile verrouillé, sélecteur de thème supprimé de l'écran login.

**Étape 1 — Fond neutre**
- `App.jsx` : `bg-gradient-to-br from-teal-50 to-cyan-100` → `bg-canvas` (#EEF2F7)
- Dégradé subtil `rgba(0,112,173,0.04)` sur 120px sous le Header (inline style)
- LoginScreen : `bg-dark-teal` supprimé → fond app uniforme

**Étape 2 — Shell d'application**
- `index.css` : tokens `--container-max`, `--page-pad`, `--gutter`, `--card-radius`, `--card-pad`, `--space-1..4`
- Classes CSS globales : `.page-bar`, `.page-content`, `.grid-12`, `.card`, `.section-label`
- `PageLayout.jsx` (nouveau) : shell à 3 slots (left/center/right) + extraBar + page-content
- `Card.jsx` (nouveau) : wrapper primitif carte
- Barres d'action : `bg-surface border-b px-6 py-3 flex justify-between shadow-sm` → `.page-bar` sur tous les écrans

**Étape 3 — Grille 12 colonnes**
- `.grid-12` = `grid-template-columns: repeat(12, 1fr); gap: var(--gutter)`
- ClientList : `max-w-2xl space-y-3` → `grid-12` + `col-span-8 col-start-3`
- PlanningOverlay : `flex gap-6` → `grid-12` col-8 + col-4
- SelectionView : `flex gap-6` → `grid-12` col-8 + col-4
- Brief : `grid-cols-1/2/3 responsive` → `grid-12` 3×col-4
- ReportView : `grid grid-cols-12 gap-6` → `.grid-12` (col-spans conservés)
- SupplierPortal : col-3/9 → col-4/8

**Étape 4 — Primitives**
- `.card` remplace `bg-surface rounded-xl shadow-md p-4/p-5` partout (normalise sur p-4)
- `.section-label` normalise les 6 variantes de labels (mb-1/3/4 → mb-3, flex gap-1.5)
- InspectionCapture : grille 3+3+3+3 conservée (4 panneaux métier)

**Fix hauteur pleine page — InspectionCapture**
- `App.jsx` : `flex-1` → `flex-1 flex flex-col` sur le wrapper de contenu
- `InspectionCapture` : `min-h-screen flex flex-col` → `flex-1 flex flex-col min-h-0`
- Résultat : colonnes remplissent exactement viewport − Header − page-bar

**Sélecteur de thème**
- `LoginScreen.jsx` : bloc "DESIGN / Classique / Agile / Aria" supprimé
- `App.jsx` : `useState("classic")` → `useState("agile")` — thème Agile verrouillé

**Corrections démo**
- SupplierPortal : barre "← Changer de compte" redondante supprimée
- PlanningOverlay : "Générer mon planning" → "Générez votre planning" (bouton)
- ClientList : double bandeau dark-teal supprimé (header interne remplacé par titre dans page-content)
- mockData : heures 9 missions AUDITS_TIMELINE dynamiques depuis `Date.now()` (T−120, T−45 = TERMINÉ ; T+45 = PROCHAIN ; T+110…T+480 = PLANIFIÉ)
- Brief : `grid-cols-3` fixe → responsive + `auto-rows-min`

**Fichiers créés**
- `frontend/src/components/PageLayout.jsx`
- `frontend/src/components/Card.jsx`

### Session du 2026-06-05 — InspectionCapture : 3 colonnes, RAG tooltip, données RATP

**Layout 3 colonnes (4/4/4)**
- Refonte de 4 colonnes 3/3/3/3 → 3 colonnes 4/4/4
- Col 1 : Check-list dynamique
- Col 2 : Capture en cours + Réponses suggérées + Articles RAG (empilés, scroll)
- Col 3 : Constats
- RAG déplacé de col-2 vers bas de col-2, sous les réponses

**Tooltip articles RAG**
- Chaque article : `group/art` + bulle `absolute bottom-full left-0 w-64 z-50`
- Affiche clause, titre complet et extrait sans troncature au survol (`opacity-0 → opacity-100`)

**Données enrichies — 10 missions RATP**
- Titres professionnels avec clauses ISO (§7.1.5, §7.2, §8.4, §8.5.1, §8.7, §9.1, §10.2)
- Champs ajoutés : `referentiel`, `effectif`, `responsable`, `nc_ouvertes`
- Sites RATP réalistes : Fontenay, Sucy, Boulogne, Montrouge, Vincennes, Châtelet (Viso), Aubervilliers, Créteil, Siège DQ
- 2 audits Bureau, 2 Viso, 6 Visite terrain

**Bouton Démarrer — toutes les missions sélectionnées**
- `SelectionView.jsx` : condition `statut === "PROCHAIN"` → `statut !== "TERMINÉ"`
- PROCHAIN : vert émeraude (`bg-brand-emerald`) ; PLANIFIÉ : bleu brand (`bg-brand`)

**Fichiers modifiés**
- `frontend/src/components/InspectionCapture.jsx` — layout 3 col, RAG tooltip
- `frontend/src/components/SelectionView.jsx` — bouton Démarrer universel
- `frontend/src/mockData.js` — données enrichies 10 missions

### Session du 2026-06-05 — Carte routing réel, statut dynamique, TC renommé

**Routing OSRM réel sur la carte (MapCard.jsx)**
- Remplace la ligne droite Leaflet par un itinéraire routier réel via OSRM public (`router.project-osrm.org`)
- Profils : `driving` (voiture/TC), `cycling` (vélo), `foot` (pied)
- `useEffect` avec cleanup (cancel) sur changement de waypoints ou mode de transport
- Fallback silencieux sur ligne droite si réseau indisponible (proxy Capgemini)
- Légende : indicateur `"calcul…"` pendant le fetch, disparaît ensuite
- Prop `transport` ajoutée à MapCard ; passée depuis SelectionView (`transportActif`) et PlanningOverlay (`transport`)

**"TC" → "Transports en commun"**
- `PlanningOverlay.jsx` : `label: "TC"` → `label: "Transports en commun"`
- `SelectionView.jsx` : `label: "Transport en commun"` → `label: "Transports en commun"`

**Statut dynamique basé sur l'heure courante (SelectionView)**
- `displayStatut` calculé live pour chaque mission via `new Date()`
  - `heureMin + duréeMin < now` → TERMINÉ (carte grisée, titre barré, "✓ Terminé")
  - `heureMin ≤ now + 45 min` → PROCHAIN (bouton vert)
  - sinon → PLANIFIÉ (bouton bleu brand)
- Remplace `item.statut` figé d'AUDITS_TIMELINE — résout l'absence de bouton sur les missions PLANIFIÉ

**Fichiers modifiés**
- `frontend/src/components/MapCard.jsx` — OSRM routing, prop transport
- `frontend/src/components/PlanningOverlay.jsx` — label TC, prop transport vers MapCard
- `frontend/src/components/SelectionView.jsx` — label TC, prop transport, displayStatut dynamique

### Session du 2026-06-07 — Badge Auditeur, mapping RAG par clause, schémas d'architecture

**Badge « Auditeur » sur les contenus ajoutés par l'auditeur**
- Brief (écran 2) : les points ajoutés dans une section IA existante (`extraItems`) portent le même badge bleu « Auditeur » que les sections custom, positionné **à droite** du texte de l'item
- Inspection (écran 3/4) : badge « Auditeur » rendu visible aussi dans la check-list, pour les sections custom **et** les points ajoutés
  - `baseChecklist` marque les contenus auditeur avec un flag `auteur: "auditeur"` (extra items + sections custom + leurs items)
  - Le flag survit aux mises à jour de statut (le reducer fait `{ ...it }`)
  - Le texte `(ajouté)` sur les sections custom est remplacé par le badge ; badge des items aligné à droite (`ml-auto`)

**Mapping articles normatifs (RAG) par clause — pilotage par la donnée**
- Avant : `if (clause.includes("7.1.5")) … else if ("8.7") … else default` → seules S2 et S3 avaient des articles pertinents ; S1 (§7.5) et S4 (§7.2) retombaient sur le paquet `default` incohérent
- Après : helper unique `articlesForClause(clause)` au niveau module
  - Match exact `RAG_ARTICLES[clause]` (clé avec `§`, ex. `§7.5`)
  - Repli tolérant sur le format backend **sans `§`** (`num.includes(k.replace(/§/g,""))`) — car la table `clauses_iso` stocke `7.1.5`, `8.7`… sans symbole
  - Repli final sur `RAG_ARTICLES.default`
- Utilisé aux 2 endroits : `useEffect` au clic d'un point **et** `handleAnalyser` (clause renvoyée par le backend)
- 2 nouveaux paquets ajoutés dans `RAG_ARTICLES` : `§7.5` (documentation : §7.5.2/7.5.3/7.5.3.2) et `§7.2` (compétences : §7.2/7.2(b)/7.3)
- Avantage : ajouter une section avec une clause répertoriée la relie automatiquement, sans toucher au code
- Popover ⓘ « Articles normatifs — RAG » mis à jour pour refléter les 4 clauses cartographiées

**Correction doc — stockage `data/`**
- `audit.db` (SQLite) est en réalité **versionné** dans le dépôt (et non gitignoré) ; déjà seedé (tables `clauses_iso`, `sites`, `audits_historiques`)
- Le seed `data/iso_9001_clauses.json` n'est lu qu'au premier `init_db()` si la table est vide ; il n'est pas versionné
- Les clauses en base sont stockées **sans `§`** — d'où le repli tolérant de `articlesForClause()`

**Schémas d'architecture (PNG 1920×1080, matplotlib)**
- `docs/architecture_technique_UC28.png` — composants React, FastAPI (main/agent/rag/database), Claude API, SQLite, FAISS + sentence-transformers, OSRM, flux REST
- `docs/architecture_fonctionnelle_UC28.png` — parcours 3 actes (AVANT/PENDANT/APRES), swimlanes acteurs, fonctions métier, valeurs produites
- Scripts sources : `scripts/gen_architecture_diagram.py` et `scripts/gen_functional_diagram.py`
- Note : pas de police emoji dans l'environnement (DejaVu Sans) → libellés en texte simple

**Fichiers modifiés / créés**
- `frontend/src/components/Brief.jsx` — badge Auditeur sur extra items (à droite)
- `frontend/src/components/InspectionCapture.jsx` — flag `auteur`, badge dans la check-list, helper `articlesForClause`, popover RAG
- `frontend/src/mockData.js` — paquets `§7.5` et `§7.2` dans `RAG_ARTICLES`
- `uc28-inspection/CLAUDE.md` — correction stockage `data/`
- `docs/architecture_technique_UC28.png`, `docs/architecture_fonctionnelle_UC28.png` — créés
- `scripts/gen_architecture_diagram.py`, `scripts/gen_functional_diagram.py` — créés

---

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

### Session du 2026-06-08 — Cohérence scope, Brief branché sur DB, UX Inspection

**Cohérence labels scope ↔ checklist**
- `SCOPE_OPTIONS` : labels alignés sur les titres exacts des sections `CHECKLIST` (S1–S6) ; `&` et noms canoniques
- Section S1 renommée "Maîtrise documentaire" (était "Maîtrise des informations documentées") — dans `CHECKLIST.titre` (×2) et `SCOPE_OPTIONS.label`
- `AUDIT_COURANT.scope` mis à jour avec les nouveaux labels

**Brief.jsx branché sur audit.db**
- Import `getSite` depuis `../api` ; nouveau state `siteData`
- `useEffect` au montage → `GET /api/sites/RATP-SUC` ; fallback silencieux sur `AUDIT_COURANT` si backend absent
- Champs `nom`, `localisation`, `effectif`, `responsable_qualite` surchargés depuis la DB dès réception
- `historiqueAudits` = `siteData.historique_audits` (normalisé) ou `AUDITS_PRECEDENTS` en fallback
- `audit.db` : `responsable_qualite` RATP-SUC corrigé (Karim Belkacem → **Mei Lin Zhang**) ; `themes_recurrents` alignés sur les nouveaux labels scope
- CHECKLIST, SCOPE_OPTIONS, SUPPLIER_DOCUMENTS restent en mockData (pas d'équivalent DB)

**Vignette photo au survol dans les constats**
- Remplacement du lien texte "Voir la photo" par une vignette `40×40px` dans chaque carte constat
- Survol → aperçu `192px` affiché au-dessus via `group-hover/photo` CSS — aucun state supplémentaire
- `pointer-events-none` sur l'aperçu pour ne pas bloquer les interactions

**Zone de capture grisée sans sélection**
- Colonne centre de l'écran Inspection : `opacity-40 + pointer-events-none` tant qu'aucun point de checklist n'est sélectionné
- Transition `duration-200` ; redevient pleinement active dès qu'un item est cliqué

**Fix `toStatement` — inversions verbales françaises**
- Bug : les formes comme `figurent-elles`, `comporte-t-il`, `dispose-t-on` n'étaient pas dans le dictionnaire → la question restait sans transformation dans l'observation
- Ajout d'un fallback regex général : `/(\w+)-t?-?(il|elle|ils|elles|on)\b/` appliqué après le dictionnaire des auxiliaires (est/sont/a/ont/avez)
- Affirmative : supprime le `-[pronom]`, garde le verbe seul ; Négative : `ne [verbe] pas`
- Dictionnaire enrichi : `est-on`, `a-t-elle`, `y-a-t-il`

**Fichiers modifiés**
- `frontend/src/mockData.js` — labels SCOPE_OPTIONS canoniques, titre S1, AUDIT_COURANT.scope
- `frontend/src/components/Brief.jsx` — getSite import, siteData state, useEffect fetch, historiqueAudits
- `data/audit.db` — responsable_qualite + themes_recurrents RATP-SUC
- `frontend/src/components/InspectionCapture.jsx` — vignette photo, grisage colonne capture, fix toStatement

### Session du 2026-06-09 — Boucle fournisseur fermée : transmission rapport BV + documents téléchargeables

**Transmission du rapport BV vers le portail fournisseur RATP (boucle fermée)**
- Scénario : Marc Lefèvre (BV) termine l'audit → ReportView → "⬆ Portail RATP" → le rapport apparaît dans le portail quand Mei Lin Zhang (RATP) se connecte
- `App.jsx` : state `savedReports` persisté dans `localStorage("bv_saved_reports")` (survit F5 + changement de compte) + `handleSaveReport`
- Props : `onSaveReport={handleSaveReport}` → ReportView ; `externalDocs={savedReports}` → SupplierPortal
- `ReportView.jsx` : bouton "⬆ Portail RATP" dans la page-bar → "✓ Transmis au portail" après clic ; `handleTransmettrePortail` génère le DOCX via `exportDocx`, le convertit en `dataUrl` (base64) et appelle `onSaveReport({ id: rapport-bv-{timestamp}, fromBV: true, dataUrl, insights, ... })`
- `SupplierPortal.jsx` : `useEffect` fusionne `externalDocs` (dédup par `id`), badge bleu "Bureau Veritas" (`doc.fromBV`), tri corrigé `id.split("-").pop()` (les IDs `rapport-bv-{ts}` cassaient `replace("doc-","")`), × masqué sur les rapports BV (`!doc.mock && !doc.fromBV`)
- `"Rapport d'audit BV"` ajouté dans `TYPES_DOCUMENT` (groupe Audit & Inspection) pour le filtre catégorie

**Documents fournisseur réellement téléchargeables**
- Les 3 docs mock (`CR_Audit_nov_2024.docx`, `Procedures_etalonnage_v3.pdf`, `Plan_qualite_2025.pdf`) n'avaient que des métadonnées (`url` jamais utilisé) — désormais de vrais fichiers générés
- `scripts/gen_supplier_docs.py` : génération avec contenu RATP réaliste (python-docx + reportlab) → `frontend/public/documents/` (servi statiquement)
  - DOCX : CR d'inspection BV (tableau NC §7.1.5 majeure + §8.7 mineure, plan d'actions, signatures)
  - PDF métrologie : inventaire équipements (3 clés dynamométriques périmées en rouge), références COFRAC, révisions
  - PDF plan qualité : objectifs, plan d'actions correctives (statuts colorés CLÔTURÉ/EN COURS/NON ALLOUÉ/PARTIEL), calendrier
- `SupplierPortal.jsx` : bouton "⬇ Télécharger" étendu à `doc.url` (`(doc.dataUrl || doc.url)`, href `doc.dataUrl ?? doc.url`) — fichiers statiques ET rapports BV base64
- `Brief.jsx` : icône ⬇ cliquable (`<a href={doc.url} download>`) sur chaque doc de la section "Documents portail RATP"

**Cohérence données planning**
- `ClientList.jsx` : `missions: 4` (codé en dur) → `AUDITS_TIMELINE.length` (= 10) ; import `AUDITS_TIMELINE`
- `mockData.js` : `KPIS.audits_jour` 4 → 10 (alignement avec les 10 missions)

**Polish ReportView**
- ⓘ tooltip "Grille de conformité" (version minimaliste : sens du taux + code couleur, sans formule mathématique) ; state `showConformiteInfo`, imports `Info`/`X`
- Photo terrain affichée dans le rapport si `c.photoUrl && !anonymise` (masquée en mode RGPD car les photos peuvent contenir des éléments identifiables)
- Bouton "Nouvel audit →" retiré de la page-bar

**Renommage scope (Brief)**
- "Ajouter un domaine" → "Ajouter un scope" ; "Tous les domaines…" → "Tous les scopes sont déjà sélectionnés"

**Script de démo**
- `docs/Script_Demo_UC28_Opus48.md` réécrit en v4 : ÉPILOGUE "La boucle se referme", 🔁 récurrences (Acte II), ⓘ audits précédents, 10 missions, persona Mei Lin Zhang, nouveaux Q&R, astuce purge localStorage

**Fichiers créés / modifiés**
- `scripts/gen_supplier_docs.py` — créé
- `frontend/public/documents/{CR_Audit_nov_2024.docx, Procedures_etalonnage_v3.pdf, Plan_qualite_2025.pdf}` — créés
- `frontend/src/App.jsx` — savedReports + localStorage + props
- `frontend/src/components/ReportView.jsx` — bouton Portail RATP, handler, tooltip conformité, photo, retrait Nouvel audit
- `frontend/src/components/SupplierPortal.jsx` — externalDocs, badge BV, tri, téléchargement url, type doc BV
- `frontend/src/components/Brief.jsx` — lien téléchargement docs, renommage scope
- `frontend/src/components/ClientList.jsx` — missions = AUDITS_TIMELINE.length
- `frontend/src/mockData.js` — KPIS.audits_jour 10
- `docs/Script_Demo_UC28_Opus48.md` — v4
