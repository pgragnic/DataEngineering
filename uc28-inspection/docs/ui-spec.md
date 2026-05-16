# UC 28 — Spec UI · Parcours complet

> Source de vérité pour l'implémentation des 4 écrans de BV·Inspect.
> À placer dans `docs/ui-spec.md` du repo.
> Référez-la dans vos prompts Claude Code : *« implémente la zone X comme spécifié dans `docs/ui-spec.md` §Y »*.
>
> **Wireframes associés** (dans `docs/wireframes/`) :
> - `UC28_Screen1_Dashboard.png` — Dashboard
> - `UC28_Screen2_Brief.png` — Brief & génération check-list
> - `UC28_UI_Layout.png` — Capture (écran principal de démo)
> - `UC28_Screen3_Report.png` — Revue & envoi
> - `UC28_Mockup.html` — Mockup HTML interactif de l'écran capture

---

## 1. Vue d'ensemble

L'app est un parcours en **4 écrans** qui suit la journée d'un inspecteur :

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  ÉCRAN 1     │ →  │  ÉCRAN 2     │ →  │  ÉCRAN 3     │ →  │  ÉCRAN 4     │
│  Dashboard   │    │  Brief &     │    │  Capture     │    │  Revue &     │
│  « Mes       │    │  génération  │    │  (zone reine │    │  envoi       │
│   audits »   │    │  check-list  │    │  de la démo) │    │  pré-rapport │
│              │    │  (Agent 1)   │    │  (Agent 2)   │    │  (Agent 3)   │
│  08h00       │    │  14h25       │    │  14h32-14h56 │    │  14h56-14h57 │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
```

**Sources de vérité.** L'écran 3 (capture) est le moment spectacle de la démo et concentre ~60% du dev. Les écrans 1, 2, 4 cadrent le parcours mais doivent rester légers (max 1-2 jours dev cumulés).

**Format cible.** Web app desktop. Viewport de référence **1440×900**. Doit fonctionner sans dégradation sur 1280×800 et 1920×1080. Pas de responsive mobile (hors scope démo).

**Mode d'affichage.** Plein écran navigateur. Aucune sidebar Next.js, aucun header global au-dessus. Chaque page consomme tout le viewport.

---

## 2. Cadre technique

**Framework.** Next.js 14 App Router (cf. blueprint §2).

**Polices.**
- UI : **Inter** (poids 400, 500, 600, 700)
- Monospace (timer, scores, badges techniques) : **JetBrains Mono** (poids 500, 700)
- Charger via `next/font/google` dans `app/layout.tsx`

**State management.** **TanStack Query** (recommandé) ou SWR. Pas de Redux, pas de Zustand pour ce scope. L'état partagé entre écrans transite par l'`inspection_id` dans l'URL ; tout est repersisté côté backend.

**Animations.** **Framer Motion** (`npm i framer-motion`) pour les transitions entre écrans et les micro-interactions.

---

## 3. Design tokens

Définis dans `tailwind.config.ts`. Identiques au deck, au schéma d'architecture et au mockup HTML.

```ts
// tailwind.config.ts — section colors
colors: {
  uc: {
    "bg-dark":   "#0F2027",
    "bg-panel":  "#164E5E",
    "panel":     "#F8FAFC",
    "primary":   "#134E5E",
    "primary-2": "#2C7A7B",
    "accent":    "#10B981",
    "accent-lt": "#6EE7B7",
    "accent-50": "#ECFDF5",
    "alert":     "#F59E0B",
    "alert-50":  "#FEF3C7",
    "danger":    "#DC2626",
    "danger-50": "#FEE2E2",
    "text-dark": "#0F172A",
    "text-body": "#334155",
    "text-mute": "#64748B",
    "border":    "#CBD5E1",
  },
},
```

**Spacing.** Tailwind par défaut. Padding intérieur des panneaux : `p-4` ou `p-6`. Gap entre cartes : `gap-3`.

**Coins arrondis.** `rounded-lg` (8px) par défaut. `rounded-xl` (12px) sur les gros panneaux. `rounded-full` sur les pastilles de score.

**Ombre.** `shadow-sm` discret sur les cartes blanches. Pas d'ombres marquées.

---

## 4. Écran 1 · Dashboard « Mes audits du jour »

**Route.** `/`

**Quand.** Antoine arrive au bureau à 8h, ouvre BV·Inspect. C'est sa première interaction de la journée.

**Composant racine.** `<DashboardPage />`

### 4.1 Layout

```
┌─────────────────────────────────────────────────────────────┐
│  HEADER (variant : pas de status pill, pas de timer)        │
├─────────────────────────────────────────────────────────────┤
│  KPI STRIP — 4 cartes horizontales                          │
├──────────────────────────────────┬──────────────────────────┤
│                                  │                          │
│  AUDITS DU JOUR (col-span-8)     │  RÉCURRENCES (col-span-4)│
│  4 cartes verticales             │  3 cartes verticales     │
│  + carte « prochain » mise       │                          │
│    en valeur (bord accent)       │                          │
│                                  │                          │
├──────────────────────────────────┴──────────────────────────┤
│  FOOTER (variant : statut auditeur, pas de CTA)             │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 Composants

**`<HeaderBar variant="dashboard" />`.** Titre = *« Mes audits — [date du jour] »*. Sous-ligne = *« [Entreprise] · [Nom auditeur] · [Ville] »*. Pas de status pill, pas de timer. Voir §6.2 pour le détail du header partagé.

**`<KpiStrip />`.** 4 KPIs en row, chaque carte = blanche avec bord gauche coloré 4px. Données depuis `GET /api/dashboard/kpis`.

| KPI | Source | Couleur bord |
|---|---|---|
| `N` audits planifiés aujourd'hui | `audits_today_count` | `uc-accent` |
| `N` audits ce mois | `audits_month_count` | `uc-primary` |
| `Xj` délai moyen audit → rapport | `avg_delay_days` | `uc-accent` |
| `N` récurrences à vérifier | `pending_recurrences_count` | `uc-alert` |

**`<AuditList />`.** Liste verticale de `<AuditCard />`. Tri chronologique. La carte du « prochain audit à démarrer » a un bord `uc-accent` 2px + léger fond `uc-accent-50`.

**`<AuditCard />`.**
- Colonne gauche (15%) : heure (gros, monospace, couleur selon état) + ville + status pill
- Colonne centre (65%) : nom client + lieu + scope
- Colonne droite (20%) : action contextuelle
  - Status `prochain` → bouton **« Démarrer → »** (`btn-primary` accent)
  - Status `terminé` → lien **« Voir le rapport ↗ »**
  - Status `planifié` → texte muet **« À HHhMM »**

**`<RecurrencesList />`.** 3 cartes verticales empilées. Chacune = client + référence article + date d'ouverture + label court. Bord gauche `uc-alert`. Clic sur une carte → modale de détail (stub hors démo).

**`<DashboardFooter />`.** Pas de CTA. Juste la signature auditeur (« Antoine Mercier · Lead Auditor · Lyon · [heure] · [statut libre] »).

### 4.3 États

- **Loading** : skeleton sur les 3 zones, 200ms max
- **Empty** (rare) : *« Aucun audit planifié aujourd'hui »* avec bouton stub *« Planifier un audit »*
- **Active** : carte du prochain audit pré-sélectionnée

### 4.4 Interactions

- Clic sur **« Démarrer → »** sur la carte mise en valeur → navigation vers `/inspection/[id]/brief` (écran 2)
- Clic sur **« Voir le rapport ↗ »** → ouvre le DOCX dans un nouvel onglet (stub)
- Clic sur une récurrence → modale détail (stub)

### 4.5 Démo

Le pilote arrive sur cet écran en état nominal pré-chargé. Il pointe la carte ALPHA mise en valeur. Il narre. Il clique « Démarrer ». Durée à l'écran : **~5-8 secondes**.

---

## 5. Écran 2 · Brief & génération de la check-list

**Route.** `/inspection/[id]/brief`

**Quand.** 14h25 — Antoine arrive sur site, démarre l'inspection depuis le dashboard. L'Agent 1 génère la check-list pendant qu'il termine son café.

**Composant racine.** `<BriefPage inspectionId={id} />`

### 5.1 Layout

```
┌─────────────────────────────────────────────────────────────┐
│  HEADER (status pill « EN PRÉPARATION », pas de timer)      │
├──────────┬───────────────────────────────┬──────────────────┤
│          │                               │                  │
│  BRIEF   │  CHECK-LIST GÉNÉRÉE           │  AUDITS          │
│  CLIENT  │  (Agent 1)                    │  PRÉCÉDENTS      │
│  22%     │  50%                          │  28%             │
│          │                               │                  │
│          │  + bandeau status Agent 1     │                  │
│          │  + sections empilées          │  + Point         │
│          │                               │    d'attention   │
│          │                               │    (NC récurrente│
│          │                               │     repérée)     │
├──────────┴───────────────────────────────┴──────────────────┤
│  FOOTER : statut + CTA « Démarrer l'inspection → »          │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Composants

**`<BriefSummary />`.** Lecture seule, liste de champs en `label / value` empilés (client, SIRET, site, référentiel, scope, contact, durée prévue). Données depuis `GET /api/inspections/{id}`.

**`<ChecklistGenerator />`.** C'est le composant central. 3 états successifs :

1. **Triggering** (au montage si la check-list n'existe pas) : bandeau `uc-accent-50` + spinner + texte *« Préparation par l'Agent 1… »*. Reste affiché 10-15 sec.
2. **Generated** (après réponse Agent 1) : bandeau passe en `uc-accent-50` avec ✓ + texte *« Préparation terminée — N secondes »* + sous-texte *« N sections · N points · N articles ISO sourcés »*. En dessous, les sections apparaissent avec une animation **stagger** (fade-in 100ms d'intervalle).
3. **Editing** (stub hors démo) : l'inspecteur peut développer une section et prioriser un point.

Pour la démo, on arrive en état **Triggering**, l'animation 10-15 sec se joue (c'est volontaire : suspense pour le jury), puis transition vers **Generated**.

**`<SectionItem />`.** Carte par section. Code (S1, S2…) en gros monospace, titre, nombre de points, N mini-checkboxes (vides à ce stade — seront cochées dans l'écran 3 au fur et à mesure de la capture).

**`<AuditHistory />`.** 3 cartes empilées avec les audits précédents du client. Date (monospace) + référentiel + résultat coloré + note. Bord gauche selon couleur (rouge si NC ouverte, vert si OK).

**Point d'attention** (sous l'historique). Carte spéciale `uc-alert-50` qui signale qu'une NC précédente n'est pas clôturée et a été automatiquement ajoutée à la check-list par l'Agent 1. C'est l'élément qui montre l'intelligence du système au-delà du référentiel pur.

**`<BriefFooter />`.** Statut à gauche (« PRÉPARATION TERMINÉE · Check-list générée · 14 sec · 4 sections · 12 points · 1 point récurrent »). CTA principal **« Démarrer l'inspection → »** disabled tant que la génération n'est pas terminée.

### 5.3 États & déclencheurs

- `?regenerate=true` dans l'URL : force la régénération même si une check-list existe. Utile pour la démo *bascule platform*.
- L'écran déclenche automatiquement `POST /api/inspections/{id}/checklist` au mount si `inspection.checklist_json` est null.

### 5.4 Interactions

- Clic **« Démarrer l'inspection → »** → navigation `/inspection/[id]/capture`. L'inspection passe en status `ongoing`.
- Clic section → expand/collapse (lecture seule)
- Clic carte historique → modale détail (stub)

### 5.5 Démo

Le pilote arrive via le clic « Démarrer » du dashboard. **L'animation de génération est visible 10-15 secondes** — c'est délibéré, le jury voit l'Agent 1 travailler. Le narrateur explique pendant ce temps : *« L'Agent de Préparation croise le scope client avec les normes ISO et l'historique fournisseur pour produire une check-list contextuelle. »* Puis clic « Démarrer l'inspection ».

---

## 6. Écran 3 · Capture (l'écran reine)

**Route.** `/inspection/[id]/capture`

**Quand.** 14h32-14h56 — Antoine est sur site. Il dicte ses constats, l'Agent 2 classifie en temps réel.

**Composant racine.** `<CapturePage inspectionId={id} />`

C'est l'écran spectacle de la démo et celui qui concentre l'essentiel du dev. Il est documenté en détail ci-dessous.

### 6.1 Grille générale

5 zones empilées verticalement. **Pas de scroll de page** — tout tient dans le viewport.

```
┌─────────────────────────────────────────────────────────────┐
│  HEADER                                          h=72px      │
├──────────────┬───────────────────────┬──────────────────────┤
│              │                       │                      │
│  CHECK-LIST  │      CAPTURE          │      CONSTATS        │
│  w=22%       │      w=50%            │      w=28%           │
│              │                       │      h=auto, flex    │
│              │                       │      (max 4 visibles,│
│              │                       │       scroll au-delà)│
├──────────────┴───────────────────────┴──────────────────────┤
│  RAG TRANSPARENCY (3 chunks horizontaux)        h=180px      │
├─────────────────────────────────────────────────────────────┤
│  FOOTER (stats + CTA)                           h=110px      │
└─────────────────────────────────────────────────────────────┘
```

Implémentation Tailwind du conteneur principal :

```tsx
<div className="h-screen w-screen bg-uc-panel flex flex-col overflow-hidden">
  <HeaderBar variant="capture" />
  <main className="flex-1 grid grid-cols-12 gap-4 p-4 min-h-0">
    <Checklist  className="col-span-3" />
    <Capture    className="col-span-6" />
    <Findings   className="col-span-3" />
  </main>
  <RagTransparency />
  <CaptureFooter />
</div>
```

### 6.2 Header partagé

**Hauteur.** 72px. Fond `uc-bg-dark`. Texte blanc. Utilisé par les 4 écrans avec des variantes.

| Élément | Capture | Brief | Dashboard | Report |
|---|---|---|---|---|
| Logo `BV·INSPECT` | ✅ | ✅ | ✅ | ✅ |
| Titre inspection | `Audit ISO 9001 · Fournisseur ALPHA` | idem | `Mes audits — [date]` | idem capture |
| Sous-ligne | `Site Tours · Auditeur : Antoine Mercier` | idem | `BVC · Antoine Mercier · Lyon` | `Site Tours · audit terminé à 14h56` |
| Status pill | ❌ | `EN PRÉPARATION` (`uc-alert`) | ❌ | `AUDIT TERMINÉ` (`uc-accent`) |
| Sélecteur référentiel | ✅ | ❌ | ❌ | ❌ |
| Avatar auditeur | ✅ | ✅ | ✅ | ✅ |
| Timer `MM:SS` | ✅ | ❌ | ❌ | ❌ |

**Sélecteur référentiel** (capture uniquement) : dropdown `<select>` stylé. Valeur active `ISO 9001`. Options `ISO 9001`, `NFC 15-100`, `ATEX`. Fond `uc-bg-panel`, bord `uc-accent`. Au changement → `confirm()` natif (*« Régénérer la check-list pour NFC 15-100 ? »*), puis `POST /api/inspections/{id}/checklist?referential=NFC_15100` avec loading skeleton dans la zone check-list. **C'est le wow moment "bascule platform"** — soigner la transition.

**Timer** : compteur côté client (`useEffect` + `setInterval`), démarré depuis `inspection.started_at`.

### 6.3 Zone check-list (gauche, 22%)

**Composant.** `<ChecklistView checklist={inspection.checklist_json} activePointId={currentPointId} validatedPointIds={...} />`

```
CHECK-LIST DYNAMIQUE
(générée par Agent 1)

▾ S1 — Documents (§7.5)
   ✓ P1  Procédure achats à jour
   ◐ P2  Contrôle réception tracé
   ○ P3  Revues management

▸ S2 — Achats (§8.4)
▸ S3 — Compétences (§7.2)
▸ S4 — Sécurité site (§7.1.4)

[légende en bas]
```

**État de chaque point :**

| Marqueur | État | Couleur |
|---|---|---|
| `✓` | validé (≥ 1 constat lié) | `uc-accent` |
| `◐` | actif (en cours de capture) | `uc-alert` |
| `○` | à venir | `uc-text-mute` |

**Interactions.**
- Clic sur une section → toggle expand/collapse
- Clic sur un point → définit `activePointId` (envoyé au composant Capture comme contexte)
- Aucun champ éditable, lecture seule

**État chargement** (pendant régénération sur bascule référentiel) : skeleton shimmer 5 sec max, puis fade-in de la nouvelle check-list.

### 6.4 Zone capture (centre, 50%) · **la zone reine**

**Composant.** `<CaptureZone activePointId={...} onConstatValidated={(c) => ...} />`

#### 6.4.1 Sous-zone Mic + waveform (haut)

- Boîte rectangulaire fond `uc-bg-dark`, h=160px
- Gauche : cercle 60×60, fond `uc-danger`, point pulsant `uc-alert` au centre (`animate-pulse` Tailwind)
- Droite : visualisation waveform temps réel. **Implémentation** : Web Audio API + `AnalyserNode` + `requestAnimationFrame` qui dessine 35 barres verticales sur un `<canvas>`. Hauteur des barres = `getByteFrequencyData()`. Couleurs `uc-accent` et `uc-accent-lt` en alternance
- Bas de la boîte : transcription temps réel sur 1 ligne, italique, `uc-accent-lt`, format `« ... »`

**API utilisée.** `webkitSpeechRecognition` (alias `SpeechRecognition`). Langue `fr-FR`. `continuous: true`, `interimResults: true`. Stop manuel via clic sur le mic.

#### 6.4.2 Carte classification (milieu) · le résultat magique

Visible **après** validation de la transcription (l'inspecteur clique « Valider » ou s'arrête de parler X secondes).

Pendant le call à l'Agent 2 (≈ 2-3 sec) :
- Carte en état loading : fond `uc-bg-dark`, bord `uc-accent` qui pulse, contenu = 3 lignes shimmer + texte « Analyse en cours… » centré
- **En parallèle, la bande RAG en bas se peuple** avec les 3 chunks remontés (voir §6.6). Synchrone visuellement

Une fois la réponse reçue, transition douce (fade + slide-up 200ms) vers l'état final :

```
┌─────────────────────────────────────────────────────────────┐
│ [NC MAJEURE]  [ISO 9001 §7.1.4]  [✓ sourcé]                 │
│                                                              │
│ Constat reformulé :                                          │
│ La sortie de secours du bâtiment B est obstruée              │
│ par un chariot de stockage.                                  │
│                                                              │
│ Action corrective suggérée :                                 │
│ Libérer la sortie · rappel consignes magasinier ·            │
│ ajout au plan d'audit sécurité annuel.                       │
└─────────────────────────────────────────────────────────────┘
```

**Tokens visuels :**

| Badge | Couleur fond | Texte |
|---|---|---|
| NC MAJEURE | `uc-danger` | blanc, `font-bold`, `text-xs`, `tracking-wider` |
| NC MINEURE | `uc-alert` | blanc, `font-bold`, `text-xs` |
| OBSERVATION | `uc-primary-2` | blanc, `font-bold`, `text-xs` |
| CONFORME | `uc-accent` | blanc, `font-bold`, `text-xs` |
| Norme `ISO 9001 §X.Y.Z` | `uc-primary-2` | blanc, `font-bold`, monospace |
| ✓ sourcé | blanc, bord `uc-border` | `uc-accent` |

#### 6.4.3 Boutons d'action (bas)

3 boutons en ligne, `gap-2`, h=44px :

| Bouton | Couleur | Action |
|---|---|---|
| `+ Ajouter photo` | fond `uc-accent`, texte blanc | ouvre input fichier ; après upload, miniature à droite de la carte classification (attache `photo_id` au constat avant validation) |
| `Valider →` | fond `uc-primary`, texte blanc | persiste le constat via `POST /api/inspections/{id}/constats`, déclenche l'animation de slide vers Zone Constats |
| `Refaire` | fond blanc, bord `uc-border`, texte `uc-text-body` | annule, retour à l'état "prêt à capturer" |

#### 6.4.4 États

- **Idle** (au chargement) : mic au repos (cercle gris), texte « Cliquez le micro ou tapez ↑ pour parler »
- **Recording** : mic rouge pulsant, waveform live, transcript en cours
- **Processing** : carte classification en shimmer, RAG en cours de peuplement
- **Result** : carte classification complète, 3 boutons actifs
- **Validated** : flash emerald 300ms sur la carte, puis slide-right vers Zone Constats, reset de la zone

#### 6.4.5 Fallback clavier

Si Web Speech API indisponible OU si l'inspecteur préfère taper : touche `T` ouvre un textarea en place de la zone mic. Submit via `Cmd+Enter`. Comportement identique ensuite. **Ce fallback est utilisé en mode démo Teams** (voir le script `UC28_Demo_Script_Teams.md`).

### 6.5 Zone constats accumulés (droite, 28%)

**Composant.** `<FindingsList constats={inspection.constats} />`

Liste verticale de cartes. Les 4 dernières visibles sans scroll. Au-delà : `overflow-y-auto` discret.

**Carte constat `<ConstatCard />` :**

```
┌────────────────────────────────────────┐
│ ▮ [BADGE]  ISO 9001 §X.Y.Z  [📷]      │
│                                         │
│ Texte reformulé tronqué à ~40 char…    │
└────────────────────────────────────────┘
```

- Bord gauche 4px de la couleur de classification (`uc-danger`, `uc-alert`, etc.)
- Badge en haut à gauche (mêmes couleurs §6.4.2)
- Référence norme en haut à droite, `uc-text-mute`, `text-xs`
- Texte reformulé tronqué à 2 lignes, `line-clamp-2`
- Miniature photo en haut à droite si présente (32×24px, `rounded-sm`)

**Interactions.**
- Hover : `shadow-md` + cursor pointer
- Clic : modale qui affiche le constat complet

**Animation d'entrée.** Nouvelle carte = `slide-in-right` 300ms + bounce léger. Liste réorganisée avec `transition-all`.

### 6.6 Zone RAG transparency (bande basse, h=180px)

Fond `#ECFDF5` (`bg-emerald-50`), bord top `uc-accent`.

**Visible quand.** Activée pendant et juste après un appel à l'Agent 2. Sinon : version effondrée 32px de hauteur (hors démo). En démo, garder déplié.

**Composant.** `<RagTransparency chunks={lastRagChunks} loading={isClassifying} />`

**Contenu.**
- Header : *« RAG · Articles normatifs remontés en temps réel »* (centré, `uc-primary`, gras)
- Sous-titre : *« Pendant la classification, les 3 chunks normatifs les plus proches s'affichent — le jury voit que c'est sourcé »* (centré, italique)
- 3 colonnes égales (`grid-cols-3 gap-4`), chacune une carte :

```
┌────────────────────────────────────┐
│ §7.1.4 — Environnement             │
│                                     │
│ L'organisme doit déterminer,        │
│ fournir et maintenir l'environnement│
│ nécessaire au fonctionnement…       │
│                                     │
│ similarité            [0.92]        │
└────────────────────────────────────┘
```

**Score chip** en bas à droite, couleur dynamique :
- `≥ 0.85` → `uc-accent`
- `0.70-0.85` → `uc-alert`
- `< 0.70` → `uc-text-mute`

**Animation.** Chunks fade-in un par un (delay 100ms entre chaque) au démarrage de l'Agent 2. Restent affichés jusqu'au prochain constat.

### 6.7 Footer capture (h=110px)

Fond `uc-bg-dark`. Padding 20px.

**Contenu (gauche → droite).**

- Bloc stats à gauche :
  - Label *INSPECTION EN COURS* (uppercase, tracking-wider, `uc-accent-lt`, `text-xs`)
  - Ligne principale : `4 constats   ·   1 NC majeure   ·   1 NC mineure   ·   1 observation   ·   1 conforme`
  - Sous-ligne italique : `Démarré à 14:32   ·   Durée : 02:30`
- Bouton CTA à droite : **`Générer le pré-rapport →`**
  - ~360×64px, fond `uc-accent`, texte blanc, `text-lg`, `font-bold`, `rounded-xl`
  - Disabled si `constats.length === 0`
  - Hover : `bg-emerald-600`, légère élévation

**Action du CTA.** C'est le wow moment final de l'écran capture.

1. Au clic : appel `POST /api/inspections/{id}/report` (Agent 3 construit la structure)
2. Pendant l'attente (10-15s) : modale plein écran avec loader emerald et 5 messages qui défilent toutes les 2s :
   - *« Synthèse exécutive… »*
   - *« Regroupement des constats par thème… »*
   - *« Construction du plan d'action priorisé… »*
   - *« Mise en forme du document… »*
   - *« Génération du DOCX… »*
3. Quand le DOCX est prêt : navigation vers `/inspection/[id]/report` (écran 4)

**Plan B si l'API rame.** Bouton caché (raccourci `Cmd+Shift+D`) joue le rapport préenregistré stocké dans `data/fixtures/alpha_report.docx`.

---

## 7. Écran 4 · Revue & envoi du pré-rapport

**Route.** `/inspection/[id]/report`

**Quand.** 14h56 — Antoine vient de cliquer « Générer le pré-rapport ». Le DOCX est prêt, il relit et envoie.

**Composant racine.** `<ReportReviewPage inspectionId={id} />`

### 7.1 Layout

```
┌─────────────────────────────────────────────────────────────┐
│  HEADER (status pill « AUDIT TERMINÉ », pas de timer)       │
├──────────┬───────────────────────────────┬──────────────────┤
│          │                               │                  │
│  SYNTHÈSE│  APERÇU DOCX                  │  ENVOI           │
│  STATS   │  (50% — DOCX preview)         │  CLIENT          │
│  22%     │                               │  28%             │
│          │                               │                  │
│  + PLAN  │                               │                  │
│  ACTION  │                               │                  │
├──────────┴───────────────────────────────┴──────────────────┤
│  FOOTER : récap + CTA « Envoyer au client → »               │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 Composants

**`<FindingsSummary />`** (colonne gauche haut). 4 cartes empilées, une par classification. Chaque carte = grand chiffre monospace + label + sous-label. Bord gauche coloré (danger/alert/primary-2/accent).

**`<ActionPlan />`** (colonne gauche bas). Carte sombre (`uc-bg-dark`) avec liste des 4 actions priorisées. Chaque action = badge priorité (P1/P2/P3) coloré + action courte + délai. C'est l'encart sombre qui attire l'œil : élément métier critique.

**`<DocxPreview />`.** Cœur de l'écran. Viewer DOCX intégré qui affiche les 6 pages du rapport généré par l'Agent 3.

**Implémentation recommandée.**
- Lib principale : **`@cyntler/react-doc-viewer`** ou **`docx-preview`** (npm). Charge un `.docx` et le rend HTML inline.
- Fallback en cas de problème : convertir côté backend en PDF (`libreoffice --convert-to pdf`) puis rendre avec `react-pdf` (plus stable).
- Pour la démo, prerender le DOCX en PDF côté backend et stocker dans `data/fixtures/alpha_report.pdf`. Le viewer charge ce fichier = zéro latence à l'écran.

Scroll natif. Indicateur de page sous le viewer (« page 1 / 6 · scroll pour voir les constats »).

**`<SendForm />`** (colonne droite). Formulaire de pré-envoi.

Champs :
- À : `marie.lemaitre@alpha.fr` (pré-rempli depuis la fiche client)
- CC : `audit.bv@bureauveritas.com` (pré-rempli depuis le profil auditeur)
- Objet : `Pré-rapport audit ALPHA — 12/05/2026` (pré-rempli template)
- Message personnalisé : textarea avec template auto-rempli (4-5 lignes courtoises)

Sous le formulaire : **pièces jointes** détectées automatiquement (DOCX + photos prises pendant l'audit).

Actions secondaires sous le form : `[ ⬇ Télécharger ]` `[ ✎ Modifier ]` (boutons ghost).

**`<ReportFooter />`.** Récap à gauche (« AUDIT TERMINÉ · 14h56 · 4 constats · 1 NC majeure (action 24h) · 1 NC mineure récurrente · Durée totale : 26 min ») et CTA principal **« Envoyer au client → »**.

### 7.3 Interactions

- Clic **« Envoyer au client → »** → `POST /api/inspections/{id}/send` → modale de confirmation (« Rapport envoyé à Marie Lemaitre · 14h57 ») → retour au dashboard après 2 sec, avec carte ALPHA passée en status `TERMINÉ`
- Clic **« Télécharger »** → download du DOCX
- Clic **« Modifier »** → mode édition (stub hors démo)

### 7.4 Démo

Le pilote arrive sur cet écran après la modale de génération de l'écran capture. Il scrolle dans le `DocxPreview` (page de garde → synthèse → plan d'action → recommandation). Il narre. Il clique **« Envoyer au client → »** vers 2:35 dans la démo. La modale de confirmation apparaît brièvement. C'est la fermeture narrative.

---

## 8. Transitions entre écrans

Vu de bout en bout, le parcours dans la démo :

```
Slide PowerPoint (Antoine, 12 mai 14h30)
   │
   ↓
ÉCRAN 1 — Dashboard /
   │  Pilote pointe la carte ALPHA mise en valeur
   │  Clic « Démarrer → » sur la carte 14h30
   ↓
ÉCRAN 2 — /inspection/[id]/brief
   │  L'Agent 1 génère la check-list (10-15 sec, animation)
   │  Pilote narre l'intelligence du système
   │  Clic « Démarrer l'inspection → »
   ↓
ÉCRAN 3 — /inspection/[id]/capture
   │  Capture des 4 constats avec classification temps réel
   │  Clic « Générer le pré-rapport → »
   │  Modale de génération (Agent 3, 10-15 sec)
   ↓
ÉCRAN 4 — /inspection/[id]/report
   │  Aperçu du DOCX, scroll, narration du plan d'action
   │  Clic « Envoyer au client → »
   ↓
Modale de confirmation
   │  « Rapport envoyé à 14h57 »
   ↓
Slide PowerPoint (24 000 heures libérées)
```

**Animations.** Framer Motion ou Next.js view transitions pour fade-outs / fade-ins de **200ms** entre routes. Pas de transitions plus longues — le jury attend l'action suivante, pas un effet stylisé.

**État partagé.** Pour la démo, tout est persisté côté backend via l'`inspection_id`. Côté frontend, TanStack Query avec `queryKey: ['inspection', id]` invalidé après chaque mutation.

---

## 9. Contrats d'API

Tous les endpoints sont mockés ou simulés en mode démo. Pas d'auth, pas de multi-tenant, base SQLite ou JSON.

### 9.1 Dashboard

```
GET    /api/dashboard/kpis
  → 200 {
      audits_today_count: 3,
      audits_month_count: 18,
      avg_delay_days: 2,
      pending_recurrences_count: 4
    }

GET    /api/dashboard/audits_today
  → 200 [{
      id, scheduled_at, client_name, location, scope, status,
      is_next: boolean
    }, ...]

GET    /api/dashboard/recurrences
  → 200 [{
      inspection_id, client_name, norm_reference, opened_at, label
    }, ...]
```

### 9.2 Inspection · CRUD & génération

```
GET    /api/inspections/{id}
  → 200 { id, client_name, site_name, auditor_name, referential,
          scope, status, started_at, checklist_json, constats: [...] }

POST   /api/inspections/{id}/checklist
  → 200 { checklist_json, generation_duration_seconds }
  Effet : appelle l'Agent 1, persiste la check-list, retourne.
  Si une check-list existe, ne régénère pas (sauf header `X-Regenerate: true`
  ou query param `?referential=NFC_15100`).

GET    /api/inspections/{id}/history
  → 200 [{ inspection_id, audit_date, referential, result,
           has_open_findings }, ...]
  Audits précédents du même client (3 dernières années).
```

### 9.3 Capture · constats

```
POST   /api/inspections/{id}/constats
  body: { raw_text, checklist_point_id?, photo_id? }
  → 201 { constat: {
      id, classification, reformulated_text, norm_reference,
      norm_excerpt, suggested_action, rag_chunks: [...]
    }}
  Effet : appelle l'Agent 2 (classification + RAG), persiste, retourne.

POST   /api/inspections/{id}/photos
  body: multipart file
  → 201 { photo_id, url }
```

### 9.4 Restitution · rapport

```
POST   /api/inspections/{id}/report
  → 200 { report_structure, generation_duration_seconds, docx_url }
  Effet : appelle l'Agent 3, génère le DOCX, retourne.

GET    /api/inspections/{id}/report.docx
  → 200 application/vnd.openxmlformats-officedocument.wordprocessingml.document

POST   /api/inspections/{id}/send
  body: {
    to: string[],
    cc?: string[],
    subject: string,
    message: string,
    attachments?: string[]   // ids des photos en plus du DOCX
  }
  → 200 { sent_at, recipient_count }
  Effet : marque l'inspection en `status: completed`, log l'envoi.
  Pour la démo : pas d'envoi email réel, juste persistence.
```

### 9.5 Helpers de démo (route admin)

```
POST   /api/dev/reset-demo
  → 200 { reset_at }
  Effet : remet le scénario ALPHA dans l'état initial (inspection prepared,
  check-list générée, 0 constats).

POST   /api/dev/replay/{constat_index}
  → 200 { constat }
  Effet : injecte le constat préenregistré n°[1-4] comme s'il venait d'être
  capturé. Pour le plan B replay mode (voir script de démo Teams).
```

---

## 10. États globaux & gestion de la donnée

**Hooks principaux** (à créer dans `frontend/lib/hooks/`) :

```ts
useDashboard()                       // GET /api/dashboard/*
useInspection(id: string)            // GET /api/inspections/{id}
useCreateConstat(inspectionId)       // mutation POST + cache invalidation
useRegenerateChecklist(inspectionId) // mutation
useGenerateReport(inspectionId)      // mutation, renvoie l'URL du DOCX
useSendReport(inspectionId)          // mutation envoi client
useSpeechRecognition()               // wrapper Web Speech API
```

**Real-time.** Pour le QR mode spectateur, endpoint SSE `/api/inspections/{id}/events` qui push les nouveaux constats. Hors scope si trop chronophage en S3 — fallback polling 2s.

---

## 11. Accessibilité (minimum vital pour la démo)

- Tous les boutons ont un `aria-label`
- Le mic a un `aria-live="polite"` qui annonce les transitions d'état
- Le bouton **« Générer le pré-rapport »** est focusable au clavier (`Tab`) et activable à `Enter`
- Contrastes vérifiés : couleurs texte/fond ≥ AA WCAG
- Pas de dépendance souris exclusive : `Tab` permet la navigation entre zones

---

## 12. Animations & micro-interactions

À implémenter avec **Framer Motion**.

| Élément | Animation | Durée |
|---|---|---|
| Transition entre écrans | fade-out / fade-in | 200ms |
| Apparition section dans `<ChecklistGenerator />` | stagger fade-in | 100ms × N |
| Carte classification apparaît | fade + slide-up 8px | 200ms |
| Constat validé → liste | slide-right + fade | 300ms |
| Bascule référentiel | check-list fade-out / shimmer / fade-in | 800ms total |
| Score chip RAG | scale 0.8 → 1 + couleur | 150ms |
| CTA « Générer le rapport » | hover : scale 1.02 | 100ms |
| Loader modale rapport | shimmer + cycle de messages | continu |
| Modale envoi rapport | fade-in + scale 0.95 → 1 | 250ms |

---

## 13. Fixtures & mode démo

Le scénario démo « Fournisseur ALPHA » est seedé dans `backend/data/fixtures/alpha.json` (voir `UC28_Fixtures_Alpha.json` livré séparément). Contient :

- L'inspection (`status: ongoing`, `started_at: 14:32`)
- La check-list complète (4 sections, 12 points)
- 4 constats préenregistrés avec classifications, références normes et chunks RAG
- La structure de pré-rapport attendue

**Raccourcis démo / plan B :**

| Raccourci | Effet |
|---|---|
| `Cmd+Shift+R` | Reset complet du scénario ALPHA |
| `Cmd+Shift+1..4` | Inject le constat préenregistré n°[1-4] dans l'écran capture |
| `Cmd+Shift+D` | Joue le rapport préenregistré si l'API plante |
| Touche `T` | Ouvre le textarea fallback clavier dans la zone capture |

---

## 14. Checklist d'implémentation Claude Code

Ordre recommandé, à découper en sessions Claude Code dédiées.

**Phase 1 — Fondations & écran capture (sessions 1-10)** ← priorité

- [ ] **Session 1.** Setup Tailwind config avec tokens UC + polices Inter/JetBrains Mono. Composant `<HeaderBar />` paramétrable + structure de layout grid 12 colonnes.
- [ ] **Session 2.** Composant `<ChecklistView />` avec données mockées. Toggle sections, légende, états.
- [ ] **Session 3.** Composant `<FindingsList />` + `<ConstatCard />` avec données mockées. Animations Framer Motion.
- [ ] **Session 4.** Composant `<CaptureZone />` — d'abord la carte classification statique avec données mockées. Tous les états visuels.
- [ ] **Session 5.** Web Speech API + waveform canvas. Wire l'enregistrement.
- [ ] **Session 6.** `<RagTransparency />` avec données mockées.
- [ ] **Session 7.** Wire au backend réel : `useInspection`, `useCreateConstat`. Remplace les mocks.
- [ ] **Session 8.** Bouton « Générer le pré-rapport », modale loader, navigation vers écran 4.
- [ ] **Session 9.** Mode démo, raccourcis, replay.
- [ ] **Session 10.** Polish capture : animations finales, accessibilité, test sur les 3 résolutions cibles.

**Phase 2 — Écrans satellites (sessions 11-20)**

- [ ] **Session 11.** Composant `<DashboardPage />` + `<KpiStrip />` + `<AuditList />` avec données mockées.
- [ ] **Session 12.** Composant `<RecurrencesList />` + intégration `GET /api/dashboard/*`.
- [ ] **Session 13.** Composant `<BriefPage />` + `<BriefSummary />` + `<AuditHistory />` avec données mockées.
- [ ] **Session 14.** Composant `<ChecklistGenerator />` avec ses 3 états (triggering / generated / editing).
- [ ] **Session 15.** Endpoint backend `POST /api/inspections/{id}/checklist` (Agent 1) — réutilisé de S1-S2.
- [ ] **Session 16.** Composant `<ReportReviewPage />` + `<FindingsSummary />` + `<ActionPlan />`.
- [ ] **Session 17.** Composant `<DocxPreview />` avec lib choisie + fallback PDF.
- [ ] **Session 18.** Composant `<SendForm />` + endpoint `POST /api/inspections/{id}/send` (stub).
- [ ] **Session 19.** Animations entre routes (Framer Motion / Next.js view transitions).
- [ ] **Session 20.** Mode démo complet : pré-chargement fixtures, raccourcis, reset rapide.

**Budget temps estimé.** Phase 1 : 4-6 jours de travail Claude Code en sub-agents parallèles. Phase 2 : 2-3 jours supplémentaires. Total : 6-9 jours de dev effectif, à étaler sur les 4 semaines du hackathon.

---

## 15. État de la phase CONCEVOIR · récap

Confrontation aux 5 éléments d'une bonne spécification (guide hackathon slide 6) :

| # | Élément | État | Livrable |
|---|---|---|---|
| 1 | Utilisateur cible et contexte métier | ✅ | `UC28_Persona.png` (Antoine Mercier) |
| 2 | Parcours en 3-5 écrans | ✅ | **4 écrans** documentés §4-§7 |
| 3 | Choix techniques | ✅ | `UC28_Blueprint_ClaudeCode.md` §2 |
| 4 | Données d'entrée | ✅ | `UC28_Fixtures_Alpha.json` |
| 5 | Critères de réussite | ✅ | Blueprint §10 + critères de démo |

**Phase CONCEVOIR : bouclée.** Vous pouvez entrer dans DÉVELOPPER avec la team.

---

**Fin de spec UI.** Les changements doivent être commités avec `docs(ui): ...` et synchronisés avec `blueprint.md` §5 (contrats API).

*— UC 28 · Inspection Augmentée · Vibe Coding Hackathon Capgemini × Anthropic*
