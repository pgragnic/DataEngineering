# UC 28 — Inspection Augmentée  ·  Blueprint Claude Code

> **Pour qui ?** Une équipe de 3 personnes qui code avec Claude Code pendant 4 semaines (12 mai → 5 juin) un démonstrateur pour le Vibe Coding Hackathon Capgemini × Anthropic.
>
> **Comment l'utiliser ?** Ce fichier est la source de vérité. Copiez la section [`CLAUDE.md` à la racine](#section-12--claudemd--à-copier-tel-quel-à-la-racine-du-repo) à la racine du repo. Référez-vous au reste pendant les sprints. Mettez à jour ce blueprint quand une décision change.

---

## Sommaire

1. [Mission & cible démo](#section-1--mission--cible-démo)
2. [Stack technique (verrouillée)](#section-2--stack-technique-verrouillée)
3. [Structure du repo](#section-3--structure-du-repo)
4. [Modèle de données](#section-4--modèle-de-données)
5. [Contrats d'API](#section-5--contrats-dapi)
6. [Les 3 agents Claude](#section-6--les-3-agents-claude)
7. [Corpus RAG normatif](#section-7--corpus-rag-normatif)
8. [Plan de sprint S1 → S4](#section-8--plan-de-sprint-s1--s4)
9. [Scénario de démo (3 min)](#section-9--scénario-de-démo-3-min)
10. [Critères d'acceptation](#section-10--critères-dacceptation)
11. [Workflow Claude Code recommandé](#section-11--workflow-claude-code-recommandé)
12. [`CLAUDE.md` à copier tel quel à la racine du repo](#section-12--claudemd--à-copier-tel-quel-à-la-racine-du-repo)

---

## Section 1 — Mission & cible démo

**Mission.** Construire un copilote IA pour les inspecteurs Bureau Veritas / APAVE qui couvre les trois temps d'un audit ISO 9001 :

1. **Avant** — générer une check-list dynamique adaptée au client et au site.
2. **Pendant** — capturer les constats par la voix et la photo, classifier en NC mineure/majeure, sourcer à l'article de norme.
3. **Après** — générer un pré-rapport DOCX structuré, prêt à être envoyé au client.

**Cible démo.** Une démonstration live de **3 minutes** sur tablette ou smartphone, montrant un audit ISO 9001 fictif d'un fournisseur. Le détail est dans la [Section 9](#section-9--scénario-de-démo-3-min).

**Ce que la démo n'est PAS.** Pas une plateforme multi-tenant. Pas un ERP. Pas un produit déployable en prod. C'est un démonstrateur qui prouve la valeur du concept en condition réaliste.

**Punchline chiffrée.** 30 inspecteurs × 4 h économisées/audit × 200 audits/an = 24 000 h libérées, soit 15 ETP redéployables.

---

## Section 2 — Stack technique (verrouillée)

Choix arrêtés. Ne pas renégocier en cours de sprint sans raison forte.

| Couche | Choix | Pourquoi |
|---|---|---|
| **Frontend** | Next.js 14 (App Router) + TypeScript + Tailwind | PWA mobile-first, écosystème mature, Claude Code excellent dessus |
| **Voix** | Web Speech API (browser natif) | Zéro dépendance, fonctionne sur Chrome mobile, suffisant pour démo |
| **Photos** | `<input type="file" accept="image/*" capture="environment">` + base64 vers backend | Pas de complexité native |
| **Backend** | FastAPI (Python 3.11+) + uvicorn | Async natif, Pydantic pour les schémas, parfait pour wrapper Claude |
| **DB relationnelle** | PostgreSQL 16 | Standard, SQLAlchemy + Alembic |
| **Vector store** | ChromaDB (mode embedded persistent) | Aucune infra à monter, fichier sur disque, suffit largement |
| **Storage objet** | Filesystem local (`./storage/`) | Pas besoin de S3 pour la démo. Abstraction `StorageBackend` pour passer à S3 plus tard |
| **LLM** | Claude Sonnet 4.6 (`claude-sonnet-4-6`) via SDK Python `anthropic` | Multimodal (texte + vision), assez rapide pour les appels temps réel |
| **Génération DOCX** | `python-docx` | Lib de référence, Claude la connaît bien |
| **Auth** | Aucune pour la démo (un seul utilisateur fictif) | Hors scope. Stub un `current_user` hardcodé |
| **Orchestration locale** | `docker-compose` (postgres + chromadb + backend + frontend) | One-command dev environment |

**Versions à figer dans les manifests** :

- `next@14.2.x`, `react@18.3.x`, `typescript@5.4.x`, `tailwindcss@3.4.x`
- `fastapi@0.115.x`, `uvicorn@0.30.x`, `sqlalchemy@2.0.x`, `alembic@1.13.x`, `pydantic@2.9.x`
- `anthropic@0.40.x`, `chromadb@0.5.x`, `python-docx@1.1.x`, `psycopg[binary]@3.2.x`

**Variables d'environnement attendues** (à mettre dans `.env`, jamais commité) :

```env
ANTHROPIC_API_KEY=sk-ant-...
DATABASE_URL=postgresql+psycopg://uc28:uc28@localhost:5432/uc28
CHROMA_PERSIST_DIR=./storage/chroma
STORAGE_DIR=./storage/files
CLAUDE_MODEL=claude-sonnet-4-6
```

---

## Section 3 — Structure du repo

```
uc28-inspection/
├── README.md                  # quickstart pour la team
├── CLAUDE.md                  # voir section 12
├── docker-compose.yml         # postgres + backend + frontend
├── .env.example               # template (sans secrets)
├── blueprint.md               # ce fichier
│
├── backend/
│   ├── pyproject.toml
│   ├── app/
│   │   ├── main.py            # FastAPI entrypoint
│   │   ├── config.py          # settings Pydantic
│   │   ├── db.py              # SQLAlchemy engine + session
│   │   ├── models/            # SQLAlchemy models (voir Section 4)
│   │   │   ├── inspection.py
│   │   │   └── constat.py
│   │   ├── schemas/           # Pydantic schemas (in/out)
│   │   ├── api/               # routes FastAPI (voir Section 5)
│   │   │   ├── inspections.py
│   │   │   ├── constats.py
│   │   │   ├── reports.py
│   │   │   └── uploads.py
│   │   ├── agents/            # les 3 agents Claude (voir Section 6)
│   │   │   ├── client.py      # wrapper anthropic SDK
│   │   │   ├── preparation.py
│   │   │   ├── capture.py
│   │   │   └── restitution.py
│   │   ├── rag/
│   │   │   ├── ingest.py      # ingestion du corpus normatif
│   │   │   └── retrieve.py    # recherche similarité
│   │   ├── docx_gen/
│   │   │   └── report.py      # génération du pré-rapport DOCX
│   │   └── storage/
│   │       └── files.py       # save/load des photos et audio
│   ├── alembic/               # migrations
│   ├── corpus/                # voir Section 7
│   │   ├── iso9001/
│   │   └── iso19011/
│   └── tests/
│       └── test_smoke.py
│
├── frontend/
│   ├── package.json
│   ├── next.config.mjs
│   ├── tailwind.config.ts
│   ├── app/
│   │   ├── layout.tsx
│   │   ├── page.tsx           # dashboard inspecteur
│   │   ├── inspection/
│   │   │   ├── new/page.tsx   # créer une inspection (brief client)
│   │   │   └── [id]/
│   │   │       ├── page.tsx   # vue inspection en cours
│   │   │       ├── capture/page.tsx  # mode terrain plein écran
│   │   │       └── report/page.tsx   # revue avant export
│   │   └── api/               # routes API Next (proxy vers backend si besoin)
│   ├── components/
│   │   ├── VoiceCapture.tsx   # Web Speech API
│   │   ├── PhotoCapture.tsx
│   │   ├── ConstatCard.tsx
│   │   ├── ChecklistView.tsx
│   │   └── NCBadge.tsx
│   ├── lib/
│   │   └── api.ts             # client typé vers le backend
│   └── public/
│       └── manifest.webmanifest  # PWA
│
└── data/                      # fixtures pour la démo
    ├── inspections/
    │   └── alpha_audit.json   # le cas démo "Fournisseur ALPHA"
    └── photos/
```

---

## Section 4 — Modèle de données

Schéma volontairement minimal. Tout est indexé par UUID.

### Tables

```python
# backend/app/models/inspection.py
class Inspection(Base):
    __tablename__ = "inspections"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    client_name: Mapped[str]            # "Fournisseur ALPHA"
    site_name: Mapped[str]              # "Usine de Tours, bât. B"
    auditor_name: Mapped[str]           # "Jean Dupont (BV)"
    referential: Mapped[str]            # "ISO 9001" (un seul pour le MVP)
    scope: Mapped[str]                  # "Processus achats et contrôle réception"
    status: Mapped[InspectionStatus]    # enum: prepared, ongoing, completed
    checklist_json: Mapped[dict | None] # généré par l'Agent 1 (voir Section 6)
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]

    constats: Mapped[list["Constat"]] = relationship(back_populates="inspection")


# backend/app/models/constat.py
class Constat(Base):
    __tablename__ = "constats"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    inspection_id: Mapped[UUID] = mapped_column(ForeignKey("inspections.id"))
    checklist_point_id: Mapped[str | None]  # id du point de la check-list

    raw_text: Mapped[str]               # texte brut (vocal ou clavier)
    reformulated_text: Mapped[str]      # version structurée par l'Agent 2
    classification: Mapped[NCLevel]     # enum: conforme, observation, nc_mineure, nc_majeure
    norm_reference: Mapped[str | None]  # "ISO 9001 §7.1.4"
    norm_excerpt: Mapped[str | None]    # court extrait sourcé
    suggested_evidence: Mapped[str | None]   # "Photo de la sortie de secours"
    suggested_action: Mapped[str | None]     # "Libérer la sortie de secours, formation magasinier"

    photo_path: Mapped[str | None]
    audio_path: Mapped[str | None]
    created_at: Mapped[datetime]

    inspection: Mapped["Inspection"] = relationship(back_populates="constats")
```

### Enums

```python
class InspectionStatus(str, Enum):
    prepared = "prepared"
    ongoing = "ongoing"
    completed = "completed"

class NCLevel(str, Enum):
    conforme = "conforme"
    observation = "observation"
    nc_mineure = "nc_mineure"
    nc_majeure = "nc_majeure"
```

### Schéma JSON de la check-list

Stocké dans `Inspection.checklist_json`. Sortie de l'Agent 1.

```json
{
  "referential": "ISO 9001:2015",
  "scope_summary": "Audit du processus achats et contrôle réception",
  "sections": [
    {
      "id": "S1",
      "title": "Maîtrise des informations documentées (§7.5)",
      "points": [
        {
          "id": "S1.P1",
          "question": "Les procédures achats sont-elles documentées, accessibles et à jour ?",
          "expected_evidence": "Procédure achats version courante, date de dernière revue",
          "norm_reference": "ISO 9001 §7.5.3"
        }
      ]
    }
  ]
}
```

---

## Section 5 — Contrats d'API

Base URL : `/api`. Toutes les routes renvoient du JSON sauf l'export DOCX.

### Inspections

```
POST   /api/inspections
  body: { client_name, site_name, auditor_name, referential, scope }
  → 201 { id, ...inspection, status: "prepared" }

GET    /api/inspections
  → 200 [{ id, client_name, site_name, status, created_at }, ...]

GET    /api/inspections/{id}
  → 200 { ...inspection, constats: [...] }

POST   /api/inspections/{id}/checklist
  → 200 { checklist_json }
  Appelle l'Agent 1. Stocke et renvoie.

PATCH  /api/inspections/{id}
  body: { status?: "ongoing" | "completed" }
  → 200 { ...inspection }
```

### Constats

```
POST   /api/inspections/{id}/constats
  body: { raw_text, checklist_point_id?, photo_id?, audio_id? }
  → 201 { ...constat with reformulated_text, classification, norm_reference, ... }
  Appelle l'Agent 2 puis enregistre.

GET    /api/inspections/{id}/constats
  → 200 [constat, ...]

DELETE /api/constats/{id}
  → 204
```

### Uploads (photos, audio)

```
POST   /api/uploads
  body: multipart/form-data (file)
  → 201 { id, path, kind: "photo" | "audio" }

GET    /api/uploads/{id}
  → renvoie le fichier
```

### Rapport

```
POST   /api/inspections/{id}/report
  → 200 { report_structure }
  Appelle l'Agent 3, structure le rapport.

GET    /api/inspections/{id}/report.docx
  → 200 application/vnd.openxmlformats-officedocument.wordprocessingml.document
  Génère et stream le DOCX.
```

### Q&A sur historique (sprint 3, bonus)

```
POST   /api/qa
  body: { question, scope?: { client_name?, date_range? } }
  → 200 { answer, sources: [...] }
```

---

## Section 6 — Les 3 agents Claude

Tous les agents utilisent **Claude Sonnet 4.6** (`claude-sonnet-4-6`). Tous renvoient du **JSON strict** (extraction par `response_format` ou parsing robuste). Tous ont **leur propre prompt système verrouillé**.

### 6.1 — Agent 1 · Préparation

**Rôle.** Génère la check-list dynamique d'inspection à partir du brief client.

**Entrées.**
- Brief de l'inspection : `client_name`, `site_name`, `referential`, `scope`
- Historique des audits de ce client (s'il existe) — depuis la DB
- Contexte normatif (via RAG sur ISO 9001 / ISO 19011)

**Sortie.** Le JSON `checklist_json` décrit en [Section 4](#schéma-json-de-la-check-list).

**Prompt système (à mettre dans `backend/app/agents/preparation.py`)** :

```text
Tu es un auditeur senior certifié IRCA spécialisé en audits qualité ISO 9001.
Ta mission est de préparer une check-list d'inspection structurée et opérationnelle pour un auditeur qui va sur le terrain.

Contraintes :
- Tu travailles sur le référentiel fourni (ISO 9001:2015).
- Tu adaptes systématiquement la check-list au périmètre demandé (scope).
- Chaque point de la check-list doit être : (1) actionnable sur le terrain, (2) lié à un article précis de la norme, (3) accompagné d'une indication des preuves attendues.
- Tu produis 3 à 6 sections, chaque section contenant 3 à 6 points. Pas plus.
- Tu ne fais pas de blabla introductif. Tu renvoies UNIQUEMENT le JSON demandé.

Format de sortie strict (JSON):
{
  "referential": "...",
  "scope_summary": "...",
  "sections": [
    { "id": "S1", "title": "...", "points": [
        { "id": "S1.P1", "question": "...", "expected_evidence": "...", "norm_reference": "ISO 9001 §..." }
    ]}
  ]
}

Si l'historique des audits passés mentionne des NC non clôturées chez ce client, inclus un point de check-list dédié à leur vérification.
```

**Appel.** Un seul appel `messages.create`. Pas de tool use nécessaire pour cet agent. Le contexte RAG est injecté dans le `user message`.

### 6.2 — Agent 2 · Capture

**Rôle.** Transforme un constat brut (voix ou texte) en constat structuré, classifié, sourcé.

**Entrées.**
- `raw_text` (transcription vocale ou saisie clavier)
- `inspection_context` (référentiel, scope, point de check-list en cours s'il y en a)
- Photo optionnelle (passée en input vision si présente)
- Contexte normatif RAG (top 3 chunks pertinents)

**Sortie.**
```json
{
  "reformulated_text": "La sortie de secours du bâtiment B est obstruée par un chariot de stockage.",
  "classification": "nc_majeure",
  "norm_reference": "ISO 9001 §7.1.4",
  "norm_excerpt": "L'organisme doit déterminer, fournir et maintenir l'environnement nécessaire au fonctionnement de ses processus...",
  "suggested_evidence": "Photo de la sortie obstruée avec le chariot, étiquette d'identification du chariot",
  "suggested_action": "Libérer immédiatement la sortie de secours ; rappel des consignes au magasinier ; ajout au plan d'audit annuel sécurité."
}
```

**Règles de classification** (à intégrer au prompt) :

- `conforme` — observation positive ou point conforme.
- `observation` — point d'amélioration, sans non-conformité.
- `nc_mineure` — écart isolé qui n'affecte pas le SMQ.
- `nc_majeure` — écart systémique OU mettant en jeu la sécurité OU empêchant le SMQ de fonctionner.

**Prompt système (`backend/app/agents/capture.py`)** :

```text
Tu es un assistant d'audit. Tu aides un inspecteur sur le terrain à formaliser et classifier ses constats en temps réel.

Ton travail, pour chaque constat brut :
1. Reformuler le constat en français professionnel et factuel, sans interprétation.
2. Le classifier : "conforme" | "observation" | "nc_mineure" | "nc_majeure".
   - conforme : tout va bien.
   - observation : point d'amélioration sans écart à la norme.
   - nc_mineure : écart ponctuel, isolé, qui n'affecte pas le système qualité.
   - nc_majeure : écart systémique, ou qui touche à la sécurité des personnes, ou qui empêche le SMQ de fonctionner.
3. Identifier l'article du référentiel le plus pertinent. Utilise UNIQUEMENT les extraits normatifs fournis dans le contexte ; ne cite jamais un article que tu n'as pas vu.
4. Indiquer la preuve à collecter (photo, document, mesure).
5. Proposer une action corrective concrète et proportionnée.

Tu renvoies UNIQUEMENT le JSON :
{
  "reformulated_text": "...",
  "classification": "conforme|observation|nc_mineure|nc_majeure",
  "norm_reference": "ISO 9001 §...",
  "norm_excerpt": "...",
  "suggested_evidence": "...",
  "suggested_action": "..."
}

Si tu ne peux pas sourcer à un article précis avec les extraits fournis, mets norm_reference: null et explique-le brièvement dans suggested_action.
```

**Appel.** Vision activée si photo présente. Tool use non nécessaire.

### 6.3 — Agent 3 · Restitution

**Rôle.** Génère la structure du pré-rapport à partir de l'inspection complète (métadonnées + tous les constats).

**Entrées.**
- L'objet `Inspection` complet avec sa `checklist_json` et tous ses `constats`
- Aucun appel RAG nécessaire à ce stade

**Sortie.**

```json
{
  "executive_summary": "L'audit du fournisseur ALPHA, mené le 12 mai 2026 sur le site de Tours, a permis de relever 2 non-conformités majeures, 1 NC mineure et 3 observations sur le processus achats et le contrôle réception...",
  "conformity_summary": {
    "conforme": 4, "observation": 3, "nc_mineure": 1, "nc_majeure": 2
  },
  "sections": [
    {
      "title": "Constats — Maîtrise des informations documentées",
      "findings": [
        {
          "classification": "nc_majeure",
          "reformulated_text": "...",
          "norm_reference": "ISO 9001 §...",
          "suggested_action": "..."
        }
      ]
    }
  ],
  "action_plan": [
    {
      "priority": 1,
      "finding_ref": "Constat #2",
      "action": "Libérer la sortie de secours",
      "responsible": "Magasinier site Tours",
      "deadline": "Sous 24h"
    }
  ],
  "next_audit_recommendation": "Audit de suivi recommandé sous 3 mois pour vérifier la clôture des NC majeures."
}
```

**Prompt système (`backend/app/agents/restitution.py`)** :

```text
Tu es un auditeur senior qui rédige le pré-rapport d'un audit ISO 9001 à destination du client audité.

Style :
- Français professionnel, factuel, sans jargon inutile.
- Phrases courtes. Pas d'opinion. Que des constats sourcés.

Tu reçois en entrée le contexte complet de l'audit : métadonnées, check-list utilisée, tous les constats avec leur classification.

Tu produis UNIQUEMENT le JSON suivant :
{
  "executive_summary": "2-3 phrases factuelles résumant l'audit, le nombre de NC par niveau, et l'appréciation globale.",
  "conformity_summary": { "conforme": N, "observation": N, "nc_mineure": N, "nc_majeure": N },
  "sections": [ { "title": "...", "findings": [ ... ] } ],
  "action_plan": [
    { "priority": 1|2|3, "finding_ref": "Constat #X", "action": "...", "responsible": "...", "deadline": "..." }
  ],
  "next_audit_recommendation": "..."
}

Règles :
- Regroupe les constats par thème (section de la check-list).
- Dans le plan d'action, priorise : priorité 1 = NC majeure, 2 = NC mineure, 3 = observation.
- Les responsables et délais sont des suggestions par défaut (à confirmer par le client).
- Recommande systématiquement un audit de suivi s'il y a au moins une NC majeure.
```

Le code Python qui génère le DOCX (`backend/app/docx_gen/report.py`) prend cette structure et compose le Word avec `python-docx` :
- Page de garde
- Sommaire automatique
- Synthèse exécutive
- Tableau de conformité
- Une section par thème de la check-list, avec les constats listés
- Tableau plan d'action
- Pied de page avec date, auditeur, signature

### 6.4 — Wrapper anthropic SDK (`backend/app/agents/client.py`)

```python
from anthropic import Anthropic
from app.config import settings

_client = Anthropic(api_key=settings.anthropic_api_key)

def call_claude_json(system: str, user_content: list[dict], max_tokens: int = 2000) -> dict:
    """
    Appelle Claude Sonnet 4.6, attend une réponse JSON, parse robustement.
    user_content suit le format multimodal anthropic (texte + image).
    """
    resp = _client.messages.create(
        model=settings.claude_model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user_content}],
    )
    text = resp.content[0].text
    # tolérance aux fences ```json
    text = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return json.loads(text)
```

---

## Section 7 — Corpus RAG normatif

**Objectif.** Permettre à l'Agent 2 de citer le bon article de norme pour chaque constat.

**Source.** ISO 9001:2015 et ISO 19011:2018 sont des normes payantes — leur **texte intégral ne peut pas être mis dans le repo**. Pour le démonstrateur, on travaille avec des **résumés et reformulations publics** :

- Les sommaires officiels publics de l'ISO.
- Les guides publics AFNOR / INRS qui paraphrasent les articles.
- Des reformulations rédigées par l'équipe (à valider par le référent métier).

**Format dans le repo** : un fichier Markdown par article, sous `backend/corpus/iso9001/` :

```
backend/corpus/iso9001/
├── 04_contexte_organisme.md
├── 05_leadership.md
├── 06_planification.md
├── 07_support.md           # contient §7.1, §7.2, §7.3, §7.4, §7.5
├── 08_realisation.md
├── 09_evaluation.md
└── 10_amelioration.md
```

Chaque fichier contient :

```markdown
# ISO 9001 §7.1.4 — Environnement pour la mise en œuvre des processus

**Référence** : ISO 9001:2015, clause 7.1.4
**Mots-clés** : environnement de travail, sécurité, conditions physiques, infrastructure

## Résumé
L'organisme doit déterminer, fournir et maintenir l'environnement nécessaire au fonctionnement de ses processus et à l'obtention de la conformité des produits et services...

## Exemples de NC observées
- Locaux mal aérés, bruit excessif, températures inadaptées
- Sortie de secours obstruée
- Postes de travail non ergonomiques
```

**Ingestion.** Script `backend/app/rag/ingest.py` qui :

1. Parcourt récursivement `backend/corpus/`.
2. Découpe chaque fichier MD en chunks d'environ 400 tokens (en gardant les sections logiques).
3. Calcule les embeddings avec l'API Anthropic (`messages.create` sur embeddings) OU `sentence-transformers` local si on veut zéro appel API en RAG (recommandé : `all-MiniLM-L6-v2`).
4. Stocke dans ChromaDB avec les métadonnées `{ norm: "ISO 9001", section: "7.1.4", ... }`.

**Retrieval.** À chaque appel de l'Agent 2 :

```python
# backend/app/rag/retrieve.py
def retrieve_norm_context(query: str, k: int = 3) -> list[dict]:
    results = chroma_collection.query(query_texts=[query], n_results=k)
    return [
        { "section": meta["section"], "excerpt": doc, "score": dist }
        for doc, meta, dist in zip(results["documents"][0], results["metadatas"][0], results["distances"][0])
    ]
```

Le résultat est concaténé dans le `user message` de l'Agent 2, après le `raw_text` du constat.

---

## Section 8 — Plan de sprint S1 → S4

Chaque sprint a un **objectif unique mesurable** et une **démo interne** vendredi soir.

### Sprint 1 — 12-16 mai · Fondations & corpus

**Objectif :** Sur la machine d'un dev, un script Python répond à la question « génère-moi une check-list pour un audit ISO 9001 du fournisseur ALPHA, scope processus achats » et renvoie un JSON valide en se servant du RAG.

**Tâches :**

- [ ] Bootstrap du repo (`backend/`, `frontend/` vides + `docker-compose.yml`)
- [ ] DB PostgreSQL + Alembic + premiers migrations (Inspection, Constat)
- [ ] Wrapper anthropic SDK (`agents/client.py`) avec test smoke
- [ ] Corpus initial : **10 fichiers Markdown** couvrant les chapitres 4 à 10 d'ISO 9001
- [ ] Ingestion RAG dans ChromaDB
- [ ] Agent 1 (Préparation) opérationnel via CLI
- [ ] Tests : 3 briefs différents génèrent 3 check-lists différentes et cohérentes

**Livrable démo S1 :** un terminal qui montre une check-list générée, validée à l'œil par le référent métier.

### Sprint 2 — 19-23 mai · Cœur métier (capture + classification)

**Objectif :** Un endpoint `POST /api/inspections/{id}/constats` qui prend un texte brut et renvoie un constat structuré, classifié, sourcé à la norme.

**Tâches :**

- [ ] FastAPI backend opérationnel, routes CRUD inspections + constats
- [ ] Agent 2 (Capture) avec retrieval RAG
- [ ] Vision activée sur l'Agent 2 (passage de photo en base64)
- [ ] Endpoint upload photos/audio (`POST /api/uploads`)
- [ ] Persistence complète DB
- [ ] Tests : 10 constats de référence (préparés par le référent métier) sont correctement classifiés à au moins 7/10

**Livrable démo S2 :** Postman/curl sur le backend qui montre l'enchaînement : créer inspection → checklist → ajouter 3 constats → ils sont classifiés et sourcés correctement.

### Sprint 3 — 26-30 mai · UX & effet WOW

**Objectif :** Une PWA mobile fonctionnelle qui couvre le parcours complet de bout en bout. Une démo brute mais qui marche.

**Tâches :**

- [ ] Next.js PWA, page de création d'inspection
- [ ] Page « inspection en cours » avec check-list affichée
- [ ] Composant `VoiceCapture` (Web Speech API) + fallback clavier
- [ ] Composant `PhotoCapture` (input camera) + preview
- [ ] Cartes de constats stylées (badge NC majeure rouge, badge norme bleu)
- [ ] Agent 3 (Restitution) opérationnel
- [ ] Génération DOCX du pré-rapport
- [ ] Page de revue avant export + bouton « télécharger DOCX »

**Livrable démo S3 :** un dev fait toute la démo sur son téléphone, sans tricher, le DOCX se télécharge.

### Sprint 4 — 2-5 juin · Polish & démo

**Objectif :** Une démo de 3 minutes répétable, sans surprise, qui passe sur le device de présentation du jury.

**Tâches :**

- [ ] Le scénario « Fournisseur ALPHA » est seedé en DB (fixture)
- [ ] Mode démo : un bouton « reset démo » remet tout à zéro
- [ ] Animation/UI polish : transitions, loading states, vide-states
- [ ] Fallback offline : si l'API Claude rame, un mode « replay » avec des constats préenregistrés
- [ ] Pitch deck final + slide vision multi-tenant
- [ ] **2 répétitions complètes par jour** sur le device cible

**Livrable démo S4 :** la démo de 3 minutes, chronométrée, sans bug, devant le jury intermédiaire (8-12 juin).

---

## Section 9 — Scénario de démo (3 min)

À répéter à l'identique. C'est le fil que toute l'équipe doit connaître par cœur.

**Setup avant démo :** un fournisseur fictif « ALPHA » est en DB. Une inspection est créée. La check-list est déjà générée (gain de temps). On démarre directement en mode terrain.

```
0:00 — "Aujourd'hui, je vais auditer le fournisseur ALPHA, processus achats, site de Tours."
       [Écran d'inspection en cours, check-list visible à gauche]

0:10 — "L'inspecteur déclenche le mode capture."
       [Bouton micro plein écran, voyant rouge]

0:20 — "Premier constat : 'la procédure achats existe, version 3, datée de mars 2026'."
       [Voice-to-text affiche le texte. 2 sec de spinner.
        Carte constat : CONFORME · ISO 9001 §7.5.3]

0:40 — "Deuxième constat : 'la sortie de secours du bâtiment B est obstruée par un chariot de stockage'."
       [Texte transcrit. Spinner.
        Carte : NC MAJEURE · ISO 9001 §7.1.4
        Suggestion : prendre une photo de la sortie obstruée]

0:55 — "L'inspecteur prend la photo."
       [Capture caméra. Photo apparaît, liée au constat. Action suggérée s'affiche :
        "Libérer la sortie de secours, rappel consignes magasinier."]

1:15 — "Troisième constat : 'pas de procédure documentée de contrôle réception, mais le magasinier a un cahier manuscrit'."
       [Carte : NC MINEURE · ISO 9001 §8.4.3]

1:35 — "Quatrième constat : 'le magasinier semble bien formé sur le geste, malgré l'absence de procédure'."
       [Carte : OBSERVATION · pas de §, action : "Pérenniser la pratique par une procédure écrite"]

1:55 — "L'audit est terminé. On clôture et on génère le pré-rapport."
       [Bouton "Générer le rapport".
        Loading 10-15 sec.
        Toast : "Pré-rapport prêt"]

2:15 — "Le DOCX s'ouvre. Page de garde, synthèse, conformité, constats, plan d'action."
       [On scrolle le DOCX. On montre :
        - Synthèse : 1 conforme, 1 observation, 1 NC mineure, 1 NC majeure.
        - Plan d'action : la NC majeure en priorité 1, action sous 24h.
        - Recommandation : audit de suivi sous 3 mois.]

2:45 — Punchline du pitcher :
       "30 inspecteurs × 4h économisées/audit × 200 audits/an = 24 000 heures libérées,
        soit 15 ETP redéployés sur l'analyse à valeur."

3:00 — Fin.
```

**Plan B si l'API Claude rame :** mode replay — les 4 constats du scénario sont préenregistrés en DB. On déclenche un bouton caché « replay démo » qui rejoue les transitions UI à vitesse normale.

---

## Section 10 — Critères d'acceptation

Pour passer la démo finale, **TOUS** les critères suivants doivent être verts :

**Fonctionnels :**

- [ ] Je peux créer une inspection en moins de 30 secondes (brief client → check-list générée).
- [ ] La check-list générée est cohérente : 3-6 sections, chaque point lié à un article de norme.
- [ ] La capture vocale fonctionne sur Chrome mobile.
- [ ] Un constat brut est classifié et sourcé en moins de 5 secondes.
- [ ] Une photo prise est correctement liée au constat (visible dans la carte).
- [ ] Le DOCX généré est ouvrable dans Word, avec page de garde, synthèse, sections, plan d'action.
- [ ] Sur 10 constats de référence validés par le métier, au moins 7 sont correctement classifiés.

**Non-fonctionnels :**

- [ ] La démo complète tient en 3 minutes ± 15 secondes.
- [ ] La PWA tourne sans bug visible sur le device de démo (à choisir et tester en S3).
- [ ] Pas d'API key Anthropic dans le repo (vérifié par grep).
- [ ] `README.md` permet à un dev externe de lancer le projet en 10 minutes.

---

## Section 11 — Workflow Claude Code recommandé

### Setup initial (S1, jour 1)

1. Chaque membre installe Claude Code : `npm install -g @anthropic-ai/claude-code` puis `claude` à la racine du repo.
2. À la racine du repo, créer le fichier `CLAUDE.md` avec le contenu de la [Section 12](#section-12--claudemd--à-copier-tel-quel-à-la-racine-du-repo).
3. À chaque ouverture de session, faire un `claude` puis donner un brief court : *"Lis CLAUDE.md et blueprint.md, et propose un plan pour la tâche X."*

### Bonnes pratiques pour ce projet

**Découper en tâches concrètes, pas en gros chantiers.** Mauvais : *"Code le frontend."* Bon : *"Crée le composant `VoiceCapture.tsx` qui utilise Web Speech API et expose un callback `onTranscript(text: string)`. Réfère-toi à la Section 5 du blueprint pour le contrat."*

**Citer le blueprint dans le prompt.** Quand on demande à Claude Code de coder un endpoint, lui pointer la section exacte : *"Implémente `POST /api/inspections/{id}/constats` comme spécifié Section 5 et Section 6.2."*

**Plan mode pour les chantiers complexes.** Avant que Claude touche au code sur un sujet à forte incertitude (l'agent de capture, la génération DOCX), demander un plan d'abord : *"Plan only, don't code yet."*

**Sous-agents pour les explorations parallèles.** Un sous-agent peut explorer un sujet (ex : « quelle lib Python pour insérer une image dans un docx avec un caption ? ») pendant qu'on continue à coder ailleurs.

**Discipline de commit.** Demander à Claude Code de proposer un message de commit conventionnel à chaque PR de fonctionnalité. Format : `feat(backend): add capture agent and norm RAG retrieval`.

### Ce qu'il NE faut PAS demander à Claude Code

- Choisir l'architecture (déjà figée Section 2-5).
- Réécrire les prompts agents sans validation équipe (Section 6).
- Toucher au scénario de démo Section 9 (la chorégraphie est figée à partir de S3).

### Vérifications quotidiennes

À la fin de chaque journée :

```bash
# tests
cd backend && pytest -q
cd ../frontend && npm run typecheck && npm run lint

# smoke test agents
python -m app.agents.preparation  # CLI rapide
python -m app.agents.capture
```

---

## Section 12 — `CLAUDE.md` à copier tel quel à la racine du repo

> Ce fichier est lu par Claude Code à chaque ouverture de session. Il doit rester **court** (cible : sous 100 lignes utiles). Tout le reste est dans `blueprint.md`, à référencer par section.

```markdown
# UC 28 — Inspection Augmentée

## Quoi
PWA de copilote IA pour les inspecteurs Bureau Veritas / APAVE.
Pipeline en 3 agents Claude : Préparation (check-list dynamique) → Capture (classification NC + sourcing norme) → Restitution (pré-rapport DOCX).
Démonstrateur hackathon, démo cible 3 min. Spec complète dans `blueprint.md`.

## Stack
- **Frontend** : Next.js 14 (App Router) + TypeScript + Tailwind. PWA mobile-first.
- **Backend** : FastAPI (Python 3.11+), SQLAlchemy 2 + Alembic, PostgreSQL 16.
- **LLM** : Claude Sonnet 4.6 (`claude-sonnet-4-6`) via SDK `anthropic`.
- **RAG** : ChromaDB embedded (`./storage/chroma/`). Corpus dans `backend/corpus/`.
- **DOCX** : `python-docx`.
- **Voix** : Web Speech API (browser natif).

## Commandes
```bash
# dev environment complet
docker-compose up -d           # postgres + chromadb
cd backend && uvicorn app.main:app --reload     # backend :8000
cd frontend && npm run dev                       # frontend :3000

# tests
cd backend && pytest -q
cd frontend && npm run typecheck && npm run lint

# migrations
cd backend && alembic upgrade head
cd backend && alembic revision --autogenerate -m "message"

# ingestion corpus
cd backend && python -m app.rag.ingest

# smoke agent (CLI)
cd backend && python -m app.agents.preparation
```

## Conventions
- **Langue** : code et commits en anglais ; messages métier (UI, prompts, DOCX) en français.
- **Commits** : Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`).
- **Tests** : Pytest pour le backend. Tests minimaux mais sur les agents (au moins un test par agent qui mocke l'API).
- **Pas d'API keys dans le repo.** Toutes les secrets via `.env` (jamais commité). `.env.example` à jour.
- **Typage strict** : `mypy` côté backend (best effort), `strict: true` côté frontend.

## Spec source de vérité
- Modèle de données : `blueprint.md` Section 4.
- Contrats d'API : `blueprint.md` Section 5.
- Prompts agents : `blueprint.md` Section 6. **Ne pas réécrire les prompts sans validation équipe.**
- Sprint en cours : voir `blueprint.md` Section 8 + onglet projet GitHub/Jira.

## Gotchas
- Le texte intégral des normes ISO 9001 et 19011 est **payant**. Le corpus dans `backend/corpus/` ne contient que des **reformulations publiques**. Ne jamais committer du texte normatif brut.
- Le scope démo s'arrête à **ISO 9001**. Pas d'autres référentiels avant la fin de S3.
- La vision Claude (photo dans Agent 2) ne doit être appelée que si une photo est effectivement attachée — sinon coût inutile.
- Web Speech API ne fonctionne **pas** sur iOS Safari < 14.5 et pas en HTTP — toujours servir la PWA en HTTPS pour les tests mobile.

## Démo
Scénario figé : `blueprint.md` Section 9. Ne pas modifier sans accord équipe à partir de S3.
```

---

**Fin du blueprint.** Document vivant — toute décision prise pendant le sprint qui modifie une section doit être reportée ici, et un commit dédié `docs(blueprint): ...` doit signaler le changement.
