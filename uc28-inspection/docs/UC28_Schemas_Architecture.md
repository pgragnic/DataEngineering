# Schémas d'architecture — UC 28 Inspection Augmentée

> Équipe Code Resonance · Hackathon Capgemini × Anthropic 2026
> Astuce : coller chaque bloc dans https://mermaid.live pour visualiser/exporter.

---

## Diagramme 1 — Architecture globale (C4 niveau 2 / conteneurs)

Montre les deux acteurs (auditeur BV et fournisseur RATP), la SPA React, le backend FastAPI et les services d'intelligence (gateway IA GEP + RAG ISO 9001 sur SQLite).

```mermaid
flowchart TB
    auditeur["👤 Auditeur BV<br/>Marc Lefèvre"]
    fournisseur["🏢 Fournisseur RATP<br/>Karim Belkacem"]

    subgraph front["Frontend — React + Vite (SPA)"]
        app["App.jsx<br/>routeur d'état"]
        portal["SupplierPortal.jsx<br/>portail fournisseur"]
        capture["InspectionCapture.jsx<br/>capture terrain"]
    end

    subgraph back["Backend — FastAPI (main.py)"]
        api["Routes REST"]
        agent["agent.py<br/>règles + criticité + actions"]
        rag["rag.py<br/>recherche de clause"]
        rapport["rapport.py<br/>export .docx"]
    end

    gep["☁️ Gateway GEP Capgemini<br/>Claude (client OpenAI-compatible)"]
    faiss["🔎 RAG ISO 9001<br/>FAISS + MiniLM-L12"]
    db[("🗄️ SQLite<br/>clauses_iso · sites · audits_historiques")]

    auditeur --> app
    fournisseur --> portal
    portal --> app
    app --> capture
    front -- "HTTP / JSON (src/api.js)" --> api
    api --> agent
    api --> rag
    api --> rapport
    agent -- "enrichissement (optionnel)" --> gep
    agent --> rag
    rag --> faiss
    faiss -. "clauses" .-> db
    agent -. "contexte site" .-> db
    rapport -- ".docx" --> auditeur
```

---

## Diagramme 2 — Séquence d'analyse d'une observation ISO

Flux complet depuis la saisie vocale jusqu'à l'affichage du badge de non-conformité, avec le repli sur les règles métier si l'IA est indisponible.

```mermaid
flowchart TD
    start(["🎙️ Saisie vocale / texte<br/>Web Speech API (fr-FR)"]) --> obs["Observation terrain"]
    obs --> post["POST /analyser<br/>{ observation, site_id? }"]
    post --> clause["rag.trouver_clause()<br/>embedding + FAISS k=1"]
    clause --> crit["_evaluer_criticite()<br/>mots-clés normalisés"]
    crit --> ctx{"site_id<br/>fourni ?"}
    ctx -- "oui" --> load["_get_site_context()<br/>historique 3 derniers audits"]
    ctx -- "non" --> rules["Diagnostic & actions<br/>par règles (_ACTIONS)"]
    load --> llm{"GEP_API_KEY<br/>présente ?"}
    rules --> llm
    llm -- "oui" --> enrich["_enrichir_diagnostic_llm()<br/>diagnostic + actions contextualisés"]
    llm -- "non / erreur" --> fallback["Fallback règles métier<br/>(silencieux)"]
    enrich --> resp["Réponse JSON<br/>clause · criticité · score · actions"]
    fallback --> resp
    resp --> badge["🏷️ Badge NC + score X/3<br/>+ alerte récurrence 🔁 si applicable"]
```

---

## Diagramme 3 — Navigation frontend (machine à états)

États pilotés par `view` dans App.jsx. La transition initiale dépend du rôle choisi au login (auditeur vs fournisseur).

```mermaid
stateDiagram-v2
    [*] --> login
    login --> portail : rôle = fournisseur
    login --> clients : rôle = auditeur
    portail --> login : retour
    clients --> dashboard : client sélectionné
    dashboard --> selection : missions confirmées
    selection --> brief : démarrer
    selection --> dashboard : retour
    brief --> inspection : démarrer l'inspection
    brief --> dashboard : retour
    inspection --> report : générer le pré-rapport
    inspection --> brief : retour
    report --> inspection : retour
    report --> dashboard : nouvel audit

    note right of dashboard
        Vue planning (PlanningOverlay) :
        missions, trait temps réel,
        carte Leaflet, filtres
    end note
    note right of inspection
        Capture : voix + photo,
        analyse ISO, suggestions,
        questions oui/non, récurrence
    end note
```
