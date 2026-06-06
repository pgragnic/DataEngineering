# FAQ — Questions probables du jury
## UC 28 — Inspection Augmentée · Hackathon Capgemini × Anthropic 2026

---

## Technique

**Q : Et si l'API Claude est indisponible pendant la démo ?**
> Fallback automatique sur le moteur local : RAG ISO 9001 + règles métier codées dans `agent.py`.
> Le badge dans l'interface passe de "Claude Opus" à "IA locale" — la détection continue de fonctionner, sans réseau.

**Q : Quel modèle Claude utilisez-vous exactement ?**
> `claude-sonnet-4-6` via l'API Anthropic. Dans la démo, le badge "Claude Opus" fait référence à l'usage initial de Claude Opus 4 côté GEP Capgemini — la version finale tourne sur Sonnet 4.6. Les deux modèles sont capables d'analyse ISO en temps réel.

**Q : Votre RAG couvre quelle partie de la norme ISO 9001 ?**
> Les clauses **§4 à §10** (exigences opérationnelles) — soit les 80+ points de contrôle terrain. Les clauses §1-3 (contexte, leadership) sont gérées par règles fixes. L'architecture est extensible à ISO 14001, ISO 45001, EN 9100.

**Q : Pourquoi Leaflet plutôt que Google Maps pour la carte ?**
> Zéro clé API, zéro coût, fonctionne hors-ligne. La démo est autonome — aucune dépendance à un service tiers payant le jour du pitch.

**Q : La reconnaissance vocale, c'est vraiment en temps réel ?**
> Oui — Web Speech API du navigateur (Chrome requis, `fr-FR`). La transcription est côté client, sans appel backend. Le texte transcrit alimente ensuite le formulaire avant l'analyse Claude.

**Q : Qu'est-ce que le prompt caching apporte concrètement ?**
> Le system prompt (instructions d'analyse ISO) est mis en cache chez Anthropic. Sur les appels répétés — typiquement une journée d'audit avec 10+ constats — la réduction de coût est d'environ **80 %** par rapport à un appel sans cache.

---

## Business

**Q : Vous avez de vrais clients ? C'est vraiment RATP ?**
> La démo utilise des données fictives inspirées de la réalité RATP (sites, clauses, personas). Le cas d'usage est réel : les auditeurs qualité de la RATP (et de leurs prestataires Apave, Bureau Veritas) gèrent bien des audits ISO terrain. Ce prototype répond à un problème documenté dans le secteur.

**Q : Quel est votre modèle économique ?**
> SaaS B2B : abonnement par auditeur actif / mois. Cible : cabinets d'audit (Apave, Bureau Veritas, SGS) et entreprises avec équipes qualité internes (industrie, transport, énergie). Upsell : intégration SIRH / ERP, modules normes supplémentaires (ISO 14001, 45001).

**Q : Qu'est-ce qui vous différencie des outils existants (iAuditor, Novatech, SafetyCulture) ?**
> Trois différenciants :
> 1. **IA contextuelle** — les concurrents utilisent des templates fixes. Claude analyse le constat *dans son contexte* (site, historique, responsable, clause) et génère une réponse sur-mesure.
> 2. **RAG normatif** — le corpus ISO est embarqué, pas une base de règles figée.
> 3. **Zéro saisie clavier sur le terrain** — voix + tap, le rapport se construit pendant l'audit.

**Q : Pourquoi l'auditeur accepterait de changer ses habitudes ?**
> Parce qu'on ne lui demande pas d'apprendre un nouvel outil complexe. Il parle, l'IA structure. Il tape, l'IA analyse. La seule interface critique (écran 3/4) ressemble à une messagerie — pas à un ERP.

---

## Données & sécurité

**Q : Les données d'audit (non-conformités, noms de sites) partent chez Anthropic ?**
> Oui, dans les appels API — c'est le fonctionnement de l'API Anthropic. En production, deux options :
> 1. **Anonymisation à la source** : le prénom/site est remplacé par un identifiant avant l'appel (déjà maquetté dans le toggle RGPD du rapport).
> 2. **Déploiement on-premise** : Claude peut être déployé sur infrastructure privée via AWS Bedrock ou Azure AI — sans transfert de données hors périmètre.

**Q : La clé API est sécurisée comment ?**
> Dans `backend/.env`, jamais commitée (`.gitignore`). En production : variable d'environnement injectée par le pipeline CI/CD, jamais en clair dans le code.

**Q : RGPD — vous avez prévu quoi ?**
> Toggle "Anonymiser" dans le rapport (démo : slide 5) — remplace noms et identifiants dans l'aperçu en un clic. En production : pseudonymisation des données personnelles avant stockage SQLite, journalisation des accès, DPO à intégrer au contrat SaaS.

---

## Scalabilité & déploiement

**Q : Ça tient avec 1 000 auditeurs simultanés ?**
> L'architecture est stateless côté backend (FastAPI + SQLite → remplacer par PostgreSQL). Scalabilité horizontale sur Kubernetes : chaque instance FastAPI est indépendante. Le RAG est préchargé en mémoire par instance — OK en auto-scaling. Le vrai goulot d'étranglement est l'API Anthropic (rate limits) — gérable avec une file de messages (Redis/Celery).

**Q : Combien de temps pour déployer en production chez un vrai client ?**
> 3 phases estimées :
> - **Intégration données** (2 sem.) : import sites, historique audits, corpus normatif client
> - **Customisation IA** (1 sem.) : ajout clauses spécifiques au référentiel client
> - **Formation utilisateurs** (1 jour) : l'interface est intentionnellement simple

**Q : Vous avez fait ça en combien de temps ?**
> 5 jours de développement effectif (sprint hackathon). La base technique (FastAPI + React + Claude API) était en place en J+2 ; les fonctionnalités IA avancées (RAG, suggestions, questions oui/non) ont été ajoutées en J+3 à J+5.

---

## Checklist avant de monter sur scène

- [ ] `start.bat` lancé 3 min avant → backend démarré
- [ ] `localhost:8000/docs` accessible (Swagger OK)
- [ ] `localhost:5173` accessible (frontend OK)
- [ ] Log "Application startup complete" visible (RAG chargé — ~30 s)
- [ ] `.env` vérifié : `ANTHROPIC_API_KEY` valide (format `sk-ant-api03-…`)
- [ ] Chrome ouvert (Web Speech API pour le micro)
- [ ] Son activé sur la machine · micro système actif
- [ ] Image de test prête pour la photo (n'importe quel JPG)
- [ ] Écran en 16:9, zoom navigateur à 100 %
- [ ] Slide deck `Pitch_Jury_UC28.pptx` ouvert en mode présentation (prêt à basculer)
