# UC 28 — 30 questions de jury & réponses calibrées

> Matériel d'entraînement Q&A pour les jurys intermédiaire (8-12 juin) et final (16-19 juin).
>
> **Comment l'utiliser.**
> - Lisez en équipe. Identifiez les 5 questions qui vous mettent le plus mal à l'aise.
> - Pour chacune : qui répond en démo ? Préparez une formulation à 2 phrases max.
> - Répétez à blanc une fois par semaine à partir du 26 mai.
>
> **Légende.** ⚡ = piège classique. 🧠 = question profonde. 🛠 = technique. 🎯 = métier.

---

## AXE 1 — TECHNIQUE (6 questions)

### Q1.1 — Pourquoi 3 agents et pas un seul ? 🛠

**Réponse en 2 phrases.** Parce que les trois temps de l'audit ont des contraintes radicalement différentes : préparation = synthèse documentaire lourde (RAG sur ~250 pages de normes), capture = latence critique (<3s pour ne pas casser le geste métier), restitution = composition narrative longue. Un agent unique serait soit trop lent en capture, soit trop léger en restitution. La spécialisation permet d'optimiser modèles, prompts et caches indépendamment.

**Données à connaître.** Capture en Sonnet 4.6 avec prompt caching sur les chunks RAG. Préparation et Restitution sans cache, appel unique long. Coût démo complet ≈ 0,15 € par audit.

---

### Q1.2 — Que se passe-t-il si Claude classifie mal un constat critique ? 🧠

**Réponse.** L'inspecteur reste maître. Chaque classification est présentée comme une **suggestion** : il peut valider, modifier ou refaire. La traçabilité du raw_text est préservée. C'est exactement le même rôle qu'un assistant junior face à un senior — il propose, l'expert décide. La conformité réglementaire repose sur la signature de l'inspecteur, pas sur l'IA.

**Argument annexe.** En production, un mode « audit qualité IA » permettrait de comparer mensuellement les corrections humaines aux suggestions pour mesurer la dérive du modèle.

---

### Q1.3 — Comment vous gérez les hallucinations sur les références de normes ? ⚡

**Réponse.** Deux garde-fous : (1) le prompt système de l'Agent 2 lui interdit explicitement de citer un article qui n'est pas dans les chunks RAG remontés — s'il ne trouve pas, il met `norm_reference: null`. (2) La bande RAG transparente affiche les 3 chunks utilisés avec leur score de similarité ; le jury et l'inspecteur voient la source. Si un chunk pertinent n'est pas remonté à >0,7 de similarité, on flag la classification comme « à vérifier ».

**Démonstration possible.** « Voulez-vous que je dicte un constat hors-norme pour vous montrer ce qui se passe ? »

---

### Q1.4 — Le texte intégral d'ISO 9001 est payant. Comment vous avez géré ? 🛠

**Réponse.** Notre corpus dans le repo contient uniquement des **reformulations publiques** : sommaires officiels ISO, guides AFNOR, INRS, paraphrasages rédigés par notre équipe et validés par notre référent métier. C'est cohérent avec la pratique de production : un client Bureau Veritas qui déploierait la solution travaillerait sur sa copie sous licence du référentiel, intégrée en privé.

**À ne pas dire.** « On a copié les normes » — tout le projet s'effondre sur la propriété intellectuelle.

---

### Q1.5 — Quelle latence sur la classification en démo ? 🛠

**Réponse.** Cible : 2-4 secondes entre fin de dictée et apparition de la carte classifiée. C'est mesuré en S2. Les composants : voice-to-text (Web Speech, instantané, dans le navigateur), retrieval RAG sur ChromaDB (50-100 ms), appel Sonnet avec prompt caching (~1,5 s en cache hit). Sans cache, on monte à 3-5 secondes.

**Si on demande pourquoi pas plus vite.** Sonnet privilégie la qualité du raisonnement. Haiku ferait 700 ms mais classifie mal les cas subtils — on a testé.

---

### Q1.6 — Vous avez vraiment utilisé Claude Code pour tout coder ? 🧠

**Réponse honnête.** ~75-80% du code est généré ou réécrit par Claude Code. Le reste : prompts agents, intégrations spécifiques, ajustements de l'UX au pixel près, et résolution de bugs où Claude tournait en boucle. Les patterns qui ont le mieux marché : plan mode avant chaque feature, sub-agents pour les explorations parallèles, un seul `CLAUDE.md` partagé maintenu rigoureusement. Le pattern qui n'a pas marché : laisser Claude designer l'UX sans maquette en input — il fait du générique. Détails dans notre contribution au Livre Blanc.

---

## AXE 2 — MÉTIER (6 questions)

### Q2.1 — Qui est votre utilisateur cible exactement ? 🎯

**Réponse.** Antoine Mercier, 42 ans, Lead Auditor Bureau Veritas Certification basé à Lyon, 14 ans d'expérience, 80 audits par an. Aujourd'hui il passe 70% de son temps en rédaction documentaire et seulement 30% sur le terrain. Notre cible est d'inverser ce ratio. (Sortir la persona physique si projetable.)

**Pourquoi cette précision.** Si on répond « les auditeurs », on perd. Le jury teste si on a une vision incarnée.

---

### Q2.2 — Pourquoi Bureau Veritas et pas un autre nom ? 🎯

**Réponse.** Trois raisons : (1) c'est le leader mondial du marché TIC, 85 000 collaborateurs dans 140 pays — l'effet de levier en cas de partenariat est massif. (2) Capgemini a déjà des liens commerciaux avec eux, on s'inscrit dans un compte existant. (3) Leur modèle économique repose sur la productivité des inspecteurs, qui est exactement le levier qu'on actionne. Mais la même solution adresse APAVE, RATP Maintenance et tout opérateur d'inspection — c'est même le slide platform de notre pitch.

---

### Q2.3 — Le retour sur investissement, vraiment ? ⚡

**Réponse.** Ordres de grandeur, à valider en pilote terrain. Sur une équipe de 30 inspecteurs : si on libère 4h par audit sur 200 audits/an/inspecteur, ça fait 24 000 heures, soit 15 ETP redéployables. À ~60 k€ chargés par ETP, c'est 900 k€/an de capacité libérée. Une licence à 30 k€/an par équipe se rembourse en un mois. **Mais.** Ces chiffres sont des cibles de pilote, pas une étude de cas. Le jury devra calibrer sur ses propres données.

**À ne pas faire.** Annoncer une économie en € comme un chiffre acquis — ça décrédibilise.

---

### Q2.4 — Pourquoi un inspecteur accepterait-il d'être « augmenté » ? 🧠

**Réponse.** Parce qu'on n'enlève pas son travail d'expert — on enlève son travail administratif. Antoine ne perd pas l'arbitrage NC majeure/mineure ; il garde la décision finale, la signature, la relation client. Ce qu'il perd, c'est 4 heures de transcription au bureau. C'est le rapport gagnant qu'aucun outil de KPI imposé d'en haut n'a jamais offert : moins d'admin, plus de métier. C'est aussi pour ça qu'on a appelé l'app « BV·Inspect » et pas « AuditAI » : c'est l'outil de l'inspecteur, pas un outil sur l'inspecteur.

---

### Q2.5 — Et la responsabilité juridique si l'IA se trompe ? ⚡

**Réponse.** L'inspecteur reste seul responsable, comme aujourd'hui avec ses outils Word et ses référentiels. L'IA est un assistant qui propose, l'inspecteur signe. La traçabilité est même *renforcée* : on conserve le raw_text, la classification proposée, la classification finale, les chunks RAG remontés et le timestamp. En cas de litige client, l'inspecteur a une preuve auditable que sa décision était documentée — c'est plus fort que des notes manuscrites.

---

### Q2.6 — Comment l'inspecteur va apprendre à l'utiliser ? 🎯

**Réponse.** Volontairement très peu de courbe d'apprentissage. L'UI mime ses gestes actuels (parler, photographier, valider). L'onboarding tient en 15 minutes : présentation de la check-list générée, du flux capture, de la génération de rapport. Le reste s'apprend en faisant. On vise une adoption sous 1 semaine sur une équipe pilote. Le risque réel n'est pas la formation, c'est le change management interne — d'où l'importance de partir de la base, pas de l'imposer.

---

## AXE 3 — VIABILITÉ DU PROJET (6 questions)

### Q3.1 — Pourquoi seulement ISO 9001 ? Vous vous limitez. ⚡

**Réponse.** C'est un choix tactique de scope démo. ISO 9001 est la norme la plus générique et publiquement documentée : on peut faire une démo crédible sans dépendre de données client. Notre architecture est agnostique : ajouter NFC 15-100, ATEX, ISO 14001 ou ISO 45001 ne demande qu'un nouveau corpus ingéré dans le RAG. On le démontre d'ailleurs en fin de pitch avec la bascule platform.

---

### Q3.2 — Vous n'avez pas de vraies données d'audit. Comment vous validez la qualité ? 🧠

**Réponse.** Trois niveaux : (1) notre référent métier nous fournit 10 constats de référence avec leurs classifications attendues — on mesure le taux de classification correcte (cible : 7/10 minimum). (2) Le scénario démo « Fournisseur ALPHA » a été validé par lui sur la cohérence métier des suggestions d'action. (3) Tout pilote terrain devrait commencer par une phase de calibration sur 50-100 audits réels — c'est dans notre proposition de pilote BV.

---

### Q3.3 — Et si Bureau Veritas refuse de partager ses normes internes propriétaires ? 🎯

**Réponse.** Notre offre ne dépend pas de leur partage. Soit ils ingèrent eux-mêmes leur corpus dans un environnement isolé — l'architecture le permet, le code est ouvert pour eux. Soit on travaille sur ISO 9001 + référentiels publics pour démarrer et on étend ensuite. Soit on signe un NDA. Trois trajectoires, aucune ne nous bloque sur la valeur démontrable.

---

### Q3.4 — Vous êtes 3, en 4 semaines. C'est crédible ? ⚡

**Réponse honnête.** C'est tendu mais cadré. On a un blueprint qui découpe le travail en 10 sessions Claude Code claires, un sprint S1 dédié aux fondations, et un scope volontairement minimal (un seul référentiel, une seule UI, un seul cas démo). Le risque réel n'est pas le code — c'est la qualité de la démo. C'est pour ça qu'on a réservé la S4 entière au polish, à la vidéo de secours et aux répétitions.

---

### Q3.5 — Si on vous donne 6 mois et un budget, qu'est-ce que vous livrez ? 🧠

**Réponse.** Pilote terrain avec Bureau Veritas Certification sur 1 ligne d'activité, équipe de 5-10 auditeurs, 50-100 audits réels. Mesure des gains (délai audit→rapport, taux de classification correcte, satisfaction inspecteur). Industrialisation du corpus RAG sur 3-5 référentiels. Mode reviewer pour le back-office. Intégration possible avec l'outillage existant BV. À la sortie : un cas d'usage chiffré, validé, replicable sur APAVE et RATP.

---

### Q3.6 — Pourquoi pas un partenariat technologique avec un acteur existant du secteur ? ⚡

**Réponse.** Aucun acteur du secteur ne couvre aujourd'hui la combinaison voix + classification IA + sourcing automatique + génération DOCX en pipeline temps réel. Les outils existants (BV·Qualios, APAVE Smart-Inspect) sont des CRM d'audit, pas des copilotes terrain. Notre angle, c'est l'expérience de l'inspecteur, pas la gestion administrative. C'est pour ça qu'on parle de partenariat *Anthropic × Capgemini* avec BV comme client pilote, et non d'OEM avec un éditeur existant.

---

## AXE 4 — GTM & STRATÉGIE COMMERCIALE (6 questions)

### Q4.1 — Comment Capgemini gagne de l'argent là-dessus ? 🎯

**Réponse.** Trois modèles non exclusifs. (1) **Build & Run** classique : Capgemini livre et opère la solution pour BV, contrat pluriannuel. (2) **Solution accelerator** : on industrialise le socle pour le revendre à APAVE, SGS, TÜV, Dekra — chaque déploiement client est une mission de personnalisation. (3) **Platform play** : Capgemini × Anthropic propose un « Inspection-as-a-Service » sur lequel des consultants Capgemini configurent les référentiels métier par client. Le modèle 1 finance le 2, le 2 dérisque le 3.

---

### Q4.2 — Quel est le concurrent direct le plus dangereux ? ⚡

**Réponse.** Trois zones de pression. (1) Un éditeur existant du secteur (Qualios, AuditCV…) qui rajoute une couche IA sur son produit. Avantage : base installée. Désavantage : architecture vieille, pas pensée pour le terrain. (2) Une startup IA spécialisée audit — il en existe quelques unes aux US. Avantage : focus produit. Désavantage : ne connaît pas le métier français. (3) Le client lui-même qui internalise avec son équipe IA. C'est le scénario le plus dangereux à 18 mois — notre fenêtre est maintenant.

---

### Q4.3 — Si vous vendez à BV, comment vous adressez ses concurrents APAVE et SGS ? 🧠

**Réponse.** Deux options stratégiques. (1) **Exclusivité BV de 12-18 mois** échangée contre un pilote rapide et des références publiques. (2) **Solution-multitenant non exclusive** vendue à chaque acteur indépendamment, avec personnalisation des référentiels et de la marque. La première est plus simple à signer, la seconde a plus de plafond. Le choix dépend de l'appétit de BV pour porter le risque de la première version. C'est exactement le type de discussion stratégique qu'on attend avec l'AI Office I&D.

---

### Q4.4 — Et hors TIC, où ça va ? 🎯

**Réponse.** Toute organisation où un expert audite un site contre un référentiel normatif a le même besoin. Trois extensions naturelles : (1) **inspection réglementaire industrielle** — Apave équipements sous pression, ATEX, levage. (2) **audits qualité internes** — directions qualité de grands groupes industriels. (3) **maintenance ferroviaire et aéronautique** — RATP, SNCF, Airbus en MRO. Chaque vertical demande un corpus de référence dédié, mais le socle technique est identique. Notre POC le démontre.

---

### Q4.5 — Combien vous demanderiez à un client BV pour cette solution ? ⚡

**Réponse.** Pas une réponse au pifomètre. Ordres de grandeur de positionnement : licence par inspecteur, ~2-3 k€/an, modèle similaire aux outils SaaS métier. Sur 1 000 inspecteurs BV France, ça fait 2-3 M€ ARR. Vs ROI attendu pour BV (3-5 M€ de productivité libérée), le ratio est favorable. Pricing exact à valider en phase commerciale.

---

### Q4.6 — Vous pensez que Anthropic veut un partenariat sectoriel comme ça ? 🧠

**Réponse.** C'est exactement le mouvement qu'Anthropic pousse avec son programme Solutions Partners et avec l'orientation Claude vers les usages métier. Capgemini est déjà partenaire stratégique. Notre démonstrateur est une preuve que la combinaison Claude + expertise Capgemini sur un vertical industriel français peut générer un cas d'usage différencié. C'est typiquement le type de matériel que l'AI Office I&D cherche pour son GTM. (Et c'est pour ça qu'on est là.)

---

## AXE 5 — VIBE CODING & LIVRE BLANC (6 questions)

### Q5.1 — Qu'est-ce que Claude Code a *vraiment* changé dans votre façon de travailler ? 🧠

**Réponse.** Trois ruptures concrètes. (1) **Le coût d'exploration est divisé.** On a testé 3 architectures différentes en S1 avant de figer la bonne — impensable sans Claude Code. (2) **Le coût documentaire est externalisé.** Le code que Claude Code génère est commenté, documenté, testable. On a moins de dette technique à 4 semaines qu'un projet artisanal. (3) **La répartition des rôles change.** Le développeur senior devient un *architecte-réviseur* qui dirige des sessions ; le junior peut produire à un niveau senior s'il sait bien spécifier. Notre Livre Blanc détaille les patterns qui marchent et ceux qui foirent.

---

### Q5.2 — Quel pattern Claude Code vous a le plus surpris ? 🧠

**Réponse.** Le **plan mode** (Shift+Tab). Au début on l'évite — on veut du code tout de suite. Au bout d'une semaine, on s'aperçoit qu'un plan de 3 minutes économise 30 minutes de débogage. Maintenant on l'utilise systématiquement avant toute feature complexe. C'est devenu un rituel d'équipe.

**Si on creuse.** Autre découverte : les sub-agents pour les explorations parallèles. Très utile quand on hésite entre deux libs ou deux approches — on les lance en parallèle.

---

### Q5.3 — Quel piège Claude Code vous a fait tomber dedans ? ⚡

**Réponse.** La boucle de planification (cf. slide 12 du guide hackathon). On a perdu une demi-journée en S2 parce qu'on laissait Claude planifier sur un problème qu'il ne comprenait pas bien. La leçon : si rien ne bouge après 90 secondes, on interrompt et on dit « code maintenant, pas de plan ». Et si le code rate, on revient au plan en lui donnant plus de contexte. Pas l'inverse.

**Autre piège.** Tendance de Claude à over-engineering quand on lui demande quelque chose de simple. Antidote : énoncer les contraintes négatives (« pas d'authentification, pas de microservices, juste un endpoint qui marche »).

---

### Q5.4 — Quelle est votre stratégie modèle ? Sonnet ? Opus ? 🛠

**Réponse.** Discipline budgétaire stricte (200$ par équipe, on a vérifié). **Haiku** : génération des fixtures, données fictives, paraphrasage du corpus normatif. **Sonnet 4.6** : 90% du dev, les 3 agents en production, la majorité des sessions Claude Code. **Opus** : réservé aux deux ou trois moments où Sonnet a tourné en boucle sur un bug profond. On bascule via la commande `/models`. À la sortie du hackathon, on aura un coût total mesuré qu'on partagera dans le Livre Blanc.

---

### Q5.5 — Vous écrivez vraiment un Livre Blanc en parallèle ? 🧠

**Réponse.** C'est notre arme secrète. Trente minutes par sprint dédiées à documenter : (1) les prompts qui ont marché ou foiré, (2) les patterns de sub-agents qu'on a trouvés, (3) le ratio code généré / code écrit à la main, (4) le temps gagné mesuré sur 5 tâches comparables. À la fin des 4 semaines, on a un document brut qui devient la base d'une contribution au Livre Blanc AI Office I&D. C'est ce qui passe quand on optimise pour la livraison technique ET pour le partage de connaissance.

---

### Q5.6 — Si vous deviez recommencer demain, qu'est-ce que vous feriez différemment ? 🧠

**Réponse.** Deux choses. (1) **Commencer par le scénario démo avant l'architecture.** On a passé trop de temps en S1 sur le RAG avant d'avoir clairement scénarisé les 4 constats de la démo. Quand on a fait l'inverse en S2, tout est devenu plus simple — l'architecture sert le scénario, pas l'inverse. (2) **Recruter le référent métier dès J-1.** On a perdu une semaine à modéliser des concepts métier qu'un auditeur senior nous aurait clarifiés en 30 minutes. La règle qu'on retient : avant le code, l'expert.

---

## Annexe — Les 3 questions à craindre vraiment

Si on devait n'en retenir que trois, ce sont celles-là. Pour chacune, **chacun des 3 membres de l'équipe doit savoir y répondre seul** :

1. **« Concrètement, qu'est-ce qui empêche Bureau Veritas de faire ça en interne ? »** (test de viabilité GTM)
2. **« Si je vous donne 30 secondes pour me dire pourquoi vous, et pas une autre équipe ? »** (test de pitch)
3. **« Montrez-moi un cas où votre démo ne marche pas. »** (test d'honnêteté technique)

Répondre honnêtement, sans esquiver, et avec un point de vue, est plus important que d'avoir la « bonne » réponse. Les jurys reconnaissent les équipes qui ont *vraiment* réfléchi à leur projet.

---

**Fin du document.** À relire avant chaque jury. Itérer les réponses au fur et à mesure que le projet avance.
