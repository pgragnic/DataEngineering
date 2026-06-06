════════════════════════════════════════════════════════════════════
  SCRIPT DE DÉMO — UC 28 · INSPECTION AUGMENTÉE
  Hackathon Capgemini × Anthropic 2026 — Équipe Code Resonance
  Durée cible : 8 minutes · Débit ~140 mots/min
════════════════════════════════════════════════════════════════════

LÉGENDE
  [DIRE]      texte à prononcer
  [CLIC]      action souris à exécuter
  [POINTER]   montrer à l'écran, sans cliquer
  [PAUSE]     silence volontaire — laisser le jury absorber
  [COULISSES] note technique pour le présentateur — NE PAS lire
  [BONUS]     séquence optionnelle, si le temps le permet
  T           temps cumulé indicatif

RÔLES SUR SCÈNE
  Narrateur   Philippe — pilote la voix et le tempo
  Pilote app  Habib — exécute les clics (ou Philippe en solo)
  Renfort     Véronique — gère les questions UI / design du jury

ANCRAGE CDC (à garder en tête — ne pas réciter)
  BU              SPS — Services Marchands
  Modèle          Bureau Veritas achète · RATP & Apave sont audités
  5 douleurs      expertise rare · prépa longue & hétérogène ·
                  historique sous-exploité · constats subjectifs ·
                  rédaction chronophage
  5 valeurs       standardisation · gain de temps · respect des normes ·
                  capitalisation expertise BV · qualité/traçabilité/explicabilité
  Référents jury  Philippe Larroye · JC Formosa (Apave) ·
                  Jérôme Fouquet (BV) · Marvin Dufresne (RATP)


════════════════════════════════════════════════════════════════════
OUVERTURE — L'ACCROCHE                                        T 0:00
════════════════════════════════════════════════════════════════════

[Écran : slide titre. Ne pas encore montrer l'app.]

[DIRE — posé, presque à voix basse]
  "Il est 21 heures.
   Marc a terminé son audit à 16 heures.
   Et il est encore devant son écran."

[PAUSE 2 secondes]

[DIRE — accélérer légèrement]
  "Il rédige son rapport. Il cherche la bonne clause ISO dans ses notes.
   Il recroise les constats du dernier passage, il y a six mois.
   Marc est un expert. Vingt-trois ans de terrain.
   Et il passe quatre-vingt-dix pour cent de son temps sur de la paperasse.
   Dix pour cent sur le terrain."

[PAUSE 2 secondes — laisser le chiffre tomber]

[DIRE — ferme, c'est la promesse]
  "Nous, on a inversé ce ratio.
   Je vous présente l'Inspection Augmentée."

[CLIC → slide suivante]


════════════════════════════════════════════════════════════════════
LE CADRE — ÉQUIPE, MODÈLE, ENJEU                             T 0:40
════════════════════════════════════════════════════════════════════

[DIRE]
  "Code Resonance — Habib, Véronique et moi-même, Philippe.

   Le modèle est simple.
   Bureau Veritas achète la solution pour ses auditeurs.
   RATP et Apave sont les sites audités.
   Une seule plateforme, deux faces : l'auditeur d'un côté,
   le fournisseur de l'autre — et le même moteur d'IA qui relie les deux."

[PAUSE]

[DIRE]
  "Le cahier des charges liste cinq douleurs :
   une expertise rare et difficile à transmettre,
   une préparation longue qui change d'un inspecteur à l'autre,
   un historique d'audits qu'on n'exploite jamais sur le terrain,
   des constats parfois subjectifs,
   et des rapports qui dévorent les soirées.

   On va les traiter toutes les cinq. Avant l'audit, pendant, et après.
   Huit minutes. Une seule journée : celle de Marc Lefèvre.
   Ça commence maintenant."

[CLIC → basculer sur le navigateur, onglet localhost:3000]


════════════════════════════════════════════════════════════════════
ÉCRAN 0 — CONNEXION                                          T 1:15
════════════════════════════════════════════════════════════════════

[CLIC → localhost:3000 déjà ouvert]

[DIRE]
  "Deux identités dans l'application.
   Marc Lefèvre, auditeur Bureau Veritas — c'est lui qu'on suit.
   Et Mei Lin Zhang, responsable qualité côté RATP,
   qui alimente le portail fournisseur. On y reviendra."

[CLIC → carte « Marc Lefèvre — Auditeur BV »]

[DIRE]
  "Son profil est déjà chargé. Aucune saisie. On se connecte."

[CLIC → « Se connecter »]


════════════════════════════════════════════════════════════════════
ÉCRAN 0.2 — CHOIX DU CLIENT                                  T 1:30
════════════════════════════════════════════════════════════════════

[DIRE]
  "Aujourd'hui, Marc intervient pour la RATP. Douze missions ce mois-ci."

[CLIC → carte RATP]


════════════════════════════════════════════════════════════════════
ÉCRAN 1 — TABLEAU DE BORD                                    T 1:40
════════════════════════════════════════════════════════════════════

[DIRE]
  "Sa journée, d'un seul regard.
   Quatre audits. Le trait rouge, c'est l'heure qu'il est.
   Marc sait où il en est sans ouvrir sa boîte mail."

[CLIC → une carte de mission → laisser l'animation de la carte se recentrer]

[DIRE]
  "La carte se cale sur le site.
   Itinéraire routier réel, calculé à la volée.
   Pas de clé API, pas de coût — ça tourne même hors ligne."

[CLIC → bascule « Planning »]

[DIRE]
  "Vue planning : les créneaux, et le temps de trajet déjà intégré —
   trente-cinq minutes pour Sucy-en-Brie."

[CLIC → bascule « Liste » → filtre « PROCHAIN »]

[DIRE]
  "Prochain rendez-vous : atelier de Sucy-en-Brie.
   Maintenance des rames MI09 du RER A. Il lance."

[CLIC → bouton « Démarrer » sur la carte Sucy-en-Brie]

  [COULISSES] La mission Sucy porte le flag alwaysDemarrer :
             le bouton « Démarrer » reste affiché quelle que soit l'heure.


════════════════════════════════════════════════════════════════════
ACTE I — AVANT L'AUDIT · LE BRIEF                            T 2:20
════════════════════════════════════════════════════════════════════

[DIRE]
  "Acte un : avant.
   Marc n'a pas encore quitté son bureau — et son brief est déjà prêt.
   Deux cent dix-huit personnes sur le site.
   Responsable qualité : Karim Belkacem.
   Périmètre : maintenance des MI09."

[PAUSE — pointer l'alerte rouge dans l'historique]

[DIRE — ralentir, c'est un point fort]
  "Et là, l'alerte.
   Une non-conformité ouverte depuis novembre 2024. Jamais clôturée.
   Marc n'a rien eu à se rappeler.
   L'application, elle, s'en souvient pour lui.
   Première douleur traitée : l'historique enfin exploité."

[POINTER l'en-tête « Check-list auto-générée » + l'icône ⓘ]

[DIRE]
  "La check-list est générée par l'agent, structurée par clause ISO.
   Section étalonnage, section non-conformités — directement dans le scope.
   Ce n'est pas un modèle générique : elle est taillée pour Sucy-en-Brie.
   Un clic sur le petit i, et on voit exactement d'où elle vient."

  ┌─ COULISSES — Les 5 sources de la check-list ─────────────────────┐
  │ 1. RÉFÉRENTIEL     ISO 9001:2015 (sélectionnable dans le Brief)   │
  │ 2. SCOPE MISSION   §7.1.5 Métrologie · §8.7 NC → sections S2/S3   │
  │ 3. CONTEXTE SITE   atelier maintenance, 218 pers., durée 2 h 30   │
  │ 4. HISTORIQUE NC   NC §7.1.5 ouverte depuis nov. 2024 → S2 prio   │
  │ 5. DOCS FOURNISS.  3 docs Mei Lin Zhang pré-analysés → alertes ⚠ │
  │                                                                    │
  │ S1 §7.5  documentaire générique                                   │
  │ S2 §7.1.5 étalonnage — cœur du scope + récidive                   │
  │ S3 §8.7  non-conformités — cœur du scope                          │
  │ S4 §7.2  compétences — lié à l'effectif de 218 personnes          │
  └────────────────────────────────────────────────────────────────────┘

[DIRE]
  "Marc reste maître de sa check-list.
   Un point ne le concerne pas ? Il le retire."

[CLIC → survoler un item → cliquer le × → l'item disparaît]
[CLIC → survoler une section → cliquer le × → la section disparaît]

[DIRE]
  "Item ou section entière, au choix.
   Et il peut ajouter ses propres points."

[CLIC → « + Ajouter un point » → saisir un point → Entrée]

[DIRE]
  "Son point apparaît avec le badge « Auditeur ».
   Voilà comment l'expertise de Marc entre dans le système —
   et devient réutilisable. Deuxième douleur : la prépa se standardise,
   sans écraser le savoir de l'auditeur."

[PAUSE — pointer les badges ⚠ RATP sur §7.1.5 et §8.7]

[DIRE]
  "Ces alertes RATP : ce sont les documents déposés par Mei Lin Zhang,
   déjà analysés par Claude. Les clauses à risque remontent
   directement dans la check-list de Marc.
   La plateforme à deux faces, en action — un seul moteur derrière."

[CLIC → « Démarrer l'inspection »]


════════════════════════════════════════════════════════════════════
ACTE II — PENDANT L'AUDIT · LE TERRAIN                       T 3:30
════════════════════════════════════════════════════════════════════

[DIRE]
  "Acte deux : pendant.
   Marc est sur site. Gants aux mains, devant les équipements.
   Il choisit le point qu'il va contrôler."

[CLIC → item §7.1.5 « Vérification des certificats d'étalonnage »]

[PAUSE — laisser charger suggestions + questions]

[DIRE]
  "Un seul clic, et trois choses arrivent en même temps :
   des exemples d'observations adaptés à ce point,
   trois questions de vérification oui/non à droite,
   et les articles de la norme qui se mettent à jour.
   Tout est contextualisé : étalonnage, atelier Sucy, historique inclus."

  ┌─ COULISSES — Questions oui/non générées par Claude ──────────────┐
  │ Déclencheur : clic sur un item · Route : POST /questions_oui_non │
  │ Contexte envoyé : texte de l'item + clause ISO + titre de section│
  │ Sortie : 3 questions terrain, pas des templates. Ex. :           │
  │   « Certificats datés de moins de 12 mois ? »                    │
  │   « Équipement couvert par un labo accrédité COFRAC ? »          │
  │   « Registre des équipements à jour ? »                          │
  │ Les réponses sont réinjectées dans le prompt d'analyse.          │
  │ Fallback : 3 questions statiques si GEP indisponible.            │
  └────────────────────────────────────────────────────────────────────┘

[CLIC → répondre oui/non à 2 questions]

[DIRE]
  "Marc qualifie en deux gestes.
   Ces réponses partent avec le constat — Claude saura ce qu'il a vu."

[CLIC → bouton micro]

[DIRE — clairement, micro actif]
  "Les clés dynamométriques du poste 12 ne sont pas étalonnées.
   Certificats périmés depuis huit mois."

[CLIC → micro pour couper]

[DIRE]
  "Dicté, transcrit. Les mains restent sur le terrain. Une photo —"

[CLIC → « Prendre une photo » → choisir une image]

[DIRE]
  "— et il analyse."

[CLIC → « Analyser »]

[PAUSE — NE PAS PARLER pendant le chargement]
[PAUSE encore 2 secondes après l'affichage — laisser respirer]

[DIRE — bas, laisser l'écran parler]
  "Non-conformité majeure.
   Clause 7.1.5 — étalonnage et surveillance des équipements.
   Action corrective : étalonnage COFRAC immédiat.
   Action préventive : planning automatisé."

[POINTER le badge « Claude Opus »]

[DIRE]
  "Ce diagnostic vient de Claude, via la plateforme GEP de Capgemini.
   Pas une règle figée. Pas un copier-coller.
   Il a tenu compte des rames MI09, de l'atelier Sucy,
   du responsable, de la NC de novembre dernier.
   Troisième et quatrième douleurs réglées d'un coup :
   le constat est objectif, et il est généré pour Marc, pas par Marc."

[POINTER l'alerte récidive 🔁 si visible]

[DIRE]
  "Récidive. Ce point était déjà non conforme en novembre 2024.
   Marc le voit à l'instant où il le constate."

[POINTER les boutons « Confirmer » / « Corriger »]

[DIRE — c'est la valeur stratégique, appuyer]
  "Et voici le cœur du dispositif.
   Marc valide le diagnostic — ou il le corrige.
   Chaque correction est enregistrée.
   Vingt-trois ans d'expertise Bureau Veritas qui, jusqu'ici,
   repartaient à la retraite avec lui — capturés, et redistribués.
   C'est la capitalisation de l'expertise, noir sur blanc dans le CDC."

[POINTER les articles RAG, bas de colonne centrale]

  ┌─ COULISSES — Sélection des articles normatifs (RAG) ─────────────┐
  │ Déclencheur : clic sur un item de la check-list                  │
  │ Logique : clause « 7.1.5 » → métrologie · « 8.7 » → NC ·         │
  │           autre → articles par défaut (§9.2, §10.3, §6.1)        │
  │ Base : corpus ISO 9001:2015 indexé par sentence-transformers     │
  │        (BERT multilingue, préchargé au démarrage du backend)     │
  │ Tooltip au survol : clause + titre officiel + extrait verbatim   │
  └────────────────────────────────────────────────────────────────────┘

[CLIC → survoler un article RAG → montrer l'extrait verbatim]

[DIRE]
  "Les articles ISO de référence. Au survol, le texte exact de la norme.
   Marc peut citer sa source sans jamais quitter l'écran.
   L'IA n'est pas une boîte noire : elle est explicable, et traçable.
   Exigence réglementaire du CDC — on y répond directement."

[CLIC → « Ajouter au rapport »]

[DIRE]
  "Deuxième constat, plus rapide."

[CLIC → item §8.7 « Traçabilité des non-conformités produits »]
[CLIC → zone de texte → dicter ou saisir :]
  "Zone de quarantaine non délimitée.
   Pièces non conformes mélangées aux pièces valides."

[CLIC → « Analyser » → attendre → « Ajouter au rapport »]


  ┌─────────────────────────────────────────────────────────────────┐
  │ [BONUS] SAISIE MANUSCRITE — 30 s si le rythme le permet          │
  └─────────────────────────────────────────────────────────────────┘

  [CLIC → bouton stylet, à droite du micro]

  [DIRE]
    "Sur tablette, Marc a un stylet.
     Il écrit son observation à la main —
     et Claude Vision la transcrit en texte structuré."

  [DESSINER quelques mots sur le canvas → CLIC « Transcrire »]

  [DIRE]
    "Le manuscrit rejoint la zone d'observation.
     Canvas natif, zéro dépendance ajoutée."


════════════════════════════════════════════════════════════════════
ACTE III — APRÈS L'AUDIT · LE RAPPORT                        T 6:00
════════════════════════════════════════════════════════════════════

[CLIC → « Rapport »]

[DIRE]
  "Acte trois : après.
   Le rapport est déjà là. Sur place. Pas ce soir — maintenant."

[POINTER le résumé de gauche]

[DIRE]
  "Deux non-conformités majeures. Plan d'actions, délais, responsables.
   La soirée de rédaction de Marc : elle vient de disparaître.
   Cinquième douleur, réglée."

[POINTER la grille de conformité]

[DIRE]
  "La grille de conformité par section ISO. Un score global, calculé.
   Les barres passent au rouge sous cinquante pour cent —
   ici, l'étalonnage est en alerte. Tout est mesuré, tout est sourcé."

[CLIC → « Modifier »]

[DIRE]
  "Marc relit, il ajuste une formulation.
   L'IA propose — l'auditeur décide. Il garde la main, toujours."

[MODIFIER une phrase → CLIC « Valider »]

[CLIC → toggle « Anonymiser »]

[DIRE]
  "Avant transmission : anonymisation RGPD en un clic.
   Les noms s'effacent de l'aperçu."

[CLIC → retoggle pour ré-afficher]

[CLIC → « Télécharger »]

[DIRE]
  "Export Word. Le .docx est généré par le backend, téléchargé directement."

[CLIC → section Signature → tracer la signature sur le canvas → « Confirmer »]

[DIRE]
  "Validation contradictoire. Karim signe à la main, sur la tablette.
   Le rapport est tracé, sourcé, signé.
   Marc quitte le site — le travail est terminé."


  ┌─────────────────────────────────────────────────────────────────┐
  │ [BONUS] PORTAIL FOURNISSEUR — 60 s si le jury accroche           │
  └─────────────────────────────────────────────────────────────────┘

  [CLIC → avatar haut droite → « Se connecter avec un autre compte »]

  [DIRE]
    "L'autre face de la plateforme. On passe côté RATP :
     Mei Lin Zhang, responsable qualité fournisseur."

  [CLIC → carte « Mei Lin Zhang » → « Se connecter » → portail]

  [DIRE]
    "Son portail. Les documents déposés avant l'audit —
     procédures, comptes-rendus, plans qualité,
     classés par catégorie, triés par date."

  [CLIC → un document → montrer l'analyse Claude : résumé + sections à risque]

  [DIRE]
    "Claude les a lus pour elle. Elle sait ce que l'auditeur va trouver
     avant même qu'il arrive.
     Et c'est exactement ce moteur qui a fait remonter
     les alertes dans la check-list de Marc. Une seule IA, deux usages."


════════════════════════════════════════════════════════════════════
CLÔTURE — REFERMER LA BOUCLE                                 T 7:20
════════════════════════════════════════════════════════════════════

[Revenir au slide de clôture, ou rester sur le rapport signé]

[DIRE — calme, on récapitule par la valeur, pas par la fonctionnalité]
  "Reprenons les cinq promesses du cahier des charges.

   Standardisation : chaque auditeur part avec la même check-list générée.
   Gain de temps : brief en trente secondes, rapport sur place.
   Respect des normes : chaque constat sourcé, l'article exact à l'écran.
   Capitalisation de l'expertise : chaque validation de Marc nourrit le système.
   Qualité, traçabilité, explicabilité : tout est daté, sourcé, signé."

[PAUSE 2 secondes]

[DIRE — revenir à l'image d'ouverture]
  "On a commencé à vingt et une heures, Marc devant son écran.
   Désormais, à seize heures, il a fini.
   L'application ne remplace pas l'auditeur.
   Elle lui rend son métier — et son terrain.

   Le quatre-vingt-dix / dix est inversé."

[PAUSE 2 secondes]

[DIRE]
  "Merci."


════════════════════════════════════════════════════════════════════
Q&R — RÉPONSES PRÉPARÉES
════════════════════════════════════════════════════════════════════

Q · « Concrètement, qui paie, et qui est audité ? »
R   Bureau Veritas achète la solution pour ses auditeurs.
    RATP et Apave sont les sites audités. Le portail fournisseur
    laisse le client déposer ses documents en amont ; Claude les pré-analyse.
    Le même moteur alimente la check-list de l'auditeur ET le tableau
    de bord côté client. Une plateforme, deux faces, un modèle de revenu B2B.

Q · « En quoi capitalisez-vous vraiment l'expertise des auditeurs ? »
R   Après chaque diagnostic, deux boutons : Confirmer ou Corriger.
    Chaque geste devient une donnée. L'expertise d'un senior comme Marc —
    aujourd'hui perdue à son départ — devient réutilisable par toute l'équipe.
    C'est l'une des cinq valeurs explicitement demandées par le CDC.

Q · « Quelle est la valeur de Claude face à une simple règle métier ? »
R   Une règle dit : « clause 7.1.5 → action générique ».
    Claude dit : « 7.1.5, atelier Sucy, NC de novembre 2024,
    responsable identifié → étalonnage COFRAC sous 72 h, alertes planifiées ».
    C'est le contexte qui fait la différence — et le contexte ne se code pas en dur.

Q · « Votre IA est-elle explicable ? »
R   Entièrement. Chaque diagnostic renvoie à sa clause ISO, et l'extrait
    verbatim de la norme est accessible au survol, dans l'interface.
    On voit exactement quel passage a guidé l'analyse. C'est une exigence
    réglementaire du CDC, et c'est natif chez nous, pas un ajout cosmétique.

Q · « Et si l'API Claude tombe pendant un audit ? »
R   Bascule automatique sur un moteur local : RAG ISO 9001 + règles métier.
    Un badge indique « IA locale » ou « Claude Opus ». La démo tourne sans réseau.

Q · « Quelle norme couvrez-vous ? Et après ? »
R   ISO 9001:2015, clauses 4 à 10, indexées par RAG. L'architecture est
    extensible — ISO 14001, 45001, EN 9100. Le sélecteur de référentiel
    est déjà présent dans le Brief.

Q · « Pourquoi GEP plutôt que l'API Anthropic en direct ? »
R   GEP est la plateforme IA interne de Capgemini : déjà déployée,
    clés centralisées, conformité entreprise. Modèle servi : Claude Opus.

Q · « La saisie manuscrite, ça tient la route ? »
R   Claude Vision reçoit le PNG du canvas et transcrit. Sur tablette à stylet,
    c'est précis. En fallback GEP, une observation par défaut prend le relais —
    la démo reste fluide dans les deux cas.

Q · « Sur quels critères la check-list est-elle générée ? »
R   Cinq sources : référentiel ISO, scope des clauses (7.1.5 et 8.7 pour Sucy),
    contexte du site (atelier, 218 personnes), historique des NC,
    et documents fournisseur pré-analysés. S2 et S3 viennent du scope ;
    S1 et S4 sont génériques.

Q · « D'où sortent les articles ISO affichés ? »
R   Corpus ISO 9001:2015, indexé par sentence-transformers (BERT multilingue),
    préchargé au démarrage. Sélection déterministe par clause.
    Le survol affiche le texte verbatim — citation de source en un coup d'œil.


════════════════════════════════════════════════════════════════════
CHECK-LIST AVANT DE MONTER SUR SCÈNE
════════════════════════════════════════════════════════════════════

  [ ] start.bat lancé 3 min avant → backend prêt
  [ ] localhost:8000/docs accessible (API vivante)
  [ ] localhost:3000 accessible (frontend)
  [ ] Log « Application startup complete » visible (RAG chargé)
  [ ] .env : GEP_API_KEY présent + GEP_MODEL=anthropic.claude-opus-4-7
  [ ] Chrome ouvert (Web Speech API requise pour le micro)
  [ ] Micro système activé + testé
  [ ] Image de test prête pour la photo (n'importe quel JPG)
  [ ] Écran 16:9, zoom navigateur à 100 %
  [ ] Mission « PROCHAIN » bien visible (statuts dynamiques selon l'heure)
  [ ] Sucy-en-Brie : flag alwaysDemarrer → bouton « Démarrer » garanti
  [ ] Pop-overs ⓘ testés (check-list, questions, RAG) — s'ouvrent/se ferment
  [ ] Signature : canvas manuscrit réactif au pointeur

  RÈGLE D'OR : ne jamais parler pendant un chargement Claude.
  Le silence vend l'instantanéité du résultat.


════════════════════════════════════════════════════════════════════
NOTES DE MISE EN SCÈNE (réécriture v3 — Opus 4.8)
════════════════════════════════════════════════════════════════════

  • Structure en trois actes : Avant / Pendant / Après — calquée
    sur le cycle d'audit ET sur l'arc narratif.
  • Accroche « scène » (21 h, Marc devant son écran) au lieu d'une
    liste de problèmes ; la boucle 90/10 ouvre et referme le pitch.
  • Les 5 douleurs du CDC sont nommées au fil du parcours, à l'instant
    où l'app les résout — pas en bloc au début.
  • La capitalisation de l'expertise (boutons Confirmer/Corriger) est
    posée comme « cœur stratégique », pas comme détail UI.
  • Explicabilité valorisée comme réponse à une exigence réglementaire.
  • Modèle économique (BV achète · RATP/Apave audités) explicité 2 fois.
  • Beats de silence balisés sur chaque moment « wow ».
  • Bonus encadrés (manuscrit, portail) — sacrifiables sans casser le fil.
  • Port :3000 · signature manuscrite seule · français accentué.
════════════════════════════════════════════════════════════════════
