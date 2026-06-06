════════════════════════════════════════════════════════════════════
  SCRIPT DE DEMO — UC 28 INSPECTION AUGMENTEE
  Hackathon Capgemini x Anthropic 2026 — Code Resonance
  Duree cible : 8 minutes
════════════════════════════════════════════════════════════════════

CONVENTIONS
  [CLIC]     = action souris a faire
  [DIRE]     = texte a prononcer
  [PAUSE]    = marquer un silence, laisser le jury regarder
  [POINTER]  = montrer a l'ecran sans cliquer
  [COULISSES]= note technique pour le presentateur, ne pas lire
  [BONUS]    = sequence optionnelle si le temps le permet
  T          = temps cumule indicatif

CONTEXTE OFFICIEL UC 28 (rappel jury)
  BU           : SPS — Services Marchands
  Clients cible: Apave / Bureau Veritas / Site de maintenance RATP
  Referents    : Philippe Larroye (AI Office + GTM)
                 JC Formosa (Apave), Jerome Fouquet (BV),
                 Marvin Dufresne (RATP)
  Equipe       : Code Resonance
                 Habib KOFFI · Veronique POILANE ZHANG · Philippe GRAGNIC

════════════════════════════════════════════════════════════════════
SLIDE 1 — TITRE                                              T 0:00
════════════════════════════════════════════════════════════════════

[DIRE]
  "Cinq problemes. Un seul produit.

   Les audits sont complexes, normes, et necessitent une forte expertise.
   La preparation est longue — et heterogene selon les inspecteurs.
   L'historique des non-conformites est difficile a exploiter sur le terrain.
   Les constats sont trop souvent subjectifs ou incomplets.
   Les rapports d'audit consomment des heures de redaction administrative.

   L'inspection Augmentee resout les cinq.
   Avant l'audit. Pendant. Et apres."

[PAUSE 3 secondes]

[DIRE]
  "Je suis Philippe, avec Habib et Veronique — equipe Code Resonance.
   Bureau Veritas achete la solution pour ses auditeurs.
   RATP et Apave sont les clients finaux.
   Vous allez voir la journee de Marc Lefevre, auditeur Bureau Veritas.
   8 minutes. Un seul parcours. Ca commence maintenant."

[CLIC → slide suivante]

════════════════════════════════════════════════════════════════════
SLIDE 2 — LE PERSONNAGE                                      T 0:50
════════════════════════════════════════════════════════════════════

[DIRE]
  "Marc. 23 ans de metier chez Bureau Veritas.
   Expert terrain ISO 9001 — il audite des sites Apave et RATP.

   Son probleme : 90 % de son temps sur la paperasse.
   10 % sur le terrain.
   Et chaque auditeur prepare son audit differemment —
   le niveau d'expertise ne se capitalise pas, ne se transmet pas.

   Notre application ne remplace pas Marc.
   Elle lui rend son terrain.
   Et elle capture son expertise pour la diffuser a toute l'equipe BV."

[PAUSE 2 secondes]

[CLIC → basculer sur le navigateur, onglet localhost:3000]

════════════════════════════════════════════════════════════════════
ECRAN 0.1 — CONNEXION                                        T 1:20
════════════════════════════════════════════════════════════════════

[CLIC → localhost:3000]

[DIRE]
  "Deux personas dans l'application.
   Marc Lefevre, l'auditeur Bureau Veritas sur le terrain.
   Et Mei Lin Zhang, la responsable qualite cote RATP —
   elle accede au portail fournisseur.
   Pour cette demo : on est Marc."

[CLIC → carte "Marc Lefevre — Auditeur BV"]

[DIRE]
  "Son identite est deja la — aucune saisie."

[CLIC → bouton "Se connecter"]

════════════════════════════════════════════════════════════════════
ECRAN 0.2 — SELECTION CLIENT                                 T 1:35
════════════════════════════════════════════════════════════════════

[DIRE]
  "Aujourd'hui il intervient pour RATP — 12 missions ce mois."

[CLIC → carte RATP]

════════════════════════════════════════════════════════════════════
ECRAN 1 — DASHBOARD                                          T 1:45
════════════════════════════════════════════════════════════════════

[DIRE]
  "Sa journee d'un coup d'oeil.
   4 audits. Le trait rouge — c'est maintenant.
   Il sait ou il en est sans ouvrir Outlook."

[CLIC → sur une carte de mission → montrer le flyTo anime sur la carte]

[DIRE]
  "La carte se recentre sur le site. Itineraire routier reel —
   calcule via OSRM. Pas de cle API, fonctionne hors ligne."

[CLIC → toggle "Planning"]

[DIRE]
  "Vue planning. Les blocs horaires. Le temps de trajet inclus —
   35 minutes en voiture pour Sucy-en-Brie."

[CLIC → toggle "Liste" → filtre "PROCHAIN"]

[DIRE]
  "Son prochain audit : Atelier Sucy-en-Brie, maintenance rames MI09, RER A.
   Il clique Demarrer."

[CLIC → bouton "Demarrer" sur la carte Sucy-en-Brie]

════════════════════════════════════════════════════════════════════
ECRAN 2 — BRIEF  (AVANT l'audit)                             T 2:25
════════════════════════════════════════════════════════════════════

[DIRE]
  "Phase 1 : AVANT. Marc n'a pas encore quitte son bureau.
   Mais son brief est deja complet.
   218 personnes. Responsable qualite : Karim Belkacem.
   Perimetre : maintenance des rames MI09, RER A."

[PAUSE — pointer l'alerte rouge dans l'historique]

[DIRE]
  "Et la — l'alerte.
   Une non-conformite mineure ouverte depuis novembre 2024.
   Non cloturee.
   Marc n'a pas a s'en souvenir. L'IA s'en souvient pour lui.
   C'est exactement ca, exploiter l'historique des audits."

[POINTER la checklist IA]

[DIRE]
  "La checklist est pre-generee par l'Agent IA.
   Elle est structuree par clause ISO —
   §7.1.5 Etalonnage, §8.7 Non-conformites, directement dans le scope.
   Pas un template generique : cette checklist est propre a Sucy-en-Brie."

  ┌─ COULISSES — Comment la checklist est generee ──────────────────┐
  │ L'Agent IA combine 5 sources pour construire cette checklist :  │
  │                                                                  │
  │  1. REFERENTIEL CHOISI    ISO 9001:2015 (selectionnable)        │
  │  2. SCOPE DE L'AUDIT      §7.1.5 Metrologie · §8.7 NC          │
  │                           -> S2 et S3 issues du scope mission   │
  │  3. CONTEXTE DU SITE      Atelier maintenance, 218 personnes,   │
  │                           duree 2h30 -> sections calibrees      │
  │  4. HISTORIQUE NC         NC mineure §7.1.5 non cloturee (2024) │
  │                           -> S2 marque priorite haute           │
  │  5. DOCUMENTS FOURNISSEUR 3 docs Mei Lin Zhang pre-analyses     │
  │                           -> alertes RATP sur §7.1.5 et §8.7   │
  │                                                                  │
  │ S1 (§7.5) = documentaire generique                              │
  │ S2 (§7.1.5) = etalonnage, directement dans le scope            │
  │ S3 (§8.7) = gestion NC, directement dans le scope + recidive   │
  │ S4 (§7.2) = competences, lie a l'effectif de 218 personnes     │
  └──────────────────────────────────────────────────────────────────┘

[DIRE]
  "Marc peut ajuster. Il supprime les points non pertinents pour ce site —
   un clic sur le x au survol de chaque item."

[CLIC → survoler un item → montrer le x → supprimer un item]

[DIRE]
  "Il peut aussi ajouter ses propres points d'audit."

[CLIC → bouton "+ Ajouter un point" → taper un point custom → Entree]

[DIRE]
  "Son point apparait avec le badge 'Auditeur'.
   Il sera dans la checklist de l'inspection.
   Chaque auditeur amene son expertise — elle est capturee."

[PAUSE — pointer les badges alerte RATP sur §7.1.5 et §8.7]

[DIRE]
  "Les alertes RATP. Les documents deposes par Mei Lin Zhang
   ont ete pre-analyses par Claude — les clauses a risque
   sont signalees directement dans sa checklist.
   C'est la plateforme biface : BV d'un cote, le fournisseur de l'autre.
   Le meme algorithme croise les deux."

[CLIC → bouton "Demarrer l'inspection"]

════════════════════════════════════════════════════════════════════
ECRANS 3/4 — INSPECTION  (PENDANT l'audit)                   T 3:35
════════════════════════════════════════════════════════════════════

[DIRE]
  "Phase 2 : PENDANT. Marc est sur le terrain.
   Gants aux mains. Devant les equipements.
   Il selectionne le point qu'il va controler."

[CLIC → item §7.1.5 "Verification des certificats d'etalonnage"]

[PAUSE — attendre la generation des suggestions et questions]

[DIRE]
  "Trois choses se passent simultanement :
   Claude genere 3 exemples d'observations terrain pour ce point,
   3 questions de verification oui/non apparaissent a droite,
   et les articles ISO de reference se mettent a jour.

   Ce n'est pas un template generique.
   C'est §7.1.5, etalonnage, atelier Sucy, historique NC inclus."

  ┌─ COULISSES — Comment les questions suggerees sont generees ──────┐
  │ Declencheur : clic sur un item de la checklist                   │
  │ Appel reel  : POST /questions_oui_non (Claude via GEP)           │
  │                                                                   │
  │ Contexte envoye a Claude :                                        │
  │   - Texte de l'item   "Verification des certificats d'etalonnage"│
  │   - Clause ISO        §7.1.5                                      │
  │   - Titre de section  "Etalonnage & equipements de mesure"       │
  │                                                                   │
  │ Claude genere 3 questions de verification terrain contextualisees,│
  │ pas des templates :                                               │
  │   "Les certificats sont-ils dates de moins de 12 mois ?"        │
  │   "L'equipement est-il couvert par un labo accredite COFRAC ?"  │
  │   "Le registre des equipements est-il a jour ?"                  │
  │                                                                   │
  │ Les reponses Oui/Non sont injectees dans le prompt d'analyse     │
  │ -> enrichissent le diagnostic Claude.                             │
  │                                                                   │
  │ Fallback : 3 questions statiques si GEP indisponible.            │
  └───────────────────────────────────────────────────────────────────┘

[CLIC → repondre Oui/Non sur 2 questions]

[DIRE]
  "Il qualifie rapidement. Ces reponses partent comme contexte
   dans le prompt d'analyse — Claude sait ce que Marc a observe."

[CLIC → bouton micro]

[DIRE a voix haute, clairement — le micro est actif :]
  "Les cles dynamometriques du poste 12 ne sont pas etalonnees,
   certificats perimes depuis 8 mois."

[CLIC → bouton micro pour arreter]

[DIRE]
  "Le texte est transcrit. Il prend une photo."

[CLIC → "Prendre une photo" → selectionner une image quelconque]

[DIRE]
  "Et maintenant — il clique Analyser."

[CLIC → bouton "Analyser"]

[PAUSE — laisser la reponse arriver — NE PAS PARLER]
[PAUSE encore 2 secondes apres l'affichage — LAISSER RESPIRER]

[DIRE]
  "NC MAJEURE. §7.1.5 — Etalonnage et surveillance.
   Action corrective : etalonnage COFRAC immediat.
   Action preventive : planning automatise."

[POINTER le badge violet "Claude Opus"]

[DIRE]
  "C'est Claude Opus, via la plateforme GEP Capgemini.
   Pas un template. Pas une regle fixe.
   Un diagnostic contextualisé : les rames MI09, l'atelier Sucy,
   Karim Belkacem, l'historique NC — tout ca est dans la reponse.
   L'IA est explicable : chaque diagnostic est source et tracable."

[POINTER l'alerte recidive si visible]

[DIRE]
  "Recidive. Ce point etait deja en NC en novembre 2024.
   Marc le voit immediatement."

[POINTER les boutons 'Confirmer' / 'Corriger' sous le resultat]

[DIRE]
  "Marc confirme ou corrige le diagnostic.
   Chaque correction est enregistree.
   C'est comme ca qu'on capitalise l'expertise BV :
   l'experience de Marc, de ses 23 ans de terrain,
   devient une donnee d'entrainement pour l'equipe."

[POINTER les articles RAG en bas de la colonne centrale]

  ┌─ COULISSES — Comment les articles normatifs sont selectionnes ───┐
  │ Declencheur : clic sur un item de la checklist                   │
  │                                                                   │
  │ Logique de selection basee sur la clause :                        │
  │   clause "7.1.5" -> articles §7.1.5 Metrologie                  │
  │   clause "8.7"   -> articles §8.7 Non-conformites               │
  │   autre clause   -> articles par defaut (§9.2, §10.3, §6.1)     │
  │                                                                   │
  │ Base : corpus ISO 9001:2015 indexe par sentence-transformers      │
  │ (BERT multilingue, precharge au demarrage du backend).            │
  │                                                                   │
  │ Chaque article affiche en tooltip au survol :                     │
  │   - Numero de clause complet                                      │
  │   - Titre normatif officiel                                       │
  │   - Extrait verbatim de la norme                                  │
  │                                                                   │
  │ Marc peut citer la source exacte sans quitter l'application.      │
  └───────────────────────────────────────────────────────────────────┘

[DIRE]
  "En bas : les articles de la norme ISO 9001 references.
   Un survol affiche l'extrait complet.
   Marc peut citer la source exacte sans quitter l'ecran.
   C'est l'IA explicable et conforme aux exigences reglementaires."

[CLIC → "Ajouter au rapport"]

[DIRE]
  "Deuxieme constat — rapide."

[CLIC → item §8.7 "Tracabilite des non-conformites produits"]

[CLIC → zone de texte → taper ou dicter :]
  "Zone quarantaine non delimitee, pieces non conformes
   melangees avec pieces valides."

[CLIC → Analyser → attendre → Ajouter au rapport]

════════════════════════════════════════════════════════════════════
  [BONUS — saisie manuscrite, 30 secondes si le temps le permet]
════════════════════════════════════════════════════════════════════

[CLIC → bouton stylet (PenLine, a droite du micro)]

[DIRE]
  "Marc a un stylet sur sa tablette.
   Il peut ecrire son observation a la main —
   Claude vision transcrit le manuscrit en texte structure."

[DESSINER quelques mots sur le canvas]

[CLIC → "Transcrire"]

[DIRE]
  "Le texte manuscrit rejoint la zone d'observation.
   Canvas HTML5 natif — aucune dependance supplementaire."

════════════════════════════════════════════════════════════════════
ECRAN 5 — RAPPORT  (APRES l'audit)                           T 6:00
════════════════════════════════════════════════════════════════════

[CLIC → bouton "Rapport"]

[DIRE]
  "Phase 3 : APRES. Le rapport est genere. Immediatement. Sur place."

[POINTER le resume gauche]

[DIRE]
  "2 NC majeures. Plan d'actions avec delais. Responsables identifies.
   Ce que Marc aurait passe sa soiree a rediger — c'est deja la."

[POINTER la grille de conformite]

[DIRE]
  "La grille de conformite par section ISO.
   Un score global calcule sur tous les constats.
   Les barres virent au rouge sous 50 % — ici §7.1.5 est en alerte.
   C'est la tracabilite et l'explicabilite en action."

[CLIC → bouton "Modifier"]

[DIRE]
  "Marc relit. Il ajuste une formulation.
   L'auditeur reste en controle — l'IA assiste, elle ne decide pas."

[MODIFIER une phrase dans un textarea]

[CLIC → "Valider"]

[CLIC → toggle "Anonymiser"]

[DIRE]
  "Pour la transmission : anonymisation RGPD en un clic.
   Les noms disparaissent de l'apercu."

[CLIC → retoggle pour desanonymiser]

[CLIC → "Telecharger"]

[DIRE]
  "Export Word. Le fichier .docx est genere par le backend FastAPI."

[CLIC → section Signature → canvas → tracer une signature → Confirmer]

[DIRE]
  "Validation contradictoire. Karim signe a la main sur la tablette —
   signature manuscrite directement dans l'interface.
   Le rapport est trace, source, valide.
   Marc sort du site avec le travail termine."

════════════════════════════════════════════════════════════════════
  [BONUS — parcours Mei Lin Zhang, 60 secondes si jury interesse]
════════════════════════════════════════════════════════════════════

[CLIC → avatar en haut a droite → "Se connecter avec un autre compte"]

[DIRE]
  "On bascule cote RATP.
   Mei Lin Zhang, responsable qualite fournisseur.
   C'est l'autre face de la plateforme :
   BV achete la solution, RATP et Apave en sont les beneficiaires."

[CLIC → carte "Mei Lin Zhang — Responsable Qualite RATP"]
[CLIC → Se connecter → portail fournisseur s'affiche]

[DIRE]
  "Son portail. Les documents deposes avant l'audit.
   Procedures, comptes-rendus, plans qualite —
   classes par categorie, tries par date."

[CLIC → un document → montrer l'analyse Claude : resume, sections a risque]

[DIRE]
  "Claude a lu ces documents pour elle.
   Elle sait ce que l'auditeur va trouver
   avant meme qu'il arrive sur site.
   Et c'est le meme algorithme qui a signale les alertes
   dans la checklist de Marc."

════════════════════════════════════════════════════════════════════
CLOTURE                                                      T 7:30
════════════════════════════════════════════════════════════════════

[DIRE]
  "Ce que vous venez de voir couvre les cinq valeurs attendues du CDC :

   1. STANDARDISATION
      Chaque auditeur BV part avec la meme checklist generee par l'IA.
      Fini l'heterogeneite entre inspecteurs.

   2. GAIN DE TEMPS
      Brief en 30 secondes. Rapport sur place.
      Le ratio 90/10 est inverse.

   3. RESPECT DES NORMES
      ISO 9001:2015 indexe par RAG — les articles verbatim au survol.
      L'auditeur cite la source exacte depuis l'ecran.

   4. CAPITALISATION EXPERTISE BV
      Les confirmations et corrections de Marc sont la donnee.
      23 ans de terrain — captures, transmis, reutilises.

   5. QUALITE, TRACABILITE, EXPLICABILITE
      Chaque diagnostic est source par clause ISO, horodate,
      signe contradictoirement sur la tablette.
      L'IA assiste. L'humain decide et valide."

[PAUSE 2 secondes]

[DIRE]
  "Merci."

════════════════════════════════════════════════════════════════════
QUESTIONS PROBABLES — REPONSES PREPAREES
════════════════════════════════════════════════════════════════════

Q : "Quels sont vos trois clients et comment le modele fonctionne ?"
R : "Bureau Veritas achete la solution pour ses auditeurs.
     RATP et Apave sont les clients finaux audites.
     Le portail fournisseur cote RATP/Apave permet de deposer
     des documents avant l'audit — Claude les pre-analyse.
     Le meme algorithme alimente la checklist de l'auditeur BV
     et le tableau de bord du responsable qualite cote client."

Q : "Comment vous capitalisez l'expertise BV ?"
R : "Apres chaque diagnostic Claude, l'auditeur a deux boutons :
     Confirmer (expertise validee) ou Corriger (avec sa correction).
     Chaque correction est une donnee d'apprentissage —
     l'expertise de Marc, 23 ans de terrain, devient reutilisable
     par toute l'equipe BV. C'est l'un des 5 objectifs officiels du CDC."

Q : "Et si l'API Claude est indisponible ?"
R : "Fallback automatique sur un moteur local —
     RAG ISO 9001 + regles metier.
     Le badge dans l'interface indique 'IA locale' vs 'Claude Opus'.
     La demo fonctionne sans reseau GEP."

Q : "Quelle norme couvrez-vous ?"
R : "ISO 9001:2015. La base RAG couvre les clauses 4 a 10.
     L'architecture est extensible a ISO 14001, 45001, EN 9100 —
     le selecteur de referentiel est deja dans le Brief."

Q : "C'est quoi la vraie valeur ajoutee de Claude vs une regle fixe ?"
R : "La regle fixe donne 'clause 7.1.5 -> action generique'.
     Claude donne 'clause 7.1.5, atelier Sucy-en-Brie,
     historique NC nov. 2024, responsable Karim Belkacem
     -> etalonnage COFRAC dans les 72h, alertes automatisees'.
     C'est le contexte qui fait la difference."

Q : "L'IA est-elle explicable ?"
R : "Completement. Chaque diagnostic est source par clause ISO,
     les articles verbatim sont accessibles au survol dans l'interface.
     Le RAG affiche exactement quel extrait de norme a guide l'analyse.
     L'auditeur peut citer la source sans quitter l'ecran —
     c'est une exigence explicite du CDC, on y repond."

Q : "Pourquoi GEP Capgemini plutot que l'API Anthropic directe ?"
R : "GEP est la plateforme IA interne Capgemini — deja en place,
     gestion des cles centralisee, conformite entreprise.
     Le modele utilise est anthropic.claude-opus-4-7."

Q : "La saisie manuscrite, ca marche vraiment ?"
R : "Claude vision recoit l'image PNG du canvas et transcrit le texte.
     Sur tablette avec stylet, la precision est excellente.
     En fallback (GEP indisponible) : observation predecfinie par item
     — le scenario de demo reste fluide dans les deux cas."

Q : "Sur quels criteres la checklist est-elle generee ?"
R : "5 sources : le referentiel ISO selectionne, le scope des clauses
     de la mission (§7.1.5 et §8.7 pour Sucy-en-Brie), le contexte
     du site (type d'atelier, effectif 218 personnes), l'historique
     des NC precedentes, et les documents deposes par le fournisseur
     pre-analyses par Claude. Les sections S2 et S3 sont directement
     issues du scope — S1 et S4 sont generiques."

Q : "Comment les questions oui/non sont-elles generees ?"
R : "Appel POST /questions_oui_non a chaque selection d'item.
     Claude genere 3 questions de verification contextualisees
     par le texte de l'item, la clause ISO et le titre de section.
     Ces reponses sont ensuite injectees dans le prompt d'analyse
     pour enrichir le diagnostic."

Q : "D'ou viennent les articles ISO affiches ?"
R : "Base documentaire ISO 9001:2015 indexee par sentence-transformers
     (BERT multilingue), prechargee au demarrage du backend.
     La selection est deterministe : clause §7.1.5 -> articles metrologie,
     §8.7 -> articles non-conformites. Le tooltip au survol affiche
     le texte verbatim de la norme — Marc peut citer la source exacte."

════════════════════════════════════════════════════════════════════
CHECKLIST AVANT DE MONTER SUR SCENE
════════════════════════════════════════════════════════════════════

  [ ] start.bat lance 3 min avant -> backend demarre
  [ ] localhost:8000/docs accessible (test API)
  [ ] localhost:3000 accessible (frontend)
  [ ] Log "Application startup complete" visible (RAG charge)
  [ ] .env verifie : GEP_API_KEY + GEP_MODEL=anthropic.claude-opus-4-7
  [ ] Chrome ouvert (Web Speech API pour le micro)
  [ ] Son active sur la machine (micro systeme)
  [ ] Image de test prete pour la photo (n'importe quel JPG)
  [ ] Ecran en 16:9, zoom navigateur a 100 %
  [ ] Verifier que le "PROCHAIN" audit est bien affiche
      (les statuts sont dynamiques — bases sur l'heure courante)
  [ ] La mission Sucy-en-Brie a le flag alwaysDemarrer
      -> bouton Demarrer toujours visible independamment de l'heure

════════════════════════════════════════════════════════════════════
CHANGEMENTS DEPUIS LA VERSION PRECEDENTE DU SCRIPT
════════════════════════════════════════════════════════════════════

  • Narrative "avant / pendant / apres" issue du CDC officiel
  • 3 clients explicites : Apave + Bureau Veritas + RATP (Apave ajoute)
  • 5 problemes metier officiels en ouverture (Slide 1)
  • 5 valeurs attendues officielles en cloture (alignees CDC)
  • Capitalisation expertise BV : boutons Confirmer/Corriger mis en valeur
  • IA explicable et tracable : angle valorise (articles verbatim + sources)
  • Plateforme biface BV <-> fournisseur : narratif clarifie
  • Signature : uniquement manuscrite (canvas), champ texte supprime
  • Port frontend : localhost:5173 -> localhost:3000
  • 2 nouvelles Q&R : modele 3 clients, capitalisation expertise BV
  • Popovers i sur Check-list auto-generee, Questions suggerees, RAG
  • Boutons x suppression sections et items dans la checklist Brief
