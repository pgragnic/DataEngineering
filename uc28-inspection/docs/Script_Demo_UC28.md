════════════════════════════════════════════════════════════════════
  SCRIPT DE DÉMO — UC 28 INSPECTION AUGMENTÉE
  Hackathon Capgemini × Anthropic 2026 — Code Resonance
  Durée cible : 8 minutes
════════════════════════════════════════════════════════════════════

CONVENTIONS
  [CLIC]     = action souris à faire
  [DIRE]     = texte à prononcer
  [PAUSE]    = marquer un silence, laisser le jury regarder
  ⏱          = temps cumulé indicatif

════════════════════════════════════════════════════════════════════
SLIDE 1 — TITRE                                              ⏱ 0:00
════════════════════════════════════════════════════════════════════

[DIRE]
  "Chaque année, des milliers d'auditeurs qualité passent
   90 % de leur temps sur de la paperasse — et 10 % sur le terrain.
   On a inversé ce ratio.
   Je vous présente l'Inspection Augmentée."

[PAUSE 3 secondes]

[DIRE]
  "Je suis Philippe, avec Habib et Véronique — équipe Code Resonance.
   On va vous montrer la journée de Marc Lefèvre, auditeur RATP.
   8 minutes. Un seul parcours. Ça commence maintenant."

[CLIC → slide suivante]

════════════════════════════════════════════════════════════════════
SLIDE 2 — LE PERSONNAGE                                      ⏱ 0:45
════════════════════════════════════════════════════════════════════

[DIRE]
  "Marc. 23 ans de métier. Expert terrain.
   Mais aujourd'hui, il passe ses soirées à rédiger des rapports.
   À chercher la bonne clause ISO dans ses notes.
   À recroiser les constatations du dernier audit.

   Ce 90/10 — c'est son quotidien. C'est celui de tous ses collègues.
   Notre application ne remplace pas Marc.
   Elle lui rend son terrain."

[PAUSE 2 secondes]

[CLIC → basculer sur le navigateur, onglet localhost:5173]

════════════════════════════════════════════════════════════════════
ÉCRAN 0.1 — CONNEXION                                        ⏱ 1:15
════════════════════════════════════════════════════════════════════

[CLIC → localhost:5173]

[DIRE]
  "Deux personas dans notre application.
   Marc Lefèvre, l'auditeur Bureau Veritas sur le terrain.
   Et Mei Lin Zhang, la responsable qualité côté RATP —
   elle accède au portail fournisseur.
   Pour cette démo : on est Marc."

[CLIC → carte "Marc Lefèvre — Auditeur BV"]

[DIRE]
  "Son identité est déjà là — aucune saisie."

[CLIC → bouton "Se connecter"]

════════════════════════════════════════════════════════════════════
ÉCRAN 0.2 — SÉLECTION CLIENT                                 ⏱ 1:30
════════════════════════════════════════════════════════════════════

[DIRE]
  "Il intervient pour RATP aujourd'hui — 12 missions ce mois."

[CLIC → carte RATP]

════════════════════════════════════════════════════════════════════
ÉCRAN 1 — DASHBOARD                                          ⏱ 1:40
════════════════════════════════════════════════════════════════════

[DIRE]
  "Sa journée d'un coup d'œil.
   4 audits. Le trait rouge — c'est maintenant.
   Il sait où il en est sans ouvrir Outlook."

[CLIC → sur une carte de mission → montrer le flyTo animé sur la carte]

[DIRE]
  "La carte se recentre sur le site. Itinéraire routier réel —
   calculé via OSRM. Pas de clé API, fonctionne hors ligne."

[CLIC → toggle "Planning"]

[DIRE]
  "Vue planning. Les blocs horaires. Le temps de trajet inclus —
   35 minutes en voiture pour Sucy-en-Brie."

[CLIC → toggle "Liste" → filtre "PROCHAIN"]

[DIRE]
  "Son prochain audit : Atelier Sucy-en-Brie, 14h30.
   Il clique Démarrer."

[CLIC → bouton "Démarrer" sur la carte Sucy-en-Brie]

════════════════════════════════════════════════════════════════════
ÉCRAN 2 — BRIEF                                              ⏱ 2:20
════════════════════════════════════════════════════════════════════

[DIRE]
  "Avant même d'arriver sur site, Marc a son brief complet.
   218 personnes. Responsable qualité : Karim Belkacem.
   Périmètre : maintenance des rames MI09, RER A."

[PAUSE — pointer l'alerte rouge dans l'historique]

[DIRE]
  "Et là — l'alerte.
   Une non-conformité mineure ouverte depuis novembre 2024.
   Non clôturée.
   Marc n'a pas à se souvenir de ça. L'IA s'en souvient pour lui."

[DIRE]
  "La checklist est structurée par clause ISO.
   Il peut supprimer les points non pertinents pour ce site
   — un clic sur le × au survol de chaque item."

[CLIC → survoler un item de la checklist → montrer le × → supprimer un item]

[DIRE]
  "Il peut aussi ajouter ses propres points d'audit."

[CLIC → bouton "+ Ajouter un point" → taper un point custom → Entrée]

[DIRE]
  "Son point apparaît avec le badge 'Auditeur'.
   Il sera dans la checklist de l'inspection."

[PAUSE — pointer les badges ⚠ RATP sur §7.1.5 et §8.7]

[DIRE]
  "Les alertes RATP. Les documents déposés côté fournisseur
   ont été pré-analysés par Claude — les clauses à risque
   sont signalées directement dans sa checklist."

[CLIC → bouton "Démarrer l'inspection"]

════════════════════════════════════════════════════════════════════
ÉCRANS 3/4 — INSPECTION                                      ⏱ 3:30
════════════════════════════════════════════════════════════════════

[DIRE]
  "Marc est sur le terrain. Gants aux mains. Devant les équipements.
   Il sélectionne le point qu'il va contrôler."

[CLIC → item §7.1.5 "Vérification des certificats d'étalonnage" dans la checklist]

[PAUSE — attendre la génération des suggestions et questions]

[DIRE]
  "Trois choses se passent simultanément :
   Claude génère 3 exemples d'observations terrain pour ce point,
   3 questions de vérification oui/non apparaissent à droite,
   et les articles ISO de référence se mettent à jour."

[POINTER la colonne du milieu — suggestions]

[DIRE]
  "Ces suggestions sont contextualisées par clause.
   Pas un template générique : §7.1.5, étalonnage, atelier Sucy."

[POINTER la colonne réponses]

[DIRE]
  "Il peut qualifier rapidement : conforme, non conforme, non applicable.
   Ces réponses sont transmises à Claude comme contexte d'analyse."

[CLIC → répondre Oui/Non sur 2 questions]

[DIRE]
  "Maintenant il dicte son constat."

[CLIC → bouton micro 🎤]

[DIRE à voix haute, clairement — le micro est actif :]
  "Les clés dynamométriques du poste 12 ne sont pas étalonnées,
   certificats périmés depuis 8 mois."

[CLIC → bouton micro pour arrêter]

[DIRE]
  "Le texte est transcrit. Il prend une photo."

[CLIC → "Prendre une photo" → sélectionner une image quelconque]

[DIRE]
  "Et maintenant — il clique Analyser."

[CLIC → bouton "Analyser"]

[PAUSE — laisser la réponse arriver — NE PAS PARLER]
[PAUSE encore 2 secondes après l'affichage — LAISSER RESPIRER]

[DIRE]
  "NC MAJEURE. Le petit rond rouge — criticité maximale.
   Clause §7.1.5 — Étalonnage et surveillance.
   Et regardez le badge : Claude Opus."

[POINTER le badge violet "Claude Opus" et le rond rouge]

[DIRE]
  "C'est Claude Opus, via la plateforme GEP Capgemini,
   qui a analysé ce constat en temps réel.
   Pas un template. Pas une règle fixe.
   Un diagnostic contextualisé : les rames MI09, l'atelier Sucy-en-Brie,
   Karim Belkacem comme responsable — tout ça est dans la réponse."

[POINTER l'alerte récidive 🔁 si visible]

[DIRE]
  "Et là — récidive.
   Ce point a déjà été en non-conformité en novembre 2024.
   Marc le voit immédiatement. Il peut appuyer dessus dans son rapport."

[POINTER l'animation moment fort si visible]

[DIRE]
  "L'action corrective : étalonnage COFRAC immédiat.
   L'action préventive : planning automatisé, responsable métrologie dédié.
   Marc n'a rien eu à chercher."

[POINTER les articles RAG en bas de la colonne centrale]

[DIRE]
  "En bas : les articles de la norme ISO 9001 référencés.
   Un survol affiche l'extrait complet — Marc peut vérifier
   la source sans quitter l'écran."

[CLIC → "Ajouter au rapport"]

[DIRE]
  "Deuxième constat — rapide."]

[CLIC → item §8.7 "Traçabilité des non-conformités produits" dans la checklist]

[CLIC → zone de texte → taper ou dicter :]
  "Zone quarantaine non délimitée, pièces non conformes
   mélangées avec pièces valides."

[CLIC → Analyser → attendre → Ajouter au rapport]

════════════════════════════════════════════════════════════════════
  [OPTIONNEL — si le temps le permet, 30 secondes]
════════════════════════════════════════════════════════════════════

[CLIC → bouton ✍️ (PenLine, à droite du micro)]

[DIRE]
  "Marc a un stylet sur sa tablette.
   Il peut écrire son observation à la main —
   Claude vision transcrit le manuscrit en texte structuré."

[DESSINER quelques mots sur le canvas]

[CLIC → "✨ Transcrire"]

[DIRE]
  "Le texte manuscrit rejoint la zone d'observation.
   Aucune dépendance logicielle supplémentaire — canvas HTML5 natif."

════════════════════════════════════════════════════════════════════
ÉCRAN 5 — RAPPORT                                            ⏱ 6:00
════════════════════════════════════════════════════════════════════

[CLIC → bouton "Rapport" ou navigation vers l'écran 5]

[DIRE]
  "Le rapport est généré. Immédiatement. Sur place."

[POINTER le résumé gauche]

[DIRE]
  "2 NC majeures. Plan d'actions avec délais. Responsables identifiés."

[POINTER la grille de conformité]

[DIRE]
  "La grille de conformité par section ISO.
   Un score global calculé sur tous les constats.
   Les barres virent au rouge sous 50 % — ici §7.1.5 est en alerte."

[CLIC → bouton "✎ Modifier"]

[DIRE]
  "Marc relit. Il veut ajuster une formulation.
   Les champs sont éditables directement."

[MODIFIER une phrase dans un textarea — ex: ajouter "urgent" quelque part]

[CLIC → "✓ Valider"]

[DIRE]
  "Il valide. Le rapport intègre sa correction."

[CLIC → toggle "🔒 Anonymiser"]

[DIRE]
  "Pour la transmission : anonymisation RGPD en un clic.
   Les noms disparaissent de l'aperçu."

[CLIC → retoggle pour désanonymiser]

[CLIC → "↓ Télécharger"]

[DIRE]
  "Export Word. Le fichier .docx est généré par le backend FastAPI
   et téléchargé directement."

[CLIC → section Signature → saisir "Karim Belkacem" → Signer]

[DIRE]
  "Validation contradictoire. Karim signe sur la tablette.
   Le rapport est tracé, sourcé, validé.
   Marc sort du site avec le travail terminé."

════════════════════════════════════════════════════════════════════
  [OPTIONNEL — parcours Mei Lin Zhang, 60 secondes si jury intéressé]
════════════════════════════════════════════════════════════════════

[CLIC → avatar en haut à droite → "Se connecter avec un autre compte"]

[DIRE]
  "On bascule côté RATP.
   Mei Lin Zhang, responsable qualité fournisseur."

[CLIC → carte "Mei Lin Zhang — Responsable Qualité RATP"]
[CLIC → Se connecter → portail fournisseur s'affiche]

[DIRE]
  "Son portail. Les documents déposés avant l'audit.
   Procédures, comptes-rendus, plans qualité —
   classés par catégorie, triés par date."

[CLIC → un document → montrer l'analyse Claude : résumé, sections à risque]

[DIRE]
  "Claude a lu ces documents pour elle.
   Elle sait ce que l'auditeur va trouver
   avant même qu'il arrive sur site."

════════════════════════════════════════════════════════════════════
CLÔTURE                                                      ⏱ 7:30
════════════════════════════════════════════════════════════════════

[DIRE]
  "Ce que vous venez de voir :

   Un auditeur qui prépare son audit en 30 secondes.
   Qui dicte — ou écrit à la main — ses constats les mains prises.
   Dont les non-conformités sont détectées, sourcées, et justifiées
   par Claude Opus — en contexte, en temps réel.
   Qui repart avec un rapport validé — pas avec une soirée de rédaction.

   Et côté client RATP : un portail fournisseur où les documents
   sont pré-analysés par Claude avant chaque visite.

   Le ratio 90/10 est inversé."

[PAUSE 2 secondes]

[DIRE]
  "Merci."

════════════════════════════════════════════════════════════════════
QUESTIONS PROBABLES — RÉPONSES PRÉPARÉES
════════════════════════════════════════════════════════════════════

Q : "Et si l'API Claude est indisponible ?"
R : "Fallback automatique sur un moteur local —
     RAG ISO 9001 + règles métier.
     Le badge dans l'interface indique 'IA locale' vs 'Claude Opus'.
     La démo fonctionne sans réseau GEP."

Q : "Quelle norme couvrez-vous ?"
R : "ISO 9001:2015. La base RAG couvre les clauses 4 à 10.
     L'architecture est extensible à ISO 14001, 45001, EN 9100 —
     le sélecteur de référentiel est déjà dans le Brief."

Q : "C'est quoi la vraie valeur ajoutée de Claude vs une règle fixe ?"
R : "La règle fixe donne 'clause 7.1.5 → action générique'.
     Claude donne 'clause 7.1.5, atelier Sucy-en-Brie,
     historique NC nov. 2024, responsable Karim Belkacem
     → étalonnage COFRAC dans les 72h, alertes automatisées'.
     C'est le contexte qui fait la différence."

Q : "Pourquoi GEP Capgemini plutôt que l'API Anthropic directe ?"
R : "GEP est la plateforme IA interne Capgemini — déjà en place,
     gestion des clés centralisée, conformité entreprise.
     Le modèle utilisé est anthropic.claude-opus-4-7."

Q : "La saisie manuscrite, ça marche vraiment ?"
R : "Claude vision reçoit l'image PNG du canvas et transcrit le texte.
     Sur tablette avec stylet, la précision est excellente.
     En fallback (GEP indisponible) : observation prédéfinie par item
     — le scénario de démo reste fluide dans les deux cas."

Q : "Comment les questions oui/non sont-elles générées ?"
R : "Appel POST /questions_oui_non à chaque sélection d'item.
     Claude génère 3 questions de vérification contextualisées
     par le texte de l'item, la clause ISO et le titre de section.
     Ces réponses sont ensuite injectées dans le prompt d'analyse
     pour enrichir le diagnostic."

════════════════════════════════════════════════════════════════════
CHECKLIST AVANT DE MONTER SUR SCÈNE
════════════════════════════════════════════════════════════════════

  [ ] start.bat lancé 3 min avant → backend démarré
  [ ] localhost:8000/docs accessible (test API)
  [ ] localhost:5173 accessible (frontend)
  [ ] Log "Application startup complete" visible (RAG chargé)
  [ ] .env vérifié : GEP_API_KEY + GEP_MODEL=anthropic.claude-opus-4-7
  [ ] Chrome ouvert (Web Speech API pour le micro)
  [ ] Son activé sur la machine (micro système)
  [ ] Image de test prête pour la photo (n'importe quel JPG)
  [ ] Écran en 16:9, zoom navigateur à 100 %
  [ ] Vérifier que le "PROCHAIN" audit est bien affiché
      (les statuts sont dynamiques — basés sur l'heure courante)

════════════════════════════════════════════════════════════════════
CHANGEMENTS DEPUIS LA VERSION PRÉCÉDENTE DU SCRIPT
════════════════════════════════════════════════════════════════════

  • Connexion : sélecteur de rôle (Marc vs Mei Lin Zhang)
    → plus de toggle thème "Agile Diagrams" (thème verrouillé)
  • Dashboard : carte interactive avec routing OSRM réel + flyTo
  • Brief : checklist éditable (× par item) + points auditeur personnalisés
  • Inspection — 3 colonnes :
      Col 1 : checklist dynamique
      Col 2 : capture + synthèse + articles RAG (avec tooltips)
      Col 3 : constats
  • Sélection d'un item → génération automatique suggestions + questions
  • Questions Oui/Non IA (colonne dédiée, contexte transmis à Claude)
  • Saisie manuscrite (canvas HTML5 + Claude vision, bouton ✍️)
  • Score X/3 → indicateur rond coloré (rouge/orange/jaune/vert)
  • Constats : badge + séparateur + constat + → action corrective
  • Rapport : grille de conformité par section ISO + anneau SVG score
  • Portail fournisseur : Mei Lin Zhang, catégories + filtres + tooltips

════════════════════════════════════════════════════════════════════
