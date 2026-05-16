# UC 28 — Script de démo Teams · 3 minutes live

> **Version Teams.** Remplace l'ancien script (in-person) qui supposait 3 personnes en salle.
> À imprimer en 3 exemplaires. Plié sur le côté de l'écran du **pilote** pendant la démo.
> **Dernière révision avant passage : compléter les blocs `[À CONFIRMER]` avec les vraies infos device/team/horaires.**

---

## Nouvelle distribution des rôles

| Rôle | Pendant les 3 min de démo | Pendant le Q&A |
|---|---|---|
| **PILOTE** (1 personne) | Partage son écran. Parle. Clique. Tape. **C'est lui le show.** | Continue à piloter le partage. Répond aux questions de démo. |
| **BACKUP TECH** (1 personne) | Caméra ON, micro OFF, en retrait visuel. Mains sur son clavier. Prêt à reprendre le partage si le pilote crash. | Active son micro pour répondre aux questions techniques (archi, agents, Claude Code). |
| **BACKUP MÉTIER** (1 personne) | Caméra ON, micro OFF. Observe les réactions de la tuile « jury ». | Active son micro pour les questions métier, GTM, ROI. |

**Le choix du pilote est la décision la plus importante du projet.** Critères :
- Élocution claire, posée
- Calme sous pression
- Connaît le code par cœur (pour improviser si bug)
- Sait que « moins on parle, plus c'est fort »

À figer avant le **30 mai**. Ne change pas après.

---

## Setup technique avant la démo Teams · 30 minutes avant

À faire par le pilote.

### Préparation poste

- [ ] **Connexion** : Ethernet câblé si possible. À défaut, wifi 5 GHz, le plus proche du routeur.
- [ ] **Notifications coupées partout** : Mac Focus Mode / Windows Focus assist activé. Slack, Mail, Teams notif desktop, navigateur. Tout. Aucune vibration de phone non plus.
- [ ] **Écran principal** : un seul écran. Si dual screen, débranche le secondaire avant le partage (évite de partager le mauvais).
- [ ] **Résolution écran** : descends à 1920×1080 (Full HD) pour éviter que le jury voie du texte trop petit côté Teams.
- [ ] **Volume monté** à 50-60% (les sons d'animation de l'app seront partagés au jury).

### Préparation Chrome

- [ ] **Une seule fenêtre Chrome ouverte**, plein écran (F11).
- [ ] **Deux onglets seulement** : 
  - **Onglet 1 = slides** (PDF des slides du deck support exporté, ouvert dans Chrome → `Cmd+1`).
  - **Onglet 2 = l'app** sur `localhost:3000/inspection/[id]/capture` → `Cmd+2`.
- [ ] **Aucune extension Chrome visible** (barre épinglée vide ou cachée).
- [ ] **Pas de barre de favoris** (`Cmd+Shift+B` pour masquer).
- [ ] **Mode navigation privée** ou profil dédié « démo » : pas d'autocomplete d'URLs perso qui surgit.
- [ ] **Cookies de l'app pré-acceptés** + permission micro déjà donnée (même si on n'utilise pas le micro, évite la pop-up).

### Préparation app

- [ ] **Reset de la démo** : `localhost:3000/dev/reset` → confirmation visuelle (check-list générée pour Fournisseur ALPHA, 0 constat).
- [ ] **Les 4 constats** copiés en presse-papier rotation, ou directement disponibles via raccourci `Cmd+Shift+1` à `Cmd+Shift+4` (replay mode keyboard). À tester ce raccourci avant la session.
- [ ] **La photo** `sortie_secours_alpha.jpg` chargée dans le système de fichiers de l'app — un clic suffit pour l'attacher.
- [ ] **L'horloge de l'app** simulée à 14:32 (fixture).
- [ ] **Onglet slides** déjà sur la première slide (Antoine intro).

### Préparation Teams

- [ ] **Connecté 5 min avant** l'heure du passage. Test caméra et micro avec un collègue.
- [ ] **Caméra ON** au démarrage. Light naturelle si possible (fenêtre face à toi, pas derrière).
- [ ] **Arrière-plan virtuel Capgemini** ou bureau neutre. **Pas** de flou (capte mal sur certains devices jury).
- [ ] **Vue Teams en mode « Galerie »** au minimum — tu dois voir toutes les tuiles du jury.
- [ ] **Partage d'écran prêt mais pas encore lancé** — tu déclenches au moment de démarrer la démo, pas avant.
- [ ] ⚠️ **CRITIQUE** : Quand tu lances le partage, **active la case « Inclure le son de l'ordinateur »** (Mac) ou « Inclure les sons de l'ordinateur » (Windows). Sinon les sons d'animation et le silence dramatique ne passent pas. À tester une fois en répétition.

### Préparation backup video

- [ ] `UC28_demo_backup.mp4` ouvert en pause dans QuickTime/VLC, en plein écran, sur un troisième espace de bureau prêt à devenir actif au pire moment.

---

## Acte 1 — Le crochet humain · 0:00 → 0:30

**À l'écran (partage actif) :** Onglet 1, **slide 1** (Antoine Mercier, 12 mai 2026, 14h30).

**Pilote :** Caméra ON. Tu regardes **la lentille de la webcam**, pas l'écran. C'est contre-intuitif mais essentiel pour que le jury ait l'impression de contact visuel.

**Pilote dit (verbatim) :**

> « Lundi 12 mai 2026, 14h30. »
>
> *(pause 1 seconde)*
>
> « Antoine Mercier, 42 ans, Lead Auditor Bureau Veritas. Il vient de finir un audit qualité chez un fournisseur à Tours. »
>
> « Avec ses outils actuels — Word, ses notes papier, ses 250 pages de normes ISO 9001 en PDF — le rapport partira au client jeudi. »
>
> *(pause 1 seconde)*
>
> « Avec ce qu'on va vous montrer maintenant, il part avant 15h. »
>
> *(pause)*
>
> « On l'appelle BV·Inspect. »

---

## Transition · 0:30 → 0:35

**Pilote :** Appuie `Cmd+2` → bascule sur l'onglet 2 (l'app).

**À l'écran :** L'écran principal de l'app — header avec « Audit ISO 9001 · Fournisseur ALPHA », check-list à gauche, timer 14:32 qui démarre, zone capture au centre.

**Pilote :** Laisse 2 secondes de silence. Le jury découvre l'interface.

---

## Acte 2 — Le cœur · 0:35 → 2:00

### Beat 1 · La check-list · 0:35 → 0:50

**Pilote :** Glisse la souris doucement sur la zone check-list (pas de clic — juste l'attention visuelle).

**Pilote dit :**

> « L'audit a commencé à 8h ce matin. Cette check-list à gauche a été générée par notre agent de préparation à partir du brief client. Antoine arrive sur site avec un guide d'audit adapté au scope du jour — pas une check-list générique. »
>
> « Maintenant, place à la capture. »

---

### Beat 2 · Constat #1 — Conforme · 0:50 → 1:10

**Pilote :** Clic sur le bouton micro (le mic devient rouge pulsant, la waveform s'anime — pour l'effet visuel seulement, pas pour la reconnaissance vocale en Teams).

**Pilote :** Tape immédiatement dans le textarea (la touche `T` ouvre le fallback clavier prévu dans la spec UI). Colle directement le texte préchargé :

```
la procédure achats existe version 3 datée de mars 2026 elle est accessible sur l'intranet
```

**Pilote :** `Cmd+Enter` (ou clic Valider).

**Pendant les 2-3 secondes de classification :** La bande RAG en bas s'allume avec les 3 chunks d'ISO 9001 §7.5.3, §7.5.2, §4.4.

**À l'écran :** Carte classification apparaît — badge **CONFORME** vert, référence **ISO 9001 §7.5.3**, texte reformulé.

**Pilote dit (pendant et après l'apparition de la carte) :**

> « Antoine vient de saisir son premier constat. En trois secondes, l'agent de capture le reformule, le classifie comme conforme, et le source à l'article 7.5.3 de l'ISO 9001 — la maîtrise des informations documentées. »
>
> « En bas de l'écran, vous voyez les trois extraits de normes que le système a remontés pour faire ce sourcing. C'est transparent. Antoine peut vérifier. Vous pouvez vérifier. »

---

### Beat 3 · Constat #2 — NC Majeure ⭐ LE MOMENT FORT · 1:10 → 1:45

**Ralentir tout.** C'est le pic de la démo. Mêmes 3 secondes que les autres mais elles doivent peser plus.

**Pilote :** Clic micro, ouvre le textarea, colle :

```
la sortie de secours du bâtiment B est obstruée par un chariot de stockage rempli de cartons
```

**Pilote :** Valide. **Et là — ne dit rien.** 3 secondes de silence pendant que la classification tourne. **Regarde la caméra**, pas l'écran. Le jury aussi est silencieux.

**À l'écran (3 sec d'attente) :** RAG band s'allume avec §7.1.4 (sim 0.93), §7.1.3, §8.5.1.

**Puis :** Carte classification — badge **NC MAJEURE** rouge, référence **ISO 9001 §7.1.4**.

**Pilote dit, voix posée, plus lente :**

> « Classifié non-conformité majeure. Sourcé à l'article 7.1.4 — environnement pour la mise en œuvre des processus. Et le système propose une action corrective : libérer la sortie, rappel aux magasiniers, ajout au plan d'audit sécurité annuel. »

**Pilote :** Clic « + Ajouter photo ». La photo `sortie_secours_alpha.jpg` est sélectionnée. Vignette apparaît en haut à droite de la carte.

**Pilote enchaîne :**

> « Antoine attache la photo de preuve. Elle est liée au constat. La traçabilité est constituée en temps réel — pas trois jours après. »

**Pilote :** Valide. Le constat glisse à droite dans la liste.

---

### Beat 4 · Constat #3 — NC Mineure récurrente · 1:45 → 2:05

**Pilote :** Clic micro, textarea, colle :

```
pas de procédure documentée de contrôle réception mais le magasinier a un cahier manuscrit où il note les écarts
```

**Pilote :** Valide.

**À l'écran :** Carte **NC MINEURE** avec ISO 9001 §8.4.3. Sous la carte, un encart « ⚠ Récurrent — déjà relevé 2025-06-15, non clôturé ».

**Pilote, point culminant métier :**

> « Et là, le système remarque quelque chose qu'aucun inspecteur n'aurait eu le temps de croiser manuellement. »
>
> *(pause, pointer l'encart de récurrence)*
>
> « Cette non-conformité a déjà été relevée lors de l'audit précédent, en juin 2025. Elle n'a pas été clôturée. »
>
> « C'est de la valeur que la mémoire d'Antoine, aussi exceptionnelle soit-elle, ne pouvait pas garantir. »

**Pilote :** Valide.

---

## Acte 3 — La frappe finale · 2:05 → 2:45

### Beat 5 · La génération du pré-rapport · 2:05 → 2:25

**Pilote dit :**

> « L'audit est terminé. Antoine clique. »

**Pilote :** Clic sur le gros bouton vert **« Générer le pré-rapport → »** dans le footer.

**À l'écran :** Modale plein écran avec spinner et messages cycliques toutes les 2 secondes.

**Pilote, pendant ces ~10-12 secondes : NE DIT RIEN.** Regarde la caméra. Sourit légèrement. **Le silence en Teams est très puissant** — le jury attend, comme toi.

---

### Beat 6 · Le DOCX révélé · 2:25 → 2:45

**À l'écran :** Le viewer DOCX s'affiche. Page de garde visible.

**Pilote :** Scrolle lentement dans le viewer.

**Pilote narre le scroll :**

> « Page de garde. Synthèse exécutive en trois lignes : une non-conformité majeure à action immédiate, une non-conformité mineure récurrente, une observation, un point conforme. »
>
> *(scroll au plan d'action)*
>
> « Plan d'action priorisé. La NC majeure en priorité 1 — sous 24 heures. La NC mineure en priorité 2 — sous 60 jours. Recommandation d'un audit de suivi sous 3 mois. »
>
> *(pause)*
>
> « Antoine envoie ce document à son client à 14h57. »
>
> *(pause 1 seconde)*
>
> « Trois jours d'avance sur sa pratique habituelle. »

---

## Closing — La punchline · 2:45 → 3:00

**Pilote :** `Cmd+1` → bascule sur l'onglet 1 (slides). **Slide 3 (24 000 heures)** apparaît plein écran.

**Pilote, voix posée et lente, regarde la caméra :**

> « Trente inspecteurs Bureau Veritas. »
>
> « Quatre heures économisées par audit. »
>
> « Deux cents audits par an. »
>
> *(pause 1 seconde)*
>
> « **Vingt-quatre mille heures libérées par an. Quinze équivalents temps plein redéployables sur l'analyse à valeur. ** »

**FIN.** Sourire à la caméra.

---

## Acte 4 OPTIONNEL · La bascule platform · 3:00 → 4:00

À jouer **uniquement** si le jury accorde plus de 3 minutes.

**Pilote :** `Cmd+2` → retour à l'app. Clic sur le dropdown « ISO 9001 » dans le header.

**Pilote dit :**

> « ISO 9001, on vient de le démontrer. Mais Bureau Veritas, c'est 150 référentiels actifs. Regardons NFC 15-100 — la sécurité électrique. »

**Pilote :** Sélectionne « NFC 15-100 ». Shimmer / fade-out / fade-in pendant 5 secondes. Nouvelle check-list apparaît, articles différents.

**Pilote :** Ouvre le textarea, colle :

```
absence de dispositif différentiel à haute sensibilité sur le tableau électrique du local technique
```

**Pilote :** Valide.

**À l'écran :** Classification NC MAJEURE, NFC 15-100 §531.

**Pilote :** `Cmd+1` → bascule slide 4 (Platform vision).

**Pilote :**

> « Le même socle technique. Le même geste métier. N'importe quel référentiel. »
>
> « Bureau Veritas, 85 000 collaborateurs, 140 pays, 150 référentiels actifs. APAVE, SGS, TÜV. Tout opérateur d'inspection a le même besoin. »
>
> « C'est ça, le platform play que cette démonstration ouvre. »

---

## Q&A — Coordination

**Pilote :** Bascule sur **slide 5** (Questions ?). Reste sur cet écran le temps du Q&A. Active sa caméra. Continue de partager.

**Les 2 backup activent leur micro maintenant.** Pas avant.

**Distribution des questions :**

| Type | Qui répond | Posture |
|---|---|---|
| Démo / « pouvez-vous montrer X ? » | **Pilote** (encore lui) | Il est déjà au clavier. Une seule personne touche au partage. |
| Technique pure (archi, agents, Claude Code) | **Backup Tech** | Active son micro, répond. Pilote tient l'écran. |
| Métier / GTM / ROI / persona | **Backup Métier** | Active son micro, répond. |
| Question ambiguë ? | **Pilote redirige verbalement** : *« Bonne question — [Prénom Backup] tu as l'angle ? »* | Évite que deux répondent en même temps en Teams (catastrophe audio). |

**Règle d'or Teams :** **un seul micro actif** dans l'équipe à tout instant pendant le Q&A. Les transitions micro-on / micro-off doivent être muettes (espace pour mute Teams = `Cmd+Shift+M` Mac / `Ctrl+Shift+M` Win).

**Si une question vous met en difficulté :** *« Bonne question. Je vous donne mon angle, mes collègues compléteront. »* Vous gagnez 3 secondes de réflexion, et c'est une invitation explicite à passer la parole.

---

## Plans B — Que faire si ça casse

### Si l'API Claude rame (carte classification reste en shimmer > 10 sec)

**Réaction immédiate :** Pilote tape `Cmd+Shift+1` (constat 1), `Cmd+Shift+2`, `Cmd+Shift+3`. Le replay mode injecte directement les constats préenregistrés avec animations normales.

**Verbal :** Aucun commentaire. Le jury ne voit rien.

### Si Teams se déconnecte du pilote

**Backup Tech reprend le partage** depuis sa machine (il a la même app installée localement).

**Verbal Backup Tech :** *« On reprend la main, juste un instant. »*

**Pilote** se reconnecte en parallèle, redevient backup. Mais le show continue sans interruption pour le jury.

### Si le partage d'écran lag ou freeze

**Pilote arrête le partage et le relance** (généralement 3-5 sec).

**Verbal :** *« Je relance le partage. »*

Ne pas s'excuser plus que ça. Continuer.

### Si la photo ne s'attache pas

**Pilote clique « Valider » sans photo.**

**Verbal :** *« Antoine attache la photo plus tard — elle sera dans le rapport final. »*

### Si la génération de pré-rapport plante

**Pilote ouvre l'onglet 3 (caché) avec `data/fixtures/alpha_report.docx` pré-rendu en PDF.**

**Verbal :** *« Le document généré : page de garde, synthèse, plan d'action — exactement comme Antoine l'envoie à son client. »*

### Si TOUT plante (réseau coupé, écran noir, l'app meurt)

**Réaction nucléaire :** Pilote lance VLC en plein écran avec `UC28_demo_backup.mp4`. Partage cette fenêtre à la place de Chrome.

**Verbal :** *« Pour ne pas vous faire perdre votre temps avec des soucis techniques, voici la démo enregistrée. Le Q&A se fera en direct ensuite. »*

**Sourire. Ne pas paniquer.** Les jurys respectent les équipes qui ont prévu un plan B et savent l'exécuter calmement.

---

## Vocabulaire · ce qu'on dit, ce qu'on évite

**À utiliser :**
- « copilote » (jamais « assistant IA »)
- « le système » ou « l'agent » (jamais « le LLM » ou « l'algorithme »)
- « sourcé à la norme » (jamais « avec hallucination contrôlée »)
- « valeur libérée » (jamais « gain de productivité » qui fait peur en interne)
- « Antoine » (jamais « l'utilisateur » ou « le persona »)

**À éviter :**
- Tout acronyme technique non expliqué (LLM, MCP, embeddings, fine-tuning). « RAG » est OK *seulement* si la bande RAG est à l'écran.
- « On a utilisé Claude » → préférer « On a construit ça en 4 semaines avec Claude Code »
- « Si on avait eu plus de temps » → jamais.
- Les chiffres non assumés. Si on cite 24 000 h, on l'a calculé.

---

## Adaptations spécifiques Teams · à intégrer dans les répétitions

1. **Eye contact = lentille webcam, pas écran.** S'entraîner consciemment. Coller un petit gommette à côté de la webcam comme repère.
2. **Voix plus posée, plus articulée qu'en présentiel.** Le micro filtre les nuances ; il faut sur-articuler.
3. **Marquer les pauses plus longuement.** Une pause de 1 seconde en présentiel = 1,5 seconde en Teams (le délai audio ronge le silence).
4. **Pas de gestes** des mains hors champ caméra — ils ne se voient pas et déconcentrent.
5. **Si le jury fait une réaction visible** dans sa tuile (sourire, hochement, sourcil levé), le pilote **peut** brièvement l'acknowledge à la fin du Q&A. Pas pendant la démo.

---

## Répétitions Teams · cadence à tenir

| Date | Objectif |
|---|---|
| **26 mai** (S3 — première complète) | Réunion Teams à 3, partage d'écran réel, tenir 3 min sans regarder le script. |
| **28 mai** | Répétition avec UN observateur externe (un collègue d'une autre équipe hackathon) joué « jury ». Recueillir 3 retours. |
| **30 mai** | Répétition à voix posée, debout (ou assis selon préférence pilote — mais figer la posture). **Filmer la session Teams via OBS ou enregistrement Teams**. Revoir. |
| **2 juin** | Répétition avec coupure simulée de Wi-Fi du pilote — tester la reprise par Backup Tech. |
| **3 juin** | Répétition propre, chronométrée précisément. |
| **4 juin matin** | Répétition finale. Pas de modification du script après ce point. |
| **5 juin** | Lecture en silence du script. Visualisation. Repos. |

**Règle :** le script en main jusqu'à la rép #3. À partir de la #4, plus le droit. Si vous bloquez, vous improvisez la formulation — jamais le contenu.

---

## 60 secondes avant le passage devant le jury

Le pilote, seul à son poste :

1. Visualiser la salle (les tuiles du jury à venir).
2. Respiration : 4 secondes inspiration, 6 expiration. Trois fois.
3. Phrase mentale : *« On l'a fait 20 fois. On le refait. Antoine attend. »*
4. Vérifier une dernière fois : Chrome plein écran, slide 1 affichée, micro coupé.
5. Sourire à la webcam.
6. Démarrer le partage d'écran avec les sons activés.
7. « Bonjour, c'est l'équipe [nom]. »

Les deux backups, dans leur coin :

1. Caméra ON, micro OFF.
2. Position neutre, regard caméra.
3. Prêts à intervenir, mais pas avant.

---

**Fin du script.**

**Bonne démo.** En 3 minutes Teams, vous allez plus marquer que la moitié des équipes en 3 minutes présentiel — parce que vous y aurez réfléchi mieux qu'elles.

*— UC 28 · Inspection Augmentée · Vibe Coding Hackathon Capgemini × Anthropic*
