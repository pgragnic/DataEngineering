// Données de démo — UC 28 RATP Inspection Augmentée

function _nowMin() {
  const d = new Date()
  return d.getHours() * 60 + d.getMinutes()
}
function _mth(m) {
  const t = ((m % 1440) + 1440) % 1440
  return `${String(Math.floor(t / 60)).padStart(2, "0")}h${String(t % 60).padStart(2, "0")}`
}
const _T = _nowMin()

export const AUDITEUR = {
  nom: "Marc Lefèvre",
  initiales: "ML",
  localisation: "Paris (75)",
  coords: { lat: 48.8566, lng: 2.3522 },
}

export const AUDITS_TIMELINE = [
  {
    id: "audit-cha",
    heure: _mth(_T - 120), heureMin: _T - 120,
    dureeMin: 45, type: "Bureau",
    titre: "Brief & revue documentaire — audit du jour",
    site: "Siège RATP — Direction Qualité", site_id: "RATP-DQ",
    scope: "§7.5 Informations documentées — Revue procédures & briefs sites",
    referentiel: "ISO 9001:2015", effectif: null,
    responsable: "Sophie Marchand", nc_ouvertes: 0,
    statut: "TERMINÉ", action: "Voir le rapport",
    trajet: { voiture: "—", velo: "—", pied: "—" }, trajetTC: "—",
    coords: { lat: 48.8724, lng: 2.3375 },
  },
  {
    id: "audit-fon",
    heure: _mth(_T - 45), heureMin: _T - 45,
    dureeMin: 120, type: "Visite",
    titre: "Audit maintenance préventive rames MI2N",
    site: "Atelier Fontenay-sous-Bois", site_id: "RATP-FON",
    scope: "§8.5.1 Maîtrise de la production — Plans de maintenance RER A",
    referentiel: "ISO 9001:2015", effectif: 142,
    responsable: "Patrick Duval", nc_ouvertes: 2,
    statut: "TERMINÉ", action: "Voir le rapport",
    trajet: { voiture: "28 min", velo: "52 min", pied: "2h15" }, trajetTC: "42 min",
    coords: { lat: 48.8505, lng: 2.4785 },
  },
  {
    id: "audit-suc",
    heure: _mth(_T + 45), heureMin: _T + 45,
    dureeMin: 150, type: "Visite",
    titre: "Audit étalonnage & gestion NC — RER A",
    site: "Atelier Sucy-en-Brie", site_id: "RATP-SUC",
    scope: "§7.1.5 Métrologie · §8.7 Non-conformités — Étalonnage & zone quarantaine",
    referentiel: "ISO 9001:2015", effectif: 218,
    responsable: "Mei Lin Zhang", nc_ouvertes: 3,
    statut: "PROCHAIN", action: "Démarrer", alwaysDemarrer: true,
    trajet: { voiture: "35 min", velo: "1h20", pied: "3h45" }, trajetTC: "55 min",
    coords: { lat: 48.7707, lng: 2.5126 },
  },
  {
    id: "audit-bou",
    heure: _mth(_T + 110), heureMin: _T + 110,
    dureeMin: 90, type: "Visite",
    titre: "Audit métrologie — équipements de mesure",
    site: "Atelier Boulogne-Billancourt", site_id: "RATP-BOU",
    scope: "§7.1.5 Ressources pour la surveillance — Registres étalonnage",
    referentiel: "ISO 9001:2015", effectif: 87,
    responsable: "Thierry Bonnet", nc_ouvertes: 1,
    statut: "PLANIFIÉ", action: `À ${_mth(_T + 110)}`,
    trajet: { voiture: "18 min", velo: "35 min", pied: "1h30" }, trajetTC: "28 min",
    coords: { lat: 48.8352, lng: 2.2389 },
  },
  {
    id: "audit-mon",
    heure: _mth(_T + 170), heureMin: _T + 170,
    dureeMin: 90, type: "Visite",
    titre: "Audit maîtrise des non-conformités",
    site: "Dépôt de bus Montrouge", site_id: "RATP-MON",
    scope: "§8.7 Éléments de sortie NC — Marquage, ségrégation, traçabilité pièces",
    referentiel: "ISO 9001:2015", effectif: 63,
    responsable: "Isabelle Renard", nc_ouvertes: 4,
    statut: "PLANIFIÉ", action: `À ${_mth(_T + 170)}`,
    trajet: { voiture: "25 min", velo: "45 min", pied: "2h00" }, trajetTC: "38 min",
    coords: { lat: 48.8139, lng: 2.3254 },
  },
  {
    id: "audit-vin",
    heure: _mth(_T + 230), heureMin: _T + 230,
    dureeMin: 60, type: "Bureau",
    titre: "Revue des performances qualité §9.1",
    site: "Atelier Vincennes", site_id: "RATP-VIN",
    scope: "§9.1 Surveillance & mesure — Indicateurs KPI, taux NC, délais clôture",
    referentiel: "ISO 9001:2015", effectif: 195,
    responsable: "Françoise Dupuis", nc_ouvertes: 0,
    statut: "PLANIFIÉ", action: `À ${_mth(_T + 230)}`,
    trajet: { voiture: "32 min", velo: "58 min", pied: "2h30" }, trajetTC: "48 min",
    coords: { lat: 48.8481, lng: 2.4391 },
  },
  {
    id: "audit-cla",
    heure: _mth(_T + 290), heureMin: _T + 290,
    dureeMin: 45, type: "Viso",
    titre: "Visio — Suivi plan d'actions correctives Q1",
    site: "Centre de contrôle Châtelet", site_id: null,
    scope: "§10.2 Non-conformité & action corrective — Avancement plan Q1 2025",
    referentiel: "ISO 9001:2015", effectif: null,
    responsable: "Ahmed Kara", nc_ouvertes: 7,
    statut: "PLANIFIÉ", action: `À ${_mth(_T + 290)}`,
    trajet: { voiture: "—", velo: "—", pied: "—" }, trajetTC: "—",
    coords: { lat: 48.8605, lng: 2.3477 },
  },
  {
    id: "audit-aub",
    heure: _mth(_T + 350), heureMin: _T + 350,
    dureeMin: 120, type: "Visite",
    titre: "Audit compétences & habilitations opérateurs",
    site: "Dépôt bus Aubervilliers", site_id: "RATP-AUB",
    scope: "§7.2 Compétences — Plans formation, habilitations conducteurs, registres",
    referentiel: "ISO 9001:2015", effectif: 310,
    responsable: "Nadia Slimani", nc_ouvertes: 2,
    statut: "PLANIFIÉ", action: `À ${_mth(_T + 350)}`,
    trajet: { voiture: "38 min", velo: "1h10", pied: "3h15" }, trajetTC: "52 min",
    coords: { lat: 48.9145, lng: 2.3810 },
  },
  {
    id: "audit-cre",
    heure: _mth(_T + 420), heureMin: _T + 420,
    dureeMin: 90, type: "Visite",
    titre: "Audit qualification prestataires externes",
    site: "Atelier Créteil", site_id: "RATP-CRE",
    scope: "§8.4 Maîtrise externe — Évaluation & qualification fournisseurs",
    referentiel: "ISO 9001:2015", effectif: 74,
    responsable: "Laurent Petit", nc_ouvertes: 1,
    statut: "PLANIFIÉ", action: `À ${_mth(_T + 420)}`,
    trajet: { voiture: "40 min", velo: "1h25", pied: "3h30" }, trajetTC: "58 min",
    coords: { lat: 48.7834, lng: 2.4544 },
  },
  {
    id: "audit-extra",
    heure: _mth(_T + 480), heureMin: _T + 480,
    dureeMin: 60, type: "Viso",
    titre: "Clôture audit — Revue avec Direction Qualité",
    site: "Siège RATP — Direction Qualité", site_id: null,
    scope: "Synthèse journée — Restitution résultats, plan curatif & préventif",
    referentiel: "ISO 9001:2015", effectif: null,
    responsable: "Sophie Marchand", nc_ouvertes: null,
    statut: "PLANIFIÉ", action: `À ${_mth(_T + 480)}`,
    trajet: { voiture: "—", velo: "—", pied: "—" }, trajetTC: "—",
    coords: { lat: 48.8724, lng: 2.3375 },
  },
]

export const AUDITS_EDF = [
  {
    id: "edf-def",
    heure: _mth(_T - 90), heureMin: _T - 90,
    dureeMin: 60, type: "Bureau",
    titre: "Brief & revue documentaire — audit du jour",
    site: "Siège EDF — Direction Qualité (La Défense)", site_id: "EDF-DEF",
    scope: "§7.5 Informations documentées — Revue procédures & référentiels sûreté",
    referentiel: "ISO 9001:2015", effectif: null,
    responsable: "Claire Fontaine", nc_ouvertes: 0,
    statut: "TERMINÉ", action: "Voir le rapport",
    trajet: { voiture: "—", velo: "—", pied: "—" }, trajetTC: "—",
    coords: { lat: 48.8929, lng: 2.2362 },
  },
  {
    id: "edf-iss",
    heure: _mth(_T + 60), heureMin: _T + 60,
    dureeMin: 120, type: "Visite",
    titre: "Audit étalonnage — capteurs de mesure process",
    site: "Centre de maintenance EDF — Issy-les-Moulineaux", site_id: "EDF-ISS",
    scope: "§7.1.5 Métrologie · §8.7 Non-conformités — Capteurs, bancs d'essai, zone NC",
    referentiel: "ISO 9001:2015", effectif: 156,
    responsable: "Bruno Mercier", nc_ouvertes: 2,
    statut: "PROCHAIN", action: "Démarrer",
    trajet: { voiture: "22 min", velo: "40 min", pied: "1h45" }, trajetTC: "32 min",
    coords: { lat: 48.8230, lng: 2.2710 },
  },
  {
    id: "edf-sac",
    heure: _mth(_T + 200), heureMin: _T + 200,
    dureeMin: 90, type: "Visite",
    titre: "Audit habilitations & compétences opérateurs",
    site: "Site R&D EDF Lab — Saclay", site_id: "EDF-SAC",
    scope: "§7.2 Compétences — Habilitations électriques, plans de formation, registres",
    referentiel: "ISO 9001:2015", effectif: 243,
    responsable: "Nathalie Perrin", nc_ouvertes: 1,
    statut: "PLANIFIÉ", action: `À ${_mth(_T + 200)}`,
    trajet: { voiture: "42 min", velo: "1h30", pied: "4h00" }, trajetTC: "1h05",
    coords: { lat: 48.7294, lng: 2.1732 },
  },
]

export const AUDITS_TOTAL = [
  {
    id: "total-def",
    heure: _mth(_T - 30), heureMin: _T - 30,
    dureeMin: 45, type: "Bureau",
    titre: "Brief & revue documentaire — audit du jour",
    site: "Siège TotalEnergies — La Défense", site_id: "TOTAL-DEF",
    scope: "§7.5 Informations documentées — Revue procédures & plans qualité refinery",
    referentiel: "ISO 9001:2015", effectif: null,
    responsable: "Arnaud Lestrade", nc_ouvertes: 0,
    statut: "TERMINÉ", action: "Voir le rapport",
    trajet: { voiture: "—", velo: "—", pied: "—" }, trajetTC: "—",
    coords: { lat: 48.8929, lng: 2.2362 },
  },
  {
    id: "total-gen",
    heure: _mth(_T + 100), heureMin: _T + 100,
    dureeMin: 120, type: "Visite",
    titre: "Audit gestion NC — réception matières premières",
    site: "Dépôt pétrolier TotalEnergies — Gennevilliers", site_id: "TOTAL-GEN",
    scope: "§8.7 Éléments de sortie NC · §8.4 Maîtrise externe — Réception & qualification",
    referentiel: "ISO 9001:2015", effectif: 88,
    responsable: "Sylvie Blanchard", nc_ouvertes: 3,
    statut: "PLANIFIÉ", action: `À ${_mth(_T + 100)}`,
    trajet: { voiture: "28 min", velo: "55 min", pied: "2h30" }, trajetTC: "42 min",
    coords: { lat: 48.9263, lng: 2.2962 },
  },
]

// Audit sélectionné pour la démo (écrans 2-3-4)
export const AUDIT_COURANT = {
  site_id: "RATP-SUC",
  nom: "Atelier de maintenance Sucy-en-Brie",
  localisation: "Sucy-en-Brie (94)",
  referentiel: "ISO 9001:2015",
  scope: ["Étalonnage des équipements de mesure", "Gestion des pièces non conformes"],
  effectif: 218,
  responsable_qualite: "Mei Lin Zhang",
  contact: "meilin.zhang@ratp.fr",
  duree_prevue: "2h30",
  auditeur: "Marc Lefèvre",
}

export const CHECKLIST = [
  {
    id: "S1",
    titre: "Maîtrise des informations documentées",
    clause: "§7.5",
    points: 4,
    items: [
      { id: "S1-1", texte: "Procédures d'étalonnage à jour", statut: "a-venir" },
      { id: "S1-2", texte: "Registres de calibration accessibles", statut: "a-venir" },
      { id: "S1-3", texte: "Instructions de travail signées", statut: "a-venir" },
      { id: "S1-4", texte: "Plan qualité révisé < 12 mois", statut: "a-venir" },
    ],
  },
  {
    id: "S2",
    titre: "Étalonnage & équipements de mesure",
    clause: "§7.1.5",
    points: 3,
    items: [
      { id: "S2-1", texte: "Clés dynamométriques étalonnées", statut: "a-venir" },
      { id: "S2-2", texte: "Étiquettes d'étalonnage visibles", statut: "a-venir" },
      { id: "S2-3", texte: "Registre des équipements complet", statut: "a-venir" },
    ],
  },
  {
    id: "S3",
    titre: "Gestion des non-conformités",
    clause: "§8.7",
    points: 3,
    items: [
      { id: "S3-1", texte: "Zone quarantaine délimitée", statut: "a-venir" },
      { id: "S3-2", texte: "Étiquetage pièces NC conforme", statut: "a-venir" },
      { id: "S3-3", texte: "Traçabilité des actions correctives", statut: "a-venir" },
    ],
  },
  {
    id: "S4",
    titre: "Compétences & formation",
    clause: "§7.2",
    points: 2,
    items: [
      { id: "S4-1", texte: "Habilitations opérateurs à jour", statut: "a-venir" },
      { id: "S4-2", texte: "Plan de formation documenté", statut: "a-venir" },
    ],
  },
]

export const AUDITS_PRECEDENTS = [
  {
    date: "2024-11-14",
    auditeur: "Marc Lefèvre",
    nc_majeures: 1,
    nc_mineures: 5,
    themes: ["Étalonnage des équipements de mesure"],
    statut: "NC mineure non clôturée",
    alerte: true,
  },
  {
    date: "2024-05-22",
    auditeur: "Marc Lefèvre",
    nc_majeures: 2,
    nc_mineures: 6,
    themes: ["Étalonnage des équipements de mesure", "Gestion des pièces non conformes"],
    statut: "Clôturé",
    alerte: false,
  },
  {
    date: "2023-10-10",
    auditeur: "Patricia Nguyen",
    nc_majeures: 0,
    nc_mineures: 3,
    themes: ["Étalonnage des équipements de mesure"],
    statut: "Clôturé",
    alerte: false,
  },
]

export const RECURRENCES = [
  {
    site: "Atelier Sucy-en-Brie",
    site_id: "RATP-SUC",
    items: ["Étalonnage équipements (§7.1.5)", "Gestion pièces NC (§8.7)"],
  },
  {
    site: "Atelier Châtillon",
    site_id: "RATP-CHA",
    items: ["Traçabilité maintenance (§7.5)", "Formation opérateurs (§7.2)"],
  },
  {
    site: "Atelier Fontenay",
    site_id: "RATP-FON",
    items: ["Documentation modes opératoires (§7.5)", "Compétences sous-traitants (§8.4)"],
  },
]

export const RAG_ARTICLES = {
  "§7.1.5": [
    {
      clause: "§7.1.5",
      titre: "Ressources pour la surveillance et la mesure",
      extrait:
        "L'organisme doit déterminer et fournir les ressources nécessaires pour assurer des résultats valides et fiables lorsque la surveillance ou la mesure est utilisée pour vérifier la conformité des produits et services.",
    },
    {
      clause: "§7.1.5.1",
      titre: "Généralités — étalonnage",
      extrait:
        "L'organisme doit s'assurer que les ressources sont adaptées au type spécifique d'activités de surveillance et de mesure entreprises.",
    },
    {
      clause: "§8.5.1",
      titre: "Maîtrise de la production et de la prestation de service",
      extrait:
        "L'organisme doit mettre en œuvre la production et la prestation de service dans des conditions maîtrisées, incluant l'utilisation d'équipements appropriés.",
    },
  ],
  "§8.7": [
    {
      clause: "§8.7",
      titre: "Maîtrise des éléments de sortie non conformes",
      extrait:
        "L'organisme doit s'assurer que les éléments de sortie qui ne sont pas conformes aux exigences sont identifiés et maîtrisés de manière à empêcher leur utilisation ou fourniture non intentionnelle.",
    },
    {
      clause: "§8.7.1",
      titre: "Traitement des non-conformités",
      extrait:
        "L'organisme doit prendre les mesures appropriées en se basant sur la nature de la non-conformité et son effet sur la conformité des produits et services.",
    },
    {
      clause: "§10.2",
      titre: "Non-conformité et action corrective",
      extrait:
        "En cas de non-conformité, l'organisme doit réagir à la non-conformité et, le cas échéant, prendre des mesures pour la maîtriser et la corriger.",
    },
  ],
  default: [
    {
      clause: "§9.2",
      titre: "Audit interne",
      extrait:
        "L'organisme doit réaliser des audits internes à des intervalles planifiés pour fournir des informations permettant de déterminer si le système de management de la qualité est conforme.",
    },
    {
      clause: "§10.3",
      titre: "Amélioration continue",
      extrait:
        "L'organisme doit améliorer en continu la pertinence, l'adéquation et l'efficacité du système de management de la qualité.",
    },
    {
      clause: "§6.1",
      titre: "Actions face aux risques et opportunités",
      extrait:
        "Lors de la planification du système de management de la qualité, l'organisme doit tenir compte des enjeux et des exigences des parties intéressées.",
    },
  ],
}

export const AUDITS_SEMAINE = {
  lundi: [
    { id: "lun-1", heure: "08h30", heureMin: 510, dureeMin: 60, type: "Bureau", titre: "Revue planning hebdomadaire", site: "Siège RATP", site_id: null, statut: "TERMINÉ", action: "Voir le rapport", trajet: { voiture: "—", velo: "—", pied: "—" }, coords: { lat: 48.8724, lng: 2.3375 } },
    { id: "lun-2", heure: "10h00", heureMin: 600, dureeMin: 120, type: "Visite", titre: "Atelier de maintenance Vitry", site: "Vitry-sur-Seine (94)", site_id: "RATP-VIT", statut: "TERMINÉ", action: "Voir le rapport", trajet: { voiture: "22 min", velo: "40 min", pied: "1h45" }, coords: { lat: 48.7880, lng: 2.3935 } },
    { id: "lun-3", heure: "14h00", heureMin: 840, dureeMin: 90, type: "Visite", titre: "Contrôle équipements Saint-Denis", site: "Saint-Denis (93)", site_id: "RATP-SDN", statut: "PLANIFIÉ", action: "À 14h00", trajet: { voiture: "35 min", velo: "1h10", pied: "3h00" }, coords: { lat: 48.9362, lng: 2.3574 } },
  ],
  mardi: [
    { id: "mar-1", heure: "09h00", heureMin: 540, dureeMin: 60, type: "Bureau", titre: "Revue documentaire — Colombes", site: "Centre de maintenance Colombes", site_id: "RATP-COL", statut: "TERMINÉ", action: "Voir le rapport", trajet: { voiture: "—", velo: "—", pied: "—" }, coords: { lat: 48.9213, lng: 2.2537 } },
    { id: "mar-2", heure: "11h00", heureMin: 660, dureeMin: 120, type: "Visite", titre: "Audit ISO 9001 — Colombes", site: "Centre de maintenance Colombes", site_id: "RATP-COL", statut: "TERMINÉ", action: "Voir le rapport", trajet: { voiture: "28 min", velo: "55 min", pied: "2h30" }, coords: { lat: 48.9213, lng: 2.2537 } },
    { id: "mar-3", heure: "16h00", heureMin: 960, dureeMin: 60, type: "Viso", titre: "Point de suivi NC — Direction", site: "Siège RATP", site_id: null, statut: "PLANIFIÉ", action: "À 16h00", trajet: { voiture: "—", velo: "—", pied: "—" }, coords: { lat: 48.8724, lng: 2.3375 } },
  ],
  mercredi: [
    { id: "audit-cha", heure: "08h30", heureMin: 510, dureeMin: 60, type: "Bureau", titre: "Revue documentaire — planning", site: "Atelier Châtillon", site_id: "RATP-CHA", statut: "TERMINÉ", action: "Voir le rapport", trajet: { voiture: "—", velo: "—", pied: "—" }, coords: { lat: 48.8017, lng: 2.2792 } },
    { id: "audit-fon", heure: "10h00", heureMin: 600, dureeMin: 120, type: "Visite", titre: "Atelier de maintenance Fontenay", site: "Fontenay-sous-Bois (94)", site_id: "RATP-FON", statut: "TERMINÉ", action: "Voir le rapport", trajet: { voiture: "28 min", velo: "52 min", pied: "2h15" }, coords: { lat: 48.8505, lng: 2.4785 } },
    { id: "audit-suc", heure: "14h30", heureMin: 870, dureeMin: 150, type: "Visite", titre: "Atelier de maintenance Sucy-en-Brie", site: "Sucy-en-Brie (94)", site_id: "RATP-SUC", statut: "PROCHAIN", action: "Démarrer", trajet: { voiture: "35 min", velo: "1h20", pied: "3h45" }, coords: { lat: 48.7707, lng: 2.5126 } },
    { id: "audit-extra", heure: "17h00", heureMin: 1020, dureeMin: 60, type: "Viso", titre: "Point de clôture — Direction Qualité", site: "Siège RATP", site_id: null, statut: "PLANIFIÉ", action: "À 17h00", trajet: { voiture: "—", velo: "—", pied: "—" }, coords: { lat: 48.8724, lng: 2.3375 } },
  ],
  jeudi: [
    { id: "jeu-1", heure: "09h00", heureMin: 540, dureeMin: 120, type: "Visite", titre: "Audit étalonnage — Noisy-le-Sec", site: "Noisy-le-Sec (93)", site_id: "RATP-NOI", statut: "PLANIFIÉ", action: "À 09h00", trajet: { voiture: "30 min", velo: "1h00", pied: "2h45" }, coords: { lat: 48.8929, lng: 2.4589 } },
    { id: "jeu-2", heure: "14h00", heureMin: 840, dureeMin: 90, type: "Bureau", titre: "Rédaction rapports — Noisy & Vitry", site: "Siège RATP", site_id: null, statut: "PLANIFIÉ", action: "À 14h00", trajet: { voiture: "—", velo: "—", pied: "—" }, coords: { lat: 48.8724, lng: 2.3375 } },
  ],
  vendredi: [
    { id: "ven-1", heure: "10h00", heureMin: 600, dureeMin: 120, type: "Visite", titre: "Contrôle documentation — Châtillon", site: "Atelier Châtillon", site_id: "RATP-CHA", statut: "PLANIFIÉ", action: "À 10h00", trajet: { voiture: "20 min", velo: "38 min", pied: "1h40" }, coords: { lat: 48.8017, lng: 2.2792 } },
    { id: "ven-2", heure: "15h00", heureMin: 900, dureeMin: 60, type: "Viso", titre: "Clôture semaine — Bilan qualité", site: "Siège RATP", site_id: null, statut: "PLANIFIÉ", action: "À 15h00", trajet: { voiture: "—", velo: "—", pied: "—" }, coords: { lat: 48.8724, lng: 2.3375 } },
  ],
}

export const KPIS = {
  audits_jour: 4,
  audits_mois: 12,
  delai_moyen: "2j",
  recurrences: 4,
}

export const QUESTIONS_SUGGEREES = {
  "7.1.5": "Quand les instruments de mesure de ce poste ont-ils été étalonnés pour la dernière fois ?",
  "7.1": "Quand les instruments de mesure de ce poste ont-ils été étalonnés pour la dernière fois ?",
  "8.7": "Quel est le processus de ségrégation et de traitement des pièces non conformes dans cet atelier ?",
  "7.2": "Les habilitations de l'ensemble des opérateurs sur ce poste sont-elles à jour ?",
  "7.5": "Les procédures et instructions de travail affichées correspondent-elles aux pratiques réelles ?",
  "8.4": "Les critères de qualification des sous-traitants ont-ils été vérifiés récemment ?",
  "9.2": "Les résultats du dernier audit interne ont-ils donné lieu à des actions correctives clôturées ?",
  "10.2": "Les actions correctives initiées suite aux dernières non-conformités ont-elles été efficaces ?",
}

// ── Plateforme fournisseur ────────────────────────────────────────────────────

export const SUPPLIER_DOCUMENTS = [
  {
    id: "doc-1",
    nom: "CR_Audit_nov_2024.docx",
    type: "DOCX",
    typeDoc: "Compte-rendu d'inspection",
    date: "14/11/2024",
    deposePar: "Eve Dupont",
    mock: true,
    statut: "analysé",
    url: "/documents/CR_Audit_nov_2024.docx",
    insights: {
      resume: "1 NC majeure §7.1.5 non clôturée (étalonnage). Zone quarantaine §8.7 partiellement traitée.",
      sections_a_risque: ["§7.1.5 — Étalonnage", "§8.7 — Zone quarantaine"],
      points_controle: ["Certificats étalonnage postes 8, 12, 15", "Délimitation zone quarantaine"],
      nc_historique: ["NC MAJEURE §7.1.5 — ouverte depuis nov. 2024 (non clôturée)"],
    },
  },
  {
    id: "doc-2",
    nom: "Procedures_etalonnage_v3.pdf",
    type: "PDF",
    typeDoc: "Procédure interne",
    date: "15/01/2025",
    deposePar: "Eve Dupont",
    mock: true,
    statut: "analysé",
    url: "/documents/Procedures_etalonnage_v3.pdf",
    insights: {
      resume: "3 clés dynamométriques périmées depuis 14 mois. Responsable métrologie vacant depuis sept. 2024.",
      sections_a_risque: ["§7.1.5 — Métrologie"],
      points_controle: ["Registre étalonnage à vérifier", "Plan de recrutement responsable métrologie"],
      nc_historique: [],
    },
  },
  {
    id: "doc-3",
    nom: "Plan_qualite_2025.pdf",
    type: "PDF",
    typeDoc: "Plan de prévention",
    date: "03/01/2025",
    deposePar: "Eve Dupont",
    mock: true,
    statut: "analysé",
    url: "/documents/Plan_qualite_2025.pdf",
    insights: {
      resume: "Action étalonnage §7.1.5 non réalisée (budget non alloué). Marquage zone quarantaine partiel.",
      sections_a_risque: ["§7.1.5 — Étalonnage COFRAC", "§8.7 — Zone quarantaine"],
      points_controle: ["Statut budget étalonnage", "Avancement marquage sol"],
      nc_historique: [],
    },
  },
]

export const SUPPLIER_DOC_CONTENT = {
  "CR_Audit_nov_2024.docx": `COMPTE-RENDU D'AUDIT ISO 9001:2015
Site : Atelier de maintenance Sucy-en-Brie (RATP) — Date : 14 novembre 2024
Auditeur : Marc Lefèvre (Bureau Veritas)

NC MAJEURE - §7.1.5 : Clés dynamométriques postes 8, 12 et 15 — certificats d'étalonnage périmés depuis mars 2024 (8 mois de retard). Aucune action corrective initiée à la date de l'audit.
NC MINEURE - §8.7 : Zone de quarantaine non clairement délimitée. Pièces non conformes et conformes mélangées dans la zone C.
OBSERVATION - §7.5 : Instruction de travail IT-054 non mise à jour depuis 18 mois.

ACTIONS DÉCIDÉES :
- Étalonnage §7.1.5 : planifié décembre 2024 (responsable Mei Lin Zhang) — STATUT : NON CLÔTURÉ
- Zone quarantaine §8.7 : marquage sol T1 2025 — STATUT : EN COURS

CONCLUSION : 1 NC majeure non clôturée. Audit de suivi requis.`,

  "Procedures_etalonnage_v3.pdf": `PROCÉDURE D'ÉTALONNAGE — PQ-054-v3
Site : Atelier Sucy-en-Brie — Mise à jour : janvier 2025

ÉQUIPEMENTS SOUMIS À ÉTALONNAGE (périodicité 12 mois, COFRAC accrédité) :
- Clés dynamométriques : postes 8, 12, 15
- Multimètres (6 unités) : périodicité 24 mois
- Jauges de pression (4 unités) : périodicité 12 mois

REGISTRE ACTUEL :
- Clé dyno poste 8 : dernier étalonnage 15/03/2024 → PÉRIMÉ (retard 14 mois)
- Clé dyno poste 12 : dernier étalonnage 22/03/2024 → PÉRIMÉ (retard 14 mois)
- Clé dyno poste 15 : dernier étalonnage 10/03/2024 → PÉRIMÉ (retard 14 mois)
- Multimètres : conformes jusqu'à 03/2026

NOTE : responsable métrologie vacant depuis septembre 2024. Intérim chef atelier.`,

  "Plan_qualite_2025.pdf": `PLAN QUALITÉ 2025 — ATELIER SUCY-EN-BRIE
Responsable : Mei Lin Zhang

OBJECTIFS 2025 : taux NC récurrentes < 10% (baseline 23%) — délai clôture NC < 30 j (baseline 67 j)

ACTIONS PRIORITAIRES :
1. Étalonnage COFRAC clés dynamométriques §7.1.5 — STATUT : NON RÉALISÉ (budget non alloué)
2. Délimitation zone quarantaine §8.7 — STATUT : PARTIELLEMENT RÉALISÉ (marquage partiel)
3. Mise à jour instructions de travail §7.5 — STATUT : EN COURS`,
}

export const SUPPLIER_ALERTS = {
  "§7.1.5": { criticite: "majeure", message: "3 clés dyno périmées — NC non clôturée nov. 2024" },
  "§8.7":   { criticite: "mineure", message: "Zone quarantaine partiellement délimitée" },
}
