// Données mock — Crédit Mutuel de Bretagne
// Titulaire : Marie-Anne Guillou

export const USER = {
  nom: 'Marie-Anne Guillou',
  email: 'marianne.guillou@outlook.fr',
  agence: 'CMB Rennes Thabor',
  conseiller: 'Fabien Le Bras',
  avatar: 'MG',
}

export const COMPTES = [
  {
    id: 'cc1',
    type: 'Compte courant',
    numero: '****4521',
    solde: 2847.63,
    solde_precedent: 1950.00,
    iban: 'FR76 1558 9000 0104 5210 0000 012',
    couleur: '#C8102E',
    icone: 'wallet',
  },
  {
    id: 'ldd1',
    type: 'Livret Développement Durable',
    numero: '****8832',
    solde: 12_000.00,
    solde_precedent: 11_900.00,
    taux: 3.0,
    couleur: '#059669',
    icone: 'piggy-bank',
  },
  {
    id: 'pel1',
    type: 'Plan Épargne Logement',
    numero: '****2204',
    solde: 35_400.00,
    solde_precedent: 35_000.00,
    taux: 2.25,
    ouverture: '2019-03-15',
    couleur: '#2563EB',
    icone: 'home',
  },
  {
    id: 'cred1',
    type: 'Crédit immobilier',
    numero: '****6678',
    capital_restant: -127_340.00,
    mensualite: 742.00,
    taux: 1.85,
    echeance: '2039-06-30',
    couleur: '#9333EA',
    icone: 'credit-card',
  },
]

// Transactions des 3 derniers mois (compte courant)
const today = new Date()
const d = (daysAgo) => {
  const dt = new Date(today)
  dt.setDate(dt.getDate() - daysAgo)
  return dt.toISOString().split('T')[0]
}

export const TRANSACTIONS = [
  // Juin
  { id: 't1',  date: d(1),  libelle: 'VIREMENT SALAIRE CAPGEMINI',       montant: 3_450.00, categorie: 'Revenus',         compte: 'cc1' },
  { id: 't2',  date: d(2),  libelle: 'LIDL RENNES ALMA',                  montant: -87.34,   categorie: 'Alimentation',    compte: 'cc1' },
  { id: 't3',  date: d(3),  libelle: 'INTERMARCHÉ THABOR',               montant: -64.12,   categorie: 'Alimentation',    compte: 'cc1' },
  { id: 't4',  date: d(4),  libelle: 'STAR RENNES — PASS MENSUEL',       montant: -57.00,   categorie: 'Transport',        compte: 'cc1' },
  { id: 't5',  date: d(5),  libelle: 'EDF ENERGIES — PRÉLÈVEMENT',       montant: -98.00,   categorie: 'Logement',         compte: 'cc1' },
  { id: 't6',  date: d(6),  libelle: 'NETFLIX',                           montant: -15.99,   categorie: 'Loisirs',          compte: 'cc1' },
  { id: 't7',  date: d(7),  libelle: 'AMAZON PRIME',                      montant: -6.99,    categorie: 'Loisirs',          compte: 'cc1' },
  { id: 't8',  date: d(8),  libelle: 'PHARMACIE DE L\'HÔPITAL',          montant: -24.50,   categorie: 'Santé',            compte: 'cc1' },
  { id: 't9',  date: d(9),  libelle: 'VIREMENT CMB PEL',                 montant: -200.00,  categorie: 'Épargne',          compte: 'cc1' },
  { id: 't10', date: d(10), libelle: 'RESTAURANT LE SAINT-MALO',         montant: -38.00,   categorie: 'Restaurant',       compte: 'cc1' },
  { id: 't11', date: d(12), libelle: 'DÉCATHLON RENNES',                  montant: -89.99,   categorie: 'Loisirs',          compte: 'cc1' },
  { id: 't12', date: d(14), libelle: 'SNCF TGV RENNES-PARIS',            montant: -52.00,   categorie: 'Transport',        compte: 'cc1' },
  { id: 't13', date: d(15), libelle: 'ORANGE MOBILE — MENSUEL',          montant: -29.99,   categorie: 'Télécom',          compte: 'cc1' },
  { id: 't14', date: d(17), libelle: 'CARREFOUR MARKET OBERTHÜR',        montant: -43.21,   categorie: 'Alimentation',    compte: 'cc1' },
  { id: 't15', date: d(19), libelle: 'REMBOURSEMENT AMELI SS',           montant: 18.60,    categorie: 'Santé',            compte: 'cc1' },
  { id: 't16', date: d(20), libelle: 'PRÉLÈVEMENT CRÉDIT IMMOBILIER',    montant: -742.00,  categorie: 'Logement',         compte: 'cc1' },
  { id: 't17', date: d(22), libelle: 'BOULANGERIE LE FOURNIL',           montant: -9.40,    categorie: 'Alimentation',    compte: 'cc1' },
  { id: 't18', date: d(24), libelle: 'CINEMA GAUMONT RENNES',            montant: -23.60,   categorie: 'Loisirs',          compte: 'cc1' },
  { id: 't19', date: d(26), libelle: 'VÊTEMENTS ZARA RENNES',            montant: -79.90,   categorie: 'Shopping',         compte: 'cc1' },
  { id: 't20', date: d(27), libelle: 'ASSURANCE MMA — AUTO',             montant: -45.80,   categorie: 'Assurance',        compte: 'cc1' },
  // Mois n-1
  { id: 't21', date: d(32), libelle: 'VIREMENT SALAIRE CAPGEMINI',       montant: 3_450.00, categorie: 'Revenus',         compte: 'cc1' },
  { id: 't22', date: d(33), libelle: 'LIDL RENNES ALMA',                  montant: -91.20,   categorie: 'Alimentation',    compte: 'cc1' },
  { id: 't23', date: d(35), libelle: 'PRÉLÈVEMENT CRÉDIT IMMOBILIER',    montant: -742.00,  categorie: 'Logement',         compte: 'cc1' },
  { id: 't24', date: d(36), libelle: 'STAR RENNES — PASS MENSUEL',       montant: -57.00,   categorie: 'Transport',        compte: 'cc1' },
  { id: 't25', date: d(37), libelle: 'EDF ENERGIES — PRÉLÈVEMENT',       montant: -98.00,   categorie: 'Logement',         compte: 'cc1' },
  { id: 't26', date: d(38), libelle: 'SUSHI SHOP RENNES',                montant: -34.50,   categorie: 'Restaurant',       compte: 'cc1' },
  { id: 't27', date: d(40), libelle: 'AMAZON ACHAT EN LIGNE',            montant: -127.40,  categorie: 'Shopping',         compte: 'cc1' },
  { id: 't28', date: d(42), libelle: 'VIREMENT CMB LDD',                 montant: -100.00,  categorie: 'Épargne',          compte: 'cc1' },
  { id: 't29', date: d(45), libelle: 'SPOTIFY',                          montant: -10.99,   categorie: 'Loisirs',          compte: 'cc1' },
  { id: 't30', date: d(48), libelle: 'MÉDECIN GÉNÉRALISTE',              montant: -26.50,   categorie: 'Santé',            compte: 'cc1' },
  { id: 't31', date: d(50), libelle: 'ORANGE MOBILE — MENSUEL',         montant: -29.99,   categorie: 'Télécom',          compte: 'cc1' },
  { id: 't32', date: d(52), libelle: 'INTERMARCHÉ THABOR',              montant: -76.88,   categorie: 'Alimentation',    compte: 'cc1' },
  { id: 't33', date: d(55), libelle: 'FNAC RENNES — LIVRE',             montant: -22.00,   categorie: 'Loisirs',          compte: 'cc1' },
  // Mois n-2
  { id: 't34', date: d(63), libelle: 'VIREMENT SALAIRE CAPGEMINI',       montant: 3_450.00, categorie: 'Revenus',         compte: 'cc1' },
  { id: 't35', date: d(64), libelle: 'PRÉLÈVEMENT CRÉDIT IMMOBILIER',    montant: -742.00,  categorie: 'Logement',         compte: 'cc1' },
  { id: 't36', date: d(65), libelle: 'LIDL RENNES ALMA',                  montant: -78.60,   categorie: 'Alimentation',    compte: 'cc1' },
  { id: 't37', date: d(66), libelle: 'STAR RENNES — PASS MENSUEL',       montant: -57.00,   categorie: 'Transport',        compte: 'cc1' },
  { id: 't38', date: d(68), libelle: 'EDF ENERGIES — PRÉLÈVEMENT',       montant: -112.00,  categorie: 'Logement',         compte: 'cc1' },
  { id: 't39', date: d(70), libelle: 'RESTO L\'ARDOISE BRETONNE',        montant: -48.20,   categorie: 'Restaurant',       compte: 'cc1' },
  { id: 't40', date: d(72), libelle: 'NETFLIX',                           montant: -15.99,   categorie: 'Loisirs',          compte: 'cc1' },
  { id: 't41', date: d(73), libelle: 'ORANGE MOBILE — MENSUEL',         montant: -29.99,   categorie: 'Télécom',          compte: 'cc1' },
  { id: 't42', date: d(75), libelle: 'ASSURANCE MMA — AUTO',            montant: -45.80,   categorie: 'Assurance',        compte: 'cc1' },
  { id: 't43', date: d(76), libelle: 'AMAZON PRIME',                     montant: -6.99,    categorie: 'Loisirs',          compte: 'cc1' },
  { id: 't44', date: d(78), libelle: 'CARREFOUR MARKET OBERTHÜR',       montant: -55.34,   categorie: 'Alimentation',    compte: 'cc1' },
  { id: 't45', date: d(80), libelle: 'SNCF ABONNEMENT IDF',             montant: -29.50,   categorie: 'Transport',        compte: 'cc1' },
  { id: 't46', date: d(82), libelle: 'VIREMENT CMB PEL',                montant: -200.00,  categorie: 'Épargne',          compte: 'cc1' },
  { id: 't47', date: d(85), libelle: 'PHARMACIE DU CENTRE',             montant: -31.80,   categorie: 'Santé',            compte: 'cc1' },
  { id: 't48', date: d(87), libelle: 'ZARA HOME RENNES',               montant: -64.90,   categorie: 'Shopping',         compte: 'cc1' },
]

// Budgets mensuels cibles
export const BUDGETS = [
  { categorie: 'Alimentation',  cible: 350, icone: '🛒', couleur: '#16A34A' },
  { categorie: 'Logement',      cible: 850, icone: '🏠', couleur: '#2563EB' },
  { categorie: 'Transport',     cible: 120, icone: '🚌', couleur: '#7C3AED' },
  { categorie: 'Restaurant',    cible: 80,  icone: '🍽️', couleur: '#EA580C' },
  { categorie: 'Loisirs',       cible: 100, icone: '🎬', couleur: '#DB2777' },
  { categorie: 'Shopping',      cible: 100, icone: '🛍️', couleur: '#0891B2' },
  { categorie: 'Santé',         cible: 60,  icone: '💊', couleur: '#DC2626' },
  { categorie: 'Télécom',       cible: 35,  icone: '📱', couleur: '#6B7280' },
  { categorie: 'Assurance',     cible: 50,  icone: '🛡️', couleur: '#92400E' },
  { categorie: 'Épargne',       cible: 300, icone: '💰', couleur: '#059669' },
]

// Évolution mensuelle (6 derniers mois)
export const EVOLUTION_MENSUELLE = [
  { mois: 'Jan', revenus: 3450, depenses: 2180, epargne: 1270 },
  { mois: 'Fév', revenus: 3450, depenses: 2340, epargne: 1110 },
  { mois: 'Mar', revenus: 3450, depenses: 2290, epargne: 1160 },
  { mois: 'Avr', revenus: 3450, depenses: 2420, epargne: 1030 },
  { mois: 'Mai', revenus: 3450, depenses: 2150, epargne: 1300 },
  { mois: 'Jun', revenus: 3450, depenses: 1960, epargne: 1490 },
]

// Projections épargne
export const OBJECTIFS = [
  { id: 'o1', nom: 'Voyage Japon', cible: 4_000, actuel: 2_800, icone: '✈️', deadline: '2026-12-01' },
  { id: 'o2', nom: 'Fonds urgence', cible: 6_000, actuel: 5_200, icone: '🛡️', deadline: '2026-09-01' },
  { id: 'o3', nom: 'Rénovation cuisine', cible: 8_000, actuel: 1_400, icone: '🏠', deadline: '2027-06-01' },
]
