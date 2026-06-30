import { TrendingUp, TrendingDown, Wallet, PiggyBank, Home, CreditCard, ArrowUpRight, ArrowDownRight } from 'lucide-react'
import { COMPTES, TRANSACTIONS, BUDGETS } from '../mockData'
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { EVOLUTION_MENSUELLE } from '../mockData'

const fmt = (n) => new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR' }).format(n)

const ICONES = {
  wallet: Wallet,
  'piggy-bank': PiggyBank,
  home: Home,
  'credit-card': CreditCard,
}

// Dépenses du mois courant (30 derniers jours) par catégorie
function getDepensesMois() {
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - 30)
  return TRANSACTIONS
    .filter(t => t.montant < 0 && new Date(t.date) >= cutoff && t.categorie !== 'Épargne')
    .reduce((acc, t) => {
      acc[t.categorie] = (acc[t.categorie] || 0) + Math.abs(t.montant)
      return acc
    }, {})
}

export default function Dashboard({ onNav }) {
  const depensesMois = getDepensesMois()
  const totalDepenses = Object.values(depensesMois).reduce((a, b) => a + b, 0)
  const totalEpargne = COMPTES.filter(c => c.solde > 0).reduce((a, c) => a + c.solde, 0)

  const recentTx = TRANSACTIONS.slice(0, 6)

  return (
    <div className="max-w-7xl mx-auto px-6 py-6 space-y-6">
      {/* KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card">
          <p className="text-xs text-gray-500 font-medium uppercase tracking-wide">Solde courant</p>
          <p className="kpi-value text-gray-900 mt-1">{fmt(2847.63)}</p>
          <span className="badge bg-green-100 text-green-700 mt-2">
            <TrendingUp size={10} /> +897€ ce mois
          </span>
        </div>
        <div className="card">
          <p className="text-xs text-gray-500 font-medium uppercase tracking-wide">Total épargne</p>
          <p className="kpi-value text-emerald-600 mt-1">{fmt(47_400)}</p>
          <span className="badge bg-emerald-100 text-emerald-700 mt-2">
            <TrendingUp size={10} /> +300€ ce mois
          </span>
        </div>
        <div className="card">
          <p className="text-xs text-gray-500 font-medium uppercase tracking-wide">Dépenses / mois</p>
          <p className="kpi-value text-orange-600 mt-1">{fmt(totalDepenses)}</p>
          <span className="badge bg-orange-100 text-orange-700 mt-2">
            <TrendingDown size={10} /> vs 2290€ mois dernier
          </span>
        </div>
        <div className="card">
          <p className="text-xs text-gray-500 font-medium uppercase tracking-wide">Taux d'épargne</p>
          <p className="kpi-value text-blue-600 mt-1">43%</p>
          <span className="badge bg-blue-100 text-blue-700 mt-2">
            <TrendingUp size={10} /> Objectif 30% atteint
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Graphique évolution */}
        <div className="card lg:col-span-2">
          <h2 className="text-sm font-semibold text-gray-900 mb-4">Évolution revenus / dépenses (6 mois)</h2>
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={EVOLUTION_MENSUELLE} margin={{ top: 4, right: 4, left: -10, bottom: 0 }}>
              <defs>
                <linearGradient id="gRevenu" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#16A34A" stopOpacity={0.2} />
                  <stop offset="95%" stopColor="#16A34A" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="gDepense" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#C8102E" stopOpacity={0.2} />
                  <stop offset="95%" stopColor="#C8102E" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#F3F4F6" />
              <XAxis dataKey="mois" tick={{ fontSize: 12, fill: '#9CA3AF' }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 12, fill: '#9CA3AF' }} axisLine={false} tickLine={false}
                tickFormatter={v => `${v / 1000}k`} />
              <Tooltip formatter={(v) => fmt(v)} labelStyle={{ fontWeight: 600 }} />
              <Area type="monotone" dataKey="revenus" stroke="#16A34A" strokeWidth={2}
                fill="url(#gRevenu)" name="Revenus" dot={false} />
              <Area type="monotone" dataKey="depenses" stroke="#C8102E" strokeWidth={2}
                fill="url(#gDepense)" name="Dépenses" dot={false} />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Comptes résumé */}
        <div className="space-y-3">
          <h2 className="text-sm font-semibold text-gray-900">Mes comptes</h2>
          {COMPTES.map(c => {
            const Icon = ICONES[c.icone]
            const solde = c.solde ?? c.capital_restant
            return (
              <button
                key={c.id}
                onClick={() => onNav('comptes')}
                className="card w-full flex items-center gap-3 hover:shadow-md transition-shadow text-left"
              >
                <div className="w-9 h-9 rounded-lg flex items-center justify-center shrink-0"
                  style={{ background: c.couleur + '18' }}>
                  <Icon size={16} style={{ color: c.couleur }} />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-xs text-gray-500 truncate">{c.type}</p>
                  <p className={`text-sm font-semibold ${solde < 0 ? 'text-purple-700' : 'text-gray-900'}`}>
                    {fmt(solde)}
                  </p>
                </div>
                <ArrowUpRight size={14} className="text-gray-300 shrink-0" />
              </button>
            )
          })}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Transactions récentes */}
        <div className="card">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-sm font-semibold text-gray-900">Dernières opérations</h2>
            <button onClick={() => onNav('comptes')} className="text-xs text-cmb-red font-medium hover:underline">
              Voir tout
            </button>
          </div>
          <div className="space-y-2">
            {recentTx.map(tx => (
              <div key={tx.id} className="flex items-center gap-3 py-2 border-b border-gray-50 last:border-0">
                <div className={`w-8 h-8 rounded-full flex items-center justify-center shrink-0 ${
                  tx.montant > 0 ? 'bg-green-100' : 'bg-red-50'
                }`}>
                  {tx.montant > 0
                    ? <ArrowDownRight size={14} className="text-green-600" />
                    : <ArrowUpRight size={14} className="text-red-500" />
                  }
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-800 truncate">{tx.libelle}</p>
                  <p className="text-xs text-gray-400">{tx.date} · {tx.categorie}</p>
                </div>
                <p className={`text-sm font-semibold shrink-0 ${tx.montant > 0 ? 'text-green-600' : 'text-gray-800'}`}>
                  {tx.montant > 0 ? '+' : ''}{fmt(tx.montant)}
                </p>
              </div>
            ))}
          </div>
        </div>

        {/* Budget du mois */}
        <div className="card">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-sm font-semibold text-gray-900">Budget du mois</h2>
            <button onClick={() => onNav('stats')} className="text-xs text-cmb-red font-medium hover:underline">
              Détails
            </button>
          </div>
          <div className="space-y-3">
            {BUDGETS.slice(0, 6).map(b => {
              const depense = depensesMois[b.categorie] || 0
              const pct = Math.min((depense / b.cible) * 100, 100)
              const depasse = depense > b.cible
              return (
                <div key={b.categorie}>
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs font-medium text-gray-700">{b.icone} {b.categorie}</span>
                    <span className={`text-xs font-semibold ${depasse ? 'text-red-600' : 'text-gray-600'}`}>
                      {fmt(depense)} / {fmt(b.cible)}
                    </span>
                  </div>
                  <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all duration-500 ${depasse ? 'bg-red-500' : 'bg-emerald-500'}`}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </div>
    </div>
  )
}
