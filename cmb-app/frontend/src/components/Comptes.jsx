import { useState } from 'react'
import { Wallet, PiggyBank, Home, CreditCard, ArrowUpRight, ArrowDownRight, Search } from 'lucide-react'
import { COMPTES, TRANSACTIONS } from '../mockData'

const fmt = (n) => new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR' }).format(n)
const ICONES = { wallet: Wallet, 'piggy-bank': PiggyBank, home: Home, 'credit-card': CreditCard }

const CAT_COLORS = {
  Revenus: 'bg-green-100 text-green-700',
  Alimentation: 'bg-lime-100 text-lime-700',
  Logement: 'bg-blue-100 text-blue-700',
  Transport: 'bg-violet-100 text-violet-700',
  Restaurant: 'bg-orange-100 text-orange-700',
  Loisirs: 'bg-pink-100 text-pink-700',
  Shopping: 'bg-cyan-100 text-cyan-700',
  Santé: 'bg-red-100 text-red-700',
  Télécom: 'bg-gray-100 text-gray-700',
  Assurance: 'bg-amber-100 text-amber-700',
  Épargne: 'bg-emerald-100 text-emerald-700',
}

export default function Comptes() {
  const [selectedCompte, setSelectedCompte] = useState('cc1')
  const [search, setSearch] = useState('')
  const [catFilter, setCatFilter] = useState('Toutes')

  const compte = COMPTES.find(c => c.id === selectedCompte)
  const txFiltered = TRANSACTIONS.filter(tx => {
    if (tx.compte !== selectedCompte) return false
    if (catFilter !== 'Toutes' && tx.categorie !== catFilter) return false
    if (search && !tx.libelle.toLowerCase().includes(search.toLowerCase())) return false
    return true
  })

  const categories = ['Toutes', ...new Set(TRANSACTIONS.filter(t => t.compte === selectedCompte).map(t => t.categorie))]
  const Icon = ICONES[compte.icone]

  return (
    <div className="max-w-7xl mx-auto px-6 py-6">
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Liste comptes */}
        <div className="space-y-3">
          <h2 className="text-sm font-semibold text-gray-500 uppercase tracking-wide px-1">Mes comptes</h2>
          {COMPTES.map(c => {
            const CI = ICONES[c.icone]
            const solde = c.solde ?? c.capital_restant
            const active = c.id === selectedCompte
            return (
              <button
                key={c.id}
                onClick={() => setSelectedCompte(c.id)}
                className={`w-full card text-left flex items-center gap-3 transition-all ${
                  active ? 'ring-2 ring-cmb-red shadow-md' : 'hover:shadow-md'
                }`}
              >
                <div className="w-10 h-10 rounded-lg flex items-center justify-center shrink-0"
                  style={{ background: c.couleur + '20' }}>
                  <CI size={18} style={{ color: c.couleur }} />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-xs text-gray-500 truncate">{c.type}</p>
                  <p className="text-xs text-gray-400">{c.numero}</p>
                  <p className={`text-base font-bold mt-0.5 ${solde < 0 ? 'text-purple-700' : 'text-gray-900'}`}>
                    {fmt(solde)}
                  </p>
                </div>
              </button>
            )
          })}
        </div>

        {/* Détail compte */}
        <div className="lg:col-span-3 space-y-4">
          {/* En-tête compte */}
          <div className="card flex items-center gap-4" style={{ borderLeft: `4px solid ${compte.couleur}` }}>
            <div className="w-12 h-12 rounded-xl flex items-center justify-center"
              style={{ background: compte.couleur + '18' }}>
              <Icon size={22} style={{ color: compte.couleur }} />
            </div>
            <div className="flex-1">
              <p className="text-lg font-bold text-gray-900">{fmt(compte.solde ?? compte.capital_restant)}</p>
              <p className="text-sm text-gray-500">{compte.type} · {compte.numero}</p>
              {compte.taux && <p className="text-xs text-gray-400">Taux : {compte.taux}% / an</p>}
              {compte.mensualite && <p className="text-xs text-gray-400">Mensualité : {fmt(compte.mensualite)} · Échéance : {compte.echeance}</p>}
            </div>
            {compte.iban && (
              <div className="text-right hidden sm:block">
                <p className="text-xs text-gray-400">IBAN</p>
                <p className="text-xs font-mono text-gray-600">{compte.iban}</p>
              </div>
            )}
          </div>

          {/* Opérations */}
          {selectedCompte === 'cc1' && (
            <>
              <div className="flex flex-wrap gap-2 items-center">
                <div className="relative flex-1 min-w-40">
                  <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                  <input
                    type="text"
                    placeholder="Rechercher une opération…"
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    className="w-full pl-8 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-cmb-red/30"
                  />
                </div>
                <div className="flex flex-wrap gap-1">
                  {categories.map(cat => (
                    <button
                      key={cat}
                      onClick={() => setCatFilter(cat)}
                      className={`px-2.5 py-1 rounded-full text-xs font-medium transition-colors ${
                        catFilter === cat
                          ? 'bg-cmb-red text-white'
                          : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                      }`}
                    >
                      {cat}
                    </button>
                  ))}
                </div>
              </div>

              <div className="card space-y-1 max-h-[60vh] overflow-y-auto scrollbar-thin">
                {txFiltered.length === 0 && (
                  <p className="text-sm text-gray-400 text-center py-8">Aucune opération trouvée</p>
                )}
                {txFiltered.map(tx => (
                  <div key={tx.id} className="flex items-center gap-3 py-2.5 border-b border-gray-50 last:border-0">
                    <div className={`w-8 h-8 rounded-full flex items-center justify-center shrink-0 ${
                      tx.montant > 0 ? 'bg-green-100' : 'bg-red-50'
                    }`}>
                      {tx.montant > 0
                        ? <ArrowDownRight size={14} className="text-green-600" />
                        : <ArrowUpRight size={14} className="text-red-400" />
                      }
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-gray-800 truncate">{tx.libelle}</p>
                      <p className="text-xs text-gray-400">{tx.date}</p>
                    </div>
                    <span className={`badge ${CAT_COLORS[tx.categorie] || 'bg-gray-100 text-gray-600'} shrink-0`}>
                      {tx.categorie}
                    </span>
                    <p className={`text-sm font-semibold w-24 text-right shrink-0 ${
                      tx.montant > 0 ? 'text-green-600' : 'text-gray-800'
                    }`}>
                      {tx.montant > 0 ? '+' : ''}{fmt(tx.montant)}
                    </p>
                  </div>
                ))}
              </div>
            </>
          )}

          {selectedCompte !== 'cc1' && (
            <div className="card text-center text-gray-400 py-12">
              <PiggyBank size={40} className="mx-auto mb-3 opacity-30" />
              <p className="text-sm">Les mouvements de ce compte ne sont pas disponibles en mode démo.</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
