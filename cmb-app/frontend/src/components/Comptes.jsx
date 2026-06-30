import { useState } from 'react'
import { Wallet, PiggyBank, Home, CreditCard, ArrowUpRight, ArrowDownRight, Search, Wifi } from 'lucide-react'
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
  Autre: 'bg-gray-100 text-gray-600',
}

function typeToIcone(type = '') {
  const t = type.toLowerCase()
  if (t.includes('courant') || t.includes('checking')) return 'wallet'
  if (t.includes('épargne') || t.includes('livret') || t.includes('savings')) return 'piggy-bank'
  if (t.includes('crédit') || t.includes('loan')) return 'credit-card'
  return 'home'
}
function typeToCouleur(type = '') {
  const t = type.toLowerCase()
  if (t.includes('courant') || t.includes('checking')) return '#C8102E'
  if (t.includes('épargne') || t.includes('livret') || t.includes('savings')) return '#059669'
  if (t.includes('crédit') || t.includes('loan')) return '#9333EA'
  return '#2563EB'
}

export default function Comptes({ connectionInfo }) {
  const isConnected = !!connectionInfo

  // Données : réelles ou mock
  const rawComptes = isConnected
    ? connectionInfo.accounts.map(a => ({
        id: a.id,
        type: a.name || a.type,
        numero: `****${String(a.number || '').slice(-4) || '0000'}`,
        solde: a.balance,
        iban: a.iban || '',
        couleur: typeToCouleur(a.type),
        icone: typeToIcone(a.type),
        connector_name: a.connector_name,
      }))
    : COMPTES

  const rawTx = isConnected ? connectionInfo.transactions : TRANSACTIONS

  const [selectedId, setSelectedId] = useState(rawComptes[0]?.id || 'cc1')
  const [search, setSearch] = useState('')
  const [catFilter, setCatFilter] = useState('Toutes')

  const compte = rawComptes.find(c => c.id === selectedId) || rawComptes[0]
  const comptesTx = isConnected
    ? rawTx.filter(tx => tx.account_id === selectedId || rawComptes.length === 1)
    : rawTx.filter(tx => tx.compte === selectedId)

  const txFiltered = comptesTx.filter(tx => {
    if (catFilter !== 'Toutes' && tx.categorie !== catFilter) return false
    if (search && !tx.libelle.toLowerCase().includes(search.toLowerCase())) return false
    return true
  })
  const categories = ['Toutes', ...new Set(comptesTx.map(t => t.categorie))]

  const Icon = ICONES[compte?.icone] || Wallet

  return (
    <div className="max-w-7xl mx-auto px-6 py-6">
      {isConnected && (
        <div className="flex items-center gap-2 mb-4 px-3 py-2 bg-emerald-50 border border-emerald-200 rounded-lg text-xs text-emerald-700">
          <Wifi size={13} />
          Données en temps réel — {connectionInfo.accounts.length} compte(s) CMB connecté(s)
        </div>
      )}
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Liste */}
        <div className="space-y-3">
          <h2 className="text-sm font-semibold text-gray-500 uppercase tracking-wide px-1">Mes comptes</h2>
          {rawComptes.map(c => {
            const CI = ICONES[c.icone] || Wallet
            const solde = c.solde ?? c.capital_restant
            const active = c.id === selectedId
            return (
              <button
                key={c.id}
                onClick={() => { setSelectedId(c.id); setCatFilter('Toutes'); setSearch('') }}
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

        {/* Détail */}
        <div className="lg:col-span-3 space-y-4">
          {compte && (
            <div className="card flex items-center gap-4" style={{ borderLeft: `4px solid ${compte.couleur}` }}>
              <div className="w-12 h-12 rounded-xl flex items-center justify-center"
                style={{ background: compte.couleur + '18' }}>
                <Icon size={22} style={{ color: compte.couleur }} />
              </div>
              <div className="flex-1">
                <p className="text-lg font-bold text-gray-900">{fmt(compte.solde ?? compte.capital_restant)}</p>
                <p className="text-sm text-gray-500">{compte.type} · {compte.numero}</p>
                {compte.iban && <p className="text-xs font-mono text-gray-400 mt-0.5">{compte.iban}</p>}
                {compte.connector_name && <p className="text-xs text-gray-400">{compte.connector_name}</p>}
              </div>
            </div>
          )}

          {comptesTx.length > 0 ? (
            <>
              <div className="flex flex-wrap gap-2 items-center">
                <div className="relative flex-1 min-w-40">
                  <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                  <input
                    type="text"
                    placeholder="Rechercher…"
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    className="w-full pl-8 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-cmb-red/30"
                  />
                </div>
                <div className="flex flex-wrap gap-1">
                  {categories.map(cat => (
                    <button key={cat} onClick={() => setCatFilter(cat)}
                      className={`px-2.5 py-1 rounded-full text-xs font-medium transition-colors ${
                        catFilter === cat ? 'bg-cmb-red text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                      }`}>{cat}</button>
                  ))}
                </div>
              </div>

              <div className="card space-y-1 max-h-[60vh] overflow-y-auto scrollbar-thin">
                {txFiltered.length === 0 && (
                  <p className="text-sm text-gray-400 text-center py-8">Aucune opération trouvée</p>
                )}
                {txFiltered.map((tx, i) => (
                  <div key={tx.id || i} className="flex items-center gap-3 py-2.5 border-b border-gray-50 last:border-0">
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
          ) : (
            <div className="card text-center text-gray-400 py-12">
              <PiggyBank size={40} className="mx-auto mb-3 opacity-30" />
              <p className="text-sm">Aucune transaction disponible pour ce compte.</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
