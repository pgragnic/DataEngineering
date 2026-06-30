import {
  PieChart, Pie, Cell, Tooltip, ResponsiveContainer,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Legend,
} from 'recharts'
import { TRANSACTIONS, BUDGETS, EVOLUTION_MENSUELLE } from '../mockData'

const fmt = (n) => new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR' }).format(n)

function getDepensesParCategorie(daysBack = 30) {
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - daysBack)
  const acc = {}
  TRANSACTIONS
    .filter(t => t.montant < 0 && new Date(t.date) >= cutoff && t.categorie !== 'Épargne')
    .forEach(t => {
      acc[t.categorie] = (acc[t.categorie] || 0) + Math.abs(t.montant)
    })
  return Object.entries(acc)
    .map(([name, value]) => ({ name, value: Math.round(value * 100) / 100 }))
    .sort((a, b) => b.value - a.value)
}

const PIE_COLORS = ['#C8102E', '#16A34A', '#2563EB', '#7C3AED', '#EA580C', '#DB2777', '#0891B2', '#6B7280', '#92400E', '#059669']

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-white border border-gray-100 shadow-lg rounded-lg px-3 py-2">
      <p className="text-sm font-semibold text-gray-800">{payload[0].name}</p>
      <p className="text-sm text-gray-600">{fmt(payload[0].value)}</p>
    </div>
  )
}

export default function Statistiques() {
  const depenses30 = getDepensesParCategorie(30)
  const total = depenses30.reduce((a, b) => a + b.value, 0)

  // Budget vs réel pour le mois
  const budgetData = BUDGETS.map(b => {
    const tx = depenses30.find(d => d.name === b.categorie)
    return { name: b.categorie, budget: b.cible, reel: Math.round(tx?.value || 0) }
  })

  return (
    <div className="max-w-7xl mx-auto px-6 py-6 space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Répartition dépenses */}
        <div className="card">
          <h2 className="text-sm font-semibold text-gray-900 mb-4">Répartition des dépenses — 30 derniers jours</h2>
          <div className="flex items-center gap-4">
            <ResponsiveContainer width={180} height={180}>
              <PieChart>
                <Pie data={depenses30} cx="50%" cy="50%" innerRadius={55} outerRadius={80}
                  paddingAngle={2} dataKey="value">
                  {depenses30.map((_, i) => (
                    <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip content={<CustomTooltip />} />
              </PieChart>
            </ResponsiveContainer>
            <div className="flex-1 space-y-1.5 text-xs">
              {depenses30.map((d, i) => (
                <div key={d.name} className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-1.5">
                    <div className="w-2.5 h-2.5 rounded-sm shrink-0" style={{ background: PIE_COLORS[i % PIE_COLORS.length] }} />
                    <span className="text-gray-600">{d.name}</span>
                  </div>
                  <span className="font-semibold text-gray-800">{Math.round(d.value / total * 100)}%</span>
                </div>
              ))}
              <div className="border-t border-gray-100 pt-1.5 flex justify-between font-semibold">
                <span className="text-gray-600">Total</span>
                <span className="text-gray-900">{fmt(total)}</span>
              </div>
            </div>
          </div>
        </div>

        {/* Évolution mensuelle barres */}
        <div className="card">
          <h2 className="text-sm font-semibold text-gray-900 mb-4">Flux mensuels — 6 derniers mois</h2>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={EVOLUTION_MENSUELLE} margin={{ top: 4, right: 4, left: -10, bottom: 0 }} barCategoryGap="30%">
              <CartesianGrid strokeDasharray="3 3" stroke="#F3F4F6" vertical={false} />
              <XAxis dataKey="mois" tick={{ fontSize: 12, fill: '#9CA3AF' }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 11, fill: '#9CA3AF' }} axisLine={false} tickLine={false}
                tickFormatter={v => `${v / 1000}k`} />
              <Tooltip formatter={fmt} />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Bar dataKey="revenus" name="Revenus" fill="#16A34A" radius={[4, 4, 0, 0]} />
              <Bar dataKey="depenses" name="Dépenses" fill="#C8102E" radius={[4, 4, 0, 0]} />
              <Bar dataKey="epargne" name="Épargne nette" fill="#2563EB" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Budget vs réel */}
      <div className="card">
        <h2 className="text-sm font-semibold text-gray-900 mb-4">Budget vs réalisé — mois en cours</h2>
        <div className="space-y-3">
          {budgetData.map(b => {
            const pct = b.budget > 0 ? Math.min((b.reel / b.budget) * 100, 110) : 0
            const depasse = b.reel > b.budget
            return (
              <div key={b.name} className="grid grid-cols-12 gap-3 items-center">
                <span className="col-span-3 text-xs font-medium text-gray-700 truncate">{b.name}</span>
                <div className="col-span-6 h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all duration-700 ${depasse ? 'bg-red-500' : 'bg-emerald-500'}`}
                    style={{ width: `${Math.min(pct, 100)}%` }}
                  />
                </div>
                <div className="col-span-3 flex items-center gap-1 justify-end">
                  <span className={`text-xs font-semibold ${depasse ? 'text-red-600' : 'text-gray-700'}`}>
                    {fmt(b.reel)}
                  </span>
                  <span className="text-xs text-gray-400">/ {fmt(b.budget)}</span>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* Tableau détaillé */}
      <div className="card">
        <h2 className="text-sm font-semibold text-gray-900 mb-4">Détail par catégorie — 30 jours</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100">
                <th className="text-left py-2 text-xs text-gray-500 font-medium">Catégorie</th>
                <th className="text-right py-2 text-xs text-gray-500 font-medium">Dépensé</th>
                <th className="text-right py-2 text-xs text-gray-500 font-medium">Budget</th>
                <th className="text-right py-2 text-xs text-gray-500 font-medium">Écart</th>
                <th className="text-right py-2 text-xs text-gray-500 font-medium">Nb opérations</th>
              </tr>
            </thead>
            <tbody>
              {BUDGETS.map(b => {
                const tx = depenses30.find(d => d.name === b.categorie)
                const reel = tx?.value || 0
                const ecart = b.cible - reel
                const nbOps = TRANSACTIONS.filter(t => t.categorie === b.categorie && t.montant < 0).length
                return (
                  <tr key={b.name} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2.5">{b.icone} {b.categorie}</td>
                    <td className="py-2.5 text-right font-medium">{fmt(reel)}</td>
                    <td className="py-2.5 text-right text-gray-500">{fmt(b.cible)}</td>
                    <td className={`py-2.5 text-right font-medium ${ecart >= 0 ? 'text-emerald-600' : 'text-red-500'}`}>
                      {ecart >= 0 ? '+' : ''}{fmt(ecart)}
                    </td>
                    <td className="py-2.5 text-right text-gray-500">{nbOps}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
