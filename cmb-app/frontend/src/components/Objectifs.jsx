import { OBJECTIFS } from '../mockData'
import { Target, CalendarDays, TrendingUp } from 'lucide-react'

const fmt = (n) => new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR' }).format(n)

function joursRestants(deadline) {
  const diff = new Date(deadline) - new Date()
  return Math.max(0, Math.ceil(diff / (1000 * 60 * 60 * 24)))
}

function mensualiteNecessaire(cible, actuel, deadline) {
  const mois = Math.max(1, Math.ceil(joursRestants(deadline) / 30))
  return Math.max(0, (cible - actuel) / mois)
}

export default function Objectifs() {
  return (
    <div className="max-w-5xl mx-auto px-6 py-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-gray-900">Mes objectifs d'épargne</h1>
        <button className="btn-primary flex items-center gap-2">
          <Target size={14} />
          Nouvel objectif
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {OBJECTIFS.map(obj => {
          const pct = Math.min((obj.actuel / obj.cible) * 100, 100)
          const jours = joursRestants(obj.deadline)
          const mensualite = mensualiteNecessaire(obj.cible, obj.actuel, obj.deadline)
          const couleurs = [
            { bg: 'bg-blue-50', ring: 'ring-blue-200', bar: 'bg-blue-500', text: 'text-blue-700' },
            { bg: 'bg-emerald-50', ring: 'ring-emerald-200', bar: 'bg-emerald-500', text: 'text-emerald-700' },
            { bg: 'bg-orange-50', ring: 'ring-orange-200', bar: 'bg-orange-500', text: 'text-orange-700' },
          ]
          const c = couleurs[OBJECTIFS.indexOf(obj) % couleurs.length]

          return (
            <div key={obj.id} className={`card ring-1 ${c.ring} ${c.bg}`}>
              <div className="flex items-center justify-between mb-3">
                <span className="text-3xl">{obj.icone}</span>
                <span className={`text-2xl font-bold ${c.text}`}>{Math.round(pct)}%</span>
              </div>
              <h3 className="text-base font-semibold text-gray-900 mb-1">{obj.nom}</h3>

              {/* Barre progression */}
              <div className="h-2.5 bg-white rounded-full overflow-hidden my-3">
                <div
                  className={`h-full rounded-full ${c.bar} transition-all duration-700`}
                  style={{ width: `${pct}%` }}
                />
              </div>

              <div className="flex justify-between text-sm mb-4">
                <span className="font-semibold text-gray-900">{fmt(obj.actuel)}</span>
                <span className="text-gray-400">/ {fmt(obj.cible)}</span>
              </div>

              <div className="space-y-1.5 text-xs text-gray-500 border-t border-white pt-3">
                <div className="flex items-center gap-1.5">
                  <CalendarDays size={12} />
                  <span>Échéance dans <strong className="text-gray-700">{jours} jours</strong></span>
                </div>
                <div className="flex items-center gap-1.5">
                  <TrendingUp size={12} />
                  <span>Effort mensuel : <strong className={c.text}>{fmt(mensualite)}</strong></span>
                </div>
                <div className="flex items-center gap-1.5">
                  <Target size={12} />
                  <span>Reste à épargner : <strong className="text-gray-700">{fmt(obj.cible - obj.actuel)}</strong></span>
                </div>
              </div>
            </div>
          )
        })}
      </div>

      {/* Conseil */}
      <div className="card border-l-4 border-cmb-red bg-red-50">
        <div className="flex gap-3">
          <TrendingUp size={20} className="text-cmb-red shrink-0 mt-0.5" />
          <div>
            <p className="text-sm font-semibold text-gray-900">Conseil CMB</p>
            <p className="text-sm text-gray-600 mt-1">
              Votre objectif "Voyage Japon" est à 70%. En maintenant un versement de <strong>400€/mois</strong>,
              vous l'atteindrez 2 mois avant l'échéance. Pensez à activer le virement automatique depuis votre
              compte courant vers votre Livret DD.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
