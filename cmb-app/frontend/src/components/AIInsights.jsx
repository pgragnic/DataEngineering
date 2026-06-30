import { useState } from 'react'
import { Sparkles, TrendingUp, TrendingDown, AlertCircle, Lightbulb, Loader2, RefreshCw } from 'lucide-react'
import { TRANSACTIONS, BUDGETS, EVOLUTION_MENSUELLE, OBJECTIFS, USER } from '../mockData'
import { analyserFinances } from '../api'

const fmt = (n) => new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR' }).format(n)

function buildPayload() {
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - 30)
  const depenses = {}
  TRANSACTIONS
    .filter(t => t.montant < 0 && new Date(t.date) >= cutoff && t.categorie !== 'Épargne')
    .forEach(t => { depenses[t.categorie] = (depenses[t.categorie] || 0) + Math.abs(t.montant) })

  return {
    user: USER.nom,
    solde_courant: 2847.63,
    revenus_mensuels: 3450,
    depenses_mois: depenses,
    budgets: Object.fromEntries(BUDGETS.map(b => [b.categorie, b.cible])),
    evolution: EVOLUTION_MENSUELLE,
    objectifs: OBJECTIFS.map(o => ({ nom: o.nom, actuel: o.actuel, cible: o.cible, deadline: o.deadline })),
    epargne_totale: 47400,
  }
}

// Insights statiques de secours si le backend est absent
const INSIGHTS_FALLBACK = {
  score: 78,
  resume: "Votre situation financière est solide avec un taux d'épargne de 43%, bien au-dessus de la moyenne française (17%). Vos dépenses alimentaires ont légèrement dépassé votre budget ce mois, mais votre épargne globale progresse positivement.",
  points_forts: [
    "Taux d'épargne de 43% — excellent vs moyenne nationale (17%)",
    "Crédit immobilier bien géré, mensualité stable et taux favorable",
    "Objectif Fonds d'urgence atteint à 87% — quasi complété",
  ],
  points_vigilance: [
    "Alimentation : 204€ dépensé vs 350€ de budget — bien maîtrisé",
    "Loisirs : cumul abonnements (Netflix + Spotify + Amazon) = 33€/mois",
    "Shopping : 2 achats impulsifs en 30 jours (Zara + Amazon 127€)",
  ],
  recommandations: [
    "Regrouper vos abonnements sur une date unique pour mieux les contrôler",
    "Automatiser un virement de 400€/mois vers votre Livret DD pour atteindre votre objectif Voyage Japon avant décembre",
    "Le PEL arrivera à maturité en 2024 — contacter votre conseiller Fabien Le Bras pour explorer les options de rachat ou de prolongation",
    "Votre taux immobilier (1,85%) est avantageux — pas de renégociation nécessaire dans l'immédiat",
  ],
}

export default function AIInsights() {
  const [insights, setInsights] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [usedFallback, setUsedFallback] = useState(false)

  async function lancer() {
    setLoading(true)
    setError(null)
    setUsedFallback(false)
    try {
      const payload = buildPayload()
      const result = await analyserFinances(payload)
      setInsights(result)
    } catch (e) {
      // Fallback sur données statiques si backend absent
      setInsights(INSIGHTS_FALLBACK)
      setUsedFallback(true)
    } finally {
      setLoading(false)
    }
  }

  const scoreColor = insights?.score >= 80 ? 'text-emerald-600' : insights?.score >= 60 ? 'text-orange-500' : 'text-red-600'
  const scoreBg = insights?.score >= 80 ? 'bg-emerald-50 border-emerald-200' : insights?.score >= 60 ? 'bg-orange-50 border-orange-200' : 'bg-red-50 border-red-200'

  return (
    <div className="max-w-5xl mx-auto px-6 py-6 space-y-6">
      {/* Header */}
      <div className="card bg-gradient-to-r from-cmb-red to-red-700 text-white">
        <div className="flex items-start gap-4">
          <div className="w-12 h-12 bg-white/20 rounded-xl flex items-center justify-center shrink-0">
            <Sparkles size={22} className="text-white" />
          </div>
          <div className="flex-1">
            <h1 className="text-xl font-bold">Analyse IA — Claude</h1>
            <p className="text-red-100 text-sm mt-1">
              Claude analyse vos données bancaires pour vous donner des recommandations personnalisées,
              identifier vos tendances de dépenses et vous aider à atteindre vos objectifs d'épargne.
            </p>
          </div>
          {!loading && (
            <button
              onClick={lancer}
              className="flex items-center gap-2 bg-white text-cmb-red px-4 py-2 rounded-lg font-semibold text-sm hover:bg-red-50 transition-colors shrink-0"
            >
              {insights ? <RefreshCw size={14} /> : <Sparkles size={14} />}
              {insights ? 'Relancer' : 'Analyser mes finances'}
            </button>
          )}
        </div>
      </div>

      {/* Loading */}
      {loading && (
        <div className="card flex flex-col items-center justify-center py-16 gap-4">
          <Loader2 size={36} className="text-cmb-red animate-spin" />
          <div className="text-center">
            <p className="font-semibold text-gray-800">Analyse en cours…</p>
            <p className="text-sm text-gray-400 mt-1">Claude examine vos transactions, budgets et objectifs</p>
          </div>
        </div>
      )}

      {/* Résultats */}
      {!loading && insights && (
        <>
          {usedFallback && (
            <div className="flex items-center gap-2 px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg text-xs text-amber-700">
              <AlertCircle size={14} />
              Mode démonstration — backend non disponible. Les insights ci-dessous sont des exemples représentatifs.
            </div>
          )}

          {/* Score santé */}
          <div className={`card border ${scoreBg} flex items-center gap-6`}>
            <div className="text-center shrink-0">
              <p className={`text-5xl font-bold ${scoreColor}`}>{insights.score}</p>
              <p className="text-xs text-gray-500 mt-1">/ 100</p>
            </div>
            <div>
              <p className="font-semibold text-gray-900 text-base">Score de santé financière</p>
              <p className="text-sm text-gray-600 mt-1">{insights.resume}</p>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Points forts */}
            <div className="card">
              <div className="flex items-center gap-2 mb-4">
                <div className="w-7 h-7 rounded-lg bg-emerald-100 flex items-center justify-center">
                  <TrendingUp size={14} className="text-emerald-600" />
                </div>
                <h2 className="text-sm font-semibold text-gray-900">Points forts</h2>
              </div>
              <ul className="space-y-3">
                {insights.points_forts.map((p, i) => (
                  <li key={i} className="flex items-start gap-2.5">
                    <span className="w-5 h-5 rounded-full bg-emerald-100 text-emerald-700 text-xs font-bold flex items-center justify-center shrink-0 mt-0.5">
                      {i + 1}
                    </span>
                    <p className="text-sm text-gray-700">{p}</p>
                  </li>
                ))}
              </ul>
            </div>

            {/* Points de vigilance */}
            <div className="card">
              <div className="flex items-center gap-2 mb-4">
                <div className="w-7 h-7 rounded-lg bg-orange-100 flex items-center justify-center">
                  <TrendingDown size={14} className="text-orange-600" />
                </div>
                <h2 className="text-sm font-semibold text-gray-900">Points de vigilance</h2>
              </div>
              <ul className="space-y-3">
                {insights.points_vigilance.map((p, i) => (
                  <li key={i} className="flex items-start gap-2.5">
                    <span className="w-5 h-5 rounded-full bg-orange-100 text-orange-700 text-xs font-bold flex items-center justify-center shrink-0 mt-0.5">
                      !
                    </span>
                    <p className="text-sm text-gray-700">{p}</p>
                  </li>
                ))}
              </ul>
            </div>
          </div>

          {/* Recommandations */}
          <div className="card">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-7 h-7 rounded-lg bg-blue-100 flex items-center justify-center">
                <Lightbulb size={14} className="text-blue-600" />
              </div>
              <h2 className="text-sm font-semibold text-gray-900">Recommandations personnalisées</h2>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {insights.recommandations.map((r, i) => (
                <div key={i} className="flex items-start gap-3 p-3 bg-blue-50 rounded-lg">
                  <span className="w-6 h-6 rounded-full bg-blue-500 text-white text-xs font-bold flex items-center justify-center shrink-0 mt-0.5">
                    {i + 1}
                  </span>
                  <p className="text-sm text-gray-700">{r}</p>
                </div>
              ))}
            </div>
          </div>

          {/* Disclaimer */}
          <p className="text-xs text-gray-400 text-center">
            Analyse générée par Claude (Anthropic) sur la base de vos données de compte.
            Ces informations sont indicatives et ne constituent pas un conseil financier personnalisé.
            Consultez votre conseiller {USER.conseiller} pour toute décision importante.
          </p>
        </>
      )}

      {/* État initial */}
      {!loading && !insights && (
        <div className="card text-center py-16">
          <Sparkles size={48} className="mx-auto text-gray-200 mb-4" />
          <p className="text-gray-500 font-medium">Cliquez sur "Analyser mes finances" pour obtenir</p>
          <p className="text-gray-400 text-sm mt-1">des insights personnalisés générés par l'IA Claude</p>
        </div>
      )}
    </div>
  )
}
