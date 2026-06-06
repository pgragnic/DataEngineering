import { useMemo, useState } from "react"
import { AUDITS_TIMELINE, AUDITS_SEMAINE } from "../mockData"
import MapCard from "./MapCard"
import { Car, Train, Bike } from "lucide-react"

// ─── Utilitaires ────────────────────────────────────────────────────────────

function parseMin(str) {
  if (!str || str === "—") return 0
  const s = str.trim()
  if (s.includes("h")) {
    const [h, m] = s.split("h")
    return parseInt(h) * 60 + parseInt(m || 0)
  }
  return parseInt(s) || 0
}

function minToHeure(m) {
  const h = Math.floor(m / 60)
  const min = m % 60
  return `${h}h${String(min).padStart(2, "0")}`
}

function minToTimeInput(m) {
  return `${String(Math.floor(m / 60)).padStart(2, "0")}:${String(m % 60).padStart(2, "0")}`
}

function parseTimeInput(val) {
  const [h, m] = val.split(":").map(Number)
  return h * 60 + (m || 0)
}

function getTrajet(audit, transport) {
  if (transport === "voiture") return audit.trajet?.voiture || "—"
  if (transport === "TC")      return audit.trajetTC || "—"
  if (transport === "velo")    return audit.trajet?.velo || "—"
  return "—"
}

const TRANSPORT_META = {
  voiture: { Icon: Car,   label: "Voiture", bg: "bg-blue-100",   text: "text-blue-600" },
  TC:      { Icon: Train, label: "Transports en commun", bg: "bg-purple-100", text: "text-purple-600" },
  velo:    { Icon: Bike,  label: "Vélo",    bg: "bg-green-100",  text: "text-green-600" },
}
const TRANSPORT_LABEL = { voiture: "Voiture", TC: "Transport en commun", velo: "Vélo" }

const PAUSE_OPTIONS = [
  { label: "30 min", value: 30 },
  { label: "1h",     value: 60 },
  { label: "1h30",   value: 90 },
  { label: "2h",     value: 120 },
]

// ─── Générateur de planning ──────────────────────────────────────────────────

function genererPlanning(audits, transport, config) {
  const { debutMin, finMin, pauseMin, pauseDuree } = config
  const pauseEnd = pauseMin + pauseDuree
  const sorted = [...audits].sort((a, b) => (a.heureMin || 0) - (b.heureMin || 0))

  let cursor = debutMin
  const result = []
  let pauseInserted = false

  for (let i = 0; i < sorted.length; i++) {
    const audit = sorted[i]
    const travelStr = i === 0 ? "—" : getTrajet(audit, transport)
    const travelMins = i === 0 ? 0 : parseMin(travelStr)

    cursor += travelMins

    // Insérer pause déjeuner si la prochaine mission DÉMARRE pendant la plage déjeuner
    if (!pauseInserted && cursor >= pauseMin && cursor < pauseEnd) {
      result.push({
        id: "__pause__",
        isPause: true,
        heureMin: cursor,
        dureeMin: pauseDuree,
        heure: minToHeure(cursor),
        titre: "Déjeuner",
      })
      cursor += pauseDuree
      pauseInserted = true
    } else if (!pauseInserted && cursor >= pauseEnd) {
      pauseInserted = true
    }

    result.push({
      ...audit,
      heureMin: cursor,
      heure: minToHeure(cursor),
      trajetAvant: i > 0 ? { label: travelStr, mins: travelMins } : null,
      hors_plage: cursor + audit.dureeMin > finMin,
    })

    cursor += audit.dureeMin

    // Insérer pause déjeuner après une mission qui s'est terminée passé midi
    if (!pauseInserted && cursor > pauseMin) {
      result.push({
        id: "__pause__",
        isPause: true,
        heureMin: cursor,
        dureeMin: pauseDuree,
        heure: minToHeure(cursor),
        titre: "Déjeuner",
      })
      cursor += pauseDuree
      pauseInserted = true
    }
  }

  return result
}

// ─── Collecte de tous les audits ────────────────────────────────────────────

function getAllAudits() {
  const all = [...AUDITS_TIMELINE]
  Object.values(AUDITS_SEMAINE).forEach(day => {
    day.forEach(a => { if (!all.find(x => x.id === a.id)) all.push(a) })
  })
  return all
}

// ─── Timeline mini ───────────────────────────────────────────────────────────

const PLAN_START = 8 * 60
const PLAN_DURATION = 12 * 60 // 8h → 20h pour couvrir les débordements

function toPercent(m) {
  return Math.max(0, Math.min(100, ((m - PLAN_START) / PLAN_DURATION) * 100))
}

const TYPE_COLOR = { Bureau: "#6366f1", Visite: "#3b82f6", Viso: "#a855f7", pause: "#d1d5db" }

// ─── Composant principal ─────────────────────────────────────────────────────

export default function SelectionView({ selectedIds, transport: initTransport, config: initConfig, onDemarrer, onBack, theme }) {
  const ag = theme === "agile"
  const ar = theme === "aria"
  const brandColor = ag ? "#5D93C1" : ar ? "#4B87F8" : "#2563EB"

  const [transportActif, setTransportActif] = useState(initTransport || "voiture")
  const [debutInput, setDebutInput] = useState(minToTimeInput(initConfig?.debutMin ?? 540))
  const [finInput, setFinInput] = useState(minToTimeInput(initConfig?.finMin ?? 1080))
  const [pauseDuree, setPauseDuree] = useState(initConfig?.pauseDuree ?? 90)

  const config = useMemo(() => ({
    debutMin: parseTimeInput(debutInput),
    finMin: parseTimeInput(finInput),
    pauseMin: 720,
    pauseDuree,
  }), [debutInput, finInput, pauseDuree])

  const allAudits = getAllAudits()
  const selectedAudits = allAudits.filter(a => selectedIds?.has(a.id))

  const planning = useMemo(
    () => genererPlanning(selectedAudits, transportActif, config),
    [selectedAudits, transportActif, config]
  )

  const auditsForMap = planning.filter(p => !p.isPause && p.coords)

  return (
    <div className="min-h-screen flex flex-col">

      {/* Barre haut */}
      <div className="page-bar">
        <button onClick={onBack} className={`text-xs hover:underline flex items-center gap-1 ${ag ? "text-[#1AAED2]" : ar ? "text-[#4B87F8]" : "text-blue-600"}`}>
          ← Retour au planning
        </button>
        <span className="text-xs font-semibold text-gray-700">Planning du jour — {selectedAudits.length} mission{selectedAudits.length > 1 ? "s" : ""}</span>
        <div className="w-36" />
      </div>

      {/* Bandeau config */}
      <div className="bg-white border-b border-gray-100 px-6 py-2.5 flex items-center gap-6 flex-wrap">
        {/* Transport */}
        <div className="flex items-center gap-1.5">
          {Object.entries(TRANSPORT_META).map(([id, opt]) => (
            <button key={id} onClick={() => setTransportActif(id)}
              className={`flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-lg border transition-all ${transportActif === id ? "text-white border-transparent" : "border-gray-200 text-gray-600 hover:bg-gray-50"}`}
              style={transportActif === id ? { backgroundColor: brandColor } : {}}>
              <div className={`w-5 h-5 rounded flex items-center justify-center shrink-0 ${transportActif === id ? "bg-white/20" : opt.bg}`}>
                <opt.Icon size={11} className={transportActif === id ? "text-white" : opt.text} />
              </div>
              {opt.label.split(" ")[0]}
            </button>
          ))}
        </div>

        <div className="h-4 w-px bg-gray-200" />

        {/* Début */}
        <label className="flex items-center gap-1.5 text-xs text-gray-600">
          Début
          <input type="time" value={debutInput} onChange={e => setDebutInput(e.target.value)}
            className="border border-gray-200 rounded px-2 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400" />
        </label>

        {/* Pause */}
        <label className="flex items-center gap-1.5 text-xs text-gray-600">
          Déjeuner
          <select value={pauseDuree} onChange={e => setPauseDuree(Number(e.target.value))}
            className="border border-gray-200 rounded px-2 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white">
            {PAUSE_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
        </label>

        {/* Fin */}
        <label className="flex items-center gap-1.5 text-xs text-gray-600">
          Fin
          <input type="time" value={finInput} onChange={e => setFinInput(e.target.value)}
            className="border border-gray-200 rounded px-2 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400" />
        </label>
      </div>

      <div className="page-content">
        {selectedAudits.length === 0 ? (
          <div className="text-center text-gray-400 text-sm py-16">
            Aucune mission sélectionnée.
            <br />
            <button onClick={onBack} className={`mt-3 text-xs underline ${ag ? "text-[#1AAED2]" : ar ? "text-[#4B87F8]" : "text-blue-600"}`}>Retourner au dashboard</button>
          </div>
        ) : (
          <div className="grid-12">

            {/* Planning liste */}
            <div className="col-span-8 min-w-0">

              {/* Mini timeline */}
              <div className="card mb-4">
                <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-3">Aperçu journée</div>
                <div className="relative h-10 bg-gray-100 rounded-lg overflow-hidden">
                  {/* Bloc pause */}
                  <div className="absolute top-0 bottom-0 bg-gray-300/70"
                    style={{ left: `${toPercent(config.pauseMin)}%`, width: `${(config.pauseDuree / PLAN_DURATION) * 100}%` }}
                    title="Déjeuner" />
                  {/* Missions */}
                  {planning.filter(p => !p.isPause).map(audit => (
                    <div key={audit.id}
                      className={`absolute top-1 bottom-1 rounded text-[8px] text-white font-semibold flex items-center px-1 overflow-hidden ${audit.hors_plage ? "opacity-50 ring-1 ring-red-400" : ""}`}
                      style={{ left: `${toPercent(audit.heureMin)}%`, width: `${(audit.dureeMin / PLAN_DURATION) * 100}%`, backgroundColor: TYPE_COLOR[audit.type] || "#6b7280" }}
                      title={audit.titre}>
                      {audit.heure}
                    </div>
                  ))}
                  {/* Début/Fin markers */}
                  <div className="absolute top-0 bottom-0 w-0.5 bg-green-400" style={{ left: `${toPercent(config.debutMin)}%` }} />
                  <div className="absolute top-0 bottom-0 w-0.5 bg-red-400" style={{ left: `${toPercent(config.finMin)}%` }} />
                </div>
                <div className="flex justify-between mt-1">
                  <span className="text-[9px] text-green-600">{minToHeure(config.debutMin)}</span>
                  <span className="text-[9px] text-gray-400">🍽 {minToHeure(config.pauseMin)}</span>
                  <span className="text-[9px] text-red-500">{minToHeure(config.finMin)}</span>
                </div>
              </div>

              {/* Liste avec flèches de trajet */}
              <div className="space-y-0">
                {planning.map((item, idx) => {
                  if (item.isPause) {
                    return (
                      <div key="pause" className="flex items-center gap-3 py-2 px-4">
                        <div className="w-0.5 bg-gray-200 self-stretch mx-1.5" />
                        <div className="flex-1 bg-gray-100 border border-gray-200 rounded-lg px-3 py-2 flex items-center gap-2">
                          <span className="text-sm">🍽</span>
                          <div>
                            <div className="text-xs font-medium text-gray-600">Déjeuner</div>
                            <div className="text-[10px] text-gray-400">{item.heure} · {item.dureeMin} min</div>
                          </div>
                        </div>
                      </div>
                    )
                  }

                  // Statut dynamique basé sur l'heure courante
                  const nowMin = new Date().getHours() * 60 + new Date().getMinutes()
                  const displayStatut = (item.heureMin + item.dureeMin) < nowMin
                    ? "TERMINÉ"
                    : item.heureMin <= nowMin + 45
                      ? "PROCHAIN"
                      : "PLANIFIÉ"

                  return (
                    <div key={item.id}>
                      {/* Flèche de trajet */}
                      {item.trajetAvant && item.trajetAvant.mins > 0 && (
                        <div className="flex items-center gap-3 py-1.5 px-3">
                          <div className="w-0.5 bg-gray-200 self-stretch mx-2" />
                          <div className={`text-[10px] flex items-center gap-1.5 ${ag ? "text-[#1AAED2]" : ar ? "text-[#4B87F8]" : "text-blue-500"}`}>
                            {(() => { const m = TRANSPORT_META[transportActif]; return m ? <m.Icon size={11} className={m.text} /> : null })()}
                            <span className="font-medium">{item.trajetAvant.label}</span>
                            <span className="text-gray-400">→ {item.site}</span>
                          </div>
                        </div>
                      )}

                      {/* Carte mission */}
                      <div className={`rounded-xl border p-4 flex items-start gap-4 shadow-sm transition-opacity ${
                        item.hors_plage ? "border-red-300 bg-red-50" :
                        displayStatut === "TERMINÉ" ? "border-green-200 bg-green-50" :
                        displayStatut === "PROCHAIN"
                          ? "bg-white border-green-400 ring-1 ring-green-300"
                          : "bg-white border-gray-200"
                      }`}>
                        <div className="text-center min-w-[52px]">
                          <div className={`font-mono text-sm font-bold ${item.hors_plage ? "text-red-600" : displayStatut === "TERMINÉ" ? "text-green-600" : ag ? "text-[#5D93C1]" : ar ? "text-[#4B87F8]" : "text-blue-900"}`}>{item.heure}</div>
                          <div className="text-[9px] text-gray-400">{item.dureeMin} min</div>
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className={`font-semibold text-sm truncate ${displayStatut === "TERMINÉ" ? "text-green-700" : "text-gray-900"}`}>{item.titre}</div>
                          <div className="text-xs text-gray-500 truncate">{item.site}</div>
                          {item.hors_plage && <div className="text-[10px] text-red-600 font-medium mt-0.5">⚠ Dépasse la fin de journée</div>}
                        </div>
                        {displayStatut === "TERMINÉ" ? (
                          <span className="text-[10px] font-semibold text-green-600 shrink-0 px-2 py-1.5">✓ Terminé</span>
                        ) : (
                          <button
                            onClick={() => onDemarrer(item)}
                            className={`text-xs font-semibold px-3 py-1.5 rounded-lg shrink-0 transition-colors ${
                              displayStatut === "PROCHAIN"
                                ? "bg-brand-emerald text-white hover:bg-brand shadow-sm"
                                : "bg-brand text-white hover:bg-brand-cyan"
                            }`}
                          >
                            Démarrer →
                          </button>
                        )}
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>

            {/* Carte */}
            <div className="col-span-4">
              <h2 className="text-xs font-bold text-gray-400 uppercase tracking-widest mb-3">Parcours sélectionné</h2>
              <MapCard audits={auditsForMap} theme={theme} compact transport={transportActif} />
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
