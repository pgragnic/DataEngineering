import { useState } from "react"
import { AUDITS_TIMELINE, AUDITS_EDF, AUDITS_TOTAL } from "../mockData"
import MapCard from "./MapCard"
import { Car, Train, Bike, CalendarDays } from "lucide-react"

const TRANSPORT_OPTIONS = [
  { id: "voiture", Icon: Car,   label: "Voiture", bg: "bg-blue-100",   text: "text-blue-600" },
  { id: "TC",      Icon: Train, label: "Transports en commun", bg: "bg-purple-100", text: "text-purple-600" },
  { id: "velo",    Icon: Bike,  label: "Vélo",    bg: "bg-green-100",  text: "text-green-600" },
]

const PAUSE_OPTIONS = [
  { label: "30 min", value: 30 },
  { label: "1h",     value: 60 },
  { label: "1h30",   value: 90 },
  { label: "2h",     value: 120 },
]

function getTrajetLabel(audit, transport) {
  if (transport === "voiture") return audit.trajet?.voiture || "—"
  if (transport === "TC")      return audit.trajetTC || "—"
  if (transport === "velo")    return audit.trajet?.velo || "—"
  return "—"
}

function parseTimeInput(val) {
  const [h, m] = val.split(":").map(Number)
  return h * 60 + (m || 0)
}

export default function PlanningOverlay({ onClose, onConfirm, selectedClient, theme }) {
  const ag = theme === "agile"
  const ar = theme === "aria"
  const brandColor = ag ? "#5D93C1" : ar ? "#4B87F8" : "#2563EB"

  const [transport, setTransport] = useState("voiture")
  const [selectedIds, setSelectedIds] = useState(new Set())
  const [debutInput, setDebutInput] = useState("09:00")
  const [finInput, setFinInput] = useState("18:00")
  const [pauseDuree, setPauseDuree] = useState(90)

  const audits = selectedClient?.id === "edf" ? AUDITS_EDF
    : selectedClient?.id === "total" ? AUDITS_TOTAL
    : AUDITS_TIMELINE

  const n = selectedIds.size
  const selectedAudits = audits.filter(a => selectedIds.has(a.id))

  function toggleAudit(id) {
    setSelectedIds(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function handleConfirm() {
    const config = {
      debutMin: parseTimeInput(debutInput),
      finMin: parseTimeInput(finInput),
      pauseMin: 720,
      pauseDuree,
    }
    onConfirm(selectedIds, transport, config)
  }

  return (
    <div className="min-h-screen flex flex-col">

      {/* Titre de page */}
      <div className="page-bar">
        <h1 className={`text-base font-bold flex items-center gap-2 ${ag ? "text-[#5D93C1]" : ar ? "text-[#4B87F8]" : "text-gray-800"}`}><CalendarDays size={16} />Générez votre planning</h1>
        <p className="text-xs text-gray-500 mt-0.5">Sélectionnez vos missions et choisissez votre mode de transport</p>
      </div>

      <div className="page-content">
        <div className="grid-12">

        {/* Panneau gauche — sélection */}
        <div className="col-span-8 flex flex-col gap-4">

          {/* Mode de transport */}
          <div className="card">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-3">Mode de transport</div>
            <div className="flex gap-2">
              {TRANSPORT_OPTIONS.map((opt) => (
                <button key={opt.id} onClick={() => setTransport(opt.id)}
                  className={`flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg text-sm font-medium border transition-all ${transport === opt.id ? "text-white border-transparent" : "border-gray-200 text-gray-600 bg-white hover:bg-gray-50"}`}
                  style={transport === opt.id ? { backgroundColor: brandColor } : {}}>
                  <div className={`w-7 h-7 rounded-lg flex items-center justify-center shrink-0 ${transport === opt.id ? "bg-white/20" : opt.bg}`}>
                    <opt.Icon size={15} className={transport === opt.id ? "text-white" : opt.text} />
                  </div>
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          {/* Contraintes journée */}
          <div className="card">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-3">Horaires de la journée</div>
            <div className="flex items-center gap-6 text-sm">
              <label className="flex items-center gap-2 text-gray-600">
                <span>Début</span>
                <input type="time" value={debutInput} onChange={e => setDebutInput(e.target.value)}
                  className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400" />
              </label>
              <label className="flex items-center gap-2 text-gray-600">
                <span>Pause déjeuner</span>
                <select value={pauseDuree} onChange={e => setPauseDuree(Number(e.target.value))}
                  className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400 bg-white">
                  {PAUSE_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                </select>
              </label>
              <label className="flex items-center gap-2 text-gray-600">
                <span>Fin</span>
                <input type="time" value={finInput} onChange={e => setFinInput(e.target.value)}
                  className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400" />
              </label>
            </div>
          </div>

          {/* Liste des missions */}
          <div className="card flex-1">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-3">
              Missions affectées — {audits.length} disponibles
            </div>
            <div className="space-y-2">
              {audits.map(audit => {
                const isSelected = selectedIds.has(audit.id)
                const trajet = getTrajetLabel(audit, transport)
                return (
                  <button key={audit.id} onClick={() => toggleAudit(audit.id)}
                    className={`w-full text-left rounded-xl border p-4 flex items-center gap-4 transition-all ${isSelected ? "text-white border-transparent" : "border-gray-200 bg-white hover:bg-gray-50"}`}
                    style={isSelected ? { backgroundColor: brandColor } : {}}>
                    <div className={`w-5 h-5 rounded border-2 flex items-center justify-center shrink-0 ${isSelected ? "border-white/50 bg-white/20" : "border-gray-300"}`}>
                      {isSelected && <span className="text-[10px] font-bold text-white">✓</span>}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className={`font-semibold text-sm truncate ${isSelected ? "text-white" : "text-gray-800"}`}>{audit.titre}</div>
                      <div className={`text-xs truncate ${isSelected ? "text-white/80" : "text-gray-500"}`}>{audit.site} · {audit.dureeMin} min</div>
                    </div>
                  </button>
                )
              })}
            </div>

            {/* Bouton bas */}
            <div className="flex items-center justify-between mt-4 pt-4 border-t border-gray-100">
              <span className="text-xs text-gray-500">
                {n > 0 ? `${n} mission${n > 1 ? "s" : ""} sélectionnée${n > 1 ? "s" : ""}` : "Aucune sélection"}
              </span>
              <button onClick={handleConfirm} disabled={n === 0}
                className="text-sm font-semibold px-5 py-2 rounded-lg text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
                style={{ backgroundColor: brandColor }}>
                <CalendarDays size={14} />Générez votre planning ({n}) →
              </button>
            </div>
          </div>
        </div>

        {/* Panneau droit — carte */}
        <div className="col-span-4">
          <h2 className="text-xs font-bold text-gray-400 uppercase tracking-widest mb-3">Parcours sélectionné</h2>
          <MapCard audits={selectedAudits.length > 0 ? selectedAudits : audits} theme={theme} compact focusCoords={null} transport={transport} />
          {selectedAudits.length === 0 && (
            <p className="text-[10px] text-gray-400 text-center mt-2">Sélectionnez des missions pour voir votre parcours</p>
          )}
        </div>
        </div>
      </div>
    </div>
  )
}
