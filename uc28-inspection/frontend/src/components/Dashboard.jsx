import { useEffect, useState } from "react"
import { AUDITS_TIMELINE, KPIS, AUDITEUR } from "../mockData"
import MapCard from "./MapCard"
import { CalendarDays, BarChart3, Clock, AlertTriangle, List, CalendarRange, PlayCircle, MapPin, CheckCircle2, Building2, Car, Bike, User } from "lucide-react"

const STATUT_STYLE = {
  TERMINÉ: "bg-divider text-ink-muted",
  CONFIRMÉ: "bg-brand/10 text-brand",
  PROCHAIN: "bg-brand-emerald/10 text-brand-emerald font-semibold",
  PLANIFIÉ: "bg-divider text-ink-muted",
}

const TYPE_STYLE = {
  Bureau: "bg-brand/10 text-brand",
  Visite: "bg-brand-cyan/10 text-brand-cyan",
  Viso:   "bg-brand-emerald/10 text-brand-emerald",
}

const TYPE_COLOR = {
  Bureau: "#0070AD",
  Visite: "#12ABDB",
  Viso:   "#10B981",
}

const STATUT_ICON_BADGE = {
  "TERMINÉ":  { Icon: CheckCircle2, bg: "bg-green-100",  text: "text-green-600" },
  "PROCHAIN": { Icon: PlayCircle,   bg: "bg-brand/10",   text: "text-brand" },
  "PLANIFIÉ": { Icon: Building2,    bg: "bg-gray-100",   text: "text-gray-500" },
  "CONFIRMÉ": { Icon: Building2,    bg: "bg-brand/10",   text: "text-brand" },
}

const STATUT_OPTIONS = ["Tous", "TERMINÉ", "PROCHAIN", "PLANIFIÉ"]
const PLAN_START = 8 * 60
const PLAN_END = 18 * 60
const PLAN_DURATION = PLAN_END - PLAN_START

function toPercent(minutes) {
  return Math.max(0, Math.min(100, ((minutes - PLAN_START) / PLAN_DURATION) * 100))
}

const KPI_ICONS = [CalendarDays, BarChart3, Clock, AlertTriangle]
const KPI_COLORS = ["text-brand", "text-brand-cyan", "text-brand-emerald", "text-ink-muted"]

function KpiCard({ value, label, highlight, index }) {
  const Icon = KPI_ICONS[index] || CalendarDays
  return (
    <div className={`rounded-xl px-5 py-4 flex flex-col gap-1 shadow-md ${
      highlight ? "bg-brand-amber/5 border-l-4 border-brand-amber" : "bg-surface"
    }`}>
      <Icon size={16} className={`mb-0.5 ${highlight ? "text-brand-amber" : KPI_COLORS[index]}`} />
      <span className={`text-3xl font-black ${highlight ? "text-brand-amber" : KPI_COLORS[index]}`}>{value}</span>
      <span className="text-xs text-ink-muted leading-tight">{label}</span>
    </div>
  )
}

function PlanningView({ audits, currentMin }) {
  const hours = Array.from({ length: 11 }, (_, i) => 8 + i)
  const nowPct = toPercent(currentMin)
  const isPastStart = currentMin > PLAN_START && currentMin < PLAN_END

  return (
    <div className="bg-surface rounded-xl shadow-md p-4">
      <h2 className="text-[10px] font-bold text-brand uppercase tracking-widest bg-wash-cyan rounded-md -mx-1 px-2 py-1.5 mb-4 flex items-center gap-1.5">
        <CalendarRange size={11} />Planning journée
      </h2>

      <div className="relative mb-2">
        <div className="flex justify-between mb-1">
          {hours.map((h) => (
            <span key={h} className="text-[9px] text-ink-muted font-mono w-0 text-center">{h}h</span>
          ))}
        </div>

        <div className="relative h-16 bg-surface-sunk shadow-inset rounded-lg overflow-hidden">
          {hours.slice(1).map((h) => (
            <div key={h} className="absolute top-0 bottom-0 w-px bg-divider" style={{ left: `${toPercent(h * 60)}%` }} />
          ))}
          {audits.filter((a) => a.heureMin).map((audit) => (
            <div
              key={audit.id}
              className="absolute top-2 bottom-2 rounded flex items-center px-1.5 overflow-hidden"
              style={{
                left: `${toPercent(audit.heureMin)}%`,
                width: `${(audit.dureeMin / PLAN_DURATION) * 100}%`,
                backgroundColor: TYPE_COLOR[audit.type] || "#64748b",
                opacity: audit.statut === "TERMINÉ" ? 0.45 : 1,
              }}
              title={audit.titre}
            >
              <span className="text-[9px] text-white font-semibold truncate">{audit.heure} {audit.titre}</span>
            </div>
          ))}
          {isPastStart && (
            <div className="absolute top-0 bottom-0 w-0.5 bg-red-500 z-10" style={{ left: `${nowPct}%` }}>
              <div className="absolute -top-1 left-1/2 -translate-x-1/2 w-2 h-2 bg-red-500 rounded-full" />
              <div className="absolute top-1 left-2 text-[8px] text-red-600 font-bold whitespace-nowrap bg-white/80 px-0.5 rounded">
                maintenant
              </div>
            </div>
          )}
        </div>
      </div>

      <div className="flex gap-4 mt-3">
        {Object.entries(TYPE_COLOR).map(([type, color]) => (
          <span key={type} className="flex items-center gap-1 text-[10px] text-ink-muted">
            <span className="w-3 h-2 rounded-sm inline-block" style={{ backgroundColor: color }} />
            {type}
          </span>
        ))}
        <span className="flex items-center gap-1 text-[10px] text-red-500 ml-auto">
          <span className="w-px h-3 bg-red-500 inline-block" /> Heure actuelle
        </span>
      </div>

      <div className="mt-4 border-t border-divider pt-3">
        <h3 className="text-[10px] font-bold text-brand uppercase tracking-widest mb-2">Itinéraire</h3>
        <div className="space-y-1.5">
          {audits.filter((a) => a.trajet && a.trajet.voiture !== "—").map((audit) => (
            <div key={audit.id} className="flex items-center gap-3 text-xs">
              <span className="font-mono w-10 shrink-0 text-brand">{audit.heure}</span>
              <span className="text-ink flex-1 truncate">{audit.site}</span>
              <span className="text-ink-muted flex items-center gap-2 shrink-0 text-[10px]">
                <span>🚗 {audit.trajet.voiture}</span>
                <span>🚲 {audit.trajet.velo}</span>
                <span>🚶 {audit.trajet.pied}</span>
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

export default function Dashboard({ onDemarrer, onOpenPlanning, theme }) {
  const [heure, setHeure] = useState("")
  const [currentMin, setCurrentMin] = useState(0)
  const [onglet, setOnglet] = useState("liste")
  const [filtreStatut, setFiltreStatut] = useState("Tous")
  const [filtreAssignees, setFiltreAssignees] = useState(true)
  const [focusCoords, setFocusCoords] = useState(null)

  useEffect(() => {
    const tick = () => {
      const now = new Date()
      setHeure(now.toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" }))
      setCurrentMin(now.getHours() * 60 + now.getMinutes())
    }
    tick()
    const id = setInterval(tick, 10000)
    return () => clearInterval(id)
  }, [])

  const today = new Date().toLocaleDateString("fr-FR", {
    weekday: "long", day: "numeric", month: "long", year: "numeric",
  })

  const auditsFiltres = AUDITS_TIMELINE.filter((a) => {
    if (filtreStatut !== "Tous" && a.statut !== filtreStatut) return false
    if (filtreAssignees && !a.site_id) return false
    return true
  })

  return (
    <div className="min-h-screen flex flex-col">
      <div className="px-6 py-2 text-xs bg-surface border-b border-divider text-ink-muted">
        Mes audits — <span className="capitalize">{today}</span>
      </div>

      <div className="flex-1 p-6 flex flex-col gap-4 max-w-screen-xl mx-auto w-full">
        {/* KPIs */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KpiCard value={KPIS.audits_jour} label="audits planifiés aujourd'hui" index={0} />
          <KpiCard value={KPIS.audits_mois} label="audits ce mois" index={1} />
          <KpiCard value={KPIS.delai_moyen} label="délai moyen audit → rapport" index={2} />
          <KpiCard value={KPIS.recurrences} label="récurrences à vérifier" highlight index={3} />
        </div>

        {/* Onglets + filtres */}
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex rounded-lg shadow-sm bg-surface overflow-hidden text-xs font-medium">
            {["liste", "planning"].map((tab) => (
              <button
                key={tab}
                onClick={() => setOnglet(tab)}
                className={`px-4 py-1.5 capitalize transition-colors ${
                  onglet === tab ? "bg-brand text-white" : "text-ink-muted hover:bg-canvas"
                }`}
              >
                {tab === "liste"
                  ? <><List size={13} className="inline mr-1" />Liste</>
                  : <><CalendarRange size={13} className="inline mr-1" />Planning</>}
              </button>
            ))}
          </div>

          <button
            onClick={() => onOpenPlanning()}
            className="text-xs font-medium px-3 py-1.5 rounded-lg border border-brand text-brand hover:bg-brand/10 transition-colors flex items-center gap-1.5"
          >
            <CalendarDays size={13} /> Mon planning
          </button>

          {onglet === "liste" && (
            <>
              <button
                onClick={() => setFiltreAssignees((v) => !v)}
                className={`text-xs px-3 py-1.5 rounded-lg border transition-colors ${
                  filtreAssignees
                    ? "bg-brand/10 border-brand text-brand"
                    : "border-divider text-ink-muted bg-surface hover:bg-canvas"
                }`}
              >
                {filtreAssignees ? "✓ Mes missions" : "Toutes les missions"}
              </button>
              <div className="flex gap-1">
                {STATUT_OPTIONS.map((s) => (
                  <button
                    key={s}
                    onClick={() => setFiltreStatut(s)}
                    className={`text-[10px] px-2.5 py-1 rounded-full border transition-colors ${
                      filtreStatut === s
                        ? "bg-dark-teal text-white border-dark-teal"
                        : "border-divider text-ink-muted bg-surface hover:bg-canvas"
                    }`}
                  >
                    {s}
                  </button>
                ))}
              </div>
            </>
          )}
        </div>

        {/* Vue Planning */}
        {onglet === "planning" && (
          <PlanningView audits={AUDITS_TIMELINE} currentMin={currentMin} />
        )}

        {/* Vue Liste */}
        {onglet === "liste" && (
          <div className="flex gap-6">
            <div className="flex-1 min-w-0">
              <h2 className="text-[10px] font-bold text-brand uppercase tracking-widest mb-3 bg-wash-cyan rounded-md px-2 py-1.5 flex items-center gap-1.5">
                <List size={11} />
                {filtreAssignees ? "Mes missions" : "Toutes les missions"} · {auditsFiltres.length} résultat{auditsFiltres.length > 1 ? "s" : ""}
              </h2>
              {auditsFiltres.length === 0 ? (
                <p className="text-sm text-ink-muted italic py-4">Aucune mission ne correspond aux filtres.</p>
              ) : (
                <div className="flex flex-col gap-3">
                  {auditsFiltres.map((audit) => (
                    <div
                      key={audit.id}
                      onClick={() => audit.coords && setFocusCoords(audit.coords)}
                      className={`bg-surface rounded-xl shadow-md p-4 flex items-start gap-4 hover:shadow-lg hover:-translate-y-px transition-all duration-150 ${audit.coords ? "cursor-pointer" : ""} ${
                        audit.statut === "PROCHAIN" ? "border-l-4 border-brand-emerald" : ""
                      }`}
                    >
                      {(() => { const si = STATUT_ICON_BADGE[audit.statut]; return si ? (
                        <div className={`w-10 h-10 rounded-lg flex items-center justify-center shrink-0 ${si.bg}`}>
                          <si.Icon size={18} className={si.text} />
                        </div>
                      ) : null })()}
                      <div className="text-center min-w-[52px]">
                        <div className="font-mono text-sm font-bold text-brand">{audit.heure}</div>
                        <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium mt-1 inline-block ${TYPE_STYLE[audit.type] || "bg-canvas text-ink-muted"}`}>
                          {audit.type}
                        </span>
                      </div>

                      <div className="flex-1 min-w-0">
                        <div className="font-semibold text-ink text-sm truncate">{audit.titre}</div>
                        <div className="text-xs text-ink-muted truncate">{audit.site}</div>
                        <div className="text-xs text-ink-muted mt-0.5 truncate">Scope : {audit.scope}</div>
                        {audit.trajet?.voiture !== "—" && (
                          <div className="mt-1 flex gap-3 text-[10px] text-ink-muted">
                            <span className="flex items-center gap-0.5"><Car size={10} />{audit.trajet.voiture}</span>
                            <span className="flex items-center gap-0.5"><Bike size={10} />{audit.trajet.velo}</span>
                            <span className="flex items-center gap-0.5"><User size={10} />{audit.trajet.pied}</span>
                          </div>
                        )}
                      </div>

                      <div className="flex flex-col items-end gap-2 shrink-0">
                        <span className={`text-[10px] px-2 py-0.5 rounded-full font-semibold ${STATUT_STYLE[audit.statut]}`}>
                          {audit.statut}
                        </span>
                        {audit.statut === "PROCHAIN" ? (
                          <button
                            onClick={(e) => { e.stopPropagation(); onDemarrer(audit) }}
                            className="text-xs font-semibold px-3 py-1.5 rounded-lg bg-brand-emerald text-white hover:bg-brand transition-colors shadow-sm"
                          >
                            <PlayCircle size={13} className="inline mr-1" />Démarrer
                          </button>
                        ) : audit.statut === "PLANIFIÉ" ? (
                          <span className="text-xs text-ink-muted">{audit.action}</span>
                        ) : (
                          <button className="text-xs text-brand hover:underline">{audit.action} ↗</button>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Carte des trajets */}
            <div className="w-72 shrink-0">
              <h2 className="text-[10px] font-bold text-brand uppercase tracking-widest mb-3 bg-wash-cyan rounded-md px-2 py-1.5 flex items-center gap-1.5">
                <MapPin size={11} />Parcours du jour
              </h2>
              <MapCard audits={auditsFiltres} theme={theme} compact focusCoords={focusCoords} />
            </div>
          </div>
        )}
      </div>

      <div className="px-6 py-2 flex items-center justify-between text-xs text-white bg-dark-teal">
        <span className="font-semibold">{AUDITEUR.nom} — Lead Auditeur</span>
        <span className="text-brand-cyan">{AUDITEUR.localisation} · {heure}</span>
      </div>
    </div>
  )
}
