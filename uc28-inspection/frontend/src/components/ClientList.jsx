import { Building2, ChevronRight, Briefcase } from "lucide-react"
import { AUDITS_TIMELINE } from "../mockData"

const CLIENTS = [
  { id: "ratp",        nom: "RATP",          secteur: "Transport ferroviaire",           missions: AUDITS_TIMELINE.length, prochaine: "Aujourd'hui 14h30", couleur: "bg-brand",         initiales: "RATP" },
  { id: "edf",         nom: "EDF",                      secteur: "Énergie & production électrique", missions: 3, prochaine: "Demain 08h00",       couleur: "bg-brand-cyan",    initiales: "EDF"  },
  { id: "total",       nom: "TotalEnergies",             secteur: "Énergie & pétrochimie",           missions: 2, prochaine: "Jeudi 10h00",        couleur: "bg-brand-emerald", initiales: "TE"   },
]

export default function ClientList({ onSelectClient }) {
  return (
    <div className="min-h-screen flex flex-col">
      <div className="page-content">
        <div className="grid-12">
          <div className="col-span-8 col-start-3">
            <h1 className="text-lg font-bold flex items-center gap-2 text-ink mb-4">
              <Building2 size={20} className="text-brand" />
              Sélectionnez un client
            </h1>
            <div className="space-y-3">
          {CLIENTS.map((client) => (
            <button
              key={client.id}
              onClick={() => onSelectClient(client)}
              className="w-full card flex items-center gap-4 hover:shadow-lg hover:-translate-y-px transition-all duration-150 text-left"
            >
              <div className={`w-12 h-12 ${client.couleur} rounded-xl flex items-center justify-center flex-shrink-0`}>
                <span className="text-white font-black text-xs">{client.initiales}</span>
              </div>
              <div className="flex-1 min-w-0">
                <div className="font-semibold text-ink text-sm">{client.nom}</div>
                <div className="text-xs text-ink-muted flex items-center gap-1">
                  <Briefcase size={10} className="shrink-0" />{client.secteur}
                </div>
              </div>
              <div className="text-right flex-shrink-0">
                <div className="text-sm font-bold text-brand">{client.missions}</div>
                <div className="text-[10px] text-ink-muted">mission{client.missions > 1 ? "s" : ""}</div>
                <div className="text-[10px] mt-0.5 text-brand-emerald">{client.prochaine}</div>
              </div>
              <ChevronRight size={16} className="text-ink-muted ml-1 shrink-0" />
            </button>
          ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
