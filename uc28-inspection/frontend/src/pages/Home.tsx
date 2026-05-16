import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { getDashboardKpis, getAuditsToday, type AuditToday } from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";

function KpiCard({ label, value, color }: { label: string; value: number | string; color: string }) {
  return (
    <div className={`bg-white rounded-lg shadow-sm border-l-4 ${color} p-4 flex flex-col gap-1`}>
      <span className="text-3xl font-mono font-bold text-uc-text-dark">{value}</span>
      <span className="text-xs text-uc-text-mute uppercase tracking-wide">{label}</span>
    </div>
  );
}

function AuditCard({ audit, onStart }: { audit: AuditToday; onStart: () => void }) {
  const highlight = audit.is_next;
  return (
    <div
      className={`bg-white rounded-lg shadow-sm p-4 flex items-center gap-4 ${
        highlight ? "border-2 border-uc-accent bg-uc-accent-50" : "border border-uc-border"
      }`}
    >
      <div className="w-20 shrink-0 text-center">
        <p className="font-mono text-lg font-bold text-uc-primary">
          {new Date(audit.scheduled_at).toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })}
        </p>
        <span
          className={`text-xs px-2 py-0.5 rounded-full font-medium ${
            audit.status === "ongoing"
              ? "bg-uc-accent text-white"
              : audit.status === "completed"
              ? "bg-uc-text-mute text-white"
              : "bg-uc-alert-50 text-uc-alert"
          }`}
        >
          {audit.status}
        </span>
      </div>
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-uc-text-dark truncate">{audit.client_name}</p>
        <p className="text-xs text-uc-text-mute truncate">{audit.location}</p>
        <p className="text-xs text-uc-text-body truncate mt-0.5">{audit.scope}</p>
      </div>
      {highlight && (
        <button
          onClick={onStart}
          className="px-4 py-2 bg-uc-accent text-white text-sm font-semibold rounded-lg hover:bg-emerald-600 transition-colors whitespace-nowrap"
        >
          Démarrer →
        </button>
      )}
      {!highlight && audit.status === "completed" && (
        <span className="text-xs text-uc-text-mute">Voir le rapport ↗</span>
      )}
    </div>
  );
}

export default function Home() {
  const navigate = useNavigate();
  const { data: kpis } = useQuery({ queryKey: ["kpis"], queryFn: getDashboardKpis });
  const { data: audits = [] } = useQuery({ queryKey: ["audits_today"], queryFn: getAuditsToday });

  return (
    <div className="h-dvh w-dvw flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="dashboard"
        title={`Mes audits — ${new Date().toLocaleDateString("fr-FR", { weekday: "long", day: "numeric", month: "long" })}`}
        subtitle="Bureau Veritas · Antoine Mercier · Lyon"
      />
      <main className="flex-1 overflow-y-auto p-6 flex flex-col gap-6">
        <div className="grid grid-cols-4 gap-4">
          <KpiCard label="Audits aujourd'hui" value={kpis?.audits_today_count ?? "—"} color="border-uc-accent" />
          <KpiCard label="Audits ce mois" value={kpis?.audits_month_count ?? "—"} color="border-uc-primary" />
          <KpiCard label="Délai moyen (j)" value={kpis?.avg_delay_days ?? "—"} color="border-uc-accent" />
          <KpiCard label="Récurrences à vérifier" value={kpis?.pending_recurrences_count ?? "—"} color="border-uc-alert" />
        </div>
        <div className="flex flex-col gap-3">
          <h2 className="text-sm font-bold text-uc-text-mute uppercase tracking-wider">Audits du jour</h2>
          {audits.length === 0 ? (
            <div className="bg-white rounded-lg p-8 text-center text-uc-text-mute">
              Aucun audit planifié aujourd'hui
            </div>
          ) : (
            audits.map((a) => (
              <AuditCard key={a.id} audit={a} onStart={() => navigate(`/inspection/${a.id}/brief`)} />
            ))
          )}
        </div>
      </main>
      <footer className="bg-uc-bg-dark text-uc-text-mute text-xs p-4 text-center shrink-0">
        Antoine Mercier · Lead Auditor · Lyon · UC 28 — Inspection Augmentée
      </footer>
    </div>
  );
}
