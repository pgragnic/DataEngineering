import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { getDashboardKpis, getAuditsToday, type AuditToday } from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";

const RECURRENCES = [
  { client: "Fournisseur ALPHA", level: "NC mineure", ref: "§8.4.3", date: "2025-06-15", label: "Procédure contrôle réception", borderColor: "border-uc-alert", textColor: "text-uc-alert" },
  { client: "Fournisseur OMEGA", level: "Observation", ref: "§7.2", date: "2025-11-08", label: "Polyvalence opérateurs", borderColor: "border-uc-primary-2", textColor: "text-uc-primary-2" },
  { client: "Sous-traitant DELTA", level: "NC majeure", ref: "§7.1.4", date: "2026-02-21", label: "Sécurité quai chargement", borderColor: "border-uc-danger", textColor: "text-uc-danger" },
];

function KpiCard({ label, value, color }: { label: string; value: number | string; color: string }) {
  return (
    <div className={`bg-white rounded-lg shadow-sm border-l-4 ${color} p-4 flex flex-col gap-1`}>
      <span className="text-3xl font-mono font-bold text-uc-text-dark">{value}</span>
      <span className="text-xs text-uc-text-mute uppercase tracking-wide">{label}</span>
    </div>
  );
}

function auditBadge(audit: AuditToday) {
  if (audit.is_next)
    return { label: "PROCHAIN", cls: "bg-uc-alert-50 text-uc-alert border border-uc-alert" };
  if (audit.status === "completed")
    return { label: "TERMINÉ", cls: "bg-uc-text-mute/20 text-uc-text-mute" };
  if (audit.status === "ongoing")
    return { label: "EN COURS", cls: "bg-uc-accent text-white" };
  return { label: "PLANIFIÉ", cls: "bg-uc-panel text-uc-text-mute border border-uc-border" };
}

function AuditCard({ audit, onStart }: { audit: AuditToday; onStart: () => void }) {
  const highlight = audit.is_next;
  const badge = auditBadge(audit);
  const time = new Date(audit.scheduled_at)
    .toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })
    .replace(":", "h");

  return (
    <div
      className={`bg-white rounded-xl p-4 flex items-center gap-4 ${
        highlight
          ? "border-2 border-uc-accent shadow-md"
          : "border border-uc-border shadow-sm"
      }`}
    >
      {/* Heure + lieu + badge */}
      <div className="w-20 shrink-0 flex flex-col items-center gap-1.5 text-center">
        <p className="font-mono text-xl font-bold text-uc-primary leading-none">{time}</p>
        <p className="text-[10px] text-uc-text-mute leading-tight">{audit.location}</p>
        <span className={`text-[10px] px-2 py-0.5 rounded-full font-bold uppercase tracking-wide ${badge.cls}`}>
          {badge.label}
        </span>
      </div>

      {/* Contenu */}
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-uc-text-dark truncate">{audit.client_name}</p>
        <p className="text-xs text-uc-text-mute truncate">{audit.location}</p>
        <p className="text-xs text-uc-text-body mt-0.5 line-clamp-2">{audit.scope}</p>
      </div>

      {/* Action */}
      {highlight && (
        <button
          onClick={onStart}
          className="px-4 py-2 bg-uc-accent text-white text-sm font-bold rounded-lg hover:bg-emerald-600 transition-colors whitespace-nowrap shrink-0"
        >
          Démarrer →
        </button>
      )}
      {!highlight && audit.status === "completed" && (
        <span className="text-xs text-uc-primary-2 cursor-pointer hover:underline whitespace-nowrap shrink-0">
          Voir le rapport ↗
        </span>
      )}
      {!highlight && audit.status === "prepared" && (
        <span className="text-xs text-uc-text-mute whitespace-nowrap shrink-0">À {time}</span>
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
        title={`Mes audits — ${new Date().toLocaleDateString("fr-FR", { weekday: "long", day: "numeric", month: "long", year: "numeric" })}`}
        subtitle="Bureau Veritas Certification · Antoine Mercier · Lyon"
      />

      <main className="flex-1 overflow-y-auto p-6 flex flex-col gap-6">
        {/* KPIs */}
        <div className="grid grid-cols-4 gap-4">
          <KpiCard label="Audits planifiés aujourd'hui" value={kpis?.audits_today_count ?? "—"} color="border-uc-accent" />
          <KpiCard label="Audits ce mois" value={kpis?.audits_month_count ?? "—"} color="border-uc-primary" />
          <KpiCard label="Délai moyen audit → rapport (j)" value={kpis?.avg_delay_days ?? "—"} color="border-uc-primary-2" />
          <KpiCard label="Récurrences à vérifier" value={kpis?.pending_recurrences_count ?? "—"} color="border-uc-alert" />
        </div>

        {/* Corps : audits + récurrences */}
        <div className="flex gap-6 flex-1 min-h-0">
          {/* Audits du jour */}
          <div className="flex-1 flex flex-col gap-3 min-w-0">
            <h2 className="text-xs font-bold text-uc-text-mute uppercase tracking-wider">
              Audits du jour
            </h2>
            {audits.length === 0 ? (
              <div className="bg-white rounded-xl p-8 text-center text-uc-text-mute border border-uc-border">
                Aucun audit planifié aujourd'hui
              </div>
            ) : (
              audits.map((a) => (
                <AuditCard
                  key={a.id}
                  audit={a}
                  onStart={() => navigate(`/inspection/${a.id}/brief`)}
                />
              ))
            )}
          </div>

          {/* Récurrences à vérifier */}
          <div className="w-72 shrink-0 flex flex-col gap-3">
            <div>
              <h2 className="text-xs font-bold text-uc-alert uppercase tracking-wider">
                Récurrences à vérifier
              </h2>
              <p className="text-[10px] text-uc-text-mute mt-0.5">
                NC non clôturées chez les fournisseurs du jour
              </p>
            </div>
            {RECURRENCES.map((r) => (
              <div
                key={r.client}
                className={`bg-white rounded-xl border border-uc-border border-l-4 ${r.borderColor} p-3 shadow-sm`}
              >
                <p className="font-semibold text-sm text-uc-text-dark">{r.client}</p>
                <p className={`text-xs font-bold ${r.textColor}`}>
                  {r.level} {r.ref}
                </p>
                <p className="text-[10px] text-uc-text-mute mt-0.5">ouverte le {r.date}</p>
                <p className="text-xs text-uc-text-body mt-1">{r.label}</p>
              </div>
            ))}
          </div>
        </div>
      </main>

      <footer className="bg-uc-bg-dark text-uc-text-mute text-xs p-4 flex items-center justify-between shrink-0">
        <span className="font-bold text-uc-accent-lt uppercase tracking-wider">
          Antoine Mercier · Lead Auditor
        </span>
        <span>
          Lyon · {new Date().toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })} · UC 28 — Inspection Augmentée
        </span>
      </footer>
    </div>
  );
}
