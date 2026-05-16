import { useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useQuery, useMutation } from "@tanstack/react-query";
import { getInspection } from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";
import NCBadge from "@/components/NCBadge";
import type { NCLevel } from "@/lib/api";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

export default function ReportPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [sent, setSent] = useState(false);

  const { data: inspection } = useQuery({
    queryKey: ["inspection", id],
    queryFn: () => getInspection(id!),
    enabled: !!id,
  });

  const sendMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch(`${API_URL}/api/inspections/${id}/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          to: ["marie.lemaitre@alpha.fr"],
          cc: ["audit.bv@bureauveritas.com"],
          subject: `Pré-rapport audit ${inspection?.client_name} — ${new Date().toLocaleDateString("fr-FR")}`,
          message: "Veuillez trouver ci-joint le pré-rapport de votre audit ISO 9001.",
        }),
      });
      return res.json();
    },
    onSuccess: () => {
      setSent(true);
      setTimeout(() => navigate("/"), 2000);
    },
  });

  const constats = inspection?.constats ?? [];
  const stats = {
    nc_majeure: constats.filter((c) => c.classification === "nc_majeure").length,
    nc_mineure: constats.filter((c) => c.classification === "nc_mineure").length,
    observation: constats.filter((c) => c.classification === "observation").length,
    conforme: constats.filter((c) => c.classification === "conforme").length,
  };

  const STAT_CONFIG: { key: NCLevel; label: string; color: string; textColor: string }[] = [
    { key: "nc_majeure", label: "NC Majeure", color: "border-uc-danger", textColor: "text-uc-danger" },
    { key: "nc_mineure", label: "NC Mineure", color: "border-uc-alert", textColor: "text-uc-alert" },
    { key: "observation", label: "Observation", color: "border-uc-primary-2", textColor: "text-uc-primary-2" },
    { key: "conforme", label: "Conforme", color: "border-uc-accent", textColor: "text-uc-accent" },
  ];

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="report"
        title={`Audit ${inspection?.referential ?? ""} · ${inspection?.client_name ?? ""}`}
        subtitle={`${inspection?.site_name ?? ""} · audit terminé`}
      />

      <main className="flex-1 grid grid-cols-12 gap-4 p-4 min-h-0 overflow-hidden">
        {/* Synthèse — 3/12 */}
        <div className="col-span-3 flex flex-col gap-3 overflow-y-auto">
          <h2 className="text-xs font-bold text-uc-text-mute uppercase tracking-wider">Synthèse</h2>
          {STAT_CONFIG.map(({ key, label, color, textColor }) => (
            <div key={key} className={`bg-white rounded-lg shadow-sm border-l-4 ${color} p-3`}>
              <span className={`text-4xl font-mono font-bold ${textColor}`}>{stats[key]}</span>
              <p className="text-xs text-uc-text-mute mt-1">{label}</p>
            </div>
          ))}
          <div className="bg-uc-bg-dark rounded-lg p-3 mt-2">
            <p className="text-xs font-bold text-uc-accent-lt mb-2">Plan d&apos;action</p>
            {constats
              .filter((c) => c.classification === "nc_majeure" || c.classification === "nc_mineure")
              .slice(0, 3)
              .map((c, i) => (
                <div key={c.id} className="flex gap-2 items-start mb-2">
                  <span className={`text-xs font-bold px-1.5 py-0.5 rounded ${i === 0 ? "bg-uc-danger text-white" : "bg-uc-alert text-white"}`}>
                    P{i + 1}
                  </span>
                  <p className="text-xs text-uc-accent-lt leading-snug">{c.suggested_action ?? c.reformulated_text}</p>
                </div>
              ))}
          </div>
        </div>

        {/* DOCX Preview — 6/12 */}
        <div className="col-span-6 bg-white rounded-xl shadow-sm overflow-y-auto p-6">
          <div className="text-center border-b border-uc-border pb-6 mb-6">
            <h1 className="text-xl font-bold text-uc-text-dark">Pré-rapport d&apos;audit qualité</h1>
            <p className="text-sm text-uc-text-mute mt-1">{inspection?.client_name} · {new Date().toLocaleDateString("fr-FR")}</p>
            <p className="text-xs text-uc-text-mute">{inspection?.site_name} · {inspection?.auditor_name}</p>
          </div>
          <div className="flex flex-col gap-4">
            {constats.map((c) => (
              <div key={c.id} className="border-l-4 border-uc-border pl-3">
                <div className="flex items-center gap-2 mb-1">
                  <NCBadge level={c.classification} />
                  {c.norm_reference && (
                    <span className="font-mono text-xs text-uc-text-mute">{c.norm_reference}</span>
                  )}
                </div>
                <p className="text-sm text-uc-text-body">{c.reformulated_text}</p>
                {c.suggested_action && (
                  <p className="text-xs text-uc-text-mute mt-1">→ {c.suggested_action}</p>
                )}
              </div>
            ))}
          </div>
          <a
            href={`${API_URL}/api/inspections/${id}/report.docx`}
            download
            className="mt-6 inline-block px-4 py-2 border border-uc-border text-uc-text-body text-sm rounded-lg hover:bg-uc-panel transition-colors"
          >
            ⬇ Télécharger DOCX
          </a>
        </div>

        {/* Send form — 3/12 */}
        <div className="col-span-3 bg-white rounded-xl shadow-sm p-4 flex flex-col gap-3 overflow-y-auto">
          <h2 className="text-xs font-bold text-uc-text-mute uppercase tracking-wider">Envoi au client</h2>
          <div className="flex flex-col gap-2 text-sm">
            {[
              ["À", "marie.lemaitre@alpha.fr"],
              ["CC", "audit.bv@bureauveritas.com"],
              ["Objet", `Pré-rapport audit ${inspection?.client_name} — ${new Date().toLocaleDateString("fr-FR")}`],
            ].map(([label, value]) => (
              <div key={label}>
                <p className="text-xs text-uc-text-mute">{label}</p>
                <p className="text-sm text-uc-text-body">{value}</p>
              </div>
            ))}
            <div>
              <p className="text-xs text-uc-text-mute">Message</p>
              <textarea
                className="w-full text-sm text-uc-text-body border border-uc-border rounded p-2 h-24 resize-none mt-1"
                defaultValue="Madame, Monsieur,\n\nVeuillez trouver ci-joint le pré-rapport de votre audit ISO 9001.\n\nCordialement,\nAntoine Mercier"
              />
            </div>
            <div>
              <p className="text-xs text-uc-text-mute">Pièces jointes</p>
              <p className="text-xs text-uc-text-body">Rapport DOCX + {constats.filter((c) => c.photo_path).length} photos</p>
            </div>
          </div>
        </div>
      </main>

      <footer className="bg-uc-bg-dark px-6 py-4 flex items-center justify-between shrink-0">
        <div>
          <p className="text-xs text-uc-accent-lt font-bold uppercase tracking-wider">AUDIT TERMINÉ</p>
          <p className="text-sm text-white">
            {constats.length} constats · {stats.nc_majeure} NC majeure · {stats.nc_mineure} NC mineure
          </p>
        </div>
        <button
          onClick={() => sendMutation.mutate()}
          disabled={sendMutation.isPending || sent}
          className="px-8 py-4 bg-uc-accent text-white text-lg font-bold rounded-xl disabled:opacity-50 hover:bg-emerald-600 transition-colors"
        >
          {sent ? "✓ Envoyé" : "Envoyer au client →"}
        </button>
      </footer>

      {sent && (
        <div className="fixed inset-0 bg-uc-bg-dark/80 flex items-center justify-center z-50">
          <div className="bg-white rounded-2xl p-8 text-center shadow-2xl">
            <p className="text-4xl mb-3">✅</p>
            <p className="text-lg font-bold text-uc-text-dark">Rapport envoyé</p>
            <p className="text-sm text-uc-text-mute">à Marie Lemaitre · {new Date().toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })}</p>
          </div>
        </div>
      )}
    </div>
  );
}
