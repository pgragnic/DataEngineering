import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { motion } from "framer-motion";
import { getInspection, generateChecklist, updateInspection } from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";
import ChecklistView from "@/components/ChecklistView";

export default function BriefPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const qc = useQueryClient();

  const { data: inspection } = useQuery({
    queryKey: ["inspection", id],
    queryFn: () => getInspection(id!),
    enabled: !!id,
  });

  const [generationSeconds, setGenerationSeconds] = useState<number | null>(null);
  const [generating, setGenerating] = useState(false);

  const checklistMutation = useMutation({
    mutationFn: () => generateChecklist(id!),
    onSuccess: (data) => {
      setGenerating(false);
      setGenerationSeconds(data.generation_duration_seconds);
      qc.invalidateQueries({ queryKey: ["inspection", id] });
    },
  });

  const startMutation = useMutation({
    mutationFn: () => updateInspection(id!, { status: "ongoing" }),
    onSuccess: () => navigate(`/inspection/${id}/capture`),
  });

  useEffect(() => {
    if (inspection && !inspection.checklist_json && !generating) {
      setGenerating(true);
      checklistMutation.mutate();
    }
  }, [inspection]);  // eslint-disable-line react-hooks/exhaustive-deps

  const hasChecklist = Boolean(inspection?.checklist_json);

  return (
    <div className="h-dvh w-dvw flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="brief"
        title={`Audit ${inspection?.referential ?? ""} · ${inspection?.client_name ?? ""}`}
        subtitle={`${inspection?.site_name ?? ""} · Auditeur : ${inspection?.auditor_name ?? ""}`}
      />
      <main className="flex-1 grid grid-cols-12 gap-4 p-4 min-h-0 overflow-hidden">
        {/* Brief client — 3/12 */}
        <div className="col-span-3 bg-white rounded-xl shadow-sm p-4 overflow-y-auto">
          <h2 className="text-xs font-bold text-uc-text-mute uppercase tracking-wider mb-3">Brief client</h2>
          {inspection && (
            <div className="flex flex-col gap-2 text-sm">
              {[
                ["Client", inspection.client_name],
                ["SIRET", inspection.client_siret ?? "—"],
                ["Site", inspection.site_name],
                ["Adresse", inspection.site_address ?? "—"],
                ["Auditeur", inspection.auditor_name],
                ["Référentiel", inspection.referential],
              ].map(([k, v]) => (
                <div key={k}>
                  <span className="text-xs text-uc-text-mute">{k}</span>
                  <p className="font-medium text-uc-text-body">{v}</p>
                </div>
              ))}
              <div>
                <span className="text-xs text-uc-text-mute">Périmètre</span>
                <p className="text-xs text-uc-text-body mt-0.5">{inspection.scope}</p>
              </div>
            </div>
          )}
        </div>

        {/* Check-list — 6/12 */}
        <div className="col-span-6 flex flex-col gap-3 overflow-hidden">
          {generating && (
            <div className="bg-uc-accent-50 border border-uc-accent rounded-lg p-4 flex items-center gap-3">
              <div className="w-5 h-5 border-2 border-uc-accent border-t-transparent rounded-full animate-spin shrink-0" />
              <div>
                <p className="text-sm font-semibold text-uc-primary">Préparation par l&apos;Agent 1…</p>
                <p className="text-xs text-uc-text-mute">Croisement du scope client avec ISO 9001 et l&apos;historique fournisseur</p>
              </div>
            </div>
          )}
          {hasChecklist && generationSeconds !== null && (
            <motion.div
              initial={{ opacity: 0, y: -4 }}
              animate={{ opacity: 1, y: 0 }}
              className="bg-uc-accent-50 border border-uc-accent rounded-lg p-3 flex items-center gap-2"
            >
              <span className="text-uc-accent font-bold">✓</span>
              <span className="text-sm text-uc-primary font-medium">
                Préparation terminée — {generationSeconds}s ·{" "}
                {inspection?.checklist_json?.sections?.length ?? 0} sections ·{" "}
                {inspection?.checklist_json?.sections?.reduce((acc, s) => acc + s.points.length, 0) ?? 0} points
              </span>
            </motion.div>
          )}

          <div className="flex-1 overflow-y-auto">
            {hasChecklist && inspection?.checklist_json && (
              <motion.div
                className="flex flex-col gap-2"
                initial="hidden"
                animate="visible"
                variants={{ visible: { transition: { staggerChildren: 0.1 } } }}
              >
                {inspection.checklist_json.sections.map((section) => (
                  <motion.div
                    key={section.id}
                    variants={{ hidden: { opacity: 0, y: 8 }, visible: { opacity: 1, y: 0 } }}
                    className="bg-white rounded-lg border border-uc-border p-3"
                  >
                    <p className="font-mono text-xs font-bold text-uc-primary">{section.id}</p>
                    <p className="text-sm font-medium text-uc-text-body">{section.title}</p>
                    <p className="text-xs text-uc-text-mute">{section.points.length} points</p>
                  </motion.div>
                ))}
              </motion.div>
            )}
          </div>
        </div>

        {/* Historique — 3/12 */}
        <div className="col-span-3 flex flex-col gap-3 overflow-y-auto">
          <h2 className="text-xs font-bold text-uc-text-mute uppercase tracking-wider">Audits précédents</h2>
          <div className="bg-uc-alert-50 border-l-4 border-uc-alert rounded-lg p-3">
            <p className="text-xs font-bold text-uc-alert">⚠ Point d&apos;attention</p>
            <p className="text-xs text-uc-text-body mt-1">
              NC mineure non clôturée depuis l&apos;audit 2025 — procédure de contrôle réception à formaliser (§8.4.3).
              Automatiquement ajoutée à la check-list.
            </p>
          </div>
          <div className="bg-white rounded-lg border border-uc-border p-3">
            <p className="font-mono text-xs text-uc-text-mute">2025-06-15</p>
            <p className="text-sm font-medium">ISO 9001</p>
            <p className="text-xs text-uc-alert">1 NC mineure ouverte</p>
          </div>
        </div>
      </main>

      <footer className="bg-uc-bg-dark p-4 flex items-center justify-between shrink-0">
        <p className="text-xs text-uc-text-mute">
          {hasChecklist
            ? `PRÉPARATION TERMINÉE · ${inspection?.checklist_json?.sections?.length ?? 0} sections · ${inspection?.checklist_json?.sections?.reduce((a, s) => a + s.points.length, 0) ?? 0} points`
            : "Génération en cours…"}
        </p>
        <button
          disabled={!hasChecklist || startMutation.isPending}
          onClick={() => startMutation.mutate()}
          className="px-6 py-3 bg-uc-accent text-white font-bold rounded-xl disabled:opacity-50 disabled:cursor-not-allowed hover:bg-emerald-600 transition-colors"
        >
          Démarrer l&apos;inspection →
        </button>
      </footer>
    </div>
  );
}
