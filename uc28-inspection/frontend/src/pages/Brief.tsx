import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { motion } from "framer-motion";
import { getInspection, generateChecklist, updateInspection } from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";

const PREV_AUDITS = [
  { date: "2025-06-15", label: "Audit ok", nc: "NC mineure → à vérifier", alert: true },
  { date: "2024-07-22", label: "Audit ok", nc: "3 observations", alert: false },
  { date: "2023-09-04", label: "NC mineure", nc: "1 observation", alert: false },
];

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
  }, [inspection]); // eslint-disable-line react-hooks/exhaustive-deps

  const hasChecklist = Boolean(inspection?.checklist_json);
  const sections = inspection?.checklist_json?.sections ?? [];
  const totalPoints = sections.reduce((acc, s) => acc + s.points.length, 0);
  const scopeBullets = inspection?.scope
    ? inspection.scope.split(/[.·]/).map((s) => s.trim()).filter((s) => s.length > 6).slice(0, 4)
    : [];

  return (
    <div className="h-dvh w-dvw flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="brief"
        title={`Audit ${inspection?.referential ?? ""} · ${inspection?.client_name ?? ""}`}
        subtitle={`${inspection?.site_name ?? ""} · auditeur · ${inspection?.auditor_name ?? ""}`}
      />

      <main className="flex-1 grid grid-cols-12 gap-4 p-4 min-h-0 overflow-hidden">

        {/* ── BRIEF CLIENT ── 3/12 */}
        <div className="col-span-3 bg-white rounded-xl shadow-sm p-4 overflow-y-auto flex flex-col gap-3">
          <h2 className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
            Brief client
            <span className="ml-1 font-normal normal-case">données saisies ou pré-enregistrées</span>
          </h2>
          {inspection && (
            <>
              {[
                { label: "Client", value: inspection.client_name },
                { label: "SIRET", value: (inspection as any).client_siret ?? "—" },
                { label: "Site", value: `${(inspection as any).site_address ?? inspection.site_name}` },
                { label: "Référentiel", value: inspection.referential },
              ].map(({ label, value }) => (
                <div key={label}>
                  <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">{label}</p>
                  <p className="text-xs text-uc-text-body mt-0.5">{value}</p>
                </div>
              ))}

              {scopeBullets.length > 0 && (
                <div>
                  <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">Scope</p>
                  <ul className="mt-0.5 flex flex-col gap-0.5">
                    {scopeBullets.map((b, i) => (
                      <li key={i} className="text-xs text-uc-text-body flex gap-1">
                        <span className="text-uc-accent shrink-0">•</span>
                        <span>{b}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              <div>
                <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">Contact</p>
                <p className="text-xs text-uc-text-body mt-0.5">Marie Lemaitre</p>
                <p className="text-[10px] text-uc-text-mute">Responsable Qualité</p>
              </div>

              <div>
                <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">Durée prévue</p>
                <p className="text-xs text-uc-text-body mt-0.5">2 h sur site</p>
              </div>
            </>
          )}
        </div>

        {/* ── CHECK-LIST AGENT 1 ── 6/12 */}
        <div className="col-span-6 flex flex-col gap-3 overflow-hidden">
          <h2 className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
            Check-list générée par l&apos;Agent 1
            <span className="ml-1 font-normal normal-case">RAG ISO 9001 + scope client + historique fournisseur</span>
          </h2>

          {generating && (
            <div className="bg-uc-accent-50 border border-uc-accent rounded-lg p-4 flex items-center gap-3">
              <div className="w-5 h-5 border-2 border-uc-accent border-t-transparent rounded-full animate-spin shrink-0" />
              <div>
                <p className="text-sm font-semibold text-uc-primary">Préparation par l&apos;Agent 1…</p>
                <p className="text-xs text-uc-text-mute">Croisement scope · normes ISO 9001 · historique fournisseur</p>
              </div>
            </div>
          )}

          {hasChecklist && generationSeconds !== null && (
            <motion.div
              initial={{ opacity: 0, y: -4 }}
              animate={{ opacity: 1, y: 0 }}
              className="bg-uc-accent-50 border border-uc-accent rounded-lg p-3 flex items-center justify-between"
            >
              <div className="flex items-center gap-2">
                <span className="text-uc-accent font-bold text-lg">✓</span>
                <span className="text-sm text-uc-primary font-semibold">
                  Préparation terminée — {generationSeconds} secondes
                </span>
              </div>
              <span className="text-xs text-uc-text-mute">
                {sections.length} sections · {totalPoints} points · {sections.length} actions ISO sources
              </span>
            </motion.div>
          )}

          <div className="flex-1 overflow-y-auto flex flex-col gap-2">
            {hasChecklist && sections.map((section) => (
              <motion.div
                key={section.id}
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                className="bg-white rounded-lg border border-uc-border p-3 flex items-start gap-3"
              >
                <span className="font-mono text-xs font-bold text-uc-primary bg-uc-panel px-2 py-1 rounded shrink-0">
                  {section.id}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-semibold text-uc-text-dark">{section.title}</p>
                  <p className="text-[10px] text-uc-text-mute mt-0.5">
                    {section.points.length} point{section.points.length > 1 ? "s" : ""} · sources ISO 9001
                  </p>
                </div>
              </motion.div>
            ))}

            {!hasChecklist && !generating && (
              <div className="flex-1 flex items-center justify-center text-uc-text-mute text-sm">
                En attente de génération…
              </div>
            )}
          </div>
        </div>

        {/* ── AUDITS PRÉCÉDENTS ── 3/12 */}
        <div className="col-span-3 flex flex-col gap-3 overflow-y-auto">
          <h2 className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
            Audits précédents
            <span className="ml-1 font-normal normal-case">Fournisseur · 3 derniers audits</span>
          </h2>

          {PREV_AUDITS.map((a) => (
            <div
              key={a.date}
              className={`rounded-lg border p-3 ${
                a.alert ? "border-uc-alert bg-uc-alert-50" : "border-uc-border bg-white"
              }`}
            >
              <p className="font-mono text-xs font-bold text-uc-text-mute">{a.date}</p>
              <p className="text-xs font-semibold text-uc-text-dark mt-0.5">{a.label}</p>
              <p className={`text-xs mt-0.5 ${a.alert ? "text-uc-alert font-medium" : "text-uc-text-mute"}`}>
                {a.nc}
              </p>
            </div>
          ))}

          <div className="bg-uc-bg-dark rounded-lg p-3 border-l-4 border-uc-alert mt-1">
            <p className="text-xs font-bold text-uc-alert mb-1">⚠ Point d&apos;attention</p>
            <p className="text-xs text-uc-accent-lt leading-snug">
              La NC mineure de 2025 n&apos;est pas clôturée → point S2 inclus automatiquement par l&apos;Agent 1 — ne pas l&apos;ignorer
            </p>
          </div>
        </div>
      </main>

      <footer className="bg-uc-bg-dark p-4 flex items-center justify-between shrink-0">
        <div>
          <p className="text-xs font-bold text-uc-accent-lt uppercase tracking-wider">
            {hasChecklist ? "PRÉPARATION TERMINÉE" : "GÉNÉRATION EN COURS…"}
          </p>
          {hasChecklist && (
            <p className="text-[10px] text-uc-text-mute mt-0.5">
              Check-list générée · {generationSeconds ?? "—"} sec · {sections.length} sections · {totalPoints} points · 1 point récurrent
            </p>
          )}
        </div>
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
