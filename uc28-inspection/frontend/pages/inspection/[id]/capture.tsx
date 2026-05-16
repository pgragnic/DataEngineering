import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/router";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import {
  getInspection,
  createConstat,
  generateReport,
  type Constat,
  type RagChunk,
} from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";
import ChecklistView from "@/components/ChecklistView";
import VoiceCapture from "@/components/VoiceCapture";
import PhotoCapture from "@/components/PhotoCapture";
import ConstatCard from "@/components/ConstatCard";
import RagTransparency from "@/components/RagTransparency";
import NCBadge from "@/components/NCBadge";

type CaptureState = "idle" | "processing" | "result" | "validated";

export default function CapturePage() {
  const router = useRouter();
  const id = router.query.id as string | undefined;
  const qc = useQueryClient();

  const { data: inspection, refetch } = useQuery({
    queryKey: ["inspection", id],
    queryFn: () => getInspection(id!),
    enabled: !!id,
    refetchInterval: 0,
  });

  const [captureState, setCaptureState] = useState<CaptureState>("idle");
  const [rawText, setRawText] = useState("");
  const [currentConstat, setCurrentConstat] = useState<Constat | null>(null);
  const [ragChunks, setRagChunks] = useState<RagChunk[]>([]);
  const [activePointId, setActivePointId] = useState<string | null>(null);
  const [photoId, setPhotoId] = useState<string | null>(null);
  const [photoPreview, setPhotoPreview] = useState<string | null>(null);
  const [showGeneratingModal, setShowGeneratingModal] = useState(false);
  const [generatingMessages] = useState([
    "Synthèse exécutive…",
    "Regroupement des constats par thème…",
    "Construction du plan d'action priorisé…",
    "Mise en forme du document…",
    "Génération du DOCX…",
  ]);
  const [genMsgIdx, setGenMsgIdx] = useState(0);
  const [textFallback, setTextFallback] = useState(false);

  const constatMutation = useMutation({
    mutationFn: (text: string) =>
      createConstat(id!, { raw_text: text, checklist_point_id: activePointId ?? undefined, photo_id: photoId ?? undefined }),
    onSuccess: (constat) => {
      setCurrentConstat(constat);
      setRagChunks(constat.rag_chunks ?? []);
      setCaptureState("result");
    },
    onError: () => setCaptureState("idle"),
  });

  const reportMutation = useMutation({
    mutationFn: () => generateReport(id!),
    onSuccess: () => {
      setShowGeneratingModal(false);
      router.push(`/inspection/${id}/report`);
    },
  });

  const handleTranscript = useCallback((text: string) => {
    setRawText(text);
    setCaptureState("processing");
    setRagChunks([]);
    constatMutation.mutate(text);
  }, [constatMutation]);

  const handleValidate = () => {
    if (!currentConstat) return;
    setCaptureState("validated");
    setTimeout(() => {
      refetch();
      setCaptureState("idle");
      setRawText("");
      setCurrentConstat(null);
      setPhotoId(null);
      setPhotoPreview(null);
    }, 300);
  };

  const handleRedo = () => {
    setCaptureState("idle");
    setRawText("");
    setCurrentConstat(null);
    setPhotoId(null);
    setPhotoPreview(null);
  };

  const handleGenerateReport = () => {
    setShowGeneratingModal(true);
    setGenMsgIdx(0);
    reportMutation.mutate();
  };

  useEffect(() => {
    if (!showGeneratingModal) return;
    const timer = setInterval(() => {
      setGenMsgIdx((i) => (i + 1) % generatingMessages.length);
    }, 2000);
    return () => clearInterval(timer);
  }, [showGeneratingModal, generatingMessages.length]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "t" || e.key === "T") setTextFallback((v) => !v);
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

  const validatedPointIds = new Set(
    (inspection?.constats ?? [])
      .filter((c) => c.checklist_point_id)
      .map((c) => c.checklist_point_id!)
  );

  const stats = {
    total: inspection?.constats?.length ?? 0,
    nc_majeure: inspection?.constats?.filter((c) => c.classification === "nc_majeure").length ?? 0,
    nc_mineure: inspection?.constats?.filter((c) => c.classification === "nc_mineure").length ?? 0,
    observation: inspection?.constats?.filter((c) => c.classification === "observation").length ?? 0,
    conforme: inspection?.constats?.filter((c) => c.classification === "conforme").length ?? 0,
  };

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="capture"
        title={`Audit ${inspection?.referential ?? ""} · ${inspection?.client_name ?? ""}`}
        subtitle={`${inspection?.site_name ?? ""} · Auditeur : ${inspection?.auditor_name ?? ""}`}
        startedAt={inspection?.started_at ?? undefined}
      />

      <main className="flex-1 grid grid-cols-12 gap-4 p-4 min-h-0 overflow-hidden">
        {/* Checklist — 3/12 */}
        <div className="col-span-3 overflow-hidden flex flex-col">
          <ChecklistView
            checklist={inspection?.checklist_json ?? null}
            activePointId={activePointId}
            validatedPointIds={validatedPointIds}
            onSelectPoint={setActivePointId}
          />
        </div>

        {/* Capture zone — 6/12 */}
        <div className="col-span-6 flex flex-col gap-3 overflow-hidden">
          {textFallback ? (
            <div className="bg-uc-bg-dark rounded-lg p-4 flex flex-col gap-2">
              <textarea
                className="bg-uc-bg-panel text-white rounded p-2 text-sm resize-none h-24 focus:outline-uc-accent"
                placeholder="Tapez votre constat ici… (Cmd+Enter pour valider)"
                value={rawText}
                onChange={(e) => setRawText(e.target.value)}
                onKeyDown={(e) => {
                  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
                    setCaptureState("processing");
                    setRagChunks([]);
                    constatMutation.mutate(rawText);
                  }
                }}
              />
              <p className="text-xs text-uc-text-mute">Cmd+Enter pour envoyer · T pour revenir au micro</p>
            </div>
          ) : (
            <VoiceCapture onTranscript={handleTranscript} />
          )}

          <AnimatePresence mode="wait">
            {captureState === "processing" && (
              <motion.div
                key="loading"
                initial={{ opacity: 0, y: 4 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0 }}
                className="bg-uc-bg-dark rounded-lg border border-uc-accent p-4 flex flex-col gap-3 animate-pulse"
              >
                <div className="h-4 bg-uc-bg-panel rounded w-2/3" />
                <div className="h-4 bg-uc-bg-panel rounded w-full" />
                <div className="h-4 bg-uc-bg-panel rounded w-1/2" />
                <p className="text-center text-sm text-uc-accent-lt">Analyse en cours…</p>
              </motion.div>
            )}

            {captureState === "result" && currentConstat && (
              <motion.div
                key="result"
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.2 }}
                className="rounded-lg border p-4 flex flex-col gap-2 bg-white border-uc-border"
              >
                <div className="flex items-center gap-2 flex-wrap">
                  <NCBadge level={currentConstat.classification} />
                  {currentConstat.norm_reference && (
                    <span className="font-mono text-xs bg-uc-primary-2 text-white px-2 py-0.5 rounded font-bold">
                      {currentConstat.norm_reference}
                    </span>
                  )}
                  {currentConstat.norm_reference && (
                    <span className="text-xs text-uc-accent border border-uc-accent px-2 py-0.5 rounded">
                      ✓ sourcé
                    </span>
                  )}
                  {photoPreview && (
                    <img src={photoPreview} alt="Photo" className="w-8 h-6 object-cover rounded-sm ml-auto" />
                  )}
                </div>
                <div>
                  <p className="text-xs text-uc-text-mute mb-0.5">Constat reformulé :</p>
                  <p className="text-sm font-medium text-uc-text-body">{currentConstat.reformulated_text}</p>
                </div>
                {currentConstat.suggested_action && (
                  <div>
                    <p className="text-xs text-uc-text-mute mb-0.5">Action corrective suggérée :</p>
                    <p className="text-xs text-uc-text-body">{currentConstat.suggested_action}</p>
                  </div>
                )}
                <div className="flex gap-2 mt-2">
                  <PhotoCapture
                    onUploaded={(pid, preview) => {
                      setPhotoId(pid);
                      setPhotoPreview(preview);
                    }}
                  />
                  <button
                    onClick={handleValidate}
                    className="flex-1 py-2 bg-uc-primary text-white text-sm font-semibold rounded-lg hover:bg-uc-primary-2 transition-colors"
                  >
                    Valider →
                  </button>
                  <button
                    onClick={handleRedo}
                    className="px-4 py-2 bg-white border border-uc-border text-uc-text-body text-sm rounded-lg hover:bg-uc-panel transition-colors"
                  >
                    Refaire
                  </button>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Constats list — 3/12 */}
        <div className="col-span-3 flex flex-col gap-2 overflow-y-auto">
          <p className="text-xs font-bold text-uc-text-mute uppercase tracking-wider">
            Constats ({inspection?.constats?.length ?? 0})
          </p>
          {(inspection?.constats ?? []).slice().reverse().map((c) => (
            <ConstatCard key={c.id} constat={c} />
          ))}
        </div>
      </main>

      <RagTransparency chunks={ragChunks} loading={captureState === "processing"} />

      <footer className="bg-uc-bg-dark px-6 py-4 flex items-center justify-between shrink-0">
        <div>
          <p className="text-xs text-uc-accent-lt font-bold uppercase tracking-wider">INSPECTION EN COURS</p>
          <p className="text-sm text-white">
            {stats.total} constats · {stats.nc_majeure} NC majeure · {stats.nc_mineure} NC mineure · {stats.observation} observation · {stats.conforme} conforme
          </p>
        </div>
        <button
          onClick={handleGenerateReport}
          disabled={stats.total === 0 || reportMutation.isPending}
          aria-label="Générer le pré-rapport"
          className="px-8 py-4 bg-uc-accent text-white text-lg font-bold rounded-xl disabled:opacity-50 disabled:cursor-not-allowed hover:bg-emerald-600 hover:scale-[1.02] transition-all"
        >
          Générer le pré-rapport →
        </button>
      </footer>

      <AnimatePresence>
        {showGeneratingModal && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="fixed inset-0 bg-uc-bg-dark/90 flex flex-col items-center justify-center gap-6 z-50"
          >
            <div className="w-16 h-16 border-4 border-uc-accent border-t-transparent rounded-full animate-spin" />
            <p className="text-uc-accent-lt text-lg font-semibold">{generatingMessages[genMsgIdx]}</p>
            <p className="text-uc-text-mute text-sm">Agent 3 — Restitution en cours…</p>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
