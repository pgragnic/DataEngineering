import { useCallback, useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
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
import RagTransparency from "@/components/RagTransparency";
import NCBadge from "@/components/NCBadge";

type CaptureState = "idle" | "processing" | "result" | "validated";

const BORDER: Record<string, string> = {
  nc_majeure: "border-uc-danger",
  nc_mineure: "border-uc-alert",
  observation: "border-uc-primary-2",
  conforme: "border-uc-accent",
};

function useElapsed(startedAt: string | null | undefined) {
  const [label, setLabel] = useState("00:00");
  useEffect(() => {
    if (!startedAt) return;
    const start = new Date(startedAt).getTime();
    const tick = () => {
      const s = Math.max(0, Math.floor((Date.now() - start) / 1000));
      setLabel(`${String(Math.floor(s / 60)).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`);
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [startedAt]);
  return label;
}

export default function CapturePage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const qc = useQueryClient();

  const { data: inspection, refetch } = useQuery({
    queryKey: ["inspection", id],
    queryFn: () => getInspection(id!),
    enabled: !!id,
    refetchInterval: 0,
  });

  const elapsed = useElapsed(inspection?.started_at);

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
      createConstat(id!, {
        raw_text: text,
        checklist_point_id: activePointId ?? undefined,
        photo_id: photoId ?? undefined,
      }),
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
      navigate(`/inspection/${id}/report`);
    },
  });

  const handleTranscript = useCallback(
    (text: string) => {
      setRawText(text);
      setCaptureState("processing");
      setRagChunks([]);
      constatMutation.mutate(text);
    },
    [constatMutation]
  );

  const handleValidate = () => {
    if (!currentConstat) return;
    setCaptureState("validated");
    setTimeout(() => {
      refetch();
      qc.invalidateQueries({ queryKey: ["inspection", id] });
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
    const timer = setInterval(() => setGenMsgIdx((i) => (i + 1) % generatingMessages.length), 2000);
    return () => clearInterval(timer);
  }, [showGeneratingModal, generatingMessages.length]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "t" || e.key === "T") setTextFallback((v) => !v);
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

  const constats = inspection?.constats ?? [];
  const validatedPointIds = new Set(
    constats.filter((c) => c.checklist_point_id).map((c) => c.checklist_point_id!)
  );
  const stats = {
    total: constats.length,
    nc_majeure: constats.filter((c) => c.classification === "nc_majeure").length,
    nc_mineure: constats.filter((c) => c.classification === "nc_mineure").length,
    observation: constats.filter((c) => c.classification === "observation").length,
    conforme: constats.filter((c) => c.classification === "conforme").length,
  };

  const startTime = inspection?.started_at
    ? new Date(inspection.started_at).toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })
    : null;

  return (
    <div className="h-dvh w-dvw flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="capture"
        title={`Audit ${inspection?.referential ?? ""} · ${inspection?.client_name ?? ""}`}
        subtitle={`${inspection?.site_name ?? ""} · Auditeur : ${inspection?.auditor_name ?? ""}`}
        startedAt={inspection?.started_at ?? undefined}
      />

      <main className="flex-1 grid grid-cols-12 gap-4 p-4 min-h-0 overflow-hidden">

        {/* ── CHECK-LIST ── 3/12 */}
        <div className="col-span-3 overflow-hidden flex flex-col">
          <ChecklistView
            checklist={inspection?.checklist_json ?? null}
            activePointId={activePointId}
            validatedPointIds={validatedPointIds}
            onSelectPoint={setActivePointId}
          />
        </div>

        {/* ── CAPTURE EN COURS ── 6/12 */}
        <div className="col-span-6 flex flex-col gap-3 overflow-hidden">
          <div>
            <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
              Capture en cours
              <span className="ml-1 font-normal normal-case">voix → classification temps réel</span>
            </p>
          </div>

          {/* Zone micro / saisie */}
          {textFallback ? (
            <div className="bg-uc-bg-dark rounded-lg p-4 flex flex-col gap-2">
              <textarea
                className="bg-uc-bg-panel text-white rounded p-2 text-sm resize-none h-20 focus:outline-none focus:ring-1 focus:ring-uc-accent"
                placeholder="Tapez votre constat ici… (Ctrl+Enter pour valider)"
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
              <p className="text-xs text-uc-text-mute">Ctrl+Enter pour envoyer · <kbd className="bg-uc-bg-panel px-1 rounded">T</kbd> pour revenir au micro</p>
            </div>
          ) : (
            <VoiceCapture onTranscript={handleTranscript} />
          )}

          {/* Transcript intermédiaire */}
          {rawText && captureState !== "idle" && (
            <p className="text-xs text-uc-text-mute italic px-1">
              → {rawText.slice(0, 100)}{rawText.length > 100 ? "…" : ""}
            </p>
          )}

          {/* Résultat de la classification */}
          <AnimatePresence mode="wait">
            {captureState === "processing" && (
              <motion.div
                key="loading"
                initial={{ opacity: 0, y: 4 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0 }}
                className="bg-uc-bg-dark rounded-lg border border-uc-accent p-4 flex flex-col gap-2 flex-1"
              >
                <div className="flex gap-3 items-center mb-2">
                  <div className="w-4 h-4 border-2 border-uc-accent border-t-transparent rounded-full animate-spin shrink-0" />
                  <span className="text-sm text-uc-accent-lt font-medium">Classification par l&apos;Agent 2…</span>
                </div>
                {[2, 3, 2].map((w, i) => (
                  <div key={i} className={`h-3 bg-uc-bg-panel rounded animate-pulse w-${w}/3`} />
                ))}
              </motion.div>
            )}

            {captureState === "result" && currentConstat && (
              <motion.div
                key="result"
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.2 }}
                className={`rounded-lg border-l-4 ${BORDER[currentConstat.classification] ?? "border-uc-border"} border border-uc-border bg-white p-4 flex flex-col gap-3`}
              >
                {/* Badges */}
                <div className="flex items-center gap-2 flex-wrap">
                  <NCBadge level={currentConstat.classification} />
                  {currentConstat.norm_reference && (
                    <span className="font-mono text-xs bg-uc-primary text-white px-2 py-0.5 rounded font-bold">
                      {currentConstat.norm_reference}
                    </span>
                  )}
                  {currentConstat.norm_reference && (
                    <span className="text-xs text-uc-accent border border-uc-accent px-2 py-0.5 rounded font-medium">
                      ✓ sourcé
                    </span>
                  )}
                  {photoPreview && (
                    <img src={photoPreview} alt="" className="w-8 h-6 object-cover rounded ml-auto" />
                  )}
                </div>

                {/* Constat reformulé */}
                <div>
                  <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider mb-1">Constat reformulé :</p>
                  <p className="text-sm text-uc-text-dark font-medium leading-snug">
                    {currentConstat.reformulated_text}
                  </p>
                </div>

                {/* Action corrective */}
                {currentConstat.suggested_action && (
                  <div>
                    <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider mb-1">Action corrective suggérée :</p>
                    <p className="text-xs text-uc-text-body leading-snug">
                      {currentConstat.suggested_action}
                    </p>
                  </div>
                )}

                {/* Actions */}
                <div className="flex gap-2 mt-1">
                  <PhotoCapture
                    onUploaded={(pid, preview) => { setPhotoId(pid); setPhotoPreview(preview); }}
                  />
                  <button
                    onClick={handleValidate}
                    className="flex-1 py-2 bg-uc-primary text-white text-sm font-bold rounded-lg hover:bg-uc-primary-2 transition-colors"
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

        {/* ── CONSTATS ── 3/12 */}
        <div className="col-span-3 flex flex-col gap-2 overflow-y-auto">
          <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
            Constats ({stats.total})
          </p>
          {constats.slice().reverse().map((c) => (
            <motion.div
              key={c.id}
              initial={{ x: 40, opacity: 0 }}
              animate={{ x: 0, opacity: 1 }}
              transition={{ duration: 0.3, type: "spring", bounce: 0.3 }}
              className={`bg-white rounded-lg border-l-4 ${BORDER[c.classification] ?? "border-uc-border"} border border-uc-border p-3`}
            >
              <div className="flex items-center gap-1.5 flex-wrap">
                <NCBadge level={c.classification} />
                {c.norm_reference && (
                  <span className="font-mono text-[10px] text-uc-text-mute">{c.norm_reference}</span>
                )}
                {c.photo_path && <span className="text-uc-text-mute text-xs ml-auto">📷</span>}
              </div>
              <p className="text-xs text-uc-text-body mt-1.5 line-clamp-2 leading-snug">
                {c.reformulated_text}
              </p>
            </motion.div>
          ))}
        </div>
      </main>

      {/* ── RAG ── */}
      <RagTransparency chunks={ragChunks} loading={captureState === "processing"} />

      {/* ── FOOTER ── */}
      <footer className="bg-uc-bg-dark px-6 py-3 flex items-center justify-between shrink-0">
        <div>
          <p className="text-xs font-bold text-uc-accent-lt uppercase tracking-wider">INSPECTION EN COURS</p>
          <p className="text-xs text-white mt-0.5">
            {stats.total} constat{stats.total > 1 ? "s" : ""} · {stats.nc_majeure} NC majeure · {stats.nc_mineure} NC mineure · {stats.observation} observation · {stats.conforme} conforme
          </p>
          {startTime && (
            <p className="text-[10px] text-uc-text-mute mt-0.5">
              Démarré à {startTime} · Durée : {elapsed}
            </p>
          )}
        </div>
        <button
          onClick={handleGenerateReport}
          disabled={stats.total === 0 || reportMutation.isPending}
          className="px-8 py-4 bg-uc-accent text-white text-base font-bold rounded-xl disabled:opacity-50 disabled:cursor-not-allowed hover:bg-emerald-600 hover:scale-[1.02] transition-all"
        >
          Générer le pré-rapport →
        </button>
      </footer>

      {/* ── MODAL GÉNÉRATION ── */}
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
