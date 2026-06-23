import { useEffect, useRef, useState } from "react"
import { Mic, MicOff, Camera, ScanLine, CheckCircle2, RotateCcw, ClipboardList, BookOpen, MessageSquare, Circle, Pencil, Check, X, PenLine, Info } from "lucide-react"
import { CHECKLIST, RAG_ARTICLES, AUDIT_COURANT, QUESTIONS_SUGGEREES, RECURRENCES, SUPPLIER_DOCUMENTS } from "../mockData"
import { analyser, synthetiser, getSuggestions, getQuestionsOuiNon, transcrireManuscrit } from "../api"
import { useT } from "../useT"

const CRITICITE_STYLE = {
  majeure:     { badge: "bg-nc-majeure text-white",  card: "border-red-200 bg-red-50",         label: "NC MAJEURE",  border: "border-l-4 border-nc-majeure"  },
  mineure:     { badge: "bg-nc-mineure text-white",  card: "border-amber-200 bg-amber-50",     label: "NC MINEURE",  border: "border-l-4 border-nc-mineure"  },
  observation: { badge: "bg-observation text-white", card: "border-emerald-200 bg-emerald-50", label: "OBSERVATION", border: "border-l-4 border-observation" },
  conforme:    { badge: "bg-conforme text-white",    card: "border-teal-200 bg-teal-50",       label: "CONFORME",    border: "border-l-4 border-conforme"    },
}

const EXEMPLES = [
  "Clé dynamométrique poste 7 sans étiquette d'étalonnage — dernier étalonnage non retrouvé",
  "Zone quarantaine pièces non conformes mal délimitée — pièces conformes et NC côte à côte",
  "Nouvel opérateur intervient seul sur opérations de freinage sans habilitation validée",
]

const QUESTIONS_FALLBACK = [
  "La situation observée est-elle conforme aux exigences ?",
  "Des preuves documentaires sont-elles disponibles ?",
  "Les responsables concernés sont-ils informés ?",
]

function articlesForClause(clause) {
  const c = (clause || "").trim()
  if (RAG_ARTICLES[c]) return RAG_ARTICLES[c]
  const num = c.replace(/§/g, "")
  const key = num && Object.keys(RAG_ARTICLES).find(
    (k) => k !== "default" && num.includes(k.replace(/§/g, ""))
  )
  return key ? RAG_ARTICLES[key] : RAG_ARTICLES.default
}

function Waveform({ active }) {
  const bars = Array.from({ length: 32 }, (_, i) => i)
  return (
    <div className="flex items-center justify-center gap-0.5 h-16 bg-gray-900 rounded-lg px-4">
      {bars.map((i) => (
        <div
          key={i}
          className={`w-1 rounded-full transition-all ${active ? "bg-green-400" : "bg-gray-600"}`}
          style={{
            height: active ? `${8 + Math.sin(i * 0.8 + Date.now() * 0.001) * 12 + Math.random() * 16}px` : "4px",
            animation: active ? `pulse ${0.3 + (i % 5) * 0.1}s ease-in-out infinite alternate` : "none",
          }}
        />
      ))}
    </div>
  )
}

export default function InspectionCapture({ constats, onAddConstat, onUpdateConstat, onDeleteConstat, onGenererRapport, onBack, startTime, theme, lang, customSections = [], extraItemsBySectionId = {}, removedItemIds = new Set(), removedSectionIds = new Set(), onFeedback }) {
  const t = useT(lang)
  const ag = theme === "agile"
  const ar = theme === "aria"

  const baseChecklist = CHECKLIST.filter(s => !removedSectionIds.has(s.id)).map((s) => ({
    ...s,
    items: [
      ...s.items.filter((it) => !removedItemIds.has(it.id)).map((it) => ({ ...it })),
      ...(extraItemsBySectionId[s.id] || []).map((p) => ({ id: p.id, texte: p.texte, statut: "a-venir", auteur: "auditeur" })),
    ],
  }))
  customSections.forEach((sec) => {
    baseChecklist.push({
      id: sec.id,
      titre: sec.titre,
      clause: sec.clause || "—",
      points: sec.items.length,
      auteur: "auditeur",
      items: sec.items.map((p) => ({ id: p.id, texte: p.texte, statut: "a-venir", auteur: "auditeur" })),
    })
  })

  const [checklist, setChecklist] = useState(baseChecklist)
  const [observation, setObservation] = useState("")
  const [loading, setLoading] = useState(false)
  const [resultat, setResultat] = useState(null)
  const [error, setError] = useState(null)
  const [waveActive, setWaveActive] = useState(false)
  const [timerStr, setTimerStr] = useState("00:00")
  const [ragArticles, setRagArticles] = useState(RAG_ARTICLES.default)
  const [isRecording, setIsRecording] = useState(false)
  const [photo, setPhoto] = useState(null)
  const [momentFort, setMomentFort] = useState(false)
  const [synthese, setSynthese] = useState(null)
  const [syntheseLoading, setSyntheseLoading] = useState(false)
  const [selectedItem, setSelectedItem] = useState(null)
  const [selectionCount, setSelectionCount] = useState(0)
  const [suggestions, setSuggestions] = useState(EXEMPLES)
  const [suggestionsLoading, setSuggestionsLoading] = useState(false)
  const [reponses, setReponses] = useState({})
  const [questionsOuiNon, setQuestionsOuiNon] = useState([])
  const [questionsLoading, setQuestionsLoading] = useState(false)
  const [feedbackDonne, setFeedbackDonne] = useState(null)
  const [correctionTexte, setCorrectionTexte] = useState("")
  const [editingConstatId, setEditingConstatId] = useState(null)
  const [editDraft, setEditDraft] = useState({ constat: "", action: "" })
  const [canvasMode, setCanvasMode] = useState(false)
  const [isDrawing, setIsDrawing] = useState(false)
  const [transcribeLoading, setTranscribeLoading] = useState(false)
  const [showQuestionsInfo, setShowQuestionsInfo] = useState(false)
  const [showRagInfo, setShowRagInfo] = useState(false)
  const [showSynthInfo, setShowSynthInfo] = useState(false)
  const canvasRef = useRef(null)
  const lastPoint = useRef(null)

  function toStatement(q, rep) {
    let txt = q.replace(/\s*\?$/, "").trim()
    const affirm = { "est-elle": "est", "est-il": "est", "est-on": "est", "sont-ils": "sont", "sont-elles": "sont", "a-t-il": "a", "a-t-elle": "a", "ont-ils": "ont", "ont-elles": "ont", "avez-vous": "vous avez", "y-a-t-il": "il y a" }
    const neg    = { "est-elle": "n'est pas", "est-il": "n'est pas", "est-on": "n'est pas", "sont-ils": "ne sont pas", "sont-elles": "ne sont pas", "a-t-il": "n'a pas", "a-t-elle": "n'a pas", "ont-ils": "n'ont pas", "ont-elles": "n'ont pas", "avez-vous": "vous n'avez pas", "y-a-t-il": "il n'y a pas" }
    const map = rep === "oui" ? affirm : neg
    for (const [from, to] of Object.entries(map)) {
      txt = txt.replace(new RegExp(`\\b${from}\\b`, "gi"), to)
    }
    const invRe = /(\w+)-t?-?(il|elle|ils|elles|on)\b/gi
    if (rep === "oui") {
      txt = txt.replace(invRe, "$1")
    } else {
      txt = txt.replace(invRe, (_, verb) => `ne ${verb} pas`)
    }
    txt = txt.replace(/\s+/g, " ").trim()
    return txt.charAt(0).toUpperCase() + txt.slice(1) + "."
  }

  function appendToObservation(statement) {
    setObservation(prev => {
      const txt = prev.trim()
      if (!txt) return statement
      return txt.endsWith(".") || txt.endsWith(",") ? txt + " " + statement : txt + ". " + statement
    })
  }

  const waveInterval = useRef(null)
  const speechRef = useRef(null)
  const photoInputRef = useRef(null)
  const momentFortTimer = useRef(null)

  const speechSupported = typeof window !== "undefined" && !!(window.SpeechRecognition || window.webkitSpeechRecognition)

  useEffect(() => {
    const id = setInterval(() => {
      const elapsed = Math.floor((Date.now() - startTime) / 1000)
      const m = String(Math.floor(elapsed / 60)).padStart(2, "0")
      const s = String(elapsed % 60).padStart(2, "0")
      setTimerStr(`${m}:${s}`)
    }, 1000)
    return () => clearInterval(id)
  }, [startTime])

  useEffect(() => {
    if (waveActive) waveInterval.current = setInterval(() => {}, 100)
    return () => clearInterval(waveInterval.current)
  }, [waveActive])

  useEffect(() => {
    return () => { speechRef.current?.stop(); clearTimeout(momentFortTimer.current) }
  }, [])

  useEffect(() => {
    if (!selectedItem) { setSuggestions(EXEMPLES); return }
    setSuggestionsLoading(true)
    getSuggestions(selectedItem.texte, selectedItem.clause, selectedItem.sectionTitre)
      .then((data) => setSuggestions(Array.isArray(data.suggestions) && data.suggestions.length === 3 ? data.suggestions : EXEMPLES))
      .catch(() => setSuggestions(EXEMPLES))
      .finally(() => setSuggestionsLoading(false))
  }, [selectionCount])

  useEffect(() => {
    if (!selectedItem) return
    setRagArticles(articlesForClause(selectedItem.clause))
  }, [selectionCount])

  useEffect(() => {
    if (!selectedItem) { setQuestionsOuiNon([]); setReponses({}); return }
    setReponses({})
    setQuestionsLoading(true)
    getQuestionsOuiNon(selectedItem.texte, selectedItem.clause, selectedItem.sectionTitre)
      .then((data) => setQuestionsOuiNon(data.questions?.length ? data.questions : QUESTIONS_FALLBACK))
      .catch(() => setQuestionsOuiNon(QUESTIONS_FALLBACK))
      .finally(() => setQuestionsLoading(false))
  }, [selectionCount])

  function toggleRecording() {
    if (isRecording) { speechRef.current?.stop(); setIsRecording(false); return }
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition
    if (!SR) return
    const rec = new SR()
    rec.lang = lang === "FR" ? "fr-FR" : "en-US"
    rec.continuous = true; rec.interimResults = true
    rec.onresult = (e) => setObservation(Array.from(e.results).map((r) => r[0].transcript).join(""))
    rec.onerror = () => setIsRecording(false)
    rec.onend = () => setIsRecording(false)
    speechRef.current = rec; rec.start(); setIsRecording(true)
  }

  function handlePhotoChange(e) {
    const file = e.target.files?.[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = (ev) => setPhoto({ url: ev.target.result, name: file.name })
    reader.readAsDataURL(file)
    e.target.value = ""
  }

  async function handleSynthetiser() {
    if (!observation.trim()) return
    setSyntheseLoading(true); setSynthese(null)
    try { const data = await synthetiser(observation.trim()); setSynthese(data.synthese) }
    catch { /* silencieux */ }
    finally { setSyntheseLoading(false) }
  }

  async function handleAnalyser() {
    if (!observation.trim()) return
    setLoading(true); setError(null); setResultat(null)
    try {
      const reponsesLignes = questionsOuiNon
        .map((q, i) => reponses[i] ? `- ${q} → ${reponses[i] === "oui" ? "Oui ✓" : "Non ✗"}` : null)
        .filter(Boolean)
      const contextPrefix = selectedItem
        ? `Point de contrôle : ${selectedItem.texte}\nClause ISO : ${selectedItem.clause} — ${selectedItem.sectionTitre}\n\nObservation terrain : `
        : ""
      const observationAvecReponses = contextPrefix + (reponsesLignes.length > 0
        ? `${observation.trim()}\n\nPoints de vérification :\n${reponsesLignes.join("\n")}`
        : observation.trim())
      const data = await analyser(observationAvecReponses, AUDIT_COURANT.site_id)
      setResultat(data)
      setRagArticles(articlesForClause(data.clause_iso?.clause))
      if ((data.criticite || "").toLowerCase() === "majeure") {
        clearTimeout(momentFortTimer.current)
        setMomentFort(true)
        momentFortTimer.current = setTimeout(() => setMomentFort(false), 3500)
      }
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  function handleValider() {
    if (!resultat) return
    const criticite = (resultat.criticite || "observation").toLowerCase()
    onAddConstat({
      id: Date.now(),
      observation: observation.trim(),
      criticite,
      clause: resultat.clause_iso?.clause || "—",
      titre_clause: resultat.clause_iso?.titre || "—",
      constat: resultat.llm_enrichi
        ? (resultat.diagnostic || observation.trim())
        : observation.trim(),
      action: resultat.action_corrective || "",
      photoUrl: photo?.url || null,
    })
    setChecklist((prev) => {
      const next = prev.map((s) => ({ ...s, items: s.items.map((it) => ({ ...it })) }))
      let marked = false
      if (selectedItem) {
        for (const sec of next) {
          const item = sec.items.find((it) => it.id === selectedItem.id)
          if (item) { item.statut = "valide"; marked = true; break }
        }
      }
      if (!marked) {
        for (const sec of next) {
          const item = sec.items.find((it) => it.statut === "a-venir")
          if (item) { item.statut = "valide"; break }
        }
      }
      return next
    })
    setSelectedItem(null); setResultat(null); setObservation(""); setPhoto(null)
    setMomentFort(false); setSynthese(null); setFeedbackDonne(null); setCorrectionTexte("")
    setCanvasMode(false)
    clearTimeout(momentFortTimer.current)
  }

  function handleRefaire() {
    setResultat(null); setObservation(""); setError(null); setMomentFort(false)
    setSynthese(null); setFeedbackDonne(null); setCorrectionTexte("")
    setCanvasMode(false)
    clearTimeout(momentFortTimer.current)
  }

  function getCanvasPoint(e) {
    const rect = canvasRef.current.getBoundingClientRect()
    const scaleX = canvasRef.current.width / rect.width
    const scaleY = canvasRef.current.height / rect.height
    const clientX = e.touches ? e.touches[0].clientX : e.clientX
    const clientY = e.touches ? e.touches[0].clientY : e.clientY
    return { x: (clientX - rect.left) * scaleX, y: (clientY - rect.top) * scaleY }
  }

  function handlePointerDown(e) {
    e.currentTarget.setPointerCapture(e.pointerId)
    setIsDrawing(true)
    lastPoint.current = getCanvasPoint(e)
  }

  function handlePointerMove(e) {
    if (!isDrawing) return
    const ctx = canvasRef.current.getContext("2d")
    const pt = getCanvasPoint(e)
    ctx.beginPath()
    ctx.moveTo(lastPoint.current.x, lastPoint.current.y)
    ctx.lineTo(pt.x, pt.y)
    ctx.strokeStyle = "#1a1a1a"
    ctx.lineWidth = e.pressure > 0 && e.pointerType === "pen" ? Math.max(1, e.pressure * 4) : 2
    ctx.lineCap = "round"
    ctx.lineJoin = "round"
    ctx.stroke()
    lastPoint.current = pt
  }

  function handlePointerUp() { setIsDrawing(false); lastPoint.current = null }

  function clearCanvas() {
    const c = canvasRef.current
    if (c) c.getContext("2d").clearRect(0, 0, c.width, c.height)
  }

  async function handleTranscrire() {
    if (!canvasRef.current) return
    const dataUrl = canvasRef.current.toDataURL("image/png")
    const base64 = dataUrl.split(",")[1]
    setTranscribeLoading(true)
    const { texte } = await transcrireManuscrit(base64, selectedItem?.texte || "")
    setTranscribeLoading(false)
    if (texte) {
      setObservation(prev => prev ? prev + " " + texte : texte)
      setCanvasMode(false)
      clearCanvas()
    }
  }

  const counts = {
    majeure: constats.filter((c) => c.criticite === "majeure").length,
    mineure: constats.filter((c) => c.criticite === "mineure").length,
    observation: constats.filter((c) => c.criticite === "observation").length,
    conforme: constats.filter((c) => c.criticite === "conforme").length,
  }

  const criticiteResultat = resultat ? (resultat.criticite || "observation").toLowerCase() : null
  const clauseStr = resultat?.clause_iso?.clause || ""
  const recurrencesSite = RECURRENCES.find((r) => r.site_id === AUDIT_COURANT.site_id)
  const recidiveDetectee = resultat && recurrencesSite
    ? recurrencesSite.items.find((item) => { const match = item.match(/§([\d.]+)/); return match && clauseStr.includes(match[1]) })
    : null

  return (
    <div className="flex-1 flex flex-col min-h-0">
      {/* Barre haut */}
      <div className="page-bar">
        <button onClick={onBack} className="text-xs text-brand hover:underline flex items-center gap-1">{t("inspection.back")}</button>
        <div className="text-xs text-ink-muted flex items-center gap-3 flex-1 justify-center min-w-0 overflow-hidden">
          <span className="font-semibold text-ink truncate min-w-0">{lang === "EN" ? (AUDIT_COURANT.titreEN || AUDIT_COURANT.titre) : AUDIT_COURANT.titre}</span>
          <span className="text-divider shrink-0">·</span>
          <span className="text-brand-cyan truncate min-w-0">{AUDIT_COURANT.nom}</span>
          <span className="text-divider shrink-0">·</span>
          <span className="shrink-0">{constats.length === 1 ? t("inspection.findings_singular", { n: 1 }) : t("inspection.findings_plural", { n: constats.length })}</span>
          {counts.majeure > 0 && <span className="text-nc-majeure font-medium">{counts.majeure} {t("inspection.nc_maj_short")}</span>}
          {counts.mineure > 0 && <span className="text-nc-mineure font-medium">{counts.mineure} {t("inspection.nc_min_short")}</span>}
          {counts.observation > 0 && <span className="text-observation">{counts.observation} {t("inspection.obs_short")}</span>}
          {counts.conforme > 0 && <span className="text-conforme">{counts.conforme} {t("inspection.compliant_short")}</span>}
          <span className="text-ink-muted">· {timerStr}</span>
        </div>
        <button
          onClick={onGenererRapport}
          disabled={constats.length === 0}
          className="disabled:bg-divider disabled:text-ink-muted disabled:cursor-not-allowed text-white font-semibold px-5 py-2 rounded-lg text-sm bg-brand hover:bg-brand-cyan transition-colors shadow-sm"
        >
          {t("inspection.generate_prereport")}
        </button>
      </div>

      <div className="flex-1 p-4 w-full overflow-hidden flex flex-col">
        <div className="grid grid-cols-12 gap-4 flex-1 min-h-0">

          {/* Colonne gauche — Check-list */}
          <div className="col-span-4 card overflow-y-auto min-h-0">
            <h3 className="section-label">
              <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><ClipboardList size={11} className="text-brand" /></span>{t("inspection.checklist_title")}
            </h3>
            {recurrencesSite?.items.length > 0 && (
              <div className="mb-3 flex items-start gap-2 bg-brand-amber/10 border border-brand-amber/40 rounded-lg px-3 py-2">
                <span className="text-brand-amber shrink-0 leading-none mt-0.5 text-sm">🔁</span>
                <div>
                  <div className="text-[10px] font-bold text-brand-amber uppercase tracking-wide mb-0.5">{t("inspection.recurring_points")}</div>
                  <ul className="space-y-0.5">
                    {recurrencesSite.items.map((item, i) => (
                      <li key={i} className="text-[10px] text-ink-muted leading-snug">· {item}</li>
                    ))}
                  </ul>
                </div>
              </div>
            )}
            {checklist.map((section) => {
              const sectionTitle = lang === "EN" && section.titreEN ? section.titreEN : section.titre
              return (
              <div key={section.id} className="mb-4">
                <div className="flex items-center justify-between gap-2 mb-1">
                  <span className={`text-xs font-bold flex items-center gap-1.5 min-w-0 ${section.auteur === "auditeur" ? "text-brand-emerald" : "text-brand"}`}>
                    <span className="truncate">{section.id} — {sectionTitle}</span>
                    {section.auteur === "auditeur" && <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-brand text-white shrink-0">{t("inspection.auditor_badge")}</span>}
                    {recurrencesSite?.items.some(item => item.includes(section.clause.replace(/§/g, ""))) && (
                      <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-brand-amber text-white shrink-0">🔁</span>
                    )}
                  </span>
                  <span className="text-[10px] text-ink-muted font-mono shrink-0 flex items-center gap-1">
                    <span className="text-[9px] text-ink-muted/60">{AUDIT_COURANT.referentiel}</span>
                    {section.clause}
                  </span>
                </div>
                {section.items.map((item) => {
                  const isSelected = selectedItem?.id === item.id
                  const itemText = lang === "EN" && item.texteEN ? item.texteEN : item.texte
                  return (
                    <div
                      key={item.id}
                      onClick={() => { setSelectedItem({ id: item.id, texte: itemText, clause: section.clause, sectionId: section.id, sectionTitre: sectionTitle }); setSelectionCount(c => c + 1) }}
                      className={`flex items-center gap-2 py-0.5 px-1.5 rounded cursor-pointer transition-colors ${
                        isSelected ? "bg-brand/10 ring-1 ring-brand/30" : "hover:bg-canvas"
                      }`}
                    >
                      {item.statut === "valide"
                        ? <CheckCircle2 size={13} className="text-brand-emerald shrink-0" />
                        : item.statut === "actif"
                        ? <Circle size={13} className="text-brand shrink-0" />
                        : <Circle size={13} className="text-divider shrink-0" />
                      }
                      <span className={`text-xs min-w-0 truncate ${
                        item.statut === "valide" ? "text-ink-muted line-through" :
                        isSelected ? "text-brand font-semibold" :
                        item.statut === "actif" ? "text-ink font-medium" : "text-ink-muted"
                      }`}>
                        {itemText}
                      </span>
                      {item.auteur === "auditeur" && (
                        <span className="ml-auto text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-brand text-white shrink-0">{t("inspection.auditor_badge")}</span>
                      )}
                    </div>
                  )
                })}
              </div>
              )
            })}
          </div>

          {/* Colonne centre — Capture + Réponses + RAG */}
          <div className={`col-span-4 flex flex-col gap-3 overflow-y-auto min-h-0 transition-opacity duration-200 ${!selectedItem ? "opacity-40 pointer-events-none select-none" : ""}`}>
            <div className="bg-surface rounded-xl shadow-md p-4">
              {selectedItem ? (
                <div className="mb-3">
                  <div className="text-[10px] font-bold text-ink-muted uppercase tracking-widest">{t("inspection.current_point_label")}</div>
                  <div className="text-sm font-semibold mt-0.5 text-brand">{selectedItem.texte}</div>
                </div>
              ) : (
                <h3 className="text-[10px] font-bold text-ink-muted uppercase tracking-widest mb-3">
                  {t("inspection.capture_header")} — <span className="font-normal normal-case">{t("inspection.select_point_hint")}</span>
                </h3>
              )}
              <Waveform active={isRecording} />

              <div className="mt-3">
                <div className="relative">
                  <textarea
                    value={observation}
                    onChange={(e) => setObservation(e.target.value)}
                    placeholder={isRecording ? t("inspection.recording_placeholder") : t("inspection.observation_placeholder")}
                    rows={3}
                    className={`w-full border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-brand resize-none pr-16 ${
                      isRecording ? "border-red-400 bg-red-50" : canvasMode ? "border-brand" : "border-divider"
                    }`}
                  />
                  {speechSupported && (
                    <button
                      onClick={toggleRecording}
                      className={`absolute right-9 top-2 p-1.5 rounded-full transition-all ${
                        isRecording ? "bg-red-500 text-white animate-pulse shadow-md shadow-red-300" : "text-ink-muted hover:text-ink hover:bg-canvas"
                      }`}
                    >
                      {isRecording ? <MicOff size={16} /> : <Mic size={16} />}
                    </button>
                  )}
                  <button
                    onClick={() => setCanvasMode(v => !v)}
                    title={t("inspection.handwriting_title")}
                    className={`absolute right-2 top-2 p-1.5 rounded-full transition-all ${
                      canvasMode ? "bg-brand text-white" : "text-ink-muted hover:text-ink hover:bg-canvas"
                    }`}
                  >
                    <PenLine size={16} />
                  </button>
                </div>

                {photo && (
                  <div className="mt-2 flex items-center gap-2 p-2 bg-surface-sunk shadow-inset rounded-lg">
                    <img src={photo.url} alt="capture terrain" className="h-12 w-12 object-cover rounded" />
                    <div className="flex-1 min-w-0">
                      <div className="text-[10px] text-ink-muted truncate">{photo.name}</div>
                      <div className="text-[10px] text-brand-emerald font-medium">{t("inspection.photo_attached")}</div>
                    </div>
                    <button onClick={() => setPhoto(null)} className="text-ink-muted hover:text-red-500 text-sm leading-none">×</button>
                  </div>
                )}

                {!resultat && observation.trim() && (
                  <div className="mt-2 relative">
                    <div className="flex items-center gap-1.5">
                      <button
                        onClick={handleSynthetiser}
                        disabled={syntheseLoading}
                        className="flex-1 text-xs border border-brand text-brand rounded-lg px-3 py-1.5 flex items-center justify-center gap-1.5 hover:bg-brand/10 transition-colors disabled:opacity-60"
                      >
                        {syntheseLoading ? (
                          <><svg className="animate-spin h-3 w-3" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" /></svg>{t("inspection.reformulating")}</>
                        ) : t("inspection.synthesize_btn")}
                      </button>
                      <button
                        onClick={() => setShowSynthInfo(v => !v)}
                        className="text-brand/50 hover:text-brand transition-colors p-1 rounded"
                        title={t("inspection.synthesize_info")}
                      >
                        <Info size={13} />
                      </button>
                    </div>
                    {showSynthInfo && (
                      <div className="absolute top-full left-0 right-0 mt-1.5 z-30 bg-white border border-brand/20 rounded-xl shadow-lg p-3 text-xs text-ink-muted">
                        <div className="flex items-center justify-between mb-1">
                          <span className="font-semibold text-brand text-[10px] uppercase tracking-wide">✨ Synthétiser & Analyser</span>
                          <button onClick={() => setShowSynthInfo(false)} className="text-ink-muted hover:text-ink"><X size={12} /></button>
                        </div>
                        <p className="leading-relaxed">{t("inspection.synthesize_info")}</p>
                      </div>
                    )}
                  </div>
                )}

                {synthese && (
                  <div className="mt-2 bg-brand/5 border border-brand/20 rounded-lg p-3">
                    <div className="text-[10px] font-bold text-brand uppercase tracking-wider mb-1">{t("inspection.synthesis_title")}</div>
                    <p className="text-xs text-ink mb-2 leading-snug">{synthese}</p>
                    <div className="flex gap-2">
                      <button onClick={() => { setObservation(synthese); setSynthese(null) }} className="text-[10px] font-semibold px-2 py-1 rounded bg-brand text-white hover:bg-brand-cyan transition-colors">{t("inspection.use_text")}</button>
                      <button onClick={() => setSynthese(null)} className="text-[10px] border border-divider text-ink-muted px-2 py-1 rounded hover:bg-canvas">{t("inspection.ignore")}</button>
                    </div>
                  </div>
                )}
              </div>

              {canvasMode && (
                <div className="mt-2 border border-brand rounded-xl overflow-hidden bg-white">
                  <div className="flex items-center justify-between px-3 py-1.5 border-b border-divider bg-canvas">
                    <span className="text-[10px] text-ink-muted font-medium flex items-center gap-1"><PenLine size={10} className="text-brand" />{t("inspection.handwriting_instruction")}</span>
                    <button onClick={clearCanvas} className="text-[10px] text-ink-muted hover:text-ink transition-colors">{t("inspection.clear")}</button>
                  </div>
                  <canvas
                    ref={canvasRef}
                    width={480}
                    height={160}
                    className="w-full touch-none cursor-crosshair block bg-white"
                    onPointerDown={handlePointerDown}
                    onPointerMove={handlePointerMove}
                    onPointerUp={handlePointerUp}
                    onPointerLeave={handlePointerUp}
                  />
                  <div className="flex items-center justify-between px-3 py-2 border-t border-divider bg-canvas">
                    <span className="text-[10px] text-ink-muted">{t("inspection.devices")}</span>
                    <button
                      onClick={handleTranscrire}
                      disabled={transcribeLoading}
                      className="text-xs font-semibold px-4 py-1.5 rounded-lg bg-brand text-white hover:bg-brand-cyan transition-colors disabled:opacity-60 flex items-center gap-1.5"
                    >
                      {transcribeLoading ? (
                        <><svg className="animate-spin h-3 w-3" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" /></svg>{t("inspection.transcribing")}</>
                      ) : t("inspection.transcribe_btn")}
                    </button>
                  </div>
                </div>
              )}

              {error && (
                <div className="mt-3 text-sm font-medium text-red-700 bg-red-50 border-2 border-red-400 rounded-lg px-4 py-3 flex items-start gap-2">
                  <span className="text-lg leading-none">⚠</span>
                  <div><div className="font-semibold">{t("inspection.analysis_error")}</div><div className="text-xs mt-0.5 text-red-600">{error}</div></div>
                </div>
              )}

              {resultat && criticiteResultat && (
                <div className={`mt-3 rounded-lg border p-3 transition-all duration-700 ${
                  momentFort ? "border-red-500 bg-red-50 ring-4 ring-red-500 ring-offset-2 shadow-lg shadow-red-200" : CRITICITE_STYLE[criticiteResultat]?.card || "border-divider bg-canvas"
                }`}>
                  {momentFort && (
                    <div className="flex items-center gap-2 mb-2 animate-pulse">
                      <span className="text-red-600 text-base leading-none">⚠</span>
                      <span className="text-[11px] font-extrabold text-red-700 uppercase tracking-widest">{t("inspection.major_nc_alert")}</span>
                    </div>
                  )}
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-1.5">
                      <span className={`text-xs font-bold px-2 py-0.5 rounded ${momentFort ? "bg-red-600 text-white animate-pulse" : CRITICITE_STYLE[criticiteResultat]?.badge}`}>
                        {CRITICITE_STYLE[criticiteResultat]?.label}
                      </span>
                      {criticiteResultat && (
                        <span className={`w-2.5 h-2.5 rounded-full shrink-0 inline-block ${
                          criticiteResultat === "majeure" ? "bg-red-500" :
                          criticiteResultat === "mineure" ? "bg-orange-400" :
                          criticiteResultat === "observation" ? "bg-yellow-400" :
                          "bg-green-500"
                        }`} title={CRITICITE_STYLE[criticiteResultat]?.label} />
                      )}
                    </div>
                    <div className="flex items-center gap-1.5">
                      {resultat.llm_enrichi
                        ? <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded bg-violet-100 text-violet-700">Claude Opus</span>
                        : <span className="text-[10px] px-1.5 py-0.5 rounded bg-canvas text-ink-muted">IA locale</span>
                      }
                      <span className="text-[10px] font-mono text-ink-muted">{resultat.clause_iso?.clause || "—"}</span>
                    </div>
                  </div>
                  <div className="text-xs text-ink mb-1"><span className="font-semibold">{t("inspection.finding_label")}</span> {resultat.llm_enrichi ? (resultat.diagnostic || observation) : observation.trim()}</div>
                  {resultat.action_corrective && (
                    <div className="text-xs text-ink"><span className="font-semibold">{t("inspection.corrective_action_label")}</span> {resultat.action_corrective}</div>
                  )}
                  {resultat.clause_iso?.titre && (
                    <div className="mt-1.5 text-[10px] italic border-t border-divider pt-1 text-brand">
                      {t("inspection.based_on_label")} {resultat.clause_iso.clause} — {resultat.clause_iso.titre}
                    </div>
                  )}
                </div>
              )}

              {resultat && feedbackDonne === null && (
                <div className="mt-2 flex items-center gap-2">
                  <span className="text-[10px] text-ink-muted shrink-0">{t("inspection.feedback_question")}</span>
                  <button onClick={() => { setFeedbackDonne("confirme"); onFeedback?.() }} className="text-[10px] font-medium px-2.5 py-1 rounded-lg bg-brand-emerald/10 text-brand-emerald border border-brand-emerald/30 hover:bg-brand-emerald/20 transition-colors">{t("inspection.confirm_btn")}</button>
                  <button onClick={() => setFeedbackDonne("corrige")} className="text-[10px] font-medium px-2.5 py-1 rounded-lg bg-brand-amber/10 text-brand-amber border border-brand-amber/30 hover:bg-brand-amber/20 transition-colors">{t("inspection.correct_btn")}</button>
                </div>
              )}
              {feedbackDonne === "confirme" && (
                <div className="mt-2 flex items-center gap-1.5 text-[10px] font-medium text-brand-emerald"><span>✓</span><span>{t("inspection.feedback_confirmed")}</span></div>
              )}
              {feedbackDonne === "corrige" && (
                <div className="mt-2 space-y-1.5">
                  <textarea value={correctionTexte} onChange={e => setCorrectionTexte(e.target.value)} placeholder={t("inspection.correction_placeholder")} rows={2} autoFocus
                    className="w-full text-xs border border-brand-amber/50 rounded-lg px-2 py-1.5 focus:outline-none focus:ring-1 focus:ring-brand-amber resize-none"
                  />
                  <div className="flex gap-1.5">
                    <button onClick={() => { setFeedbackDonne("corrige-envoye"); onFeedback?.() }} disabled={!correctionTexte.trim()} className="text-[10px] font-semibold px-2.5 py-1 rounded-lg text-white disabled:opacity-40 bg-brand-amber hover:bg-brand transition-colors">{t("inspection.send_correction")}</button>
                    <button onClick={() => { setFeedbackDonne(null); setCorrectionTexte("") }} className="text-[10px] text-ink-muted hover:text-ink px-1">{t("inspection.cancel_title")}</button>
                  </div>
                </div>
              )}
              {feedbackDonne === "corrige-envoye" && (
                <div className="mt-2 flex items-center gap-1.5 text-[10px] font-medium text-brand-amber"><span>✓</span><span>{t("inspection.correction_sent")}</span></div>
              )}

              {recidiveDetectee && (
                <div className="mt-2 flex items-start gap-2 bg-brand-amber/10 border border-brand-amber/40 rounded-lg px-3 py-2">
                  <span className="text-brand-amber text-sm leading-none mt-0.5">🔁</span>
                  <div>
                    <div className="text-[10px] font-bold uppercase tracking-wider mb-0.5 text-brand-amber">{t("inspection.recurrence_title")}</div>
                    <div className="text-xs text-ink">{t("inspection.recurrence_msg")} <span className="font-medium">{recidiveDetectee}</span></div>
                  </div>
                </div>
              )}

              <div className="flex gap-2 mt-3">
                <input ref={photoInputRef} type="file" accept="image/*" capture="environment" className="hidden" onChange={handlePhotoChange} />
                <button
                  onClick={() => photoInputRef.current?.click()}
                  className={`flex items-center gap-1 text-xs border rounded-lg px-3 py-1.5 transition-colors ${
                    photo ? "border-brand-emerald text-brand-emerald bg-brand-emerald/5" : "border-divider text-ink-muted hover:bg-canvas"
                  }`}
                >
                  <Camera size={13} />{photo ? t("inspection.photo_done_btn") : t("inspection.photo_btn")}
                </button>

                {!resultat && (
                  <button
                    onClick={handleAnalyser}
                    disabled={loading || !observation.trim()}
                    className="flex-1 disabled:bg-divider disabled:cursor-not-allowed text-white text-xs font-semibold px-3 py-1.5 rounded-lg bg-brand hover:bg-brand-cyan transition-colors flex items-center justify-center gap-1 shadow-sm"
                  >
                    {loading ? (
                      <><svg className="animate-spin h-3 w-3" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" /></svg>{t("inspection.analyzing")}</>
                    ) : <><ScanLine size={13} className="inline mr-1" />{t("inspection.analyze_btn")}</>}
                  </button>
                )}
                {resultat && (
                  <button onClick={handleValider} className="text-xs font-semibold px-3 py-1.5 rounded-lg bg-brand-emerald text-white hover:bg-brand transition-colors shadow-sm">
                    <CheckCircle2 size={13} className="inline mr-1" />{t("inspection.validate_btn")}
                  </button>
                )}
                {resultat && (
                  <button onClick={handleRefaire} className="text-xs border border-divider rounded-lg px-3 py-1.5 text-ink-muted hover:bg-canvas">
                    <RotateCcw size={12} className="inline mr-1" />{t("inspection.redo_btn")}
                  </button>
                )}
              </div>
            </div>

            <div className="card">
            <div className="relative">
              <h3 className="section-label">
                <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><MessageSquare size={11} className="text-brand" /></span>
                {t("inspection.suggested_responses_title")}
                <button onClick={() => setShowQuestionsInfo(v => !v)} className="ml-1 text-brand/50 hover:text-brand transition-colors" title={t("inspection.how_questions_question")}>
                  <Info size={12} />
                </button>
              </h3>
              {showQuestionsInfo && (
                <div className="absolute top-7 left-0 right-0 z-30 bg-white border border-brand/20 rounded-xl shadow-lg p-3 text-xs text-ink-muted space-y-1.5">
                  <div className="flex items-center justify-between mb-1">
                    <span className="font-semibold text-brand text-[10px] uppercase tracking-wide">{t("inspection.how_questions_question")}</span>
                    <button onClick={() => setShowQuestionsInfo(false)} className="text-ink-muted hover:text-ink"><X size={12} /></button>
                  </div>
                  <p>{t("inspection.questions_ai_intro")}</p>
                  <ul className="space-y-1 pl-2">
                    <li><span className="font-medium text-ink">{t("inspection.item_text_label")}</span> — ex. "Vérification des certificats d'étalonnage"</li>
                    <li><span className="font-medium text-ink">{t("inspection.iso_clause_label")}</span> — ex. §7.1.5</li>
                    <li><span className="font-medium text-ink">{t("inspection.section_title_label")}</span> — ex. "Étalonnage & équipements de mesure"</li>
                  </ul>
                  <p className="pt-1">{t("inspection.claude_generates")}</p>
                </div>
              )}
            </div>
            {!selectedItem ? (
              <p className="text-xs text-ink-muted italic">{t("inspection.select_point_for_questions")}</p>
            ) : questionsLoading ? (
              <div className="flex items-center gap-2 text-xs text-ink-muted">
                <svg className="animate-spin h-3 w-3 shrink-0" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" /></svg>
                {t("inspection.generating_questions")}
              </div>
            ) : (
              <div className="space-y-3">
                <div className="text-[10px] font-semibold text-brand uppercase tracking-wide mb-2">
                  {selectedItem.clause} — {selectedItem.sectionTitre}
                </div>
                {questionsOuiNon.map((q, i) => {
                  const rep = reponses[i]
                  return (
                    <div key={i} className="bg-surface-sunk shadow-inset rounded-lg p-2.5">
                      <p className="text-xs text-ink mb-2 leading-snug">{q}</p>
                      <div className="flex gap-1.5">
                        <button
                          onClick={() => {
                            const newVal = reponses[i] === "oui" ? null : "oui"
                            setReponses(prev => ({ ...prev, [i]: newVal }))
                            if (newVal === "oui") appendToObservation(toStatement(q, "oui"))
                          }}
                          className={`flex-1 text-xs font-semibold py-1 rounded transition-colors ${
                            rep === "oui" ? "bg-brand-emerald text-white" : "bg-canvas text-ink-muted hover:bg-brand-emerald/10 hover:text-brand-emerald"
                          }`}
                        >{t("inspection.yes")}</button>
                        <button
                          onClick={() => {
                            const newVal = reponses[i] === "non" ? null : "non"
                            setReponses(prev => ({ ...prev, [i]: newVal }))
                            if (newVal === "non") appendToObservation(toStatement(q, "non"))
                          }}
                          className={`flex-1 text-xs font-semibold py-1 rounded transition-colors ${
                            rep === "non" ? "bg-nc-majeure text-white" : "bg-canvas text-ink-muted hover:bg-nc-majeure/10 hover:text-nc-majeure"
                          }`}
                        >{t("inspection.no")}</button>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
            </div>

          </div>

          {/* Colonne droite — Constats */}
          <div className="col-span-4 card overflow-y-auto min-h-0">
            <h3 className="section-label">
              {t("inspection.findings_title", { n: constats.length })}
            </h3>
            {constats.length === 0 ? (
              <p className="text-xs text-ink-muted italic">{t("inspection.no_findings")}</p>
            ) : (
              <div className="space-y-2">
                {constats.map((c) => (
                  <div key={c.id} className={`bg-surface-sunk shadow-inset rounded-lg border p-3 ${CRITICITE_STYLE[c.criticite]?.border || ""}`}>
                    {editingConstatId === c.id ? (
                      <>
                        <div className="flex items-center justify-between mb-1.5">
                          <div className="flex items-center gap-1.5">
                            <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded ${CRITICITE_STYLE[c.criticite]?.badge}`}>
                              {CRITICITE_STYLE[c.criticite]?.label}
                            </span>
                            <span className={`w-2 h-2 rounded-full shrink-0 inline-block ${
                              c.criticite === "majeure" ? "bg-red-500" :
                              c.criticite === "mineure" ? "bg-orange-400" :
                              c.criticite === "observation" ? "bg-yellow-400" :
                              "bg-green-500"
                            }`} title={CRITICITE_STYLE[c.criticite]?.label} />
                          </div>
                          <span className="text-[10px] font-mono text-ink-muted">{c.clause}</span>
                        </div>
                        <textarea
                          className="w-full text-xs border border-brand/40 rounded p-1.5 resize-none bg-surface focus:outline-none focus:border-brand"
                          rows={3}
                          value={editDraft.constat}
                          onChange={(e) => setEditDraft((d) => ({ ...d, constat: e.target.value }))}
                        />
                        <textarea
                          className="w-full text-[10px] border border-divider rounded p-1.5 resize-none bg-surface mt-1 italic text-ink-muted focus:outline-none focus:border-brand"
                          rows={2}
                          value={editDraft.action}
                          onChange={(e) => setEditDraft((d) => ({ ...d, action: e.target.value }))}
                          placeholder={t("inspection.corrective_action_placeholder")}
                        />
                        <div className="flex gap-1 mt-1.5 justify-end">
                          <button
                            onClick={() => { onUpdateConstat(c.id, editDraft); setEditingConstatId(null) }}
                            className="p-1 rounded hover:bg-brand/10 text-brand-emerald" title={t("inspection.save_title")}
                          ><Check size={11} /></button>
                          <button
                            onClick={() => setEditingConstatId(null)}
                            className="p-1 rounded hover:bg-red-50 text-ink-muted" title={t("inspection.cancel_title")}
                          ><X size={11} /></button>
                        </div>
                      </>
                    ) : (
                      <>
                        <div className="flex items-center justify-between mb-2">
                          <div className="flex items-center gap-1.5">
                            <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded ${CRITICITE_STYLE[c.criticite]?.badge}`}>
                              {CRITICITE_STYLE[c.criticite]?.label}
                            </span>
                            <span className={`w-2 h-2 rounded-full shrink-0 inline-block ${
                              c.criticite === "majeure" ? "bg-red-500" :
                              c.criticite === "mineure" ? "bg-orange-400" :
                              c.criticite === "observation" ? "bg-yellow-400" :
                              "bg-green-500"
                            }`} title={CRITICITE_STYLE[c.criticite]?.label} />
                          </div>
                          <div className="flex items-center gap-1">
                            <span className="text-[10px] font-mono text-ink-muted">{c.clause}</span>
                            <button
                              onClick={() => { setEditingConstatId(c.id); setEditDraft({ constat: c.constat, action: c.action || "" }) }}
                              className="p-0.5 rounded hover:bg-brand/10 text-ink-muted hover:text-brand" title={t("inspection.edit_title")}
                            ><Pencil size={9} /></button>
                            <button
                              onClick={() => onDeleteConstat(c.id)}
                              className="p-0.5 rounded hover:bg-red-50 text-ink-muted hover:text-red-500" title={t("inspection.delete_title")}
                            ><X size={9} /></button>
                          </div>
                        </div>
                        <div className="border-t border-divider mb-2" />
                        <div className="text-xs text-ink leading-snug">{c.constat}</div>
                        {c.action && (
                          <div className="flex items-start gap-1 mt-2">
                            <span className="text-[10px] text-brand-emerald font-bold shrink-0 mt-px">→</span>
                            <span className="text-[10px] text-ink-muted leading-snug">{c.action}</span>
                          </div>
                        )}
                        {c.photoUrl && (
                          <div className="relative group/photo mt-1.5 self-start">
                            <img
                              src={c.photoUrl}
                              alt="preuve terrain"
                              className="h-10 w-10 object-cover rounded cursor-zoom-in border border-divider"
                            />
                            <div className="pointer-events-none absolute bottom-full left-0 mb-1 z-50 opacity-0 group-hover/photo:opacity-100 transition-opacity duration-150">
                              <img
                                src={c.photoUrl}
                                alt="preuve terrain agrandie"
                                className="w-48 h-auto object-cover rounded-lg shadow-lg border border-divider"
                              />
                            </div>
                          </div>
                        )}
                      </>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>

        </div>
      </div>
    </div>
  )
}
