import { useEffect, useRef, useState } from "react"
import { Mic, MicOff, Camera, ScanLine, CheckCircle2, RotateCcw, ClipboardList, BookOpen, MessageSquare, Sparkles, Circle } from "lucide-react"
import { CHECKLIST, RAG_ARTICLES, AUDIT_COURANT, QUESTIONS_SUGGEREES, RECURRENCES } from "../mockData"
import { analyser, synthetiser, getSuggestions, getQuestionsOuiNon } from "../api"

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

export default function InspectionCapture({ constats, onAddConstat, onGenererRapport, onBack, startTime, juryMode, theme, customSections = [], extraItemsBySectionId = {}, removedItemIds = new Set(), onFeedback }) {
  const ag = theme === "agile"
  const ar = theme === "aria"

  const baseChecklist = CHECKLIST.map((s) => ({
    ...s,
    items: [
      ...s.items.filter((it) => !removedItemIds.has(it.id)).map((it) => ({ ...it })),
      ...(extraItemsBySectionId[s.id] || []).map((p) => ({ id: p.id, texte: p.texte, statut: "a-venir" })),
    ],
  }))
  customSections.forEach((sec) => {
    baseChecklist.push({
      id: sec.id,
      titre: sec.titre,
      clause: sec.clause || "—",
      points: sec.items.length,
      items: sec.items.map((p) => ({ id: p.id, texte: p.texte, statut: "a-venir" })),
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

  function toStatement(q, rep) {
    let t = q.replace(/\s*\?$/, "").trim()
    const affirm = { "est-elle": "est", "est-il": "est", "sont-ils": "sont", "sont-elles": "sont", "a-t-il": "a", "ont-ils": "ont", "ont-elles": "ont", "avez-vous": "vous avez" }
    const neg    = { "est-elle": "n'est pas", "est-il": "n'est pas", "sont-ils": "ne sont pas", "sont-elles": "ne sont pas", "a-t-il": "n'a pas", "ont-ils": "n'ont pas", "ont-elles": "n'ont pas", "avez-vous": "vous n'avez pas" }
    const map = rep === "oui" ? affirm : neg
    for (const [from, to] of Object.entries(map)) {
      t = t.replace(new RegExp(`\\b${from}\\b`, "gi"), to)
    }
    t = t.replace(/\s+/g, " ").trim()
    return t.charAt(0).toUpperCase() + t.slice(1) + "."
  }

  function appendToObservation(statement) {
    setObservation(prev => {
      const t = prev.trim()
      if (!t) return statement
      return t.endsWith(".") || t.endsWith(",") ? t + " " + statement : t + ". " + statement
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
    const clause = selectedItem.clause || ""
    if (clause.includes("7.1.5")) setRagArticles(RAG_ARTICLES["§7.1.5"])
    else if (clause.includes("8.7")) setRagArticles(RAG_ARTICLES["§8.7"])
    else setRagArticles(RAG_ARTICLES.default)
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
    rec.lang = "fr-FR"; rec.continuous = true; rec.interimResults = true
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
    setLoading(true); setError(null); setResultat(null); setWaveActive(true)
    try {
      const reponsesLignes = questionsOuiNon
        .map((q, i) => reponses[i] ? `- ${q} → ${reponses[i] === "oui" ? "Oui ✓" : "Non ✗"}` : null)
        .filter(Boolean)
      const observationAvecReponses = reponsesLignes.length > 0
        ? `${observation.trim()}\n\nPoints de vérification :\n${reponsesLignes.join("\n")}`
        : observation.trim()
      const data = await analyser(observationAvecReponses, AUDIT_COURANT.site_id)
      setResultat(data)
      const clauseStr = data.clause_iso?.clause || ""
      if (clauseStr.includes("7.1.5")) setRagArticles(RAG_ARTICLES["§7.1.5"])
      else if (clauseStr.includes("8.7")) setRagArticles(RAG_ARTICLES["§8.7"])
      else setRagArticles(RAG_ARTICLES.default)
      if ((data.criticite || "").toLowerCase() === "majeure") {
        clearTimeout(momentFortTimer.current)
        setMomentFort(true)
        momentFortTimer.current = setTimeout(() => setMomentFort(false), 3500)
      }
    } catch (e) { setError(e.message) }
    finally { setLoading(false); setWaveActive(false) }
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
      constat: resultat.diagnostic || resultat.observation || observation.trim(),
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
    clearTimeout(momentFortTimer.current)
  }

  function handleRefaire() {
    setResultat(null); setObservation(""); setError(null); setMomentFort(false)
    setSynthese(null); setFeedbackDonne(null); setCorrectionTexte("")
    clearTimeout(momentFortTimer.current)
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
        <button onClick={onBack} className="text-xs text-brand hover:underline flex items-center gap-1">← Retour</button>
        <div className="text-xs text-ink-muted flex gap-4">
          <span>{constats.length} constats</span>
          {counts.majeure > 0 && <span className="text-nc-majeure font-medium">{counts.majeure} NC maj.</span>}
          {counts.mineure > 0 && <span className="text-nc-mineure font-medium">{counts.mineure} NC min.</span>}
          {counts.observation > 0 && <span className="text-observation">{counts.observation} obs.</span>}
          {counts.conforme > 0 && <span className="text-conforme">{counts.conforme} conforme(s)</span>}
          <span className="text-ink-muted">· {timerStr}</span>
        </div>
        <button
          onClick={onGenererRapport}
          disabled={constats.length === 0}
          className="disabled:bg-divider disabled:text-ink-muted disabled:cursor-not-allowed text-white font-semibold px-5 py-2 rounded-lg text-sm bg-brand hover:bg-brand-cyan transition-colors shadow-sm"
        >
          Générer le pré-rapport →
        </button>
      </div>

      <div className="flex-1 p-4 w-full overflow-hidden flex flex-col">
        <div className="grid grid-cols-12 gap-4 flex-1 min-h-0">

          {/* Colonne gauche — Check-list */}
          <div className="col-span-4 card overflow-y-auto min-h-0">
            <h3 className="section-label">
              <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><ClipboardList size={11} className="text-brand" /></span>Check-list dynamique
            </h3>
            {checklist.map((section) => (
              <div key={section.id} className="mb-4">
                <div className="flex items-center justify-between mb-1">
                  <span className={`text-xs font-bold ${section.id.startsWith("Sx") ? "text-brand-emerald" : "text-brand"}`}>
                    {section.id} — {section.titre}
                    {section.id.startsWith("Sx") && <span className="ml-1 text-[9px] font-normal opacity-70">(ajouté)</span>}
                  </span>
                  <span className="text-[10px] text-ink-muted font-mono">{section.clause}</span>
                </div>
                {section.items.map((item) => {
                  const isSelected = selectedItem?.id === item.id
                  return (
                    <div
                      key={item.id}
                      onClick={() => { setSelectedItem({ id: item.id, texte: item.texte, clause: section.clause, sectionId: section.id, sectionTitre: section.titre }); setSelectionCount(c => c + 1) }}
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
                      <span className={`text-xs ${
                        item.statut === "valide" ? "text-ink-muted line-through" :
                        isSelected ? "text-brand font-semibold" :
                        item.statut === "actif" ? "text-ink font-medium" : "text-ink-muted"
                      }`}>
                        {item.texte}
                      </span>
                    </div>
                  )
                })}
              </div>
            ))}
          </div>

          {/* Colonne centre — Capture + Réponses + RAG */}
          <div className="col-span-4 flex flex-col gap-3 overflow-y-auto min-h-0">
            <div className="bg-surface rounded-xl shadow-md p-4">
              {selectedItem ? (
                <div className="mb-3">
                  <div className="text-[10px] font-bold text-ink-muted uppercase tracking-widest">Point en cours</div>
                  <div className="text-sm font-semibold mt-0.5 text-brand">{selectedItem.texte}</div>
                  <div className="text-[10px] text-ink-muted mt-0.5">{selectedItem.sectionId} — {selectedItem.clause}</div>
                </div>
              ) : (
                <h3 className="text-[10px] font-bold text-ink-muted uppercase tracking-widest mb-3">
                  Capture en cours — <span className="font-normal normal-case">sélectionner un point à gauche</span>
                </h3>
              )}
              <Waveform active={loading || waveActive || isRecording} />

              <div className="mt-3">
                <div className="relative">
                  <textarea
                    value={observation}
                    onChange={(e) => setObservation(e.target.value)}
                    placeholder={isRecording ? "Dictée en cours — parlez maintenant…" : "Décrivez l'observation terrain…"}
                    rows={3}
                    className={`w-full border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-brand resize-none pr-10 ${
                      isRecording ? "border-red-400 bg-red-50" : "border-divider"
                    }`}
                  />
                  {speechSupported && (
                    <button
                      onClick={toggleRecording}
                      className={`absolute right-2 top-2 p-1.5 rounded-full transition-all ${
                        isRecording ? "bg-red-500 text-white animate-pulse shadow-md shadow-red-300" : "text-ink-muted hover:text-ink hover:bg-canvas"
                      }`}
                    >
                      {isRecording ? <MicOff size={16} /> : <Mic size={16} />}
                    </button>
                  )}
                </div>

                {photo && (
                  <div className="mt-2 flex items-center gap-2 p-2 bg-surface-sunk shadow-inset rounded-lg">
                    <img src={photo.url} alt="capture terrain" className="h-12 w-12 object-cover rounded" />
                    <div className="flex-1 min-w-0">
                      <div className="text-[10px] text-ink-muted truncate">{photo.name}</div>
                      <div className="text-[10px] text-brand-emerald font-medium">Photo attachée au constat</div>
                    </div>
                    <button onClick={() => setPhoto(null)} className="text-ink-muted hover:text-red-500 text-sm leading-none">×</button>
                  </div>
                )}

                {!resultat && observation.trim() && (
                  <button
                    onClick={handleSynthetiser}
                    disabled={syntheseLoading}
                    className="mt-2 w-full text-xs border border-brand text-brand rounded-lg px-3 py-1.5 flex items-center justify-center gap-1.5 hover:bg-brand/10 transition-colors disabled:opacity-60"
                  >
                    {syntheseLoading ? (
                      <><svg className="animate-spin h-3 w-3" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" /></svg>Reformulation…</>
                    ) : "✨ Synthétiser"}
                  </button>
                )}

                {synthese && (
                  <div className="mt-2 bg-brand/5 border border-brand/20 rounded-lg p-3">
                    <div className="text-[10px] font-bold text-brand uppercase tracking-wider mb-1">✨ Synthèse IA</div>
                    <p className="text-xs text-ink mb-2 leading-snug">{synthese}</p>
                    <div className="flex gap-2">
                      <button onClick={() => { setObservation(synthese); setSynthese(null) }} className="text-[10px] font-semibold px-2 py-1 rounded bg-brand text-white hover:bg-brand-cyan transition-colors">Utiliser ce texte</button>
                      <button onClick={() => setSynthese(null)} className="text-[10px] border border-divider text-ink-muted px-2 py-1 rounded hover:bg-canvas">Ignorer</button>
                    </div>
                  </div>
                )}
              </div>

              {error && (
                <div className="mt-3 text-sm font-medium text-red-700 bg-red-50 border-2 border-red-400 rounded-lg px-4 py-3 flex items-start gap-2">
                  <span className="text-lg leading-none">⚠</span>
                  <div><div className="font-semibold">Analyse impossible</div><div className="text-xs mt-0.5 text-red-600">{error}</div></div>
                </div>
              )}

              {resultat && criticiteResultat && (
                <div className={`mt-3 rounded-lg border p-3 transition-all duration-700 ${
                  momentFort ? "border-red-500 bg-red-50 ring-4 ring-red-500 ring-offset-2 shadow-lg shadow-red-200" : CRITICITE_STYLE[criticiteResultat]?.card || "border-divider bg-canvas"
                }`}>
                  {momentFort && (
                    <div className="flex items-center gap-2 mb-2 animate-pulse">
                      <span className="text-red-600 text-base leading-none">⚠</span>
                      <span className="text-[11px] font-extrabold text-red-700 uppercase tracking-widest">Non-conformité majeure détectée</span>
                    </div>
                  )}
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-1.5">
                      <span className={`text-xs font-bold px-2 py-0.5 rounded ${momentFort ? "bg-red-600 text-white animate-pulse" : CRITICITE_STYLE[criticiteResultat]?.badge}`}>
                        {CRITICITE_STYLE[criticiteResultat]?.label}
                      </span>
                      {resultat.score_criticite !== undefined && (
                        <span className="text-[10px] font-mono bg-canvas text-ink-muted px-1.5 py-0.5 rounded">score {resultat.score_criticite}/3</span>
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
                  <div className="text-xs text-ink mb-1"><span className="font-semibold">Constat :</span> {resultat.diagnostic || observation}</div>
                  {resultat.action_corrective && (
                    <div className="text-xs text-ink"><span className="font-semibold">Action corrective :</span> {resultat.action_corrective}</div>
                  )}
                  {resultat.clause_iso?.titre && (
                    <div className="mt-1.5 text-[10px] italic border-t border-divider pt-1 text-brand">
                      Fondé sur : {resultat.clause_iso.clause} — {resultat.clause_iso.titre}
                    </div>
                  )}
                </div>
              )}

              {resultat && feedbackDonne === null && (
                <div className="mt-2 flex items-center gap-2">
                  <span className="text-[10px] text-ink-muted shrink-0">Analyse correcte ?</span>
                  <button onClick={() => { setFeedbackDonne("confirme"); onFeedback?.() }} className="text-[10px] font-medium px-2.5 py-1 rounded-lg bg-brand-emerald/10 text-brand-emerald border border-brand-emerald/30 hover:bg-brand-emerald/20 transition-colors">👍 Confirmer</button>
                  <button onClick={() => setFeedbackDonne("corrige")} className="text-[10px] font-medium px-2.5 py-1 rounded-lg bg-brand-amber/10 text-brand-amber border border-brand-amber/30 hover:bg-brand-amber/20 transition-colors">✏️ Corriger</button>
                </div>
              )}
              {feedbackDonne === "confirme" && (
                <div className="mt-2 flex items-center gap-1.5 text-[10px] font-medium text-brand-emerald"><span>✓</span><span>Retour enregistré — l'expertise BV s'enrichit.</span></div>
              )}
              {feedbackDonne === "corrige" && (
                <div className="mt-2 space-y-1.5">
                  <textarea value={correctionTexte} onChange={e => setCorrectionTexte(e.target.value)} placeholder="Décrivez la correction (clause, criticité, diagnostic…)" rows={2} autoFocus
                    className="w-full text-xs border border-brand-amber/50 rounded-lg px-2 py-1.5 focus:outline-none focus:ring-1 focus:ring-brand-amber resize-none"
                  />
                  <div className="flex gap-1.5">
                    <button onClick={() => { setFeedbackDonne("corrige-envoye"); onFeedback?.() }} disabled={!correctionTexte.trim()} className="text-[10px] font-semibold px-2.5 py-1 rounded-lg text-white disabled:opacity-40 bg-brand-amber hover:bg-brand transition-colors">Envoyer la correction</button>
                    <button onClick={() => { setFeedbackDonne(null); setCorrectionTexte("") }} className="text-[10px] text-ink-muted hover:text-ink px-1">Annuler</button>
                  </div>
                </div>
              )}
              {feedbackDonne === "corrige-envoye" && (
                <div className="mt-2 flex items-center gap-1.5 text-[10px] font-medium text-brand-amber"><span>✓</span><span>Correction transmise — merci, l'expertise BV est mise à jour.</span></div>
              )}

              {recidiveDetectee && (
                <div className="mt-2 flex items-start gap-2 bg-brand-amber/10 border border-brand-amber/40 rounded-lg px-3 py-2">
                  <span className="text-brand-amber text-sm leading-none mt-0.5">🔁</span>
                  <div>
                    <div className="text-[10px] font-bold uppercase tracking-wider mb-0.5 text-brand-amber">Récidive détectée</div>
                    <div className="text-xs text-ink">Ce point a déjà été signalé sur ce site : <span className="font-medium">{recidiveDetectee}</span></div>
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
                  <Camera size={13} />{photo ? "Photo ✓" : "+ Photo"}
                </button>

                {!resultat && (
                  <button
                    onClick={handleAnalyser}
                    disabled={loading || !observation.trim()}
                    className="flex-1 disabled:bg-divider disabled:cursor-not-allowed text-white text-xs font-semibold px-3 py-1.5 rounded-lg bg-brand hover:bg-brand-cyan transition-colors flex items-center justify-center gap-1 shadow-sm"
                  >
                    {loading ? (
                      <><svg className="animate-spin h-3 w-3" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" /></svg>Analyse…</>
                    ) : <><ScanLine size={13} className="inline mr-1" />Analyser</>}
                  </button>
                )}
                {resultat && (
                  <button onClick={handleValider} className="text-xs font-semibold px-3 py-1.5 rounded-lg bg-brand-emerald text-white hover:bg-brand transition-colors shadow-sm">
                    <CheckCircle2 size={13} className="inline mr-1" />Valider
                  </button>
                )}
                {resultat && (
                  <button onClick={handleRefaire} className="text-xs border border-divider rounded-lg px-3 py-1.5 text-ink-muted hover:bg-canvas">
                    <RotateCcw size={12} className="inline mr-1" />Refaire
                  </button>
                )}
              </div>
            </div>

            <div className="card">
            <h3 className="section-label">
              <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><MessageSquare size={11} className="text-brand" /></span>Réponses suggérées
            </h3>
            {!selectedItem ? (
              <p className="text-xs text-ink-muted italic">Sélectionner un point pour voir les réponses suggérées.</p>
            ) : questionsLoading ? (
              <div className="flex items-center gap-2 text-xs text-ink-muted">
                <svg className="animate-spin h-3 w-3 shrink-0" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" /></svg>
                Génération des réponses…
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
                        >Oui</button>
                        <button
                          onClick={() => {
                            const newVal = reponses[i] === "non" ? null : "non"
                            setReponses(prev => ({ ...prev, [i]: newVal }))
                            if (newVal === "non") appendToObservation(toStatement(q, "non"))
                          }}
                          className={`flex-1 text-xs font-semibold py-1 rounded transition-colors ${
                            rep === "non" ? "bg-nc-majeure text-white" : "bg-canvas text-ink-muted hover:bg-nc-majeure/10 hover:text-nc-majeure"
                          }`}
                        >Non</button>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
            </div>

            {/* RAG avec tooltip au survol */}
            <div className="card shrink-0">
              <h3 className="section-label">
                <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><BookOpen size={11} className="text-brand" /></span>Articles normatifs — RAG
              </h3>
              <div className="grid grid-cols-3 gap-2">
                {ragArticles.map((art, i) => (
                  <div key={i} className="relative group/art bg-surface-sunk shadow-inset rounded-lg p-2 cursor-default">
                    <div className="font-mono text-[10px] font-bold mb-1 text-brand">{art.clause}</div>
                    <div className="text-[10px] font-semibold text-ink mb-1 line-clamp-1">{art.titre}</div>
                    <div className="text-[10px] text-ink-muted leading-tight line-clamp-2">{art.extrait}</div>
                    <div className="pointer-events-none absolute z-50 bottom-full left-0 w-64 mb-1.5 bg-white rounded-xl shadow-lg border border-gray-200 p-3 opacity-0 group-hover/art:opacity-100 transition-opacity">
                      <div className="font-mono text-[10px] font-bold text-brand mb-1">{art.clause}</div>
                      <div className="text-xs font-semibold text-ink mb-1.5">{art.titre}</div>
                      <p className="text-[11px] text-ink-muted leading-relaxed">{art.extrait}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Colonne droite — Constats */}
          <div className="col-span-4 card overflow-y-auto min-h-0">
            <h3 className="section-label">
              Constats ({constats.length})
            </h3>
            {constats.length === 0 ? (
              <p className="text-xs text-ink-muted italic">Aucun constat enregistré</p>
            ) : (
              <div className="space-y-2">
                {constats.map((c) => (
                  <div key={c.id} className={`bg-surface-sunk shadow-inset rounded-lg border p-3 ${CRITICITE_STYLE[c.criticite]?.border || ""}`}>
                    <div className="flex items-center justify-between mb-1">
                      <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded ${CRITICITE_STYLE[c.criticite]?.badge}`}>
                        {CRITICITE_STYLE[c.criticite]?.label}
                      </span>
                      <span className="text-[10px] font-mono text-ink-muted">{c.clause}</span>
                    </div>
                    <div className="text-xs text-ink leading-tight">{c.constat}</div>
                    {c.action && <div className="text-[10px] text-ink-muted mt-1 italic">{c.action}</div>}
                    {c.photoUrl && (
                      <a href={c.photoUrl} target="_blank" rel="noopener noreferrer" className="mt-1 text-[10px] text-brand hover:underline flex items-center gap-1">
                        <Camera size={10} />Voir la photo
                      </a>
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
