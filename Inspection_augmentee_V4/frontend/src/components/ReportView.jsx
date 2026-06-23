import { useState, useRef } from "react"
import { AUDIT_COURANT, AUDITEUR, CHECKLIST } from "../mockData"
import { exportDocx } from "../api"
import { Download, Send, PenLine, Lock, Unlock, BarChart2, ListChecks, FileCheck, LayoutList, Wrench, Info, X } from "lucide-react"
import { useT } from "../useT"

const CRITICITE_STYLE = {
  majeure:     { badge: "bg-nc-majeure text-white",  label: "NC MAJEURE",  dot: "bg-nc-majeure" },
  mineure:     { badge: "bg-nc-mineure text-white",  label: "NC MINEURE",  dot: "bg-nc-mineure" },
  observation: { badge: "bg-observation text-white", label: "OBSERVATION", dot: "bg-observation" },
  conforme:    { badge: "bg-conforme text-white",    label: "CONFORME",    dot: "bg-conforme" },
}

function ScoreRing({ score }) {
  if (score === null) return <span className="text-2xl font-black text-divider">—</span>
  const r = 28, circ = 2 * Math.PI * r, offset = circ - (score / 100) * circ
  const color = score >= 80 ? "var(--observation)" : score >= 50 ? "var(--amber)" : "var(--nc-majeure)"
  const textCls = score >= 80 ? "text-observation" : score >= 50 ? "text-brand-amber" : "text-nc-majeure"
  return (
    <div className="relative w-16 h-16 flex items-center justify-center">
      <svg className="-rotate-90 absolute inset-0" width="64" height="64" viewBox="0 0 72 72">
        <circle cx="36" cy="36" r={r} fill="none" stroke="var(--border)" strokeWidth="6" />
        <circle cx="36" cy="36" r={r} fill="none" stroke={color} strokeWidth="6"
          strokeDasharray={circ} strokeDashoffset={offset} strokeLinecap="round"
          style={{ transition: "stroke-dashoffset 0.7s ease" }} />
      </svg>
      <span className={`relative text-sm font-black ${textCls}`}>{score}%</span>
    </div>
  )
}

export default function ReportView({ constats, dureeAudit, onBack, onNewAudit, theme, lang, feedbackCount = 0, onSaveReport }) {
  const t = useT(lang)
  const [sent, setSent] = useState(false)
  const [showConformiteInfo, setShowConformiteInfo] = useState(false)
  const [downloading, setDownloading] = useState(false)
  const [reportSaved, setReportSaved] = useState(false)
  const [transmettreLoading, setTransmettreLoading] = useState(false)
  const [signatureMode, setSignatureMode] = useState(false)
  const [signataire, setSignataire] = useState("Mei Lin Zhang")
  const [signed, setSigned] = useState(false)
  const [isSignDrawing, setIsSignDrawing] = useState(false)
  const [signDataUrl, setSignDataUrl] = useState(null)
  const signCanvasRef = useRef(null)
  const signLastPt = useRef(null)
  const [anonymise, setAnonymise] = useState(false)
  const [editMode, setEditMode] = useState(false)

  const STATUTS_CYCLE = [t("report.status_waiting"), t("report.status_in_progress"), t("report.status_closed")]
  const [suiviStatuts, setSuiviStatuts] = useState({})
  function cycleStatut(i) {
    setSuiviStatuts(prev => {
      const current = prev[i] ?? suiviActions[i]?.statut ?? t("report.status_waiting")
      const idx = STATUTS_CYCLE.indexOf(current)
      const next = STATUTS_CYCLE[(idx >= 0 ? idx + 1 : 1) % STATUTS_CYCLE.length]
      return { ...prev, [i]: next }
    })
  }

  const STATUT_ACTION = {
    [t("report.status_waiting")]:     "bg-nc-majeure/10 text-nc-majeure",
    [t("report.status_in_progress")]: "bg-nc-mineure/10 text-nc-mineure",
    [t("report.status_closed")]:      "bg-observation/10 text-observation",
  }

  const [editConstats, setEditConstats] = useState(() => constats.map(c => ({ ...c })))
  const [editReco, setEditReco] = useState(t("report.default_reco"))

  function updateConstat(i, field, val) {
    setEditConstats(prev => prev.map((c, idx) => idx === i ? { ...c, [field]: val } : c))
  }

  function getSignPt(e) {
    const rect = signCanvasRef.current.getBoundingClientRect()
    const scaleX = signCanvasRef.current.width / rect.width
    const scaleY = signCanvasRef.current.height / rect.height
    const clientX = e.touches ? e.touches[0].clientX : e.clientX
    const clientY = e.touches ? e.touches[0].clientY : e.clientY
    return { x: (clientX - rect.left) * scaleX, y: (clientY - rect.top) * scaleY }
  }
  function onSignDown(e) { e.currentTarget.setPointerCapture(e.pointerId); setIsSignDrawing(true); signLastPt.current = getSignPt(e) }
  function onSignMove(e) {
    if (!isSignDrawing) return
    const ctx = signCanvasRef.current.getContext("2d")
    const pt = getSignPt(e)
    ctx.beginPath(); ctx.moveTo(signLastPt.current.x, signLastPt.current.y); ctx.lineTo(pt.x, pt.y)
    ctx.strokeStyle = "#0070AD"
    ctx.lineWidth = e.pressure > 0 && e.pointerType === "pen" ? Math.max(1, e.pressure * 3) : 1.5
    ctx.lineCap = "round"; ctx.lineJoin = "round"; ctx.stroke()
    signLastPt.current = pt
  }
  function onSignUp() { setIsSignDrawing(false); signLastPt.current = null }
  function clearSignCanvas() { const c = signCanvasRef.current; if (c) c.getContext("2d").clearRect(0, 0, c.width, c.height) }
  function confirmerSignature() {
    if (signCanvasRef.current) setSignDataUrl(signCanvasRef.current.toDataURL("image/png"))
    setSigned(true)
  }

  async function handleTransmettrePortail() {
    if (!onSaveReport) return
    setTransmettreLoading(true)
    const clausesMajeures = [...new Set(constats.filter(c => c.criticite === "majeure").map(c => c.clause).filter(Boolean))]
    const clausesMineures = [...new Set(constats.filter(c => c.criticite === "mineure").map(c => c.clause).filter(Boolean))]
    const sectionsRisque = [...clausesMajeures, ...clausesMineures].map(cl =>
      `${cl} — ${constats.find(c => c.clause === cl)?.titre_clause || "Non-conformité"}`
    )
    const insights = {
      resume: `${counts.majeure} NC majeure(s), ${counts.mineure} NC mineure(s), ${counts.observation} observation(s) — Audit BV du ${today}`,
      sections_a_risque: sectionsRisque,
      points_controle: constats.map(c => c.action).filter(Boolean).slice(0, 4),
      nc_historique: constats.filter(c => c.criticite === "majeure").map(c => `NC MAJEURE ${c.clause} — ${c.constat.slice(0, 60)}…`),
    }
    let dataUrl = null
    try {
      const blob = await exportDocx({ site_nom: AUDIT_COURANT.nom, site_localisation: AUDIT_COURANT.localisation, site_id: AUDIT_COURANT.site_id, referentiel: AUDIT_COURANT.referentiel, auditeur: AUDITEUR.nom, duree: dureeAudit, date: today, constats })
      dataUrl = await new Promise(resolve => { const r = new FileReader(); r.onload = e => resolve(e.target.result); r.readAsDataURL(blob) })
    } catch { /* backend absent — on transmet sans le fichier */ }
    onSaveReport({
      id: `rapport-bv-${Date.now()}`,
      nom: `Rapport_BV_RATP_SUC_${new Date().toISOString().slice(0, 10)}.docx`,
      type: "DOCX",
      typeDoc: "Rapport d'audit BV",
      date: today,
      deposePar: AUDITEUR.nom,
      mock: false,
      fromBV: true,
      statut: "analysé",
      dataUrl,
      insights,
    })
    setReportSaved(true)
    setTransmettreLoading(false)
  }

  async function handleTelecharger() {
    setDownloading(true)
    try {
      const blob = await exportDocx({ site_nom: AUDIT_COURANT.nom, site_localisation: AUDIT_COURANT.localisation, site_id: AUDIT_COURANT.site_id, referentiel: AUDIT_COURANT.referentiel, auditeur: AUDITEUR.nom, duree: dureeAudit, date: today, constats })
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a"); a.href = url
      a.download = `Rapport_RATP_${AUDIT_COURANT.site_id}_${today.replace(/\//g, "-")}.docx`
      a.click(); URL.revokeObjectURL(url)
    } finally { setDownloading(false) }
  }

  const today = new Date().toLocaleDateString(lang === "FR" ? "fr-FR" : "en-GB")
  const counts = {
    majeure: constats.filter((c) => c.criticite === "majeure").length,
    mineure: constats.filter((c) => c.criticite === "mineure").length,
    observation: constats.filter((c) => c.criticite === "observation").length,
    conforme: constats.filter((c) => c.criticite === "conforme").length,
  }

  const [emailTo, setEmailTo] = useState(AUDIT_COURANT.contact)
  const [message, setMessage] = useState(
    t("report.default_message", { date: today, nom: AUDIT_COURANT.nom, auditeur: AUDITEUR.nom })
  )

  const SCORE_CRIT = { majeure: 0, mineure: 1, observation: 2, conforme: 3 }
  const sectionScores = CHECKLIST.map(section => {
    const clauseNum = section.clause.replace("§", "")
    const sc = constats.filter(c => c.clause && c.clause.includes(clauseNum))
    if (!sc.length) return { ...section, audited: false, score: null }
    const pct = Math.round(sc.reduce((s, c) => s + (SCORE_CRIT[c.criticite] ?? 0), 0) / (sc.length * 3) * 100)
    return { ...section, audited: true, score: pct }
  })
  const globalScore = constats.length === 0 ? null
    : Math.round(constats.reduce((s, c) => s + (SCORE_CRIT[c.criticite] ?? 0), 0) / (constats.length * 3) * 100)

  const suiviActions = [
    ...constats.filter((c) => c.criticite === "majeure").map((c) => ({ nc: c.constat, action: c.action || "—", delai: "24h", statut: t("report.status_waiting"), resp: t("report.resp_quality") })),
    ...constats.filter((c) => c.criticite === "mineure").map((c) => ({ nc: c.constat, action: c.action || "—", delai: t("report.delai_30j"), statut: t("report.status_in_progress"), resp: t("report.resp_chef") })),
  ]

  const planActions = [
    ...constats.filter((c) => c.criticite === "majeure").map((c, i) => ({ prio: `P${i + 1}`, delai: "24h", texte: c.action || c.constat, cls: "text-nc-majeure" })),
    ...constats.filter((c) => c.criticite === "mineure").map((c, i) => ({ prio: `P${counts.majeure + i + 1}`, delai: t("report.delai_30j"), texte: c.action || c.constat, cls: "text-nc-mineure" })),
  ]

  return (
    <div className="min-h-screen flex flex-col">
      {/* Barre haut */}
      <div className="page-bar">
        <button onClick={onBack} className="text-xs text-brand hover:underline flex items-center gap-1">{t("report.back")}</button>
        <div className="flex items-center gap-3 text-xs text-ink-muted flex-1 justify-center min-w-0 overflow-hidden">
          <span className="font-semibold text-ink truncate min-w-0">{lang === "EN" ? (AUDIT_COURANT.titreEN || AUDIT_COURANT.titre) : AUDIT_COURANT.titre}</span>
          <span className="text-divider shrink-0">·</span>
          <span className="text-brand-cyan truncate min-w-0">{AUDIT_COURANT.nom}</span>
          <span className="text-divider shrink-0">·</span>
          <span className="shrink-0">{t("report.audit_done_prefix")} {constats.length} {t("report.findings_suffix")} ·{" "}
          {counts.majeure > 0 && <span className="text-nc-majeure font-medium">{counts.majeure} {t("report.nc_maj_short")} · </span>}
          {counts.mineure > 0 && <span className="text-nc-mineure">{counts.mineure} {t("report.nc_min_short")} · </span>}
          {t("report.duration_prefix")} {dureeAudit}</span>
          {feedbackCount > 0 && (
            <span className="flex items-center gap-1 font-semibold px-2 py-0.5 rounded-full text-[10px] bg-brand-emerald/10 text-brand-emerald">
              {feedbackCount === 1
                ? t("report.feedback_registered_singular", { n: feedbackCount })
                : t("report.feedback_registered_plural", { n: feedbackCount })}
            </span>
          )}
        </div>
        <div className="flex gap-3">
          {!sent && (
            <button onClick={() => setSent(true)} className="font-semibold px-5 py-2 rounded-lg text-sm bg-brand-emerald text-white hover:bg-brand transition-colors shadow-sm">
              <Send size={13} className="inline mr-1" />{t("report.send_to_client")}
            </button>
          )}
          <button
            onClick={handleTransmettrePortail}
            disabled={reportSaved || transmettreLoading}
            className={`text-xs font-semibold px-4 py-2 rounded-lg border transition-colors ${
              reportSaved ? "border-brand-emerald text-brand-emerald bg-brand-emerald/5 cursor-default"
              : transmettreLoading ? "border-divider text-ink-muted opacity-60 cursor-wait"
              : "border-brand text-brand hover:bg-brand/5"
            }`}
          >
            {reportSaved ? t("report.transmitted") : transmettreLoading ? t("report.generating") : t("report.portal_button")}
          </button>
        </div>
      </div>

      <div className="page-content">
        <div className="grid-12">

          {/* Colonne gauche */}
          <div className="col-span-3 flex flex-col gap-4">
            {/* Synthèse */}
            <div className="card">
              <h3 className="section-label"><span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><LayoutList size={11} className="text-brand" /></span>{t("report.summary_title")}</h3>
              <div className="space-y-2 mt-3">
                {Object.entries(counts).map(([type, count]) => (
                  <div key={type} className="flex items-center gap-2">
                    <span className={`min-w-[1.5rem] h-6 rounded-full flex items-center justify-center text-xs font-bold px-1.5 ${CRITICITE_STYLE[type]?.badge}`}>
                      {count}
                    </span>
                    <span className="text-xs text-ink flex-1">{CRITICITE_STYLE[type]?.label}</span>
                  </div>
                ))}
              </div>
            </div>

            {/* Grille de conformité */}
            <div className="card">
              <div className="relative">
                <h3 className="section-label">
                  <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><BarChart2 size={11} className="text-brand" /></span>
                  {t("report.conformity_title")}
                  <button onClick={() => setShowConformiteInfo(v => !v)} className="ml-1 text-brand/50 hover:text-brand transition-colors" title={t("report.how_to_read")}>
                    <Info size={12} />
                  </button>
                </h3>
                {showConformiteInfo && (
                  <div className="absolute top-7 left-0 right-0 z-30 bg-white border border-brand/20 rounded-xl shadow-lg p-3 text-xs text-ink-muted space-y-1.5">
                    <div className="flex items-center justify-between mb-1">
                      <span className="font-semibold text-brand text-[10px] uppercase tracking-wide">{t("report.how_to_read")}</span>
                      <button onClick={() => setShowConformiteInfo(false)} className="text-ink-muted hover:text-ink"><X size={12} /></button>
                    </div>
                    <p>{t("report.conformity_explanation")}</p>
                    <ul className="space-y-1 pl-2">
                      <li><span className="font-medium text-ink">{t("report.grade_100")}</span></li>
                      <li><span className="font-medium text-ink">{t("report.grade_0")}</span></li>
                    </ul>
                    <p className="pt-0.5">{t("report.color_code")}</p>
                    <ul className="space-y-1 pl-2">
                      <li><span className="font-medium text-observation">{t("report.grade_good")}</span></li>
                      <li><span className="font-medium text-brand-amber">{t("report.grade_watch")}</span></li>
                      <li><span className="font-medium text-nc-majeure">{t("report.grade_critical")}</span></li>
                    </ul>
                    <p className="pt-0.5">{t("report.unaudited_note")}</p>
                  </div>
                )}
              </div>
              <div className="flex items-center justify-between mb-3 pt-2 pb-2 border-b border-divider">
                <span className="text-xs font-semibold text-ink-muted">{t("report.global_score")}</span>
                <ScoreRing score={globalScore} />
              </div>
              <div className="space-y-2.5">
                {sectionScores.map(s => {
                  const scoreColor = !s.audited ? "text-divider" : s.score >= 80 ? "text-observation" : s.score >= 50 ? "text-brand-amber" : "text-nc-majeure"
                  const barColor = !s.audited ? "bg-canvas" : s.score >= 80 ? "bg-observation" : s.score >= 50 ? "bg-brand-amber" : "bg-nc-majeure"
                  return (
                    <div key={s.id}>
                      <div className="flex items-center justify-between mb-0.5">
                        <span className="text-[10px] text-ink-muted truncate max-w-[145px]" title={s.titre}>
                          <span className="font-mono text-ink-muted mr-1">{s.clause}</span>{s.titre}
                        </span>
                        <span className={`text-[11px] font-bold ${scoreColor}`}>{s.audited ? `${s.score} %` : "—"}</span>
                      </div>
                      <div className="h-2 bg-surface-sunk shadow-inset rounded-full overflow-hidden">
                        <div className={`h-full rounded-full transition-all duration-700 ${barColor}`} style={{ width: s.audited ? `${s.score}%` : "0%" }} />
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>

            {planActions.length > 0 && (
              <div className="card">
                <h3 className="section-label"><span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><Wrench size={11} className="text-brand" /></span>{t("report.action_plan_title")}</h3>
                <div className="space-y-2 mt-1">
                  {planActions.map((p, i) => (
                    <div key={i} className="flex items-start gap-2">
                      <span className={`text-xs font-bold shrink-0 ${p.cls}`}>{p.prio}</span>
                      <div className="flex-1 min-w-0">
                        <div className="text-xs text-ink leading-tight truncate" title={p.texte}>{p.texte}</div>
                        <div className="text-[10px] text-ink-muted">{p.delai}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {suiviActions.length > 0 && (
              <div className="card">
                <h3 className="section-label">
                  <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><ListChecks size={11} className="text-brand" /></span>{t("report.corrective_tracking_title")}
                </h3>
                <div className="space-y-2 mt-1">
                  {suiviActions.map((a, i) => {
                    const statut = suiviStatuts[i] ?? a.statut
                    return (
                      <div key={i} className="bg-surface-sunk shadow-inset rounded-lg p-2">
                        <div className="text-[10px] text-ink truncate mb-1" title={a.nc}>{a.nc}</div>
                        <div className="flex items-center justify-between">
                          <span className="text-[9px] text-ink-muted">{a.resp} · {a.delai}</span>
                          <button onClick={() => cycleStatut(i)} className={`text-[9px] font-semibold px-1.5 py-0.5 rounded cursor-pointer hover:opacity-75 transition-opacity ${STATUT_ACTION[statut] || "bg-canvas text-ink-muted"}`}>
                            {statut}
                          </button>
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
            )}
          </div>

          {/* Colonne centre — Pré-rapport */}
          <div className="col-span-5">
            <div className="bg-surface rounded-xl shadow-md">
              <div className="flex items-center justify-between px-4 pt-4 pb-2 border-b border-divider">
                <h3 className="text-[10px] font-bold text-brand uppercase tracking-widest">{t("report.prereport_title")}</h3>
                <div className="flex gap-2">
                  <button
                    onClick={() => setAnonymise((v) => !v)}
                    className={`text-xs border rounded px-2 py-1 transition-colors ${anonymise ? "border-brand-amber bg-brand-amber/10 text-brand-amber font-semibold" : "border-divider text-ink-muted hover:bg-canvas"}`}
                  >
                    {anonymise ? <><Unlock size={12} className="inline mr-1" />{t("report.anonymized")}</> : <><Lock size={12} className="inline mr-1" />{t("report.anonymize")}</>}
                  </button>
                  <button onClick={handleTelecharger} disabled={downloading} className="text-xs rounded px-2 py-1 bg-brand text-white hover:bg-brand-cyan transition-colors disabled:opacity-50">
                    {downloading ? "…" : <><Download size={13} className="inline mr-1" />{t("report.download")}</>}
                  </button>
                  <button
                    onClick={() => setEditMode(v => !v)}
                    className={`text-xs border rounded px-2 py-1 transition-colors ${editMode ? "border-brand-emerald bg-brand-emerald/10 text-brand-emerald font-semibold" : "border-divider text-ink-muted hover:bg-canvas"}`}
                  >
                    {editMode ? <><FileCheck size={13} className="inline mr-1" />{t("report.validate")}</> : <><PenLine size={13} className="inline mr-1" />{t("report.edit")}</>}
                  </button>
                </div>
              </div>

              <div className="bg-surface-sunk shadow-inset mx-4 my-3 rounded-lg p-4 text-xs font-sans overflow-y-auto max-h-[520px]">
                <div className="text-center mb-4 border-b border-divider pb-3">
                  <div className="font-black text-sm uppercase tracking-wide text-brand">RATP</div>
                  <div className="font-bold text-ink mt-1">{t("report.report_header_title")}</div>
                </div>

                <table className="w-full text-xs mb-4 border-collapse">
                  <tbody>
                    {[
                      [t("report.field_client"),   anonymise ? t("report.anon_client")    : AUDIT_COURANT.nom],
                      [t("report.field_site"),     anonymise ? t("report.anon_site")      : AUDIT_COURANT.localisation],
                      [t("report.field_date"),     today],
                      [t("report.field_auditor"),  anonymise ? t("report.anon_inspector") : AUDITEUR.nom],
                      [t("report.field_standard"), AUDIT_COURANT.referentiel],
                      [t("report.field_duration"), dureeAudit],
                    ].map(([k, v]) => (
                      <tr key={k} className="border border-divider">
                        <td className="bg-canvas font-semibold px-2 py-1 w-28">{k}</td>
                        <td className="px-2 py-1 text-ink">{v}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>

                <div className="font-bold text-ink mb-2 uppercase text-[10px] tracking-widest border-b border-divider pb-1">{t("report.exec_summary")}</div>
                <p className="text-ink mb-3 leading-relaxed">
                  {t("report.audit_summary_intro", { nom: AUDIT_COURANT.nom, n: constats.length })}
                  {counts.majeure > 0 && ` ${t("report.major_nc_text", { n: counts.majeure })}`}
                  {counts.mineure > 0 && ` ${t("report.minor_nc_text", { n: counts.mineure })}`}
                  {counts.observation > 0 && ` ${t("report.observation_text", { n: counts.observation })}`}
                  {counts.conforme > 0 && ` ${t("report.compliant_text", { n: counts.conforme })}`}
                </p>

                {editConstats.map((c, i) => {
                  const borderColor = c.criticite === "majeure" ? "border-nc-majeure" : c.criticite === "mineure" ? "border-nc-mineure" : c.criticite === "observation" ? "border-observation" : "border-conforme"
                  return (
                    <div key={i} className={`mb-3 border-l-2 ${borderColor} pl-3 ${editMode ? "bg-brand-amber/5 rounded-r pr-1" : ""}`}>
                      <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded mr-1 ${CRITICITE_STYLE[c.criticite]?.badge}`}>{CRITICITE_STYLE[c.criticite]?.label}</span>
                      <span className="font-mono text-[10px] text-ink-muted">{c.clause}</span>
                      {editMode ? (
                        <textarea className="mt-1 w-full text-ink border border-divider rounded p-1 text-xs resize-none focus:outline-none focus:ring-1 focus:ring-brand" rows={3} value={c.constat} onChange={e => updateConstat(i, "constat", e.target.value)} />
                      ) : (
                        <div className="mt-1 text-ink">{c.constat}</div>
                      )}
                      {editMode ? (
                        <textarea className="w-full text-ink-muted border border-divider rounded p-1 text-xs resize-none focus:outline-none focus:ring-1 focus:ring-brand italic mt-0.5" rows={2} value={c.action || ""} placeholder={t("report.action_label")} onChange={e => updateConstat(i, "action", e.target.value)} />
                      ) : (
                        c.action && <div className="text-ink-muted italic mt-0.5">{t("report.action_label")} {c.action}</div>
                      )}
                      {c.photoUrl && !anonymise && (
                        <figure className="mt-1.5">
                          <img src={c.photoUrl} alt={`${t("report.photo_caption", { clause: c.clause })}`} className="rounded border border-divider max-h-40 object-contain" />
                          <figcaption className="text-[9px] text-ink-muted mt-0.5">{t("report.photo_caption", { clause: c.clause })}</figcaption>
                        </figure>
                      )}
                    </div>
                  )
                })}

                <div className="font-bold text-ink mb-2 uppercase text-[10px] tracking-widest border-b border-divider pb-1 mt-4">{t("report.recommendation_title")}</div>
                {editMode ? (
                  <textarea className="w-full text-ink border border-divider rounded p-1.5 text-xs resize-none leading-relaxed focus:outline-none focus:ring-1 focus:ring-brand bg-brand-amber/5" rows={3} value={editReco} onChange={e => setEditReco(e.target.value)} />
                ) : (
                  <p className="text-ink leading-relaxed">{editReco}</p>
                )}

                <div className="mt-6 flex justify-between text-[10px] text-ink-muted border-t border-divider pt-2">
                  <span>{AUDITEUR.nom} — {today}</span><span>{t("report.page_number") || "Page 1/1"}</span>
                </div>
              </div>
              <div className="h-2" />
            </div>
          </div>

          {/* Colonne droite — Envoi + Signature */}
          <div className="col-span-4 flex flex-col gap-4">
            <div className="card">
              <h3 className="section-label">{t("report.send_title")}</h3>
              {sent ? (
                <div className="flex flex-col items-center justify-center h-48 gap-3">
                  <div className="w-12 h-12 rounded-full flex items-center justify-center bg-brand-emerald/10 shadow-md">
                    <span className="text-2xl text-brand-emerald">✓</span>
                  </div>
                  <div className="text-sm font-semibold text-brand-emerald">{t("report.email_sent")}</div>
                  <div className="text-xs text-ink-muted text-center">{t("report.email_sent_desc", { email: emailTo })}</div>
                </div>
              ) : (
                <div className="space-y-3">
                  {[
                    [t("report.to_label"),      emailTo,                                                                          (e) => setEmailTo(e.target.value), "email"],
                    [t("report.cc_label"),       "audit-qualite@ratp.fr",                                                          null, "email"],
                    [t("report.subject_label"),  `${t("report.prereport_title").split(" —")[0]} ${AUDIT_COURANT.nom} — ${today}`, null, "text"],
                  ].map(([label, val, onChange, type]) => (
                    <div key={label}>
                      <label className="text-[10px] font-semibold text-ink-muted uppercase block mb-1">{label}</label>
                      <input type={type} defaultValue={onChange ? undefined : val} value={onChange ? val : undefined} onChange={onChange || undefined}
                        className="w-full border border-divider rounded px-2 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-brand"
                      />
                    </div>
                  ))}
                  <div>
                    <label className="text-[10px] font-semibold text-ink-muted uppercase block mb-1">{t("report.message_label")}</label>
                    <textarea value={message} onChange={(e) => setMessage(e.target.value)} rows={5}
                      className="w-full border border-divider rounded px-2 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-brand resize-none"
                    />
                  </div>
                  <div>
                    <label className="text-[10px] font-semibold text-ink-muted uppercase block mb-1">{t("report.attachment_label")}</label>
                    <div className="text-xs rounded px-2 py-1.5 text-brand bg-brand/5 border border-brand/20">
                      📎 Rapport_RATP_{AUDIT_COURANT.site_id}_{today.replace(/\//g, "-")}.docx
                    </div>
                  </div>
                </div>
              )}
            </div>

            <div className="card">
              <h3 className="section-label">{t("report.contradictory_validation")}</h3>
              {signed ? (
                <div className="flex flex-col items-center gap-2 py-3">
                  <span className="text-2xl">✅</span>
                  {signDataUrl
                    ? <img src={signDataUrl} alt="Signature" className="h-10 object-contain border-b border-brand/30 pb-1" />
                    : <div className="text-sm font-semibold text-ink italic">{signataire}</div>
                  }
                  <div className="text-xs font-semibold text-brand-emerald">{t("report.validated_by", { name: signataire })}</div>
                  <div className="text-[10px] text-ink-muted">{today}</div>
                </div>
              ) : signatureMode ? (
                <div className="space-y-3">
                  <div>
                    <div className="flex items-center justify-between mb-1">
                      <label className="text-[10px] font-semibold text-ink-muted uppercase">{t("report.signature_label")}</label>
                      <button onClick={clearSignCanvas} className="text-[10px] text-ink-muted hover:text-ink underline">{t("report.clear")}</button>
                    </div>
                    <canvas
                      ref={signCanvasRef}
                      width={600} height={110}
                      className="w-full rounded-lg border border-brand/30 bg-white cursor-crosshair touch-none"
                      onPointerDown={onSignDown}
                      onPointerMove={onSignMove}
                      onPointerUp={onSignUp}
                    />
                  </div>
                  <div className="text-[10px] text-ink-muted">{t("report.date_label")} {today}</div>
                  <div className="flex gap-2">
                    <button onClick={confirmerSignature} className="flex-1 text-xs font-semibold py-1.5 rounded-lg bg-brand-emerald text-white hover:bg-brand transition-colors shadow-sm">{t("report.confirm_signature")}</button>
                    <button onClick={() => setSignatureMode(false)} className="text-xs border border-divider rounded-lg px-3 py-1.5 text-ink-muted hover:bg-canvas">{t("common.cancel")}</button>
                  </div>
                </div>
              ) : (
                <div>
                  <p className="text-xs text-ink-muted mb-3">{t("report.sign_instruction")}</p>
                  <button onClick={() => setSignatureMode(true)} className="w-full border border-divider rounded-lg px-3 py-2 text-xs text-ink hover:bg-canvas font-medium">{t("report.sign_report")}</button>
                </div>
              )}
            </div>
          </div>

        </div>
      </div>
    </div>
  )
}
