import { useRef, useState, useEffect } from "react"
import { SUPPLIER_DOCUMENTS, SUPPLIER_DOC_CONTENT } from "../mockData"
import { analyserDocumentFournisseur } from "../api"
import { Upload, FileText, AlertOctagon, Calendar, Building2, ClipboardList, BookOpen, ListChecks, Wrench, ShieldAlert, ArrowUpDown, ChevronDown, Info } from "lucide-react"
import { useT } from "../useT"

// Groupes et items en français — les valeurs sont stockées dans doc.typeDoc donc restent en FR
const TYPES_DOCUMENT = [
  { groupe: "Audit & Inspection",           groupeKey: "portal.cat_audit",      tooltipKey: "portal.tooltip_audit",      items: ["Rapport d'audit BV", "Rapport d'audit", "Compte-rendu d'inspection", "Non-conformité", "Incident / anomalie", "Observation terrain", "Plan d'actions correctives", "Suivi des actions"] },
  { groupe: "Normes & Exigences",           groupeKey: "portal.cat_norms",      tooltipKey: "portal.tooltip_norms",      items: ["Norme ISO", "Réglementation nationale", "Directive / réglementation européenne", "Référentiel de certification", "Exigence légale", "Obligation contractuelle"] },
  { groupe: "Procédures & Règles internes", groupeKey: "portal.cat_procedures", tooltipKey: "portal.tooltip_procedures", items: ["Procédure interne", "Processus métier", "Mode opératoire", "Règles d'exploitation", "Instruction de travail", "Bonnes pratiques", "Guide interne"] },
  { groupe: "Technique & Équipements",      groupeKey: "portal.cat_technical",  tooltipKey: "portal.tooltip_technical",  items: ["Documentation technique", "Fiche équipement", "Plan / schéma", "Notice constructeur", "Maintenance (préventive / corrective)", "Spécifications techniques"] },
  { groupe: "Sécurité & Risques",           groupeKey: "portal.cat_safety",     tooltipKey: "portal.tooltip_safety",     items: ["Analyse de risques", "Plan de prévention", "Consignes de sécurité", "Document unique (DUERP)", "Gestion des incidents", "Plan d'urgence / crise"] },
  { groupe: "Autres",                       groupeKey: "portal.cat_other",      tooltipKey: null,                        items: ["Autres"] },
]

const CATEGORIE_META = {
  "Audit & Inspection":           { groupeKey: "portal.cat_audit",      tooltipKey: "portal.tooltip_audit",      Icon: ClipboardList, iconBg: "bg-orange-100", text: "text-orange-600", dot: "bg-orange-500", pill: "bg-orange-50 text-orange-700 border-orange-200" },
  "Normes & Exigences":           { groupeKey: "portal.cat_norms",      tooltipKey: "portal.tooltip_norms",      Icon: BookOpen,      iconBg: "bg-brand/10",   text: "text-brand",     dot: "bg-brand",      pill: "bg-brand/10 text-brand border-brand/20" },
  "Procédures & Règles internes": { groupeKey: "portal.cat_procedures", tooltipKey: "portal.tooltip_procedures", Icon: ListChecks,    iconBg: "bg-teal-100",   text: "text-teal-600",  dot: "bg-teal-500",   pill: "bg-teal-50 text-teal-700 border-teal-200" },
  "Technique & Équipements":      { groupeKey: "portal.cat_technical",  tooltipKey: "portal.tooltip_technical",  Icon: Wrench,        iconBg: "bg-slate-100",  text: "text-slate-600", dot: "bg-slate-500",  pill: "bg-slate-100 text-slate-600 border-slate-200" },
  "Sécurité & Risques":           { groupeKey: "portal.cat_safety",     tooltipKey: "portal.tooltip_safety",     Icon: ShieldAlert,   iconBg: "bg-amber-100",  text: "text-amber-600", dot: "bg-amber-500",  pill: "bg-amber-50 text-amber-700 border-amber-200" },
  "Autres":                       { groupeKey: "portal.cat_other",      tooltipKey: null,                        Icon: FileText,      iconBg: "bg-canvas",     text: "text-ink-muted", dot: "bg-ink-muted",  pill: "bg-canvas text-ink-muted border-divider" },
}

function getGroupe(typeDoc) {
  if (!typeDoc) return null
  return TYPES_DOCUMENT.find(g => g.items.includes(typeDoc))?.groupe ?? "Autres"
}

function DocStatusBadge({ statut, ag, ar, t }) {
  if (statut === "analysé")
    return <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${ag ? "bg-[#4CDFB2]/20 text-[#0a6e68]" : ar ? "bg-[#22C55E]/15 text-[#166534]" : "bg-green-100 text-green-700"}`}>{t("portal.analyzed_badge")}</span>
  if (statut === "analyse")
    return <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-blue-50 text-blue-600 flex items-center gap-1">
      <svg className="animate-spin h-2.5 w-2.5" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" /></svg>
      {t("portal.analyzing_badge")}
    </span>
  return <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-red-50 text-red-600">{t("portal.error_badge")}</span>
}

function InsightsPanel({ insights, ag, ar }) {
  if (!insights) return null
  return (
    <div className={`mt-2 rounded-lg p-3 text-[11px] ${ag ? "bg-[#5D93C1]/8 border border-[#5D93C1]/20" : ar ? "bg-[#4B87F8]/5 border border-[#4B87F8]/20" : "bg-blue-50 border border-blue-100"}`}>
      <p className="text-gray-700 mb-1.5 leading-snug">{insights.resume}</p>
      {insights.sections_a_risque?.length > 0 && (
        <div className="flex flex-wrap gap-1 mb-1">
          {insights.sections_a_risque.map((s, i) => (
            <span key={i} className="bg-orange-100 text-orange-700 px-1.5 py-0.5 rounded text-[10px] font-medium">{s}</span>
          ))}
        </div>
      )}
      {insights.nc_historique?.length > 0 && (
        <div className="space-y-0.5">
          {insights.nc_historique.map((nc, i) => (
            <div key={i} className="text-red-600 flex items-start gap-1">
              <span className="shrink-0">⚠</span><span>{nc}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function SupplierPortal({ theme, onBack, externalDocs = [], lang }) {
  const t = useT(lang)
  const ag = theme === "agile"
  const ar = theme === "aria"
  const fileInputRef = useRef(null)
  const [docs, setDocs] = useState(SUPPLIER_DOCUMENTS)
  const [expandedDoc, setExpandedDoc] = useState(null)
  const [typeDoc, setTypeDoc] = useState("")
  const [autreTexte, setAutreTexte] = useState("")
  const [filtreCategorie, setFiltreCategorie] = useState("Tous")
  const [triDate, setTriDate] = useState("desc")
  const [open, setOpen] = useState(false)
  const dropdownRef = useRef(null)

  useEffect(() => {
    function handleClickOutside(e) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener("mousedown", handleClickOutside)
    return () => document.removeEventListener("mousedown", handleClickOutside)
  }, [])

  useEffect(() => {
    if (!externalDocs?.length) return
    setDocs(prev => {
      const existing = new Set(prev.map(d => d.id))
      const newDocs = externalDocs.filter(d => !existing.has(d.id))
      return newDocs.length ? [...prev, ...newDocs] : prev
    })
  }, [externalDocs])

  const typeEffectif = typeDoc === "Autres" ? autreTexte.trim() : typeDoc
  const uploadDisabled = !typeDoc || (typeDoc === "Autres" && !autreTexte.trim())

  const docsFiltres = docs
    .filter(d => filtreCategorie === "Tous" || getGroupe(d.typeDoc) === filtreCategorie)
    .sort((a, b) => {
      const ta = parseInt(a.id.split("-").pop()) || 0
      const tb = parseInt(b.id.split("-").pop()) || 0
      return triDate === "desc" ? tb - ta : ta - tb
    })

  async function handleFileChange(e) {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ""

    const tempId = `doc-${Date.now()}`
    setDocs(prev => [...prev, { id: tempId, nom: file.name, type: file.name.split(".").pop().toUpperCase(), typeDoc: typeEffectif, date: "Aujourd'hui", statut: "analyse", insights: null }])
    setExpandedDoc(tempId)
    setTypeDoc("")
    setAutreTexte("")

    const contenu = await new Promise((resolve) => {
      const reader = new FileReader()
      reader.onload = (ev) => {
        const dataUrl = ev.target.result
        const b64 = dataUrl.includes(",") ? dataUrl.split(",")[1] : dataUrl
        resolve(b64)
      }
      reader.onerror = () => resolve(SUPPLIER_DOC_CONTENT["Procedures_etalonnage_v3.pdf"])
      reader.readAsDataURL(file)
    })

    const result = await analyserDocumentFournisseur(file.name, contenu)
    setDocs(prev => prev.map(d => d.id === tempId ? { ...d, statut: "analysé", insights: result, typeDoc: typeEffectif } : d))
  }

  const accentCls = ag ? "text-[#1AAED2]" : ar ? "text-[#4B87F8]" : "text-blue-600"
  const btnCls = ag
    ? "bg-[#5D93C1] hover:bg-[#1AAED2] text-white"
    : ar
      ? "bg-[#4B87F8] hover:bg-[#1C2B4A] text-white"
      : "bg-blue-600 hover:bg-blue-700 text-white"

  return (
    <div className="min-h-screen flex flex-col">
    <div className="page-content">
      <div className="grid-12">

        {/* Colonne gauche — Identité fournisseur + Audit planifié */}
        <div className="col-span-4 flex flex-col gap-4">

          <div className="card">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-3">{t("portal.supplier_header")}</div>
            <div className="flex items-center gap-3 mb-3">
              <img src="/mei-lin-zhang.png" alt="Mei Lin Zhang" className="w-10 h-10 rounded-full object-cover shrink-0" />
              <div>
                <div className="font-semibold text-sm text-gray-900">Mei Lin Zhang</div>
                <div className="text-xs text-gray-500">{t("portal.job_title")}</div>
              </div>
            </div>
            <dl className="space-y-2 text-xs">
              <div><dt className="text-[10px] text-gray-400 uppercase font-semibold">{t("portal.company_label")}</dt><dd className="text-gray-700 font-medium">RATP — Atelier Sucy-en-Brie</dd></div>
              <div><dt className="text-[10px] text-gray-400 uppercase font-semibold">{t("portal.staff_label")}</dt><dd className="text-gray-700">218 {lang === "FR" ? "personnes" : "persons"}</dd></div>
              <div><dt className="text-[10px] text-gray-400 uppercase font-semibold">{t("portal.standard_label")}</dt><dd className={`font-medium ${accentCls}`}>ISO 9001:2015</dd></div>
              <div><dt className="text-[10px] text-gray-400 uppercase font-semibold">{t("portal.contact_label")}</dt><dd className={`text-xs ${accentCls}`}>meilin.zhang@ratp.fr</dd></div>
            </dl>
          </div>

          <div className={`rounded-xl border shadow-sm p-4 ${ag ? "bg-[#494949] border-[#5D93C1]/30" : ar ? "bg-[#1C2B4A] border-[#4B87F8]/30" : "bg-blue-700 border-blue-600"}`}>
            <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest mb-2">{t("portal.next_audit_title")}</div>
            <div className="text-white font-bold text-sm mb-0.5">Marc Lefèvre</div>
            <div className="text-white/70 text-xs mb-2">Bureau Veritas — ISO 9001</div>
            <div className="flex items-center gap-2 text-xs text-white/80">
              <Calendar size={13} /><span>{lang === "FR" ? "Mercredi 14h30" : "Wednesday 2:30 PM"}</span>
            </div>
            <div className="flex items-center gap-2 text-xs text-white/80 mt-1">
              <span>⏱</span><span>{t("portal.expected_duration")} 2h30</span>
            </div>
            <div className={`mt-3 text-[10px] font-semibold px-2 py-1 rounded text-center ${ag ? "bg-[#4CDFB2]/20 text-[#4CDFB2]" : ar ? "bg-[#22C55E]/20 text-[#22C55E]" : "bg-white/10 text-white"}`}>
              {(() => {
                const n = docs.filter(d => d.statut === "analysé").length
                return n === 1 ? t("portal.docs_shared_singular", { n }) : t("portal.docs_shared_plural", { n })
              })()}
            </div>
          </div>

          {/* Alertes IA */}
          <div className="card">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-3 flex items-center gap-1.5"><AlertOctagon size={11} />{t("portal.ai_alerts_title")}</div>
            <div className="space-y-2">
              <div className="flex items-start gap-2 p-2 bg-red-50 border border-red-200 rounded-lg">
                <span className="text-red-500 shrink-0 text-sm leading-tight">🔴</span>
                <div>
                  <div className="text-[10px] font-bold text-red-700">§7.1.5 — NC MAJEURE</div>
                  <div className="text-[10px] text-red-600">3 {lang === "FR" ? "clés dyno périmées — non clôturée nov. 2024" : "expired torque wrenches — not closed Nov 2024"}</div>
                </div>
              </div>
              <div className="flex items-start gap-2 p-2 bg-orange-50 border border-orange-200 rounded-lg">
                <span className="text-orange-500 shrink-0 text-sm leading-tight">🟡</span>
                <div>
                  <div className="text-[10px] font-bold text-orange-700">§8.7 — NC MINEURE</div>
                  <div className="text-[10px] text-orange-600">{lang === "FR" ? "Zone quarantaine partiellement délimitée" : "Quarantine zone partially delimited"}</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Colonne principale — Documents */}
        <div className="col-span-8">
          <div className="card">
            <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100">
              <div>
                <h2 className="font-semibold text-gray-900 text-sm">{t("portal.docs_title")}</h2>
                <p className="text-[11px] text-gray-400 mt-0.5">{t("portal.docs_description")}</p>
              </div>
              <div className="flex items-end gap-2">
                <span className="text-xs text-gray-400 self-center">{t("portal.analyzed_count", { n: docs.filter(d => d.statut === "analysé").length, total: docs.length })}</span>
                <div className="flex flex-col gap-1.5">
                  <div ref={dropdownRef} className="relative min-w-[220px]">
                    <button
                      type="button"
                      onClick={() => setOpen(v => !v)}
                      className="w-full text-xs border border-gray-200 rounded-lg px-2 py-1.5 bg-white focus:outline-none focus:ring-1 focus:ring-brand text-left flex items-center justify-between gap-1"
                    >
                      <span className={typeDoc ? "text-gray-700" : "text-gray-400"}>
                        {typeDoc || t("portal.doc_type_placeholder")}
                      </span>
                      <ChevronDown size={12} className={`text-gray-400 transition-transform ${open ? "rotate-180" : ""}`} />
                    </button>
                    {open && (
                      <div className="absolute top-full left-0 mt-1 w-full bg-white border border-gray-200 rounded-lg shadow-md z-30 overflow-visible">
                        {TYPES_DOCUMENT.map(g => {
                          const meta = CATEGORIE_META[g.groupe]
                          const tooltip = g.tooltipKey ? t(g.tooltipKey) : null
                          return (
                            <div key={g.groupe}>
                              <div className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-50 border-b border-gray-100">
                                <span className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">{t(g.groupeKey)}</span>
                                {tooltip && (
                                  <div className="relative group/tip">
                                    <Info size={11} className="text-brand cursor-default" />
                                    <span className="pointer-events-none absolute left-1/2 -translate-x-1/2 top-full mt-1 px-2 py-0.5 rounded bg-gray-800 text-white text-[9px] whitespace-nowrap opacity-0 group-hover/tip:opacity-100 transition-opacity z-40">
                                      {tooltip}
                                    </span>
                                  </div>
                                )}
                              </div>
                              {g.items.map(item => (
                                <button
                                  key={item}
                                  type="button"
                                  onClick={() => { setTypeDoc(item); setAutreTexte(""); setOpen(false) }}
                                  className={`w-full text-left text-xs px-4 py-1.5 hover:bg-brand/5 transition-colors ${typeDoc === item ? "text-brand font-medium bg-brand/5" : "text-gray-700"}`}
                                >
                                  {item}
                                </button>
                              ))}
                            </div>
                          )
                        })}
                      </div>
                    )}
                  </div>
                  {typeDoc === "Autres" && (
                    <input
                      type="text"
                      value={autreTexte}
                      onChange={e => setAutreTexte(e.target.value)}
                      placeholder={t("portal.other_type_placeholder")}
                      className="text-xs border border-gray-200 rounded-lg px-2 py-1.5 focus:outline-none focus:ring-1 focus:ring-brand"
                    />
                  )}
                </div>
                <input ref={fileInputRef} type="file" accept=".pdf,.docx,.doc" className="hidden" onChange={handleFileChange} />
                <button
                  onClick={() => fileInputRef.current?.click()}
                  disabled={uploadDisabled}
                  className={`text-xs font-semibold px-4 py-2 rounded-lg transition-colors self-end ${uploadDisabled ? "bg-gray-200 text-gray-400 cursor-not-allowed" : `${btnCls}`}`}
                >
                  <Upload size={13} className="inline mr-1" />{t("portal.upload_btn")}
                </button>
              </div>
            </div>

            {/* Barre de filtres */}
            <div className="px-5 py-2.5 border-b border-gray-100 flex items-center gap-2 flex-wrap bg-canvas">
              <button
                onClick={() => setFiltreCategorie("Tous")}
                className={`text-[10px] font-semibold px-2.5 py-1 rounded-full border transition-colors ${filtreCategorie === "Tous" ? "bg-dark-teal text-white border-dark-teal" : "border-divider text-ink-muted hover:bg-surface"}`}
              >
                {t("portal.all_filter", { n: docs.length })}
              </button>
              {TYPES_DOCUMENT.map(g => {
                const count = docs.filter(d => getGroupe(d.typeDoc) === g.groupe).length
                if (count === 0) return null
                const meta = CATEGORIE_META[g.groupe]
                const label = t(g.groupeKey).includes(" & ") ? t(g.groupeKey).split(" & ")[0] : t(g.groupeKey)
                const tooltip = g.tooltipKey ? t(g.tooltipKey) : null
                return (
                  <div key={g.groupe} className="relative group">
                    <button
                      onClick={() => setFiltreCategorie(g.groupe)}
                      className={`text-[10px] font-semibold px-2.5 py-1 rounded-full border transition-colors flex items-center gap-1 ${
                        filtreCategorie === g.groupe ? `${meta.dot} text-white border-transparent` : meta.pill
                      }`}
                    >
                      <meta.Icon size={9} />{label} ({count})
                    </button>
                    {tooltip && (
                      <span className="pointer-events-none absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 px-2 py-0.5 rounded bg-gray-800 text-white text-[9px] whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity z-20">
                        {tooltip}
                      </span>
                    )}
                  </div>
                )
              })}
              <div className="ml-auto flex items-center gap-1.5">
                <span className="text-[10px] text-ink-muted">{t("portal.sort_label")}</span>
                <button
                  onClick={() => setTriDate(v => v === "desc" ? "asc" : "desc")}
                  className="text-[10px] font-medium px-2 py-1 rounded-full border border-divider text-ink-muted hover:bg-surface flex items-center gap-1"
                >
                  <ArrowUpDown size={9} />{triDate === "desc" ? t("portal.sort_recent") : t("portal.sort_old")}
                </button>
              </div>
            </div>

            <div className="divide-y divide-gray-100">
              {docsFiltres.map(doc => (
                <div key={doc.id} className="px-5 py-4">
                  <div className="flex items-center gap-4">
                    {(() => {
                      const groupe = getGroupe(doc.typeDoc)
                      const meta = groupe ? CATEGORIE_META[groupe] : null
                      return (
                        <div className={`w-10 h-10 rounded-lg flex items-center justify-center shrink-0 ${meta ? meta.iconBg : "bg-canvas"}`}>
                          {meta
                            ? <meta.Icon size={18} className={meta.text} />
                            : <FileText size={18} className="text-ink-muted" />
                          }
                        </div>
                      )
                    })()}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-3">
                        <span className="font-medium text-sm text-gray-900 truncate">{doc.nom}</span>
                        <DocStatusBadge statut={doc.statut} ag={ag} ar={ar} t={t} />
                        {doc.fromBV && (
                          <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-brand text-white shrink-0">{t("portal.bureau_veritas_badge")}</span>
                        )}
                      </div>
                      <div className="flex items-center gap-2 mt-0.5">
                        <span className="text-[11px] text-gray-400">{t("portal.deposited_on")} {doc.date}</span>
                        {doc.deposePar && (
                          <span className="text-[11px] text-gray-400">· {doc.deposePar}</span>
                        )}
                        {doc.typeDoc && (
                          <span className="text-[10px] font-medium px-1.5 py-0.5 rounded bg-brand/10 text-brand">{doc.typeDoc}</span>
                        )}
                      </div>
                    </div>
                    {doc.statut === "analysé" && (
                      <button
                        onClick={() => setExpandedDoc(expandedDoc === doc.id ? null : doc.id)}
                        className={`text-xs shrink-0 ${accentCls} hover:underline`}
                      >
                        {expandedDoc === doc.id ? t("portal.hide_analysis") : t("portal.show_analysis")}
                      </button>
                    )}
                    {(doc.dataUrl || doc.url) && (
                      <button
                        onClick={() => { const a = document.createElement("a"); a.href = doc.dataUrl ?? doc.url; a.download = doc.nom; a.click() }}
                        className="text-xs shrink-0 text-brand-emerald hover:underline flex items-center gap-1"
                      >{t("portal.download")}</button>
                    )}
                    {!doc.mock && !doc.fromBV && (
                      <button
                        onClick={() => setDocs(prev => prev.filter(d => d.id !== doc.id))}
                        className="shrink-0 text-gray-300 hover:text-red-400 transition-colors text-base leading-none"
                        title={t("portal.delete_tooltip")}
                      >
                        ×
                      </button>
                    )}
                  </div>
                  {expandedDoc === doc.id && doc.insights && (
                    <div className="mt-3 ml-14">
                      <InsightsPanel insights={doc.insights} ag={ag} ar={ar} />
                      {doc.insights.points_controle?.length > 0 && (
                        <div className="mt-2">
                          <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-1">{t("portal.control_points_title")}</div>
                          <ul className="space-y-0.5">
                            {doc.insights.points_controle.map((p, i) => (
                              <li key={i} className="text-[11px] text-gray-600 flex items-start gap-1.5">
                                <span className={`shrink-0 mt-0.5 ${accentCls}`}>›</span>{p}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              ))}

              {docsFiltres.length === 0 && (
                <div className="px-5 py-12 text-center text-gray-400 text-sm">
                  {docs.length === 0
                    ? t("portal.no_docs")
                    : t("portal.no_docs_filtered")}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
    </div>
  )
}
