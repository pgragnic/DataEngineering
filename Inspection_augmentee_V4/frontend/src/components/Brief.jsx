import { useEffect, useState } from "react"
import { AUDIT_COURANT, CHECKLIST, AUDITS_PRECEDENTS, SUPPLIER_DOCUMENTS, SUPPLIER_ALERTS, SCOPE_OPTIONS, RECURRENCES } from "../mockData"
import { getSite } from "../api"
import { FileText, History, ClipboardList, Upload, AlertTriangle, Calendar, Pencil, Check, X, Info, ChevronRight, Users, BookOpen, RotateCcw } from "lucide-react"
import { useT } from "../useT"

const inputCls = "w-full text-xs border border-divider rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-brand bg-surface"

export default function Brief({ onDemarrer, onBack, theme, lang }) {
  const t = useT(lang)
  const [sectionsVisibles, setSectionsVisibles] = useState(0)
  const [genere, setGenere] = useState(false)
  const [genSec, setGenSec] = useState(null)
  const [customSections, setCustomSections] = useState([])
  const [extraItemsBySectionId, setExtraItemsBySectionId] = useState({})
  const [addingItemTo, setAddingItemTo] = useState(null)
  const [newItemTexte, setNewItemTexte] = useState("")
  const [addingSection, setAddingSection] = useState(false)
  const [newSectionTitre, setNewSectionTitre] = useState("")
  const [newSectionClause, setNewSectionClause] = useState("")
  const [removedItemIds, setRemovedItemIds] = useState(new Set())
  const [removedSectionIds, setRemovedSectionIds] = useState(new Set())
  const [addingScopeItem, setAddingScopeItem] = useState(false)
  const [showChecklistInfo, setShowChecklistInfo] = useState(false)
  const [showHistoriqueInfo, setShowHistoriqueInfo] = useState(false)
  const [selectedRef, setSelectedRef] = useState("ISO 9001:2015")
  const [siteData, setSiteData] = useState(null)

  const [editingSection, setEditingSection] = useState(null)
  const [snapshot, setSnapshot] = useState(null)
  const [showHistory, setShowHistory] = useState(false)

  const [editedBriefData, setEditedBriefData] = useState({
    nom: AUDIT_COURANT.nom,
    localisation: AUDIT_COURANT.localisation,
    effectif: AUDIT_COURANT.effectif,
    scope: [...AUDIT_COURANT.scope],
    responsable_qualite: AUDIT_COURANT.responsable_qualite,
    contact: AUDIT_COURANT.contact,
    duree_prevue: AUDIT_COURANT.duree_prevue,
  })

  const startEdit = (section) => {
    setSnapshot({ ...editedBriefData, scope: [...editedBriefData.scope] })
    setEditingSection(section)
  }

  const saveSection = () => setEditingSection(null)

  const cancelSection = () => {
    setEditedBriefData(snapshot)
    setEditingSection(null)
  }

  useEffect(() => {
    getSite(AUDIT_COURANT.site_id)
      .then(data => {
        setSiteData(data)
        setEditedBriefData(prev => ({
          ...prev,
          nom:                 data.nom,
          localisation:        data.localisation,
          effectif:            data.effectif,
          responsable_qualite: data.responsable_qualite,
        }))
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    const start = Date.now()
    let count = 0
    const interval = setInterval(() => {
      count += 1
      setSectionsVisibles(count)
      if (count >= CHECKLIST.length) {
        clearInterval(interval)
        setGenSec(((Date.now() - start) / 1000).toFixed(1))
        setGenere(true)
      }
    }, 300)
    return () => clearInterval(interval)
  }, [])

  const totalPoints = CHECKLIST.reduce((s, c) => s + c.points, 0)

  const scopeActiveSections = new Set(
    editedBriefData.scope.flatMap(label => {
      const opt = SCOPE_OPTIONS.find(o => o.label === label)
      return opt ? opt.sections : []
    })
  )

  const historiqueAudits = siteData?.historique_audits
    ? siteData.historique_audits.map(a => ({
        date:        a.date,
        auditeur:    a.auditeur,
        nc_majeures: a.non_conformites_majeures,
        nc_mineures: a.non_conformites_mineures,
        themes:      Array.isArray(a.themes_recurrents) ? a.themes_recurrents : [],
        alerte:      false,
      }))
    : AUDITS_PRECEDENTS

  const SectionHeader = ({ label, id }) => (
    <dt className="text-[10px] font-semibold text-ink-muted uppercase flex items-center justify-between">
      <span>{label}</span>
      {editingSection === id ? (
        <span className="flex gap-1">
          <button onClick={saveSection} className="p-0.5 rounded hover:bg-brand/10 text-brand-emerald" title={t("brief.save_title")}><Check size={10} /></button>
          <button onClick={cancelSection} className="p-0.5 rounded hover:bg-red-50 text-ink-muted" title={t("brief.cancel_title")}><X size={10} /></button>
        </span>
      ) : (
        <button onClick={() => startEdit(id)} className="p-0.5 rounded hover:bg-brand/10 text-ink-muted hover:text-brand" title={t("brief.edit_title")}>
          <Pencil size={10} />
        </button>
      )}
    </dt>
  )

  return (
    <div className="min-h-screen flex flex-col">
      {/* Barre haut */}
      <div className="page-bar">
        <button onClick={onBack} className="text-xs text-brand hover:underline flex items-center gap-1">
          {t("brief.back")}
        </button>
        <div className="text-xs text-ink-muted flex items-center gap-3 flex-1 justify-center min-w-0 overflow-hidden">
          <span className="font-semibold text-ink truncate min-w-0">
            {lang === "EN" ? (AUDIT_COURANT.titreEN || AUDIT_COURANT.titre) : AUDIT_COURANT.titre}
          </span>
          <span className="text-divider shrink-0">·</span>
          <span className="text-brand-cyan truncate min-w-0">{AUDIT_COURANT.nom}</span>
          <span className="text-divider shrink-0">·</span>
          {genere ? (
            <span className="font-medium text-brand-emerald shrink-0">
              {t("brief.checklist_count", { n: CHECKLIST.length, p: totalPoints })}
            </span>
          ) : (
            <span className="shrink-0">{t("brief.generating_checklist")}</span>
          )}
        </div>
        <button
          onClick={() => {
            const effectiveRemovedSections = scopeActiveSections.size > 0
              ? new Set([...removedSectionIds, ...CHECKLIST.map(s => s.id).filter(id => !scopeActiveSections.has(id))])
              : removedSectionIds
            onDemarrer(customSections, extraItemsBySectionId, removedItemIds, effectiveRemovedSections)
          }}
          disabled={!genere}
          className="disabled:bg-divider disabled:text-ink-muted disabled:cursor-not-allowed text-white font-semibold px-5 py-2 rounded-lg text-sm bg-brand hover:bg-brand-cyan transition-colors shadow-sm"
        >
          {t("brief.start_button")}
        </button>
      </div>

      <div className="page-content">
        <div className="grid-12">

          {/* ── Colonne gauche — Mission Brief ─────────────────────────────── */}
          <div className={`card ${showHistory ? "col-span-4" : "col-span-6"}`}>
            <h2 className="section-label">
              <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><FileText size={11} className="text-brand" /></span>{t("brief.brief_title")}
            </h2>

            {/* ── Audit scope ── */}
            <div className="mb-4">
              <div className="text-[10px] font-bold text-brand uppercase tracking-widest flex items-center gap-1.5 mb-2 pb-1 border-b border-divider">
                <BookOpen size={10} />{t("brief.section_audit_scope")}
              </div>
              <dl className="space-y-2.5 text-sm">
                <div>
                  <dt className="text-[10px] font-semibold text-ink-muted uppercase">{lang === "EN" ? "Mission" : "Mission"}</dt>
                  <dd className="mt-0.5 font-semibold text-ink text-sm">{AUDIT_COURANT.titre}</dd>
                </div>
                <div>
                  <dt className="text-[10px] font-semibold text-ink-muted uppercase">{t("brief.client_label")}</dt>
                  <dd className="mt-0.5"><span className="text-ink">{AUDIT_COURANT.client}</span></dd>
                </div>
                <div>
                  <SectionHeader label={t("brief.location_label")} id="localisation" />
                  <dd className="mt-0.5">
                    {editingSection === "localisation"
                      ? <input autoFocus value={editedBriefData.localisation} onChange={e => setEditedBriefData(p => ({ ...p, localisation: e.target.value }))} className={inputCls} />
                      : <span className="text-ink">{editedBriefData.localisation}</span>
                    }
                  </dd>
                </div>
                <div>
                  <SectionHeader label={t("brief.staff_label")} id="effectif" />
                  <dd className="mt-0.5">
                    {editingSection === "effectif"
                      ? (
                        <span className="flex items-center gap-1">
                          <input autoFocus type="number" value={editedBriefData.effectif} onChange={e => setEditedBriefData(p => ({ ...p, effectif: Number(e.target.value) }))} className="text-xs border border-divider rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-brand bg-surface w-20 text-center" />
                          <span className="text-xs text-ink-muted">{t("brief.persons")}</span>
                        </span>
                      )
                      : <span className="text-ink">{editedBriefData.effectif} {t("brief.persons")}</span>
                    }
                  </dd>
                </div>
                <div>
                  <SectionHeader label={t("brief.scope_label")} id="scope" />
                  <dd className="mt-0.5 space-y-1">
                    {editedBriefData.scope.map((s, i) => (
                      editingSection === "scope" ? (
                        <div key={i} className="flex items-center gap-1">
                          <input
                            value={s}
                            onChange={e => {
                              const ns = [...editedBriefData.scope]
                              ns[i] = e.target.value
                              setEditedBriefData(p => ({ ...p, scope: ns }))
                            }}
                            className="flex-1 text-xs border border-divider rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-brand bg-surface"
                          />
                          <button onClick={() => setEditedBriefData(p => ({ ...p, scope: p.scope.filter((_, j) => j !== i) }))} className="text-ink-muted hover:text-red-500 shrink-0 text-sm leading-none">×</button>
                        </div>
                      ) : (
                        <div key={i} className="text-xs text-ink py-0.5 flex items-center gap-1">
                          <ChevronRight size={10} className="text-brand shrink-0" />
                          {lang === "EN" ? (SCOPE_OPTIONS.find(o => o.label === s)?.labelEN || s) : s}
                        </div>
                      )
                    ))}
                    {editingSection === "scope" && (
                      <button onClick={() => setEditedBriefData(p => ({ ...p, scope: [...p.scope, ""] }))} className="text-[10px] text-brand hover:text-brand-cyan mt-0.5">{t("brief.add_free_text")}</button>
                    )}
                    {!addingScopeItem ? (
                      <button onClick={() => setAddingScopeItem(true)} className="mt-1 text-[10px] flex items-center gap-1 text-brand hover:text-brand-cyan transition-colors">
                        {t("brief.add_scope")}
                      </button>
                    ) : (
                      <div className="mt-2 bg-surface border border-divider rounded-lg p-2 space-y-0.5">
                        {SCOPE_OPTIONS.filter(o => !editedBriefData.scope.includes(o.label)).map(o => (
                          <button
                            key={o.id}
                            onClick={() => { setEditedBriefData(p => ({ ...p, scope: [...p.scope, o.label] })); setAddingScopeItem(false) }}
                            className="w-full text-left text-xs px-2 py-1 rounded hover:bg-brand/5 hover:text-brand transition-colors"
                          >
                            {lang === "EN" ? o.labelEN : o.label}
                          </button>
                        ))}
                        {SCOPE_OPTIONS.every(o => editedBriefData.scope.includes(o.label)) && (
                          <div className="text-[10px] text-ink-muted px-2 py-1">{t("brief.all_scopes_selected")}</div>
                        )}
                        <button onClick={() => setAddingScopeItem(false)} className="text-[10px] text-ink-muted hover:text-ink px-2 pt-1">{t("brief.cancel_title")}</button>
                      </div>
                    )}
                  </dd>
                </div>
              </dl>
            </div>

            {/* ── Standards ── */}
            <div className="mb-4 pt-3 border-t border-divider">
              <div className="text-[10px] font-bold text-brand uppercase tracking-widest flex items-center gap-1.5 mb-2 pb-1 border-b border-divider">
                <FileText size={10} />{t("brief.section_standards")}
              </div>
              <dl className="space-y-2.5 text-sm">
                <div>
                  <dt className="text-[10px] font-semibold text-ink-muted uppercase mb-1.5">{t("brief.standard_label")}</dt>
                  <dd className="flex flex-wrap gap-1">
                    {["ISO 9001:2015", "ISO 14001:2015", "ISO 45001:2018"].map(ref => (
                      <button
                        key={ref}
                        onClick={() => setSelectedRef(ref)}
                        className={`text-[10px] font-semibold px-2 py-0.5 rounded-full border transition-colors ${
                          selectedRef === ref
                            ? "bg-brand text-white border-brand"
                            : "bg-surface text-ink-muted border-divider hover:border-ink-muted hover:text-ink"
                        }`}
                      >{ref}</button>
                    ))}
                  </dd>
                  {selectedRef !== "ISO 9001:2015" && (
                    <dd className="text-[10px] mt-1.5 italic text-brand-emerald">
                      {t("brief.checklist_adapted", { ref: selectedRef })}
                    </dd>
                  )}
                </div>
                <div>
                  <SectionHeader label={t("brief.duration_label")} id="duree" />
                  <dd className="mt-0.5">
                    {editingSection === "duree"
                      ? (
                        <span className="flex items-center gap-1">
                          <input autoFocus value={editedBriefData.duree_prevue} onChange={e => setEditedBriefData(p => ({ ...p, duree_prevue: e.target.value }))} className="text-xs border border-divider rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-brand bg-surface w-20" />
                          <span className="text-xs text-ink-muted">{t("brief.on_site")}</span>
                        </span>
                      )
                      : <span className="text-ink">{editedBriefData.duree_prevue} {t("brief.on_site")}</span>
                    }
                  </dd>
                </div>
              </dl>
            </div>

            {/* ── Quality contacts ── */}
            <div className="mb-4 pt-3 border-t border-divider">
              <div className="text-[10px] font-bold text-brand uppercase tracking-widest flex items-center gap-1.5 mb-2 pb-1 border-b border-divider">
                <Users size={10} />{t("brief.section_quality_contacts")}
              </div>
              <dl className="space-y-2.5 text-sm">
                <div>
                  <SectionHeader label={t("brief.contact_label")} id="contact" />
                  <dd className="mt-0.5">
                    {editingSection === "contact"
                      ? <input autoFocus value={editedBriefData.responsable_qualite} onChange={e => setEditedBriefData(p => ({ ...p, responsable_qualite: e.target.value }))} className={inputCls} />
                      : <span className="text-ink">{editedBriefData.responsable_qualite}</span>
                    }
                  </dd>
                  <dd className="mt-0.5">
                    {editingSection === "contact"
                      ? <input value={editedBriefData.contact} onChange={e => setEditedBriefData(p => ({ ...p, contact: e.target.value }))} className={inputCls} />
                      : <span className="text-xs text-brand">{editedBriefData.contact}</span>
                    }
                  </dd>
                </div>
              </dl>
            </div>

            {/* Documents portail RATP */}
            <div className="mt-4 pt-4 border-t border-divider">
              <div className="flex items-center justify-between mb-2">
                <div className="text-[10px] font-semibold text-ink-muted uppercase flex items-center gap-1"><Upload size={10} />{t("brief.portal_docs_title")}</div>
                <span className="text-[10px] font-bold px-1.5 py-0.5 rounded-full bg-brand-emerald/10 text-brand-emerald">
                  {t("brief.portal_docs_analyzed", { n: SUPPLIER_DOCUMENTS.length })}
                </span>
              </div>
              <div className="space-y-1.5">
                {SUPPLIER_DOCUMENTS.map(doc => (
                  <div key={doc.id} className="relative group/doc flex items-start gap-2 bg-surface-sunk shadow-inset rounded-lg px-2.5 py-2">
                    <span className="text-xs leading-none mt-0.5 shrink-0 text-brand-emerald">✓</span>
                    <div className="min-w-0 flex-1">
                      <a href={doc.url} target="_blank" rel="noopener noreferrer" className="text-[10px] font-medium text-ink hover:text-brand hover:underline truncate block">{doc.nom}</a>
                      <div className="text-[10px] text-ink-muted">{doc.date} · {doc.insights?.sections_a_risque?.[0] ?? "Analysé"}</div>
                    </div>
                    {doc.url && (
                      <a href={doc.url} download={doc.nom} className="text-[10px] text-brand hover:underline shrink-0 self-center">⬇</a>
                    )}
                    {/* Aperçu au survol */}
                    <div className="absolute bottom-full left-0 mb-1.5 w-72 z-40 opacity-0 group-hover/doc:opacity-100 pointer-events-none transition-opacity duration-150">
                      <div className="bg-white border border-divider rounded-xl shadow-lg overflow-hidden">
                        {doc.type === "PDF" ? (
                          <embed src={doc.url} type="application/pdf" className="w-full h-44" />
                        ) : (
                          <div className="bg-blue-50 flex items-center justify-center h-20 gap-2">
                            <span className="text-2xl">📄</span>
                            <span className="text-xs font-semibold text-blue-700">{t("brief.document_word")}</span>
                          </div>
                        )}
                        <div className="p-2.5 space-y-1.5">
                          <p className="text-[10px] text-ink leading-relaxed">{doc.insights?.resume}</p>
                          {doc.insights?.sections_a_risque?.length > 0 && (
                            <div>
                              <div className="text-[9px] font-bold text-brand-amber uppercase mb-0.5">{t("brief.risk_sections")}</div>
                              {doc.insights.sections_a_risque.map((s, i) => (
                                <div key={i} className="text-[9px] text-ink-muted">· {s}</div>
                              ))}
                            </div>
                          )}
                          {doc.insights?.points_controle?.length > 0 && (
                            <div>
                              <div className="text-[9px] font-bold text-brand uppercase mb-0.5">{t("brief.control_points_check")}</div>
                              {doc.insights.points_controle.slice(0, 2).map((p, i) => (
                                <div key={i} className="text-[9px] text-ink-muted">· {p}</div>
                              ))}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
              <div className="mt-2 text-[10px] text-center text-brand">
                {t("brief.portal_docs_deposited_by")}
              </div>
            </div>
          </div>

          {/* ── Colonne centre — Check-list IA ────────────────────────────── */}
          <div className={`card ${showHistory ? "col-span-4" : "col-span-6"}`}>
            <div className="relative">
              <h2 className="section-label">
                <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><ClipboardList size={11} className="text-brand" /></span>
                {t("brief.checklist_title")}
                <button onClick={() => setShowChecklistInfo(v => !v)} className="ml-1 text-brand/50 hover:text-brand transition-colors" title={t("brief.how_generated_question")}>
                  <Info size={12} />
                </button>
              </h2>
              {showChecklistInfo && (
                <div className="absolute top-7 left-0 right-0 z-30 bg-white border border-brand/20 rounded-xl shadow-lg p-3 text-xs text-ink-muted space-y-1.5">
                  <div className="flex items-center justify-between mb-1">
                    <span className="font-semibold text-brand text-[10px] uppercase tracking-wide">{t("brief.how_generated_title")}</span>
                    <button onClick={() => setShowChecklistInfo(false)} className="text-ink-muted hover:text-ink"><X size={12} /></button>
                  </div>
                  <p>{t("brief.how_generated_intro", { n: 5 })}</p>
                  <ul className="space-y-1 pl-2">
                    <li><span className="font-medium text-ink">{t("brief.source_1_label")}</span> — {t("brief.source_1_desc")}</li>
                    <li><span className="font-medium text-ink">{t("brief.source_2_label")}</span> — {t("brief.source_2_desc")}</li>
                    <li><span className="font-medium text-ink">{t("brief.source_3_label")}</span> — {t("brief.source_3_desc")}</li>
                    <li><span className="font-medium text-ink">{t("brief.source_4_label")}</span> — {t("brief.source_4_desc")}</li>
                    <li><span className="font-medium text-ink">{t("brief.source_5_label")}</span> — {t("brief.source_5_desc")}</li>
                  </ul>
                </div>
              )}
            </div>
            {!genere && (
              <div className="flex items-center gap-2 mb-4 mt-3">
                <svg className="animate-spin h-3 w-3 text-brand" viewBox="0 0 24 24" fill="none">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
                </svg>
                <span className="text-xs text-brand">{t("brief.generating")}</span>
              </div>
            )}

            <div className="space-y-3">
              {CHECKLIST.slice(0, sectionsVisibles).filter(s => !removedSectionIds.has(s.id)).map((section) => {
                const extraItems = extraItemsBySectionId[section.id] || []
                const alerte = SUPPLIER_ALERTS[section.clause] || null
                const inScope = scopeActiveSections.has(section.id)
                const sectionTitle = lang === "EN" && section.titreEN ? section.titreEN : section.titre
                const siteRec = RECURRENCES.find(r => r.site_id === AUDIT_COURANT.site_id)
                const hasRecurrence = siteRec?.items?.some(item => item.includes(section.clause))
                const borderCls = alerte
                  ? alerte.criticite === "majeure" ? "border-l-4 border-red-500" : "border-l-4 border-amber-400"
                  : inScope ? "border-l-4 border-brand" : ""
                return (
                  <div key={section.id} className={`bg-surface-sunk shadow-inset rounded-lg p-3 animate-fade-in transition-opacity ${borderCls} ${!inScope && scopeActiveSections.size > 0 ? "opacity-50" : ""}`}>
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-1.5 min-w-0 flex-wrap">
                        <span className="text-xs font-bold text-brand shrink-0">{section.id}</span>
                        {inScope && (
                          <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-brand/15 text-brand shrink-0">{t("brief.scope_badge")}</span>
                        )}
                        {alerte && (
                          <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded shrink-0 ${alerte.criticite === "majeure" ? "bg-nc-majeure text-white" : "bg-nc-mineure text-white"}`}>
                            {alerte.criticite === "majeure" ? t("brief.ratp_nc_maj") : t("brief.ratp_nc_min")}
                          </span>
                        )}
                        <button
                          onClick={() => setShowHistory(true)}
                          className="text-[9px] font-medium text-brand/70 hover:text-brand flex items-center gap-0.5 border border-brand/20 hover:border-brand/50 px-1.5 py-0.5 rounded transition-colors"
                          title={t("brief.see_history")}
                        >
                          <History size={9} />{t("brief.see_history")}
                        </button>
                      </div>
                      <button
                        onClick={() => setRemovedSectionIds(prev => new Set([...prev, section.id]))}
                        className="text-gray-400 hover:text-red-500 hover:bg-red-50 w-5 h-5 flex items-center justify-center rounded text-sm leading-none transition-colors"
                        title={t("brief.delete_section_title")}
                      >×</button>
                    </div>
                    <div className="text-sm font-medium text-ink flex items-center gap-1.5 flex-wrap">
                      {sectionTitle}
                      {hasRecurrence && (
                        <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-orange-100 text-orange-700 flex items-center gap-0.5 shrink-0">
                          🔁 {t("inspection.recurrence_title")}
                        </span>
                      )}
                    </div>
                    {alerte && (
                      <div className="mt-1 text-[10px] flex items-start gap-1" style={{ color: alerte.criticite === "majeure" ? "#ef4444" : "#f59e0b" }}>
                        <AlertTriangle size={10} className="shrink-0 mt-0.5" />
                        <span>{lang === "EN" ? (alerte.messageEN || alerte.message) : alerte.message}</span>
                      </div>
                    )}
                    <div className="text-xs text-ink-muted mt-1 mb-2">
                      {(() => {
                        const n = section.items.filter(i => !removedItemIds.has(i.id)).length + extraItems.length
                        return `${n} ${n > 1 ? t("brief.control_points_plural") : t("brief.control_points_singular")}`
                      })()}
                    </div>
                    <div className="space-y-0.5 mb-1">
                      {section.items.filter(i => !removedItemIds.has(i.id)).map(item => (
                        <div key={item.id} className="flex items-center justify-between gap-2 text-xs text-ink py-0.5">
                          <span className="flex items-center gap-1.5 min-w-0">
                            <span className="text-ink-muted shrink-0">○</span>
                            <span className="truncate">{lang === "EN" && item.texteEN ? item.texteEN : item.texte}</span>
                          </span>
                          <button
                            onClick={() => setRemovedItemIds(prev => new Set([...prev, item.id]))}
                            className="text-gray-400 hover:text-red-500 hover:bg-red-50 w-4 h-4 flex items-center justify-center rounded text-xs leading-none shrink-0 transition-colors"
                          >×</button>
                        </div>
                      ))}
                    </div>
                    {extraItems.length > 0 && (
                      <div className="space-y-0.5 mb-1">
                        {extraItems.map(item => (
                          <div key={item.id} className="flex items-center justify-between gap-2 text-xs px-2 py-1 rounded bg-brand/10">
                            <div className="flex items-center gap-1.5 min-w-0">
                              <span className="truncate text-brand">{item.texte}</span>
                              <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-brand text-white shrink-0">{t("brief.auditor_badge")}</span>
                            </div>
                            <button onClick={() => setExtraItemsBySectionId(prev => ({ ...prev, [section.id]: prev[section.id].filter(i => i.id !== item.id) }))} className="text-ink-muted hover:text-red-500 shrink-0">×</button>
                          </div>
                        ))}
                      </div>
                    )}
                    {genere && (
                      addingItemTo === section.id ? (
                        <div className="mt-2">
                          <input autoFocus type="text" value={newItemTexte} onChange={e => setNewItemTexte(e.target.value)}
                            onKeyDown={e => {
                              if (e.key === "Enter" && newItemTexte.trim()) {
                                setExtraItemsBySectionId(prev => ({ ...prev, [section.id]: [...(prev[section.id] || []), { id: `extra-${Date.now()}`, texte: newItemTexte.trim() }] }))
                                setNewItemTexte(""); setAddingItemTo(null)
                              }
                              if (e.key === "Escape") { setAddingItemTo(null); setNewItemTexte("") }
                            }}
                            placeholder={t("brief.control_point_placeholder")}
                            className="w-full text-xs border border-divider rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand mb-1 bg-surface"
                          />
                          <div className="flex gap-1.5">
                            <button onClick={() => { if (newItemTexte.trim()) { setExtraItemsBySectionId(prev => ({ ...prev, [section.id]: [...(prev[section.id] || []), { id: `extra-${Date.now()}`, texte: newItemTexte.trim() }] })); setNewItemTexte(""); setAddingItemTo(null) } }} disabled={!newItemTexte.trim()} className="text-[10px] font-medium px-2 py-0.5 rounded disabled:opacity-40 text-white bg-brand">{t("brief.add_control_point").replace("+ ", "")}</button>
                            <button onClick={() => { setAddingItemTo(null); setNewItemTexte("") }} className="text-[10px] text-ink-muted hover:text-ink px-1">{t("brief.cancel_title")}</button>
                          </div>
                        </div>
                      ) : (
                        <button onClick={() => { setAddingItemTo(section.id); setAddingSection(false) }} className="mt-2 text-[10px] flex items-center gap-1 text-brand hover:text-brand-cyan transition-colors">
                          {t("brief.add_control_point")}
                        </button>
                      )
                    )}
                  </div>
                )
              })}

              {!genere && sectionsVisibles < CHECKLIST.length && (
                <div className="bg-surface-sunk shadow-inset rounded-lg p-3 opacity-50">
                  <div className="flex items-center gap-2">
                    <svg className="animate-spin h-3 w-3 text-ink-muted" viewBox="0 0 24 24" fill="none">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
                    </svg>
                    <span className="text-xs text-ink-muted">{t("brief.analyzing")}</span>
                  </div>
                </div>
              )}

              {customSections.map((sec) => (
                <div key={sec.id} className="bg-surface-sunk shadow-inset border border-dashed border-brand/30 rounded-lg p-3">
                  <div className="flex items-start justify-between gap-2 mb-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-brand text-white">{t("brief.auditor_badge")}</span>
                      <span className="text-xs font-bold text-brand">{sec.titre}</span>
                    </div>
                    <button onClick={() => setCustomSections(prev => prev.filter(s => s.id !== sec.id))} className="text-ink-muted hover:text-red-500 text-sm leading-none shrink-0">×</button>
                  </div>
                  {sec.items.length > 0 && (
                    <div className="space-y-1 mb-2">
                      {sec.items.map(item => (
                        <div key={item.id} className="flex items-center justify-between gap-2 text-xs text-ink">
                          <span className="flex items-center gap-1"><span className="text-ink-muted">○</span> {item.texte}</span>
                          <button onClick={() => setCustomSections(prev => prev.map(s => s.id === sec.id ? { ...s, items: s.items.filter(i => i.id !== item.id) } : s))} className="text-ink-muted hover:text-red-500 shrink-0">×</button>
                        </div>
                      ))}
                    </div>
                  )}
                  {addingItemTo === sec.id ? (
                    <div>
                      <input autoFocus type="text" value={newItemTexte} onChange={e => setNewItemTexte(e.target.value)}
                        onKeyDown={e => {
                          if (e.key === "Enter" && newItemTexte.trim()) {
                            setCustomSections(prev => prev.map(s => s.id === sec.id ? { ...s, items: [...s.items, { id: `item-${Date.now()}`, texte: newItemTexte.trim() }] } : s))
                            setNewItemTexte(""); setAddingItemTo(null)
                          }
                          if (e.key === "Escape") { setAddingItemTo(null); setNewItemTexte("") }
                        }}
                        placeholder={t("brief.control_point_placeholder")}
                        className="w-full text-xs border border-divider rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand mb-1 bg-surface"
                      />
                      <div className="flex gap-1.5">
                        <button onClick={() => { if (newItemTexte.trim()) { setCustomSections(prev => prev.map(s => s.id === sec.id ? { ...s, items: [...s.items, { id: `item-${Date.now()}`, texte: newItemTexte.trim() }] } : s)); setNewItemTexte(""); setAddingItemTo(null) } }} disabled={!newItemTexte.trim()} className="text-[10px] font-medium px-2 py-0.5 rounded disabled:opacity-40 text-white bg-brand">{t("brief.add_control_point").replace("+ ", "")}</button>
                        <button onClick={() => { setAddingItemTo(null); setNewItemTexte("") }} className="text-[10px] text-ink-muted hover:text-ink px-1">{t("brief.cancel_title")}</button>
                      </div>
                    </div>
                  ) : (
                    <button onClick={() => { setAddingItemTo(sec.id); setAddingSection(false) }} className="text-[10px] flex items-center gap-1 text-brand hover:text-brand-cyan transition-colors">
                      {t("brief.add_control_point")}
                    </button>
                  )}
                </div>
              ))}

              {genere && (
                addingSection ? (
                  <div className="bg-surface-sunk shadow-inset border border-dashed border-brand/30 rounded-lg p-3">
                    <div className="text-[10px] font-semibold text-ink-muted mb-2">{t("brief.new_scope_title")}</div>
                    <input autoFocus type="text" value={newSectionTitre} onChange={e => setNewSectionTitre(e.target.value)}
                      onKeyDown={e => { if (e.key === "Escape") { setAddingSection(false); setNewSectionTitre(""); setNewSectionClause("") } }}
                      placeholder={t("brief.section_title_placeholder")}
                      className="w-full text-sm border border-divider rounded px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand mb-2 bg-surface"
                    />
                    <input type="text" value={newSectionClause} onChange={e => setNewSectionClause(e.target.value)}
                      placeholder={t("brief.clause_placeholder")}
                      className="w-full text-sm border border-divider rounded px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand mb-2 bg-surface"
                    />
                    <div className="flex gap-2">
                      <button
                        onClick={() => {
                          if (newSectionTitre.trim()) {
                            const id = `Sx${customSections.length + 1}`
                            setCustomSections(prev => [...prev, { id, titre: newSectionTitre.trim(), clause: newSectionClause.trim(), items: [] }])
                            setNewSectionTitre(""); setNewSectionClause(""); setAddingSection(false)
                          }
                        }}
                        disabled={!newSectionTitre.trim()}
                        className="text-xs font-medium px-3 py-1 rounded disabled:opacity-40 text-white bg-brand hover:bg-brand-cyan transition-colors"
                      >{t("brief.create_section")}</button>
                      <button onClick={() => { setAddingSection(false); setNewSectionTitre(""); setNewSectionClause("") }} className="text-xs text-ink-muted hover:text-ink px-2 py-1">{t("brief.cancel_title")}</button>
                    </div>
                  </div>
                ) : (
                  <button onClick={() => { setAddingSection(true); setAddingItemTo(null) }}
                    className="w-full border border-dashed border-brand/30 rounded-lg p-2.5 text-xs flex items-center justify-center gap-1.5 text-brand hover:bg-brand/5 transition-colors"
                  >
                    {t("brief.new_scope_button")}
                  </button>
                )
              )}
            </div>
          </div>

          {/* ── Colonne droite — Historique (conditionnelle) ───────────────── */}
          {showHistory && (
            <div className="card col-span-4">
              <div className="flex items-center justify-between mb-3">
                <h2 className="section-label mb-0">
                  <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><History size={11} className="text-brand" /></span>
                  {t("brief.previous_audits_title")}
                </h2>
                <button
                  onClick={() => setShowHistory(false)}
                  className="text-[10px] text-ink-muted hover:text-ink flex items-center gap-1 border border-divider rounded px-2 py-0.5 hover:bg-surface transition-colors"
                >
                  <X size={10} />{t("brief.hide_history")}
                </button>
              </div>
              <div className="space-y-3">
                {historiqueAudits.map((audit, i) => (
                  <div
                    key={i}
                    className={`bg-surface-sunk shadow-inset rounded-lg p-3 ${audit.alerte ? "border-l-4 border-brand-amber" : ""}`}
                  >
                    <div className="flex items-center justify-between mb-1">
                      <span className="font-mono text-xs font-bold text-ink flex items-center gap-1">
                        <Calendar size={11} className="text-ink-muted" />{audit.date}
                      </span>
                      {audit.alerte && (
                        <span className="text-[10px] bg-orange-100 text-orange-800 px-1.5 py-0.5 rounded font-semibold flex items-center gap-0.5">
                          <AlertTriangle size={10} />{t("brief.not_closed")}
                        </span>
                      )}
                    </div>
                    <div className="text-xs text-ink-muted">{t("brief.auditor_label")} {audit.auditeur}</div>
                    <div className="flex gap-2 mt-1 flex-wrap">
                      {audit.nc_majeures > 0 && (
                        <span className="text-[10px] font-bold bg-red-100 text-red-700 px-1.5 py-0.5 rounded">
                          {audit.nc_majeures} Major NC
                        </span>
                      )}
                      {audit.nc_mineures > 0 && (
                        <span className="text-[10px] font-bold bg-amber-100 text-amber-700 px-1.5 py-0.5 rounded">
                          {audit.nc_mineures} Minor NC
                        </span>
                      )}
                    </div>
                    {audit.themes.map((th, j) => (
                      <div key={j} className="text-[10px] text-ink-muted mt-0.5 flex items-center gap-1">
                        <RotateCcw size={9} className="text-divider shrink-0" />{th}
                      </div>
                    ))}
                    {audit.alerte && (
                      <div className="mt-2 text-[10px] text-orange-700 font-bold border-t border-orange-200 pt-2 flex items-start gap-1">
                        <AlertTriangle size={10} className="shrink-0 mt-0.5" />
                        {t("brief.unclosed_alert")}
                      </div>
                    )}
                    {/* Documents historiques avec hover preview + download */}
                    {audit.docs && audit.docs.length > 0 && (
                      <div className="mt-2 pt-2 border-t border-divider space-y-1">
                        {audit.docs.map(doc => (
                          <div key={doc.id} className="relative group/hdoc flex items-center gap-2">
                            <a
                              href={doc.url}
                              download={doc.nom}
                              className="flex-1 min-w-0 flex items-center gap-1.5 text-[10px] text-brand hover:underline"
                            >
                              <FileText size={9} className="shrink-0" />
                              <span className="truncate font-medium">{doc.nom}</span>
                            </a>
                            <span className="text-[9px] text-ink-muted shrink-0">{doc.date_depot}</span>
                            {/* Aperçu au survol */}
                            <div className="absolute bottom-full left-0 mb-1.5 w-64 z-40 opacity-0 group-hover/hdoc:opacity-100 pointer-events-none transition-opacity duration-150">
                              <div className="bg-white border border-divider rounded-xl shadow-lg overflow-hidden">
                                {doc.type === "PDF" ? (
                                  <embed src={doc.url} type="application/pdf" className="w-full h-36" />
                                ) : (
                                  <div className="bg-blue-50 flex items-center justify-center h-16 gap-2">
                                    <span className="text-xl">📄</span>
                                    <span className="text-xs font-semibold text-blue-700">{t("brief.document_word")}</span>
                                  </div>
                                )}
                                <div className="p-2 text-[10px] text-ink-muted">{doc.nom} · {doc.date_depot}</div>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

        </div>
      </div>
    </div>
  )
}
