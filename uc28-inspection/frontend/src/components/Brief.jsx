import { useEffect, useState } from "react"
import { AUDIT_COURANT, CHECKLIST, AUDITS_PRECEDENTS, SUPPLIER_DOCUMENTS, SUPPLIER_ALERTS, SCOPE_OPTIONS } from "../mockData"
import { getSite } from "../api"
import { FileText, History, ClipboardList, Upload, CheckCircle2, AlertTriangle, Calendar, Pencil, Check, X, Info } from "lucide-react"

const inputCls = "w-full text-xs border border-divider rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-brand bg-surface"

export default function Brief({ onDemarrer, onBack, theme }) {
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
  const [selectedRef, setSelectedRef] = useState("ISO 9001:2015")
  const [siteData, setSiteData] = useState(null)

  // ── Brief client inline edit ───────────────────────────────────────────────
  const [editingSection, setEditingSection] = useState(null) // 'nom' | 'localisation' | 'effectif' | 'scope' | 'contact' | 'duree'
  const [snapshot, setSnapshot] = useState(null)

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
  const recurrents = 1

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

  // ── Section header helper ──────────────────────────────────────────────────
  const SectionHeader = ({ label, id }) => (
    <dt className="text-[10px] font-semibold text-ink-muted uppercase flex items-center justify-between">
      <span>{label}</span>
      {editingSection === id ? (
        <span className="flex gap-1">
          <button onClick={saveSection} className="p-0.5 rounded hover:bg-brand/10 text-brand-emerald" title="Enregistrer"><Check size={10} /></button>
          <button onClick={cancelSection} className="p-0.5 rounded hover:bg-red-50 text-ink-muted" title="Annuler"><X size={10} /></button>
        </span>
      ) : (
        <button onClick={() => startEdit(id)} className="p-0.5 rounded hover:bg-brand/10 text-ink-muted hover:text-brand" title="Modifier">
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
          ← Retour au dashboard
        </button>
        <div className="text-xs text-ink-muted">
          {genere ? (
            <span className="font-medium text-brand-emerald">
              Check-list : {CHECKLIST.length} sections · {totalPoints} points de contrôle · {recurrents} point de contrôle récurrent
            </span>
          ) : (
            <span>Génération de la check-list…</span>
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
          Démarrer mon inspection →
        </button>
      </div>

      <div className="page-content">
        <div className="grid-12">

          {/* ── Colonne gauche — Brief client ─────────────────────────────── */}
          <div className="card col-span-4">
            <h2 className="section-label">
              <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><FileText size={11} className="text-brand" /></span>Brief client
            </h2>
            <dl className="space-y-3 text-sm">

              <div>
                <SectionHeader label="Client" id="nom" />
                <dd className="mt-0.5">
                  {editingSection === "nom"
                    ? <input autoFocus value={editedBriefData.nom} onChange={e => setEditedBriefData(p => ({ ...p, nom: e.target.value }))} className={inputCls} />
                    : <span className="font-semibold text-ink">{editedBriefData.nom}</span>
                  }
                </dd>
              </div>

              <div>
                <SectionHeader label="Localisation" id="localisation" />
                <dd className="mt-0.5">
                  {editingSection === "localisation"
                    ? <input autoFocus value={editedBriefData.localisation} onChange={e => setEditedBriefData(p => ({ ...p, localisation: e.target.value }))} className={inputCls} />
                    : <span className="text-ink">{editedBriefData.localisation}</span>
                  }
                </dd>
              </div>

              <div>
                <SectionHeader label="Effectif" id="effectif" />
                <dd className="mt-0.5">
                  {editingSection === "effectif"
                    ? (
                      <span className="flex items-center gap-1">
                        <input autoFocus type="number" value={editedBriefData.effectif} onChange={e => setEditedBriefData(p => ({ ...p, effectif: Number(e.target.value) }))} className="text-xs border border-divider rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-brand bg-surface w-20 text-center" />
                        <span className="text-xs text-ink-muted">personnes</span>
                      </span>
                    )
                    : <span className="text-ink">{editedBriefData.effectif} personnes</span>
                  }
                </dd>
              </div>

              <div>
                <dt className="text-[10px] font-semibold text-ink-muted uppercase mb-1.5">Référentiel</dt>
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
                    ✓ Checklist adaptée au référentiel {selectedRef}
                  </dd>
                )}
              </div>

              <div>
                <SectionHeader label="Scope" id="scope" />
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
                      <div key={i} className="text-xs text-ink py-0.5">{s}</div>
                    )
                  ))}
                  {editingSection === "scope" && (
                    <button onClick={() => setEditedBriefData(p => ({ ...p, scope: [...p.scope, ""] }))} className="text-[10px] text-brand hover:text-brand-cyan mt-0.5">+ Ajouter texte libre</button>
                  )}
                  {!addingScopeItem ? (
                    <button onClick={() => setAddingScopeItem(true)} className="mt-1 text-[10px] flex items-center gap-1 text-brand hover:text-brand-cyan transition-colors">
                      + Ajouter un domaine
                    </button>
                  ) : (
                    <div className="mt-2 bg-surface border border-divider rounded-lg p-2 space-y-0.5">
                      {SCOPE_OPTIONS.filter(o => !editedBriefData.scope.includes(o.label)).map(o => (
                        <button
                          key={o.id}
                          onClick={() => { setEditedBriefData(p => ({ ...p, scope: [...p.scope, o.label] })); setAddingScopeItem(false) }}
                          className="w-full text-left text-xs px-2 py-1 rounded hover:bg-brand/5 hover:text-brand transition-colors"
                        >
                          {o.label}
                        </button>
                      ))}
                      {SCOPE_OPTIONS.every(o => editedBriefData.scope.includes(o.label)) && (
                        <div className="text-[10px] text-ink-muted px-2 py-1">Tous les domaines sont déjà sélectionnés</div>
                      )}
                      <button onClick={() => setAddingScopeItem(false)} className="text-[10px] text-ink-muted hover:text-ink px-2 pt-1">Annuler</button>
                    </div>
                  )}
                </dd>
              </div>

              <div>
                <SectionHeader label="Contact qualité" id="contact" />
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

              <div>
                <SectionHeader label="Durée prévue" id="duree" />
                <dd className="mt-0.5">
                  {editingSection === "duree"
                    ? (
                      <span className="flex items-center gap-1">
                        <input autoFocus value={editedBriefData.duree_prevue} onChange={e => setEditedBriefData(p => ({ ...p, duree_prevue: e.target.value }))} className="text-xs border border-divider rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-brand bg-surface w-20" />
                        <span className="text-xs text-ink-muted">sur site</span>
                      </span>
                    )
                    : <span className="text-ink">{editedBriefData.duree_prevue} sur site</span>
                  }
                </dd>
              </div>

            </dl>

            {/* Documents portail RATP */}
            <div className="mt-4 pt-4 border-t border-divider">
              <div className="flex items-center justify-between mb-2">
                <div className="text-[10px] font-semibold text-ink-muted uppercase flex items-center gap-1"><Upload size={10} />Documents portail RATP</div>
                <span className="text-[10px] font-bold px-1.5 py-0.5 rounded-full bg-brand-emerald/10 text-brand-emerald">
                  {SUPPLIER_DOCUMENTS.length} analysés
                </span>
              </div>
              <div className="space-y-1.5">
                {SUPPLIER_DOCUMENTS.map(doc => (
                  <div key={doc.id} className="flex items-start gap-2 bg-surface-sunk shadow-inset rounded-lg px-2.5 py-2">
                    <span className="text-xs leading-none mt-0.5 shrink-0 text-brand-emerald">✓</span>
                    <div className="min-w-0">
                      <div className="text-[10px] font-medium text-ink truncate">{doc.nom}</div>
                      <div className="text-[10px] text-ink-muted">{doc.date} · {doc.insights?.sections_a_risque?.[0] ?? "Analysé"}</div>
                    </div>
                  </div>
                ))}
              </div>
              <div className="mt-2 text-[10px] text-center text-brand">
                Déposés par Mei Lin Zhang · Portail RATP
              </div>
            </div>
          </div>

          {/* ── Colonne centre — Check-list IA ────────────────────────────── */}
          <div className="card col-span-4">
            <div className="relative">
              <h2 className="section-label">
                <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><ClipboardList size={11} className="text-brand" /></span>
                Check-list auto-générée
                <button onClick={() => setShowChecklistInfo(v => !v)} className="ml-1 text-brand/50 hover:text-brand transition-colors" title="Comment est générée cette liste ?">
                  <Info size={12} />
                </button>
              </h2>
              {showChecklistInfo && (
                <div className="absolute top-7 left-0 right-0 z-30 bg-white border border-brand/20 rounded-xl shadow-lg p-3 text-xs text-ink-muted space-y-1.5">
                  <div className="flex items-center justify-between mb-1">
                    <span className="font-semibold text-brand text-[10px] uppercase tracking-wide">Comment est générée cette liste ?</span>
                    <button onClick={() => setShowChecklistInfo(false)} className="text-ink-muted hover:text-ink"><X size={12} /></button>
                  </div>
                  <p>L'Agent IA combine <strong>5 sources</strong> pour construire la checklist :</p>
                  <ul className="space-y-1 pl-2">
                    <li><span className="font-medium text-ink">1. Référentiel</span> — ISO 9001:2015 (sélectionnable ci-dessous)</li>
                    <li><span className="font-medium text-ink">2. Scope de la mission</span> — §7.1.5 Métrologie · §8.7 NC → S2 et S3 ciblées</li>
                    <li><span className="font-medium text-ink">3. Contexte du site</span> — atelier maintenance, 218 personnes, 2h30</li>
                    <li><span className="font-medium text-ink">4. Historique NC</span> — non-conformité §7.1.5 ouverte depuis nov. 2024</li>
                    <li><span className="font-medium text-ink">5. Documents fournisseur</span> — 3 docs Mei Lin Zhang pré-analysés → alertes ⚠</li>
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
                <span className="text-xs text-brand">Génération en cours…</span>
              </div>
            )}

            <div className="space-y-3">
              {CHECKLIST.slice(0, sectionsVisibles).filter(s => !removedSectionIds.has(s.id)).map((section) => {
                const extraItems = extraItemsBySectionId[section.id] || []
                const alerte = SUPPLIER_ALERTS[section.clause] || null
                const inScope = scopeActiveSections.has(section.id)
                return (
                  <div key={section.id} className={`bg-surface-sunk shadow-inset rounded-lg p-3 animate-fade-in transition-opacity ${alerte ? "border-l-4 border-brand-amber" : inScope ? "border-l-4 border-brand" : ""} ${!inScope && scopeActiveSections.size > 0 ? "opacity-50" : ""}`}>
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-1.5 min-w-0">
                        <span className="text-xs font-bold text-brand shrink-0">{section.id}</span>
                        {inScope && (
                          <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-brand/15 text-brand shrink-0">Scope</span>
                        )}
                        {alerte && (
                          <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded shrink-0 ${alerte.criticite === "majeure" ? "bg-nc-majeure text-white" : "bg-nc-mineure text-white"}`}>
                            ⚠ RATP {alerte.criticite === "majeure" ? "NC maj." : "NC min."}
                          </span>
                        )}
                      </div>
                      <button
                        onClick={() => setRemovedSectionIds(prev => new Set([...prev, section.id]))}
                        className="text-gray-400 hover:text-red-500 hover:bg-red-50 w-5 h-5 flex items-center justify-center rounded text-sm leading-none transition-colors"
                        title="Supprimer cette section"
                      >×</button>
                    </div>
                    <div className="text-sm font-medium text-ink">{section.titre}</div>
                    {alerte && (
                      <div className="mt-1 text-[10px] text-brand-amber flex items-start gap-1">
                        <span className="shrink-0">⚠</span>
                        <span>{alerte.message}</span>
                      </div>
                    )}
                    <div className="text-xs text-ink-muted mt-1 mb-2">
                      {section.items.filter(i => !removedItemIds.has(i.id)).length + extraItems.length} point{(section.items.filter(i => !removedItemIds.has(i.id)).length + extraItems.length) > 1 ? "s" : ""} de contrôle
                    </div>
                    <div className="space-y-0.5 mb-1">
                      {section.items.filter(i => !removedItemIds.has(i.id)).map(item => (
                        <div key={item.id} className="flex items-center justify-between gap-2 text-xs text-ink py-0.5">
                          <span className="flex items-center gap-1.5 min-w-0">
                            <span className="text-ink-muted shrink-0">○</span>
                            <span className="truncate">{item.texte}</span>
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
                              <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-brand text-white shrink-0">Auditeur</span>
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
                            placeholder="Point de contrôle à vérifier…"
                            className="w-full text-xs border border-divider rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand mb-1 bg-surface"
                          />
                          <div className="flex gap-1.5">
                            <button onClick={() => { if (newItemTexte.trim()) { setExtraItemsBySectionId(prev => ({ ...prev, [section.id]: [...(prev[section.id] || []), { id: `extra-${Date.now()}`, texte: newItemTexte.trim() }] })); setNewItemTexte(""); setAddingItemTo(null) } }} disabled={!newItemTexte.trim()} className="text-[10px] font-medium px-2 py-0.5 rounded disabled:opacity-40 text-white bg-brand">Ajouter</button>
                            <button onClick={() => { setAddingItemTo(null); setNewItemTexte("") }} className="text-[10px] text-ink-muted hover:text-ink px-1">Annuler</button>
                          </div>
                        </div>
                      ) : (
                        <button onClick={() => { setAddingItemTo(section.id); setAddingSection(false) }} className="mt-2 text-[10px] flex items-center gap-1 text-brand hover:text-brand-cyan transition-colors">
                          + point de contrôle
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
                    <span className="text-xs text-ink-muted">Analyse en cours…</span>
                  </div>
                </div>
              )}

              {customSections.map((sec) => (
                <div key={sec.id} className="bg-surface-sunk shadow-inset border border-dashed border-brand/30 rounded-lg p-3">
                  <div className="flex items-start justify-between gap-2 mb-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-brand text-white">Auditeur</span>
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
                        placeholder="Point de contrôle à vérifier…"
                        className="w-full text-xs border border-divider rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand mb-1 bg-surface"
                      />
                      <div className="flex gap-1.5">
                        <button onClick={() => { if (newItemTexte.trim()) { setCustomSections(prev => prev.map(s => s.id === sec.id ? { ...s, items: [...s.items, { id: `item-${Date.now()}`, texte: newItemTexte.trim() }] } : s)); setNewItemTexte(""); setAddingItemTo(null) } }} disabled={!newItemTexte.trim()} className="text-[10px] font-medium px-2 py-0.5 rounded disabled:opacity-40 text-white bg-brand">Ajouter</button>
                        <button onClick={() => { setAddingItemTo(null); setNewItemTexte("") }} className="text-[10px] text-ink-muted hover:text-ink px-1">Annuler</button>
                      </div>
                    </div>
                  ) : (
                    <button onClick={() => { setAddingItemTo(sec.id); setAddingSection(false) }} className="text-[10px] flex items-center gap-1 text-brand hover:text-brand-cyan transition-colors">
                      + point de contrôle
                    </button>
                  )}
                </div>
              ))}

              {genere && (
                addingSection ? (
                  <div className="bg-surface-sunk shadow-inset border border-dashed border-brand/30 rounded-lg p-3">
                    <div className="text-[10px] font-semibold text-ink-muted mb-2">Nouvelle section</div>
                    <input autoFocus type="text" value={newSectionTitre} onChange={e => setNewSectionTitre(e.target.value)}
                      onKeyDown={e => { if (e.key === "Escape") { setAddingSection(false); setNewSectionTitre(""); setNewSectionClause("") } }}
                      placeholder="Titre de la section *"
                      className="w-full text-sm border border-divider rounded px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand mb-2 bg-surface"
                    />
                    <input type="text" value={newSectionClause} onChange={e => setNewSectionClause(e.target.value)}
                      placeholder="Clause ISO (optionnel, ex : §8.1)"
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
                      >Créer la section</button>
                      <button onClick={() => { setAddingSection(false); setNewSectionTitre(""); setNewSectionClause("") }} className="text-xs text-ink-muted hover:text-ink px-2 py-1">Annuler</button>
                    </div>
                  </div>
                ) : (
                  <button onClick={() => { setAddingSection(true); setAddingItemTo(null) }}
                    className="w-full border border-dashed border-brand/30 rounded-lg p-2.5 text-xs flex items-center justify-center gap-1.5 text-brand hover:bg-brand/5 transition-colors"
                  >
                    + Nouvelle section
                  </button>
                )
              )}
            </div>
          </div>

          {/* ── Colonne droite — Audits précédents ───────────────────────── */}
          <div className="card col-span-4">
            <h2 className="section-label">
              <History size={11} />Audits précédents
            </h2>
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
                        <AlertTriangle size={10} />Non clôturée
                      </span>
                    )}
                  </div>
                  <div className="text-xs text-ink-muted">Auditeur : {audit.auditeur}</div>
                  <div className="flex gap-2 mt-1">
                    {audit.nc_majeures > 0 && (
                      <span className="text-[10px] font-bold bg-red-100 text-red-700 px-1.5 py-0.5 rounded">
                        {audit.nc_majeures} NC maj.
                      </span>
                    )}
                    <span className="text-[10px] font-bold bg-orange-100 text-orange-700 px-1.5 py-0.5 rounded">
                      {audit.nc_mineures} NC min.
                    </span>
                  </div>
                  {audit.themes.map((t, j) => (
                    <div key={j} className="text-[10px] text-ink-muted mt-0.5 flex items-center gap-1">
                      <span className="text-divider">›</span>{t}
                    </div>
                  ))}
                  {audit.alerte && (
                    <div className="mt-2 text-[10px] text-orange-700 font-bold border-t border-orange-200 pt-2 flex items-start gap-1">
                      <AlertTriangle size={10} className="shrink-0 mt-0.5" />
                      NC mineure de 2024 non clôturée — à vérifier en priorité
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>

        </div>
      </div>
    </div>
  )
}
