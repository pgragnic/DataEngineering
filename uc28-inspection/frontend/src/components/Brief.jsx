import { useEffect, useRef, useState } from "react"
import { AUDIT_COURANT, CHECKLIST, AUDITS_PRECEDENTS, SUPPLIER_DOCUMENTS, SUPPLIER_ALERTS } from "../mockData"
import { FileText, History, ClipboardList, Upload, CheckCircle2, AlertTriangle, Calendar } from "lucide-react"

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
  const [selectedRef, setSelectedRef] = useState("ISO 9001:2015")

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
              Check-list générée · {genSec} sec · {CHECKLIST.length} sections · {totalPoints} points · {recurrents} point récurrent
            </span>
          ) : (
            <span>Génération de la check-list…</span>
          )}
        </div>
        <button
          onClick={() => onDemarrer(customSections, extraItemsBySectionId, removedItemIds)}
          disabled={!genere}
          className="disabled:bg-divider disabled:text-ink-muted disabled:cursor-not-allowed text-white font-semibold px-5 py-2 rounded-lg text-sm bg-brand hover:bg-brand-cyan transition-colors shadow-sm"
        >
          Démarrer l'inspection →
        </button>
      </div>

      <div className="page-content">
        <div className="grid-12">

          {/* Colonne gauche — Brief client */}
          <div className="card col-span-4">
            <h2 className="section-label">
              <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><FileText size={11} className="text-brand" /></span>Brief client
            </h2>
            <dl className="space-y-3 text-sm">
              <div>
                <dt className="text-[10px] font-semibold text-ink-muted uppercase">Client</dt>
                <dd className="font-semibold text-ink">{AUDIT_COURANT.nom}</dd>
              </div>
              <div>
                <dt className="text-[10px] font-semibold text-ink-muted uppercase">Localisation</dt>
                <dd className="text-ink">{AUDIT_COURANT.localisation}</dd>
              </div>
              <div>
                <dt className="text-[10px] font-semibold text-ink-muted uppercase">Effectif</dt>
                <dd className="text-ink">{AUDIT_COURANT.effectif} personnes</dd>
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
                <dt className="text-[10px] font-semibold text-ink-muted uppercase">Scope</dt>
                <dd className="text-ink">
                  {AUDIT_COURANT.scope.map((s, i) => (
                    <div key={i} className="flex items-start gap-1">
                      <span className="text-brand-cyan mt-0.5">›</span> {s}
                    </div>
                  ))}
                </dd>
              </div>
              <div>
                <dt className="text-[10px] font-semibold text-ink-muted uppercase">Contact qualité</dt>
                <dd className="text-ink">{AUDIT_COURANT.responsable_qualite}</dd>
                <dd className="text-xs text-brand">{AUDIT_COURANT.contact}</dd>
              </div>
              <div>
                <dt className="text-[10px] font-semibold text-ink-muted uppercase">Durée prévue</dt>
                <dd className="text-ink">{AUDIT_COURANT.duree_prevue} sur site</dd>
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

          {/* Colonne centre — Check-list IA */}
          <div className="card col-span-4">
            <h2 className="section-label">
              <span className="w-5 h-5 rounded bg-brand/15 flex items-center justify-center shrink-0"><ClipboardList size={11} className="text-brand" /></span>Check-list générée par l'Agent IA
            </h2>
            {genere ? (
              <div className="flex items-center gap-2 mb-4 mt-3">
                <span className="w-2 h-2 rounded-full inline-block bg-brand-emerald"></span>
                <span className="text-xs font-medium flex items-center gap-1 text-brand-emerald">
                  <CheckCircle2 size={13} />Préparation terminée — {genSec} sec
                </span>
              </div>
            ) : (
              <div className="flex items-center gap-2 mb-4 mt-3">
                <svg className="animate-spin h-3 w-3 text-brand" viewBox="0 0 24 24" fill="none">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
                </svg>
                <span className="text-xs text-brand">Génération en cours…</span>
              </div>
            )}

            <div className="space-y-3">
              {/* Sections IA */}
              {CHECKLIST.slice(0, sectionsVisibles).map((section) => {
                const extraItems = extraItemsBySectionId[section.id] || []
                return (
                  <div key={section.id} className="bg-surface-sunk shadow-inset rounded-lg p-3 animate-fade-in">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-xs font-bold text-brand">{section.id}</span>
                      <div className="flex items-center gap-1.5">
                        {SUPPLIER_ALERTS[section.clause] && (
                          <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-full ${SUPPLIER_ALERTS[section.clause].criticite === "majeure" ? "bg-red-100 text-red-600" : "bg-orange-100 text-orange-600"}`}>
                            ⚠ RATP
                          </span>
                        )}
                        <span className="text-[10px] text-ink-muted font-mono">{section.clause}</span>
                      </div>
                    </div>
                    <div className="text-sm font-medium text-ink">{section.titre}</div>
                    <div className="text-xs text-ink-muted mt-1 mb-2">
                      {section.items.filter(i => !removedItemIds.has(i.id)).length + extraItems.length} point{(section.items.filter(i => !removedItemIds.has(i.id)).length + extraItems.length) > 1 ? "s" : ""}
                      {section.id === "S1" && (
                        <span className="ml-2 font-medium text-brand-amber">· 1 récurrent</span>
                      )}
                    </div>
                    {/* Items de base */}
                    <div className="space-y-0.5 mb-1">
                      {section.items.filter(i => !removedItemIds.has(i.id)).map(item => (
                        <div key={item.id} className="flex items-center justify-between gap-2 text-xs text-ink group py-0.5">
                          <span className="flex items-center gap-1.5 min-w-0">
                            <span className="text-ink-muted shrink-0">○</span>
                            <span className="truncate">{item.texte}</span>
                          </span>
                          <button
                            onClick={() => setRemovedItemIds(prev => new Set([...prev, item.id]))}
                            className="text-divider hover:text-red-500 shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
                          >×</button>
                        </div>
                      ))}
                    </div>
                    {/* Points ajoutés */}
                    {extraItems.length > 0 && (
                      <div className="space-y-0.5 mb-1">
                        {extraItems.map(item => (
                          <div key={item.id} className="flex items-center justify-between gap-2 text-xs px-2 py-1 rounded bg-brand/10 text-brand">
                            <span className="truncate">+ {item.texte}</span>
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
                            placeholder="Point à vérifier…"
                            className="w-full text-xs border border-divider rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand mb-1 bg-surface"
                          />
                          <div className="flex gap-1.5">
                            <button onClick={() => { if (newItemTexte.trim()) { setExtraItemsBySectionId(prev => ({ ...prev, [section.id]: [...(prev[section.id] || []), { id: `extra-${Date.now()}`, texte: newItemTexte.trim() }] })); setNewItemTexte(""); setAddingItemTo(null) } }} disabled={!newItemTexte.trim()} className="text-[10px] font-medium px-2 py-0.5 rounded disabled:opacity-40 text-white bg-brand">Ajouter</button>
                            <button onClick={() => { setAddingItemTo(null); setNewItemTexte("") }} className="text-[10px] text-ink-muted hover:text-ink px-1">Annuler</button>
                          </div>
                        </div>
                      ) : (
                        <button onClick={() => { setAddingItemTo(section.id); setAddingSection(false) }} className="mt-2 text-[10px] flex items-center gap-1 text-brand hover:text-brand-cyan transition-colors">
                          + point
                        </button>
                      )
                    )}
                  </div>
                )
              })}

              {/* Spinner génération */}
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

              {/* Sections personnalisées */}
              {customSections.map((sec) => (
                <div key={sec.id} className="bg-surface-sunk shadow-inset border border-dashed border-brand/30 rounded-lg p-3">
                  <div className="flex items-start justify-between gap-2 mb-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded bg-brand text-white">Auditeur</span>
                      <span className="text-xs font-bold text-brand">{sec.titre}</span>
                      {sec.clause && <span className="text-[10px] text-ink-muted font-mono">{sec.clause}</span>}
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
                        placeholder="Point à vérifier…"
                        className="w-full text-xs border border-divider rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand mb-1 bg-surface"
                      />
                      <div className="flex gap-1.5">
                        <button onClick={() => { if (newItemTexte.trim()) { setCustomSections(prev => prev.map(s => s.id === sec.id ? { ...s, items: [...s.items, { id: `item-${Date.now()}`, texte: newItemTexte.trim() }] } : s)); setNewItemTexte(""); setAddingItemTo(null) } }} disabled={!newItemTexte.trim()} className="text-[10px] font-medium px-2 py-0.5 rounded disabled:opacity-40 text-white bg-brand">Ajouter</button>
                        <button onClick={() => { setAddingItemTo(null); setNewItemTexte("") }} className="text-[10px] text-ink-muted hover:text-ink px-1">Annuler</button>
                      </div>
                    </div>
                  ) : (
                    <button onClick={() => { setAddingItemTo(sec.id); setAddingSection(false) }} className="text-[10px] flex items-center gap-1 text-brand hover:text-brand-cyan transition-colors">
                      + point
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

          {/* Colonne droite — Audits précédents */}
          <div className="card col-span-4">
            <h2 className="section-label">
              <History size={11} />Audits précédents
            </h2>
            <div className="space-y-3">
              {AUDITS_PRECEDENTS.map((audit, i) => (
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
                      <span className="text-[10px] bg-red-100 text-red-700 px-1.5 py-0.5 rounded">
                        {audit.nc_majeures} NC maj.
                      </span>
                    )}
                    <span className="text-[10px] bg-orange-100 text-orange-700 px-1.5 py-0.5 rounded">
                      {audit.nc_mineures} NC min.
                    </span>
                  </div>
                  {audit.themes.map((t, j) => (
                    <div key={j} className="text-[10px] text-ink-muted mt-0.5 flex items-center gap-1">
                      <span className="text-divider">›</span>{t}
                    </div>
                  ))}
                  {audit.alerte && (
                    <div className="mt-2 text-[10px] text-orange-700 font-medium border-t border-orange-200 pt-2 flex items-start gap-1">
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
