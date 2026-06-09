import { useRef, useState, useEffect } from "react"
import { SUPPLIER_DOCUMENTS, SUPPLIER_DOC_CONTENT } from "../mockData"
import { analyserDocumentFournisseur } from "../api"
import { Upload, FileText, AlertOctagon, Calendar, Building2, ClipboardList, BookOpen, ListChecks, Wrench, ShieldAlert, ArrowUpDown, ChevronDown, Info } from "lucide-react"

const TYPES_DOCUMENT = [
  { groupe: "Audit & Inspection", items: ["Rapport d'audit BV", "Rapport d'audit", "Compte-rendu d'inspection", "Non-conformité", "Incident / anomalie", "Observation terrain", "Plan d'actions correctives", "Suivi des actions"] },
  { groupe: "Normes & Exigences", items: ["Norme ISO", "Réglementation nationale", "Directive / réglementation européenne", "Référentiel de certification", "Exigence légale", "Obligation contractuelle"] },
  { groupe: "Procédures & Règles internes", items: ["Procédure interne", "Processus métier", "Mode opératoire", "Règles d'exploitation", "Instruction de travail", "Bonnes pratiques", "Guide interne"] },
  { groupe: "Technique & Équipements", items: ["Documentation technique", "Fiche équipement", "Plan / schéma", "Notice constructeur", "Maintenance (préventive / corrective)", "Spécifications techniques"] },
  { groupe: "Sécurité & Risques", items: ["Analyse de risques", "Plan de prévention", "Consignes de sécurité", "Document unique (DUERP)", "Gestion des incidents", "Plan d'urgence / crise"] },
  { groupe: "Autres", items: ["Autres"] },
]

const CATEGORIE_META = {
  "Audit & Inspection":           { Icon: ClipboardList, iconBg: "bg-orange-100", text: "text-orange-600", dot: "bg-orange-500", pill: "bg-orange-50 text-orange-700 border-orange-200", tooltip: "ce qui a été constaté" },
  "Normes & Exigences":           { Icon: BookOpen,      iconBg: "bg-brand/10",   text: "text-brand",     dot: "bg-brand",      pill: "bg-brand/10 text-brand border-brand/20",           tooltip: "ce qui est imposé" },
  "Procédures & Règles internes": { Icon: ListChecks,    iconBg: "bg-teal-100",   text: "text-teal-600",  dot: "bg-teal-500",   pill: "bg-teal-50 text-teal-700 border-teal-200",         tooltip: "comment on travaille" },
  "Technique & Équipements":      { Icon: Wrench,        iconBg: "bg-slate-100",  text: "text-slate-600", dot: "bg-slate-500",  pill: "bg-slate-100 text-slate-600 border-slate-200",     tooltip: "avec quoi on travaille" },
  "Sécurité & Risques":           { Icon: ShieldAlert,   iconBg: "bg-amber-100",  text: "text-amber-600", dot: "bg-amber-500",  pill: "bg-amber-50 text-amber-700 border-amber-200",      tooltip: "les risques et protections" },
  "Autres":                       { Icon: FileText,      iconBg: "bg-canvas",     text: "text-ink-muted", dot: "bg-ink-muted",  pill: "bg-canvas text-ink-muted border-divider",          tooltip: null },
}

function getGroupe(typeDoc) {
  if (!typeDoc) return null
  return TYPES_DOCUMENT.find(g => g.items.includes(typeDoc))?.groupe ?? "Autres"
}

function DocStatusBadge({ statut, ag, ar }) {
  if (statut === "analysé")
    return <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${ag ? "bg-[#4CDFB2]/20 text-[#0a6e68]" : ar ? "bg-[#22C55E]/15 text-[#166534]" : "bg-green-100 text-green-700"}`}>✓ Analysé</span>
  if (statut === "analyse")
    return <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-blue-50 text-blue-600 flex items-center gap-1">
      <svg className="animate-spin h-2.5 w-2.5" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" /></svg>
      En cours…
    </span>
  return <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-red-50 text-red-600">Erreur</span>
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

export default function SupplierPortal({ theme, onBack, externalDocs = [] }) {
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

    // Lire le fichier réel en base64 pour l'envoyer au backend
    const contenu = await new Promise((resolve) => {
      const reader = new FileReader()
      reader.onload = (ev) => {
        const dataUrl = ev.target.result
        // dataUrl = "data:<mime>;base64,<b64>" → on extrait la partie base64
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
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-3">Fournisseur</div>
            <div className="flex items-center gap-3 mb-3">
              <img src="/mei-lin-zhang.png" alt="Mei Lin Zhang" className="w-10 h-10 rounded-full object-cover shrink-0" />
              <div>
                <div className="font-semibold text-sm text-gray-900">Mei Lin Zhang</div>
                <div className="text-xs text-gray-500">Responsable Qualité</div>
              </div>
            </div>
            <dl className="space-y-2 text-xs">
              <div><dt className="text-[10px] text-gray-400 uppercase font-semibold">Société</dt><dd className="text-gray-700 font-medium">RATP — Atelier Sucy-en-Brie</dd></div>
              <div><dt className="text-[10px] text-gray-400 uppercase font-semibold">Effectif</dt><dd className="text-gray-700">218 personnes</dd></div>
              <div><dt className="text-[10px] text-gray-400 uppercase font-semibold">Référentiel</dt><dd className={`font-medium ${accentCls}`}>ISO 9001:2015</dd></div>
              <div><dt className="text-[10px] text-gray-400 uppercase font-semibold">Contact</dt><dd className={`text-xs ${accentCls}`}>meilin.zhang@ratp.fr</dd></div>
            </dl>
          </div>

          <div className={`rounded-xl border shadow-sm p-4 ${ag ? "bg-[#494949] border-[#5D93C1]/30" : ar ? "bg-[#1C2B4A] border-[#4B87F8]/30" : "bg-blue-700 border-blue-600"}`}>
            <div className="text-[10px] font-bold text-white/60 uppercase tracking-widest mb-2">Prochain audit BV</div>
            <div className="text-white font-bold text-sm mb-0.5">Marc Lefèvre</div>
            <div className="text-white/70 text-xs mb-2">Bureau Veritas — ISO 9001</div>
            <div className="flex items-center gap-2 text-xs text-white/80">
              <Calendar size={13} /><span>Mercredi 14h30</span>
            </div>
            <div className="flex items-center gap-2 text-xs text-white/80 mt-1">
              <span>⏱</span><span>Durée prévue : 2h30</span>
            </div>
            <div className={`mt-3 text-[10px] font-semibold px-2 py-1 rounded text-center ${ag ? "bg-[#4CDFB2]/20 text-[#4CDFB2]" : ar ? "bg-[#22C55E]/20 text-[#22C55E]" : "bg-white/10 text-white"}`}>
              {docs.filter(d => d.statut === "analysé").length} document(s) transmis à l'auditeur
            </div>
          </div>

          {/* Alertes IA */}
          <div className="card">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-3 flex items-center gap-1.5"><AlertOctagon size={11} />Alertes remontées par l'IA</div>
            <div className="space-y-2">
              <div className="flex items-start gap-2 p-2 bg-red-50 border border-red-200 rounded-lg">
                <span className="text-red-500 shrink-0 text-sm leading-tight">🔴</span>
                <div>
                  <div className="text-[10px] font-bold text-red-700">§7.1.5 — NC MAJEURE</div>
                  <div className="text-[10px] text-red-600">3 clés dyno périmées — non clôturée nov. 2024</div>
                </div>
              </div>
              <div className="flex items-start gap-2 p-2 bg-orange-50 border border-orange-200 rounded-lg">
                <span className="text-orange-500 shrink-0 text-sm leading-tight">🟡</span>
                <div>
                  <div className="text-[10px] font-bold text-orange-700">§8.7 — NC MINEURE</div>
                  <div className="text-[10px] text-orange-600">Zone quarantaine partiellement délimitée</div>
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
                <h2 className="font-semibold text-gray-900 text-sm">Documents déposés</h2>
                <p className="text-[11px] text-gray-400 mt-0.5">Analysés automatiquement par BV·Inspect · transmis à Marc Lefèvre avant l'audit</p>
              </div>
              <div className="flex items-end gap-2">
                <span className="text-xs text-gray-400 self-center">{docs.filter(d => d.statut === "analysé").length}/{docs.length} analysé(s)</span>
                <div className="flex flex-col gap-1.5">
                  <div ref={dropdownRef} className="relative min-w-[220px]">
                    <button
                      type="button"
                      onClick={() => setOpen(v => !v)}
                      className="w-full text-xs border border-gray-200 rounded-lg px-2 py-1.5 bg-white focus:outline-none focus:ring-1 focus:ring-brand text-left flex items-center justify-between gap-1"
                    >
                      <span className={typeDoc ? "text-gray-700" : "text-gray-400"}>
                        {typeDoc || "— Nature du document —"}
                      </span>
                      <ChevronDown size={12} className={`text-gray-400 transition-transform ${open ? "rotate-180" : ""}`} />
                    </button>
                    {open && (
                      <div className="absolute top-full left-0 mt-1 w-full bg-white border border-gray-200 rounded-lg shadow-md z-30 overflow-visible">
                        {TYPES_DOCUMENT.map(g => {
                          const meta = CATEGORIE_META[g.groupe]
                          return (
                            <div key={g.groupe}>
                              <div className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-50 border-b border-gray-100">
                                <span className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">{g.groupe}</span>
                                {meta.tooltip && (
                                  <div className="relative group/tip">
                                    <Info size={11} className="text-brand cursor-default" />
                                    <span className="pointer-events-none absolute left-1/2 -translate-x-1/2 top-full mt-1 px-2 py-0.5 rounded bg-gray-800 text-white text-[9px] whitespace-nowrap opacity-0 group-hover/tip:opacity-100 transition-opacity z-40">
                                      {meta.tooltip}
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
                      placeholder="Préciser la nature…"
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
                  <Upload size={13} className="inline mr-1" />Déposer
                </button>
              </div>
            </div>

            {/* Barre de filtres */}
            <div className="px-5 py-2.5 border-b border-gray-100 flex items-center gap-2 flex-wrap bg-canvas">
              <button
                onClick={() => setFiltreCategorie("Tous")}
                className={`text-[10px] font-semibold px-2.5 py-1 rounded-full border transition-colors ${filtreCategorie === "Tous" ? "bg-dark-teal text-white border-dark-teal" : "border-divider text-ink-muted hover:bg-surface"}`}
              >
                Tous ({docs.length})
              </button>
              {TYPES_DOCUMENT.map(g => {
                const count = docs.filter(d => getGroupe(d.typeDoc) === g.groupe).length
                if (count === 0) return null
                const meta = CATEGORIE_META[g.groupe]
                const label = g.groupe.includes(" & ") ? g.groupe.split(" & ")[0] : g.groupe
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
                    {meta.tooltip && (
                      <span className="pointer-events-none absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 px-2 py-0.5 rounded bg-gray-800 text-white text-[9px] whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity z-20">
                        {meta.tooltip}
                      </span>
                    )}
                  </div>
                )
              })}
              <div className="ml-auto flex items-center gap-1.5">
                <span className="text-[10px] text-ink-muted">Trier :</span>
                <button
                  onClick={() => setTriDate(v => v === "desc" ? "asc" : "desc")}
                  className="text-[10px] font-medium px-2 py-1 rounded-full border border-divider text-ink-muted hover:bg-surface flex items-center gap-1"
                >
                  <ArrowUpDown size={9} />{triDate === "desc" ? "Récent" : "Ancien"}
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
                        <DocStatusBadge statut={doc.statut} ag={ag} ar={ar} />
                        {doc.fromBV && (
                          <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-brand text-white shrink-0">Bureau Veritas</span>
                        )}
                      </div>
                      <div className="flex items-center gap-2 mt-0.5">
                        <span className="text-[11px] text-gray-400">Déposé le {doc.date}</span>
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
                        {expandedDoc === doc.id ? "Masquer ▲" : "Voir analyse ▼"}
                      </button>
                    )}
                    {(doc.dataUrl || doc.url) && (
                      <button
                        onClick={() => { const a = document.createElement("a"); a.href = doc.dataUrl ?? doc.url; a.download = doc.nom; a.click() }}
                        className="text-xs shrink-0 text-brand-emerald hover:underline flex items-center gap-1"
                      >⬇ Télécharger</button>
                    )}
                    {!doc.mock && !doc.fromBV && (
                      <button
                        onClick={() => setDocs(prev => prev.filter(d => d.id !== doc.id))}
                        className="shrink-0 text-gray-300 hover:text-red-400 transition-colors text-base leading-none"
                        title="Supprimer"
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
                          <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-1">Points à vérifier sur site</div>
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
                    ? "Aucun document déposé — sélectionnez la nature puis cliquez sur « Déposer »."
                    : "Aucun document dans cette catégorie."}
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
