import { useEffect, useRef, useState } from "react"
import { UserPlus } from "lucide-react"

const BREADCRUMB_STEPS = {
  dashboard:  [{ label: "Accueil", view: "clients" }, { label: "Planning", view: "dashboard" }],
  planning:   [{ label: "Accueil", view: "clients" }, { label: "Planning", view: "dashboard" }],
  selection:  [{ label: "Accueil", view: "clients" }, { label: "Planning", view: "dashboard" }, { label: "Ma sélection", view: "selection" }],
  brief:      [{ label: "Accueil", view: "clients" }, { label: "Planning", view: "dashboard" }, { label: "Ma sélection", view: "selection" }, { label: "Brief", view: "brief" }],
  inspection: [{ label: "Accueil", view: "clients" }, { label: "Planning", view: "dashboard" }, { label: "Ma sélection", view: "selection" }, { label: "Brief", view: "brief" }, { label: "Inspection", view: "inspection" }],
  report:     [{ label: "Accueil", view: "clients" }, { label: "Planning", view: "dashboard" }, { label: "Ma sélection", view: "selection" }, { label: "Brief", view: "brief" }, { label: "Inspection", view: "inspection" }, { label: "Rapport", view: "report" }],
}

const BADGE_CONFIG = {
  brief:      { label: "EN PRÉPARATION", cls: "bg-brand-amber/20 text-brand-amber border border-brand-amber/50" },
  inspection: { label: "EN COURS",       cls: "bg-brand-emerald/20 text-brand-emerald border border-brand-emerald/50" },
  report:     { label: "AUDIT TERMINÉ",  cls: "bg-brand/20 text-white border border-brand/50" },
}

export default function Header({ view, auditContext, onNavigate, theme, user, lang, onLangChange, onLogout }) {
  const isAuditView = view === "brief" || view === "inspection" || view === "report"
  const [isOnline, setIsOnline] = useState(typeof navigator !== "undefined" ? navigator.onLine : true)
  const [menuOpen, setMenuOpen] = useState(false)
  const menuRef = useRef(null)

  const initials = user
    ? user.label.split(" ").map(w => w[0]).filter(Boolean).slice(0, 2).join("").toUpperCase()
    : "?"

  useEffect(() => {
    function closeMenu(e) { if (menuRef.current && !menuRef.current.contains(e.target)) setMenuOpen(false) }
    document.addEventListener("mousedown", closeMenu)
    return () => document.removeEventListener("mousedown", closeMenu)
  }, [])

  useEffect(() => {
    const setOnline = () => setIsOnline(true)
    const setOffline = () => setIsOnline(false)
    window.addEventListener("online", setOnline)
    window.addEventListener("offline", setOffline)
    return () => {
      window.removeEventListener("online", setOnline)
      window.removeEventListener("offline", setOffline)
    }
  }, [])

  return (
    <header className="bg-dark-teal text-white shadow-md">
      <div className="px-4 py-3 flex items-center justify-between">
        {/* Logo */}
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded bg-white flex items-center justify-center">
            <span className="text-dark-teal font-black text-lg leading-none">R</span>
          </div>
          <div>
            <div className="font-bold text-base leading-tight">RATP — Inspection Augmentée</div>
            {isAuditView && auditContext ? (
              <div className="text-xs text-brand-cyan">
                Audit ISO 9001 · {auditContext.nom} · {auditContext.auditeur}
              </div>
            ) : (
              <div className="text-xs text-brand-cyan">UC 28 · Hackathon Capgemini × Anthropic 2026</div>
            )}
          </div>
        </div>

        {/* Centre — badge statut */}
        {isAuditView && BADGE_CONFIG[view] && (
          <span className={`text-xs font-semibold px-3 py-1 rounded-full ${BADGE_CONFIG[view].cls}`}>
            {BADGE_CONFIG[view].label}
          </span>
        )}

        {/* Droite */}
        <div className="flex items-center gap-3">
          {!isOnline && (
            <span className="text-[10px] font-semibold px-2 py-0.5 rounded flex items-center gap-1 bg-brand-amber text-ink">
              <span>⚡</span> Mode hors-ligne
            </span>
          )}
          {view === "inspection" && (
            <>
              <span className="text-xs px-2 py-1 rounded font-mono bg-brand">ISO 9001</span>
            </>
          )}
          <div ref={menuRef} className="relative">
            <button
              onClick={() => setMenuOpen(v => !v)}
              className="w-8 h-8 rounded-full overflow-hidden flex items-center justify-center text-xs font-bold bg-brand text-white hover:ring-2 hover:ring-white/40 transition"
            >
              {user?.avatar
                ? <img src={user.avatar} alt={initials} className="w-full h-full object-cover" />
                : initials}
            </button>

            {menuOpen && user && (
              <div className="absolute right-0 top-full mt-2 w-72 bg-white rounded-xl shadow-lg border border-gray-100 z-50 overflow-hidden">
                {/* Ligne org + déconnexion */}
                <div className="flex items-center justify-between px-4 py-2 border-b border-gray-100">
                  <span className="text-xs font-semibold text-gray-500">Capgemini</span>
                  <button
                    onClick={() => { setMenuOpen(false); onLogout?.() }}
                    className="text-xs text-brand hover:underline"
                  >
                    Se déconnecter
                  </button>
                </div>

                {/* Carte utilisateur */}
                <div className="flex items-center gap-3 px-4 py-4">
                  <div className="w-12 h-12 rounded-full overflow-hidden flex items-center justify-center text-sm font-bold bg-brand text-white shrink-0">
                    {user.avatar
                      ? <img src={user.avatar} alt={initials} className="w-full h-full object-cover" />
                      : initials}
                  </div>
                  <div className="min-w-0">
                    <div className="font-semibold text-sm text-gray-900 truncate">{user.label}</div>
                    <div className="text-[11px] text-gray-500 truncate">{user.sublabel}</div>
                    <div className="text-[11px] text-brand truncate">{user.email}</div>
                  </div>
                </div>

                {/* Changer de compte */}
                <div className="border-t border-gray-100">
                  <button
                    onClick={() => { setMenuOpen(false); onLogout?.() }}
                    className="w-full flex items-center gap-3 px-4 py-3 text-xs text-gray-700 hover:bg-gray-50 transition-colors"
                  >
                    <UserPlus size={15} className="text-gray-400 shrink-0" />
                    Se connecter avec un autre compte
                  </button>
                </div>

                {/* Langue */}
                <div className="border-t border-gray-100 px-4 py-3 flex items-center gap-2">
                  <span className="text-[11px] text-gray-500 mr-1">Langue :</span>
                  {["FR", "EN"].map(l => (
                    <button
                      key={l}
                      onClick={() => onLangChange?.(l)}
                      className={`text-[11px] font-semibold px-2.5 py-0.5 rounded-full border transition-colors ${
                        lang === l ? "bg-brand text-white border-brand" : "border-gray-200 text-gray-500 hover:bg-gray-50"
                      }`}
                    >
                      {l}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Fil d'ariane */}
      {BREADCRUMB_STEPS[view] && (
        <div className="px-4 py-1.5 flex items-center gap-1.5 text-[11px] border-t border-white/10 bg-black/20">
          {BREADCRUMB_STEPS[view].map((step, i) => {
            const isCurrent = i === BREADCRUMB_STEPS[view].length - 1
            return (
              <span key={step.view} className="flex items-center gap-1.5">
                {i > 0 && <span className="text-white/30">›</span>}
                {isCurrent ? (
                  <span className="font-semibold text-white">{step.label}</span>
                ) : (
                  <button
                    onClick={() => onNavigate?.(step.view)}
                    className="text-brand-cyan hover:text-white hover:underline transition-colors"
                  >
                    {step.label}
                  </button>
                )}
              </span>
            )
          })}
        </div>
      )}
    </header>
  )
}
