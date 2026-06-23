import { useEffect, useRef, useState } from "react"
import { UserPlus } from "lucide-react"
import { useT } from "../useT"

export default function Header({ view, auditContext, onNavigate, theme, user, lang, onLangChange, onLogout }) {
  const t = useT(lang)

  const ALL_STEPS = [
    { labelKey: "header.nav_home",       view: "clients"    },
    { labelKey: "header.nav_planning",   view: "dashboard"  },
    { labelKey: "header.nav_brief",      view: "brief"      },
    { labelKey: "header.nav_inspection", view: "inspection" },
    { labelKey: "header.nav_report",     view: "report"     },
  ]

  const VIEW_INDEX = { clients: 0, dashboard: 1, planning: 1, selection: 1, brief: 2, inspection: 3, report: 4 }
  const currentStepIndex = VIEW_INDEX[view] ?? -1

  const BADGE_CONFIG = {
    brief:      { labelKey: "header.badge_brief",      cls: "bg-brand-amber/20 text-brand-amber border border-brand-amber/50" },
    inspection: { labelKey: "header.badge_inspection", cls: "bg-brand-emerald/20 text-brand-emerald border border-brand-emerald/50" },
    report:     { labelKey: "header.badge_report",     cls: "bg-brand/20 text-white border border-brand/50" },
  }

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
    <header className="bg-dark-teal text-white shadow-md sticky top-0 z-50">
      <div className="px-4 py-3 grid grid-cols-3 items-center">
        {/* Logo */}
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded bg-white flex items-center justify-center">
            <span className="text-dark-teal font-black text-sm leading-none">BV</span>
          </div>
          <div>
            <div className="font-bold text-base leading-tight">{t("header.app_title")}</div>
            {isAuditView && auditContext ? (
              <div className="text-xs text-brand-cyan">
                {auditContext.nom} · {auditContext.auditeur}
              </div>
            ) : (
              <div className="text-xs text-brand-cyan">{t("header.subtitle_default")}</div>
            )}
          </div>
        </div>

        {/* Centre — badge statut (toujours présent pour tenir la grille) */}
        <div className="flex justify-center">
          {isAuditView && BADGE_CONFIG[view] && (
            <span className={`text-xs font-semibold px-3 py-1 rounded-full ${BADGE_CONFIG[view].cls}`}>
              {t(BADGE_CONFIG[view].labelKey)}
            </span>
          )}
        </div>

        {/* Droite */}
        <div className="flex items-center gap-3 justify-end">
          {user && (
            <span className="text-sm text-white/70 hidden sm:block truncate max-w-[200px]">
              {t("header.hello_greeting")} {user.label}
            </span>
          )}
          {!isOnline && (
            <span className="text-[10px] font-semibold px-2 py-0.5 rounded flex items-center gap-1 bg-brand-amber text-ink">
              <span>⚡</span> {t("header.offline")}
            </span>
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
                    {t("header.logout")}
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
                    {t("header.switch_account")}
                  </button>
                </div>

                {/* Langue */}
                <div className="border-t border-gray-100 px-4 py-3 flex items-center gap-2">
                  <span className="text-[11px] text-gray-500 mr-1">{t("header.lang_label")}</span>
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

      {/* Fil d'ariane — toutes les étapes toujours visibles */}
      {currentStepIndex >= 1 && (
        <div className="px-4 py-2 flex items-center gap-1.5 text-xs border-t border-white/10 bg-black/20">
          {ALL_STEPS.map((step, i) => {
            const isCurrent = i === currentStepIndex
            const isPast = i < currentStepIndex
            return (
              <span key={step.view} className="flex items-center gap-1.5">
                {i > 0 && <span className="text-white/20">›</span>}
                {isCurrent ? (
                  <span className="font-bold text-white">{t(step.labelKey)}</span>
                ) : isPast ? (
                  <button
                    onClick={() => onNavigate?.(step.view)}
                    className="text-brand-cyan hover:text-white hover:underline transition-colors"
                  >
                    {t(step.labelKey)}
                  </button>
                ) : (
                  <span className="text-white/30">{t(step.labelKey)}</span>
                )}
              </span>
            )
          })}
        </div>
      )}
      {/* Mission & site sous le fil d'ariane */}
      {isAuditView && auditContext && (
        <div className="px-4 py-1 text-[11px] flex items-center gap-2 border-t border-white/5 bg-black/15">
          <span className="font-semibold text-white truncate">{auditContext.titre || auditContext.nom}</span>
          <span className="text-white/30">·</span>
          <span className="text-brand-cyan truncate">{auditContext.emplacement || auditContext.localisation}</span>
        </div>
      )}
    </header>
  )
}
