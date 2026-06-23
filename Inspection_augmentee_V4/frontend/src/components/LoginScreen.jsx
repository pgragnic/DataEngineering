import { useState } from "react"
import { useT } from "../useT"

const ROLES = [
  { id: "auditeur",    icon: "👤", label: "Marc Lefèvre",   email: "marc.lefevre@bureauveritas.com", avatar: "/marc-lefevre.png",  sublabelKey: "login.role_auditor_sublabel" },
  { id: "fournisseur", icon: "🏢", label: "Mei Lin Zhang", email: "meilin.zhang@ratp.fr",           avatar: "/mei-lin-zhang.png", sublabelKey: "login.role_supplier_sublabel" },
]

export default function LoginScreen({ onLogin, theme, onThemeChange, lang, onLangChange }) {
  const t = useT(lang)
  const [selectedRole, setSelectedRole] = useState("auditeur")
  const [email, setEmail] = useState("marc.lefevre@bureauveritas.com")
  const [password, setPassword] = useState("••••••••")

  function selectRole(roleId) {
    setSelectedRole(roleId)
    setEmail(ROLES.find(r => r.id === roleId).email)
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-4">
      <div className="bg-surface rounded-2xl shadow-2xl w-full max-w-sm p-8">

        {/* Sélecteur de langue */}
        <div className="flex justify-end gap-1 mb-4">
          {["FR", "EN"].map(l => (
            <button
              key={l}
              onClick={() => onLangChange?.(l)}
              className={`text-[11px] font-semibold px-2.5 py-0.5 rounded-full border transition-colors ${
                lang === l ? "bg-brand text-white border-brand" : "border-divider text-ink-muted hover:bg-canvas"
              }`}
            >
              {l}
            </button>
          ))}
        </div>

        <h1 className="text-center text-lg font-bold text-ink mb-1">
          {t("login.title")}
        </h1>
        <p className="text-center text-xs text-ink-muted mb-5">{t("login.subtitle")}</p>

        {/* Sélecteur de rôle */}
        <div className="grid grid-cols-2 gap-2 mb-5">
          {ROLES.map(({ id, icon, label, avatar, sublabelKey }) => (
            <button
              key={id}
              onClick={() => selectRole(id)}
              className={`p-3 rounded-xl border-2 text-left transition-all ${
                selectedRole === id
                  ? "border-brand bg-brand/10"
                  : "border-divider hover:border-ink-muted bg-surface"
              }`}
            >
              <div className="mb-2">
                {avatar
                  ? <img src={avatar} alt={label} className="w-10 h-10 rounded-full object-cover" />
                  : <span className="text-xl">{icon}</span>}
              </div>
              <div className="text-xs font-bold text-ink leading-tight">{label}</div>
              <div className="text-[10px] text-ink-muted mt-0.5">{t(sublabelKey)}</div>
            </button>
          ))}
        </div>

        <div className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-ink-muted mb-1">{t("login.email_label")}</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full border border-divider rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-brand"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-ink-muted mb-1">{t("login.password_label")}</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full border border-divider rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-brand"
            />
          </div>
        </div>

        <button
          onClick={() => {
            const roleObj = ROLES.find(r => r.id === selectedRole) || ROLES[0]
            onLogin({ id: roleObj.id, label: roleObj.label, sublabel: t(roleObj.sublabelKey), email, avatar: roleObj.avatar })
          }}
          className="w-full mt-6 text-white font-semibold py-2.5 rounded-lg text-sm bg-brand hover:bg-brand-cyan transition-colors"
        >
          {t("login.login_button")}
        </button>

        <p className="text-center text-[10px] text-ink-muted mt-4">
          {t("login.secure_notice")}
        </p>
      </div>
    </div>
  )
}
