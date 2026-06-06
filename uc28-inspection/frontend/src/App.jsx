import { useState } from "react"
import Header from "./components/Header"
import LoginScreen from "./components/LoginScreen"
import ClientList from "./components/ClientList"
// Dashboard remplacé par PlanningOverlay
import Brief from "./components/Brief"
import InspectionCapture from "./components/InspectionCapture"
import ReportView from "./components/ReportView"
import SelectionView from "./components/SelectionView"
import PlanningOverlay from "./components/PlanningOverlay"
import { AUDIT_COURANT } from "./mockData"
import SupplierPortal from "./components/SupplierPortal"

export default function App() {
  const [view, setView] = useState("login")
  const [constats, setConstats] = useState([])
  const [startTime, setStartTime] = useState(null)
  const [customSections, setCustomSections] = useState([])
  const [extraItemsBySectionId, setExtraItemsBySectionId] = useState({})
  const [removedItemIds, setRemovedItemIds] = useState(new Set())
  const [removedSectionIds, setRemovedSectionIds] = useState(new Set())
  const [theme, setTheme] = useState("agile")
  const [user, setUser] = useState(null)
  const [lang, setLang] = useState("FR")
  const [feedbackCount, setFeedbackCount] = useState(0)
  const [selectedAuditIds, setSelectedAuditIds] = useState(new Set())
  const [selectedTransport, setSelectedTransport] = useState("voiture")
  const [selectedConfig, setSelectedConfig] = useState({ debutMin: 540, finMin: 1080, pauseMin: 720, pauseDuree: 90 })
  const [selectedClient, setSelectedClient] = useState(null)

  function handleOpenPlanning() {
    setView("planning")
  }

  function handleOpenSelection(ids, transport, config) {
    setSelectedAuditIds(ids)
    setSelectedTransport(transport || "voiture")
    setSelectedConfig(config || { debutMin: 540, finMin: 1080, pauseMin: 720, pauseDuree: 90 })
    setView("selection")
  }

  function handleDemarrerBrief() {
    setView("brief")
  }

  function handleDemarrerInspection(sections = [], extraItems = {}, removedIds = new Set(), removedSecs = new Set()) {
    setStartTime(Date.now())
    setConstats([])
    setCustomSections(sections)
    setExtraItemsBySectionId(extraItems)
    setRemovedItemIds(removedIds)
    setRemovedSectionIds(removedSecs)
    setView("inspection")
  }

  function handleAddConstat(constat) {
    setConstats((prev) => [...prev, constat])
  }

  function handleUpdateConstat(id, updates) {
    setConstats((prev) => prev.map((c) => c.id === id ? { ...c, ...updates } : c))
  }

  function handleDeleteConstat(id) {
    setConstats((prev) => prev.filter((c) => c.id !== id))
  }

  function handleGenererRapport() {
    setView("report")
  }

  // Durée audit formatée
  function getDuree() {
    if (!startTime) return "—"
    const elapsed = Math.floor((Date.now() - startTime) / 1000)
    const h = Math.floor(elapsed / 3600)
    const m = Math.floor((elapsed % 3600) / 60)
    const s = elapsed % 60
    if (h > 0) return `${h}h${String(m).padStart(2, "0")}m`
    return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
  }


  const showHeader = view !== "login"

  return (
    <div className="min-h-screen flex flex-col bg-canvas">
      {showHeader && (
        <Header
          view={view}
          auditContext={view !== "dashboard" ? AUDIT_COURANT : null}
          onNavigate={(v) => setView(v)}
          theme={theme}
          user={user}
          lang={lang}
          onLangChange={setLang}
          onLogout={() => { setUser(null); setView("login") }}
        />
      )}

      <div className="flex-1 flex flex-col" style={{ backgroundImage: "linear-gradient(to bottom, rgba(0,112,173,0.04) 0px, transparent 120px)" }}>
        {view === "login" && (
          <LoginScreen
            onLogin={(userData) => {
              setUser(userData)
              setView(userData.id === "fournisseur" ? "portail" : "clients")
            }}
            theme={theme}
            onThemeChange={setTheme}
          />
        )}
        {view === "portail" && (
          <SupplierPortal theme={theme} onBack={() => setView("login")} />
        )}
        {view === "clients" && (
          <ClientList onSelectClient={(client) => { setSelectedClient(client); setView("dashboard") }} theme={theme} />
        )}
        {(view === "dashboard" || view === "planning") && (
          <PlanningOverlay
            onClose={() => setView("clients")}
            onConfirm={handleOpenSelection}
            selectedClient={selectedClient}
            theme={theme}
          />
        )}
        {view === "selection" && (
          <SelectionView
            selectedIds={selectedAuditIds}
            transport={selectedTransport}
            config={selectedConfig}
            onDemarrer={handleDemarrerBrief}
            onBack={() => setView("dashboard")}
            theme={theme}
          />
        )}
        {view === "brief" && (
          <Brief
            onDemarrer={handleDemarrerInspection}
            onBack={() => setView("dashboard")}
            theme={theme}
          />
        )}
        {view === "inspection" && (
          <InspectionCapture
            constats={constats}
            onAddConstat={handleAddConstat}
            onUpdateConstat={handleUpdateConstat}
            onDeleteConstat={handleDeleteConstat}
            onGenererRapport={handleGenererRapport}
            onBack={() => setView("brief")}
            startTime={startTime}
            theme={theme}
            customSections={customSections}
            extraItemsBySectionId={extraItemsBySectionId}
            removedItemIds={removedItemIds}
            removedSectionIds={removedSectionIds}
            onFeedback={() => setFeedbackCount(c => c + 1)}
          />
        )}
        {view === "report" && (
          <ReportView
            constats={constats}
            dureeAudit={getDuree()}
            onBack={() => setView("inspection")}
            onNewAudit={() => { setView("dashboard"); setConstats([]) }}
            theme={theme}
            feedbackCount={feedbackCount}
          />
        )}
      </div>
    </div>
  )
}
