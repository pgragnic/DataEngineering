import { useState, useEffect } from 'react'
import { USER } from './mockData'
import Header from './components/Header'
import Dashboard from './components/Dashboard'
import Comptes from './components/Comptes'
import Statistiques from './components/Statistiques'
import Objectifs from './components/Objectifs'
import AIInsights from './components/AIInsights'
import BankConnect from './components/BankConnect'
import OAuthCallback from './components/OAuthCallback'

// Détecte si on est sur la page de callback OAuth (/callback)
const IS_CALLBACK = window.location.pathname === '/callback'

export default function App() {
  const [view, setView] = useState('dashboard')
  // connectionInfo = null (mode mock) | { token, accounts, transactions, connectedAt }
  const [connectionInfo, setConnectionInfo] = useState(null)

  // Page de callback OAuth — gérée immédiatement, sans afficher l'app
  if (IS_CALLBACK) return <OAuthCallback />

  function handleConnected(info) {
    setConnectionInfo(info)
    setView('dashboard')
  }

  function handleDisconnect() {
    setConnectionInfo(null)
  }

  const VIEWS = {
    dashboard: <Dashboard onNav={setView} connectionInfo={connectionInfo} />,
    comptes: <Comptes connectionInfo={connectionInfo} />,
    stats: <Statistiques connectionInfo={connectionInfo} />,
    objectifs: <Objectifs />,
    insights: <AIInsights connectionInfo={connectionInfo} />,
    connect: (
      <BankConnect
        onConnected={handleConnected}
        onDisconnect={handleDisconnect}
        connectionInfo={connectionInfo}
      />
    ),
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <Header user={USER} activeView={view} onNav={setView} connectionInfo={connectionInfo} />
      <main>{VIEWS[view] || VIEWS.dashboard}</main>
    </div>
  )
}
