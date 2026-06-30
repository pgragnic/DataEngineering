import { useState } from 'react'
import { USER } from './mockData'
import Header from './components/Header'
import Dashboard from './components/Dashboard'
import Comptes from './components/Comptes'
import Statistiques from './components/Statistiques'
import Objectifs from './components/Objectifs'
import AIInsights from './components/AIInsights'

export default function App() {
  const [view, setView] = useState('dashboard')

  const VIEWS = {
    dashboard: <Dashboard onNav={setView} />,
    comptes: <Comptes />,
    stats: <Statistiques />,
    objectifs: <Objectifs />,
    insights: <AIInsights />,
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <Header user={USER} activeView={view} onNav={setView} />
      <main>{VIEWS[view] || VIEWS.dashboard}</main>
    </div>
  )
}
