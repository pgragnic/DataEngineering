import { User, Bell, ChevronDown, LogOut, Wifi, WifiOff } from 'lucide-react'
import { useState } from 'react'

export default function Header({ user, activeView, onNav, connectionInfo }) {
  const [menuOpen, setMenuOpen] = useState(false)
  const isConnected = !!connectionInfo

  const NAV = [
    { id: 'dashboard', label: 'Tableau de bord' },
    { id: 'comptes', label: 'Mes comptes' },
    { id: 'stats', label: 'Statistiques' },
    { id: 'objectifs', label: 'Objectifs' },
    { id: 'insights', label: '✨ Analyse IA' },
  ]

  return (
    <header className="bg-white border-b border-gray-200 sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-6 flex items-center justify-between h-14">
        {/* Logo */}
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-cmb-red flex items-center justify-center shrink-0">
            <span className="text-white text-xs font-bold">CMB</span>
          </div>
          <span className="font-semibold text-gray-900 hidden sm:block">Mon Espace Finances</span>
        </div>

        {/* Nav */}
        <nav className="hidden md:flex items-center gap-1">
          {NAV.map(n => (
            <button
              key={n.id}
              onClick={() => onNav(n.id)}
              className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
                activeView === n.id
                  ? 'bg-cmb-red text-white'
                  : 'text-gray-600 hover:bg-gray-100'
              }`}
            >
              {n.label}
            </button>
          ))}
        </nav>

        {/* Actions droite */}
        <div className="flex items-center gap-2">
          {/* Indicateur de connexion */}
          <button
            onClick={() => onNav('connect')}
            className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors ${
              isConnected
                ? 'bg-emerald-100 text-emerald-700 hover:bg-emerald-200'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
            title={isConnected ? 'Connecté à CMB' : 'Données démo — cliquer pour connecter'}
          >
            {isConnected
              ? <Wifi size={13} />
              : <WifiOff size={13} />
            }
            <span className="hidden sm:inline">{isConnected ? 'En direct' : 'Démo'}</span>
          </button>

          <button className="relative p-2 rounded-lg hover:bg-gray-100 text-gray-600">
            <Bell size={18} />
            <span className="absolute top-1.5 right-1.5 w-2 h-2 bg-cmb-red rounded-full" />
          </button>

          {/* Menu profil */}
          <div className="relative">
            <button
              onClick={() => setMenuOpen(v => !v)}
              className="flex items-center gap-2 px-3 py-1.5 rounded-lg hover:bg-gray-100"
            >
              <div className="w-7 h-7 rounded-full bg-cmb-red text-white text-xs font-bold flex items-center justify-center">
                {user.avatar}
              </div>
              <span className="text-sm font-medium text-gray-700 hidden sm:block">{user.nom.split(' ')[0]}</span>
              <ChevronDown size={14} className="text-gray-400" />
            </button>
            {menuOpen && (
              <div className="absolute right-0 top-full mt-1 w-60 bg-white rounded-xl shadow-lg border border-gray-100 py-2 z-50">
                <div className="px-4 py-2 border-b border-gray-100">
                  <p className="text-sm font-semibold text-gray-900">{user.nom}</p>
                  <p className="text-xs text-gray-500">{user.email}</p>
                  <p className="text-xs text-gray-400 mt-0.5">{user.agence}</p>
                </div>
                <div className="px-4 py-2 border-b border-gray-100">
                  <p className="text-xs text-gray-500">Conseiller : <strong className="text-gray-700">{user.conseiller}</strong></p>
                  <div className={`flex items-center gap-1.5 mt-1 text-xs font-medium ${isConnected ? 'text-emerald-600' : 'text-gray-400'}`}>
                    {isConnected ? <Wifi size={11} /> : <WifiOff size={11} />}
                    {isConnected ? `Compte connecté · ${connectionInfo.accounts.length} compte(s)` : 'Mode démonstration'}
                  </div>
                </div>
                <button
                  onClick={() => { setMenuOpen(false); onNav('connect') }}
                  className={`w-full flex items-center gap-2 px-4 py-2 text-sm hover:bg-gray-50 ${isConnected ? 'text-emerald-700' : 'text-cmb-red'}`}
                >
                  <Wifi size={14} />
                  {isConnected ? 'Gérer la connexion CMB' : 'Connecter mon compte CMB'}
                </button>
                <button className="w-full flex items-center gap-2 px-4 py-2 text-sm text-gray-500 hover:bg-gray-50">
                  <LogOut size={14} />
                  Se déconnecter
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
    </header>
  )
}
