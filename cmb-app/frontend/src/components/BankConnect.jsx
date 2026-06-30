import { useState, useEffect } from 'react'
import { Shield, Wifi, AlertCircle, CheckCircle2, Loader2, ExternalLink, Lock, RefreshCw, LogOut } from 'lucide-react'
import { initConnection, exchangeCode, fetchAccounts, fetchTransactions, checkConnectionStatus } from '../api'

const REDIRECT_URI = `${window.location.origin}/callback`

export default function BankConnect({ onConnected, onDisconnect, connectionInfo }) {
  const [step, setStep] = useState('idle') // idle | loading | webview | polling | success | error
  const [error, setError] = useState(null)
  const [statusMsg, setStatusMsg] = useState('')
  const [webviewUrl, setWebviewUrl] = useState(null)
  const [webviewWindow, setWebviewWindow] = useState(null)

  // Vérifie si un token existe déjà en localStorage
  useEffect(() => {
    const savedToken = localStorage.getItem('powens_token')
    if (savedToken && !connectionInfo) {
      setStep('loading')
      setStatusMsg('Vérification de la connexion…')
      checkConnectionStatus(savedToken)
        .then(res => {
          if (res.connected) {
            loadRealData(savedToken)
          } else {
            localStorage.removeItem('powens_token')
            setStep('idle')
          }
        })
        .catch(() => setStep('idle'))
    }
  }, [])

  // Écoute le callback OAuth dans la fenêtre popup
  useEffect(() => {
    function handleMessage(event) {
      if (event.data?.type === 'POWENS_CALLBACK') {
        const { code } = event.data
        if (code) handleCallback(code)
        webviewWindow?.close()
      }
    }
    window.addEventListener('message', handleMessage)
    return () => window.removeEventListener('message', handleMessage)
  }, [webviewWindow])

  // Poll la fenêtre popup pour détecter la fermeture manuelle
  useEffect(() => {
    if (!webviewWindow) return
    const timer = setInterval(() => {
      if (webviewWindow.closed) {
        clearInterval(timer)
        if (step === 'webview') setStep('idle')
      }
    }, 1000)
    return () => clearInterval(timer)
  }, [webviewWindow, step])

  async function startConnection() {
    setStep('loading')
    setError(null)
    setStatusMsg('Initialisation de la connexion sécurisée…')
    try {
      const { webview_url } = await initConnection(REDIRECT_URI)
      setWebviewUrl(webview_url)
      setStep('webview')

      // Ouvre la webview Powens dans une popup
      const popup = window.open(
        webview_url,
        'powens-webview',
        'width=520,height=700,left=200,top=100,resizable=yes,scrollbars=yes'
      )
      setWebviewWindow(popup)
    } catch (e) {
      setError(e.message)
      setStep('error')
    }
  }

  async function handleCallback(code) {
    setStep('loading')
    setStatusMsg('Connexion établie — récupération du token…')
    try {
      const { access_token } = await exchangeCode(code)
      localStorage.setItem('powens_token', access_token)
      await loadRealData(access_token)
    } catch (e) {
      setError(e.message)
      setStep('error')
    }
  }

  async function loadRealData(token) {
    setStep('loading')
    setStatusMsg('Récupération de vos comptes CMB…')
    try {
      const [accountsRes, txRes] = await Promise.all([
        fetchAccounts(token),
        fetchTransactions(token, { minDate: ninetyDaysAgo() }),
      ])
      setStatusMsg('Chargement des transactions…')
      setStep('success')
      onConnected({
        token,
        accounts: accountsRes.accounts,
        transactions: txRes.transactions,
        connectedAt: new Date().toISOString(),
      })
    } catch (e) {
      setError(`Données indisponibles : ${e.message}`)
      setStep('error')
    }
  }

  function disconnect() {
    localStorage.removeItem('powens_token')
    onDisconnect()
    setStep('idle')
    setWebviewUrl(null)
  }

  if (connectionInfo) {
    return (
      <div className="card border border-emerald-200 bg-emerald-50">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-emerald-100 rounded-xl flex items-center justify-center shrink-0">
            <CheckCircle2 size={20} className="text-emerald-600" />
          </div>
          <div className="flex-1">
            <p className="text-sm font-semibold text-emerald-800">Compte CMB connecté en temps réel</p>
            <p className="text-xs text-emerald-600 mt-0.5">
              {connectionInfo.accounts.length} compte(s) · {connectionInfo.transactions.length} transactions ·{' '}
              Mis à jour {new Date(connectionInfo.connectedAt).toLocaleTimeString('fr-FR')}
            </p>
          </div>
          <div className="flex gap-2 shrink-0">
            <button
              onClick={() => loadRealData(localStorage.getItem('powens_token'))}
              className="btn-ghost text-xs flex items-center gap-1.5"
            >
              <RefreshCw size={12} />
              Actualiser
            </button>
            <button
              onClick={disconnect}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-gray-200 text-xs text-gray-600 hover:bg-gray-50"
            >
              <LogOut size={12} />
              Déconnecter
            </button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="max-w-xl mx-auto px-6 py-6">
      <div className="card space-y-5">
        {/* En-tête */}
        <div className="flex items-start gap-4">
          <div className="w-12 h-12 bg-cmb-red/10 rounded-xl flex items-center justify-center shrink-0">
            <Wifi size={22} className="text-cmb-red" />
          </div>
          <div>
            <h2 className="text-lg font-bold text-gray-900">Connexion temps réel à votre compte CMB</h2>
            <p className="text-sm text-gray-500 mt-1">
              Connectez votre compte Crédit Mutuel de Bretagne via Open Banking
              (norme DSP2) pour afficher vos vrais soldes et transactions.
            </p>
          </div>
        </div>

        {/* Garanties sécurité */}
        <div className="grid grid-cols-3 gap-3">
          {[
            { icon: Lock, label: 'Chiffré TLS', sub: 'Données sécurisées' },
            { icon: Shield, label: 'Agrément ACPR', sub: 'Powens agréé DSP2' },
            { icon: CheckCircle2, label: 'Lecture seule', sub: 'Aucune action possible' },
          ].map(({ icon: Icon, label, sub }) => (
            <div key={label} className="text-center p-3 bg-gray-50 rounded-xl">
              <Icon size={18} className="text-emerald-600 mx-auto mb-1" />
              <p className="text-xs font-semibold text-gray-700">{label}</p>
              <p className="text-xs text-gray-400 mt-0.5">{sub}</p>
            </div>
          ))}
        </div>

        {/* Étapes */}
        <div className="space-y-2 text-sm">
          {[
            'Une fenêtre sécurisée Powens s\'ouvre',
            'Cherchez "Crédit Mutuel de Bretagne" et connectez-vous avec vos identifiants',
            'Vos soldes et transactions apparaissent automatiquement dans l\'app',
          ].map((s, i) => (
            <div key={i} className="flex items-center gap-3">
              <span className="w-5 h-5 rounded-full bg-cmb-red text-white text-xs font-bold flex items-center justify-center shrink-0">
                {i + 1}
              </span>
              <span className="text-gray-600">{s}</span>
            </div>
          ))}
        </div>

        {/* Statut */}
        {step === 'loading' && (
          <div className="flex items-center gap-3 p-3 bg-blue-50 rounded-lg">
            <Loader2 size={16} className="text-blue-600 animate-spin shrink-0" />
            <p className="text-sm text-blue-700">{statusMsg}</p>
          </div>
        )}

        {step === 'webview' && (
          <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
            <p className="font-medium">Fenêtre de connexion ouverte…</p>
            <p className="text-xs mt-0.5">Connectez-vous dans la fenêtre Powens. Elle se fermera automatiquement.</p>
            <button onClick={() => window.open(webviewUrl, '_blank')} className="text-xs text-amber-800 underline mt-1 flex items-center gap-1">
              <ExternalLink size={10} /> Réouvrir si fermée
            </button>
          </div>
        )}

        {step === 'error' && (
          <div className="flex items-start gap-3 p-3 bg-red-50 border border-red-200 rounded-lg">
            <AlertCircle size={16} className="text-red-600 shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-medium text-red-700">Connexion échouée</p>
              <p className="text-xs text-red-600 mt-0.5">{error}</p>
              <p className="text-xs text-red-500 mt-1">
                Vérifiez que POWENS_CLIENT_ID et POWENS_CLIENT_SECRET sont définis dans le fichier .env du backend.
              </p>
            </div>
          </div>
        )}

        {/* Bouton */}
        <button
          onClick={startConnection}
          disabled={step === 'loading' || step === 'webview'}
          className="btn-primary w-full flex items-center justify-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
        >
          {step === 'loading' ? (
            <Loader2 size={15} className="animate-spin" />
          ) : (
            <Wifi size={15} />
          )}
          Connecter mon compte CMB
        </button>

        <p className="text-xs text-gray-400 text-center">
          Connexion via <strong>Powens</strong> (anciennement Budget Insight) — agrégateur bancaire agréé par l'ACPR.
          Vos identifiants ne transitent jamais par nos serveurs.
        </p>
      </div>

      {/* Guide configuration */}
      <details className="mt-4 card">
        <summary className="text-sm font-semibold text-gray-700 cursor-pointer select-none">
          Comment configurer Powens ? (guide développeur)
        </summary>
        <ol className="mt-3 space-y-2 text-sm text-gray-600 list-decimal list-inside">
          <li>Créez un compte sur <strong>developers.powens.com</strong></li>
          <li>Créez une nouvelle application → notez <code className="bg-gray-100 px-1 rounded">Client ID</code> et <code className="bg-gray-100 px-1 rounded">Client Secret</code></li>
          <li>
            Ajoutez dans <code className="bg-gray-100 px-1 rounded">cmb-app/backend/.env</code> :
            <pre className="mt-1 bg-gray-900 text-green-400 text-xs p-2 rounded overflow-x-auto">{`POWENS_DOMAIN=votre-domaine
POWENS_CLIENT_ID=xxxxx
POWENS_CLIENT_SECRET=xxxxx`}</pre>
          </li>
          <li>Ajoutez <code className="bg-gray-100 px-1 rounded">{window.location.origin}/callback</code> dans les <em>Redirect URIs</em> autorisées</li>
          <li>Redémarrez le backend et cliquez sur "Connecter mon compte CMB"</li>
        </ol>
        <p className="mt-2 text-xs text-gray-400">En mode sandbox (POWENS_DOMAIN=sandbox), utilisez les connecteurs de test Powens — aucun vrai compte bancaire requis.</p>
      </details>
    </div>
  )
}

function ninetyDaysAgo() {
  const d = new Date()
  d.setDate(d.getDate() - 90)
  return d.toISOString().split('T')[0]
}
