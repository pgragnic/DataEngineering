import { useEffect } from 'react'
import { Loader2 } from 'lucide-react'

// Cette page reçoit le callback OAuth de Powens (?code=xxx)
// Elle poste le code à la fenêtre parente puis se ferme.
export default function OAuthCallback() {
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const code = params.get('code')
    const error = params.get('error')

    if (code && window.opener) {
      window.opener.postMessage({ type: 'POWENS_CALLBACK', code }, window.location.origin)
      window.close()
    } else if (error) {
      window.opener?.postMessage({ type: 'POWENS_CALLBACK', error }, window.location.origin)
      window.close()
    }
  }, [])

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <div className="text-center">
        <Loader2 size={32} className="text-cmb-red animate-spin mx-auto mb-3" />
        <p className="text-sm text-gray-600">Finalisation de la connexion…</p>
        <p className="text-xs text-gray-400 mt-1">Cette fenêtre va se fermer automatiquement.</p>
      </div>
    </div>
  )
}
