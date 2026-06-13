import { useState, useEffect } from 'react'
import Logo from '../components/Logo.jsx'
import { fmt } from '../utils.js'

export default function Suivi({ API, TRACK_USER }) {
  const [trackData,    setTrackData]    = useState(null)
  const [trackLoading, setTrackLoading] = useState(false)

  function loadTrack() {
    setTrackLoading(true)
    fetch(`${API}/api/track/${TRACK_USER}`)
      .then(r => r.json())
      .then(d => { setTrackData(d); setTrackLoading(false) })
      .catch(() => setTrackLoading(false))
  }

  useEffect(() => { loadTrack() }, [])

  return (
    <div className="tab-content">
      <div className="track-header">
        <span className="track-username">@{TRACK_USER}</span>
        <button className="btn-refresh" onClick={loadTrack}>↻ Actualiser</button>
      </div>
      {trackLoading && <div className="spinner">Chargement…</div>}
      {trackData?.error && <div className="empty">Erreur : {trackData.error}</div>}
      {trackData && !trackData.error && !trackData.history?.length && !trackLoading && (
        <div className="empty">Aucun mouvement détecté pour l'instant — revenez plus tard.</div>
      )}
      {trackData?.history?.length > 0 && (
        <>
          <div className="section-title">Mouvements récents</div>
          {trackData.history.map((h, i) => {
            const sym    = (h.name || '').split('/')[0].split(' ')[0]
            const isOpen = h.action === 'open'
            return (
              <div key={i} className="card">
                <div className="card-row">
                  <div className="card-left">
                    <div className="card-logo-name">
                      <Logo symbol={sym} />
                      <div>
                        <div className="card-name">{h.name}</div>
                        <div className="card-sub">
                          <span className={isOpen ? 'pnl-pos' : 'pnl-neg'}>
                            {isOpen ? '● Ouvert' : '○ Fermé'}
                          </span>
                          {' · '}{h.isBuy ? '▲ Long' : '▼ Short'}
                        </div>
                      </div>
                    </div>
                  </div>
                  <div className="card-right">
                    {h.amount > 0 && <div className="card-amount">{fmt(h.amount)}%</div>}
                    <div className="card-sub">{h.date}</div>
                  </div>
                </div>
              </div>
            )
          })}
        </>
      )}
    </div>
  )
}
