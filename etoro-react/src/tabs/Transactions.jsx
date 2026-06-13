import { useState, useEffect } from 'react'
import { TxDetailModal } from '../components/Modals.jsx'
import { fmt } from '../utils.js'

export default function Transactions({ API }) {
  const [txData,    setTxData]    = useState(null)
  const [txLoading, setTxLoading] = useState(false)
  const [txDetail,  setTxDetail]  = useState(null)

  function loadTx() {
    setTxLoading(true)
    fetch(`${API}/api/transactions`)
      .then(r => r.json())
      .then(d => { setTxData(Array.isArray(d) ? d : []); setTxLoading(false) })
      .catch(() => setTxLoading(false))
  }

  useEffect(() => { loadTx() }, [])

  return (
    <div className="tab-content">
      <TxDetailModal tx={txDetail} onClose={() => setTxDetail(null)} />
      <div className="track-header">
        <span className="track-username">Historique</span>
        <button className="btn-refresh" onClick={loadTx}>↻ Actualiser</button>
      </div>
      {txLoading && <div className="spinner">Chargement…</div>}
      {txData && !txData.length && !txLoading && (
        <div className="empty">Aucune transaction enregistrée</div>
      )}
      {txData?.map((tx, i) => {
        const isBuy = tx.type === 'buy'
        const isOk  = tx.status === 'ok'
        const date  = tx.date ? tx.date.replace('T', ' ').replace('Z', '') : ''
        return (
          <div key={i} className="card selectable" onClick={() => setTxDetail(tx)}>
            <div className="card-row">
              <div className="card-left">
                <div className="card-logo-name">
                  <div className="pos-logo-wrap">
                    <div className="pos-logo-init" style={{
                      color: isBuy ? 'var(--green)' : 'var(--red)',
                      fontWeight: 700, fontSize: '0.7rem'
                    }}>
                      {isBuy ? 'BUY' : 'SELL'}
                    </div>
                  </div>
                  <div>
                    <div className="card-name">
                      {tx.symbol || `Position #${tx.positionId}`}
                      {!isOk && <span className="badge" style={{ background: 'var(--red)' }}>ERR</span>}
                    </div>
                    <div className="card-sub">{date}</div>
                    {tx.error && <div className="card-sub" style={{ color: 'var(--red)' }}>{tx.error}</div>}
                  </div>
                </div>
              </div>
              <div className="card-right">
                {tx.pct    != null && <div className="card-sub">{tx.pct}%</div>}
                {tx.amount != null && <div className="card-amount">${fmt(tx.amount)}</div>}
                {tx.orderId && <div className="card-sub">#{tx.orderId}</div>}
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}
