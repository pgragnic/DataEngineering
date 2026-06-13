import { fmt } from '../utils.js'

export function ConfirmModal({ confirm, onClose, onSell, onCancel }) {
  if (!confirm) return null
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-title">
          {confirm.type === 'sell' ? 'Vendre cette position ?' : 'Annuler cet ordre ?'}
        </div>
        <div className="modal-name">{confirm.name}</div>
        {confirm.amount != null && <div className="modal-amount">${fmt(confirm.amount)}</div>}
        <div className="modal-buttons">
          <button className="btn-modal-back" onClick={onClose}>Retour</button>
          <button
            className={confirm.type === 'sell' ? 'btn-modal-sell' : 'btn-modal-cancel'}
            onClick={() => confirm.type === 'sell' ? onSell(confirm.pid) : onCancel(confirm.oid)}
          >
            {confirm.type === 'sell' ? 'Vendre' : 'Confirmer'}
          </button>
        </div>
      </div>
    </div>
  )
}

export function TxDetailModal({ tx, onClose }) {
  if (!tx) return null
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal-tx" onClick={e => e.stopPropagation()}>
        <div className="modal-title">
          {tx.type === 'buy' ? '▲ BUY' : '▼ SELL'} {tx.symbol || `#${tx.positionId}`}
        </div>
        <div className="modal-name">{tx.date?.replace('T', ' ').replace('Z', '')}</div>
        <table className="tx-detail-table">
          <tbody>
            {Object.entries(tx).filter(([k]) => k !== 'detail').map(([k, v]) => (
              <tr key={k}>
                <td>{k}</td>
                <td>{v == null ? '—' : String(v)}</td>
              </tr>
            ))}
            {tx.detail && Object.entries(tx.detail).map(([k, v]) => (
              <tr key={'d_' + k} className="tx-detail-row">
                <td>{k}</td>
                <td>{v == null ? '—' : String(v)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="modal-buttons">
          <button className="btn-modal-back" onClick={onClose}>Fermer</button>
        </div>
      </div>
    </div>
  )
}

export function ConfigModal({ show, ratio, saving, onRatioChange, onSave, onClose }) {
  if (!show) return null
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-title">Ratio d'affichage</div>
        <div className="modal-name" style={{ marginBottom: 16 }}>
          Multiplie toutes les valeurs monétaires.<br />
          Exemple : 0.691 pour afficher ~69% du réel.
        </div>
        <input
          type="number"
          step="0.001"
          min="0.001"
          value={ratio}
          onChange={e => onRatioChange(e.target.value)}
          style={{ textAlign: 'center', fontSize: '1.2rem', fontWeight: 700 }}
        />
        <div className="modal-buttons">
          <button className="btn-modal-back" onClick={onClose}>Annuler</button>
          <button className="btn-modal-sell" onClick={onSave} disabled={saving}>
            {saving ? '…' : 'Enregistrer'}
          </button>
        </div>
      </div>
    </div>
  )
}
