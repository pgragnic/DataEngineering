import ResponseBox from '../components/ResponseBox.jsx'
import { fmt } from '../utils.js'

export default function Pending({ pending, loading, actionResp, actionRespData, setConfirm }) {
  return (
    <div className="tab-content">
      <ResponseBox state={actionResp} data={actionRespData} />
      {loading && <div className="spinner">Chargement…</div>}
      {!loading && !pending.length && <div className="empty">Aucun ordre en attente</div>}
      {pending.map(o => (
        <div key={o.orderId} className="card">
          <div className="card-row">
            <div className="card-left">
              <div className="card-name">{o.name}</div>
              <div className="card-sub">Ordre #{o.orderId}</div>
            </div>
            <div className="card-right">
              <div className="card-amount">${fmt(o.amount)}</div>
            </div>
          </div>
          <button
            className="btn-cancel-order"
            onClick={() => setConfirm({ type: 'cancel', oid: o.orderId, name: o.name, amount: o.amount })}
          >Annuler</button>
        </div>
      ))}
    </div>
  )
}
