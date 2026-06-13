import { GroupedCard } from '../components/GroupedCard.jsx'
import ResponseBox from '../components/ResponseBox.jsx'
import { fmt, pnlCls, sign, SORTS } from '../utils.js'

export default function Portfolio({ positions, groupList, loading, sortKey, setSortKey, actionResp, actionRespData, setConfirm, invested, cash, totalPnl }) {
  return (
    <div className="tab-content">
      <div className="summary-row">
        <div className="summary-card">
          <div className="summary-label">Investi</div>
          <div className="summary-value">${fmt(invested)}</div>
        </div>
        <div className="summary-card">
          <div className="summary-label">Cash</div>
          <div className="summary-value">${fmt(cash)}</div>
        </div>
        <div className="summary-card">
          <div className="summary-label">P&L</div>
          <div className={`summary-value ${pnlCls(totalPnl)}`}>{sign(totalPnl)}${fmt(totalPnl)}</div>
        </div>
      </div>
      <div className="sort-row">
        {SORTS.map(s => (
          <button key={s.key} className={`sort-btn${sortKey === s.key ? ' active' : ''}`} onClick={() => setSortKey(s.key)}>
            {s.label}
          </button>
        ))}
      </div>
      <ResponseBox state={actionResp} data={actionRespData} />
      {loading && <div className="spinner">Chargement…</div>}
      {groupList.map((group, i) => (
        <GroupedCard key={group[0].name + i} group={group} setConfirm={setConfirm} />
      ))}
      {!loading && !positions.length && <div className="empty">Aucune position ouverte</div>}
    </div>
  )
}
