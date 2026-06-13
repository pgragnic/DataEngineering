import { PieChart, BarChart, DailyMovers } from '../components/Charts.jsx'
import { fmt, pnlCls, sign } from '../utils.js'

export default function Stats({ positions, sorted, equity, totalPnl, cash, loading }) {
  return (
    <div className="tab-content">
      <DailyMovers positions={positions} />
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label">Equity</div>
          <div className="stat-value">${fmt(equity)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Positions</div>
          <div className="stat-value">{positions.length}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">P&L total</div>
          <div className={`stat-value ${pnlCls(totalPnl)}`}>{sign(totalPnl)}${fmt(totalPnl)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Cash</div>
          <div className="stat-value">${fmt(cash)}</div>
        </div>
      </div>
      {positions.length > 0 && (
        <>
          <div className="section-title">Répartition du portefeuille</div>
          <div className="card">
            <PieChart positions={sorted} />
          </div>
          <div className="section-title">P&L par position (top 8)</div>
          <div className="card bar-card">
            <BarChart positions={positions} />
          </div>
        </>
      )}
      {loading && <div className="spinner">Chargement…</div>}
      {!loading && !positions.length && <div className="empty">Aucune donnée disponible</div>}
    </div>
  )
}
