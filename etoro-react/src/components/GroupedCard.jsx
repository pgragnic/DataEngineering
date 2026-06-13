import { useState } from 'react'
import Logo from './Logo.jsx'
import { fmt, pnlCls, sign } from '../utils.js'

export function GroupedCard({ group, setConfirm }) {
  const [expanded, setExpanded] = useState(false)
  const total    = group.reduce((s, p) => s + p.amount, 0)
  const totalPnl = group.reduce((s, p) => s + p.pnl, 0)
  const first    = group[0]
  const sym      = (first.name || '').split('/')[0].split(' ')[0]

  if (group.length === 1) {
    const p = group[0]
    return (
      <div className="card">
        <div className="card-row">
          <div className="card-left">
            <div className="card-logo-name">
              <Logo symbol={sym} />
              <div>
                <div className="card-name">{p.name}</div>
                <div className="card-sub">{p.isBuy ? '▲ Long' : '▼ Short'} · {p.openDate}</div>
                {p.openRate > 0 && (
                  <div className="card-rates">Ouv {p.openRate}{p.closeRate > 0 ? ` · Act ${p.closeRate}` : ''}</div>
                )}
              </div>
            </div>
          </div>
          <div className="card-right">
            <div className="card-amount">${fmt(p.amount)}</div>
            <div className={`card-pnl ${pnlCls(p.pnl)}`}>{sign(p.pnl)}${fmt(p.pnl)}</div>
            <button
              className="btn-sell"
              onClick={() => setConfirm({ type: 'sell', pid: p.positionID, name: p.name, amount: p.amount })}
            >Vendre</button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="card">
      <div className="card-row" onClick={() => setExpanded(!expanded)} style={{ cursor: 'pointer' }}>
        <div className="card-left">
          <div className="card-logo-name">
            <Logo symbol={sym} />
            <div>
              <div className="card-name">
                {first.name}
                <span className="badge">{group.length}</span>
              </div>
              <div className="card-sub">{group.length} positions · {expanded ? '▲ réduire' : '▼ détail'}</div>
            </div>
          </div>
        </div>
        <div className="card-right">
          <div className="card-amount">${fmt(total)}</div>
          <div className={`card-pnl ${pnlCls(totalPnl)}`}>{sign(totalPnl)}${fmt(totalPnl)}</div>
        </div>
      </div>
      {expanded && (
        <div className="sub-positions">
          {group.map(p => (
            <div key={p.positionID} className="sub-pos">
              <div className="sub-pos-info">
                <span>{p.isBuy ? '▲' : '▼'} {p.openDate}</span>
                <span>Ouv {p.openRate}</span>
                <span className={pnlCls(p.pnl)}>{sign(p.pnl)}${fmt(p.pnl)}</span>
              </div>
              <button
                className="btn-sell-sm"
                onClick={() => setConfirm({ type: 'sell', pid: p.positionID, name: p.name, amount: p.amount })}
              >Vendre</button>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
