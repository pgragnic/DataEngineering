import { useState } from 'react'
import Logo from './Logo.jsx'
import { fmt, pnlCls, sign, COLORS } from '../utils.js'

export function PieChart({ positions }) {
  if (!positions.length) return null
  const total = positions.reduce((s, p) => s + p.amount, 0)
  if (total === 0) return null

  const r = 70, cx = 100, cy = 100
  const circ = 2 * Math.PI * r
  let cumAngle = -90

  const slices = positions.slice(0, 11).map((p, i) => {
    const pct = p.amount / total
    const startAngle = cumAngle
    cumAngle += pct * 360
    return { dash: pct * circ, startAngle, color: COLORS[i % COLORS.length], name: p.name, pct }
  })

  return (
    <div className="pie-wrap">
      <svg viewBox="0 0 200 200" className="pie-svg">
        <circle cx={cx} cy={cy} r={r} fill="none" stroke="#1A1D24" strokeWidth="36" />
        {slices.map((s, i) => (
          <circle
            key={i} cx={cx} cy={cy} r={r}
            fill="none" stroke={s.color} strokeWidth="34"
            strokeDasharray={`${s.dash} ${circ - s.dash}`}
            transform={`rotate(${s.startAngle} ${cx} ${cy})`}
          />
        ))}
      </svg>
      <div className="pie-legend">
        {slices.map((s, i) => (
          <div key={i} className="legend-item">
            <span className="legend-dot" style={{ background: s.color }} />
            <span className="legend-name">{s.name.split('/')[0].slice(0, 18)}</span>
            <span className="legend-pct">{(s.pct * 100).toFixed(1)}%</span>
          </div>
        ))}
      </div>
    </div>
  )
}

export function BarChart({ positions }) {
  const top = [...positions].sort((a, b) => Math.abs(b.pnl) - Math.abs(a.pnl)).slice(0, 8)
  if (!top.length) return null
  const maxAbs = Math.max(...top.map(p => Math.abs(p.pnl)), 0.01)
  const H = 110, W = top.length * 38

  return (
    <svg viewBox={`0 0 ${W} ${H + 30}`} className="bar-svg">
      {top.map((p, i) => {
        const bh   = Math.max((Math.abs(p.pnl) / maxAbs) * H, 2)
        const x    = i * 38 + 5
        const fill = p.pnl >= 0 ? '#00C288' : '#FF4D6D'
        return (
          <g key={i}>
            <rect x={x} y={H - bh} width={28} height={bh} fill={fill} rx="2" />
            <text x={x + 14} y={H + 14} textAnchor="middle" fontSize="8.5" fill="#6B7280">
              {p.name.split('/')[0].split(' ')[0].slice(0, 6)}
            </text>
            <text x={x + 14} y={H - bh - 4} textAnchor="middle" fontSize="7.5" fill={fill}>
              {sign(p.pnl)}{fmt(p.pnl)}
            </text>
          </g>
        )
      })}
      <line x1="0" y1={H} x2={W} y2={H} stroke="#2A2D35" strokeWidth="1" />
    </svg>
  )
}

export function DailyMovers({ positions }) {
  const [mode, setMode] = useState('gainers')

  const withPct = positions
    .filter(p => p.amount > 0 && (p.amount - p.pnl) !== 0)
    .map(p => ({ ...p, pnlPct: (p.pnl / (p.amount - p.pnl)) * 100 }))

  const items = mode === 'gainers'
    ? [...withPct].sort((a, b) => b.pnlPct - a.pnlPct).slice(0, 5)
    : [...withPct].sort((a, b) => a.pnlPct - b.pnlPct).slice(0, 5)

  if (!items.length) return null
  const maxAbs = Math.max(...items.map(p => Math.abs(p.pnlPct)), 0.01)

  return (
    <div className="movers-card">
      <div className="movers-header">
        <span className="movers-title">Performances</span>
        <div className="movers-toggle">
          <button className={mode === 'gainers' ? 'active' : ''} onClick={() => setMode('gainers')}>↗</button>
          <button className={mode === 'losers'  ? 'active' : ''} onClick={() => setMode('losers')}>↘</button>
        </div>
      </div>
      <div className="movers-chart">
        {items.map(p => {
          const pct   = p.pnlPct
          const bh    = Math.max((Math.abs(pct) / maxAbs) * 100, 4)
          const color = pct >= 0 ? '#00C288' : '#FF4D6D'
          const sym   = (p.name || '').split('/')[0].split(' ')[0]
          return (
            <div key={p.positionID} className="mover-col">
              <div className="mover-pct" style={{ color }}>
                {pct >= 0 ? '+' : ''}{pct.toFixed(2)}%
              </div>
              <div className="mover-bar-wrap">
                <div className="mover-bar" style={{ height: bh, background: color + 'CC' }} />
              </div>
              <div className="mover-logo"><Logo symbol={sym} /></div>
              <div className="mover-sym">{sym.slice(0, 7)}</div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
