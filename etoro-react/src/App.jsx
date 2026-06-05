import { useState, useEffect } from 'react'
import './App.css'

const CACHE_KEY = 'etoro_portfolio'
const CACHE_TTL = 5 * 60 * 1000

const fmt = n => new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n)
const pnlCls = n => n > 0 ? 'pos' : n < 0 ? 'neg' : ''
const sign = n => n > 0 ? '+' : ''

const COLORS = [
  '#00C288', '#00A878', '#008E60', '#007448',
  '#58B8FF', '#3A94E0', '#1C70C0',
  '#FFB020', '#E09000',
  '#C878FF', '#A050E0',
]

const SORTS = [
  { key: 'amount_desc', label: 'Valeur ↓' },
  { key: 'amount_asc',  label: 'Valeur ↑' },
  { key: 'pnl_desc',   label: 'P&L ↓' },
  { key: 'pnl_asc',    label: 'P&L ↑' },
  { key: 'name_asc',   label: 'A–Z' },
]

function sortPositions(positions, key) {
  return [...positions].sort((a, b) => {
    if (key === 'amount_desc') return b.amount - a.amount
    if (key === 'amount_asc')  return a.amount - b.amount
    if (key === 'pnl_desc')    return b.pnl - a.pnl
    if (key === 'pnl_asc')     return a.pnl - b.pnl
    if (key === 'name_asc')    return a.name.localeCompare(b.name)
    return 0
  })
}

function Logo({ symbol }) {
  const [err, setErr] = useState(false)
  const sym = (symbol || '').split('/')[0].split(' ')[0].toLowerCase()
  if (err) {
    return (
      <div className="pos-logo-wrap">
        <div className="pos-logo-init">{(symbol || '').slice(0, 3).toUpperCase()}</div>
      </div>
    )
  }
  return (
    <div className="pos-logo-wrap">
      <img
        className="pos-logo"
        src={`https://etoro-cdn.etorostatic.com/market-avatars/${sym}/150x150.png`}
        onError={() => setErr(true)}
        alt={symbol}
      />
    </div>
  )
}

function FuturesBanner({ futures, marketOpen }) {
  if (marketOpen || !futures.length) return null
  return (
    <div className="futures-banner">
      <div className="futures-scroll">
        {futures.map((f, i) => (
          <div key={i} className="futures-card">
            <div className="futures-name">{f.name}</div>
            <div className={`futures-price ${pnlCls(f.changePct)}`}>
              {f.price ? `$${fmt(f.price)}` : '—'}
            </div>
            {f.changePct != null && (
              <div className={`futures-chg ${pnlCls(f.changePct)}`}>
                {sign(f.changePct)}{f.changePct.toFixed(2)}%
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

function GroupedCard({ group, setConfirm }) {
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

function TrackCard({ p }) {
  const sym = (p.name || '').split('/')[0].split(' ')[0]
  return (
    <div className="card">
      <div className="card-row">
        <div className="card-left">
          <div className="card-logo-name">
            <Logo symbol={sym} />
            <div>
              <div className="card-name">{p.name}</div>
              <div className="card-sub">{p.isBuy ? '▲ Long' : '▼ Short'}</div>
            </div>
          </div>
        </div>
        <div className="card-right">
          <div className="card-amount">{fmt(p.amount)}%</div>
          <div className={`card-pnl ${pnlCls(p.pnl)}`}>{sign(p.pnl)}{fmt(Math.abs(p.pnl))}%</div>
        </div>
      </div>
    </div>
  )
}

function ResponseBox({ state, data }) {
  if (!state) return null
  if (state === 'loading') return <div className="resp-box loading">Traitement en cours…</div>
  return (
    <div className={`resp-box ${state}`}>
      <pre>{JSON.stringify(data, null, 2)}</pre>
    </div>
  )
}

function PieChart({ positions }) {
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

function BarChart({ positions }) {
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

function DailyMovers({ positions }) {
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

export default function App() {
  const [tab, setTab]               = useState('portfolio')
  const [data, setData]             = useState(null)
  const [loading, setLoading]       = useState(true)
  const [lastUpdate, setLastUpdate] = useState(null)
  const [fromCache, setFromCache]   = useState(false)
  const [sortKey, setSortKey]       = useState('amount_desc')
  const [confirm, setConfirm]       = useState(null)
  const [futures, setFutures]       = useState([])
  const [marketOpen, setMarketOpen] = useState(true)

  const [searchQuery,   setSearchQuery]   = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [searching,     setSearching]     = useState(false)
  const [selected,      setSelected]      = useState(null)
  const [buyPct,        setBuyPct]        = useState('')
  const [buyResp,       setBuyResp]       = useState(null)
  const [buyRespData,   setBuyRespData]   = useState(null)

  const [actionResp,     setActionResp]     = useState(null)
  const [actionRespData, setActionRespData] = useState(null)

  const [trackData,    setTrackData]    = useState(null)
  const [trackLoading, setTrackLoading] = useState(false)

  const [showConfig,   setShowConfig]   = useState(false)
  const [configRatio,  setConfigRatio]  = useState('')
  const [configSaving, setConfigSaving] = useState(false)

  const API        = import.meta.env.VITE_API_URL || ''
  const TRACK_USER = 'Thomaspj'

  const load = (silent = false) => {
    if (!silent) setLoading(true)
    fetch(`${API}/api/portfolio`)
      .then(r => r.json())
      .then(d => {
        setData(d)
        setLastUpdate(new Date().toLocaleTimeString('fr-FR'))
        setFromCache(false)
        setLoading(false)
        try { localStorage.setItem(CACHE_KEY, JSON.stringify({ data: d, time: Date.now() })) } catch {}
      })
      .catch(() => setLoading(false))
  }

  const loadFutures = () => {
    fetch(`${API}/api/futures`)
      .then(r => r.json())
      .then(d => { setFutures(d.futures || []); setMarketOpen(d.marketOpen ?? true) })
      .catch(() => {})
  }

  const loadTrack = () => {
    setTrackLoading(true)
    fetch(`${API}/api/track/${TRACK_USER}`)
      .then(r => r.json())
      .then(d => { setTrackData(d); setTrackLoading(false) })
      .catch(() => setTrackLoading(false))
  }

  const openConfig = () => {
    fetch(`${API}/api/config`)
      .then(r => r.json())
      .then(d => { setConfigRatio(String(d.ratio ?? 1)); setShowConfig(true) })
      .catch(() => { setConfigRatio('1'); setShowConfig(true) })
  }

  const saveRatio = () => {
    const r = parseFloat(configRatio)
    if (isNaN(r) || r <= 0) return
    setConfigSaving(true)
    fetch(`${API}/api/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ratio: r }),
    })
      .then(() => { setShowConfig(false); setConfigSaving(false); load() })
      .catch(() => setConfigSaving(false))
  }

  useEffect(() => {
    try {
      const cached = localStorage.getItem(CACHE_KEY)
      if (cached) {
        const { data: d, time } = JSON.parse(cached)
        if (Date.now() - time < CACHE_TTL) {
          setData(d)
          setLastUpdate(new Date(time).toLocaleTimeString('fr-FR'))
          setFromCache(true)
          setLoading(false)
        }
      }
    } catch {}
    load()
    loadFutures()
    const iv = setInterval(() => { load(true); loadFutures() }, CACHE_TTL)
    return () => clearInterval(iv)
  }, [])

  useEffect(() => {
    if (tab === 'suivi' && !trackData && !trackLoading) loadTrack()
  }, [tab])

  const positions = data?.positions || []
  const pending   = data?.pending   || []
  const equity    = data?.equity    ?? 0
  const cash      = data?.cash      ?? 0
  const totalPnl  = positions.reduce((s, p) => s + p.pnl, 0)
  const invested  = positions.reduce((s, p) => s + p.amount, 0)
  const sorted    = sortPositions(positions, sortKey)

  const groupMap = sorted.reduce((acc, p) => {
    if (!acc[p.name]) acc[p.name] = []
    acc[p.name].push(p)
    return acc
  }, {})
  const groupList = Object.values(groupMap)

  async function doSearch(q) {
    if (!q.trim()) { setSearchResults([]); return }
    setSearching(true)
    try {
      const r = await fetch(`${API}/api/search/${encodeURIComponent(q.trim().toUpperCase())}`)
      const d = await r.json()
      setSearchResults(d.error ? [] : [{ ...d, symbol: q.trim().toUpperCase() }])
    } catch { setSearchResults([]) }
    setSearching(false)
  }

  async function doBuy() {
    if (!selected || !buyPct) return
    setBuyResp('loading'); setBuyRespData(null)
    try {
      const r = await fetch(`${API}/api/buy`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ symbol: selected.symbol, pct: parseFloat(buyPct) }),
      })
      const d = await r.json()
      setBuyResp(d.error ? 'err' : 'ok'); setBuyRespData(d)
      if (!d.error) load()
    } catch (e) { setBuyResp('err'); setBuyRespData({ error: e.message }) }
  }

  async function doSell(pid) {
    setConfirm(null)
    setActionResp('loading'); setActionRespData(null)
    try {
      const r = await fetch(`${API}/api/sell/${pid}`, { method: 'POST' })
      const d = await r.json()
      setActionResp(d.error ? 'err' : 'ok'); setActionRespData(d)
      if (!d.error) load()
    } catch (e) { setActionResp('err'); setActionRespData({ error: e.message }) }
  }

  async function doCancel(oid) {
    setConfirm(null)
    setActionResp('loading'); setActionRespData(null)
    try {
      const r = await fetch(`${API}/api/cancel/${oid}`, { method: 'DELETE' })
      const d = await r.json()
      setActionResp(d.error ? 'err' : 'ok'); setActionRespData(d)
      if (!d.error) load()
    } catch (e) { setActionResp('err'); setActionRespData({ error: e.message }) }
  }

  return (
    <div className="app">
      <header>
        <button className="btn-config" onClick={openConfig} aria-label="Paramètres">⚙</button>
        <div className="header-title">eToro Portfolio</div>
        <div className="header-equity">${fmt(equity)}</div>
        <div className="header-sub">
          <span className={pnlCls(totalPnl)}>{sign(totalPnl)}${fmt(totalPnl)}</span>
          {lastUpdate && (
            <span className={`header-time ${fromCache ? 'update-cache' : 'update-live'}`}>
              {' · '}{fromCache ? '▷' : '●'} MàJ {lastUpdate}
            </span>
          )}
        </div>
      </header>

      <FuturesBanner futures={futures} marketOpen={marketOpen} />

      <nav>
        {['portfolio', 'acheter', 'pending', 'stats', 'suivi'].map(t => (
          <button key={t} className={tab === t ? 'active' : ''} onClick={() => setTab(t)}>
            {t === 'portfolio' ? 'Portfolio'
              : t === 'acheter' ? 'Acheter'
              : t === 'pending' ? 'En Attente'
              : t === 'stats'   ? 'Stats'
              : 'Suivi'}
          </button>
        ))}
      </nav>

      {showConfig && (
        <div className="modal-overlay" onClick={() => setShowConfig(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-title">Ratio d'affichage</div>
            <div className="modal-name" style={{marginBottom:16}}>
              Multiplie toutes les valeurs monétaires.<br/>
              Exemple : 0.691 pour afficher ~69% du réel.
            </div>
            <input
              type="number"
              step="0.001"
              min="0.001"
              value={configRatio}
              onChange={e => setConfigRatio(e.target.value)}
              style={{textAlign:'center', fontSize:'1.2rem', fontWeight:700}}
            />
            <div className="modal-buttons">
              <button className="btn-modal-back" onClick={() => setShowConfig(false)}>Annuler</button>
              <button className="btn-modal-sell" onClick={saveRatio} disabled={configSaving}>
                {configSaving ? '…' : 'Enregistrer'}
              </button>
            </div>
          </div>
        </div>
      )}

      {confirm && (
        <div className="modal-overlay" onClick={() => setConfirm(null)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-title">
              {confirm.type === 'sell' ? 'Vendre cette position ?' : 'Annuler cet ordre ?'}
            </div>
            <div className="modal-name">{confirm.name}</div>
            {confirm.amount != null && <div className="modal-amount">${fmt(confirm.amount)}</div>}
            <div className="modal-buttons">
              <button className="btn-modal-back" onClick={() => setConfirm(null)}>Retour</button>
              <button
                className={confirm.type === 'sell' ? 'btn-modal-sell' : 'btn-modal-cancel'}
                onClick={() => confirm.type === 'sell' ? doSell(confirm.pid) : doCancel(confirm.oid)}
              >
                {confirm.type === 'sell' ? 'Vendre' : 'Confirmer'}
              </button>
            </div>
          </div>
        </div>
      )}

      {tab === 'portfolio' && (
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
      )}

      {tab === 'acheter' && (
        <div className="tab-content">
          <input
            type="text"
            placeholder="Rechercher un symbole (ex: AAPL)"
            value={searchQuery}
            onChange={e => { setSearchQuery(e.target.value); doSearch(e.target.value) }}
          />
          {searching && <div className="spinner">Recherche…</div>}
          {searchResults.map(r => (
            <div
              key={r.instrumentId}
              className={`card selectable${selected?.instrumentId === r.instrumentId ? ' selected' : ''}`}
              onClick={() => setSelected(r)}
            >
              <div className="card-name">{r.symbol}</div>
              <div className="card-sub">{r.name}</div>
            </div>
          ))}
          {selected && (
            <>
              <div className="selected-banner">
                Sélectionné : <strong>{selected.symbol}</strong> — {selected.name}
              </div>
              <input
                type="number" min="0.1" max="100" step="0.1"
                placeholder="% du portefeuille (ex: 5)"
                value={buyPct}
                onChange={e => setBuyPct(e.target.value)}
              />
              <button
                className="btn-buy"
                onClick={() => {
                  if (!buyPct || parseFloat(buyPct) <= 0) return
                  if (!window.confirm(`Acheter ${buyPct}% du portefeuille en ${selected.symbol} ?`)) return
                  doBuy()
                }}
              >Acheter {buyPct ? `${buyPct}%` : ''}</button>
            </>
          )}
          <ResponseBox state={buyResp} data={buyRespData} />
        </div>
      )}

      {tab === 'pending' && (
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
      )}

      {tab === 'stats' && (
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
      )}

      {tab === 'suivi' && (
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
                const sym = (h.name || '').split('/')[0].split(' ')[0]
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
      )}
    </div>
  )
}
