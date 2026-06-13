import { useState, useEffect, useMemo } from 'react'
import './App.css'
import { usePortfolio } from './hooks/usePortfolio.js'
import { useConfig } from './hooks/useConfig.js'
import FuturesBanner from './components/FuturesBanner.jsx'
import { ConfirmModal, ConfigModal } from './components/Modals.jsx'
import Portfolio from './tabs/Portfolio.jsx'
import Acheter from './tabs/Acheter.jsx'
import Pending from './tabs/Pending.jsx'
import Stats from './tabs/Stats.jsx'
import Suivi from './tabs/Suivi.jsx'
import Transactions from './tabs/Transactions.jsx'
import { fmt, pnlCls, sign, sortPositions } from './utils.js'

const API        = import.meta.env.VITE_API_URL || ''
const TRACK_USER = import.meta.env.VITE_TRACK_USER || 'Thomaspj'
const CACHE_TTL  = 5 * 60 * 1000

export default function App() {
  const [tab,            setTab]            = useState('portfolio')
  const [sortKey,        setSortKey]        = useState('amount_desc')
  const [confirm,        setConfirm]        = useState(null)
  const [actionResp,     setActionResp]     = useState(null)
  const [actionRespData, setActionRespData] = useState(null)

  const { data, loading, lastUpdate, fromCache, futures, marketOpen,
          load, loadFutures, initFromCache } = usePortfolio(API)

  const { showConfig, configRatio, configSaving, setConfigRatio,
          openConfig, saveRatio, closeConfig } = useConfig(API, load)

  useEffect(() => {
    if (!initFromCache()) load()
    loadFutures()
    const iv = setInterval(() => { load(true); loadFutures() }, CACHE_TTL)
    return () => clearInterval(iv)
  }, [])

  const { positions, pending, equity, cash, totalPnl, invested, sorted, groupList } = useMemo(() => {
    const positions = data?.positions || []
    const pending   = data?.pending   || []
    const equity    = data?.equity    ?? 0
    const cash      = data?.cash      ?? 0
    const totalPnl  = positions.reduce((s, p) => s + p.pnl, 0)
    const invested  = positions.reduce((s, p) => s + p.amount, 0)
    const sorted    = sortPositions(positions, sortKey)
    const groupMap  = sorted.reduce((acc, p) => {
      if (!acc[p.name]) acc[p.name] = []
      acc[p.name].push(p)
      return acc
    }, {})
    return { positions, pending, equity, cash, totalPnl, invested, sorted, groupList: Object.values(groupMap) }
  }, [data, sortKey])

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

      <FuturesBanner futures={futures} />

      <ConfirmModal confirm={confirm} onClose={() => setConfirm(null)} onSell={doSell} onCancel={doCancel} />
      <ConfigModal show={showConfig} ratio={configRatio} saving={configSaving}
                   onRatioChange={setConfigRatio} onSave={saveRatio} onClose={closeConfig} />

      <nav>
        {['portfolio', 'acheter', 'pending', 'stats', 'suivi', 'transactions'].map(t => (
          <button key={t} className={tab === t ? 'active' : ''} onClick={() => setTab(t)}>
            {t === 'portfolio'       ? 'Portfolio'
              : t === 'acheter'      ? 'Acheter'
              : t === 'pending'      ? 'En Attente'
              : t === 'stats'        ? 'Stats'
              : t === 'suivi'        ? 'Suivi'
              : 'Transactions'}
          </button>
        ))}
      </nav>

      {tab === 'portfolio'    && <Portfolio positions={positions} groupList={groupList} loading={loading}
                                             sortKey={sortKey} setSortKey={setSortKey}
                                             actionResp={actionResp} actionRespData={actionRespData}
                                             setConfirm={setConfirm} invested={invested} cash={cash} totalPnl={totalPnl} />}
      {tab === 'acheter'      && <Acheter API={API} load={load} />}
      {tab === 'pending'      && <Pending pending={pending} loading={loading}
                                          actionResp={actionResp} actionRespData={actionRespData}
                                          setConfirm={setConfirm} />}
      {tab === 'stats'        && <Stats positions={positions} sorted={sorted} equity={equity}
                                         totalPnl={totalPnl} cash={cash} loading={loading} />}
      {tab === 'suivi'        && <Suivi API={API} TRACK_USER={TRACK_USER} />}
      {tab === 'transactions' && <Transactions API={API} />}
    </div>
  )
}
