import { useState } from 'react'
import ResponseBox from '../components/ResponseBox.jsx'

export default function Acheter({ API, load }) {
  const [searchQuery,   setSearchQuery]   = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [searching,     setSearching]     = useState(false)
  const [selected,      setSelected]      = useState(null)
  const [buyPct,        setBuyPct]        = useState('')
  const [buyResp,       setBuyResp]       = useState(null)
  const [buyRespData,   setBuyRespData]   = useState(null)

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

  return (
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
  )
}
