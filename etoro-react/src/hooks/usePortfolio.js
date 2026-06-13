import { useState, useCallback } from 'react'

const CACHE_KEY = 'etoro_portfolio'
export const CACHE_TTL = 5 * 60 * 1000

export function usePortfolio(API) {
  const [data,       setData]       = useState(null)
  const [loading,    setLoading]    = useState(true)
  const [lastUpdate, setLastUpdate] = useState(null)
  const [fromCache,  setFromCache]  = useState(false)
  const [futures,    setFutures]    = useState([])
  const [marketOpen, setMarketOpen] = useState(true)

  const load = useCallback((silent = false) => {
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
  }, [API])

  const loadFutures = useCallback(() => {
    fetch(`${API}/api/futures`)
      .then(r => r.json())
      .then(d => { setFutures(d.futures || []); setMarketOpen(d.marketOpen ?? true) })
      .catch(() => {})
  }, [API])

  const initFromCache = useCallback(() => {
    try {
      const cached = localStorage.getItem(CACHE_KEY)
      if (cached) {
        const { data: d, time } = JSON.parse(cached)
        if (Date.now() - time < CACHE_TTL) {
          setData(d)
          setLastUpdate(new Date(time).toLocaleTimeString('fr-FR'))
          setFromCache(true)
          setLoading(false)
          return true
        }
      }
    } catch {}
    return false
  }, [])

  return { data, loading, lastUpdate, fromCache, futures, marketOpen, load, loadFutures, initFromCache, CACHE_TTL }
}
