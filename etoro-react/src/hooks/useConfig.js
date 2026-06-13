import { useState, useCallback } from 'react'

export function useConfig(API, onSaved) {
  const [showConfig,   setShowConfig]   = useState(false)
  const [configRatio,  setConfigRatio]  = useState('')
  const [configSaving, setConfigSaving] = useState(false)

  const openConfig = useCallback(() => {
    fetch(`${API}/api/config`)
      .then(r => r.json())
      .then(d => { setConfigRatio(String(d.ratio ?? 1)); setShowConfig(true) })
      .catch(() => { setConfigRatio('1'); setShowConfig(true) })
  }, [API])

  const saveRatio = useCallback(() => {
    const r = parseFloat(configRatio)
    if (isNaN(r) || r <= 0) return
    setConfigSaving(true)
    fetch(`${API}/api/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ratio: r }),
    })
      .then(() => { setShowConfig(false); setConfigSaving(false); if (onSaved) onSaved() })
      .catch(() => setConfigSaving(false))
  }, [API, configRatio, onSaved])

  const closeConfig = useCallback(() => setShowConfig(false), [])

  return { showConfig, configRatio, configSaving, setConfigRatio, openConfig, saveRatio, closeConfig }
}
