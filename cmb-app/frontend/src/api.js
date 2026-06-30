const BASE = '/api'

export async function analyserFinances(payload) {
  const r = await fetch(`${BASE}/analyser`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  return r.json()
}
