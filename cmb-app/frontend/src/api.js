const BASE = '/api'

// ── IA ────────────────────────────────────────────────────────────────────────

export async function analyserFinances(payload) {
  const r = await fetch(`${BASE}/analyser`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  return r.json()
}

// ── Powens Open Banking ───────────────────────────────────────────────────────

export async function initConnection(redirectUri) {
  const params = new URLSearchParams({ redirect_uri: redirectUri })
  const r = await fetch(`${BASE}/connect/init?${params}`)
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`)
  return r.json() // { webview_url, client_token }
}

export async function exchangeCode(code) {
  const params = new URLSearchParams({ code })
  const r = await fetch(`${BASE}/connect/callback?${params}`)
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`)
  return r.json() // { access_token, expires_in }
}

export async function fetchAccounts(token) {
  const r = await fetch(`${BASE}/connect/accounts?token=${encodeURIComponent(token)}`)
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  return r.json()
}

export async function fetchTransactions(token, { accountId, minDate, limit = 100 } = {}) {
  const params = new URLSearchParams({ token, limit })
  if (accountId) params.set('account_id', accountId)
  if (minDate) params.set('min_date', minDate)
  const r = await fetch(`${BASE}/connect/transactions?${params}`)
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  return r.json()
}

export async function checkConnectionStatus(token) {
  const r = await fetch(`${BASE}/connect/status?token=${encodeURIComponent(token)}`)
  if (!r.ok) return { connected: false }
  return r.json()
}
