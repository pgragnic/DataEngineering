import { useState } from 'react'

export default function Logo({ symbol }) {
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
