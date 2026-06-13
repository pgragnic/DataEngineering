import { fmt, pnlCls, sign } from '../utils.js'

export default function FuturesBanner({ futures }) {
  if (!futures.length) return null
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
