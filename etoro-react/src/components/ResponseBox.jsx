export default function ResponseBox({ state, data }) {
  if (!state) return null
  if (state === 'loading') return <div className="resp-box loading">Traitement en cours…</div>
  return (
    <div className={`resp-box ${state}`}>
      <pre>{JSON.stringify(data, null, 2)}</pre>
    </div>
  )
}
