export const fmt = n =>
  new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n)

export const pnlCls = n => n > 0 ? 'pos' : n < 0 ? 'neg' : ''

export const sign = n => n > 0 ? '+' : ''

export const COLORS = [
  '#00C288', '#00A878', '#008E60', '#007448',
  '#58B8FF', '#3A94E0', '#1C70C0',
  '#FFB020', '#E09000',
  '#C878FF', '#A050E0',
]

export const SORTS = [
  { key: 'amount_desc', label: 'Valeur ↓' },
  { key: 'amount_asc',  label: 'Valeur ↑' },
  { key: 'pnl_desc',   label: 'P&L ↓' },
  { key: 'pnl_asc',    label: 'P&L ↑' },
  { key: 'name_asc',   label: 'A–Z' },
]

export function sortPositions(positions, key) {
  return [...positions].sort((a, b) => {
    if (key === 'amount_desc') return b.amount - a.amount
    if (key === 'amount_asc')  return a.amount - b.amount
    if (key === 'pnl_desc')    return b.pnl - a.pnl
    if (key === 'pnl_asc')     return a.pnl - b.pnl
    if (key === 'name_asc')    return a.name.localeCompare(b.name)
    return 0
  })
}
