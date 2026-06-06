export default function PageLayout({ left, center, right, children, extraBar }) {
  const hasBar = left || center || right
  return (
    <>
      {hasBar && (
        <div className="page-bar">
          <div className="flex-1">{left}</div>
          <div className="flex-1 text-center">{center}</div>
          <div className="flex-1 flex justify-end">{right}</div>
        </div>
      )}
      {extraBar}
      <div className="page-content">{children}</div>
    </>
  )
}
