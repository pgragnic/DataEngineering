import { useEffect, useRef, useState } from "react"
import { MapContainer, TileLayer, Marker, Popup, Polyline, useMap } from "react-leaflet"
import L from "leaflet"
import { AUDITEUR } from "../mockData"

function FlyTo({ coords }) {
  const map = useMap()
  useEffect(() => {
    if (coords) map.flyTo([coords.lat, coords.lng], 15, { duration: 1.2 })
  }, [coords])
  return null
}

function SaveMapRef({ mapRef }) {
  const map = useMap()
  useEffect(() => { mapRef.current = map }, [map])
  return null
}

delete L.Icon.Default.prototype._getIconUrl

function makeIcon(color) {
  return L.divIcon({
    className: "",
    html: `<div style="
      width:16px;height:16px;border-radius:50%;
      background:${color};border:2.5px solid white;
      box-shadow:0 1px 5px rgba(0,0,0,.5);
    "></div>`,
    iconSize: [16, 16],
    iconAnchor: [8, 8],
    popupAnchor: [0, -10],
  })
}

function makeStartIcon(color) {
  return L.divIcon({
    className: "",
    html: `<div style="
      width:20px;height:20px;border-radius:50%;
      background:${color};border:3px solid white;
      box-shadow:0 2px 8px rgba(0,0,0,.5);
      display:flex;align-items:center;justify-content:center;
      font-size:9px;color:white;font-weight:700;
    ">M</div>`,
    iconSize: [20, 20],
    iconAnchor: [10, 10],
    popupAnchor: [0, -12],
  })
}

const STATUT_COLOR = {
  "TERMINÉ":   "#22C55E",
  "PROCHAIN":  "#3B82F6",
  "PLANIFIÉ":  "#9CA3AF",
}

const STATUT_COLOR_AGILE = {
  "TERMINÉ":   "#4CDFB2",
  "PROCHAIN":  "#5D93C1",
  "PLANIFIÉ":  "#999999",
}

const STATUT_COLOR_ARIA = {
  "TERMINÉ":   "#22C55E",
  "PROCHAIN":  "#4B87F8",
  "PLANIFIÉ":  "#9CA3AF",
}

const ROUTE_COLOR = "#F97316"

// Profil OSRM selon le mode de transport
const OSRM_PROFILE = {
  voiture: "driving",
  TC:      "driving",  // pas de profil TC dans OSRM public — on utilise driving
  velo:    "cycling",
  pied:    "foot",
}

async function fetchOSRMRoute(waypoints, mode) {
  const profile = OSRM_PROFILE[mode] || "driving"
  const coordStr = waypoints.map(([lat, lng]) => `${lng},${lat}`).join(";")
  const url = `https://router.project-osrm.org/route/v1/${profile}/${coordStr}?overview=full&geometries=geojson`
  const res = await fetch(url)
  const json = await res.json()
  const geom = json.routes?.[0]?.geometry?.coordinates
  if (!geom) return null
  // GeoJSON : [lng, lat] → Leaflet : [lat, lng]
  return geom.map(([lng, lat]) => [lat, lng])
}

export default function MapCard({ audits, theme, compact = false, focusCoords = null, transport = "voiture" }) {
  const mapRef = useRef(null)
  const ag = theme === "agile"
  const ar = theme === "aria"
  const colors = ag ? STATUT_COLOR_AGILE : ar ? STATUT_COLOR_ARIA : STATUT_COLOR
  const brandColor = ag ? "#5D93C1" : ar ? "#4B87F8" : "#2563EB"

  const sorted = [...audits]
    .filter(a => a.coords)
    .sort((a, b) => a.heureMin - b.heureMin)

  const straightPoints = [
    [AUDITEUR.coords.lat, AUDITEUR.coords.lng],
    ...sorted.map(a => [a.coords.lat, a.coords.lng]),
  ]

  const [routeGeometry, setRouteGeometry] = useState(null)

  // Fetch OSRM route when waypoints or transport mode change
  useEffect(() => {
    if (straightPoints.length < 2) return
    let cancelled = false
    fetchOSRMRoute(straightPoints, transport)
      .then(geom => { if (!cancelled && geom) setRouteGeometry(geom) })
      .catch(() => {}) // fallback silencieux sur ligne droite
    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(straightPoints), transport])

  const routePoints = routeGeometry || straightPoints

  const allLats = straightPoints.map(p => p[0])
  const allLngs = straightPoints.map(p => p[1])
  const center = [
    allLats.reduce((s, v) => s + v, 0) / allLats.length,
    allLngs.reduce((s, v) => s + v, 0) / allLngs.length,
  ]

  const mapHeight = compact ? "340px" : "240px"

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
      {!compact && (
        <div className={`px-4 py-2 flex items-center justify-between border-b border-gray-100 ${ag ? "bg-[#494949]" : ar ? "bg-[#1C2B4A]" : "bg-gray-50"}`}>
          <span className={`text-[10px] font-bold uppercase tracking-widest ${ag || ar ? "text-white" : "text-gray-500"}`}>
            Parcours du jour
          </span>
          <div className="flex items-center gap-3 text-[10px]">
            {["TERMINÉ", "PROCHAIN", "PLANIFIÉ"].map(s => (
              <span key={s} className="flex items-center gap-1" style={{ color: colors[s] }}>
                <span className="inline-block w-2 h-2 rounded-full" style={{ background: colors[s] }} />
                {s}
              </span>
            ))}
          </div>
        </div>
      )}
      <MapContainer
        center={center}
        zoom={11}
        style={{ height: mapHeight, width: "100%" }}
        zoomControl={true}
        scrollWheelZoom={true}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <SaveMapRef mapRef={mapRef} />
        <FlyTo coords={focusCoords} />

        {/* Halo blanc derrière la ligne */}
        <Polyline
          positions={routePoints}
          pathOptions={{ color: "white", weight: 9, opacity: 0.6, lineCap: "round", lineJoin: "round" }}
        />
        {/* Ligne principale orange */}
        <Polyline
          positions={routePoints}
          pathOptions={{ color: ROUTE_COLOR, weight: 5, opacity: 1, lineCap: "round", lineJoin: "round" }}
        />

        {/* Marqueur départ */}
        <Marker position={[AUDITEUR.coords.lat, AUDITEUR.coords.lng]} icon={makeStartIcon(brandColor)}>
          <Popup>
            <div className="text-xs font-medium">{AUDITEUR.nom}</div>
            <div className="text-[11px] text-gray-500">{AUDITEUR.localisation} — départ</div>
          </Popup>
        </Marker>

        {/* Marqueurs des sites */}
        {sorted.map((audit) => (
          <Marker
            key={audit.id}
            position={[audit.coords.lat, audit.coords.lng]}
            icon={makeIcon(colors[audit.statut] || "#9CA3AF")}
          >
            <Popup>
              <div className="text-xs font-semibold">{audit.site}</div>
              <div className="text-[11px] text-gray-600">{audit.heure} · {audit.dureeMin} min</div>
              {audit.trajet.voiture !== "—" && (
                <div className="text-[11px] text-gray-500 mt-0.5">🚗 {audit.trajet.voiture}</div>
              )}
              <div className="text-[11px] mt-0.5" style={{ color: colors[audit.statut] }}>
                {audit.statut}
              </div>
            </Popup>
          </Marker>
        ))}
      </MapContainer>

      {/* Légende compacte */}
      {compact && (
        <div className="px-3 py-2 flex items-center gap-3 border-t border-gray-100 bg-gray-50">
          <span className="flex items-center gap-1 text-[10px] text-gray-500">
            <span className="inline-block w-5 h-1 rounded-full" style={{ background: ROUTE_COLOR }} />
            Itinéraire {!routeGeometry && <span className="opacity-50">(calcul…)</span>}
          </span>
          {["TERMINÉ", "PROCHAIN", "PLANIFIÉ"].map(s => (
            <span key={s} className="flex items-center gap-1 text-[10px]" style={{ color: colors[s] }}>
              <span className="inline-block w-2 h-2 rounded-full" style={{ background: colors[s] }} />
              {s}
            </span>
          ))}
          <button
            onClick={() => mapRef.current?.flyToBounds(L.latLngBounds(straightPoints), { padding: [30, 30], duration: 1 })}
            className="ml-auto text-[10px] text-gray-400 hover:text-gray-700 border border-gray-200 rounded px-2 py-0.5 bg-white hover:bg-gray-100 transition-colors"
            title="Voir tout le parcours"
          >
            ⊖ Tout voir
          </button>
        </div>
      )}
    </div>
  )
}
