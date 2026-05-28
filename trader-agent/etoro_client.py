"""
Client eToro (API publique non officielle).

Les endpoints utilisés correspondent aux appels réseau effectués par le
navigateur lors de la consultation de https://www.etoro.com/people/{username}.
Si eToro modifie ses routes, inspecter l'onglet Réseau du navigateur pour
trouver les nouveaux chemins.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE = "https://www.etoro.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Referer": f"{_BASE}/",
}


@dataclass
class Position:
    instrument: str
    direction: str      # "buy" | "sell"
    amount: float
    open_rate: float
    current_rate: float
    profit_pct: float
    leverage: int
    opened_at: str
    position_id: str

    def to_dict(self) -> dict:
        return self.__dict__

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        return cls(**d)


@dataclass
class PortfolioSnapshot:
    username: str
    fetched_at: str
    positions: list[Position] = field(default_factory=list)
    equity: float = 0.0
    gain_pct: float = 0.0

    def to_dict(self) -> dict:
        return {
            "username": self.username,
            "fetched_at": self.fetched_at,
            "equity": self.equity,
            "gain_pct": self.gain_pct,
            "positions": [p.to_dict() for p in self.positions],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PortfolioSnapshot":
        snap = cls(
            username=d["username"],
            fetched_at=d["fetched_at"],
            equity=d.get("equity", 0.0),
            gain_pct=d.get("gain_pct", 0.0),
        )
        snap.positions = [Position.from_dict(p) for p in d.get("positions", [])]
        return snap


class EtoroClient:
    def __init__(self, username: str):
        self.username = username
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get_snapshot(self) -> PortfolioSnapshot:
        """Récupère le portfolio public actuel du trader."""
        from datetime import datetime, timezone

        raw = self._fetch_portfolio()
        positions = self._parse_positions(raw)
        stats = self._fetch_stats()

        return PortfolioSnapshot(
            username=self.username,
            fetched_at=datetime.now(timezone.utc).isoformat(),
            positions=positions,
            equity=stats.get("equity", 0.0),
            gain_pct=stats.get("gain", 0.0),
        )

    # ------------------------------------------------------------------
    # Internal API calls
    # ------------------------------------------------------------------

    def _fetch_portfolio(self) -> dict[str, Any]:
        """
        Endpoint public portfolio eToro.
        Retourne les positions ouvertes du trader (profil public activé).
        """
        url = f"{_BASE}/api/logininfo/v1.1/users/{self.username}/portfolio/public"
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def _fetch_stats(self) -> dict[str, Any]:
        """Récupère les statistiques publiques (gain, équité)."""
        url = f"{_BASE}/sapi/userstats/gain"
        params = {"username": self.username, "period": "CurrYear"}
        try:
            resp = self._session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            return {
                "gain": data.get("gain", 0.0),
                "equity": data.get("equity", 0.0),
            }
        except Exception as exc:
            logger.warning("Impossible de récupérer les stats : %s", exc)
            return {}

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_positions(self, raw: dict) -> list[Position]:
        positions = []
        # eToro retourne les positions dans raw["AggregatedPositions"] ou "Positions"
        items = raw.get("AggregatedPositions") or raw.get("Positions") or []
        for item in items:
            try:
                pos = Position(
                    instrument=item.get("InstrumentID") or item.get("Instrument", ""),
                    direction="buy" if item.get("IsBuy", True) else "sell",
                    amount=float(item.get("InvestedAmount", 0)),
                    open_rate=float(item.get("OpenRate", 0)),
                    current_rate=float(item.get("CurrentRate", 0)),
                    profit_pct=float(item.get("NetProfit", 0)),
                    leverage=int(item.get("Leverage", 1)),
                    opened_at=str(item.get("OpenDateTime", "")),
                    position_id=str(item.get("PositionID", item.get("CopyPositionID", ""))),
                )
                positions.append(pos)
            except Exception as exc:
                logger.debug("Position ignorée (%s): %s", exc, item)
        return positions
