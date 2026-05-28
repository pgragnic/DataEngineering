"""
Client eToro (API publique non officielle).

Endpoints confirmés par inspection réseau du navigateur sur eToro :
  1. CID lookup  : GET /api/logininfo/v1.1/users/{username}
  2. Portfolio   : GET /sapi/trade-data-real/live/public/portfolios
  3. Historique  : GET /sapi/trade-data-real/history/public/credit/flat
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
        self._cid: str | None = None
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def get_snapshot(self) -> PortfolioSnapshot:
        cid = self._get_cid()
        raw = self._fetch_portfolio(cid)
        positions = self._parse_positions(raw)
        equity = float(raw.get("Equity", raw.get("equity", 0.0)))

        return PortfolioSnapshot(
            username=self.username,
            fetched_at=datetime.now(timezone.utc).isoformat(),
            positions=positions,
            equity=equity,
        )

    # ------------------------------------------------------------------
    # CID resolution
    # ------------------------------------------------------------------

    def _get_cid(self) -> str:
        """Résout le CID numérique à partir du pseudo eToro (mis en cache)."""
        if self._cid:
            return self._cid

        url = f"{_BASE}/api/logininfo/v1.1/users/{self.username}"
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # La réponse contient "realCID", "cid" ou "customerId"
        cid = (
            data.get("realCID")
            or data.get("cid")
            or data.get("customerId")
            or data.get("CID")
        )
        if not cid:
            raise ValueError(f"CID introuvable dans la réponse : {data}")

        self._cid = str(cid)
        logger.info("CID de %s : %s", self.username, self._cid)
        return self._cid

    # ------------------------------------------------------------------
    # Portfolio live
    # ------------------------------------------------------------------

    def _fetch_portfolio(self, cid: str) -> dict[str, Any]:
        url = f"{_BASE}/sapi/trade-data-real/live/public/portfolios"
        params = {
            "format": "json",
            "cid": cid,
            "client_request_id": str(uuid.uuid4()),
        }
        resp = self._session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_positions(self, raw: dict) -> list[Position]:
        positions = []
        # L'API retourne les positions dans "PublicPortfolio" → "Positions"
        portfolio = raw.get("PublicPortfolio") or raw
        items = (
            portfolio.get("Positions")
            or portfolio.get("AggregatedPositions")
            or []
        )
        for item in items:
            try:
                pos = Position(
                    instrument=str(item.get("InstrumentID", item.get("Instrument", ""))),
                    direction="buy" if item.get("IsBuy", True) else "sell",
                    amount=float(item.get("InvestedAmount", item.get("Amount", 0))),
                    open_rate=float(item.get("OpenRate", 0)),
                    current_rate=float(item.get("CurrentRate", 0)),
                    profit_pct=float(item.get("NetProfit", item.get("Profit", 0))),
                    leverage=int(item.get("Leverage", 1)),
                    opened_at=str(item.get("OpenDateTime", "")),
                    position_id=str(
                        item.get("PositionID", item.get("CopyPositionID", ""))
                    ),
                )
                positions.append(pos)
            except Exception as exc:
                logger.debug("Position ignorée (%s) : %s", exc, item)
        return positions
