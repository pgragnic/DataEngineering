"""
Persistance de l'état du portfolio entre deux cycles de polling.
"""

from __future__ import annotations

import json
import logging
import os

from etoro_client import PortfolioSnapshot

logger = logging.getLogger(__name__)


def load(path: str) -> PortfolioSnapshot | None:
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return PortfolioSnapshot.from_dict(json.load(f))
    except Exception as exc:
        logger.warning("Impossible de lire l'état sauvegardé (%s), reset.", exc)
        return None


def save(path: str, snapshot: PortfolioSnapshot) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(snapshot.to_dict(), f, indent=2, ensure_ascii=False)
