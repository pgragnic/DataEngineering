"""
Agent de suivi ThomasPJ — point d'entrée.

Usage :
    python main.py            # tourne en boucle (POLL_INTERVAL_SECONDS)
    python main.py --once     # un seul cycle puis quitte
"""

from __future__ import annotations

import argparse
import logging
import time

import config
import state as state_store
from agent import analyse, compute_diff, has_changes
from etoro_client import EtoroClient
from notifier import notify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("trader-agent")


def run_cycle(client: EtoroClient) -> None:
    logger.info("Récupération du portfolio de %s…", config.ETORO_USERNAME)

    try:
        current = client.get_snapshot()
    except Exception as exc:
        logger.error("Impossible de récupérer le portfolio eToro : %s", exc)
        return

    previous = state_store.load(config.STATE_FILE)

    if previous is None:
        logger.info("Premier cycle — état initial sauvegardé. Aucune alerte envoyée.")
        state_store.save(config.STATE_FILE, current)
        return

    diff = compute_diff(previous, current)

    if not has_changes(diff):
        logger.info(
            "Aucun changement détecté (%d positions).", len(current.positions)
        )
        state_store.save(config.STATE_FILE, current)
        return

    logger.info(
        "Changements : %d ouverture(s), %d fermeture(s), %d modif(s).",
        len(diff.opened),
        len(diff.closed),
        len(diff.modified),
    )

    alert = analyse(diff, current)
    notify(alert)
    state_store.save(config.STATE_FILE, current)


def main() -> None:
    parser = argparse.ArgumentParser(description="Agent IA — trader ThomasPJ")
    parser.add_argument(
        "--once", action="store_true", help="Exécute un seul cycle puis quitte"
    )
    args = parser.parse_args()

    client = EtoroClient(config.ETORO_USERNAME)

    if args.once:
        run_cycle(client)
        return

    logger.info(
        "Démarrage de l'agent (intervalle : %ds)…", config.POLL_INTERVAL_SECONDS
    )
    while True:
        run_cycle(client)
        time.sleep(config.POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
