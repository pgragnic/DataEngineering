"""
Envoi des alertes via Telegram et/ou email.
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

from config import (
    ALERT_EMAIL_TO,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USER,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
)

logger = logging.getLogger(__name__)

_RISK_EMOJI = {"faible": "🟢", "modéré": "🟡", "élevé": "🔴"}


def _format_message(alert) -> str:  # alert: TradeAlert
    diff = alert.diff
    opened = len(diff.opened)
    closed = len(diff.closed)
    modified = len(diff.modified)

    emoji = _RISK_EMOJI.get(alert.risk_level, "⚪")

    parts = [
        f"📊 *Alerte ThomasPJ — eToro*",
        f"Risque : {emoji} *{alert.risk_level.upper()}*",
        "",
        f"*Résumé :* {alert.summary}",
        "",
        f"*Mouvements :* {opened} ouverture(s) · {closed} fermeture(s) · {modified} modif(s)",
        "",
        f"*Analyse :*\n{alert.analysis}",
        "",
        f"*À surveiller :*\n{alert.recommendation}",
    ]
    return "\n".join(parts)


def send_telegram(alert) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram non configuré, notification ignorée.")
        return False

    text = _format_message(alert)
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Alerte Telegram envoyée.")
        return True
    except Exception as exc:
        logger.error("Échec Telegram : %s", exc)
        return False


def send_email(alert) -> bool:
    if not SMTP_USER or not ALERT_EMAIL_TO:
        logger.debug("Email non configuré, notification ignorée.")
        return False

    text = _format_message(alert).replace("*", "")  # strip markdown
    subject = f"[ThomasPJ] {alert.summary[:60]}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ALERT_EMAIL_TO
    msg.attach(MIMEText(text, "plain", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, ALERT_EMAIL_TO, msg.as_string())
        logger.info("Alerte email envoyée à %s.", ALERT_EMAIL_TO)
        return True
    except Exception as exc:
        logger.error("Échec email : %s", exc)
        return False


def notify(alert) -> None:
    """Envoie l'alerte via tous les canaux configurés."""
    sent = send_telegram(alert) or send_email(alert)
    if not sent:
        # Fallback console
        logger.warning("Aucun canal de notification configuré. Alerte :\n%s", _format_message(alert))
