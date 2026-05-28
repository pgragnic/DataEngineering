import os
from dotenv import load_dotenv

load_dotenv()

# eToro
ETORO_USERNAME = os.getenv("ETORO_USERNAME", "ThomasPJ")

# Claude
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Notifications — au moins un doit être configuré
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "pgragnic@hotmail.com")

# Polling interval en secondes (défaut : 5 minutes)
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

# Fichier d'état local
STATE_FILE = os.getenv("STATE_FILE", "state.json")
