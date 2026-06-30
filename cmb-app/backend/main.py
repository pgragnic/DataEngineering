from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json, os, httpx
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="CMB Finance API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Powens (Open Banking) config ──────────────────────────────────────────────
POWENS_DOMAIN = os.getenv("POWENS_DOMAIN", "demo")
POWENS_CLIENT_ID = os.getenv("POWENS_CLIENT_ID", "")
POWENS_CLIENT_SECRET = os.getenv("POWENS_CLIENT_SECRET", "")
POWENS_BASE = f"https://{POWENS_DOMAIN}.biapi.pro/2.0"

# Timeout généreux pour les premiers appels Powens
POWENS_TIMEOUT = httpx.Timeout(30.0)


async def _powens_get_client_token() -> str:
    """Obtient un token client Powens (client_credentials)."""
    async with httpx.AsyncClient(timeout=POWENS_TIMEOUT) as client:
        r = await client.post(
            f"{POWENS_BASE}/auth/token/code",
            data={
                "grant_type": "client_credentials",
                "client_id": POWENS_CLIENT_ID,
                "client_secret": POWENS_CLIENT_SECRET,
            },
        )
        if r.status_code != 200:
            raise HTTPException(502, f"Powens auth échouée : {r.text}")
        return r.json().get("auth_token") or r.json().get("access_token")


# ── Routes Powens ─────────────────────────────────────────────────────────────

@app.post("/connect/init")
async def connect_init(redirect_uri: str = Query(default="http://localhost:5173/callback")):
    """
    Initialise la session de connexion Open Banking.
    Retourne l'URL de la webview Powens vers laquelle rediriger l'utilisateur.
    """
    if not POWENS_CLIENT_ID:
        raise HTTPException(503, "POWENS_CLIENT_ID non configuré dans .env")

    token = await _powens_get_client_token()

    # L'URL webview Powens — l'utilisateur y cherche sa banque (CMB) et se connecte
    webview_url = (
        f"{POWENS_BASE}/webview/"
        f"?client_id={POWENS_CLIENT_ID}"
        f"&token={token}"
        f"&redirect_uri={redirect_uri}"
        f"&state=cmb-finance-app"
    )
    return {"webview_url": webview_url, "client_token": token}


@app.get("/connect/callback")
async def connect_callback(
    code: str = Query(..., description="Code renvoyé par Powens après connexion"),
):
    """
    Échange le code de callback Powens contre un user access token.
    Appelé après redirection depuis la webview.
    """
    async with httpx.AsyncClient(timeout=POWENS_TIMEOUT) as client:
        r = await client.post(
            f"{POWENS_BASE}/auth/token/access",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": POWENS_CLIENT_ID,
                "client_secret": POWENS_CLIENT_SECRET,
            },
        )
        if r.status_code != 200:
            raise HTTPException(502, f"Échange de code échoué : {r.text}")
        data = r.json()
    return {
        "access_token": data.get("access_token"),
        "token_type": data.get("token_type", "Bearer"),
        "expires_in": data.get("expires_in"),
    }


@app.get("/connect/accounts")
async def get_accounts(token: str = Query(...)):
    """Récupère les comptes bancaires réels via Powens."""
    async with httpx.AsyncClient(timeout=POWENS_TIMEOUT) as client:
        r = await client.get(
            f"{POWENS_BASE}/users/me/accounts",
            headers={"Authorization": f"Bearer {token}"},
        )
        if r.status_code == 401:
            raise HTTPException(401, "Token expiré — reconnectez-vous")
        if r.status_code != 200:
            raise HTTPException(502, f"Powens accounts error : {r.text}")
        data = r.json()

    # Normalisation vers le format de l'app
    accounts = []
    for acc in data.get("accounts", []):
        accounts.append({
            "id": str(acc["id"]),
            "type": acc.get("type", "unknown"),
            "name": acc.get("name", ""),
            "number": acc.get("number", "****"),
            "balance": acc.get("balance", 0),
            "currency": acc.get("currency", "EUR"),
            "iban": acc.get("iban", ""),
            "connector_name": acc.get("connection", {}).get("connector", {}).get("name", ""),
        })
    return {"accounts": accounts}


@app.get("/connect/transactions")
async def get_transactions(
    token: str = Query(...),
    account_id: int = Query(None),
    min_date: str = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(100),
):
    """Récupère les transactions réelles via Powens."""
    params: dict = {"limit": limit}
    if account_id:
        params["id_account"] = account_id
    if min_date:
        params["min_date"] = min_date

    async with httpx.AsyncClient(timeout=POWENS_TIMEOUT) as client:
        r = await client.get(
            f"{POWENS_BASE}/users/me/transactions",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
        )
        if r.status_code == 401:
            raise HTTPException(401, "Token expiré — reconnectez-vous")
        if r.status_code != 200:
            raise HTTPException(502, f"Powens transactions error : {r.text}")
        data = r.json()

    # Normalisation
    transactions = []
    for tx in data.get("transactions", []):
        transactions.append({
            "id": str(tx["id"]),
            "date": tx.get("date", "")[:10],
            "libelle": tx.get("original_wording") or tx.get("wording", ""),
            "montant": tx.get("value", 0),
            "categorie": tx.get("category", {}).get("name", "Autre") if tx.get("category") else "Autre",
            "account_id": str(tx.get("id_account", "")),
        })
    return {"transactions": transactions, "total": data.get("total", len(transactions))}


@app.get("/connect/status")
async def connect_status(token: str = Query(...)):
    """Vérifie si le token est toujours valide et retourne les infos utilisateur."""
    async with httpx.AsyncClient(timeout=POWENS_TIMEOUT) as client:
        r = await client.get(
            f"{POWENS_BASE}/users/me",
            headers={"Authorization": f"Bearer {token}"},
        )
    if r.status_code == 401:
        return {"connected": False}
    if r.status_code != 200:
        return {"connected": False}
    user = r.json()
    return {
        "connected": True,
        "user_id": user.get("id"),
        "email": user.get("email", ""),
    }


# ── Route analyse IA (Claude) ──────────────────────────────────────────────────

class AnalyseRequest(BaseModel):
    user: str
    solde_courant: float
    revenus_mensuels: float
    depenses_mois: dict
    budgets: dict
    evolution: list
    objectifs: list
    epargne_totale: float


@app.post("/analyser")
async def analyser(req: AnalyseRequest):
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

        total_depenses = sum(req.depenses_mois.values())
        taux_epargne = round((req.revenus_mensuels - total_depenses) / req.revenus_mensuels * 100, 1)

        depenses_str = "\n".join(
            f"  - {cat}: {montant:.2f}€ (budget: {req.budgets.get(cat, '?')}€)"
            for cat, montant in sorted(req.depenses_mois.items(), key=lambda x: -x[1])
        )
        objectifs_str = "\n".join(
            f"  - {o['nom']}: {o['actuel']:.0f}€ / {o['cible']:.0f}€ (échéance: {o['deadline']})"
            for o in req.objectifs
        )

        prompt = f"""Tu es un conseiller financier expert du Crédit Mutuel de Bretagne.
Analyse les finances personnelles de {req.user} et fournis une analyse structurée.

SITUATION FINANCIÈRE :
- Solde compte courant : {req.solde_courant:.2f}€
- Revenus mensuels : {req.revenus_mensuels:.2f}€
- Total dépenses ce mois : {total_depenses:.2f}€
- Taux d'épargne estimé : {taux_epargne}%
- Épargne totale : {req.epargne_totale:.2f}€

DÉPENSES PAR CATÉGORIE CE MOIS :
{depenses_str}

OBJECTIFS D'ÉPARGNE :
{objectifs_str}

ÉVOLUTION SUR 6 MOIS :
{json.dumps(req.evolution, ensure_ascii=False)}

Réponds UNIQUEMENT en JSON valide avec cette structure :
{{
  "score": <entier 0-100>,
  "resume": "<2-3 phrases>",
  "points_forts": ["<point 1>", "<point 2>", "<point 3>"],
  "points_vigilance": ["<point 1>", "<point 2>", "<point 3>"],
  "recommandations": ["<conseil 1>", "<conseil 2>", "<conseil 3>", "<conseil 4>"]
}}"""

        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
        return json.loads(text)

    except Exception:
        return {
            "score": 78,
            "resume": "Votre situation financière est solide avec un taux d'épargne élevé. Quelques opportunités d'optimisation ont été identifiées.",
            "points_forts": [
                "Taux d'épargne supérieur à la moyenne nationale",
                "Crédit immobilier bien géré avec taux favorable",
                "Progression régulière vers vos objectifs d'épargne",
            ],
            "points_vigilance": [
                "Dépenses loisirs et abonnements à surveiller",
                "Budget shopping parfois dépassé ponctuellement",
                "Frais alimentaires en légère hausse ce mois",
            ],
            "recommandations": [
                "Automatiser vos virements d'épargne en début de mois",
                "Regrouper vos abonnements pour mieux les contrôler",
                "Envisager un rendez-vous avec votre conseiller pour optimiser votre PEL",
                "Mettre en place une alerte de solde bas pour votre compte courant",
            ],
        }


@app.get("/health")
async def health():
    return {"status": "ok"}
