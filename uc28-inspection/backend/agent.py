"""
Agent d'analyse d'inspection — UC 28 Inspection Augmentée
Moteur de règles métier : criticité par mots-clés, actions par clause ISO 9001.
Si GEP_API_KEY est défini, enrichit le diagnostic via Claude (GEP Capgemini).
"""
import base64
import io
import json
import os
import unicodedata

from dotenv import load_dotenv
from database import get_connection
from rag import trouver_clause

load_dotenv()

_gep_client = None

def _get_gep_client():
    global _gep_client
    if _gep_client is not None:
        return _gep_client
    key = os.getenv("GEP_API_KEY", "")
    url = os.getenv("GEP_API_URL", "https://openai.generative.engine.capgemini.com/v1")
    if not key:
        return None
    try:
        from openai import OpenAI
        _gep_client = OpenAI(api_key=key, base_url=url)
        return _gep_client
    except Exception:
        return None


def _enrichir_diagnostic_llm(observation: str, clause: dict, criticite: str, site_context: str) -> dict | None:
    client = _get_gep_client()
    if client is None:
        return None
    model = os.getenv("GEP_MODEL", "anthropic.claude-sonnet-4-6")
    prompt = f"""Tu es un expert auditeur ISO 9001. Analyse ce constat d'inspection et retourne UNIQUEMENT un JSON valide.

Constat : {observation}
Clause ISO identifiée : {clause['clause']} — {clause['titre']}
Criticité détectée : {criticite}
{f"Contexte site : {site_context}" if site_context else ""}

Retourne ce JSON (sans markdown, sans explication, 1 phrase max par champ) :
{{
  "diagnostic": "phrase courte décrivant la non-conformité et son impact",
  "action_corrective": "action immédiate à mener",
  "action_preventive": "action systémique pour éviter la récurrence"
}}"""
    try:
        resp = client.chat.completions.create(
            model=model,
            max_completion_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        # Supprimer les balises markdown ```json ... ``` si présentes
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        return json.loads(raw)
    except Exception:
        return None

def generer_suggestions(item_texte: str, clause: str, section_titre: str) -> list[str]:
    """Génère 3 exemples d'observations terrain contextualisées à un point de checklist."""
    client = _get_gep_client()
    if client is None:
        return []
    model = os.getenv("GEP_MODEL", "anthropic.claude-opus-4-7")
    prompt = (
        f"Tu es un auditeur ISO 9001 sur le terrain. Le point de check-list à vérifier est :\n"
        f"Point : {item_texte}\n"
        f"Section : {section_titre} ({clause})\n\n"
        f"Génère 3 observations terrain concrètes et variées qu'un auditeur pourrait noter pour ce point.\n"
        f"Chaque observation doit être :\n"
        f"- Courte (1 phrase, max 15 mots)\n"
        f"- Factuelle et précise (lieu, équipement ou situation réelle)\n"
        f"- Au format : [constat observé] — [détail ou impact]\n\n"
        f"Retourne UNIQUEMENT un tableau JSON de 3 chaînes, sans markdown :\n"
        f'["observation 1", "observation 2", "observation 3"]'
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            max_completion_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        if isinstance(result, list) and len(result) == 3:
            return [str(s) for s in result]
        return []
    except Exception:
        return []


def generer_questions_oui_non(item_texte: str, clause: str, section_titre: str) -> list[str]:
    """Génère 3 questions de vérification oui/non contextualisées à un point de checklist."""
    client = _get_gep_client()
    if client is None:
        return []
    model = os.getenv("GEP_MODEL", "anthropic.claude-sonnet-4-6")
    prompt = (
        f"Tu es un auditeur ISO 9001 expert.\n"
        f"Pour le point de contrôle suivant :\n"
        f"Point : {item_texte}\n"
        f"Section : {section_titre} ({clause})\n\n"
        f"Génère exactement 3 questions de vérification terrain auxquelles l'auditeur peut répondre par OUI ou NON.\n"
        f"Chaque question doit être courte (max 12 mots), concrète et observable directement sur le terrain.\n"
        f"Retourne UNIQUEMENT un tableau JSON de 3 chaînes, sans markdown :\n"
        f'["question 1", "question 2", "question 3"]'
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            max_completion_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        if isinstance(result, list) and len(result) == 3:
            return [str(s) for s in result]
        return []
    except Exception:
        return []


def synthetiser_observation(observation: str) -> str:
    """Reformule une observation brute en un constat d'audit concis et professionnel."""
    client = _get_gep_client()
    if client is None:
        return observation
    model = os.getenv("GEP_MODEL", "anthropic.claude-sonnet-4-6")
    prompt = (
        "Tu es un expert auditeur ISO 9001. Reformule le constat brut suivant en une seule "
        "phrase nominale, concise, professionnelle et factuelle, sans jugement ni hypothèse.\n\n"
        f"Constat brut : {observation}\n\n"
        "Retourne UNIQUEMENT la phrase reformulée, sans guillemets, sans explication."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            max_completion_tokens=120,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return observation


def _extraire_texte(nom: str, contenu: str) -> str:
    """Extrait le texte d'un document encodé en base64 ou retourne le contenu texte brut."""
    ext = nom.rsplit(".", 1)[-1].lower() if "." in nom else ""
    # Détection base64 : pas d'espaces, longueur typique d'un fichier encodé
    is_b64 = len(contenu) > 200 and " " not in contenu and "\n" not in contenu[:200]
    if not is_b64 or ext not in ("pdf", "docx", "doc"):
        return contenu  # déjà du texte brut

    try:
        data = base64.b64decode(contenu)
        if ext == "pdf":
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(data))
            return "\n".join(p.extract_text() or "" for p in reader.pages)[:6000]
        if ext in ("docx", "doc"):
            import docx as python_docx
            doc = python_docx.Document(io.BytesIO(data))
            return "\n".join(p.text for p in doc.paragraphs if p.text.strip())[:6000]
    except Exception:
        pass
    return contenu  # fallback : contenu brut si extraction échoue


def analyser_document_fournisseur(nom: str, contenu: str) -> dict:
    """Analyse un document déposé par le fournisseur et extrait les points clés pour l'auditeur BV."""
    texte = _extraire_texte(nom, contenu)
    client = _get_gep_client()
    if client is None:
        return {
            "resume": f"Document {nom} reçu — analyse IA indisponible (mode local).",
            "sections_a_risque": ["§7.1.5 — Étalonnage"],
            "points_controle": ["Vérifier les certificats d'étalonnage en date"],
            "nc_historique": [],
        }
    model = os.getenv("GEP_MODEL", "anthropic.claude-sonnet-4-6")
    prompt = (
        "Tu es un expert auditeur ISO 9001 chez Bureau Veritas. "
        "Analyse ce document déposé par le fournisseur RATP avant un audit qualité. "
        "Retourne UNIQUEMENT un JSON valide (sans markdown) avec ces champs :\n"
        '{"resume": "1-2 phrases résumant les points critiques", '
        '"sections_a_risque": ["clause — description", ...], '
        '"points_controle": ["point à vérifier sur site", ...], '
        '"nc_historique": ["NC ou observation significative", ...]}\n\n'
        f"Nom du document : {nom}\n\nContenu :\n{texte[:3000]}"
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            max_completion_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        return result
    except Exception:
        return {
            "resume": f"Analyse du document {nom} disponible (mode dégradé).",
            "sections_a_risque": [],
            "points_controle": [],
            "nc_historique": [],
        }


_MAJEURE_KEYWORDS = [
    "absent", "manquant", "inexistant", "introuvable",
    "non etalon", "pas etalon", "non conforme", "pas conforme",
    "hors service", "perime", "expire", "bloque", "interdit", "dangereux",
]

_MINEURE_KEYWORDS = [
    "incomplet", "partiel", "partiellement", "retard",
    "delai", "en cours", "a mettre a jour", "non signe",
]


def _normaliser(text: str) -> str:
    """Lowercase + suppression des accents pour comparaison robuste."""
    return unicodedata.normalize("NFD", text.lower()).encode("ascii", "ignore").decode()

_ACTIONS: dict[str, dict[str, str]] = {
    "7.1.5": {
        "corrective": "Retirer les équipements de la production et planifier leur étalonnage.",
        "preventive": "Mettre en place un calendrier d'étalonnage avec alertes automatiques.",
    },
    "7.2": {
        "corrective": "Identifier les opérateurs sans habilitation valide et les former en priorité.",
        "preventive": "Créer un suivi automatisé des dates d'expiration des habilitations.",
    },
    "7.5": {
        "corrective": "Mettre à jour les documents concernés et en assurer la diffusion.",
        "preventive": "Planifier une revue documentaire trimestrielle.",
    },
    "8.4": {
        "corrective": "Demander les justificatifs de qualification aux sous-traitants concernés.",
        "preventive": "Intégrer la vérification des qualifications dans le processus de sélection.",
    },
    "8.7": {
        "corrective": "Isoler et étiqueter les éléments non conformes en zone quarantaine.",
        "preventive": "Renforcer les contrôles à réception et formaliser le processus NC.",
    },
    "9.2": {
        "corrective": "Reprogrammer l'audit interne manqué dans les 30 jours.",
        "preventive": "Établir un calendrier annuel d'audits internes validé par la direction.",
    },
    "10.2": {
        "corrective": "Ouvrir une fiche d'action corrective et désigner un responsable.",
        "preventive": "Analyser les causes racines et mettre à jour le plan d'amélioration.",
    },
    "_default": {
        "corrective": "Documenter la non-conformité et engager une action corrective.",
        "preventive": "Renforcer la surveillance sur ce point lors du prochain audit.",
    },
}


_SCORE_CRITICITE = {"MAJEURE": 3, "MINEURE": 2, "OBSERVATION": 1, "CONFORME": 0}


def _evaluer_criticite(observation: str) -> str:
    text = _normaliser(observation)
    if any(kw in text for kw in _MAJEURE_KEYWORDS):
        return "MAJEURE"
    if any(kw in text for kw in _MINEURE_KEYWORDS):
        return "MINEURE"
    return "OBSERVATION"


def _get_actions(clause_num: str) -> dict[str, str]:
    # Cherche le préfixe le plus long correspondant (ex. "7.1.5" avant "7.1" avant "7")
    for key in sorted(_ACTIONS.keys(), key=len, reverse=True):
        if key != "_default" and clause_num.startswith(key):
            return _ACTIONS[key]
    return _ACTIONS["_default"]


def _get_site_context(site_id: str) -> str:
    """Charge le contexte du site depuis la base de données."""
    conn = get_connection()
    site = conn.execute(
        "SELECT * FROM sites WHERE site_id = ?", (site_id,)
    ).fetchone()
    if site is None:
        conn.close()
        return ""

    audits = conn.execute(
        """SELECT date, auditeur, non_conformites_majeures, non_conformites_mineures,
                  themes_recurrents
           FROM audits_historiques
           WHERE site_id = ?
           ORDER BY date DESC
           LIMIT 3""",
        (site_id,),
    ).fetchall()
    conn.close()

    site_dict = dict(site)
    historique = [
        {
            "date": a["date"],
            "auditeur": a["auditeur"],
            "nc_majeures": a["non_conformites_majeures"],
            "nc_mineures": a["non_conformites_mineures"],
            "themes": json.loads(a["themes_recurrents"] or "[]"),
        }
        for a in audits
    ]

    return (
        f"Site : {site_dict['nom']} ({site_id})\n"
        f"Localisation : {site_dict['localisation']}\n"
        f"Périmètre : {site_dict['perimetre']}\n"
        f"Effectif : {site_dict['effectif']}\n"
        f"Responsable qualité : {site_dict['responsable_qualite']}\n"
        f"Dernière certification : {site_dict['derniere_certification_iso']}\n"
        f"Prochaine recertification : {site_dict['prochaine_recertification']}\n"
        f"Historique audits récents :\n{json.dumps(historique, ensure_ascii=False, indent=2)}"
    )


def analyser_observation(observation: str, site_id: str | None = None) -> dict:
    """
    Analyse une observation terrain via règles métier et retourne un diagnostic structuré.

    Returns:
        {
          "observation": str,
          "site_id": str | None,
          "clause_iso": {"clause": str, "titre": str, "exigence": str, "score": float},
          "criticite": "MAJEURE" | "MINEURE" | "OBSERVATION",
          "diagnostic": str,
          "action_corrective": str,
          "action_preventive": str,
        }
    """
    clause = trouver_clause(observation)
    site_context = _get_site_context(site_id) if site_id else ""
    criticite = _evaluer_criticite(observation)
    actions = _get_actions(clause["clause"])

    # Diagnostic par défaut (rule-based)
    diagnostic = (
        f"Non-conformité identifiée sur la clause {clause['clause']} "
        f"– {clause['titre']} : {observation[:120]}."
    )
    action_corrective = actions["corrective"]
    action_preventive = actions["preventive"]

    # Enrichissement LLM via GEP si disponible
    llm = _enrichir_diagnostic_llm(observation, clause, criticite, site_context)
    if llm:
        diagnostic = llm.get("diagnostic", diagnostic)
        action_corrective = llm.get("action_corrective", action_corrective)
        action_preventive = llm.get("action_preventive", action_preventive)

    return {
        "observation": observation,
        "site_id": site_id,
        "clause_iso": clause,
        "criticite": criticite,
        "score_criticite": _SCORE_CRITICITE.get(criticite, 1),
        "diagnostic": diagnostic,
        "action_corrective": action_corrective,
        "action_preventive": action_preventive,
        "llm_enrichi": llm is not None,
    }
