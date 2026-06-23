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

# Données fictives par point de contrôle (item_texte exact)
_QUESTIONS_MOCK_ITEM = {
    "Procédures d'étalonnage à jour": [
        "La date de révision de la procédure est-elle visible ?",
        "La version en vigueur est-elle affichée au poste ?",
        "L'approbation de la dernière révision est-elle signée ?",
    ],
    "Registres de calibration accessibles": [
        "Les registres sont-ils disponibles sur le poste de travail ?",
        "Les dernières entrées sont-elles renseignées et lisibles ?",
        "L'accès au registre numérique est-il fonctionnel ?",
    ],
    "Instructions de travail signées": [
        "Chaque instruction porte-t-elle une signature d'approbation ?",
        "Les opérateurs ont-ils signé les instructions qui les concernent ?",
        "Les instructions signées sont-elles affichées au poste ?",
    ],
    "Plan qualité révisé < 12 mois": [
        "La date de dernière révision du plan qualité est-elle < 12 mois ?",
        "La revue du plan qualité est-elle documentée et signée ?",
        "Les actions du plan qualité sont-elles suivies et à jour ?",
    ],
    "Clés dynamométriques étalonnées": [
        "Les étiquettes d'étalonnage des clés sont-elles valides ?",
        "Toutes les clés en service figurent-elles au registre ?",
        "Les clés périmées sont-elles retirées du poste ?",
    ],
    "Étiquettes d'étalonnage visibles": [
        "Chaque équipement porte-t-il une étiquette lisible ?",
        "La date de prochain étalonnage est-elle visible ?",
        "Les étiquettes sont-elles correctement fixées et non altérées ?",
    ],
    "Registre des équipements complet": [
        "Tous les équipements de mesure figurent-ils au registre ?",
        "Les dates d'étalonnage sont-elles renseignées pour chaque équipement ?",
        "Le registre est-il mis à jour après chaque intervention ?",
    ],
    "Zone quarantaine délimitée": [
        "La zone quarantaine est-elle physiquement délimitée et identifiée ?",
        "L'accès à la zone quarantaine est-il contrôlé ?",
        "La signalétique de la zone est-elle visible et conforme ?",
    ],
    "Étiquetage pièces NC conforme": [
        "Chaque pièce NC porte-t-elle une étiquette de non-conformité ?",
        "Les étiquettes NC mentionnent-elles la cause et la décision ?",
        "Les pièces NC en attente de décision sont-elles identifiées ?",
    ],
    "Traçabilité des actions correctives": [
        "Chaque NC fait-elle l'objet d'une fiche d'action corrective ?",
        "Les délais de traitement des actions sont-ils respectés ?",
        "L'efficacité des actions correctives est-elle vérifiée ?",
    ],
    "Habilitations opérateurs à jour": [
        "Les habilitations de tous les opérateurs en poste sont-elles valides ?",
        "Les habilitations périmées ont-elles été retirées ?",
        "Le planning de renouvellement est-il tenu à jour ?",
    ],
    "Plan de formation documenté": [
        "Le plan de formation annuel est-il formalisé et approuvé ?",
        "Les formations réalisées sont-elles tracées avec présences ?",
        "Les besoins identifiés en entretien sont-ils pris en compte ?",
    ],
}

_SUGGESTIONS_MOCK_ITEM = {
    "Procédures d'étalonnage à jour": [
        "Procédure PE-07 v2 — version v3 approuvée non déployée au poste",
        "Date de révision absente sur la procédure d'étalonnage affichée atelier",
        "Procédure signée par responsable démissionnaire en 2023 — non mise à jour",
    ],
    "Registres de calibration accessibles": [
        "Registre de calibration introuvable poste 12 — opérateur oriente vers bureau",
        "Registre papier illisible — encre effacée sur les 3 dernières colonnes",
        "Accès GPAO refusé — droits non mis à jour depuis mutation de l'opérateur",
    ],
    "Instructions de travail signées": [
        "Fiche de travail FT-032 sans signature responsable — utilisée depuis 8 mois",
        "3 opérateurs n'ont pas signé la mise à jour instruction freinage janv. 2025",
        "Affichage poste 5 : instruction v1 de 2022 non remplacée par la v3 actuelle",
    ],
    "Plan qualité révisé < 12 mois": [
        "Plan qualité daté de janvier 2024 — aucune révision depuis 17 mois",
        "Revue annuelle plan qualité non réalisée — réunion annulée, non reprogrammée",
        "5 actions du plan qualité 'en cours' depuis plus de 12 mois sans mise à jour",
    ],
    "Clés dynamométriques étalonnées": [
        "Clé dynamométrique poste 7 — étiquette périmée depuis 14 mois, encore en service",
        "3 clés sans étiquette d'étalonnage identifiées sur la ligne B",
        "Clé retirée du registre mais toujours utilisée par l'opérateur de nuit",
    ],
    "Étiquettes d'étalonnage visibles": [
        "Étiquette étalonnage illisible sur manomètre ligne C — usée par les solvants",
        "4 équipements de contrôle sans étiquette — arrachée, non remplacée",
        "Date de prochain étalonnage dépassée de 3 mois — non signalée au responsable",
    ],
    "Registre des équipements complet": [
        "Registre des équipements : 6 appareils présents en atelier absents de la liste",
        "Colonne 'dernier étalonnage' vide pour 8 équipements sur 22",
        "Mise à jour du registre non réalisée depuis départ du responsable métrologie",
    ],
    "Zone quarantaine délimitée": [
        "Zone quarantaine délimitée par ruban adhésif partiellement décollé — confusion possible",
        "Aucun panneau 'QUARANTAINE' visible — opérateurs ne distinguent pas la zone",
        "Pièces conformes stockées à 50 cm de la zone NC sans séparation physique",
    ],
    "Étiquetage pièces NC conforme": [
        "10 pièces en zone quarantaine sans étiquette NC — statut inconnu des opérateurs",
        "Étiquettes NC sans mention de la cause ni du responsable décision",
        "Pièces 'dérogation en attente' non distinguées des pièces 'à rebuter'",
    ],
    "Traçabilité des actions correctives": [
        "NC ouverte nov. 2024 sans action corrective associée — statut toujours 'ouvert'",
        "3 fiches 8D dont le délai est dépassé — aucune relance documentée",
        "Clôture d'action corrective sans vérification d'efficacité terrain",
    ],
    "Habilitations opérateurs à jour": [
        "Habilitation de 3 techniciens expirée depuis janv. 2025 — actifs sur ligne",
        "Nouvel opérateur réalise des opérations de freinage sans habilitation validée",
        "Registre des habilitations non mis à jour depuis départ de l'ancienne RH",
    ],
    "Plan de formation documenté": [
        "Plan de formation 2025 non établi — aucun document ni calendrier visible",
        "Formations réalisées en mars 2025 sans émargement ni feuille de présence",
        "3 besoins formation identifiés en entretien annuel absents du plan 2025",
    ],
}

_QUESTIONS_MOCK = {
    "§7.5":   ["Les documents de référence sont-ils signés et à jour ?",
               "Les enregistrements sont-ils accessibles et lisibles ?",
               "La maîtrise des versions est-elle assurée ?"],
    "§7.1.5": ["Les certificats d'étalonnage sont-ils valides ?",
               "Les équipements affichent-ils une étiquette d'étalonnage ?",
               "Le registre d'étalonnage est-il complet et à jour ?"],
    "§8.7":   ["La zone quarantaine est-elle délimitée et identifiée ?",
               "Les pièces NC sont-elles séparées des pièces conformes ?",
               "Un responsable NC est-il désigné et informé ?"],
    "§7.2":   ["Les habilitations des opérateurs sont-elles à jour ?",
               "Les formations requises ont-elles été réalisées ?",
               "Les fiches de compétences sont-elles disponibles ?"],
    "§8.4":   ["Les fournisseurs critiques sont-ils évalués annuellement ?",
               "Les critères de sélection fournisseurs sont-ils documentés ?",
               "Les non-conformités fournisseurs sont-elles tracées ?"],
    "§8.5":   ["Les paramètres de production sont-ils maîtrisés ?",
               "La traçabilité des lots est-elle assurée ?",
               "Les plans de contrôle sont-ils appliqués ?"],
    "§9.1":   ["Les indicateurs de performance sont-ils mesurés régulièrement ?",
               "Les résultats d'audit interne sont-ils documentés ?",
               "Les actions correctives sont-elles suivies et clôturées ?"],
    "§10.2":  ["Les non-conformités font-elles l'objet d'une analyse de cause ?",
               "Les actions correctives sont-elles efficaces et vérifiées ?",
               "Les récurrences sont-elles tracées et traitées ?"],
}
_DEFAULT_QUESTIONS = ["La situation observée est-elle conforme aux exigences ?",
                      "Des preuves documentaires sont-elles disponibles ?",
                      "Les responsables concernés sont-ils informés ?"]

_SUGGESTIONS_MOCK = {
    "§7.5":   ["Procédure de contrôle v2 obsolète — version v3 non diffusée aux opérateurs",
               "Enregistrements de contrôle de la semaine absents — classeur vide",
               "Plan qualité non signé par le responsable depuis 14 mois"],
    "§7.1.5": ["Clé dynamométrique poste 7 sans étiquette — dernier étalonnage non retrouvé",
               "3 manomètres ligne B périmés depuis 6 mois — utilisés quotidiennement",
               "Registre étalonnage vierge pour les équipements de contrôle thermique"],
    "§8.7":   ["Zone quarantaine mal délimitée — pièces conformes et NC côte à côte",
               "Bac NC sans étiquetage — opérateurs ne distinguent pas les statuts",
               "10 pièces rebutées sans fiche de non-conformité associée"],
    "§7.2":   ["Nouvel opérateur intervient seul sans habilitation validée",
               "Habilitations de 3 techniciens expirées depuis janvier 2025",
               "Plan de formation 2025 non établi — aucun calendrier visible"],
    "§8.4":   ["Fournisseur acier non évalué depuis 2 ans — aucun audit planifié",
               "Bon de commande sans référence aux exigences qualité applicables",
               "NC fournisseur ouverte en nov. 2024 — aucune réponse reçue"],
    "§8.5":   ["Paramètre de soudure hors tolérance non détecté avant expédition",
               "Fiche suiveuse absente sur 4 lots en cours de fabrication",
               "Contrôle intermédiaire non réalisé — opérateur n'a pas signé"],
    "§9.1":   ["Tableau de bord qualité non mis à jour depuis 3 mois",
               "Taux de rebut du mois non calculé — données brutes non consolidées",
               "Indicateur livraison à l'heure à 62 % — objectif 90 % non atteint"],
    "§10.2":  ["Même cause racine identifiée pour 3 NC consécutives — action non efficace",
               "Fiche 8D ouverte en janvier — actions correctives non clôturées",
               "Récidive §8.7 constatée — plan d'action précédent non appliqué"],
}
_DEFAULT_SUGGESTIONS = [
    "Clé dynamométrique poste 7 sans étiquette d'étalonnage — dernier étalonnage non retrouvé",
    "Zone quarantaine pièces non conformes mal délimitée — pièces conformes et NC côte à côte",
    "Nouvel opérateur intervient seul sur opérations de freinage sans habilitation validée",
]

_TRANSCRIPTION_MOCK = {
    "Procédures d'étalonnage à jour":
        "Certificats étalonnage capteurs pression absents du classeur site. Dernière vérification datée > 12 mois.",
    "Registres de calibration accessibles":
        "Registre de calibration introuvable au poste 12 — opérateur oriente vers le bureau du responsable.",
    "Instructions de travail signées":
        "Fiche de travail FT-032 sans signature responsable — utilisée depuis 8 mois sans mise à jour.",
    "Plan qualité révisé < 12 mois":
        "Plan qualité daté de janvier 2024 — aucune révision depuis 17 mois, revue annuelle non réalisée.",
    "Clés dynamométriques étalonnées":
        "Clé dynamométrique poste 7 — étiquette périmée depuis 14 mois, encore en service sur la ligne.",
    "Étiquettes d'étalonnage visibles":
        "4 équipements de contrôle sans étiquette visible — arrachées, non remplacées sur atelier B.",
    "Registre des équipements complet":
        "6 appareils présents en atelier absents du registre. Colonne dernier étalonnage vide pour 8 équipements.",
    "Zone quarantaine délimitée":
        "Zone quarantaine délimitée par ruban adhésif partiellement décollé — confusion possible avec zone conforme.",
    "Étiquetage pièces NC conforme":
        "10 pièces en zone quarantaine sans étiquette NC — statut inconnu des opérateurs de ligne.",
    "Traçabilité des actions correctives":
        "3 fiches 8D dont le délai est dépassé — aucune relance documentée. NC nov. 2024 sans action associée.",
    "Habilitations opérateurs à jour":
        "2 habilitations BR échues depuis sept. 2024 — agents Dupont et Moreau toujours en poste sur zone HTA.",
    "Plan de formation documenté":
        "Plan de formation 2025 non établi. Formations mars 2025 sans émargement ni feuille de présence.",
}
_DEFAULT_TRANSCRIPTION = "Observation terrain enregistrée par saisie manuscrite — à compléter."

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

def _llm_json_list(prompt: str, max_tokens: int = 300) -> list[str]:
    """Appelle GEP et retourne une liste JSON de 3 chaînes, ou [] si indisponible."""
    gep = _get_gep_client()
    if not gep:
        return []
    try:
        model = os.getenv("GEP_MODEL", "anthropic.claude-sonnet-4-6")
        resp = gep.chat.completions.create(
            model=model, max_completion_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
    except Exception:
        return []
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    try:
        result = json.loads(raw)
        if isinstance(result, list) and len(result) == 3:
            return [str(s) for s in result]
    except Exception:
        pass
    return []


def generer_suggestions(item_texte: str, clause: str, section_titre: str) -> list[str]:
    """Génère 3 exemples d'observations terrain contextualisées à un point de checklist."""
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
    result = _llm_json_list(prompt, max_tokens=300)
    if result:
        return result
    # Fallback statique : item d'abord, puis clause
    if item_texte in _SUGGESTIONS_MOCK_ITEM:
        return _SUGGESTIONS_MOCK_ITEM[item_texte]
    key = next((k for k in _SUGGESTIONS_MOCK if clause.startswith(k)), None)
    return _SUGGESTIONS_MOCK.get(key, _DEFAULT_SUGGESTIONS)


def generer_questions_oui_non(item_texte: str, clause: str, section_titre: str) -> list[str]:
    """Génère 3 questions de vérification oui/non contextualisées à un point de checklist."""
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
    result = _llm_json_list(prompt, max_tokens=200)
    if result:
        return result
    # Fallback statique : item d'abord, puis clause
    if item_texte in _QUESTIONS_MOCK_ITEM:
        return _QUESTIONS_MOCK_ITEM[item_texte]
    key = next((k for k in _QUESTIONS_MOCK if clause.startswith(k)), None)
    return _QUESTIONS_MOCK.get(key, _DEFAULT_QUESTIONS)


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


def transcrire_manuscrit(image_base64: str, item_texte: str = "") -> str:
    """Transcrit une image manuscrite en texte d'observation. Fallback par item si GEP indisponible."""
    client = _get_gep_client()
    if client:
        model = os.getenv("GEP_MODEL", "anthropic.claude-sonnet-4-6")
        try:
            resp = client.chat.completions.create(
                model=model,
                max_completion_tokens=300,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image_url",
                         "image_url": {"url": f"data:image/png;base64,{image_base64}"}},
                        {"type": "text",
                         "text": "Transcris exactement le texte manuscrit visible dans cette image. Retourne uniquement le texte, sans commentaire ni ponctuation ajoutée."},
                    ],
                }],
            )
            texte = resp.choices[0].message.content.strip()
            if texte:
                return texte
        except Exception:
            pass
    # Fallback statique par item, puis défaut
    return _TRANSCRIPTION_MOCK.get(item_texte, _DEFAULT_TRANSCRIPTION)


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
