"""Agent 1 — Préparation : génère la check-list dynamique d'inspection."""

from __future__ import annotations

import json

from app.agents.client import call_claude_json
from app.rag.retrieve import retrieve_norm_context

SYSTEM_PROMPT = """Tu es un auditeur senior certifié IRCA spécialisé en audits qualité ISO 9001.
Ta mission est de préparer une check-list d'inspection structurée et opérationnelle pour un auditeur qui va sur le terrain.

Contraintes :
- Tu travailles sur le référentiel fourni (ISO 9001:2015).
- Tu adaptes systématiquement la check-list au périmètre demandé (scope).
- Chaque point de la check-list doit être : (1) actionnable sur le terrain, (2) lié à un article précis de la norme, (3) accompagné d'une indication des preuves attendues.
- Tu produis 3 à 6 sections, chaque section contenant 3 à 6 points. Pas plus.
- Tu ne fais pas de blabla introductif. Tu renvoies UNIQUEMENT le JSON demandé.

Format de sortie strict (JSON):
{
  "referential": "...",
  "scope_summary": "...",
  "sections": [
    { "id": "S1", "title": "...", "points": [
        { "id": "S1.P1", "question": "...", "expected_evidence": "...", "norm_reference": "ISO 9001 §..." }
    ]}
  ]
}

Si l'historique des audits passés mentionne des NC non clôturées chez ce client, inclus un point de check-list dédié à leur vérification."""


def generate_checklist(
    client_name: str,
    site_name: str,
    referential: str,
    scope: str,
    previous_audits: list[dict] | None = None,
) -> dict:
    rag_chunks = retrieve_norm_context(scope, k=5)
    rag_text = "\n\n".join(
        f"[{c['section']}] {c['excerpt']}" for c in rag_chunks
    )

    history_text = ""
    if previous_audits:
        history_text = "\n\nHistorique des audits précédents :\n" + json.dumps(
            previous_audits, ensure_ascii=False, indent=2
        )

    user_content = [
        {
            "type": "text",
            "text": (
                f"Client : {client_name}\n"
                f"Site : {site_name}\n"
                f"Référentiel : {referential}\n"
                f"Périmètre (scope) : {scope}"
                f"{history_text}\n\n"
                f"Extraits normatifs pertinents (RAG) :\n{rag_text}"
            ),
        }
    ]

    return call_claude_json(SYSTEM_PROMPT, user_content, max_tokens=3000)


if __name__ == "__main__":
    result = generate_checklist(
        client_name="Fournisseur ALPHA",
        site_name="Site de production — Tours, bâtiment B",
        referential="ISO 9001:2015",
        scope="Audit qualité — Processus achats et contrôle réception",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
