"""Agent 2 — Capture : classifie et structure un constat brut."""

from __future__ import annotations

import base64
import json

from app.agents.client import call_claude_json
from app.rag.retrieve import retrieve_norm_context

SYSTEM_PROMPT = """Tu es un assistant d'audit. Tu aides un inspecteur sur le terrain à formaliser et classifier ses constats en temps réel.

Ton travail, pour chaque constat brut :
1. Reformuler le constat en français professionnel et factuel, sans interprétation.
2. Le classifier : "conforme" | "observation" | "nc_mineure" | "nc_majeure".
   - conforme : tout va bien.
   - observation : point d'amélioration sans écart à la norme.
   - nc_mineure : écart ponctuel, isolé, qui n'affecte pas le système qualité.
   - nc_majeure : écart systémique, ou qui touche à la sécurité des personnes, ou qui empêche le SMQ de fonctionner.
3. Identifier l'article du référentiel le plus pertinent. Utilise UNIQUEMENT les extraits normatifs fournis dans le contexte ; ne cite jamais un article que tu n'as pas vu.
4. Indiquer la preuve à collecter (photo, document, mesure).
5. Proposer une action corrective concrète et proportionnée.

Tu renvoies UNIQUEMENT le JSON :
{
  "reformulated_text": "...",
  "classification": "conforme|observation|nc_mineure|nc_majeure",
  "norm_reference": "ISO 9001 §...",
  "norm_excerpt": "...",
  "suggested_evidence": "...",
  "suggested_action": "..."
}

Si tu ne peux pas sourcer à un article précis avec les extraits fournis, mets norm_reference: null et explique-le brièvement dans suggested_action."""


def classify_constat(
    raw_text: str,
    referential: str = "ISO 9001:2015",
    scope: str = "",
    checklist_point: dict | None = None,
    photo_bytes: bytes | None = None,
    photo_media_type: str = "image/jpeg",
) -> tuple[dict, list[dict]]:
    """Returns (structured_constat, rag_chunks)."""
    query = raw_text + (" " + scope if scope else "")
    rag_chunks = retrieve_norm_context(query, k=3)
    rag_text = "\n\n".join(
        f"[{c['section']}] {c['excerpt']}" for c in rag_chunks
    )

    context_parts = [
        f"Référentiel : {referential}",
        f"Périmètre : {scope}" if scope else "",
    ]
    if checklist_point:
        context_parts.append(
            f"Point de check-list en cours : {checklist_point.get('question', '')} "
            f"({checklist_point.get('norm_reference', '')})"
        )

    context_text = "\n".join(p for p in context_parts if p)

    user_content: list[dict] = [
        {
            "type": "text",
            "text": (
                f"{context_text}\n\n"
                f"Constat brut de l'inspecteur :\n{raw_text}\n\n"
                f"Extraits normatifs pertinents (RAG) :\n{rag_text}"
            ),
        }
    ]

    if photo_bytes:
        user_content.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": photo_media_type,
                    "data": base64.b64encode(photo_bytes).decode(),
                },
            }
        )

    result = call_claude_json(SYSTEM_PROMPT, user_content, max_tokens=1500)
    return result, rag_chunks


if __name__ == "__main__":
    result, chunks = classify_constat(
        raw_text="la sortie de secours du bâtiment B est obstruée par un chariot de stockage",
        scope="Processus achats et contrôle réception",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("\nRAG chunks:", [c["section"] for c in chunks])
