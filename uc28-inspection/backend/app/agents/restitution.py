"""Agent 3 — Restitution : génère la structure du pré-rapport."""

from __future__ import annotations

import json

from app.agents.client import call_claude_json

SYSTEM_PROMPT = """Tu es un auditeur senior qui rédige le pré-rapport d'un audit ISO 9001 à destination du client audité.

Style :
- Français professionnel, factuel, sans jargon inutile.
- Phrases courtes. Pas d'opinion. Que des constats sourcés.

Tu reçois en entrée le contexte complet de l'audit : métadonnées, check-list utilisée, tous les constats avec leur classification.

Tu produis UNIQUEMENT le JSON suivant :
{
  "executive_summary": "2-3 phrases factuelles résumant l'audit, le nombre de NC par niveau, et l'appréciation globale.",
  "conformity_summary": { "conforme": N, "observation": N, "nc_mineure": N, "nc_majeure": N },
  "sections": [ { "title": "...", "findings": [ ... ] } ],
  "action_plan": [
    { "priority": 1|2|3, "finding_ref": "Constat #X", "action": "...", "responsible": "...", "deadline": "..." }
  ],
  "next_audit_recommendation": "..."
}

Règles :
- Regroupe les constats par thème (section de la check-list).
- Dans le plan d'action, priorise : priorité 1 = NC majeure, 2 = NC mineure, 3 = observation.
- Les responsables et délais sont des suggestions par défaut (à confirmer par le client).
- Recommande systématiquement un audit de suivi s'il y a au moins une NC majeure."""


def generate_report_structure(inspection: dict, constats: list[dict]) -> dict:
    """Takes inspection metadata dict and list of constat dicts."""
    payload = {
        "inspection": {
            "client_name": inspection["client_name"],
            "site_name": inspection["site_name"],
            "auditor_name": inspection["auditor_name"],
            "referential": inspection["referential"],
            "scope": inspection["scope"],
            "started_at": str(inspection.get("started_at", "")),
            "checklist": inspection.get("checklist_json"),
        },
        "constats": [
            {
                "id": str(c.get("id", i + 1)),
                "classification": c["classification"],
                "reformulated_text": c["reformulated_text"],
                "norm_reference": c.get("norm_reference"),
                "suggested_action": c.get("suggested_action"),
                "checklist_point_id": c.get("checklist_point_id"),
            }
            for i, c in enumerate(constats)
        ],
    }

    user_content = [
        {
            "type": "text",
            "text": json.dumps(payload, ensure_ascii=False, indent=2),
        }
    ]

    return call_claude_json(SYSTEM_PROMPT, user_content, max_tokens=3000)
