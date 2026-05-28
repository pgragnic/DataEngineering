"""
Agent Claude qui analyse les changements détectés dans le portfolio de ThomasPJ.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import anthropic

from config import ANTHROPIC_API_KEY
from etoro_client import PortfolioSnapshot, Position

logger = logging.getLogger(__name__)

_CLIENT = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
_MODEL = "claude-sonnet-4-6"


@dataclass
class PortfolioDiff:
    opened: list[Position]    # nouvelles positions
    closed: list[Position]    # positions fermées
    modified: list[tuple[Position, Position]]  # (avant, après)


@dataclass
class TradeAlert:
    summary: str
    analysis: str
    recommendation: str
    risk_level: str   # "faible" | "modéré" | "élevé"
    diff: PortfolioDiff


# --------------------------------------------------------------------------
# Détection des changements
# --------------------------------------------------------------------------

def compute_diff(prev: PortfolioSnapshot, curr: PortfolioSnapshot) -> PortfolioDiff:
    prev_by_id = {p.position_id: p for p in prev.positions}
    curr_by_id = {p.position_id: p for p in curr.positions}

    opened = [p for pid, p in curr_by_id.items() if pid not in prev_by_id]
    closed = [p for pid, p in prev_by_id.items() if pid not in curr_by_id]
    modified = [
        (prev_by_id[pid], curr_by_id[pid])
        for pid in prev_by_id
        if pid in curr_by_id and prev_by_id[pid].amount != curr_by_id[pid].amount
    ]
    return PortfolioDiff(opened=opened, closed=closed, modified=modified)


def has_changes(diff: PortfolioDiff) -> bool:
    return bool(diff.opened or diff.closed or diff.modified)


# --------------------------------------------------------------------------
# Agent IA
# --------------------------------------------------------------------------

def analyse(
    diff: PortfolioDiff,
    curr: PortfolioSnapshot,
) -> TradeAlert:
    """
    Envoie les changements à Claude pour analyse et retourne une alerte structurée.
    """
    prompt = _build_prompt(diff, curr)

    message = _CLIENT.messages.create(
        model=_MODEL,
        max_tokens=1024,
        system=(
            "Tu es un analyste financier expert spécialisé dans le copy trading. "
            "Tu analyses les mouvements d'un trader eToro expérimenté (ThomasPJ) "
            "et tu fournis des analyses concises et actionnables en français. "
            "Sois factuel, prudent et rappelle toujours que le trading comporte des risques."
        ),
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text
    return _parse_response(raw, diff)


def _build_prompt(diff: PortfolioDiff, curr: PortfolioSnapshot) -> str:
    lines = [
        f"## Changements détectés dans le portfolio de {curr.username}",
        f"Snapshot : {curr.fetched_at}",
        f"Equity totale : {curr.equity:.2f} USD | Gain annuel : {curr.gain_pct:.2f}%",
        "",
    ]

    if diff.opened:
        lines.append("### Nouvelles positions ouvertes")
        for p in diff.opened:
            lines.append(
                f"- **{p.instrument}** | {p.direction.upper()} | "
                f"Montant : {p.amount:.2f} USD | Levier : x{p.leverage} | "
                f"Prix d'entrée : {p.open_rate}"
            )

    if diff.closed:
        lines.append("\n### Positions fermées")
        for p in diff.closed:
            lines.append(
                f"- **{p.instrument}** | {p.direction.upper()} | "
                f"Résultat : {p.profit_pct:+.2f}%"
            )

    if diff.modified:
        lines.append("\n### Positions modifiées (redimensionnement)")
        for before, after in diff.modified:
            lines.append(
                f"- **{before.instrument}** : {before.amount:.2f} → {after.amount:.2f} USD"
            )

    lines += [
        "",
        "---",
        "Fournis une analyse structurée avec exactement ces 4 sections :",
        "**RÉSUMÉ** : Une phrase résumant l'action principale.",
        "**ANALYSE** : Interprétation de la stratégie (2-3 phrases).",
        "**RECOMMANDATION** : Ce qu'un investisseur copiant ce trader devrait surveiller.",
        "**RISQUE** : Niveau de risque global de ces mouvements (Faible / Modéré / Élevé).",
    ]

    return "\n".join(lines)


def _parse_response(text: str, diff: PortfolioDiff) -> TradeAlert:
    """Extrait les sections de la réponse Claude."""

    def extract(label: str) -> str:
        marker = f"**{label}**"
        if marker not in text:
            return ""
        start = text.index(marker) + len(marker)
        # cherche le prochain marqueur ou fin
        next_markers = [f"**{m}**" for m in ("ANALYSE", "RECOMMANDATION", "RISQUE")]
        end = len(text)
        for nm in next_markers:
            if nm in text and text.index(nm) > start:
                end = min(end, text.index(nm))
        return text[start:end].strip(" :\n")

    risk_raw = extract("RISQUE").lower()
    if "élevé" in risk_raw or "eleve" in risk_raw or "high" in risk_raw:
        risk = "élevé"
    elif "modéré" in risk_raw or "moyen" in risk_raw or "medium" in risk_raw:
        risk = "modéré"
    else:
        risk = "faible"

    return TradeAlert(
        summary=extract("RÉSUMÉ"),
        analysis=extract("ANALYSE"),
        recommendation=extract("RECOMMANDATION"),
        risk_level=risk,
        diff=diff,
    )
