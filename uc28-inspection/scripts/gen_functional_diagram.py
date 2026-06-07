"""
Génère le schéma d'architecture fonctionnelle UC28 — Inspection Augmentée
en PNG 1920×1080 (16:9).

Structure : parcours utilisateur en 3 actes (AVANT / PENDANT / APRES)
avec les acteurs, les fonctions métier et les valeurs produites.
"""

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

BRAND   = "#0070AD"
EMERALD = "#059669"
SLATE   = "#1E293B"
MIST    = "#EEF2F7"
AMBER   = "#D97706"
VIOLET  = "#7C3AED"
GRAY    = "#64748B"
WHITE   = "#FFFFFF"
DIVIDER = "#CBD5E1"
CYAN    = "#0891B2"
RED     = "#DC2626"
INK     = "#1E293B"

fig, ax = plt.subplots(figsize=(19.2, 10.8), dpi=100)
fig.patch.set_facecolor(MIST)
ax.set_xlim(0, 19.2)
ax.set_ylim(0, 10.8)
ax.axis("off")


# ── Helpers ───────────────────────────────────────────────────────────────────

def sbox(x, y, w, h, color, edge=None, lw=1.5, alpha=1.0):
    edge = edge or color
    ps = FancyBboxPatch((x+0.06, y-0.06), w, h, boxstyle="round,pad=0",
                        facecolor="#00000012", edgecolor="none", linewidth=0, zorder=1)
    ax.add_patch(ps)
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0",
                       facecolor=color, edgecolor=edge,
                       linewidth=lw, alpha=alpha, zorder=2)
    ax.add_patch(p)

def txt(x, y, text, size=9, color=WHITE, bold=False, ha="center", va="center", z=5):
    ax.text(x, y, text, fontsize=size, color=color, fontweight="bold" if bold else "normal",
            ha=ha, va=va, zorder=z)

def pill(x, y, text, bg, fg=WHITE, size=7.2):
    ax.text(x, y, f"  {text}  ", fontsize=size, color=fg, ha="center", va="center", zorder=6,
            bbox=dict(boxstyle="round,pad=0.18", facecolor=bg, edgecolor="none"))

def section_label(x, y, text, color, size=8.5):
    ax.text(x, y, text, fontsize=size, color=color, fontweight="bold",
            ha="center", va="center", zorder=6,
            bbox=dict(boxstyle="round,pad=0.25", facecolor=color + "20",
                      edgecolor=color, linewidth=1.5))

def arrow(x1, y1, x2, y2, color=GRAY, lw=2.0, dashed=False):
    ls = (0, (5, 3)) if dashed else "solid"
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=color, lw=lw,
                                mutation_scale=14, linestyle=ls),
                zorder=4)

def func_card(x, y, w, h, title, bullets, color):
    sbox(x, y, w, h, WHITE, edge=color, lw=2.0)
    # barre colorée en haut
    sbox(x, y + h - 0.36, w, 0.36, color, edge=color, lw=0)
    txt(x + w/2, y + h - 0.18, title, size=8, color=WHITE, bold=True)
    for i, b in enumerate(bullets):
        ax.text(x + 0.14, y + h - 0.62 - i * 0.32, b,
                fontsize=6.8, color=INK, ha="left", va="center", zorder=5)

def value_badge(x, y, text, color):
    ax.text(x, y, text, fontsize=7, color=WHITE, fontweight="bold",
            ha="center", va="center", zorder=6,
            bbox=dict(boxstyle="round,pad=0.28", facecolor=color, edgecolor="none"))


# ── Titre ─────────────────────────────────────────────────────────────────────
sbox(0.4, 10.0, 18.4, 0.65, SLATE)
txt(9.6, 10.33, "UC28 — Inspection Augmentee  ·  Architecture Fonctionnelle", size=13, bold=True)
txt(9.6, 10.05, "Parcours auditeur  ·  3 actes  ·  Valeurs metier produites  ·  Hackathon Capgemini x Anthropic 2026",
    size=8, color="#94A3B8")

# ── Acteurs (colonne gauche) ───────────────────────────────────────────────────
sbox(0.3, 1.0, 2.5, 8.75, "#F8FAFC", edge=DIVIDER, lw=1.2)
section_label(1.55, 9.62, "Acteurs", GRAY)

actors = [
    ("Marc Lefevre", "Auditeur BV", BRAND),
    ("Mei Lin Zhang", "Qualite RATP", EMERALD),
    ("BV (Biface)", "Expertise ISO", CYAN),
    ("Claude AI", "Agent IA", AMBER),
    ("OSRM", "Routing", GRAY),
]
for i, (name, role, col) in enumerate(actors):
    y = 8.55 - i * 1.42
    sbox(0.5, y - 0.55, 2.1, 0.95, col)
    txt(1.55, y - 0.07, name, size=8.5, bold=True)
    txt(1.55, y - 0.35, role, size=7.5)

# ── Swimlanes 3 actes ─────────────────────────────────────────────────────────
ACTES = [
    ("AVANT", "Preparation de la mission", BRAND,   3.1,  5.3),
    ("PENDANT", "Inspection terrain",      EMERALD, 8.55, 5.3),
    ("APRES",   "Cloture et rapport",      VIOLET,  13.95, 5.3),
]

for label_txt, subtitle, col, cx, lane_w in ACTES:
    x = cx - lane_w / 2
    # fond swimlane
    sbox(x, 1.0, lane_w, 8.75, WHITE, edge=col, lw=2.0, alpha=0.92)
    # en-tete coloré
    sbox(x, 9.38, lane_w, 0.37, col, edge=col, lw=0)
    txt(cx, 9.56, label_txt, size=10, bold=True)
    txt(cx, 9.22, subtitle, size=8, color=col, bold=False)

# ── Acte I — AVANT ────────────────────────────────────────────────────────────
AX = 0.6   # x base acte I

func_card(AX, 7.55, 5.1, 1.55,
          "Planification journee",
          ["- Planning visuel (Gantt jour)",
           "- Calcul trajet OSRM (voiture/velo/pied)",
           "- Selection multi-missions"],
          BRAND)

func_card(AX, 5.75, 5.1, 1.6,
          "Brief pre-audit (IA)",
          ["- Contexte site RATP (scope, effectif, contact)",
           "- Documents deposes par Mei Lin Zhang",
           "- Alertes portail : NC ouvertes, risques"],
          BRAND)

func_card(AX, 3.95, 5.1, 1.65,
          "Checklist auto-generee",
          ["- Generation par Claude : ref. ISO + scope + historique",
           "- Points recurrents signales (NC nov. 2024)",
           "- Personnalisation : ajouter/supprimer sections & items"],
          BRAND)

func_card(AX, 1.1, 5.1, 2.65,
          "Connaissances capitalisees BV",
          ["- 13 clauses ISO 9001:2015 en base (SQLite)",
           "- Embeddings sentence-transformers + index FAISS",
           "- Historique NC par site (audits_historiques)",
           "- Documents fournisseur pre-analyses (portail RATP)",
           "- CHECKLIST et RAG_ARTICLES preconfigures"],
          CYAN)

# Valeurs AVANT
pill(3.65, 7.05, "Planification optimisee", BRAND)
pill(3.65, 6.68, "Preparation 4x plus rapide", BRAND)

# ── Acte II — PENDANT ─────────────────────────────────────────────────────────
BX = 5.9

func_card(BX, 7.55, 5.2, 1.55,
          "Saisie observation terrain",
          ["- Saisie vocale (Web Speech API, fr-FR)",
           "- Capture photo (preuve terrain)",
           "- Manuscrit canvas (croquis, annotation)"],
          EMERALD)

func_card(BX, 5.7, 5.2, 1.7,
          "Analyse IA temps reel",
          ["- Clause ISO identifiee par RAG (FAISS + BERT)",
           "- Criticite : NC MAJEURE / MINEURE / OBSERVATION / CONFORME",
           "- Enrichissement LLM : diagnostic + action corrective"],
          EMERALD)

func_card(BX, 3.9, 5.2, 1.65,
          "Aide a la decision",
          ["- Articles normatifs pertinents (par clause)",
           "- Questions oui/non generees par Claude",
           "- Suggestions d'observations contextuelles (Opus)"],
          EMERALD)

func_card(BX, 1.1, 5.2, 2.65,
          "Capitalisation expertise BV",
          ["- Alerte recidive : NC clause identique detectee",
           "- Feedback auditeur (confirmation / correction LLM)",
           "- Synthese observation (reformulation pro Claude)",
           "- Marquage moment fort NC MAJEURE (alerte visuelle)"],
          CYAN)

# Valeurs PENDANT
pill(11.35, 7.05, "Analyse instantanee", EMERALD)
pill(11.35, 6.68, "Explicitabilite IA (RAG)", EMERALD)

# ── Acte III — APRES ──────────────────────────────────────────────────────────
CX = 11.3

func_card(CX, 7.55, 5.2, 1.55,
          "Rapport structure",
          ["- Liste constats (criticite + clause + action)",
           "- Grille de conformite (score % par section ISO)",
           "- Synthese KPIs : NC maj / min / observations"],
          VIOLET)

func_card(CX, 5.7, 5.2, 1.7,
          "Cloture contradictoire",
          ["- Anonymisation RGPD (toggle)",
           "- Signature manuscrite canvas (partie auditee)",
           "- Export PDF pret a envoyer"],
          VIOLET)

func_card(CX, 3.9, 5.2, 1.65,
          "Suivi actions",
          ["- Tableau actions correctives (NC + responsable + delai)",
           "- Statut mock : En cours / Planifie / Cloture",
           "- Traçabilite par clause ISO"],
          VIOLET)

func_card(CX, 1.1, 5.2, 2.65,
          "Boucle d'amelioration continue",
          ["- Feedback expertise reinjecte dans le systeme",
           "- Historique NC enrichi pour les audits futurs",
           "- Capitalisation BV : savoir-faire preserves et transmis",
           "- Portail RATP : documents analyses pre-audit",
           "- Biface : BV vend, RATP et Apave beneficient"],
          CYAN)

# Valeurs APRES
pill(16.6, 7.05, "90/10 inverse", VIOLET)
pill(16.6, 6.68, "Marc libre a 16h", VIOLET)

# ── Fleches de transition entre actes ─────────────────────────────────────────
for ax_x in [5.75, 11.15]:
    arrow(ax_x - 0.05, 5.5, ax_x + 0.45, 5.5, color=SLATE, lw=2.5)

# ── Valeurs metier (bande basse) ──────────────────────────────────────────────
sbox(0.3, 0.08, 18.6, 0.8, SLATE, alpha=0.95)
txt(1.8, 0.48, "Valeurs produites :", size=8, color="#94A3B8", bold=True)

values = [
    ("Checklist 4x plus vite",   BRAND),
    ("Analyse ISO instantanee",  EMERALD),
    ("Explicabilite RAG",        CYAN),
    ("NC majeures 0 oubli",      RED),
    ("Expertise BV capitalisee", AMBER),
    ("Rapport en 1 clic",        VIOLET),
    ("90/10 inverse",            EMERALD),
    ("Multi-normes ready",       GRAY),
]
for i, (v, col) in enumerate(values):
    value_badge(4.0 + i * 1.97, 0.48, v, col)

# ── Sauvegarde ────────────────────────────────────────────────────────────────
out = "uc28-inspection/docs/architecture_fonctionnelle_UC28.png"
plt.tight_layout(pad=0)
plt.savefig(out, dpi=100, bbox_inches="tight", facecolor=MIST)
plt.close()
print(f"OK : {out}")
