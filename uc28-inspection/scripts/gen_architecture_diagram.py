"""
Génère le schéma d'architecture technique UC28 — Inspection Augmentée
en PNG 1920×1080 (16:9).
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe

# ── Palette UC28 ─────────────────────────────────────────────────────────────
BRAND    = "#0070AD"
EMERALD  = "#059669"
SLATE    = "#1E293B"
MIST     = "#EEF2F7"
AMBER    = "#D97706"
VIOLET   = "#7C3AED"
GRAY     = "#64748B"
WHITE    = "#FFFFFF"
DIVIDER  = "#CBD5E1"
INK      = "#1E293B"
CYAN     = "#0891B2"

fig, ax = plt.subplots(figsize=(19.2, 10.8), dpi=100)
fig.patch.set_facecolor(MIST)
ax.set_xlim(0, 19.2)
ax.set_ylim(0, 10.8)
ax.axis("off")


# ── Helpers ──────────────────────────────────────────────────────────────────

def box(x, y, w, h, color, radius=0.25, alpha=1.0, edge=None, lw=1.5, ls="-"):
    edge = edge or color
    p = FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0",
        facecolor=color, edgecolor=edge,
        linewidth=lw, linestyle=ls, alpha=alpha,
        zorder=2,
    )
    ax.add_patch(p)

def sbox(x, y, w, h, color, radius=0.25, alpha=1.0, edge=None, lw=1.5):
    """Shadow + box."""
    # shadow
    ps = FancyBboxPatch(
        (x + 0.06, y - 0.06), w, h,
        boxstyle="round,pad=0",
        facecolor="#00000015", edgecolor="none",
        linewidth=0, zorder=1,
    )
    ax.add_patch(ps)
    box(x, y, w, h, color, radius, alpha, edge, lw)

def label(x, y, text, size=9, color=WHITE, bold=False, ha="center", va="center", zorder=5):
    weight = "bold" if bold else "normal"
    ax.text(x, y, text, fontsize=size, color=color, fontweight=weight,
            ha=ha, va=va, zorder=zorder, wrap=False)

def pill(x, y, text, bg, fg=WHITE, size=7.5):
    ax.text(x, y, f"  {text}  ", fontsize=size, color=fg,
            ha="center", va="center", zorder=6,
            bbox=dict(boxstyle="round,pad=0.18", facecolor=bg, edgecolor="none"))

def arrow(x1, y1, x2, y2, color=GRAY, lw=1.5, style="->", dashed=False):
    ls = (0, (4, 3)) if dashed else "solid"
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle=style, color=color, lw=lw,
                                linestyle=ls, connectionstyle="arc3,rad=0.0"),
                zorder=3)

def darrow(x1, y1, x2, y2, color=GRAY, lw=1.5):
    """Double-headed arrow."""
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="<->", color=color, lw=lw,
                                connectionstyle="arc3,rad=0.0"),
                zorder=3)

def section_label(x, y, text, color):
    ax.text(x, y, text, fontsize=7.5, color=color, fontweight="bold",
            ha="center", va="center", zorder=6,
            bbox=dict(boxstyle="round,pad=0.22", facecolor=color + "22",
                      edgecolor=color, linewidth=1.2))


# ── Titre ─────────────────────────────────────────────────────────────────────
sbox(0.4, 10.0, 18.4, 0.65, SLATE)
label(9.6, 10.33, "UC28 — Inspection Augmentée · Architecture Technique", size=13, bold=True)
label(9.6, 10.05, "React + Vite  ·  FastAPI  ·  Claude claude-sonnet-4-6  ·  sentence-transformers + FAISS  ·  SQLite",
      size=8, color="#94A3B8")

# ── Zone 1 : Navigateurs / Utilisateurs ──────────────────────────────────────
sbox(0.4, 8.0, 3.5, 1.75, "#F8FAFC", edge=DIVIDER, lw=1.2)
section_label(2.15, 9.62, "Utilisateurs", GRAY)

sbox(0.6, 8.15, 1.4, 1.25, BRAND)
label(1.30, 8.95, "Marc Lefèvre", size=8, bold=True)
label(1.30, 8.72, "Auditeur BV", size=7)
label(1.30, 8.50, "Browser", size=7.5)

sbox(2.15, 8.15, 1.55, 1.25, EMERALD)
label(2.925, 8.95, "Mei Lin Zhang", size=8, bold=True)
label(2.925, 8.72, "Qualité RATP", size=7)
label(2.925, 8.50, "Browser", size=7.5)

# ── Zone 2 : Frontend ─────────────────────────────────────────────────────────
sbox(4.3, 6.8, 5.0, 3.0, "#F0F9FF", edge=BRAND, lw=1.5)
section_label(6.8, 9.68, "Frontend  —  React + Vite  (port 5173)", BRAND)

# Composants React
comps = [
    ("LoginScreen", 4.5, 8.75),
    ("ClientList", 5.55, 8.75),
    ("PlanningOverlay", 6.85, 8.75),
    ("SelectionView", 8.1, 8.75),
    ("Brief", 4.8, 7.85),
    ("InspectionCapture", 6.35, 7.85),
    ("ReportView", 8.1, 7.85),
    ("SupplierPortal", 5.6, 6.98),
]
comp_colors = [BRAND, BRAND, BRAND, BRAND, CYAN, CYAN, CYAN, EMERALD]
for (name, cx, cy), col in zip(comps, comp_colors):
    ax.text(cx, cy, name, fontsize=7, color=WHITE, fontweight="bold",
            ha="center", va="center", zorder=6,
            bbox=dict(boxstyle="round,pad=0.22", facecolor=col, edgecolor="none"))

# App.jsx router
sbox(4.5, 7.4, 4.7, 0.35, SLATE, alpha=0.85)
label(6.85, 7.575, "App.jsx — Routeur d'état  (login → clients → brief → inspection → report)", size=7)

label(6.85, 9.30, "SPA 8 écrans · Tailwind CSS · Lucide React", size=7.5, color=BRAND, bold=False)

# ── Zone 3 : Backend ──────────────────────────────────────────────────────────
sbox(4.3, 3.5, 5.0, 3.0, "#F0FDF4", edge=EMERALD, lw=1.5)
section_label(6.8, 6.38, "Backend  —  FastAPI + Uvicorn  (port 8000)", EMERALD)

# Modules backend
mods = [
    ("main.py\nRoutes FastAPI", 4.65, 5.65, EMERALD),
    ("agent.py\nLogique métier + LLM", 6.35, 5.65, EMERALD),
    ("rag.py\nRAG ISO 9001", 7.95, 5.65, EMERALD),
    ("database.py\nSQLite ORM", 5.5, 4.65, SLATE),
]
for (name, bx, by, col) in mods:
    sbox(bx - 0.6, by - 0.38, 1.3, 0.78, col)
    lines = name.split("\n")
    label(bx, by + 0.10, lines[0], size=7.5, bold=True)
    if len(lines) > 1:
        label(bx, by - 0.14, lines[1], size=6.5)

# Routes pills
routes = [
    "POST /analyser", "POST /synthetiser", "POST /suggestions",
    "POST /questions_oui_non", "POST /analyser_document_fournisseur",
]
for i, r in enumerate(routes):
    col = i % 2
    row = i // 2
    pill(4.8 + col * 2.2 + (0 if i < 4 else 1.1), 3.72 + row * 0.27, r,
         bg=EMERALD + "33", fg=EMERALD, size=6.5)

# ── Zone 4 : Données ──────────────────────────────────────────────────────────
sbox(0.4, 3.5, 3.5, 3.0, "#FAFAFA", edge=DIVIDER, lw=1.2)
section_label(2.15, 6.38, "Données", SLATE)

sbox(0.6, 5.55, 3.1, 0.68, SLATE)
label(2.15, 5.98, "audit.db (SQLite)", size=8.5, bold=True)
label(2.15, 5.72, "clauses_iso · sites · audits_historiques", size=7)

sbox(0.6, 4.70, 3.1, 0.68, SLATE, alpha=0.85)
label(2.15, 5.13, "mockData.js", size=8.5, bold=True)
label(2.15, 4.87, "CHECKLIST · RAG_ARTICLES · KPIS · AUDITS", size=7)

sbox(0.6, 3.7, 1.45, 0.72, "#7C3AED")
label(1.325, 4.14, "FAISS Index", size=8, bold=True)
label(1.325, 3.88, "embeddings", size=7)

sbox(2.2, 3.7, 1.5, 0.72, "#7C3AED")
label(2.95, 4.14, "sentence-", size=8, bold=True)
label(2.95, 3.88, "transformers", size=7)

# ── Zone 5 : Services externes ────────────────────────────────────────────────
sbox(9.7, 6.8, 4.8, 3.0, "#FFF7ED", edge=AMBER, lw=1.5)
section_label(12.1, 9.68, "Services externes", AMBER)

# Claude API
sbox(9.9, 8.2, 4.4, 1.35, SLATE)
label(12.1, 9.08, "Claude API  —  Anthropic", size=9, bold=True)
label(12.1, 8.78, "claude-sonnet-4-6", size=8, color="#94A3B8")
pill(10.5, 8.42, "Prompt caching", bg=AMBER, size=7)
pill(12.1, 8.42, "System prompt", bg=AMBER, size=7)
pill(13.5, 8.42, "Tool use", bg=AMBER, size=7)

# Fonctions Claude
funcs = ["analyser_observation()", "enrichir_diagnostic_llm()", "synthetiser_observation()",
         "generer_suggestions()", "generer_questions_oui_non()", "analyser_document_fournisseur()"]
for i, f in enumerate(funcs):
    col = i % 2
    row = i // 2
    ax.text(10.1 + col * 2.35, 8.05 - row * 0.28, f"→ {f}", fontsize=6.3,
            color="#D97706", ha="left", va="center", zorder=5)

# OpenStreetMap / OSRM
sbox(9.9, 6.95, 4.4, 0.72, "#065F46")
label(12.1, 7.38, "OSRM  —  Routing voiture / vélo / pied", size=8.5, bold=True)
label(12.1, 7.1, "router.project-osrm.org  (MapCard — tracé itinéraire)", size=7, color="#6EE7B7")

# ── Zone 6 : Légende flux ─────────────────────────────────────────────────────
sbox(9.7, 3.5, 4.8, 3.0, "#FAFAFA", edge=DIVIDER, lw=1.2)
section_label(12.1, 6.38, "Flux de donnees cles", GRAY)

flows = [
    ("Sélection item checklist", "→ RAG clause lookup → articles normatifs", BRAND),
    ("Observation terrain", "→ POST /analyser → Claude (criticité + action)", EMERALD),
    ("Analyse bcknd result", "→ articlesForClause() → mise à jour RAG panel", CYAN),
    ("Dépôt document", "→ POST /analyser_document_fournisseur → Claude", AMBER),
    ("Planning journée", "→ OSRM API → itinéraire routier sur carte", "#065F46"),
    ("Login fournisseur", "→ SupplierPortal → Brief docs portail RATP", VIOLET),
]
for i, (a_text, b_text, col) in enumerate(flows):
    y = 6.08 - i * 0.41
    ax.plot([10.0, 10.25], [y, y], color=col, lw=2, solid_capstyle="round", zorder=4)
    ax.text(10.35, y + 0.04, a_text, fontsize=7, color=INK, fontweight="bold",
            ha="left", va="center", zorder=5)
    ax.text(10.35, y - 0.16, b_text, fontsize=6.5, color=GRAY,
            ha="left", va="center", zorder=5)

# ── Flèches de connexion ──────────────────────────────────────────────────────

# Navigateur → Frontend
darrow(3.9, 9.0, 4.3, 9.0, color=BRAND, lw=2)
label(4.1, 9.15, "HTTP", size=6.5, color=BRAND)

# Frontend → Backend
darrow(6.8, 6.8, 6.8, 6.5, color=EMERALD, lw=2)
label(7.15, 6.65, "REST /api", size=6.5, color=EMERALD)

# Backend → Claude API
darrow(9.3, 5.65, 9.9, 8.55, color=AMBER, lw=1.8, )
label(9.7, 7.5, "HTTPS", size=6.5, color=AMBER)

# Backend → SQLite
darrow(4.3, 5.05, 3.7, 5.05, color=SLATE, lw=1.8)
label(4.0, 5.22, "SQL", size=6.5, color=SLATE)

# Backend → FAISS/RAG
darrow(5.5, 3.5, 2.2, 4.14, color=VIOLET, lw=1.8)
label(3.8, 3.9, "encode +\nsearch", size=6.3, color=VIOLET)

# Backend → OSRM
darrow(9.3, 4.5, 9.9, 7.1, color="#065F46", lw=1.5)

# mockData → Frontend
arrow(3.7, 4.87, 4.3, 7.4, color=CYAN, lw=1.5, dashed=True)
label(3.95, 6.0, "import", size=6.3, color=CYAN)

# ── Footer ────────────────────────────────────────────────────────────────────
sbox(0.4, 0.1, 18.4, 0.55, SLATE, alpha=0.9)
label(5.0, 0.37, "Hackathon Capgemini × Anthropic 2026", size=8, color="#94A3B8")
label(9.6, 0.37, "Code Resonance — Habib KOFFI · Véronique POILANE ZHANG · Philippe GRAGNIC",
      size=8, color=WHITE, bold=False)
label(16.5, 0.37, "UC28 — v1.0", size=8, color="#94A3B8")

# ── Sauvegarde ────────────────────────────────────────────────────────────────
out = "uc28-inspection/docs/architecture_technique_UC28.png"
plt.tight_layout(pad=0)
plt.savefig(out, dpi=100, bbox_inches="tight", facecolor=MIST)
plt.close()
print(f"✓ Schéma généré : {out}")
