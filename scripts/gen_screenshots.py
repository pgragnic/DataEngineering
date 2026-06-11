"""
Génère 6 mock screenshots de l'app UC28 — Inspection Augmentée
Sortie : docs/screenshots/*.png  (1280 × 720)
"""
from PIL import Image, ImageDraw, ImageFont
import os

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "uc28-inspection", "docs", "screenshots")
os.makedirs(OUT_DIR, exist_ok=True)

W, H = 1280, 720

# ── Palette ──────────────────────────────────────────────────────────────────
DARK_TEAL   = (0,   60,  80)
BRAND       = (0,  112, 173)
CYAN        = (0,  176, 240)
EMERALD     = (0,  126,  92)
AMBER       = (217, 122,   0)
AMBER_LIGHT = (255, 248, 225)
RED         = (192,  57,  43)
RED_LIGHT   = (255, 235, 233)
WHITE       = (255, 255, 255)
CANVAS      = (238, 242, 247)
SURFACE     = (255, 255, 255)
DIVIDER     = (207, 216, 220)
INK         = (26,  46,  59)
INK_MUTED   = (96, 125, 139)
SECTION_BG  = (227, 242, 253)
EMERALD_LIGHT=(232, 245, 233)
PURPLE      = (171,  71, 188)

FONT_R = "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"
FONT_B = "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"

def fnt(size, bold=False):
    path = FONT_B if bold else FONT_R
    return ImageFont.truetype(path, size)


# ── Drawing helpers ───────────────────────────────────────────────────────────

def new_img():
    img = Image.new("RGB", (W, H), CANVAS)
    return img, ImageDraw.Draw(img)


def rrect(d, xy, r, fill, outline=None, width=1):
    d.rounded_rectangle(xy, radius=r, fill=fill,
                        outline=outline, width=width)


def text(d, xy, txt, size=13, bold=False, color=INK, anchor="la"):
    d.text(xy, txt, font=fnt(size, bold), fill=color, anchor=anchor)


def header(d, title, subtitle, breadcrumbs):
    """Dark-teal header bar, 70px"""
    d.rectangle([0, 0, W, 70], fill=DARK_TEAL)
    # BV logo
    rrect(d, [14, 14, 50, 56], 4, WHITE)
    text(d, (32, 35), "BV", 13, True, DARK_TEAL, "mm")
    # Title
    text(d, (62, 22), title, 15, True, WHITE)
    text(d, (62, 44), subtitle, 10, False, CYAN)
    # Breadcrumb bar
    d.rectangle([0, 70, W, 90], fill=(0, 40, 55))
    x = 12
    for i, (label, active) in enumerate(breadcrumbs):
        if i > 0:
            text(d, (x, 80), "›", 10, False, (100, 130, 145), "lm")
            x += 14
        c = WHITE if active else CYAN
        text(d, (x, 80), label, 10, active, c, "lm")
        bbox = fnt(10, active).getbbox(label)
        x += (bbox[2] - bbox[0]) + 8


def page_bar(d, left_text, right_btn, btn_color=BRAND, badge_text=None, badge_color=None):
    """White page-bar, 48px, at y=90"""
    d.rectangle([0, 90, W, 138], fill=SURFACE)
    d.line([0, 138, W, 138], fill=DIVIDER, width=1)
    text(d, (18, 114), left_text, 11, False, BRAND, "lm")
    # right button
    bw = max(160, len(right_btn) * 8 + 32)
    rrect(d, [W - bw - 14, 100, W - 14, 132], 6, btn_color)
    text(d, (W - bw//2 - 14, 116), right_btn, 11, True, WHITE, "mm")
    # badge centre
    if badge_text:
        blen = len(badge_text) * 7 + 20
        bc = badge_color or AMBER
        # lighter badge with border
        bx = W//2 - blen//2
        rrect(d, [bx, 104, bx + blen, 130], 12,
              (*bc, 30) if len(bc) == 3 else bc,
              outline=bc, width=1)
        # draw text in badge color
        d.text((W//2, 117), badge_text, font=fnt(10, True), fill=bc, anchor="mm")


def card(d, xy, fill=SURFACE, outline=DIVIDER, width=1, r=8):
    rrect(d, xy, r, fill, outline, width)


def section_title(d, x, y, text_val, color=INK_MUTED):
    text(d, (x, y), text_val.upper(), 9, True, color)


def bullet_list(d, x, y, items, size=11, color=INK, spacing=20, bold_key=False):
    for item in items:
        if bold_key and ":" in item:
            k, _, v = item.partition(":")
            text(d, (x, y), f"•  {k}:", size, True, BRAND, "la")
            bw = fnt(size, True).getbbox(f"•  {k}:")[2]
            text(d, (x + bw + 4, y), v, size, False, INK_MUTED, "la")
        else:
            text(d, (x, y), f"•  {item}", size, False, color, "la")
        y += spacing
    return y


def tag(d, x, y, label, color, text_color=WHITE, r=4):
    bw = len(label) * 7 + 12
    rrect(d, [x, y, x + bw, y + 20], r, color)
    text(d, (x + bw//2, y + 10), label, 9, True, text_color, "mm")
    return x + bw + 6


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 1 — Dashboard
# ══════════════════════════════════════════════════════════════════════════════

def screen_dashboard():
    img, d = new_img()
    header(d, "BV — Inspection Augmentée",
           "UC 28 · Hackathon Capgemini × Anthropic 2026",
           [("Accueil", False), ("Planning", True)])
    page_bar(d, "← Retour", "Générez votre planning →", BRAND)

    TOP = 148

    # ── KPIs ──────────────────────────────────────────────────────────────────
    kpis = [
        ("10", "Audits planifiés", BRAND),
        ("2",  "Terminés",         EMERALD),
        ("1",  "En cours",         AMBER),
        ("7",  "Planifiés",        INK_MUTED),
    ]
    for i, (val, label, c) in enumerate(kpis):
        x0 = 14 + i * 200
        card(d, [x0, TOP, x0 + 188, TOP + 68])
        text(d, (x0 + 30, TOP + 34), val, 26, True, c, "mm")
        text(d, (x0 + 66, TOP + 34), label, 11, False, INK_MUTED, "lm")

    # ── Toggle Liste/Planning ──────────────────────────────────────────────────
    TOP2 = TOP + 80
    rrect(d, [14, TOP2, 130, TOP2 + 30], 6, SECTION_BG)
    rrect(d, [14, TOP2, 72, TOP2 + 30], 6, BRAND)
    text(d, (43, TOP2 + 15), "Liste", 11, True, WHITE, "mm")
    text(d, (101, TOP2 + 15), "Planning", 11, False, BRAND, "mm")

    # Filtre pills
    for i, (label, active) in enumerate([("Tous", True), ("TERMINÉ", False), ("PROCHAIN", False), ("PLANIFIÉ", False)]):
        px = 145 + i * 90
        rrect(d, [px, TOP2, px + 80, TOP2 + 30], 15,
              BRAND if active else SURFACE,
              outline=BRAND if active else DIVIDER, width=1)
        text(d, (px + 40, TOP2 + 15), label, 10, active,
             WHITE if active else INK_MUTED, "mm")

    # ── Mission cards ──────────────────────────────────────────────────────────
    TOP3 = TOP2 + 42
    missions = [
        ("TERMINÉ",  EMERALD,  "Atelier Châtillon",        "8:30 · 90 min · §9.1",  "✓  Terminé"),
        ("PROCHAIN", BRAND,    "Atelier Sucy-en-Brie",     "10:45 · 150 min · §7.1.5", "▶  Démarrer"),
        ("PLANIFIÉ", INK_MUTED,"Fontenay-sous-Bois",       "14:00 · 120 min · §8.7",  "▶  Démarrer"),
        ("PLANIFIÉ", INK_MUTED,"Siège RATP — DQ",          "16:30 · 60 min · §7.2",   "▶  Démarrer"),
    ]
    for i, (statut, sc, site, info, btn) in enumerate(missions):
        y0 = TOP3 + i * 78
        card(d, [14, y0, 780, y0 + 70])
        # statut badge
        rrect(d, [18, y0 + 8, 18 + 44, y0 + 44], 6, sc)
        text(d, (40, y0 + 26), statut[0], 16, True, WHITE, "mm")
        # site
        text(d, (74, y0 + 18), site, 13, True, INK)
        text(d, (74, y0 + 38), info, 10, False, INK_MUTED)
        # bouton
        bw = 110
        bc = EMERALD if statut == "TERMINÉ" else (BRAND if statut == "PROCHAIN" else INK_MUTED)
        rrect(d, [670, y0 + 18, 670 + bw, y0 + 48], 6, bc)
        text(d, (670 + bw//2, y0 + 33), btn, 10, True, WHITE, "mm")

    # ── Carte sidebar ──────────────────────────────────────────────────────────
    card(d, [800, TOP, W - 14, H - 14])
    text(d, (816, TOP + 12), "PARCOURS SÉLECTIONNÉ", 9, True, INK_MUTED)
    # Map placeholder
    rrect(d, [810, TOP + 32, W - 24, H - 24], 6, (230, 240, 235))
    # Fake map roads
    d.line([900, 200, 1100, 280], fill=(180, 200, 190), width=3)
    d.line([850, 280, 1050, 380], fill=(180, 200, 190), width=3)
    d.line([900, 200, 980, 420], fill=(200, 210, 200), width=2)
    # Route orange
    d.line([920, 190, 970, 260, 1000, 340, 1040, 420], fill=(249, 115, 22), width=4)
    # Markers
    for mx, my, mc, ml in [(920, 190, BRAND, "A"), (970, 260, BRAND, "1"),
                            (1000, 340, BRAND, "2"), (1040, 420, EMERALD, "3")]:
        rrect(d, [mx-12, my-12, mx+12, my+12], 12, mc)
        text(d, (mx, my), ml, 10, True, WHITE, "mm")
    text(d, (W//2 + 100, H - 36), "OpenStreetMap · OSRM routing", 9, False, INK_MUTED, "mm")

    img.save(os.path.join(OUT_DIR, "01_dashboard.png"))
    print("✓ 01_dashboard.png")


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 2 — Brief (contexte + historique)
# ══════════════════════════════════════════════════════════════════════════════

def screen_brief_context():
    img, d = new_img()
    header(d, "BV — Inspection Augmentée",
           "Audit ISO 9001 · Atelier Sucy-en-Brie · Marc Lefèvre",
           [("Accueil", False), ("Planning", False), ("Ma sélection", False), ("Brief", True)])
    page_bar(d, "← Retour au dashboard", "Démarrer l'inspection →",
             EMERALD, "EN PRÉPARATION", AMBER)

    TOP = 148
    CW = (W - 42) // 3  # ~412px per column

    # ── Col 1 — Contexte site ──────────────────────────────────────────────────
    x1 = 14
    card(d, [x1, TOP, x1 + CW, H - 14])
    section_title(d, x1 + 12, TOP + 12, "Contexte du site")
    ctx = [
        ("Site", "Atelier Sucy-en-Brie"),
        ("Réf.", "RATP-SUC"),
        ("Périmètre", "Maintenance MI09 — RER A"),
        ("Effectif", "218 personnes"),
        ("Resp. qualité", "Mei Lin Zhang"),
        ("Référentiel", "ISO 9001:2015"),
    ]
    y = TOP + 34
    for k, v in ctx:
        text(d, (x1 + 12, y), k + " :", 10, True, BRAND)
        text(d, (x1 + 12, y + 16), v, 11, False, INK)
        y += 40
    # Audit header
    d.line([x1 + 12, y + 4, x1 + CW - 12, y + 4], fill=DIVIDER, width=1)
    y += 16
    text(d, (x1 + 12, y), "RÉFÉRENTIEL", 9, True, INK_MUTED)
    y += 18
    for ref, active in [("ISO 9001:2015", True), ("ISO 14001:2015", False), ("ISO 45001:2018", False)]:
        rrect(d, [x1 + 12, y, x1 + CW - 12, y + 26], 13,
              BRAND if active else SURFACE,
              outline=BRAND if active else DIVIDER, width=1)
        text(d, (x1 + CW//2, y + 13), ref, 10, active,
             WHITE if active else INK_MUTED, "mm")
        y += 32

    # ── Col 2 — Historique + NC ────────────────────────────────────────────────
    x2 = x1 + CW + 7
    card(d, [x2, TOP, x2 + CW, H - 14])
    section_title(d, x2 + 12, TOP + 12, "Audits précédents  ⓘ")

    audits = [
        ("Nov 2024", "NC MAJEURE §7.1.5", RED, True),
        ("Mai 2024", "NC MINEURE §8.7", AMBER, False),
        ("Nov 2023", "Conforme", EMERALD, False),
    ]
    y = TOP + 34
    for date, label, c, alert in audits:
        bg = RED_LIGHT if alert else SURFACE
        rrect(d, [x2 + 8, y, x2 + CW - 8, y + 46], 6, bg,
              outline=c if alert else DIVIDER, width=2 if alert else 1)
        text(d, (x2 + 18, y + 10), date, 10, True, INK_MUTED)
        text(d, (x2 + 18, y + 26), label, 11, True, c)
        if alert:
            tag(d, x2 + CW - 95, y + 12, "⚠ OUVERTE", RED)
        y += 56

    # NC alerte box
    rrect(d, [x2 + 8, y + 8, x2 + CW - 8, y + 80], 6, RED_LIGHT, RED, 2)
    text(d, (x2 + 18, y + 18), "NC §7.1.5 — jamais clôturée", 11, True, RED)
    text(d, (x2 + 18, y + 36), "Certificats étalonnage périmés", 10, False, INK)
    text(d, (x2 + 18, y + 52), "Action corrective non réalisée", 10, False, INK)
    text(d, (x2 + 18, y + 68), "→  Section S2 prioritaire", 10, True, RED)

    # Thèmes récurrents
    y += 100
    text(d, (x2 + 12, y), "THÈMES RÉCURRENTS  🔁", 9, True, AMBER)
    y += 18
    for t in ["Étalonnage équipements (§7.1.5)", "Gestion pièces NC (§8.7)"]:
        rrect(d, [x2 + 8, y, x2 + CW - 8, y + 24], 12,
              AMBER_LIGHT, outline=AMBER, width=1)
        text(d, (x2 + 18, y + 12), t, 10, False, AMBER, "lm")
        y += 30

    # ── Col 3 — Check-list générée ────────────────────────────────────────────
    x3 = x2 + CW + 7
    card(d, [x3, TOP, x3 + CW, H - 14])
    section_title(d, x3 + 12, TOP + 12, "Check-list auto-générée  ⓘ")

    sections = [
        ("S1", "§7.5", "Maîtrise documentaire", INK_MUTED, False, False),
        ("S2", "§7.1.5", "Étalonnage & équipements", RED, True, True),
        ("S3", "§8.7", "Gestion des non-conformités", AMBER, False, True),
        ("S4", "§7.2", "Compétences", INK_MUTED, False, False),
    ]
    y = TOP + 34
    for code, clause, titre, c, nc_alert, recur in sections:
        bg = RED_LIGHT if nc_alert else (AMBER_LIGHT if recur else SURFACE)
        rrect(d, [x3 + 8, y, x3 + CW - 8, y + 58], 6, bg,
              outline=c, width=2 if nc_alert or recur else 1)
        # Section badge
        rrect(d, [x3 + 14, y + 8, x3 + 40, y + 28], 4, c)
        text(d, (x3 + 27, y + 18), code, 10, True, WHITE, "mm")
        text(d, (x3 + 48, y + 12), clause, 9, True, c)
        text(d, (x3 + 48, y + 26), titre, 10, True, INK)
        if recur:
            tag(d, x3 + CW - 65, y + 10, "🔁", AMBER)
        if nc_alert:
            tag(d, x3 + CW - 100, y + 30, "NC OUVERTE", RED)
        y += 66

    img.save(os.path.join(OUT_DIR, "02_brief_context.png"))
    print("✓ 02_brief_context.png")


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 3 — Brief (scope + docs portail)
# ══════════════════════════════════════════════════════════════════════════════

def screen_brief_scope():
    img, d = new_img()
    header(d, "BV — Inspection Augmentée",
           "Audit ISO 9001 · Atelier Sucy-en-Brie · Marc Lefèvre",
           [("Accueil", False), ("Planning", False), ("Ma sélection", False), ("Brief", True)])
    page_bar(d, "← Retour au dashboard", "Démarrer l'inspection →",
             EMERALD, "EN PRÉPARATION", AMBER)

    TOP = 148
    CW = (W - 42) // 3

    # ── Col 1 — Scope de l'audit ───────────────────────────────────────────────
    x1 = 14
    card(d, [x1, TOP, x1 + CW, H - 14])
    section_title(d, x1 + 12, TOP + 12, "Scope de l'audit")

    scopes = [
        "Étalonnage & équipements de mesure",
        "Gestion des non-conformités",
        "Maîtrise documentaire",
    ]
    y = TOP + 34
    for scope in scopes:
        rrect(d, [x1 + 8, y, x1 + CW - 8, y + 30], 6,
              SECTION_BG, outline=BRAND, width=1)
        text(d, (x1 + 20, y + 15), "✓  " + scope, 10, False, BRAND, "lm")
        y += 36

    # Ajouter scope btn
    rrect(d, [x1 + 8, y + 6, x1 + CW - 8, y + 34], 6, SURFACE, DIVIDER, 1)
    text(d, (x1 + CW//2, y + 20), "+ Ajouter un scope de l'audit", 10, False, BRAND, "mm")
    y += 50

    # Séparateur
    d.line([x1 + 12, y, x1 + CW - 12, y], fill=DIVIDER, width=1)
    y += 14

    section_title(d, x1 + 12, y, "Saisie d'un point personnalisé")
    y += 20
    rrect(d, [x1 + 8, y, x1 + CW - 8, y + 32], 4, SURFACE, BRAND, 2)
    text(d, (x1 + 18, y + 16), "Vérifier badge auditeur pièce 12…", 10, False, INK_MUTED, "lm")
    y += 38
    rrect(d, [x1 + 8, y, x1 + CW - 8, y + 30], 6, SECTION_BG, BRAND, 1)
    text(d, (x1 + 20, y + 15), "⬥  Vérifier badge auditeur pièce 12", 10, False, INK, "lm")
    tag(d, x1 + CW - 85, y + 7, "Auditeur", BRAND)

    # ── Col 2 — Documents portail RATP ────────────────────────────────────────
    x2 = x1 + CW + 7
    card(d, [x2, TOP, x2 + CW, H - 14], outline=AMBER)
    # Warning header
    rrect(d, [x2, TOP, x2 + CW, TOP + 32], 8, AMBER)
    text(d, (x2 + CW//2, TOP + 16), "⚠  Documents portail RATP", 11, True, WHITE, "mm")

    docs = [
        ("CR_Audit_nov_2024.docx",      "WORD",  "Compte-rendu · nov 2024",       "§7.1.5 à risque", AMBER),
        ("Procedures_etalonnage_v3.pdf", "PDF",   "Procédure métrologie v3",        "3 équipements périmés", RED),
        ("Plan_qualite_2025.pdf",        "PDF",   "Plan qualité 2025",              "Budget non alloué", RED),
    ]
    y = TOP + 42
    for nom, typ, desc, alert_txt, ac in docs:
        rrect(d, [x2 + 8, y, x2 + CW - 8, y + 72], 6, SURFACE, DIVIDER, 1)
        tc = RED if typ == "PDF" else BRAND
        rrect(d, [x2 + 14, y + 8, x2 + 52, y + 30], 4, tc)
        text(d, (x2 + 33, y + 19), typ, 9, True, WHITE, "mm")
        text(d, (x2 + 60, y + 14), nom, 10, True, INK)
        text(d, (x2 + 60, y + 30), desc, 9, False, INK_MUTED)
        # Alert
        rrect(d, [x2 + 14, y + 44, x2 + 14 + len(alert_txt) * 7 + 14, y + 64], 4, (255, 243, 205))
        text(d, (x2 + 21, y + 54), "⚠  " + alert_txt, 9, True, AMBER, "lm")
        # download
        text(d, (x2 + CW - 28, y + 14), "⬇", 14, False, BRAND)
        y += 80

    # ── Col 3 — Check-list éditable ───────────────────────────────────────────
    x3 = x2 + CW + 7
    card(d, [x3, TOP, x3 + CW, H - 14])
    section_title(d, x3 + 12, TOP + 12, "Check-list éditable")

    items = [
        ("S2 §7.1.5", "Vérif. certificats d'étalonnage", True, "⚠"),
        ("S2 §7.1.5", "Registre des équipements", True, ""),
        ("S3 §8.7", "Traçabilité NC produits", True, ""),
        ("S3 §8.7", "Zone de quarantaine", True, ""),
        ("S1 §7.5", "Maîtrise documents qualité", False, ""),
    ]
    y = TOP + 34
    for clause, label, active, warn in items:
        bg = SECTION_BG if active else SURFACE
        rrect(d, [x3 + 8, y, x3 + CW - 8, y + 44], 6, bg,
              outline=BRAND if active else DIVIDER, width=1)
        text(d, (x3 + 18, y + 10), clause, 9, True, BRAND)
        text(d, (x3 + 18, y + 26), label, 10, False, INK)
        if warn:
            tag(d, x3 + CW - 68, y + 12, "⚠ RATP", AMBER)
        text(d, (x3 + CW - 26, y + 22), "×", 14, False, (180, 180, 180))
        y += 50

    # Ajouter point
    rrect(d, [x3 + 8, y + 4, x3 + CW - 8, y + 30], 6, SURFACE, DIVIDER, 1)
    text(d, (x3 + CW//2, y + 17), "+ Ajouter un point", 10, False, BRAND, "mm")

    img.save(os.path.join(OUT_DIR, "03_brief_scope.png"))
    print("✓ 03_brief_scope.png")


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 4 — Inspection (capture + résultat)
# ══════════════════════════════════════════════════════════════════════════════

def screen_inspection():
    img, d = new_img()
    header(d, "BV — Inspection Augmentée",
           "Audit ISO 9001 · Atelier Sucy-en-Brie · Marc Lefèvre",
           [("Accueil", False), ("Planning", False), ("Ma sélection", False),
            ("Brief", False), ("Inspection", True)])
    page_bar(d, "← Retour", "Pré-rapport →", DARK_TEAL, "EN COURS", EMERALD)

    # Bannière récurrences
    d.rectangle([0, 138, W, 168], fill=AMBER_LIGHT)
    d.line([0, 168, W, 168], fill=AMBER, width=1)
    text(d, (18, 153), "🔁  Points récurrents sur ce site : §7.1.5 Étalonnage · §8.7 NC", 10, True, AMBER, "lm")

    TOP = 176
    CW = (W - 28) // 3

    # ── Col 1 — Check-list ────────────────────────────────────────────────────
    x1 = 8
    card(d, [x1, TOP, x1 + CW, H - 8])
    section_title(d, x1 + 10, TOP + 10, "Check-list  ·  4 sections")

    items = [
        ("S2 §7.1.5", "Vérif. certificats étalonnage", RED, True, True),
        ("S2 §7.1.5", "Registre des équipements", BRAND, False, False),
        ("S2 §7.1.5", "Fréquence d'étalonnage", BRAND, False, False),
        ("S3 §8.7", "Traçabilité NC produits", AMBER, False, False),
        ("S3 §8.7", "Zone de quarantaine", AMBER, False, False),
        ("S1 §7.5", "Maîtrise documentaire", INK_MUTED, False, False),
    ]
    y = TOP + 30
    for clause, label, c, selected, nc in items:
        bg = RED_LIGHT if selected else SURFACE
        rrect(d, [x1 + 6, y, x1 + CW - 6, y + 42], 6, bg,
              outline=c if selected else DIVIDER, width=2 if selected else 1)
        # bullet
        bc = c if selected else DIVIDER
        d.ellipse([x1 + 13, y + 14, x1 + 27, y + 28], fill=bc)
        if selected:
            text(d, (x1 + 20, y + 21), "●", 8, True, WHITE, "mm")
        text(d, (x1 + 34, y + 8), clause, 9, True, c)
        text(d, (x1 + 34, y + 22), label, 10, selected, INK if not selected else RED)
        if selected:
            tag(d, x1 + CW - 60, y + 10, "🔁", AMBER)
        y += 48

    # ── Col 2 — Capture ────────────────────────────────────────────────────────
    x2 = x1 + CW + 6
    card(d, [x2, TOP, x2 + CW, H - 8])

    # Header "point en cours"
    rrect(d, [x2, TOP, x2 + CW, TOP + 44], 6, RED)
    text(d, (x2 + 10, TOP + 10), "POINT EN COURS", 9, True, (255, 180, 180))
    text(d, (x2 + 10, TOP + 26), "Vérif. certificats d'étalonnage", 11, True, WHITE)

    y = TOP + 54
    section_title(d, x2 + 10, y, "Observation")
    y += 18
    rrect(d, [x2 + 8, y, x2 + CW - 8, y + 68], 4, SURFACE, BRAND, 2)
    text(d, (x2 + 14, y + 10), "Les clés dynamométriques du poste 12", 10, False, INK)
    text(d, (x2 + 14, y + 26), "ne sont pas étalonnées. Certificats", 10, False, INK)
    text(d, (x2 + 14, y + 42), "périmés depuis 8 mois.", 10, False, INK)
    y += 78

    # Boutons micro / photo
    bw = (CW - 24) // 2
    rrect(d, [x2 + 8, y, x2 + 8 + bw, y + 32], 6, RED)
    text(d, (x2 + 8 + bw//2, y + 16), "🎙  Micro actif", 10, True, WHITE, "mm")
    rrect(d, [x2 + 16 + bw, y, x2 + 16 + 2*bw, y + 32], 6, INK_MUTED)
    text(d, (x2 + 16 + bw + bw//2, y + 16), "📷  Photo", 10, True, WHITE, "mm")
    y += 42

    # Questions oui/non
    section_title(d, x2 + 10, y, "Réponses suggérées par IA")
    y += 18
    questions = [
        ("✓ OUI", "Certificats < 12 mois ?",         EMERALD),
        ("✗ NON", "Accréditation COFRAC présente ?",  RED),
        ("—",     "Registre équipements à jour ?",    INK_MUTED),
    ]
    for rep, q, c in questions:
        rrect(d, [x2 + 8, y, x2 + CW - 8, y + 30], 4, SURFACE, DIVIDER, 1)
        rrect(d, [x2 + 8, y, x2 + 56, y + 30], 4, c)
        text(d, (x2 + 32, y + 15), rep, 9, True, WHITE, "mm")
        text(d, (x2 + 64, y + 15), q, 10, False, INK, "lm")
        y += 34

    y += 4
    rrect(d, [x2 + 8, y, x2 + CW - 8, y + 34], 6, BRAND)
    text(d, (x2 + CW//2, y + 17), "⚡  Analyser", 12, True, WHITE, "mm")

    # ── Col 3 — Résultat ───────────────────────────────────────────────────────
    x3 = x2 + CW + 6
    card(d, [x3, TOP, x3 + CW, H - 8])

    # NC MAJEURE header
    rrect(d, [x3, TOP, x3 + CW, TOP + 44], 6, RED)
    text(d, (x3 + 10, TOP + 10), "NC MAJEURE", 11, True, WHITE)
    text(d, (x3 + 10, TOP + 28), "§7.1.5 · Étalonnage", 10, False, (255, 180, 180))
    tag(d, x3 + CW - 110, TOP + 10, "🔁 RÉCIDIVE", AMBER)
    tag(d, x3 + CW - 130, TOP + 28, "Claude Opus", BRAND)

    y = TOP + 54
    fields = [
        ("Constat",     "Clés dynamométriques poste 12\nnon étalonnées. Certificats périmés."),
        ("Action",      "Étalonnage COFRAC immédiat."),
        ("Préventif",   "Planning automatisé des certif."),
        ("Délai",       "< 72 h · Mei Lin Zhang"),
        ("Norme",       "ISO 9001:2015 §7.1.5.1"),
    ]
    for k, v in fields:
        text(d, (x3 + 10, y), k + " :", 10, True, BRAND)
        for line in v.split("\n"):
            y += 16
            text(d, (x3 + 10, y), line, 10, False, INK)
        y += 10
    y += 4
    d.line([x3 + 10, y, x3 + CW - 10, y], fill=DIVIDER, width=1)
    y += 10

    # Feedback
    bw2 = (CW - 24) // 2
    rrect(d, [x3 + 8, y, x3 + 8 + bw2, y + 30], 6, EMERALD)
    text(d, (x3 + 8 + bw2//2, y + 15), "👍 Confirmer", 10, True, WHITE, "mm")
    rrect(d, [x3 + 16 + bw2, y, x3 + 16 + 2*bw2, y + 30], 6, SURFACE, INK_MUTED, 1)
    text(d, (x3 + 16 + bw2 + bw2//2, y + 15), "✏️ Corriger", 10, True, INK_MUTED, "mm")
    y += 40

    # Ajouter au rapport
    rrect(d, [x3 + 8, y, x3 + CW - 8, y + 30], 6, DARK_TEAL)
    text(d, (x3 + CW//2, y + 15), "+ Ajouter au rapport", 11, True, WHITE, "mm")

    img.save(os.path.join(OUT_DIR, "04_inspection.png"))
    print("✓ 04_inspection.png")


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 5 — Rapport
# ══════════════════════════════════════════════════════════════════════════════

def screen_report():
    img, d = new_img()
    header(d, "BV — Inspection Augmentée",
           "Audit ISO 9001 · Atelier Sucy-en-Brie · Marc Lefèvre",
           [("Accueil", False), ("Planning", False), ("Ma sélection", False),
            ("Brief", False), ("Inspection", False), ("Rapport", True)])
    page_bar(d, "← Retour à l'inspection", "⬆  Portail RATP", DARK_TEAL,
             "AUDIT TERMINÉ", BRAND)

    TOP = 148
    # 2 colonnes : 380 | rest
    CL = 380

    # ── Col gauche — synthèse ─────────────────────────────────────────────────
    card(d, [8, TOP, 8 + CL, H - 8])

    section_title(d, 18, TOP + 12, "Synthèse")
    counts = [("NC MAJEURES", "2", RED), ("NC MINEURES", "0", AMBER),
              ("OBSERVATIONS", "0", BRAND), ("CONFORMES", "2", EMERALD)]
    y = TOP + 32
    for label, val, c in counts:
        rrect(d, [14, y, 52, y + 36], 4, c)
        text(d, (33, y + 18), val, 16, True, WHITE, "mm")
        text(d, (58, y + 18), label, 10, False, INK_MUTED, "lm")
        y += 42

    # Score ring (fake)
    cx_r, cy_r, r_r = 100, y + 60, 45
    d.arc([cx_r - r_r, cy_r - r_r, cx_r + r_r, cy_r + r_r], -90, -90 + 360 * 0.35,
          fill=RED, width=8)
    d.arc([cx_r - r_r, cy_r - r_r, cx_r + r_r, cy_r + r_r], -90 + 360 * 0.35, 270,
          fill=DIVIDER, width=8)
    text(d, (cx_r, cy_r), "35%", 14, True, RED, "mm")
    text(d, (cx_r, cy_r + 16), "global", 9, False, INK_MUTED, "mm")
    text(d, (160, cy_r - 10), "Score de conformité", 10, True, INK)
    text(d, (160, cy_r + 8), "< 50% = alerte rouge", 9, False, INK_MUTED)

    y += 130
    d.line([14, y, CL - 6, y], fill=DIVIDER, width=1)
    y += 12
    section_title(d, 18, y, "Grille de conformité")
    y += 20
    conformite = [
        ("§7.1.5 Étalonnage",     18, RED),
        ("§8.7 Non-conformités",   42, AMBER),
        ("§7.5 Documentation",     80, EMERALD),
        ("§7.2 Compétences",       85, EMERALD),
    ]
    for section, pct, c in conformite:
        text(d, (14, y), section, 9, False, INK_MUTED)
        bar_w = 140
        bar_x = 220
        rrect(d, [bar_x, y + 2, bar_x + bar_w, y + 14], 4, DIVIDER)
        rrect(d, [bar_x, y + 2, bar_x + int(bar_w * pct / 100), y + 14], 4, c)
        text(d, (bar_x + bar_w + 8, y + 8), f"{pct}%", 9, True, c, "lm")
        y += 22

    # ── Col droite — rapport ──────────────────────────────────────────────────
    x2 = 8 + CL + 8
    card(d, [x2, TOP, W - 8, H - 8])
    section_title(d, x2 + 12, TOP + 12, "Rapport d'inspection — Atelier Sucy-en-Brie")
    text(d, (x2 + 12, TOP + 30), "Bureau Veritas · Marc Lefèvre · 11/06/2026", 10, False, INK_MUTED)

    y = TOP + 54
    constats = [
        ("NC MAJEURE", RED,   "§7.1.5", "Clés dynamométriques poste 12 non étalonnées.",
         "Action : étalonnage COFRAC immédiat · Délai : < 72 h"),
        ("NC MAJEURE", RED,   "§8.7",   "Zone de quarantaine non délimitée.",
         "Action : marquage physique de la zone · Délai : immédiat"),
    ]
    for statut, c, clause, obs, action in constats:
        rrect(d, [x2 + 8, y, W - 16, y + 72], 6, RED_LIGHT if c == RED else AMBER_LIGHT,
              outline=c, width=2)
        rrect(d, [x2 + 14, y + 8, x2 + 14 + 90, y + 28], 4, c)
        text(d, (x2 + 59, y + 18), statut, 9, True, WHITE, "mm")
        text(d, (x2 + 112, y + 12), clause, 9, True, c)
        text(d, (x2 + 112, y + 28), obs, 10, False, INK)
        text(d, (x2 + 14, y + 46), action, 9, False, INK_MUTED)
        y += 80

    # Actions en bas
    y += 8
    btns = [
        ("✏️  Modifier", INK_MUTED, SURFACE),
        ("🔒  Anonymiser RGPD", INK_MUTED, SURFACE),
        ("⬇  Télécharger Word", EMERALD, EMERALD_LIGHT),
        ("✍️  Signer", DARK_TEAL, DARK_TEAL),
    ]
    bx = x2 + 8
    for label, fc, bg in btns:
        bw_b = len(label) * 7 + 18
        rrect(d, [bx, y, bx + bw_b, y + 30], 6, bg,
              outline=fc, width=1)
        text(d, (bx + bw_b//2, y + 15), label, 10, True,
             WHITE if bg not in (SURFACE, EMERALD_LIGHT) else fc, "mm")
        bx += bw_b + 8

    # Bouton portail (highlight)
    rrect(d, [x2 + 8, y + 38, W - 16, y + 72], 6, DARK_TEAL)
    text(d, ((x2 + 8 + W - 16)//2, y + 55), "⬆  Portail RATP — Transmettre le rapport signé",
         12, True, WHITE, "mm")

    img.save(os.path.join(OUT_DIR, "05_report.png"))
    print("✓ 05_report.png")


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 6 — Portail fournisseur RATP (Mei Lin Zhang)
# ══════════════════════════════════════════════════════════════════════════════

def screen_portal():
    img, d = new_img()
    header(d, "BV — Inspection Augmentée",
           "Portail fournisseur · Mei Lin Zhang · RATP",
           [("Accueil", True)])
    page_bar(d, "← Changer de compte", "Déposer un document →", BRAND)

    TOP = 148

    # Filter bar
    d.rectangle([8, TOP, W - 8, TOP + 42], fill=SURFACE)
    d.line([8, TOP + 42, W - 8, TOP + 42], fill=DIVIDER, width=1)
    pills = [("Tous", True), ("Audit & Inspection", False), ("Procédures", False),
             ("Sécurité & Risques", False)]
    px = 18
    for label, active in pills:
        pw = len(label) * 8 + 20
        rrect(d, [px, TOP + 8, px + pw, TOP + 34], 13,
              BRAND if active else SURFACE,
              outline=BRAND if active else DIVIDER, width=1)
        text(d, (px + pw//2, TOP + 21), label, 10, active,
             WHITE if active else INK_MUTED, "mm")
        px += pw + 8

    # Sort
    text(d, (W - 130, TOP + 21), "Trier : Récent ▼", 10, False, INK_MUTED, "lm")

    TOP2 = TOP + 52

    # Documents
    docs = [
        # (nom, type_tag, type_color, date, analyse, is_bv, insights)
        ("Rapport d'audit BV — Sucy-en-Brie",
         "Rapport d'audit BV", BRAND, "11/06/2026",
         "2 NC majeures · §7.1.5 · §8.7 — actions correctives définies",
         True, "BUREAU VERITAS"),
        ("CR_Audit_nov_2024.docx",
         "Compte-rendu", INK_MUTED, "14/11/2024",
         "§7.1.5 NC MAJEURE · §8.7 NC MINEURE — plan d'actions joint",
         False, "Eve Dupont · RATP"),
        ("Procedures_etalonnage_v3.pdf",
         "Procédure interne", INK_MUTED, "02/01/2025",
         "3 équipements périmés · fréquence non respectée",
         False, "Eve Dupont · RATP"),
        ("Plan_qualite_2025.pdf",
         "Plan de prévention", INK_MUTED, "15/01/2025",
         "Budget étalonnage non alloué · zone quarantaine : partiel",
         False, "Eve Dupont · RATP"),
    ]
    y = TOP2
    for nom, typ, tc, date, analyse, is_bv, deposant in docs:
        h_card = 86
        bg_card = SECTION_BG if is_bv else SURFACE
        border = BRAND if is_bv else DIVIDER
        bw2 = 2 if is_bv else 1
        rrect(d, [8, y, W - 8, y + h_card], 8, bg_card, border, bw2)

        # Type badge
        rrect(d, [16, y + 10, 16 + len(typ) * 7 + 16, y + 30], 4, tc)
        text(d, (16 + (len(typ)*7+16)//2, y + 20), typ, 9, True, WHITE, "mm")

        # BV badge
        if is_bv:
            tag(d, W - 170, y + 10, "Bureau Veritas", BRAND)

        # Nom
        text(d, (16, y + 36), nom, 12, True, INK)
        text(d, (16, y + 54), f"{date} · Déposé par {deposant}", 9, False, INK_MUTED)
        text(d, (16, y + 68), analyse, 9, False, BRAND if is_bv else INK_MUTED)

        # Boutons
        bx_btn = W - 270
        for label, c in [("Voir analyse ▼", BRAND), ("⬇ Télécharger", EMERALD)]:
            bw_b = len(label) * 7 + 16
            rrect(d, [bx_btn, y + 30, bx_btn + bw_b, y + 54], 6,
                  SECTION_BG if c == BRAND else EMERALD_LIGHT,
                  outline=c, width=1)
            text(d, (bx_btn + bw_b//2, y + 42), label, 9, True, c, "mm")
            bx_btn += bw_b + 8

        y += h_card + 8

    img.save(os.path.join(OUT_DIR, "06_portal.png"))
    print("✓ 06_portal.png")


# ── Run all ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    screen_dashboard()
    screen_brief_context()
    screen_brief_scope()
    screen_inspection()
    screen_report()
    screen_portal()
    print(f"\n✓ 6 screenshots → {OUT_DIR}")
