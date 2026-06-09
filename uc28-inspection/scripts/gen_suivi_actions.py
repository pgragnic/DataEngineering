"""
Génère RATP_Sucy_Suivi_Actions_Correctives_2026.pdf
Cohérent avec les données du Brief UC28 (Mei Lin Zhang, alertes §7.1.5 et §8.7)
"""

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, HRFlowable
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY

RED_RATP  = colors.HexColor("#CC0000")
DARK_GRAY = colors.HexColor("#2C2C2C")
MID_GRAY  = colors.HexColor("#666666")
LIGHT_BG  = colors.HexColor("#F5F5F5")
RED_BG    = colors.HexColor("#FFF0F0")
GREEN     = colors.HexColor("#1A7A1A")
ORANGE    = colors.HexColor("#CC6600")
TABLE_HDR = colors.HexColor("#CC0000")

OUT = "uc28-inspection/docs/RATP_Sucy_Suivi_Actions_Correctives_2026.pdf"

doc = SimpleDocTemplate(
    OUT, pagesize=A4,
    leftMargin=2*cm, rightMargin=2*cm,
    topMargin=1.8*cm, bottomMargin=2*cm,
    title="Rapport de Suivi des Actions Correctives ISO 9001:2015",
    author="RATP — Direction de la Maintenance Ferroviaire",
)

styles = getSampleStyleSheet()

def style(name, **kw):
    s = ParagraphStyle(name, parent=styles["Normal"], **kw)
    return s

S_header_left  = style("HL", fontSize=9, textColor=MID_GRAY)
S_header_title = style("HT", fontSize=16, textColor=RED_RATP, alignment=TA_CENTER,
                        fontName="Helvetica-Bold", spaceAfter=4)
S_sub_title    = style("ST", fontSize=9, textColor=DARK_GRAY, alignment=TA_CENTER,
                        spaceAfter=2)
S_meta_label   = style("ML", fontSize=9, fontName="Helvetica-Bold", textColor=DARK_GRAY)
S_meta_value   = style("MV", fontSize=9, textColor=DARK_GRAY)
S_intro        = style("IN", fontSize=8.5, textColor=DARK_GRAY, alignment=TA_JUSTIFY,
                        leading=13)
S_section      = style("SC", fontSize=10, fontName="Helvetica-Bold", textColor=RED_RATP,
                        spaceBefore=10, spaceAfter=4)
S_cell         = style("CE", fontSize=8, textColor=DARK_GRAY, leading=11)
S_cell_bold    = style("CB", fontSize=8, fontName="Helvetica-Bold", textColor=DARK_GRAY, leading=11)
S_red          = style("RD", fontSize=8, fontName="Helvetica-Bold",
                        textColor=RED_RATP, leading=11)
S_orange       = style("OR", fontSize=8, fontName="Helvetica-Bold",
                        textColor=ORANGE, leading=11)
S_green        = style("GR", fontSize=8, fontName="Helvetica-Bold",
                        textColor=GREEN, leading=11)
S_body         = style("BD", fontSize=9, textColor=DARK_GRAY, leading=13,
                        alignment=TA_JUSTIFY, spaceBefore=4)
S_sign_label   = style("SL", fontSize=9, fontName="Helvetica-Bold", textColor=DARK_GRAY)
S_sign_value   = style("SV", fontSize=9, textColor=DARK_GRAY, leading=14)
S_footer       = style("FT", fontSize=7, textColor=MID_GRAY, alignment=TA_CENTER)

story = []

# ── En-tête ──────────────────────────────────────────────────────────────────
header_data = [[
    Paragraph("<b>RATP</b>", ParagraphStyle("rh", fontSize=14, fontName="Helvetica-Bold",
                                             textColor=RED_RATP)),
    Paragraph("Direction de la Maintenance Ferroviaire", S_header_left),
]]
header_table = Table(header_data, colWidths=[3*cm, 14*cm])
header_table.setStyle(TableStyle([
    ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
    ("LINEBELOW", (0,0), (-1,-1), 0.5, MID_GRAY),
    ("BOTTOMPADDING", (0,0), (-1,-1), 6),
]))
story.append(header_table)
story.append(Spacer(1, 0.4*cm))

story.append(Paragraph("Rapport de Suivi des Actions Correctives – ISO 9001:2015", S_header_title))
story.append(Spacer(1, 0.3*cm))

# ── Métadonnées ───────────────────────────────────────────────────────────────
meta = [
    ("Site :",          "Atelier de maintenance Sucy-en-Brie (94)"),
    ("Référentiel :",   "ISO 9001:2015"),
    ("Responsable :",   "Mei Lin Zhang – Responsable Qualité"),
    ("Période :",       "Novembre 2024 – Mai 2026"),
    ("Date d'édition :", "12 mai 2026"),
    ("Destinataire :",  "Bureau Veritas – Marc Lefèvre (audit du 09/06/2026)"),
]
meta_table_data = [[Paragraph(k, S_meta_label), Paragraph(v, S_meta_value)] for k, v in meta]
meta_table = Table(meta_table_data, colWidths=[4*cm, 13*cm])
meta_table.setStyle(TableStyle([
    ("VALIGN", (0,0), (-1,-1), "TOP"),
    ("TOPPADDING", (0,0), (-1,-1), 3),
    ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ("BACKGROUND", (0,0), (-1,-1), LIGHT_BG),
    ("LEFTPADDING", (0,0), (-1,-1), 8),
    ("RIGHTPADDING", (0,0), (-1,-1), 8),
    ("BOX", (0,0), (-1,-1), 0.5, MID_GRAY),
    ("INNERGRID", (0,0), (-1,-1), 0.3, colors.HexColor("#DDDDDD")),
]))
story.append(meta_table)
story.append(Spacer(1, 0.4*cm))

story.append(Paragraph(
    "Ce document présente l'état d'avancement des actions correctives ouvertes suite à l'audit "
    "ISO 9001 réalisé le 14 novembre 2024 par Bureau Veritas sur le site de l'Atelier de "
    "maintenance Sucy-en-Brie. Il est transmis à l'équipe d'audit BV en amont de la visite de "
    "suivi du 09 juin 2026.", S_intro))
story.append(Spacer(1, 0.5*cm))

# ── Tableau actions correctives ───────────────────────────────────────────────
story.append(Paragraph("Tableau de suivi des actions correctives", S_section))

col_w = [1.4*cm, 1.6*cm, 6.0*cm, 2.8*cm, 2.0*cm, 3.0*cm, 2.2*cm]

hdr = [
    Paragraph("<b>Réf.</b>",         ParagraphStyle("th", fontSize=8, fontName="Helvetica-Bold", textColor=colors.white, alignment=TA_CENTER)),
    Paragraph("<b>Clause</b>",       ParagraphStyle("th", fontSize=8, fontName="Helvetica-Bold", textColor=colors.white, alignment=TA_CENTER)),
    Paragraph("<b>Constat d'origine</b>", ParagraphStyle("th", fontSize=8, fontName="Helvetica-Bold", textColor=colors.white)),
    Paragraph("<b>Responsable</b>",  ParagraphStyle("th", fontSize=8, fontName="Helvetica-Bold", textColor=colors.white)),
    Paragraph("<b>Échéance</b>",     ParagraphStyle("th", fontSize=8, fontName="Helvetica-Bold", textColor=colors.white, alignment=TA_CENTER)),
    Paragraph("<b>Avancement</b>",   ParagraphStyle("th", fontSize=8, fontName="Helvetica-Bold", textColor=colors.white)),
    Paragraph("<b>Statut</b>",       ParagraphStyle("th", fontSize=8, fontName="Helvetica-Bold", textColor=colors.white, alignment=TA_CENTER)),
]

rows = [
    ("AC-001", "§7.1.5",
     "Clés dynamométriques postes 8, 12 et 15 :\ncertificats d'étalonnage périmés depuis\nmars 2024 (14 mois de retard).",
     "M. L. Zhang\nResp. Qualité", "31/12/2024",
     "Budget non alloué.\nÉtalonnage COFRAC\nnon planifié à ce jour.", "NON CLÔTURÉ", "red"),
    ("AC-002", "§8.7",
     "Zone de quarantaine non clairement\ndélimitée. Pièces NC mélangées\navec conformes (zone C).",
     "Chef d'atelier\nN. Rousseau", "28/02/2025",
     "Marquage au sol partiel\n(50%). Signalétique\nen attente commande.", "PARTIEL", "orange"),
    ("AC-003", "§7.5",
     "Instruction de travail IT-054\nnon mise à jour depuis 18 mois.\nVersion en vigueur obsolète.",
     "Resp. Méthodes\nP. Aubert", "31/03/2025",
     "Document mis à jour\nle 15/03/2025.\nValidé et diffusé.", "CLÔTURÉ", "green"),
    ("AC-004", "§7.2",
     "Habilitations électriques de 3\nopérateurs expirées (postes\nmaintenance MI09).",
     "Service RH\nC. Martin", "30/04/2025",
     "Recyclage effectué\nle 08/04/2025.\nHabilitations renouvelées.", "CLÔTURÉ", "green"),
    ("AC-005", "§7.1.5",
     "Poste de Responsable Métrologie\nvacant depuis septembre 2024.\nIntérim insuffisant.",
     "Direction\nG. Moreau", "30/06/2026",
     "Recrutement en cours.\nAnnonce publiée\njanvier 2026.", "EN COURS", "orange"),
]

status_style = {"red": S_red, "orange": S_orange, "green": S_green}

table_data = [hdr]
for ref, clause, constat, resp, ech, avanc, statut, color in rows:
    table_data.append([
        Paragraph(ref, S_cell_bold),
        Paragraph(clause, S_cell_bold),
        Paragraph(constat.replace("\n", "<br/>"), S_cell),
        Paragraph(resp.replace("\n", "<br/>"), S_cell),
        Paragraph(ech, ParagraphStyle("ec", fontSize=8, textColor=DARK_GRAY, alignment=TA_CENTER)),
        Paragraph(avanc.replace("\n", "<br/>"), S_cell),
        Paragraph(statut, status_style[color]),
    ])

t = Table(table_data, colWidths=col_w, repeatRows=1)
t.setStyle(TableStyle([
    ("BACKGROUND", (0,0), (-1,0), TABLE_HDR),
    ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, LIGHT_BG]),
    ("BACKGROUND", (0,1), (-1,1), RED_BG),   # AC-001 fond légèrement rouge
    ("VALIGN", (0,0), (-1,-1), "TOP"),
    ("TOPPADDING", (0,0), (-1,-1), 4),
    ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ("LEFTPADDING", (0,0), (-1,-1), 5),
    ("RIGHTPADDING", (0,0), (-1,-1), 5),
    ("BOX", (0,0), (-1,-1), 0.8, MID_GRAY),
    ("INNERGRID", (0,0), (-1,-1), 0.3, colors.HexColor("#CCCCCC")),
    ("ALIGN", (4,0), (4,-1), "CENTER"),
]))
story.append(t)

# ── Page 2 : synthèse ─────────────────────────────────────────────────────────
from reportlab.platypus import PageBreak
story.append(PageBreak())

# En-tête page 2
story.append(header_table)
story.append(Spacer(1, 0.3*cm))

synth_hdr = Table([[
    Paragraph("<b>RATP</b>  Direction de la Maintenance Ferroviaire",
              ParagraphStyle("sh", fontSize=10, fontName="Helvetica-Bold", textColor=colors.white)),
    Paragraph("Synthèse et points d'attention pour l'audit du 09/06/2026",
              ParagraphStyle("sh2", fontSize=10, fontName="Helvetica-Bold", textColor=colors.white)),
]], colWidths=[5*cm, 12*cm])
synth_hdr.setStyle(TableStyle([
    ("BACKGROUND", (0,0), (-1,-1), RED_RATP),
    ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
    ("TOPPADDING", (0,0), (-1,-1), 6),
    ("BOTTOMPADDING", (0,0), (-1,-1), 6),
    ("LEFTPADDING", (0,0), (-1,-1), 8),
]))
story.append(synth_hdr)
story.append(Spacer(1, 0.4*cm))

story.append(Paragraph(
    "L'action <b>AC-001</b> (clause §7.1.5 – étalonnage des clés dynamométriques) demeure la "
    "priorité absolue : 3 équipements sont hors certification depuis plus de 14 mois, ce qui "
    "compromet la fiabilité des contrôles sur les rames MI09. L'absence de responsable métrologie "
    "(AC-005) aggrave le risque. Un plan d'urgence d'étalonnage COFRAC doit être présenté à "
    "l'auditeur BV lors de la visite du 09 juin 2026.", S_body))
story.append(Spacer(1, 0.3*cm))

story.append(Paragraph(
    "L'action <b>AC-002</b> (clause §8.7 – zone quarantaine) est partiellement traitée : le "
    "marquage au sol reste insuffisant et des non-conformités de gestion des pièces ont été "
    "constatées lors d'une vérification interne en avril 2026.", S_body))
story.append(Spacer(1, 1.0*cm))

sign_data = [[
    [Paragraph("Établi par :", S_sign_label),
     Paragraph("Mei Lin Zhang", S_sign_value),
     Paragraph("Responsable Qualité – Atelier Sucy-en-Brie", S_sign_value),
     Paragraph("Date : 12/05/2026", S_sign_value)],
    [Paragraph("Validé par :", S_sign_label),
     Paragraph("Direction Qualité RATP", S_sign_value),
     Paragraph("Frédéric Moreau", S_sign_value),
     Paragraph("Date : 12/05/2026", S_sign_value)],
]]

sign_table = Table([[
    Table([[p] for p in sign_data[0][0]], colWidths=[8.5*cm]),
    Table([[p] for p in sign_data[0][1]], colWidths=[8.5*cm]),
]], colWidths=[8.5*cm, 8.5*cm])
sign_table.setStyle(TableStyle([
    ("VALIGN", (0,0), (-1,-1), "TOP"),
    ("LINEABOVE", (0,0), (0,0), 1.5, RED_RATP),
    ("LINEABOVE", (1,0), (1,0), 1.5, RED_RATP),
    ("TOPPADDING", (0,0), (-1,-1), 6),
]))
story.append(sign_table)

# ── Footer via onPage ─────────────────────────────────────────────────────────
FOOTER_TEXT = ("Document confidentiel RATP – Transmis à Bureau Veritas "
               "dans le cadre de l'audit ISO 9001 du 09/06/2026")

def add_footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(MID_GRAY)
    w, h = A4
    canvas.drawCentredString(w/2, 1.2*cm, FOOTER_TEXT)
    canvas.drawRightString(w - 2*cm, 1.2*cm, f"Page {doc.page}")
    canvas.line(2*cm, 1.5*cm, w - 2*cm, 1.5*cm)
    canvas.restoreState()

doc.build(story, onFirstPage=add_footer, onLaterPages=add_footer)
print(f"OK : {OUT}")
