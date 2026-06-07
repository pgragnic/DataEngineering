#!/usr/bin/env python3
"""
Génère 3 photos de démo (preuves visuelles) pour la section S2 — Étalonnage (§7.1.5)
de l'UC28 Inspection Augmentée. Les images servent de pièces jointes lors de la
capture terrain ; elles ne sont PAS analysées par l'IA (c'est le texte dicté qui
pilote l'analyse). Contexte : atelier maintenance Sucy-en-Brie (RATP), rames MI09,
clé CD-12, récidive de la NC §7.1.5 ouverte en novembre 2024.

Sortie : docs/demo-photos/01_cle_dynamometrique_tag.png
         docs/demo-photos/02_etiquette_etalonnage.png
         docs/demo-photos/03_registre_equipements.png
"""
from pathlib import Path
import random
from PIL import Image, ImageDraw, ImageFont, ImageFilter

random.seed(28)

OUT = Path(__file__).resolve().parent.parent / "docs" / "demo-photos"
OUT.mkdir(parents=True, exist_ok=True)

DEJAVU = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
DEJAVU_B = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
MONO = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
MONO_B = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"

W, H = 1280, 960


def font(path, size):
    return ImageFont.truetype(path, size)


def center_text(d, box, text, fnt, fill):
    x0, y0, x1, y1 = box
    tb = d.textbbox((0, 0), text, font=fnt)
    tw, th = tb[2] - tb[0], tb[3] - tb[1]
    d.text((x0 + (x1 - x0 - tw) / 2 - tb[0], y0 + (y1 - y0 - th) / 2 - tb[1]),
           text, font=fnt, fill=fill)


def grain(img, sigma=14, alpha=0.10):
    """Ajoute un grain photographique léger."""
    noise = Image.effect_noise((img.width, img.height), sigma).convert("L")
    noise_rgb = Image.merge("RGB", (noise, noise, noise))
    return Image.blend(img, noise_rgb, alpha)


def vignette(img, strength=0.55):
    mask = Image.new("L", img.size, 0)
    md = ImageDraw.Draw(mask)
    md.ellipse([-img.width * 0.25, -img.height * 0.25,
                img.width * 1.25, img.height * 1.25], fill=255)
    mask = mask.filter(ImageFilter.GaussianBlur(220))
    dark = Image.new("RGB", img.size, (0, 0, 0))
    return Image.composite(img, Image.blend(img, dark, strength), mask)


def metal_bg(base=(110, 116, 124)):
    """Fond type établi métallique brossé."""
    img = Image.new("RGB", (W, H), base)
    d = ImageDraw.Draw(img)
    for y in range(H):
        t = y / H
        c = tuple(int(base[i] * (0.78 + 0.30 * t)) for i in range(3))
        d.line([(0, y), (W, y)], fill=c)
    # rayures de brossage horizontales
    for _ in range(2600):
        x = random.randint(0, W)
        y = random.randint(0, H)
        ln = random.randint(20, 120)
        sh = random.randint(-18, 18)
        c = tuple(max(0, min(255, base[i] + sh)) for i in range(3))
        d.line([(x, y), (x + ln, y)], fill=c, width=1)
    return img


def paste_with_shadow(base, layer, pos, angle=0, blur=18, shadow_alpha=120, dx=14, dy=20):
    if angle:
        layer = layer.rotate(angle, expand=True, resample=Image.BICUBIC)
    a = layer.split()[3]
    shadow = Image.new("RGBA", base.size, (0, 0, 0, 0))
    sh_mask = Image.new("L", base.size, 0)
    sh_mask.paste(a, (pos[0] + dx, pos[1] + dy))
    sh_mask = sh_mask.filter(ImageFilter.GaussianBlur(blur))
    shadow.putalpha(0)
    black = Image.new("RGBA", base.size, (0, 0, 0, shadow_alpha))
    base.paste(black, (0, 0), sh_mask)
    base.paste(layer, pos, layer)


# ─────────────────────────────────────────────────────────────────────────────
# IMAGE 1 — Clé dynamométrique + étiquette-volante d'étalonnage PÉRIMÉE
# ─────────────────────────────────────────────────────────────────────────────
def torque_wrench_layer():
    lw, lh = 1080, 320
    img = Image.new("RGBA", (lw, lh), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    cy = lh // 2
    # corps métallique (manche)
    shaft_l, shaft_r = 180, 1040
    th = 46
    for i in range(-th, th):
        t = (i + th) / (2 * th)
        c = int(120 + 110 * (1 - abs(0.5 - t) * 2))  # reflet central
        d.line([(shaft_l, cy + i), (shaft_r, cy + i)], fill=(c, c + 4, c + 10, 255))
    # poignée caoutchouc (droite)
    grip_l = 720
    d.rounded_rectangle([grip_l, cy - th - 6, shaft_r, cy + th + 6], radius=26,
                        fill=(28, 30, 34, 255))
    for x in range(grip_l + 24, shaft_r - 18, 26):
        d.line([(x, cy - th + 2), (x, cy + th - 2)], fill=(60, 62, 68, 255), width=6)
    # collet / molette de réglage
    d.rounded_rectangle([grip_l - 40, cy - th - 12, grip_l + 6, cy + th + 12],
                        radius=10, fill=(70, 72, 78, 255))
    # tête à cliquet (gauche)
    R = 96
    hx, hy = shaft_l, cy
    d.ellipse([hx - R, hy - R, hx + R, hy + R], fill=(150, 154, 162, 255),
              outline=(70, 72, 78, 255), width=4)
    d.ellipse([hx - R + 22, hy - R + 22, hx + R - 22, hy + R - 22],
              fill=(96, 100, 108, 255))
    # carré d'entraînement
    d.rectangle([hx - 18, hy - 18, hx + 18, hy + 18], fill=(60, 62, 68, 255))
    return img


def make_tag():
    tw, th = 360, 250
    img = Image.new("RGBA", (tw, th), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    # carton orange clair avec coin coupé + œillet
    pts = [(16, 70), (70, 16), (tw - 16, 16), (tw - 16, th - 16),
           (16, th - 16)]
    d.polygon(pts, fill=(236, 222, 188, 255), outline=(120, 96, 60, 255))
    d.ellipse([34, 40, 60, 66], outline=(120, 96, 60, 255), width=4)
    f_h = font(DEJAVU_B, 30)
    f_b = font(DEJAVU, 22)
    f_s = font(DEJAVU_B, 22)
    d.rectangle([78, 30, tw - 28, 70], fill=(214, 122, 30, 255))
    center_text(d, (78, 30, tw - 28, 70), "ÉTALONNAGE", font(DEJAVU_B, 26), "white")
    d.text((40, 86), "Clé dynamométrique", font=f_s, fill=(40, 34, 24))
    d.text((40, 120), "Réf. CD-12  ·  20–100 Nm", font=f_b, fill=(60, 52, 40))
    d.text((40, 152), "Dernier étal. : 10/2024", font=f_b, fill=(60, 52, 40))
    d.text((40, 184), "Échéance : 10/2025", font=f_b, fill=(150, 40, 30))
    return img


def stamp_perime():
    sw, sh = 300, 110
    img = Image.new("RGBA", (sw, sh), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    red = (190, 40, 36, 235)
    d.rounded_rectangle([6, 6, sw - 6, sh - 6], radius=14, outline=red, width=7)
    center_text(d, (0, 0, sw, sh), "PÉRIMÉ", font(DEJAVU_B, 58), red)
    return img


def build_image1():
    base = metal_bg((112, 118, 126))
    wrench = torque_wrench_layer()
    paste_with_shadow(base, wrench, (110, 300), angle=-9, blur=22, shadow_alpha=110)
    tag = make_tag()
    paste_with_shadow(base, tag, (430, 470), angle=-6, blur=16, shadow_alpha=120)
    # ficelle reliant la tête au tag
    d = ImageDraw.Draw(base)
    d.line([(330, 470), (470, 540)], fill=(180, 170, 150), width=4)
    # tampon PÉRIMÉ par-dessus le tag
    stamp = stamp_perime()
    base.paste(stamp, (470, 690), stamp.rotate(-8, expand=True, resample=Image.BICUBIC)
               if False else stamp)
    base = base.convert("RGB")
    base = vignette(base, 0.45)
    base = grain(base, 13, 0.08)
    base.save(OUT / "01_cle_dynamometrique_tag.png", quality=90)


# ─────────────────────────────────────────────────────────────────────────────
# IMAGE 2 — Étiquette d'étalonnage (sticker) défraîchie / illisible sur équipement
# ─────────────────────────────────────────────────────────────────────────────
def make_sticker(faded=True):
    sw, sh = 470, 320
    img = Image.new("RGBA", (sw, sh), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([8, 8, sw - 8, sh - 8], radius=18,
                        fill=(248, 248, 244, 255), outline=(150, 150, 150, 255), width=3)
    # bandeau titre
    d.rounded_rectangle([8, 8, sw - 8, 74], radius=18, fill=(26, 86, 140, 255))
    d.rectangle([8, 50, sw - 8, 74], fill=(26, 86, 140, 255))
    center_text(d, (8, 8, sw - 8, 74), "CONTRÔLE MÉTROLOGIQUE", font(DEJAVU_B, 28), "white")
    f = font(DEJAVU, 26)
    fb = font(DEJAVU_B, 26)
    d.text((34, 96), "N° équip. :", font=f, fill=(40, 40, 40))
    d.text((220, 96), "CD-12", font=fb, fill=(20, 20, 20))
    d.text((34, 140), "Étalonné le :", font=f, fill=(40, 40, 40))
    d.text((230, 140), "10 / 2024", font=fb, fill=(20, 20, 20))
    d.text((34, 184), "Proch. vérif. :", font=f, fill=(40, 40, 40))
    d.text((250, 184), "10 / 2025", font=fb, fill=(170, 40, 30))
    d.text((34, 232), "Labo COFRAC :", font=f, fill=(40, 40, 40))
    d.text((250, 232), "n° 2-1843", font=fb, fill=(20, 20, 20))
    d.text((34, 274), "Vérif. par : J. Moreau", font=font(DEJAVU, 22), fill=(90, 90, 90))
    if faded:
        # voile de vieillissement + zone illisible
        veil = Image.new("RGBA", (sw, sh), (210, 205, 180, 70))
        img.alpha_composite(veil)
        d = ImageDraw.Draw(img)
        # rayures d'usure
        for _ in range(60):
            x = random.randint(20, sw - 20)
            y = random.randint(90, sh - 20)
            d.line([(x, y), (x + random.randint(10, 50), y)],
                   fill=(200, 195, 175, 120), width=1)
        # coin décollé
        d.polygon([(sw - 8, sh - 8), (sw - 120, sh - 8), (sw - 8, sh - 120)],
                  fill=(225, 222, 210, 255), outline=(160, 160, 150, 255))
    return img


def build_image2():
    base = metal_bg((96, 104, 116))
    # plaque équipement plus claire (capot d'un appareil)
    d = ImageDraw.Draw(base)
    d.rounded_rectangle([120, 150, W - 120, H - 150], radius=30,
                        fill=(140, 146, 154))
    d.rounded_rectangle([120, 150, W - 120, H - 150], radius=30,
                        outline=(70, 74, 80), width=6)
    # vis d'angle
    for vx, vy in [(170, 200), (W - 170, 200), (170, H - 200), (W - 170, H - 200)]:
        d.ellipse([vx - 14, vy - 14, vx + 14, vy + 14], fill=(80, 84, 90),
                  outline=(50, 52, 56), width=2)
        d.line([(vx - 9, vy), (vx + 9, vy)], fill=(40, 42, 46), width=3)
    sticker = make_sticker(faded=True)
    paste_with_shadow(base, sticker, (405, 330), angle=-4, blur=14, shadow_alpha=110)
    base = base.convert("RGB")
    base = vignette(base, 0.40)
    base = grain(base, 12, 0.08)
    base.save(OUT / "02_etiquette_etalonnage.png", quality=90)


# ─────────────────────────────────────────────────────────────────────────────
# IMAGE 3 — Registre des équipements de mesure (incomplet)
# ─────────────────────────────────────────────────────────────────────────────
def build_image3():
    base = metal_bg((92, 98, 108))
    # feuille papier
    px0, py0, px1, py1 = 90, 70, W - 90, H - 70
    sheet = Image.new("RGBA", (px1 - px0, py1 - py0), (252, 251, 247, 255))
    sd = ImageDraw.Draw(sheet)
    sw, shh = sheet.size
    # en-tête
    sd.rectangle([0, 0, sw, 120], fill=(26, 86, 140, 255))
    sd.text((34, 26), "REGISTRE DES ÉQUIPEMENTS DE MESURE", font=font(DEJAVU_B, 34), fill="white")
    sd.text((34, 74), "Atelier maintenance — Sucy-en-Brie (RATP)  ·  Rames MI09",
            font=font(DEJAVU, 24), fill=(208, 226, 244))
    sd.text((sw - 250, 30), "ISO 9001 §7.1.5", font=font(DEJAVU_B, 24), fill=(208, 226, 244))
    sd.text((sw - 250, 74), "Maj : 10/2024", font=font(DEJAVU, 22), fill=(208, 226, 244))

    cols = [(34, "Réf."), (150, "Désignation"), (470, "Dernier étal."),
            (660, "Échéance"), (840, "Statut")]
    head_y = 150
    sd.rectangle([20, head_y, sw - 20, head_y + 46], fill=(225, 232, 240, 255))
    for cx, label in cols:
        sd.text((cx, head_y + 10), label, font=font(DEJAVU_B, 24), fill=(30, 50, 70))

    rows = [
        ("CD-07", "Clé dyn. 5–25 Nm", "03/2025", "03/2026", "Dépassé", "bad"),
        ("CD-12", "Clé dyn. 20–100 Nm", "10/2024", "10/2025", "Périmé +8 m", "bad"),
        ("PM-02", "Pied à coulisse", "01/2026", "01/2027", "Conforme", "ok"),
        ("MR-15", "Manomètre frein", "—", "—", "Non renseigné", "bad"),
        ("CP-09", "Comparateur 0–10", "05/2026", "05/2027", "Conforme", "ok"),
        ("TH-03", "Thermomètre IR", "02/2025", "", "Incomplet", "bad"),
        ("BR-21", "Banc de freinage", "—", "—", "Non renseigné", "bad"),
    ]
    ry = head_y + 46
    rh = 64
    f = font(DEJAVU, 24)
    fb = font(DEJAVU_B, 23)
    for i, (ref, des, der, ech, stat, kind) in enumerate(rows):
        if i % 2 == 1:
            sd.rectangle([20, ry, sw - 20, ry + rh], fill=(244, 246, 249, 255))
        col = (24, 24, 24)
        sd.text((34, ry + 18), ref, font=fb, fill=(30, 30, 30))
        sd.text((150, ry + 18), des, font=f, fill=col)
        sd.text((470, ry + 18), der if der else "·····", font=f,
                fill=col if der not in ("—", "") else (190, 60, 50))
        sd.text((660, ry + 18), ech if ech else "·····", font=f,
                fill=col if ech not in ("—", "") else (190, 60, 50))
        badge = (40, 140, 90) if kind == "ok" else (196, 52, 44)
        sd.rounded_rectangle([840, ry + 14, sw - 30, ry + rh - 12], radius=12,
                             fill=badge)
        center_text(sd, (840, ry + 14, sw - 30, ry + rh - 12), stat,
                    font(DEJAVU_B, 20), "white")
        ry += rh
    # cartouche bas
    sd.line([(20, ry + 14), (sw - 20, ry + 14)], fill=(180, 180, 180), width=2)
    sd.text((34, ry + 30), "4 équipements non conformes / non renseignés sur 7",
            font=font(DEJAVU_B, 24), fill=(170, 40, 30))
    sd.text((34, ry + 68), "Visa responsable métrologie : ______________   Date : __ / __ / ____",
            font=font(DEJAVU, 22), fill=(80, 80, 80))

    paste_with_shadow(base, sheet, (px0, py0), angle=-1.2, blur=20, shadow_alpha=120)
    base = base.convert("RGB")
    base = vignette(base, 0.35)
    base = grain(base, 10, 0.06)
    base.save(OUT / "03_registre_equipements.png", quality=90)


if __name__ == "__main__":
    build_image1()
    build_image2()
    build_image3()
    for p in sorted(OUT.glob("*.png")):
        print(f"  ✓ {p.relative_to(OUT.parent.parent)}  ({p.stat().st_size // 1024} Ko)")
