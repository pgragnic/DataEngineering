#!/usr/bin/env python3
"""Generate 4 design mockup PNGs for the eToro portfolio app."""
from PIL import Image, ImageDraw, ImageFont
import os

W = 390
H = 860
FONT_REG  = "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"
FONT_BOLD = "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
OUT_DIR   = os.path.dirname(os.path.abspath(__file__))

def c(h):
    h = h.lstrip('#')
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

THEMES = {
    "1_light_navy": {
        "label":        "① Light Navy  —  pro / corporate",
        "bg_page":      "#F0F4FA",
        "header_bg":    "#1A2F5C",
        "header_text":  "#FFFFFF",
        "header_dim":   "#8AACDA",
        "nav_bg":       "#FFFFFF",
        "nav_border":   "#E2E8F0",
        "nav_active":   "#1E6FC4",
        "nav_inactive": "#94A3B8",
        "card_bg":      "#FFFFFF",
        "border":       "#E2E8F0",
        "text_primary": "#0F172A",
        "text_sec":     "#475569",
        "text_muted":   "#94A3B8",
        "accent":       "#1E6FC4",
        "logo_bg":      "#E8EDF5",
        "pos":          "#16A34A",
        "neg":          "#DC2626",
    },
    "2_dark_premium": {
        "label":        "② Dark Premium  —  Bloomberg / luxe sombre",
        "bg_page":      "#080C18",
        "header_bg":    "#0C1020",
        "header_text":  "#F5F0E0",
        "header_dim":   "#8A7A50",
        "nav_bg":       "#0C1020",
        "nav_border":   "#1A2035",
        "nav_active":   "#F59E0B",
        "nav_inactive": "#4A5070",
        "card_bg":      "#111828",
        "border":       "#1C2438",
        "text_primary": "#EDE8D8",
        "text_sec":     "#9A9070",
        "text_muted":   "#4A5070",
        "accent":       "#F59E0B",
        "logo_bg":      "#192038",
        "pos":          "#22C55E",
        "neg":          "#FB7185",
    },
    "3_minimal_green": {
        "label":        "③ Minimal Green  —  Robinhood / épuré",
        "bg_page":      "#F9FAFB",
        "header_bg":    "#FFFFFF",
        "header_text":  "#111827",
        "header_dim":   "#9CA3AF",
        "nav_bg":       "#FFFFFF",
        "nav_border":   "#E5E7EB",
        "nav_active":   "#16A34A",
        "nav_inactive": "#9CA3AF",
        "card_bg":      "#FFFFFF",
        "border":       "#E5E7EB",
        "text_primary": "#111827",
        "text_sec":     "#6B7280",
        "text_muted":   "#9CA3AF",
        "accent":       "#16A34A",
        "logo_bg":      "#F3F4F6",
        "pos":          "#16A34A",
        "neg":          "#DC2626",
    },
    "4_slate_teal": {
        "label":        "④ Slate Teal  —  fintech moderne / cyan",
        "bg_page":      "#0F172A",
        "header_bg":    "#162040",
        "header_text":  "#E2E8F0",
        "header_dim":   "#5880A8",
        "nav_bg":       "#1E293B",
        "nav_border":   "#1E3A5F",
        "nav_active":   "#0EA5E9",
        "nav_inactive": "#4A6080",
        "card_bg":      "#1E293B",
        "border":       "#1E3A5F",
        "text_primary": "#E2E8F0",
        "text_sec":     "#94A3B8",
        "text_muted":   "#4A6080",
        "accent":       "#0EA5E9",
        "logo_bg":      "#0F2040",
        "pos":          "#10B981",
        "neg":          "#F87171",
    },
}

def rr(draw, xy, radius, fill, outline=None, width=1):
    """Rounded rectangle helper."""
    draw.rounded_rectangle(xy, radius=radius, fill=fill,
                           outline=outline, width=width)

def make_mockup(name, t):
    img  = Image.new('RGB', (W, H), c(t['bg_page']))
    draw = ImageDraw.Draw(img)

    fT  = ImageFont.truetype(FONT_REG,  9)
    fXS = ImageFont.truetype(FONT_REG,  11)
    fS  = ImageFont.truetype(FONT_REG,  12)
    fB  = ImageFont.truetype(FONT_REG,  14)
    fMB = ImageFont.truetype(FONT_BOLD, 14)
    fLB = ImageFont.truetype(FONT_BOLD, 17)
    fXL = ImageFont.truetype(FONT_BOLD, 34)

    pad = 14
    y   = 0

    # ── Header ──────────────────────────────────────────────
    hh = 108
    draw.rectangle([0, 0, W, hh], fill=c(t['header_bg']))
    draw.text((W//2, 14), "ETORO PORTFOLIO", font=fT, fill=c(t['header_dim']), anchor="mt")
    draw.text((W//2, 28), "$14,487.21", font=fXL, fill=c(t['header_text']), anchor="mt")
    draw.text((W//2 - 38, 78), "-$531.29", font=fS, fill=c(t['neg']), anchor="mt")
    draw.text((W//2 + 6,  78), "● MàJ 11:05", font=fS, fill=c(t['header_dim']), anchor="mt")
    y = hh

    # ── Nav ─────────────────────────────────────────────────
    nh = 44
    draw.rectangle([0, y, W, y + nh], fill=c(t['nav_bg']))
    draw.line([(0, y + nh), (W, y + nh)], fill=c(t['nav_border']), width=1)
    tabs = ["Portfolio", "Acheter", "En Att.", "Stats", "Suivi"]
    tw = W // len(tabs)
    for i, tab in enumerate(tabs):
        tx = i * tw + tw // 2
        active = i == 0
        col = c(t['nav_active']) if active else c(t['nav_inactive'])
        draw.text((tx, y + nh // 2), tab, font=fXS, fill=col, anchor="mm")
        if active:
            draw.line([(i*tw+6, y+nh-2), ((i+1)*tw-6, y+nh-2)],
                      fill=c(t['nav_active']), width=2)
    y += nh + pad

    # ── Summary cards ────────────────────────────────────────
    sh = 58
    sw = (W - pad*2 - 8) // 2
    summaries = [("INVESTI", "$10,137", t['text_primary']),
                 ("P&L",     "-$531",   t['neg'])]
    for i, (lbl, val, vcol) in enumerate(summaries):
        sx = pad + i*(sw+8)
        rr(draw, [sx, y, sx+sw, y+sh], 8, fill=c(t['card_bg']),
           outline=c(t['border']), width=1)
        draw.text((sx+sw//2, y+12), lbl, font=fT, fill=c(t['text_muted']), anchor="mt")
        draw.text((sx+sw//2, y+28), val, font=fMB, fill=c(vcol), anchor="mt")
    y += sh + 10

    # ── Sort pills ───────────────────────────────────────────
    pills = [("Valeur ↓", True), ("Valeur ↑", False), ("P&L ↓", False), ("P&L ↑", False)]
    px = pad
    for pill, active in pills:
        pw = len(pill) * 6 + 20
        fill    = c(t['accent'])       if active else c(t['card_bg'])
        outline = c(t['accent'])       if active else c(t['border'])
        tcol    = (255,255,255)        if active else c(t['text_sec'])
        rr(draw, [px, y, px+pw, y+26], 13, fill=fill, outline=outline)
        draw.text((px+pw//2, y+13), pill, font=fXS, fill=tcol, anchor="mm")
        px += pw + 6
    y += 36

    # ── Section title ────────────────────────────────────────
    draw.text((pad, y), "POSITIONS OUVERTES", font=fT, fill=c(t['accent']), anchor="lm")
    draw.line([(pad, y+14), (W-pad, y+14)], fill=c(t['border']), width=1)
    y += 22

    # ── Position cards ───────────────────────────────────────
    positions = [
        ("GLD", "SPDR Gold",     "Long",  "$2,466", "-$100", False),
        ("SLV", "iShares Silver","Long",  "$2,559", "-$217", False),
        ("TTE", "TotalEnergies", "Short", "$1,607", "+$88",  True),
    ]
    for sym, pos_name, direction, amount, pnl, is_pos in positions:
        ch = 78
        rr(draw, [pad, y, W-pad, y+ch], 10,
           fill=c(t['card_bg']), outline=c(t['border']), width=1)

        # Logo circle
        lx = pad + 20
        ly = y + ch // 2
        draw.ellipse([lx-18, ly-18, lx+18, ly+18], fill=c(t['logo_bg']))
        draw.text((lx, ly), sym, font=ImageFont.truetype(FONT_BOLD, 9),
                  fill=c(t['text_sec']), anchor="mm")

        # Name & sub
        tx = lx + 28
        draw.text((tx, y+16), pos_name, font=fMB, fill=c(t['text_primary']), anchor="lm")
        dir_col = c(t['pos']) if direction == "Long" else c(t['neg'])
        draw.text((tx, y+36), f"{'▲' if direction=='Long' else '▼'} {direction}  ·  2026-05-13",
                  font=fXS, fill=c(t['text_muted']), anchor="lm")
        draw.text((tx, y+52), "Ouv 434.78  ·  Act 417.11",
                  font=ImageFont.truetype(FONT_REG, 10), fill=c(t['text_muted']), anchor="lm")

        # Amount & P&L
        draw.text((W-pad-8, y+18), amount, font=fLB, fill=c(t['text_primary']), anchor="rm")
        pnl_col = c(t['pos']) if is_pos else c(t['neg'])
        draw.text((W-pad-8, y+38), pnl, font=fS, fill=pnl_col, anchor="rm")

        # Vendre button
        bw, bh = 58, 22
        bx = W - pad - 8 - bw
        by = y + ch - 12 - bh
        rr(draw, [bx, by, bx+bw, by+bh], 5, fill=c(t['accent']))
        draw.text((bx+bw//2, by+bh//2), "Vendre", font=fXS,
                  fill=(255,255,255), anchor="mm")

        y += ch + 8

    # ── Label (design name) ──────────────────────────────────
    draw.rectangle([0, H-30, W, H], fill=c(t['header_bg']))
    draw.text((W//2, H-15), t['label'], font=fXS,
              fill=c(t['header_dim']), anchor="mm")

    out = os.path.join(OUT_DIR, f"mockup_{name}.png")
    img.save(out)
    print(f"✓  {out}")

for name, theme in THEMES.items():
    make_mockup(name, theme)

print("\nDone — 4 mockups generated.")
