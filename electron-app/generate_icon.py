#!/usr/bin/env python3
"""
Script pour generer une icone PNG simple pour l'application Electron.
Necessite Pillow: pip install pillow
"""
import sys
import os

def create_icon():
    try:
        from PIL import Image, ImageDraw
    except ImportError:
        print("Pillow non installe. Installation...")
        os.system(sys.executable + " -m pip install pillow")
        from PIL import Image, ImageDraw

    script_dir = os.path.dirname(os.path.abspath(__file__))
    assets_dir = os.path.join(script_dir, "assets")
    os.makedirs(assets_dir, exist_ok=True)

    for size in [16, 32, 48, 64, 128, 256, 512]:
        img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        # Fond arrondi
        r = size // 8
        draw.rounded_rectangle([0, 0, size-1, size-1], radius=r, fill=(43, 43, 43, 255))

        # Cercle orange (agent)
        cx, cy = size // 2, size * 2 // 5
        cr = size // 6
        draw.ellipse([cx-cr, cy-cr, cx+cr, cy+cr], fill=(232, 130, 90, 255))

        # Barres (lignes de texte)
        bw = size * 5 // 8
        bh = max(2, size // 16)
        by = size * 3 // 5
        draw.rounded_rectangle([cx-bw//2, by, cx+bw//2, by+bh], radius=1, fill=(232, 130, 90, 200))
        by2 = by + bh + max(2, size//16)
        draw.rounded_rectangle([cx-bw//3, by2, cx+bw//3, by2+bh], radius=1, fill=(100, 100, 100, 200))

        out = os.path.join(assets_dir, f"icon_{size}x{size}.png")
        img.save(out, 'PNG')
        print(f"Cree: {out}")

    # PNG principal (256x256)
    import shutil
    shutil.copy(os.path.join(assets_dir, "icon_256x256.png"), os.path.join(assets_dir, "icon.png"))
    print("icon.png cree (256x256)")

    # Essayer de creer un .ico (multi-taille)
    try:
        sizes = [(s, s) for s in [16, 32, 48, 64, 128, 256]]
        images = []
        for s, _ in sizes:
            f = os.path.join(assets_dir, f"icon_{s}x{s}.png")
            if os.path.exists(f):
                images.append(Image.open(f))
        if images:
            ico_path = os.path.join(assets_dir, "icon.ico")
            images[0].save(ico_path, format='ICO', sizes=[(i.width, i.height) for i in images], append_images=images[1:])
            print(f"icon.ico cree: {ico_path}")
    except Exception as e:
        print(f"Impossible de creer .ico: {e}")
        print("Installez Pillow >= 9.1.0 pour le support ICO")

if __name__ == '__main__':
    create_icon()
    print("\nIcones generees avec succes!")
