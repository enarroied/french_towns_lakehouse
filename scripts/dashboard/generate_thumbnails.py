"""Generate 200px-wide thumbnails for DCIM photos to use in map popups."""

from pathlib import Path

from PIL import Image


DCIM_DIR = Path("generate_qfield/communes/out/DCIM")
THUMB_DIR = Path("blog/data/img")
THUMB_WIDTH = 200
EXTENSIONS = {".jpg", ".jpeg", ".png"}


def main() -> None:
    THUMB_DIR.mkdir(parents=True, exist_ok=True)

    generated = 0
    skipped = 0

    for img_path in sorted(DCIM_DIR.iterdir()):
        if img_path.suffix.lower() not in EXTENSIONS:
            continue
        if img_path.name.startswith("."):
            continue

        thumb_path = THUMB_DIR / img_path.name

        if (
            thumb_path.exists()
            and thumb_path.stat().st_mtime >= img_path.stat().st_mtime
        ):
            skipped += 1
            continue

        img = Image.open(img_path)
        img.load()
        ratio = THUMB_WIDTH / img.width
        new_height = int(img.height * ratio)
        thumb = img.resize((THUMB_WIDTH, new_height), Image.Resampling.LANCZOS)
        thumb.save(thumb_path, quality=85)
        generated += 1

    print(f"✅ Thumbnails: {generated} generated, {skipped} skipped")


if __name__ == "__main__":
    main()
