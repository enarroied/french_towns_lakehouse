import logging
from pathlib import Path

import requests


logger = logging.getLogger(__name__)


DARK_BG = (20, 25, 35)
WHITE = (255, 255, 255)
LIGHT_GRAY = (200, 200, 200)
ACCENT_GREEN = (76, 175, 80)
ACCENT_RED = (244, 67, 54)
ACCENT_BLUE = (66, 165, 245)
ACCENT_ORANGE = (255, 152, 0)

WIDTH = 1920
HEIGHT = 1080
DPI = 300

MARGIN_X = int(WIDTH * 0.07)
MARGIN_Y = int(HEIGHT * 0.07)

FONT_DIR = Path.home() / ".fonts"
MONTSERRAT_VARIABLE = FONT_DIR / "Montserrat-Variable.ttf"
FONT_FALLBACK = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_FALLBACK_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

BASE_URL = "https://raw.githubusercontent.com/google/fonts/main/ofl/montserrat"
VARIABLE_FONT_URL = f"{BASE_URL}/Montserrat%5Bwght%5D.ttf"

MONTSERRAT_VARIABLE = FONT_DIR / "Montserrat-Variable.ttf"


def download_montserrat() -> None:
    if MONTSERRAT_VARIABLE.exists():
        return
    FONT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        resp = requests.get(VARIABLE_FONT_URL, timeout=10)
        resp.raise_for_status()
        MONTSERRAT_VARIABLE.write_bytes(resp.content)
    except Exception:
        logger.warning("Could not download Montserrat font, using DejaVu Sans fallback")


def get_font_path(bold: bool = False, light: bool = False) -> str:
    download_montserrat()
    if MONTSERRAT_VARIABLE.exists():
        return str(MONTSERRAT_VARIABLE)
    if bold:
        return FONT_FALLBACK_BOLD
    return FONT_FALLBACK
