"""Generate dim_calendar enrichment CSVs using reliable registry packages.

Produces:
  french_holidays.csv       - Official French public holidays
  market_holidays.csv       - Euronext Paris market closures (boolean only)
  religious_holidays.csv    - Christian / Jewish / Muslim / Chinese holiday flags

Also calls timeline_to_csv to regenerate political-timeline CSVs.
"""

import csv
import importlib.util
from contextlib import suppress
from datetime import date
from datetime import timedelta
from pathlib import Path

import exchange_calendars as xc
import holidays
from dateutil.easter import easter
from lunarcalendar.festival import ChineseNewYear
from lunarcalendar.festival import ChingMing
from lunarcalendar.festival import ChongYang
from lunarcalendar.festival import DongJie
from lunarcalendar.festival import DragonBoat
from lunarcalendar.festival import Ghost
from lunarcalendar.festival import LaBa
from lunarcalendar.festival import Lantern
from lunarcalendar.festival import MidAutumn
from lunarcalendar.festival import Qixi
from lunarcalendar.festival import XiaoNian


OUTPUT_DIR = Path(__file__).parent
START_YEAR = 1900
END_YEAR = 2100
YEAR_RANGE = list(range(START_YEAR, END_YEAR + 1))

CHINESE_FESTIVAL_CLASSES = [
    ChineseNewYear,
    ChingMing,
    DragonBoat,
    MidAutumn,
    ChongYang,
    Lantern,
    Qixi,
    Ghost,
    DongJie,
    XiaoNian,
    LaBa,
]

CHRISTIAN_FIXED_FEASTS: dict[tuple[int, int], str] = {
    (1, 1): "Solemnity of Mary, Mother of God",
    (1, 6): "Epiphany",
    (2, 2): "Presentation of the Lord",
    (3, 19): "Saint Joseph",
    (3, 25): "Annunciation",
    (6, 24): "Nativity of Saint John the Baptist",
    (6, 29): "Saints Peter and Paul",
    (8, 6): "Transfiguration",
    (8, 15): "Assumption of Mary",
    (9, 8): "Nativity of Mary",
    (9, 14): "Exaltation of the Holy Cross",
    (11, 1): "All Saints",
    (11, 2): "All Souls",
    (12, 8): "Immaculate Conception",
    (12, 25): "Christmas",
    (12, 26): "Saint Stephen",
    (12, 28): "Holy Innocents",
}


def _easter_based_dates(year: int) -> set[date]:
    """Return set of movable Christian feast dates for a given year."""
    e = easter(year)
    offsets = [-46, -7, -3, -2, 0, 1, 7, 39, 49, 50, 56, 60, 68]
    return {e + timedelta(days=offset) for offset in offsets}


def _generate_christian_dates() -> set[date]:
    """Return set of all Christian feast dates in the calendar range."""
    all_dates: set[date] = set()
    for year in YEAR_RANGE:
        all_dates.update(_easter_based_dates(year))
        for month, day in CHRISTIAN_FIXED_FEASTS:
            all_dates.add(date(year, month, day))
    return all_dates


def _generate_jewish_dates() -> set[date]:
    """Return set of holiday dates from the Israel calendar."""
    il_holidays = holidays.country_holidays("IL", years=YEAR_RANGE, language="en")
    return {d for d in il_holidays if isinstance(d, date)}


def _generate_muslim_dates() -> set[date]:
    """Return set of holiday dates from the Saudi Arabia calendar."""
    sa_holidays = holidays.country_holidays("SA", years=YEAR_RANGE, language="en")
    return {d for d in sa_holidays if isinstance(d, date)}


def _generate_chinese_dates() -> set[date]:
    """Return set of traditional Chinese festival dates via lunarcalendar."""
    all_dates: set[date] = set()
    for year in YEAR_RANGE:
        for cls in CHINESE_FESTIVAL_CLASSES:
            with suppress(Exception):
                all_dates.add(cls(year))
    return all_dates


def _generate_french_holidays() -> dict[date, str]:
    """Official French public holidays compiled efficiently."""
    fr_holidays = holidays.France(years=YEAR_RANGE, language="fr")
    return {d: name for d, name in fr_holidays.items() if isinstance(d, date)}


def _write_french_holidays(fr_holidays: dict[date, str]) -> None:
    csv_path = OUTPUT_DIR / "french_holidays.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "holiday_name", "is_public_holiday"])
        for d, name in sorted(fr_holidays.items()):
            writer.writerow([d.strftime("%Y-%m-%d"), name, 1])
    print(f"✅ french_holidays.csv: {len(fr_holidays)} dates")


def _write_market_holidays(fr_holiday_dates: set[date]) -> None:
    """Market closures safely bounds-checked against Euronext Paris."""
    paris = xc.get_calendar("XPAR")
    sch = paris.schedule
    xpar_start, xpar_end = sch.index[0].date(), sch.index[-1].date()

    market_dates: set[date] = set()

    for y in YEAR_RANGE:
        d = date(y, 1, 1)
        while d.year == y:
            # Safe boundary strategy: avoid exchange_calendars evaluation if out-of-bounds
            if xpar_start <= d <= xpar_end:
                is_open = paris.is_session(d.strftime("%Y-%m-%d"))
            else:
                is_open = not (d.weekday() >= 5 or d in fr_holiday_dates)

            if not is_open and d.weekday() < 5:
                market_dates.add(d)
            d += timedelta(days=1)

    csv_path = OUTPUT_DIR / "market_holidays.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "is_market_holiday"])
        for d in sorted(market_dates):
            writer.writerow([d.strftime("%Y-%m-%d"), 1])
    print(f"✅ market_holidays.csv: {len(market_dates)} dates")


def _write_religious_holidays() -> None:
    """Generate religious_holidays.csv with one row per date that has an observance."""
    christian = _generate_christian_dates()
    jewish = _generate_jewish_dates()
    muslim = _generate_muslim_dates()
    chinese = _generate_chinese_dates()

    all_religious = christian | jewish | muslim | chinese

    csv_path = OUTPUT_DIR / "religious_holidays.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "date",
                "is_christian_holiday",
                "is_jewish_holiday",
                "is_muslim_holiday",
                "is_chinese_holiday",
            ]
        )
        for d in sorted(all_religious):
            writer.writerow(
                [
                    d.strftime("%Y-%m-%d"),
                    1 if d in christian else 0,
                    1 if d in jewish else 0,
                    1 if d in muslim else 0,
                    1 if d in chinese else 0,
                ]
            )

    print(f"✅ religious_holidays.csv: {len(all_religious)} dates")
    print(
        f"   Christian: {len(christian)}, Jewish: {len(jewish)}, Muslim: {len(muslim)}, Chinese: {len(chinese)}"
    )


def write_csv_files() -> None:
    """Generate all enrichment CSV files."""
    fr_holidays = _generate_french_holidays()
    fr_holiday_dates = set(fr_holidays.keys())

    _write_french_holidays(fr_holidays)
    _write_market_holidays(fr_holiday_dates)
    _write_religious_holidays()

    # Dynamic runner execution
    _tl_spec = importlib.util.spec_from_file_location(
        "timeline_to_csv", OUTPUT_DIR / "timeline_to_csv.py"
    )
    if _tl_spec is None or _tl_spec.loader is None:
        raise RuntimeError("Could not load timeline_to_csv.py")
    _tl_mod = importlib.util.module_from_spec(_tl_spec)
    _tl_spec.loader.exec_module(_tl_mod)
    _tl_mod.write_timeline_csv_files()


if __name__ == "__main__":
    write_csv_files()
