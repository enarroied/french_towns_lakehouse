"""Generate dim_calendar enrichment CSVs.

Produces:
  french_holidays.csv – Public & religious holidays (Christian, Islamic, Jewish)
  market_holidays.csv – Stock-market closed days
"""

import csv
from datetime import date
from datetime import timedelta
from pathlib import Path


OUT = Path(__file__).parent


# ── Christian / Computus ──────────────────────────────────────────────


def easter(year: int) -> tuple[int, int, int]:
    """Anonymous Gregorian algorithm – returns (year, month, day)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = (h + ell - 7 * m + 114) % 31 + 1
    return (year, month, day)


def add_days(y: int, m: int, d: int, n: int) -> tuple[int, int, int]:
    """Add *n* days to a date."""
    dt = date(y, m, d) + timedelta(days=n)
    return (dt.year, dt.month, dt.day)


HOLIDAYS_FR: list[tuple[int, int, int, str, bool]] = []
# Fixed holidays
FIXED = [
    (1, 1, "Jour de l'an", True),
    (5, 1, "Fête du Travail", True),
    (5, 8, "Fête de la Victoire", True),
    (7, 14, "Fête nationale", True),
    (8, 15, "Assomption", True),
    (11, 1, "Toussaint", True),
    (11, 11, "Armistice 1918", True),
    (12, 25, "Noël", True),
    (12, 26, "Saint-Étienne", False),  # regional
]

for y in range(1900, 2101):
    for month, day, name, pub in FIXED:
        HOLIDAYS_FR.append((y, month, day, name, pub))
    try:
        ey, em, ed = easter(y)
    except Exception:
        continue
    HOLIDAYS_FR.append((*add_days(ey, em, ed, 1), "Lundi de Pâques", True))
    HOLIDAYS_FR.append((*add_days(ey, em, ed, 39), "Ascension", True))
    HOLIDAYS_FR.append((*add_days(ey, em, ed, 50), "Lundi de Pentecôte", True))


# ── Islamic (tabular calendar) ────────────────────────────────────────


def _jdn_to_greg(jdn: int) -> tuple[int, int, int]:
    """Julian Day Number → Gregorian date."""
    a = jdn + 32044
    b = (4 * a + 3) // 146097
    c = a - (146097 * b) // 4
    d = (4 * c + 3) // 1461
    e = c - (1461 * d) // 4
    f = (5 * e + 2) // 153
    gy = 100 * b + d - 4800 + f // 10
    gm = f + 3 - 12 * (f // 10)
    gd = e - (153 * f + 2) // 5 + 1
    return (gy, gm, gd)


def _greg_to_jdn(gy: int, gm: int, gd: int) -> int:
    """Gregorian date → Julian Day Number."""
    a = (14 - gm) // 12
    y = gy + 4800 - a
    m = gm + 12 * a - 3
    return gd + (153 * m + 2) // 5 + 365 * y + y // 4 - y // 100 + y // 400 - 32045


ISLAMIC_EPOCH = 1948440  # JDN of 1 Muharram 1 AH
ISLAMIC_LEAP = {2, 5, 7, 10, 13, 16, 18, 21, 24, 26, 29}  # in 30-year cycle


def _islamic_year_days(iy: int) -> int:
    return 354 + (1 if (iy % 30) in ISLAMIC_LEAP else 0)


def _islamic_month_days(iy: int, im: int) -> int:
    """Days in Islamic month (tabular calendar)."""
    if im <= 6:
        return 30 if im % 2 == 1 else 29
    return 29 if im % 2 == 1 else 30


def _date_range_gregorian(start_y: int, end_y: int):
    """Yields (y, m, d) tuples for every day in range, year-bound."""
    d = date(start_y, 1, 1)
    end = date(end_y, 12, 31)
    while d <= end:
        yield (d.year, d.month, d.day)
        d += timedelta(days=1)


def generate_islamic_holidays() -> list[tuple[int, int, int, str, bool]]:
    """Generate Islamic holidays for 1900-2100 by scanning each day."""
    res = []
    for gy, gm, gd in _date_range_gregorian(1900, 2100):
        jdn = _greg_to_jdn(gy, gm, gd)
        total = jdn - ISLAMIC_EPOCH
        if total < 0:
            continue
        iy = 1
        days_counted = 0
        while True:
            yd = _islamic_year_days(iy)
            if days_counted + yd > total:
                break
            days_counted += yd
            iy += 1
        day_in_year = total - days_counted
        im = 1
        for month in range(1, 13):
            md = _islamic_month_days(iy, month)
            if day_in_year < md:
                im = month
                break
            day_in_year -= md
            im = month + 1
        id = day_in_year + 1

        if im == 9:
            name = "Ramadan" if id > 1 else "1er jour de Ramadan"
            res.append((gy, gm, gd, name, False))
        elif im == 10 and id == 1:
            res.append((gy, gm, gd, "Aïd el-Fitr", True))
        elif im == 12 and id == 10:
            res.append((gy, gm, gd, "Aïd el-Adha", True))
        elif im == 1 and id == 1:
            res.append((gy, gm, gd, "Nouvel An islamique", False))
    return res


HOLIDAYS_FR.extend(generate_islamic_holidays())


# ── Jewish / Hebrew calendar ──────────────────────────────────────────

HEBREW_EPOCH = 347998  # JDN of 1 Tishrei 1 (3761 BCE)
HOUR_PARTS = 1080
DAY_PARTS = 24 * HOUR_PARTS
MONTH_PARTS = 29 * DAY_PARTS + 12 * HOUR_PARTS + 793
WEEK_DAYS = 7
HEBREW_LEAP_YEARS = {0, 3, 6, 8, 11, 14, 17}


def _molad(hy: int) -> int:
    """Molad of Tishrei for Hebrew year hy, in parts since epoch."""
    months_elapsed = (hy - 1) * 12 + ((hy - 1) // 19) * 7
    leap_years_before = 0
    for y in range((hy - 1) % 19):
        if y in HEBREW_LEAP_YEARS:
            leap_years_before += 1
    months_elapsed += leap_years_before
    return (
        HEBREW_EPOCH * DAY_PARTS + months_elapsed * MONTH_PARTS + 3 * HOUR_PARTS + 876
    )


def _rosh_hashanah_jdn(hy: int) -> int:
    """JDN of 1 Tishrei (Rosh Hashanah) for Hebrew year hy."""
    molad_parts = _molad(hy)
    molad_day = molad_parts // DAY_PARTS
    molad_time = molad_parts % DAY_PARTS
    dow = molad_day % WEEK_DAYS
    delay = 0
    if molad_time >= 18 * HOUR_PARTS:
        delay = 1
    new_dow = (dow + delay) % WEEK_DAYS
    if new_dow in (0, 3, 5):
        delay += 1
    is_leap = hebrew_is_leap(hy)
    prev_is_leap = hebrew_is_leap(hy - 1)
    if is_leap and (dow + delay) % WEEK_DAYS == 1:
        delay += 1
    if prev_is_leap and (dow + delay) % WEEK_DAYS == 2:
        delay += 1

    return molad_day + delay


def hebrew_year_days(hy: int) -> int:
    """Total days in Hebrew year hy."""
    return _rosh_hashanah_jdn(hy + 1) - _rosh_hashanah_jdn(hy)


def hebrew_is_leap(hy: int) -> bool:
    return hy % 19 in HEBREW_LEAP_YEARS


# ruff: noqa: PLR0911, PLR0912
def hebrew_month_days(hy: int, hm: int) -> int:
    """Days in Hebrew month hm of year hy."""
    if hm <= 1:
        return 30
    if hm == 2:
        return 29
    if hm == 3:
        return 30 if hebrew_year_days(hy) in (355, 385) else 29
    if hm == 4:
        return 29
    if hm == 5:
        return 30
    if hm == 6:
        return 30 if hebrew_is_leap(hy) else 29
    if hm == 7:
        return 29
    hm_actual = hm - (0 if hebrew_is_leap(hy) else 1)
    if hm_actual <= 7:
        return 30
    if hm_actual == 8:
        return 29
    if hm_actual == 9:
        return 30
    if hm_actual == 10:
        return 29
    if hm_actual == 11:
        return 30
    if hm_actual == 12:
        return 29
    return 29


def jdn_to_hebrew(jdn: int) -> tuple[int, int, int]:
    """JDN → (Hebrew year, month, day)."""
    hy = (jdn - HEBREW_EPOCH + 6) // 365 + 1
    while _rosh_hashanah_jdn(hy + 1) <= jdn:
        hy += 1
    while _rosh_hashanah_jdn(hy) > jdn:
        hy -= 1
    rosh = _rosh_hashanah_jdn(hy)
    day_of_year = jdn - rosh
    hm = 1
    while day_of_year > 0:
        md = hebrew_month_days(hy, hm)
        if day_of_year < md:
            return (hy, hm, day_of_year + 1)
        day_of_year -= md
        hm += 1
    return (hy, 1, 1)


# ruff: noqa: PLR0912, PLR0915
def generate_jewish_holidays() -> list[tuple[int, int, int, str, bool]]:
    """Generate Jewish holidays for 1900-2100 by scanning each day."""
    res = []
    for gy, gm, gd in _date_range_gregorian(1900, 2100):
        jdn = _greg_to_jdn(gy, gm, gd)
        try:
            hy, hm, hd = jdn_to_hebrew(jdn)
        except Exception:
            continue
        is_leap = hebrew_is_leap(hy)
        month_offset = 1 if is_leap else 0
        nisan_month = 7 + month_offset
        iyar_month = nisan_month + 1
        sivan_month = nisan_month + 2
        av_month = nisan_month + 4

        if hm == 1 and hd == 1:
            res.append((gy, gm, gd, "Rosh Hashana", False))
        elif hm == 1 and hd == 2:
            res.append((gy, gm, gd, "Rosh Hashana (2e jour)", False))
        elif hm == 1 and hd == 10:
            res.append((gy, gm, gd, "Yom Kippour", False))
        elif hm == 1 and hd == 15:
            res.append((gy, gm, gd, "Souccot", False))
        elif hm == 1 and hd == 22:
            res.append((gy, gm, gd, "Chemini Atseret", False))
        elif hm == 3 and 25 <= hd <= 30:
            name = "Hanoucca" if hd > 25 else "Hanoucca (1er jour)"
            res.append((gy, gm, gd, name, False))
        elif hm == 4 and hd <= 2:
            res.append((gy, gm, gd, "Hanoucca", False))
        elif hm == (6 if is_leap else 6) and hd == 14:
            res.append((gy, gm, gd, "Pourim", False))
        elif hm == nisan_month and hd == 15:
            res.append((gy, gm, gd, "Pessa'h", False))
        elif hm == nisan_month and 16 <= hd <= 20:
            res.append((gy, gm, gd, "Pessa'h (fête)", False))
        elif hm == nisan_month and hd == 21:
            res.append((gy, gm, gd, "Pessa'h (7e jour)", False))
        elif hm == nisan_month and hd == 27:
            res.append((gy, gm, gd, "Yom HaShoah", False))
        elif hm == iyar_month and hd == 5:
            res.append((gy, gm, gd, "Yom HaAtzmaout", False))
        elif hm == iyar_month and hd == 18:
            res.append((gy, gm, gd, "Lag BaOmer", False))
        elif hm == sivan_month and hd == 6:
            res.append((gy, gm, gd, "Chavouot", False))
        elif hm == av_month and hd == 9:
            res.append((gy, gm, gd, "Tisha Beav", False))
        elif hm == 5 and hd == 15:
            res.append((gy, gm, gd, "Tou Bichvat", False))
    return res


HOLIDAYS_FR.extend(generate_jewish_holidays())


# ── Write french_holidays.csv ─────────────────────────────────────────

seen = set()
deduped: list[tuple[int, int, int, str, bool]] = []
for h in sorted(HOLIDAYS_FR):
    key = (h[0], h[1], h[2], h[3])
    if key not in seen:
        seen.add(key)
        deduped.append(h)

date_map: dict[tuple[int, int, int], tuple[list[str], list[bool]]] = {}
for h in deduped:
    y, m, d, name, pub = h
    key = (y, m, d)
    if key not in date_map:
        date_map[key] = ([], [])
    date_map[key][0].append(name)
    date_map[key][1].append(pub)

with (OUT / "french_holidays.csv").open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["date", "holiday_name", "is_public_holiday"])
    for (y, m, d), (names, pubs) in sorted(date_map.items()):
        w.writerow(
            [
                f"{y:04d}-{m:02d}-{d:02d}",
                " / ".join(names),
                1 if any(pubs) else 0,
            ]
        )

print(f"✅ french_holidays.csv: {len(date_map)} dates")


# ── Write market_holidays.csv ─────────────────────────────────────────

market_dates: set[tuple[int, int, int]] = set()
for y, m, d, _name, pub in deduped:
    if pub:
        dt_obj = date(y, m, d)
        if dt_obj.weekday() < 5:
            market_dates.add((y, m, d))

with (OUT / "market_holidays.csv").open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["date", "is_market_holiday", "market_holiday_name"])
    for (y, m, d), (names, pubs) in sorted(date_map.items()):
        if any(pubs):
            dt_obj = date(y, m, d)
            if dt_obj.weekday() < 5:
                w.writerow(
                    [
                        f"{y:04d}-{m:02d}-{d:02d}",
                        1,
                        " / ".join(names),
                    ]
                )

print("✅ market_holidays.csv: written")
print("Done.")
