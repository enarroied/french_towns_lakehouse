"""Generate dim_calendar enrichment CSVs using reliable registry packages.

Produces:
  french_holidays.csv – Official French public holidays
  market_holidays.csv – Euronext Paris stock market closures
"""

import csv
from datetime import date
from datetime import timedelta
from pathlib import Path

import exchange_calendars as xc
import holidays


OUTPUT_DIR = Path(__file__).parent
START_YEAR = 1900
END_YEAR = 2100


def _generate_french_holidays() -> dict[date, tuple[str, bool]]:
    """Fetch official French public holidays via the python-holidays registry."""
    fr_holidays: dict[date, tuple[str, bool]] = {}
    for y in range(START_YEAR, END_YEAR + 1):
        for d, name in sorted(holidays.France(years=y, language="fr").items()):
            if isinstance(d, date):
                fr_holidays[d] = (name, True)
    return fr_holidays


def _xpar_range() -> tuple[date, date]:
    """Return (first, last) date for which XPAR has data."""
    paris = xc.get_calendar("XPAR")
    sch = paris.schedule
    return (sch.index[0].date(), sch.index[-1].date())


def _market_closed(d: date, fr_holidays: dict[date, tuple[str, bool]]) -> bool:
    """True if date is a market holiday (XPAR data or heuristic fallback)."""
    if d.weekday() >= 5:
        return True
    return d in fr_holidays


def _market_holiday_name(d: date, fr_holidays: dict[date, tuple[str, bool]]) -> str:
    """Return the holiday name for a market-closed date."""
    if d in fr_holidays:
        return fr_holidays[d][0]
    return "Market Closure"


def write_csv_files() -> None:
    """Generate and write holiday CSVs."""
    fr_holidays = _generate_french_holidays()
    xpar_start, xpar_end = _xpar_range()
    paris = xc.get_calendar("XPAR")

    market_data: dict[date, str] = {}

    for y in range(START_YEAR, END_YEAR + 1):
        d = date(y, 1, 1)
        while d.year == y:
            is_open = (
                paris.is_session(d.strftime("%Y-%m-%d"))
                if xpar_start <= d <= xpar_end
                else not _market_closed(d, fr_holidays)
            )
            if not is_open and d.weekday() < 5:
                market_data[d] = _market_holiday_name(d, fr_holidays)
            d += timedelta(days=1)

    # Write french_holidays.csv
    csv_path = OUTPUT_DIR / "french_holidays.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "holiday_name", "is_public_holiday"])
        for d, (name, _) in sorted(fr_holidays.items()):
            writer.writerow([d.strftime("%Y-%m-%d"), name, 1])

    # Write market_holidays.csv
    csv_path = OUTPUT_DIR / "market_holidays.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "is_market_holiday", "market_holiday_name"])
        for d, name in sorted(market_data.items()):
            writer.writerow([d.strftime("%Y-%m-%d"), 1, name])

    print(f"✅ french_holidays.csv: {len(fr_holidays)} dates")
    print(f"✅ market_holidays.csv: {len(market_data)} dates")


if __name__ == "__main__":
    write_csv_files()
