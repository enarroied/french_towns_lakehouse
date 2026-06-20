"""Generate a daily lunar-phase dimension CSV for dim_calendar enrichment.

Design notes
------------
- astral.moon.phase() returns a continuous value on a 0-28 scale where
  0 = new moon, 7 = first quarter, 14 = full moon, 21 = last quarter.
- The 8-name split (New/Waxing Crescent/First Quarter/.../Waning Crescent)
  is OUR recalibration on top of astral's 4 coarse bands. The 2-unit/5-unit
  alternating widths are a deliberate choice — adjust PHASE_BOUNDARIES if a
  downstream consumer expects different cutoffs.
- Illumination is a linear "triangle" approximation peaking at the full
  moon, expressed as a 0.0-1.0 fraction (NOT a percentage). Adequate for BI
  trend analysis; swap in a cosine model if you need physically accurate values.
- lunar_cycle_id is the Brown Lunation Number: Lunation #1 begins on the
  1923-01-17 epoch. The +1 offset aligns with the published numbering.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

from astral import moon


OUTPUT_DIR = Path(__file__).parent

FULL_MOON_POSITION = 14.0
SYNODIC_MONTH_DAYS = 29.53059
BROWN_LUNATION_EPOCH = date(1923, 1, 17)
ONE_DAY = timedelta(days=1)

FULL_MOON_NAME = "Full Moon"
NEW_MOON_NAME = "New Moon"


class PhaseBand(NamedTuple):
    upper_bound: float
    name: str


PHASE_BOUNDARIES: tuple[PhaseBand, ...] = (
    PhaseBand(1.0, NEW_MOON_NAME),
    PhaseBand(6.0, "Waxing Crescent"),
    PhaseBand(8.0, "First Quarter"),
    PhaseBand(13.0, "Waxing Gibbous"),
    PhaseBand(15.0, FULL_MOON_NAME),
    PhaseBand(20.0, "Waning Gibbous"),
    PhaseBand(22.0, "Last Quarter"),
    PhaseBand(27.0, "Waning Crescent"),
)


@dataclass(frozen=True)
class LunarDay:
    calendar_date: date
    moon_phase_value: float
    moon_phase_name: str
    moon_illumination_fraction: float
    is_full_moon: int
    is_new_moon: int
    lunar_cycle_id: int

    def as_csv_row(self) -> dict:
        row = asdict(self)
        return {"date": row.pop("calendar_date"), **row}


def classify_phase_name(phase_value: float) -> str:
    for band in PHASE_BOUNDARIES:
        if phase_value < band.upper_bound:
            return band.name
    return NEW_MOON_NAME


def estimate_illumination(phase_value: float) -> float:
    distance_from_full = abs(phase_value - FULL_MOON_POSITION)
    illumination = 1.0 - (distance_from_full / FULL_MOON_POSITION)
    return max(0.0, min(1.0, illumination))


def calculate_lunation_id(target_date: date) -> int:
    days_elapsed = (target_date - BROWN_LUNATION_EPOCH).days
    return int(days_elapsed / SYNODIC_MONTH_DAYS) + 1


def build_lunar_day(target_date: date) -> LunarDay:
    phase_value = moon.phase(target_date)
    phase_name = classify_phase_name(phase_value)

    return LunarDay(
        calendar_date=target_date,
        moon_phase_value=round(phase_value, 2),
        moon_phase_name=phase_name,
        moon_illumination_fraction=round(estimate_illumination(phase_value), 2),
        is_full_moon=int(phase_name == FULL_MOON_NAME),
        is_new_moon=int(phase_name == NEW_MOON_NAME),
        lunar_cycle_id=calculate_lunation_id(target_date),
    )


def generate_lunar_dimension(start_year: int, end_year: int) -> Iterator[LunarDay]:
    if end_year < start_year:
        raise ValueError(f"end_year ({end_year}) must be >= start_year ({start_year})")

    current_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    while current_date <= end_date:
        yield build_lunar_day(current_date)
        current_date += ONE_DAY


def write_lunar_dimension_csv(start_year: int, end_year: int, output_path: str) -> int:
    rows_written = 0
    with Path(output_path).open("w", newline="") as f:
        writer = None
        for lunar_day in generate_lunar_dimension(start_year, end_year):
            row = lunar_day.as_csv_row()
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                writer.writeheader()
            writer.writerow(row)
            rows_written += 1
    return rows_written


if __name__ == "__main__":
    count = write_lunar_dimension_csv(
        2026, 2026, str(OUTPUT_DIR / "lunar_dimension_2026.csv")
    )
    print(f"Wrote {count} rows to lunar_dimension_2026.csv")
