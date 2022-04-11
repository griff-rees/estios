#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from pathlib import Path
from typing import Final, Iterable

# ONS jobs data

UK_JOBS_BY_SECTOR_PATH: Final[Path] = Path("jobs05sep2021.xls")
DATE_COLUMN_NAME: Final[str] = "SIC 2007 section"
UK_NATIONAL_EMPLOYMENT_SHEET: Final[str] = "15. United Kingdom"
UK_JOBS_BY_SECTOR_SCALING: Final[float] = 1000

UK_CITY_SECTOR_YEARS: Final[list] = [2011, 2017]

CITY_SECTOR_REGION_PREFIX: Final[str] = "towncity:"

# Note: These are *template* str that may be superfluous
CITY_SECTOR_AVERAGE_EARNINGS_COLUMN_STR: Final[
    str
] = "Average Weekly Workplace Earnings YEAR  (Â£)"
CITY_SECTOR_POPULATION_COLUMN_STR: Final[str] = "Population YEAR"  # 1981 - 2019
CITY_SECTOR_EDUCATION_COLUMN_STR: Final[
    str
] = "Pupils achieving 9-4 grades in Maths & English at GCSE YEAR  (%)"  # 2017 - 2019

CITY_SECTOR_AVERAGE_EARNINGS_COLUMN: Final[
    str
] = "Average Weekly Workplace Earnings 2017  (Â£)"
CITY_SECTOR_POPULATION_COLUMN: Final[str] = "Population 2017"  # 1981 - 2019
CITY_SECTOR_EDUCATION_COLUMN: Final[
    str
] = "Pupils achieving 9-4 grades in Maths & English at GCSE 2017  (%)"  # 2017 - 2019


def generate_employment_quarterly_dates(
    years: Iterable[int], reverse: bool = False
) -> Iterable[date]:
    """Return quaterly dates for UK employment data in reverse chronological order."""
    for year in years:
        if reverse:
            for month in range(12, 0, -3):
                yield date(year, month, 1)
        else:
            for month in range(3, 13, 3):
                yield date(year, month, 1)


CONFIG_2017_QUARTERLY: Final[dict[date, dict["str", date]]] = {
    date: {"employment_date": date}
    for date in generate_employment_quarterly_dates([2017], reverse=False)
}
EMPLOYMENT_QUARTER_DEC_2017: Final[date] = tuple(CONFIG_2017_QUARTERLY)[-1]

CONFIG_2015_TO_2017_QUARTERLY: Final[dict[date, dict["str", date]]] = {
    date: {"employment_date": date}
    for date in generate_employment_quarterly_dates([2015, 2016, 2017], reverse=False)
}
