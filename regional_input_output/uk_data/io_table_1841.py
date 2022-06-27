#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import PathLike
from pathlib import Path
from typing import Final

# from data import io_table_1841


# Input-Output 1841 Table sourced from
# SARA HORRELL, JANE HUMPHRIES, MARTIN WEALE
# in The Economic History Review, August 1994 see:
# https://doi.org/10.1111/j.1468-0289.1994.tb01390.x


NAME: Final[str] = "1841 Input-Output Table"
YEAR: Final[int] = 1841
REGION: Final[str] = "United Kingdom"
AUTHORS: Final[dict[str, str]] = {
    "Sara Horrell": "https://www.lse.ac.uk/Economic-History/People/Faculty-and-teachers/Professor-Sara-Horrell",
    "Jane Humphries": "https://www.asc.ox.ac.uk/person/professor-jane-humphries",
    "Martin Weale": "https://www.kcl.ac.uk/people/martin-weale-1",
}
DOI: Final[str] = "10.2307/2597593"


CSV_FILE_NAME: Final[PathLike] = Path("uk-input-output-1841-Horrell-Humphries-Weal.csv")
FIRST_CODE_ROW: Final[int] = 0
SKIPROWS: Final[list[int]] = [21, 22]  # Skip rows 17 and 18
INDEX_COL: Final[str] = "Sectors"

HISTORIC_UK_SECTORS: Final[list[str]] = [
    "Agriculture",
    "Mining and quarrying",
    "Food, drink, and tobacco",
    "Metal manufacture",
    "Soap, candles, and dyes",
    "Textiles, clothing, and leather goods",
    "Metal goods",
    "Bricks, pottery, and glass",
    "Other manufacturing",
    "Construction",
    "Gas and water",
    "Transport",
    "Distribution",
    "Domestic service",
    "Other service",
    "Public adminstration and defence",
    "Housing services",
]

FINAL_COLUMNS: Final[list[str]] = ["Consumption", "Investment", "Exports", "Total"]

COLUMNS: Final[list[str]] = (
    [
        INDEX_COL,
    ]
    + HISTORIC_UK_SECTORS
    + FINAL_COLUMNS
)
