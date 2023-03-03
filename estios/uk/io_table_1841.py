#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from os import PathLike
from pathlib import Path
from typing import Final

# from data import io_table_1841
from ..sources import MetaData, pandas_from_path_or_package

NAME: Final[str] = "1841 Input-Output Table"
YEAR: Final[int] = 1841
REGION: Final[str] = "United Kingdom"
AUTHORS: Final[dict[str, str]] = {
    "Sara Horrell": "https://www.lse.ac.uk/Economic-History/People/Faculty-and-teachers/Professor-Sara-Horrell",
    "Jane Humphries": "https://www.asc.ox.ac.uk/person/professor-jane-humphries",
    "Martin Weale": "https://www.kcl.ac.uk/people/martin-weale-1",
}
DOI: Final[str] = "10.2307/2597593"

ABSTRACT: Final[
    str
] = "An input-output table for the UK for 1841 shows the country's economic structure and the interdependence and linkages between industries. Using estimates of the capital and labour employed, the productivity of each industry can be identified. The early industrial economy exhibited considerable interrelatedness between industries, and the importance of the linkages of the metal industries is confirmed. But the economy was bifurcated into agriculture-based and mineral-based industries with the latter having less important backward linkages and lower productivity than the former."

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

METADATA: Final[MetaData] = MetaData(
    name=NAME,
    year=YEAR,
    date_published=date(1994, 8, 1),
    authors=AUTHORS,
    region=REGION,
    doi=DOI,
    description=ABSTRACT,
    path=CSV_FILE_NAME,
    file_name_from_url=False,
    auto_download=True,
    needs_scaling=True,
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=dict(skiprows=SKIPROWS, index_col=INDEX_COL),
)
