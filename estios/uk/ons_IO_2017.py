#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import PathLike
from pathlib import Path
from typing import Callable, Final, Union

# Input-Output Tables sourced from
#  https://www.ons.gov.uk/economy/nationalaccounts/supplyandusetables/datasets/inputoutputsupplyandusetables
#  https://www.ons.gov.uk/economy/nationalaccounts/supplyandusetables/datasets/ukinputoutputanalyticaltablesdetailed
#  https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/supplyandusetables/datasets/ukinputoutputanalyticaltablesdetailed/2017/nasu1719pr.xlsx

# Input-Ouput Table excel data file and configuration

EXCEL_FILE_NAME: Final[PathLike] = Path("nasu1719pr.xlsx")
FIRST_CODE_ROW: Final[int] = 1
USECOLS: Final[str] = "A:DO"
SKIPROWS: Final[list[int]] = [0, 1, 2, 5]  # Skips Rows 3 and 4
INDEX_COL: Final[int] = 1  # Sets index to what was the 4th (now 2nd) row
HEADER: Final[Union[int, list[int], None]] = None

# Census export Nomis city and sector employment data

CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME: Final[Path] = Path(
    "nomis-city-employment-2017.csv"
)
CITY_SECTOR_SKIPROWS: Final[int] = 7
CITY_SECTOR_SKIPFOOTER: Final[int] = 8
CITY_SECTOR_USECOLS: Final[Callable[[str], bool]] = lambda x: "Unnamed" not in x
CITY_SECTOR_INDEX_COLUMN: Final[int] = 0
