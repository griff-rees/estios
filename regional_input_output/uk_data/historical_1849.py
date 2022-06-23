#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import PathLike
from pathlib import Path
from typing import Final

# Input-Output 1841 Table sourced from
# SARA HORRELL, JANE HUMPHRIES, MARTIN WEALE
# in The Economic History Review, August 1994 see:
# https://doi.org/10.1111/j.1468-0289.1994.tb01390.x


CSV_PATH: Final[PathLike] = Path("data/uk-input-output-1841-Horrell-Humphries-Weal.csv")
FIRST_CODE_ROW: Final[int] = 0
USECOLS: Final[str] = "Sectors:Total"
SKIPROWS: Final[list[int]] = [17, 18]  # Skip rows 17 and 18
INDEX_COL: Final[int] = 0
