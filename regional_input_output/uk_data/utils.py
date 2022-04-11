#!/usr/bin/env python
# -*- coding: utf-8 -*-

from importlib.resources import open_binary, open_text
from logging import getLogger

from pandas import DataFrame, Series, read_csv, read_excel

# from ..input_output_tables import (
#     IO_TABLE_FINAL_DEMAND_COLUMN_NAMES,
#     IO_TABLE_NAME,
#     IO_TABLE_IMPORTS_COLUMN_NAME,
#     AggregatedSectorDictType
# )
# import ons_IO_2017

logger = getLogger(__name__)

# from .ons_IO_2017 import (
#     2017_EXCEL_PATH,
#     FIST_CODE_ROW,
#     USECOLS,
#     SKIPROWS,
#     INDEX_COL,
#     HEADER,
# )
# UK_IO_TABLE_TOTAL_PRODUCTION_COLUMN_NAME: Final[str] = "Total Sales"
# UK_IO_TABLE_IMPORTS_COLUMN_NAME: Final[str] = "Imports"
# UK_IO_TABLE_FINAL_DEMAND_COLUMN_NAMES: Final[list[str]] = [

# IO_TABLE_2017_EXCEL_PATH
