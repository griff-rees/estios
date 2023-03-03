#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import PathLike
from pathlib import Path
from typing import Final, Union

from pandas import DataFrame

from ..input_output_tables import SECTOR_DESC_COLUMN_NAME

# from ..input_output_tables import IO_TABLE_NAME
from ..sources import MetaData, OpenGovernmentLicense, pandas_from_path_or_package

# Input-Output Tables sourced from
#  https://www.ons.gov.uk/economy/nationalaccounts/supplyandusetables/datasets/inputoutputsupplyandusetables
#  https://www.ons.gov.uk/economy/nationalaccounts/supplyandusetables/datasets/ukinputoutputanalyticaltablesdetailed
#  https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/supplyandusetables/datasets/ukinputoutputanalyticaltablesdetailed/2017/nasu1719pr.xlsx

# Input-Ouput Table excel data file and configuration
# UK_GOV_INVESTMENT_COLUMN_NAMES: Final[tuple[str, ...]] = ('P51G', 'P52', 'P53')
UK_GOV_INVESTMENT_COLUMN_NAMES: Final[tuple[str, ...]] = (
    "Gross fixed capital formation",
    "changes in inventories",
    "Acquisitions less disposals of valuables",
)

CPA_COLUMN_NAME: Final[str] = "CPA"

EXCEL_FILE_NAME: Final[PathLike] = Path("nasu1719pr.xlsx")
FIRST_CODE_ROW: Final[int] = 1
USECOLS: Final[str] = "A:DO"
SKIPROWS: Final[list[int]] = [0, 1, 2, 5]  # Skips Rows 3 and 4
INDEX_COL: Final[int] = 1  # Sets index to what was the 4th (now 2nd) row
HEADER: Final[Union[int, list[int], None]] = None
# from ..input_output_tables import IO_TABLE_NAME (same as IOT)
SHEET_NAME: Final[str] = "IOT"

READ_IO_TABLE_KWARGS = dict(
    usecols=USECOLS,
    skiprows=SKIPROWS,
    index_col=INDEX_COL,
    header=None,
    sheet_name=SHEET_NAME,
)
ONS_2017_IO_TABLE_SCALING: Final[float] = 10000000.0

ONS_IO_TABLE_2017_METADATA = MetaData(
    name="ONS UK Input-Output Analytical Tables",
    path=EXCEL_FILE_NAME,
    url=(
        "https://www.ons.gov.uk/file?uri=/economy/"
        "nationalaccounts/supplyandusetables/datasets/"
        "ukinputoutputanalyticaltablesdetailed/2018/nasu1719pr.xlsx"
    ),
    year=2017,
    info_url="https://www.ons.gov.uk/economy/nationalaccounts/supplyandusetables/datasets/ukinputoutputanalyticaltablesdetailed",
    authors="Office of National Statistics",
    cite_as="Office for National Statistics – UK input-output analytical tables - product by product, UK: July to September 2017",
    auto_download=False,
    license=OpenGovernmentLicense,
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=READ_IO_TABLE_KWARGS,
)


def arrange_cpa_io_table(
    io_table: DataFrame,
    cpa_column_name: str = CPA_COLUMN_NAME,
    sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
    # imports_column_name: str = IMPORTS_COLUMN_NAME,
    # net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
    # intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
) -> DataFrame:
    """Standardise CPA indexes and columns."""
    io_table.loc[sector_desc_column_name][0] = cpa_column_name
    io_table.loc[cpa_column_name][0] = cpa_column_name
    io_table.columns = io_table.loc[sector_desc_column_name]
    io_table.index = io_table.loc[cpa_column_name, :]
    io_table.drop(sector_desc_column_name, inplace=True)
    io_table.columns = io_table.loc[:, cpa_column_name]

    # io_table.loc["Use of imported products, cif"][cpa_column_name] = imports_column_name
    # io_table.loc["Taxes less subsidies on products"][
    #     cpa_column_name
    # ] = net_subsidies_column_name
    # io_table.loc["Total intermediate/final use at purchaser's prices"][
    #     cpa_column_name
    # ] = intermediate_column_name
    assert False
    return io_table


# Census export Nomis city and sector employment data

# CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME: Final[Path] = Path(
#     "nomis-city-employment-2017.csv"
# )
# CITY_SECTOR_SKIPROWS: Final[int] = 7
# CITY_SECTOR_SKIPFOOTER: Final[int] = 8
# CITY_SECTOR_USECOLS: Final[Callable[[str], bool]] = lambda x: "Unnamed" not in x
# CITY_SECTOR_INDEX_COLUMN: Final[int] = 0
