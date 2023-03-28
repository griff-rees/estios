#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import PathLike
from pathlib import Path
from typing import Any, Final, Union

from pandas import DataFrame, Series

from ..input_output_tables import (
    CPA_COLUMN_NAME,
    CPA_IMPORTS_COST_INSURANCE_FREIGHT_ROW_NAME,
    CPA_TAXES_NET_SUBSIDIES_ROW_NAME,
    CPA_TOTAL_INTERMEDIATE_AT_PURCHASERS_PRICE_FIXED,
    IO_TABLE_DEFAULT_COLUMNS_NAME,
    IO_TABLE_DEFAULT_INDEX_NAME,
    SECTOR_DESC_COLUMN_NAME,
    post_read_io_table_wrapper,
)

# from ..input_output_tables import IO_TABLE_NAME
from ..sources import MetaData, OpenGovernmentLicense, pandas_from_path_or_package

# Input-Output Tables sourced from
#  https://www.ons.gov.uk/economy/nationalaccounts/supplyandusetables/datasets/inputoutputsupplyandusetables
#  https://www.ons.gov.uk/economy/nationalaccounts/supplyandusetables/datasets/ukinputoutputanalyticaltablesdetailed
#  https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/supplyandusetables/datasets/ukinputoutputanalyticaltablesdetailed/2017/nasu1719pr.xlsx

# Input-Ouput Table excel data file and configuration
# UK_GOV_INVESTMENT_COLUMN_NAMES: Final[tuple[str, ...]] = ('P51G', 'P52', 'P53')

UK_EXPORT_COLUMN_NAMES: Final[list[str]] = [
    "Exports to EU",
    "Exports outside EU",
    "Exports of services",
]

SECTOR_DESC_COLUMN_NAME: Final[str] = "Product"
TOTAL_OUTPUT_COLUMN_NAME: Final[str] = "Total Purchase"

INTERMEDIATE_DEMAND_BASE_PRICE_ROW_NAME: Final[str] = "Intermediate Demand base price"
INTERMEDIATE_DEMAND_BASE_PRICE_CODE: Final[str] = "_T"
INTERMEDIATE_DEMAND_PRICE_ROW_NAME: Final[str] = "Intermediate Demand purchase price"
TOTAL_SALES_ROW_NAME: Final[str] = "Total Sales"
IMPORTS_ROW_NAME: Final[str] = "Imports"
GROSS_VALUE_ADDED_ROW_NAME: Final[str] = "Gross Value Added"
NET_SUBSIDIES_ROW_NAME: Final[str] = "Net subsidies"

INTERMEDIATE_ROW_NAME: Final[str] = "Intermediate/final use w/purchaser's prices"

UK_FINAL_DEMAND_HOUSEHOLD_PURCHASE_CODE: Final[str] = "P3 S14"
UK_FINAL_DEMAND_GOVERNMENT_PURCHASE_CODE: Final[str] = "P3 S13"
UK_FINAL_DEMAND_NON_PROFIT_PURCHASE_CODE: Final[str] = "P3 S15"


UK_FINAL_DEMAND_HOUSEHOLD_PURCHASE_LABEL: Final[str] = "Household Purchase"
UK_FINAL_DEMAND_GOVERNMENT_PURCHASE_LABEL: Final[str] = "Government Purchase"
UK_FINAL_DEMAND_NON_PROFIT_PURCHASE_LABEL: Final[str] = "Non-profit Purchase"

UK_FINAL_DEMAND_COLUMN_KEYS: Final[dict[str, str]] = {
    UK_FINAL_DEMAND_HOUSEHOLD_PURCHASE_CODE: UK_FINAL_DEMAND_HOUSEHOLD_PURCHASE_LABEL,
    UK_FINAL_DEMAND_GOVERNMENT_PURCHASE_CODE: UK_FINAL_DEMAND_GOVERNMENT_PURCHASE_LABEL,
    UK_FINAL_DEMAND_NON_PROFIT_PURCHASE_CODE: UK_FINAL_DEMAND_NON_PROFIT_PURCHASE_LABEL,
}

UK_EXPORTS_TO_EU_COLUMN_CODE: Final[str] = "P61EU"
UK_EXPORTS_OUTSIDE_EU_CODE: Final[str] = "P61RW"
UK_EXPORTS_OF_SERVICES_CODE: Final[str] = "P62"


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


def arrange_cpa_io_table(
    io_table: DataFrame,
    cpa_row_name: str | None = CPA_COLUMN_NAME,
    cpa_column_name: str | None = CPA_COLUMN_NAME,
    sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
    imports_row_name: str = IMPORTS_ROW_NAME,
    net_subsidies_row_name: str = NET_SUBSIDIES_ROW_NAME,
    intermediate_row_name: str = INTERMEDIATE_ROW_NAME,
    cpa_import_cpa_row_name: str = CPA_IMPORTS_COST_INSURANCE_FREIGHT_ROW_NAME,
    cpa_taxes_net_subsidies_row_name: str = CPA_TAXES_NET_SUBSIDIES_ROW_NAME,
    cpa_intermediate_at_purchase_price_row_name: str = CPA_TOTAL_INTERMEDIATE_AT_PURCHASERS_PRICE_FIXED,
    input_index_label: str = IO_TABLE_DEFAULT_INDEX_NAME,
    output_column_label: str = IO_TABLE_DEFAULT_COLUMNS_NAME,
    return_tuple: bool = False,
) -> tuple[DataFrame, Series, Series]:
    """Standardise CPA rows and columns.

    Todo:
        * Check if cpa parameters are too UK sepecific
        * If possible more to estios/uk/intput_output_table.py
        * See https://www.ons.gov.uk/economy/grossdomesticproductgdp/compendium/unitedkingdomnationalaccountsthebluebook/2022/pdf
    """
    io_table.loc[sector_desc_column_name][0] = cpa_row_name
    io_table.loc[cpa_row_name][0] = cpa_row_name
    io_table.columns = io_table.loc[sector_desc_column_name]
    io_table.drop(sector_desc_column_name, inplace=True)

    io_table.loc[cpa_import_cpa_row_name][cpa_column_name] = imports_row_name
    io_table.loc[cpa_taxes_net_subsidies_row_name][
        cpa_row_name
    ] = net_subsidies_row_name
    io_table.loc[cpa_intermediate_at_purchase_price_row_name][
        cpa_column_name
    ] = intermediate_row_name
    io_table.index.name = input_index_label
    io_table.columns.name = output_column_label
    input_row_labels: Series = io_table.index
    output_column_labels: Series = io_table.columns
    input_row_labels = input_row_labels.drop(cpa_row_name)
    output_column_labels = output_column_labels.drop(cpa_column_name)
    input_row_labels = input_row_labels.map(lambda x: x.strip())
    output_column_labels = output_column_labels.map(lambda x: x.strip())
    # io_table.set_index(cpa_column_name, inplace=True)# = io_table.loc[cpa_row_name, :]
    io_table.set_index(cpa_column_name, inplace=True)  # = io_table.loc[cpa_row_name, :]
    io_table.columns = io_table.loc[cpa_row_name, :]
    # io_table.index = io_table.loc[:, cpa_column_name]
    io_table.drop(cpa_row_name, inplace=True)
    # io_table.drop(cpa_column_name, axis="columns", inplace=True)
    io_table.index.name = input_index_label
    io_table.columns.name = output_column_label
    assert len(io_table.index) == len(input_row_labels)
    assert len(io_table.columns) == len(output_column_labels)
    if return_tuple:
        return io_table, input_row_labels, output_column_labels
    else:
        return io_table


POST_READ_KWARGS: dict[str, Any] = dict(
    func=arrange_cpa_io_table,
    return_tuple=True,
)

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
    auto_download=True,
    # auto_download=False,
    license=OpenGovernmentLicense,
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=READ_IO_TABLE_KWARGS,
    _post_read_func=post_read_io_table_wrapper,
    _post_read_kwargs=POST_READ_KWARGS,
)


# def arrange_cpa_io_table(
#     io_table: DataFrame,
#     cpa_column_name: str = CPA_COLUMN_NAME,
#     sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
#     # imports_column_name: str = IMPORTS_COLUMN_NAME,
#     # net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
#     # intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
# ) -> DataFrame:
#     """Standardise CPA indexes and columns."""
#     io_table.loc[sector_desc_column_name][0] = cpa_column_name
#     io_table.loc[cpa_column_name][0] = cpa_column_name
#     io_table.columns = io_table.loc[sector_desc_column_name]
#     io_table.index = io_table.loc[cpa_column_name, :]
#     io_table.drop(sector_desc_column_name, inplace=True)
#     io_table.columns = io_table.loc[:, cpa_column_name]
#
#     # io_table.loc["Use of imported products, cif"][cpa_column_name] = imports_column_name
#     # io_table.loc["Taxes less subsidies on products"][
#     #     cpa_column_name
#     # ] = net_subsidies_column_name
#     # io_table.loc["Total intermediate/final use at purchaser's prices"][
#     #     cpa_column_name
#     # ] = intermediate_column_name
#     assert False
#     return io_table


# Census export Nomis city and sector employment data

# CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME: Final[Path] = Path(
#     "nomis-city-employment-2017.csv"
# )
# CITY_SECTOR_SKIPROWS: Final[int] = 7
# CITY_SECTOR_SKIPFOOTER: Final[int] = 8
# CITY_SECTOR_USECOLS: Final[Callable[[str], bool]] = lambda x: "Unnamed" not in x
# CITY_SECTOR_INDEX_COLUMN: Final[int] = 0
