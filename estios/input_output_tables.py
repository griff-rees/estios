#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Infrastructure for managing Input-Output tables

Todo:
    * Move UK specific elements to uk/intput_output_table.py
    * Rename dog_leg_row and dog_leg_row (perhaps summary row and columns or total...?)
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import (
    Any,
    Callable,
    Final,
    Generator,
    Iterable,
    Literal,
    Optional,
    Sequence,
)

from pandas import DataFrame, Index, MultiIndex, Series
from pymrio import IOSystem, MRIOMetaData, download_oecd, parse_oecd

from .calc import technical_coefficients
from .sector_codes import OECD_FINAL_DEMAND_COLUMN_NAMES
from .sources import (
    MetaData,
    MetaFileOrDataFrameType,
    ModelDataSourcesHandler,
    OECDTermsAndConditions,
    pandas_from_path_or_package,
)

# from .uk import io_table_1841  # , ons_employment_2017
from .utils import (
    REGION_COLUMN_NAME,
    SECTOR_10_CODE_DICT,
    SECTOR_COLUMN_NAME,
    AggregatedSectorDictType,
    DateType,
    SectorName,
    YearType,
    df_to_trimmed_multi_index,
    gen_region_attr_multi_index,
    match_df_cols_rows,
)

logger = getLogger(__name__)

IO_TABLE_DEFAULT_INDEX_NAME: str = "Input"
IO_TABLE_DEFAULT_COLUMNS_NAME: str = "Output"

DogLegType = dict[str, str] | Sequence[str]
IO_TABLE_NAME: Final[str] = "IOT"  # Todo: see if this is the standard sheet name
COEFFICIENT_TABLE_NAME: Final[str] = "A"

CPA_COLUMN_NAME: Final[str] = "CPA"
TOTAL_PRODUCTION_ROW_NAME: Final[str] = "Intermediate Demand"
SECTOR_DESC_COLUMN_NAME: Final[str] = "Product"


### Todo: resolve ambiguity in ROW/INDEX variable labels

TOTAL_PRODUCTION_INDEX_NAME: Final[str] = TOTAL_PRODUCTION_ROW_NAME
ONS_GROSS_VALUE_ADDED_INDEX_NAME: Final[str] = "Gross value added"
IMPORTS_COLUMN_NAME: Final[str] = "Imports"

FINAL_DEMAND_LABEL: Final[str] = "Final Demand"
FINAL_DEMAND_COLUMN_NAMES: Final[list[str]] = [
    "Household Purchase",
    "Government Purchase",
    "Non-profit Purchase",
]

DEFAULT_REGION: Final[str] = "Region"
# Todo: Rename ot DEFAULT_REGION_LABEL
# UK_EXPORT_COLUMN_NAMES: Final[list[str]] = [
#     "Exports to EU",
#     "Exports outside EU",
#     "Exports of services",
# ]


COVID_FLAGS_COLUMN: Final[str] = "COVID_FLAGS"

NET_SUBSIDIES_COLUMN_NAME: Final[str] = "Net subsidies"
INTERMEDIATE_COLUMN_NAME: Final[str] = "Intermediate/final use w/purchaser's prices"

TOTAL_OUTPUT_COLUMN_NAME: Final[str] = "Total Purchase"
GROSS_CAPITAL_FORMATION_COLUMN_NAME: Final[str] = "Gross fixed capital formation"
INVENTORY_CHANGE_COLUMN_NAME: Final[str] = "changes in inventories"
ACQUISITION_NET_VALUABLES_DISPOAL_COLUMN_NAME: Final[
    str
] = "Acquisitions less disposals of valuables"

INTERMEDIATE_DEMAND_BASE_PRICE_ROW_NAME: Final[str] = "Intermediate Demand base price"
INTERMEDIATE_DEMAND_PRICE_ROW_NAME: Final[str] = "Intermediate Demand purchase price"
TOTAL_SALES_ROW_NAME: Final[str] = "Total Sales"
IMPORTS_ROW_NAME: Final[str] = "Imports"
GROSS_VALUE_ADDED_ROW_NAME: Final[str] = "Gross Value Added"
NET_SUBSIDIES_ROW_NAME: Final[str] = "Net subsidies"

INTERMEDIATE_ROW_NAME: Final[str] = "Intermediate/final use w/purchaser's prices"

CPA_IMPORTS_COST_INSURANCE_FREIGHT_ROW_NAME: Final[
    str
] = "Use of imported products, cif"
CPA_TAXES_NET_SUBSIDIES_ROW_NAME: Final[str] = "Taxes less subsidies on products"
CPA_TOTAL_INTERMEDIATE_AT_PURCHASERS_PRICE: Final[
    str
] = "Total intermediate/final use at purchaser's prices"
# CPA_TOTAL_INTERMEDIATE_AT_PURCHASERS_PRICE_FIXED: Final[
#     str
# ] = "Total intermediate use at purchaser's prices"


DEFAULT_DOG_LEG_ROWS: Final[tuple[str, ...]] = (
    INTERMEDIATE_DEMAND_BASE_PRICE_ROW_NAME,
    INTERMEDIATE_DEMAND_PRICE_ROW_NAME,
    TOTAL_SALES_ROW_NAME,
    IMPORTS_ROW_NAME,
    GROSS_VALUE_ADDED_ROW_NAME,
    NET_SUBSIDIES_ROW_NAME,
)

DEFAULT_DOG_LEG_COLUMNS: Final[tuple[str, ...]] = (
    *FINAL_DEMAND_COLUMN_NAMES,
    TOTAL_OUTPUT_COLUMN_NAME,
)

DEFAULT_INPUT_OUTPUT_META_DATA_NAME: Final[str] = "Input-Output Data File"

DEFAULT_OECD_STORAGE_PATH: Final[Path] = Path("data/oecd/")


IO_TABLE_ATTR_NAME: Final[str] = "raw_io_table"
IO_TABLE_ALL_INPUT_ROW_LABELS_ATTR_NAME: Final[str] = "all_input_row_labels"
IO_TABLE_ALL_INPUT_COLUMN_LABELS_ATTR_NAME: Final[str] = "all_output_column_labels"

DEFAULT_IO_MODEL_ATTR_LABELS: Final[tuple[str, str, str]] = (
    IO_TABLE_ATTR_NAME,
    IO_TABLE_ALL_INPUT_ROW_LABELS_ATTR_NAME,
    IO_TABLE_ALL_INPUT_COLUMN_LABELS_ATTR_NAME,
)

IO_TABLE_ALL_INPUT_ROW_ATTR: Final[
    str
] = f"_{IO_TABLE_ATTR_NAME}__{IO_TABLE_ALL_INPUT_ROW_LABELS_ATTR_NAME}"
IO_TABLE_ALL_INPUT_COLUMN_ATTR: Final[
    str
] = f"_{IO_TABLE_ATTR_NAME}__{IO_TABLE_ALL_INPUT_COLUMN_LABELS_ATTR_NAME}"


def post_read_io_table_wrapper(
    data: DataFrame | Series,
    func: Callable,
    attr_labels=DEFAULT_IO_MODEL_ATTR_LABELS,
    **kwargs,
) -> dict:
    # df, input_row_labels, output_row_labels = func(data, **kwargs)
    results_tuple = func(data, **kwargs)
    assert len(results_tuple) == len(attr_labels)
    return {attr_labels[i]: results_tuple[i] for i in range(len(results_tuple))}


def crop_io_table_to_sectors(
    full_io_table_df: DataFrame, sectors: Iterable[str], sector_prefix: str = ""
) -> DataFrame:
    """Drop extra rows and colums of full_io_table_df to just input-output of sectors."""
    if sector_prefix:
        sectors = [sector_prefix + sector for sector in sectors]
    return full_io_table_df.filter(items=sectors, axis="index").filter(
        items=sectors, axis="columns"
    )


def cpa_io_table_to_codes(
    full_io_table_df: DataFrame, sector_code_column: str = CPA_COLUMN_NAME
) -> DataFrame:
    """Convert an Input Output DataFrame table (default ONS) to use coded column names."""
    io_table: DataFrame = full_io_table_df.set_index(sector_code_column)
    io_table.columns = io_table.loc[sector_code_column]
    return io_table.drop(sector_code_column)


# def load_io_table_csv(
#     path: FilePathType = io_table_1841.CSV_FILE_NAME,
#     usecols: Optional[Union[str, list[str]]] = io_table_1841.COLUMNS,
#     skiprows: Optional[list[int]] = io_table_1841.SKIPROWS,
#     index_col: Optional[Union[int, str]] = io_table_1841.INDEX_COL,
#     cpa_column_name: Optional[str] = None,
#     sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
#     imports_column_name: str = IMPORTS_ROW_NAME,
#     net_subsidies_column_name: str = NET_SUBSIDIES_ROW_NAME,
#     intermediate_column_name: str = INTERMEDIATE_ROW_NAME,
#     **kwargs,
# ) -> DataFrame:
#     """Import an Input-Ouput Table as a DataFrame from a csv file.
#
#     Todo:
#         * Raise warning if the file has the wrong extension.
#         * Fix packaging of csv file
#     """
#     path = path_or_package_data(path, io_table_1841.CSV_FILE_NAME)
#     io_table: DataFrame = read_csv(
#         path,
#         usecols=usecols,
#         skiprows=skiprows,
#         index_col=index_col,
#         **kwargs,
#     )
#     if cpa_column_name:
#         io_table = arrange_cpa_io_table(
#             io_table,
#             cpa_column_name,
#             sector_desc_column_name,
#             imports_column_name,
#             net_subsidies_column_name,
#             intermediate_column_name,
#         )
#     return io_table
#
#
# def load_io_table_excel(
#     path: FilePathType = ons_IO_2017.EXCEL_FILE_NAME,
#     sheet_name: str = IO_TABLE_NAME,
#     usecols: Optional[str] = ons_IO_2017.USECOLS,
#     skiprows: Optional[list[int]] = ons_IO_2017.SKIPROWS,  # Default skips Rows 3 and 4
#     index_col: Optional[int] = ons_IO_2017.INDEX_COL,
#     header: Optional[Union[int, list[int]]] = ons_IO_2017.HEADER,
#     cpa_column_name: str = CPA_COLUMN_NAME,
#     sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
#     imports_column_name: str = IMPORTS_ROW_NAME,
#     net_subsidies_column_name: str = NET_SUBSIDIES_ROW_NAME,
#     intermediate_column_name: str = INTERMEDIATE_ROW_NAME,
#     **kwargs,
# ) -> DataFrame:
#     """Import an Input-Ouput Table as a DataFrame from an ONS xlsx file."""
#     path = path_or_package_data(path, ons_IO_2017.EXCEL_FILE_NAME)
#     io_table: DataFrame = read_excel(
#         path,
#         sheet_name=sheet_name,
#         usecols=usecols,
#         skiprows=skiprows,
#         index_col=index_col,
#         header=header,
#         **kwargs,
#     )
#     if cpa_column_name:
#         io_table = arrange_cpa_io_table(
#             io_table,
#             cpa_column_name,
#             sector_desc_column_name,
#             imports_column_name,
#             net_subsidies_column_name,
#             intermediate_column_name,
#         )
#     return io_table


def arrange_cpa_io_table(
    io_table: DataFrame,
    cpa_column_name: Optional[str] = None,
    sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
    imports_column_name: str = IMPORTS_COLUMN_NAME,
    net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
    intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
) -> DataFrame:
    """Standardise CPA indexes and columns."""
    io_table.loc[sector_desc_column_name][0] = cpa_column_name
    io_table.loc[cpa_column_name][0] = cpa_column_name
    io_table.columns = io_table.loc[sector_desc_column_name]
    io_table.drop(sector_desc_column_name, inplace=True)

    # io_table.loc["Use of imported products, cif"][cpa_column_name] = imports_column_name
    # io_table.loc["Taxes less subsidies on products"][
    #     cpa_column_name
    # ] = net_subsidies_column_name
    # io_table.loc["Total intermediate/final use at purchaser's prices"][
    #     cpa_column_name
    # ] = intermediate_column_name
    return io_table


# Comment this out if possible
# def load_io_table_csv(
#     path: FilePathType = io_table_1841.CSV_FILE_NAME,
#     usecols: Optional[Union[str, list[str]]] = io_table_1841.COLUMNS,
#     skiprows: Optional[list[int]] = io_table_1841.SKIPROWS,
#     index_col: Optional[Union[int, str]] = io_table_1841.INDEX_COL,
#     cpa_column_name: Optional[str] = None,
#     sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
#     imports_column_name: str = IMPORTS_COLUMN_NAME,
#     net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
#     intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
#     **kwargs,
# ) -> DataFrame:
#     """Import an Input-Ouput Table as a DataFrame from a csv file.
#
#     Todo:
#         * Raise warning if the file has the wrong extension.
#         * Fix packaging of csv file
#     """
#     path = path_or_package_data(path, io_table_1841.CSV_FILE_NAME)
#     io_table: DataFrame = read_csv(
#         path,
#         usecols=usecols,
#         skiprows=skiprows,
#         index_col=index_col,
#         **kwargs,
#     )
#     if cpa_column_name:
#         io_table = arrange_cpa_io_table(
#             io_table,
#             cpa_column_name,
#             sector_desc_column_name,
#             imports_column_name,
#             net_subsidies_column_name,
#             intermediate_column_name,
#         )
#     return io_table


# def load_io_table_excel(
#     path: FilePathType = ons_IO_2017.EXCEL_FILE_NAME,
#     sheet_name: str = IO_TABLE_NAME,
#     usecols: Optional[str] = ons_IO_2017.USECOLS,
#     skiprows: Optional[list[int]] = ons_IO_2017.SKIPROWS,  # Default skips Rows 3 and 4
#     index_col: Optional[int] = ons_IO_2017.INDEX_COL,
#     header: Optional[Union[int, list[int]]] = ons_IO_2017.HEADER,
#     cpa_column_name: str = CPA_COLUMN_NAME,
#     sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
#     imports_column_name: str = IMPORTS_COLUMN_NAME,
#     net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
#     intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
#     **kwargs,
# ) -> DataFrame:
#     """Import an Input-Ouput Table as a DataFrame from an ONS xlsx file."""
#     path = path_or_package_data(path, ons_IO_2017.EXCEL_FILE_NAME)
#     io_table: DataFrame = read_excel(
#         path,
#         sheet_name=sheet_name,
#         usecols=usecols,
#         skiprows=skiprows,
#         index_col=index_col,
#         header=header,
#         **kwargs,
#     )
#     if cpa_column_name:
#         io_table = arrange_cpa_io_table(
#             io_table,
#             cpa_column_name,
#             sector_desc_column_name,
#             imports_column_name,
#             net_subsidies_column_name,
#             intermediate_column_name,
#         )
#     return io_table


# def load_employment_by_region_and_sector_csv(
#     path: FilePathType = ons_employment_2017.CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME,
#     skiprows: int = ons_employment_2017.CITY_SECTOR_SKIPROWS,
#     skipfooter: int = ons_employment_2017.CITY_SECTOR_SKIPFOOTER,
#     engine: str = ons_employment_2017.CITY_SECTOR_ENGINE,
#     usecols: Callable[[str], bool] = ons_employment_2017.CITY_SECTOR_USECOLS,
#     index_col: int = ons_employment_2017.CITY_SECTOR_INDEX_COLUMN,
#     **kwargs,
# ) -> DataFrame:
#     """Import region level sector employment data as a DataFrame."""
#     path = path_or_package_data(
#         path, ons_employment_2017.CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME
#     )
#     return read_csv(
#         path,
#         skiprows=skiprows,
#         skipfooter=skipfooter,
#         engine=engine,
#         usecols=usecols,
#         index_col=index_col,
#         **kwargs,
#     )


# def aggregate_io_table(
#     agg_sector_dict: AggregatedSectorDictType,  # UK_SECTOR_10_CODE_DICT would suit
#     code_io_table: DataFrame,
#     dog_leg_columns: dict[str, str],
#     dog_leg_rows: dict[str, str],
# ) -> DataFrame:
#     """Return an aggregated Input Output table via an aggregated mapping of sectors."""
#     # Todo: decide whether this dict copy (shallow) is worth keeping
#     aggregated_sector_io_table = DataFrame(
#         columns=list(agg_sector_dict.keys()) + list(dog_leg_columns.keys()),
#         index=list(agg_sector_dict.keys()) + list(dog_leg_rows.keys()),
#     )
#
#     for sector_column in agg_sector_dict:
#         for sector_row in agg_sector_dict:
#             sector_column_names: Sequence[str] = agg_sector_dict[sector_column]
#             sector_row_names: Sequence[str] = agg_sector_dict[sector_row]
#             aggregated_sector_io_table.loc[
#                 sector_column, sector_row
#             ] = (  # Check column row order
#                 code_io_table.loc[sector_column_names, sector_row_names].sum().sum()
#             )
#             for dog_leg_column, source_column_name in dog_leg_columns.items():
#                 aggregated_sector_io_table.loc[
#                     sector_row, dog_leg_column
#                 ] = code_io_table.loc[sector_row_names, source_column_name].sum()
#         for dog_leg_row, source_row_name in dog_leg_rows.items():
#             aggregated_sector_io_table.loc[
#                 dog_leg_row, sector_column
#             ] = code_io_table.loc[source_row_name, sector_column_names].sum()
#     return aggregated_sector_io_table


# @dataclass
# class InputOutputTable:
#
#     """Manage processing and aggregating Input Output Tables."""
#
#     path: Optional[FilePathType] = None
#     full_io_table: Optional[DataFrame] = None
#     base_io_table: Optional[DataFrame] = None
#     io_scaling_factor: float = IO_TABLE_SCALING
#     sector_names: Optional[Series] = field(
#         default_factory=lambda: io_table_1841.HISTORIC_UK_SECTORS
#     )
#     sector_aggregation_dict: Optional[AggregatedSectorDictType] = None
#     sector_prefix_str: str = ""
#     io_table_kwargs: dict[str, Any] = field(default_factory=dict)
#     meta_data: Optional[MetaData] = None
#     national_gva: str | Series | None = GROSS_VALUE_ADDED_INDEX_NAME
#     national_net_subsidies: str | Series | None = NET_SUBSIDIES_COLUMN_NAME
#
#     dog_leg_columns: dict[str, str] = field(default_factory=dict)
#     dog_leg_rows: dict[str, str] = field(default_factory=dict)
#     _process_full_io_table: Callable[..., DataFrame] = crop_io_table_to_sectors
#     _table_load_func: Callable[..., DataFrame] = load_io_table_csv
#
#     class NullIOTableError(Exception):
#         pass
#
#     class NoSectorAggregationDictError(Exception):
#         pass
#
#     def _init_base_io_tables(self) -> None:
#         if (
#             self.full_io_table is None
#             and self.base_io_table is None
#             and self.path is None
#         ):
#             raise self.NullIOTableError(
#                 "One of full_io_table, base_io_table or path attributes must be set."
#             )
#         if self.full_io_table is None and self.path:
#             self.full_io_table: DataFrame = self._table_load_func(
#                 self.path, **self.io_table_kwargs
#             )
#         if self.base_io_table is None:  # Assumes full_io_table is set
#             self.base_io_table = self._process_full_io_table(
#                 self.full_io_table, self.sectors, self.sector_prefix_str
#             )
#         if not self.meta_data and self.path == io_table_1841.CSV_FILE_NAME:
#             self.meta_data = MetaData(
#                 name=io_table_1841.NAME,
#                 year=io_table_1841.YEAR,
#                 region=io_table_1841.REGION,
#                 authors=io_table_1841.AUTHORS,
#                 doi=io_table_1841.DOI,
#             )
#
#     @property
#     def sectors(self) -> Series:
#         """If sector_names is None, populate with sector_aggregation_dict keys, else error.
#
#         Todo:
#             * This may need a further refactor
#             * Assume simpler! and possibly remote sector_names now that the
#               index is managed in CPA
#         """
#         if self.sector_names is not None:
#             return self.sector_names
#         elif (
#             self.sector_aggregation_dict
#         ):  # Default to sector_aggregation_dict keys if sector_names not set
#             logger.debug("Returning {self} sector_aggregation_dict keys")
#             return Series(self.sector_aggregation_dict.keys())
#         raise ValueError("Neither {self} sector_names nor sector_aggregation_dict set.")
#
#     def __post_init__(self) -> None:
#         self._init_base_io_tables()
#
#     @property
#     def _aggregated_sectors_dict(self) -> AggregatedSectorDictType:
#         """Call aggregate_sector_dict on the sectors property."""
#         if self.sector_aggregation_dict:
#             return aggregate_sector_dict(
#                 self.sectors, self.sector_aggregation_dict, self.sector_prefix_str
#             )
#         else:
#             raise self.NoSectorAggregationDictError
#
#     def get_aggregated_io_table(self) -> DataFrame:
#         """Return aggregated io_table"""
#         return aggregate_io_table(
#             self._aggregated_sectors_dict,
#             self.base_io_table,
#             self.dog_leg_columns,
#             self.dog_leg_rows,
#         )
#
#
# @dataclass
# class InputOutputCPATable(InputOutputTable):
#
#     """Manage processing and aggregating CPA format Input Output Tables.
#
#     Note:
#      * CPA stands for Classification of Products by Activity, see
#        https://ec.europa.eu/eurostat/web/cpa/cpa-2008
#      * Sector aggregation defaults to 10 see
#        https://ec.europa.eu/eurostat/documents/1965803/1978839/NACEREV.2INTRODUCTORYGUIDELINESEN.pdf/f48c8a50-feb1-4227-8fe0-935b58a0a332
#     """
#
#     path: FilePathType = ons_IO_2017.EXCEL_FILE_NAME
#     cpa_column_name: str = CPA_COLUMN_NAME
#     sector_prefix_str: str = CPA_COLUMN_NAME
#     sector_aggregation_dict: Optional[AggregatedSectorDictType] = field(
#         default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
#     )
#     io_table_kwargs: dict[str, Any] = field(
#         default_factory=lambda: {"sheet_name": IO_TABLE_NAME}
#     )
#     dog_leg_columns: dict[str, str] = field(
#         default_factory=lambda: UK_DOG_LEG_CODES["columns"]
#     )
#     dog_leg_rows: dict[str, str] = field(
#         default_factory=lambda: UK_DOG_LEG_CODES["rows"]
#     )
#     _first_code_row: int = ons_IO_2017.FIRST_CODE_ROW
#     _io_table_code_to_labels_func: Callable[
#         [DataFrame, str], DataFrame
#     ] = io_table_to_codes
#     _table_load_func: Callable[..., DataFrame] = load_io_table_excel
#
#     def __post_init__(self) -> None:
#         """Call the core _init_base_io_tables method and then set code_io_table.
#
#         Todo:
#             * Decide whether to make sector_names frozen
#         """
#         self._init_base_io_tables()
#         self.code_io_table: DataFrame = self._io_table_code_to_labels_func(
#             self.full_io_table, self.cpa_column_name
#         )
#         self.sector_names = self._CPA_sectors_to_names
#         # for attr in ['national_gva', 'national_net_subsidies']:
#         #     if isinstance(attr, str):
#         #         logger.info(f"Setting {attr} from `self.io_table[{attr}]`.")
#         #         setattr(self, f"_{attr}_str", getattr(self, attr))
#         #         setattr(self, attr, self.code_io_table[attr])
#
#     @property
#     def _CPA_sectors_to_names(self) -> Series:
#         """Series for mapping CPA codes to standard names."""
#         return self.row_codes[
#             self.row_codes.index.str.startswith(self.sector_prefix_str)
#         ]
#
#     @property
#     def _CPA_index(self) -> Index:
#         return self._CPA_sectors_to_names.index
#
#     @property
#     def row_codes(self) -> Series:  # Default skip first row
#         """Return the values in the index_column (intended to return sector codes)."""
#         if self.full_io_table is None:
#             raise self.NullIOTableError
#
#         return (
#             self.full_io_table.reset_index()
#             .set_index(self.cpa_column_name)
#             .iloc[:, 0][self._first_code_row :]
#         )
#
#     @property
#     def _aggregated_sectors_dict(self) -> AggregatedSectorDictType:
#         """Call aggregate_sector_dict on the _CPA_index property."""
#         if self.sector_aggregation_dict:
#             return aggregate_sector_dict(
#                 self._CPA_index, self.sector_aggregation_dict, self.sector_prefix_str
#             )
#         else:
#             raise self.NoSectorAggregationDictError
#
#     def get_aggregated_io_table(self) -> DataFrame:
#         """Return aggregated io_table"""
#         return aggregate_io_table(
#             self._aggregated_sectors_dict,
#             self.code_io_table,
#             self.dog_leg_columns,
#             self.dog_leg_rows,
#         )


def aggregate_sectors_by_dict_with_prefix(
    sectors: Iterable[str],
    sector_aggregation_dict: AggregatedSectorDictType = SECTOR_10_CODE_DICT,
    sector_code_prefix: str = CPA_COLUMN_NAME,
) -> AggregatedSectorDictType:
    """Generate a dictionary to aid aggregating sector data."""
    aggregated_sectors: AggregatedSectorDictType = {}
    for sector, code_letters in sector_aggregation_dict.items():
        sector_list: list[str] = []
        for letter in code_letters:
            sector_list += [
                sector_code
                for sector_code in sectors
                if sector_code.startswith(f"{sector_code_prefix}_{letter}")
            ]
        aggregated_sectors[sector] = sector_list
    return aggregated_sectors


class PyMRIOManager(IOSystem):

    """Wrapper for managing `pymrio` with InputOutputTable."""

    # io_host: InputOutputTable

    # region_index_name: str = REGION_COLUMN_NAME

    def __init__(
        self,
        io_host: InputOutputTable,
        region_index_name: str = REGION_COLUMN_NAME,
        # sector_index_name: str = SECTOR_COLUMN_NAME,
        # final_demand_index_label: str = FINAL_DEMAND_LABEL,
        **kwargs,
    ) -> None:
        """Set connection to io_host then init `IOSystem`."""
        self.io_host = io_host
        self.region_index_name = region_index_name
        # self.sector_index_name = sector_index_name
        # self.final_demand_index_label = final_demand_index_label
        super().__init__(Z=self._Z, Y=self._Y, **kwargs)

    @property
    def sector_index_name(self) -> str:
        return self.io_host.sector_index_name

    @property
    def table_index_names(self) -> tuple[str, str]:
        return (self.region_index_name, self.sector_index_name)

    @property
    def full_io_table(self) -> DataFrame:
        return self.io_host.full_io_table

    @property
    def core_io_table(self) -> DataFrame:
        return self.io_host.core_io_table

    @property
    def all_regions(self) -> Sequence[str]:
        return self.io_host.all_regions

    @property
    def all_sectors(self) -> Sequence[str]:
        return self.io_host.all_sectors

    @property
    def table_multi_index_names(self) -> tuple[str, str]:
        return (self.region_index_name, self.sector_index_name)

    @property
    def host_is_pymrio(self) -> bool:
        """Return whether `self` resembles a PyMRIO shape.

        Todo:
            * Determine if a type of `IOSystem` is better/safer.
        """
        if isinstance(self.full_io_table.index, MultiIndex):
            return self.full_io_table.index.names == self.table_index_names
        else:
            return False

    @property
    def _Z(self) -> DataFrame:
        if self.host_is_pymrio:
            return self.full_io_table.loc[self.io_index, self.io_index]
        else:
            return df_to_trimmed_multi_index(
                self.core_io_table, columns=self.io_index, index=self.io_index
            )

    @property
    def _Y(self) -> DataFrame:
        if self.host_is_pymrio:
            return self.full_io_table.loc[self.io_index, self.final_demand_index]
        else:
            return df_to_trimmed_multi_index(
                self.full_io_table,
                columns=self.final_demand_index,
                index=self.io_index,
            )

    @property
    def io_index(self) -> MultiIndex:
        """Return a MultiIndex from `all_regions` and `all_sectors`."""
        return gen_region_attr_multi_index(self.all_regions, self.all_sectors)

    def Z_by_region(self, region: str) -> Index:
        return self.Z.loc[region, region]

    def Y_by_region(self, region: str) -> Index:
        return self.Y.loc[region, region]

    @property
    def final_demand_column_names(self) -> Sequence[str]:
        return self.io_host.final_demand_column_names

    @property
    def final_demand_label(self) -> tuple[str, str]:
        """Return pyrmio `final_demand` labels."""
        return (self.region_index_name, self.final_demand_index_label)

    @property
    def final_demand_index(self) -> MultiIndex:
        """Return a MultiIndex from `all_regions` and `final_demand` columns."""
        return gen_region_attr_multi_index(
            self.all_regions, self.final_demand_column_names, self.final_demand_label
        )

    @property
    def final_demand_index_label(self) -> str:
        return self.io_host.final_demand_index_label


@dataclass(kw_only=True)
class InputOutputTable(ModelDataSourcesHandler):

    """Manage processing and aggregating Input Output Tables.

    Todo:
        * Replace _table_load_func with sources MetaData.read etc.
        * Consider means of warning if path and meta_data.path are not equal
    """

    # path: Optional[FilePathType] = None
    raw_io_table: MetaFileOrDataFrameType | IOSystem
    # meta_data: Optional[MetaData] = None
    # full_io_table: Optional[DataFrame] = None
    # base_io_table: Optional[DataFrame] = None
    io_scaling_factor: float = 1.0
    sector_names: list[SectorName] = field(default_factory=list)
    all_regions: Sequence[str] | str = field(default_factory=lambda: [])

    all_sectors: Sequence[str] | str = field(default_factory=lambda: [])
    all_sector_labels: Sequence[str] | str = field(default_factory=lambda: [])

    # raw_sector_codes_row: str | None = None
    # raw_sector_labels_row: str | None = None
    # all_sector_labels: list[str] | str = field(default_factory=list)
    # all_sector_codes: list[str] | str = field(default_factory=list)
    sector_aggregation_dict: Optional[AggregatedSectorDictType] = None
    sector_prefix_str: str = ""
    # io_table_kwargs: dict[str, Any] = field(default_factory=dict)
    date: DateType | YearType | None = None

    all_output_columns: Sequence[str] | str = field(default_factory=lambda: [])
    all_output_column_labels: Sequence[str] | str = field(default_factory=lambda: [])
    all_input_rows: Sequence[str] | str = field(default_factory=lambda: [])
    all_input_row_labels: Sequence[str] | str = field(default_factory=lambda: [])

    dog_leg_columns: DogLegType = field(default_factory=lambda: DEFAULT_DOG_LEG_COLUMNS)
    dog_leg_rows: DogLegType = field(default_factory=lambda: DEFAULT_DOG_LEG_ROWS)
    final_demand_column_names: Sequence[str] = field(
        default_factory=lambda: FINAL_DEMAND_COLUMN_NAMES
    )
    gross_value_added_row_name: str | None = GROSS_VALUE_ADDED_ROW_NAME
    intertermediate_demand_base_price_row_name: str | None = (
        INTERMEDIATE_DEMAND_BASE_PRICE_ROW_NAME
    )
    intermediate_demand_row_name: str | None = INTERMEDIATE_DEMAND_PRICE_ROW_NAME
    final_demand_index_label: str = FINAL_DEMAND_LABEL
    sector_index_name: str = SECTOR_COLUMN_NAME
    sector_codes_skip: Sequence[str] = field(default_factory=list)
    # process_base_io_table_func: Callable[..., DataFrame] | None = None
    # process_base_io_table_kwargs: dict[str, Any] = field(default_factory=dict)
    full_io_table_func: Callable[..., DataFrame] | None = None
    full_io_table_kwargs: dict[str, Any] = field(default_factory=dict)
    # _io_table_attr_names  = Literal['raw', 'base', 'full', 'aggregate']
    _default_meta_data_name: str = DEFAULT_INPUT_OUTPUT_META_DATA_NAME
    # _process_full_io_table: Callable[..., DataFrame] = crop_io_table_to_sectors
    _table_load_func: Callable[..., DataFrame] = pandas_from_path_or_package
    # _calc_full_io_table: Callable[..., DataFrame] = calc_full_io_table
    _aggregate_sectors_func: Callable[..., AggregatedSectorDictType] | None = None
    _aggregate_sectors_kwargs: dict[str, Any] = field(default_factory=dict)
    _aggregate_io_table_func: Callable[..., DataFrame | InputOutputTable] | None = None
    _aggregate_io_table_kwargs: dict[str, Any] = field(default_factory=dict)

    class NullIOTableError(Exception):
        ...

    class SectorNotInIOTable(Exception):
        ...

    class NoSectorAggregationDictError(Exception):
        ...

    class FullIOTableNotSet(Exception):
        ...

    class BaseIOTableNotSet(Exception):
        ...

    class MissingRowOrColumn(Exception):
        ...

    class MissingRowOrColumnName(Exception):
        ...

    class IOTableIndexingError(Exception):
        ...

    # @property
    # def full_io_table_scaled(self) -> DataFrame:
    #     assert isinstance(self.full_io_table, DataFrame)
    #     return self.full_io_table * self.io_scaling_factor

    # @property
    # def base_io_table_scaled(self) -> DataFrame:
    #     assert isinstance(self.base_io_table, DataFrame)
    #     return self.base_io_table * self.io_scaling_factor

    def __repr__(self) -> str:
        """Return a str indicated class type and number of sectors.

        Todo:
            * Apply __repr__ format coherence across classes
        """
        repr: str = f"{self.__class__.__name__}("
        if self.date:
            if isinstance(self.date, int):
                repr += f"year={self.date}, "
            else:
                repr += f"date={self.date}, "
        return repr + f"sectors_count={self.all_sectors_count})"

    @property
    def sectors(self) -> Sequence[SectorName]:
        """If sector_names is None, populate with sector_aggregation_dict keys, else error.

        Todo:
            * This may need a further refactor
            * Assume simpler! and possibly remote sector_names now that the
              index is managed in CPA
        """
        if self.all_sector_labels:
            if isinstance(self.all_sector_labels, str):
                logger.warning(
                    f"{self} has `all_sector_labels` set to {self.all_sector_labels}. "
                    "Check if that is a column or index name."
                )
                return [self.all_sector_labels]
            else:
                return self.all_sector_labels
        if self.sector_names is not None:
            return self.sector_names
        # elif (
        #     self.sector_aggregation_dict
        # ):  # Default to sector_aggregation_dict keys if sector_names not set
        #     logger.debug("Returning {self} sector_aggregation_dict keys")
        #     return list(self.sector_aggregation_dict.keys())
        raise ValueError("Neither {self} sector_names nor sector_aggregation_dict set.")

    @property
    def all_sectors_count(self) -> int:
        """Return the number of sectors."""
        return len(self.all_sectors)

    def set_all_regions(self) -> None:
        """Check and set `all_regions` attribute.

        Todo:
            * Consider cases where not IOStystem but still multi-region
        """
        if not self.all_regions:
            logger.warning("`all_regions` not set.")
            if isinstance(self.full_io_table, IOSystem):
                self.all_regions = tuple(self.full_io_table.get_regions())
            else:
                logger.warning(f"Setting `all_regions` to default: {DEFAULT_REGION}")
                self.all_regions = (DEFAULT_REGION,)
        elif isinstance(self.all_regions, str):
            logger.info(
                f"{self.all_regions} is not iterable, assuming is a column name"
            )
            assert self.all_regions in self.full_io_table.columns
            logger.info(
                f"Setting `self._all_regions_column_name` to {self.all_regions}"
            )
            self._all_regions_column_name = self.all_regions
            logger.info(
                f"Setting `self.all_regions` to `self._all_regions_column_name` in `self.full_io_table`"
            )
            self.all_regions = self.full_io_table[self._all_regions_column_name]
        try:
            assert (
                isinstance(self.all_regions, Sequence | Series | Index)
                and len(self.all_regions) >= 1
            )
        except AssertionError:
            raise ValueError(
                f"Not valid `all_regions` set on {self}: {self.all_regions}"
            )

    def set_all_output_columns(self) -> None:
        """Check and set `self.all_output_columns` attribute."""
        if not self.all_output_columns:
            logger.warning("`all_output_columns` not set.")
            if isinstance(self.full_io_table, IOSystem):
                logger.warning(
                    f"Using untested get_sectors() and get_Y_categories() methods"
                )
                raise NotImplementedError(
                    f"Processing from pymrio not fully implemented yet."
                )
                # self.all_output_columns = self.full_io_table.get_sectors() + self.full_io_table.get_Y_categories()
            else:
                logger.warning("Assuming default columns fit output.")
                self.all_output_columns = self.full_io_table.columns
        elif isinstance(self.all_output_columns, str):
            logger.info(
                f"{self.all_output_columns} is not iterable, assuming is an index name"
            )
            self._all_output_columns_row: str = self.all_output_columns
            self.all_output_columns = self.full_io_table.loc[
                self._all_output_columns_row
            ]
        else:
            raise self.IOTableIndexingError(
                f"{self.full_io_table} not available in `full_io_table` index."
            )
        try:
            assert (
                isinstance(self.all_output_columns, Sequence | Series | Index)
                and len(self.all_output_columns) >= 1
            )
        except AssertionError:
            raise ValueError(
                f"Not valid `all_output_columns` set on {self}: {self.all_output_columns}"
            )

    def set_all_input_rows(self) -> None:
        """Check and set `self.all_input_columns` attribute."""
        if not self.all_input_rows:
            logger.warning("`all_input_columns` not set.")
            if isinstance(self.full_io_table, IOSystem):
                logger.warning(
                    f"Using untested get_sectors() and get_Y_categories() methods"
                )
                raise NotImplementedError(
                    f"Processing from pymrio not fully implemented yet."
                )
                # self.all_input_columns = self.full_io_table.get_sectors() + self.full_io_table.get_Y_categories()
            else:
                logger.warning("Assuming default columns fit output.")
                self.all_input_rows = self.full_io_table.index
        elif isinstance(self.all_input_rows, str):
            logger.info(
                f"{self.all_input_rows} is not iterable, assuming is an index name"
            )
            self._all_input_rows_column: str = self.all_input_rows
            self.all_output_columns = self.full_io_table.loc[
                :, self._all_input_rows_column
            ]
        else:
            raise self.IOTableIndexingError(
                f"{self.full_io_table} not available in `full_io_table` index."
            )
        try:
            assert (
                isinstance(self.all_input_rows, Sequence | Series | Index)
                and len(self.all_input_rows) >= 1
            )
        except AssertionError:
            raise ValueError(
                f"Not valid `all_input_rows` set on {self}: {self.all_input_rows}"
            )

    def set_all_sectors(self) -> None:
        """Check and set `all_sectors` attribute."""
        if not self.all_sectors:
            logger.warning("`all_sectors` not set.")
            if isinstance(self.full_io_table, IOSystem):
                self.all_sectors = self.full_io_table.get_sectors()
            else:
                logger.warning(f"Calling `match_df_cols_rows` to guess sectors.")
                self.all_sectors = match_df_cols_rows(
                    self.full_io_table, skip=self.sector_codes_skip
                )
                logger.info(f"Set {self} `all_sectors` to {self.all_sectors}")
        elif isinstance(self.all_sectors, str):
            logger.info(
                f"{self.all_sectors} is not iterable, assuming is a column name"
            )
            if self.all_sectors in self.full_io_table.columns:
                logger.info(
                    f"Setting `self._all_sectors_column_name` to {self.all_sectors}"
                )
                self._all_sectors_column_name = self.all_sectors
                logger.info(
                    f"Setting `self.all_sectors` to `self._all_sectors_column_name` in `self.full_io_table`"
                )
                self.all_sectors = match_df_cols_rows(
                    self.full_io_table[self._all_sectors_column_name]
                )
            elif self.all_sectors in self.full_io_table.index:
                logger.info(
                    f"Setting `self._all_sectors_row_name` to {self.all_sectors}"
                )
                self._all_sectors_row_name = self.all_sectors
                logger.info(
                    f"Setting `self.all_sectors` to `self._all_sectors_row_name` in `self.full_io_table`"
                )
                self.all_sectors = match_df_cols_rows(
                    self.full_io_table.loc[self._all_sectors_row_name]
                )
            else:
                raise self.SectorNotInIOTable(
                    f"{self.all_sectors} not available in full_io_table index or columns."
                )
        try:
            assert (
                isinstance(self.all_sectors, Sequence | Series | Index)
                and len(self.all_sectors) >= 1
            )
        except AssertionError:
            raise ValueError(
                f"Not valid `all_sectors` set on {self}: {self.all_sectors}"
            )

    def set_pymrio(self) -> None:
        self.pymrio_table = PyMRIOManager(io_host=self)
        if isinstance(self.full_io_table, IOSystem):
            assert self.all_regions == self.pymrio_table.get_regions()
            assert self.all_sectors == self.pymrio_table.get_sectors()

    def __post_init__(self) -> None:
        # if self.all_sectors_in_raw_columns and not
        # self._init_base_io_tables()
        # self._gen_methods_and_check_io_table('raw_io_table')
        # if callable(self.calc_full_table):
        self._set_all_meta_file_or_data_fields()
        # if not isinstance(self.raw_io_table, DataFrame | IOSystem):
        #     # Todo: assess cases where this isn't covered in _set_all_meta_file_or_data_fields
        #     self._set_meta_file_or_data_field("raw_io_table")
        assert isinstance(self.raw_io_table, DataFrame | IOSystem)

        if callable(self.full_io_table_func):
            (
                self.full_io_table,
                self.all_input_row_labels,
                self.all_output_column_labels,
            ) = self.full_io_table_func(self.raw_io_table, **self.full_io_table_kwargs)
        else:
            self.full_io_table = self.raw_io_table
        for attr, meta_attr in {
            "all_output_column_labels": IO_TABLE_ALL_INPUT_COLUMN_ATTR,
            "all_input_row_labels": IO_TABLE_ALL_INPUT_ROW_ATTR,
        }.items():
            if hasattr(self, meta_attr):
                setattr(self, attr, getattr(self, meta_attr))
        self.set_all_regions()
        self.set_all_input_rows()
        self.set_all_output_columns()
        self.set_all_sectors()
        self.unscaled_full_io_table = self.full_io_table
        self.full_io_table = self.unscaled_full_io_table * self.io_scaling_factor
        self.set_pymrio()

    @property
    def core_io_table(self) -> DataFrame:
        """Return just Input-Output tables without dog legs for all sectors."""
        df: DataFrame = self.full_io_table.loc[self.all_sectors, self.all_sectors]
        df.columns.name = self.sector_index_name
        return df

    @property
    def core_final_demand(self) -> DataFrame:
        "Return Final Demand from `full_io_table` based on sepcified `final_demand_column_names`."
        df: DataFrame = self.full_io_table.loc[
            self.all_sectors, self.final_demand_column_names
        ]
        df.columns.name = self.final_demand_index_label
        return df

    @property
    def base_io_table(self) -> DataFrame:
        """Return Input-Output tables without dog legs for `sector_codes`.

        Todo:
            * Refactor to replace this with `core_io_table`.
        """
        return self.core_io_table

    @property
    def _aggregated_sectors_dict(self) -> AggregatedSectorDictType:
        """Call aggregate_sector_dict on the sectors property."""
        assert self._aggregate_sectors_func
        if self.sector_aggregation_dict:
            return self._aggregate_sectors_func(
                self.all_sectors,
                self.sector_aggregation_dict,
                **self._aggregate_sectors_kwargs,
            )
        else:
            raise self.NoSectorAggregationDictError

    def get_aggregated_io_table(self) -> DataFrame | InputOutputTable:
        """Return aggregated io_table"""
        assert self._aggregate_io_table_func
        return self._aggregate_io_table_func(
            self._aggregated_sectors_dict,
            self.full_io_table,
            self.dog_leg_columns,
            self.dog_leg_rows,
            **self._aggregate_io_table_kwargs,
        )

    @property
    def technical_coefficients(self) -> DataFrame:
        """Return the technical coefficients derived from self.io_table."""
        return technical_coefficients(
            self.base_io_table, self.all_sectors, self.intermediate_demand_base
        )

    @property
    def national_gross_value_added(self) -> Series:
        if not isinstance(self.full_io_table, DataFrame):
            raise self.FullIOTableNotSet(
                f"Gross Value Added not available without `full_io_table`."
            )
        return self.full_io_table.loc[self.gross_value_added_row_name]

    # def calc_full_table(self, force_replace_full_io_table: bool = False, **kwargs) -> DataFrame:
    #     """If `full_io_table` not set, infer from self.base_io_table."""
    #     raise NotImplementedError(f"`calc_full_table` not yet implemented for {self}.")
    # if not isinstance(self.base_io_table, DataFrame):
    #     raise self.BaseIOTableNotSet(f"Cannot calculate `full_io_table` if `base_io_table` not set on {self}")
    # elif not self.full_io_table or force_replace_full_io_table:
    #     if self.full_io_table:
    #         logger.warning(f"Overwriting `full_io_table` for {self}")
    #     logger.info(f"Caculating `full_io_table` from `base_io_table` for {self}")
    #     self.full_io_table = self._calc_full_io_table(self, **kwargs)
    #     return self.full_io_table

    def _get_or_raise_row(
        self,
        name: str | None,
        row_or_col: Literal["row", "col"] = "row",
        table_attr_name: str = "full_io_table",
    ) -> Series:
        """Return row from `table_attr_name` on self, else raise error."""
        table: DataFrame = getattr(self, table_attr_name)
        # if not name or (name and not hasattr(self, name)):
        #     raise self.MissingRowOrColumnName(f"{name} is `None` on {self}.")
        if not isinstance(table, DataFrame):
            calling_method: str = inspect.stack()[1][3]
            raise self.FullIOTableNotSet(
                f"`{calling_method}` not available without " f"`{table_attr_name}`."
            )
        if row_or_col == "row":
            if name in table.index:
                return table.loc[name]
            else:
                raise self.MissingRowOrColumn(
                    f"Row `{name}` not in `self.{table_attr_name}`."
                )
        else:
            if name in table.colums:
                return table[name]
            else:
                raise self.MissingRowOrColumn(
                    f"Column `{name}` not in `self.{table_attr_name}`."
                )

    @property
    def intermediate_demand(self) -> Series:
        return self._get_or_raise_row(self.intermediate_demand_row_name)
        # if not isinstance(self.full_io_table, DataFrame):
        #     raise self.FullIOTableNotSet(f"Intermediate Demand not available without `full_io_table`.")
        # else:
        #     if self.intermediate_demand_row_name in self.full_io_table.index:
        #         return self.full_io_table.loc[self.intermediate_demand_row_name]
        #     else:
        #         raise self.MissingRowOrColumn(f"Row {self.intermediate_demand_row_name} not in `self.full_io_table`.")
        # self._get_or_raise_row()

    @property
    def intermediate_demand_base(self) -> Series:
        return self._get_or_raise_row(self.intertermediate_demand_base_price_row_name)
        if not isinstance(self.full_io_table, DataFrame):
            raise self.FullIOTableNotSet(
                f"Intermediate Base Demand not available without `full_io_table`."
            )
        else:
            return self.full_io_table.loc[
                self.intertermediate_demand_base_price_row_name
            ]

    @property
    def national_gross_domestic_product(self) -> Series:
        return self.national_gross_domestic_product + self.intermediate_demand

    # @property
    # def gdp(self) -> float:
    #     return


def aggregate_io_table(
    agg_sector_dict: AggregatedSectorDictType,  # UK_SECTOR_10_CODE_DICT would suit
    full_io_table: DataFrame | InputOutputTable,
    dog_leg_columns: DogLegType,
    dog_leg_rows: DogLegType,
) -> DataFrame | InputOutputTable:
    """Return an aggregated Input Output table via an aggregated mapping of sectors.

    Todo:
        * Consider returning an InputOutputTable type rather than a DataFrame
    """
    # Todo: decide whether this dict copy (shallow) is worth keeping
    if isinstance(full_io_table, InputOutputTable):
        raise NotImplementedError(
            f"Managing and returning an `InputOutputTable` not yet implemented."
        )
    final_dog_leg_column_names: list[str] = (
        list(dog_leg_columns.keys())
        if isinstance(dog_leg_columns, dict)
        else list(dog_leg_columns)
    )
    final_dog_leg_row_names: list[str] = (
        list(dog_leg_rows.keys())
        if isinstance(dog_leg_rows, dict)
        else list(dog_leg_rows)
    )
    aggregated_sector_io_table = DataFrame(
        columns=list(agg_sector_dict.keys()) + final_dog_leg_column_names,
        index=list(agg_sector_dict.keys()) + final_dog_leg_row_names,
    )

    for sector_column in agg_sector_dict:
        for sector_row in agg_sector_dict:
            sector_column_names: Sequence[str] = agg_sector_dict[sector_column]
            sector_row_names: Sequence[str] = agg_sector_dict[sector_row]
            aggregated_sector_io_table.loc[
                sector_column, sector_row
            ] = (  # Check column row order
                full_io_table.loc[sector_column_names, sector_row_names].sum().sum()
            )
            if isinstance(dog_leg_columns, dict):
                for dog_leg_column, source_column_name in dog_leg_columns.items():
                    aggregated_sector_io_table.loc[
                        sector_row, dog_leg_column
                    ] = full_io_table.loc[sector_row_names, source_column_name].sum()
            else:
                raise NotImplementedError(
                    "Not implemented means of managing `dog_leg_columns` "
                    f"as Sequence type {type(dog_leg_columns)}. Must be a dict."
                )
        if isinstance(dog_leg_rows, dict):
            for dog_leg_row, source_row_name in dog_leg_rows.items():
                aggregated_sector_io_table.loc[
                    dog_leg_row, sector_column
                ] = full_io_table.loc[source_row_name, sector_column_names].sum()
        else:
            raise NotImplementedError(
                "Not implemented means of managing `dog_leg_rows` "
                f"as Sequence type {type(dog_leg_rows)}. Must be a dict."
            )
    return aggregated_sector_io_table


# def arrange_cpa_io_table(
#     io_table: DataFrame,
#     cpa_row_name: str | None = None,
#     sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
#     imports_row_name: str = IMPORTS_ROW_NAME,
#     net_subsidies_row_name: str = NET_SUBSIDIES_ROW_NAME,
#     intermediate_row_name: str = INTERMEDIATE_ROW_NAME,
#     cpa_import_cpa_row_name: str = CPA_IMPORTS_COST_INSURANCE_FREIGHT_ROW_NAME,
#     cpa_taxes_net_subsidies_row_name: str = CPA_TAXES_NET_SUBSIDIES_ROW_NAME,
#     cpa_intermediate_at_purchase_price_row_name: str = CPA_TOTAL_INTERMEDIATE_AT_PURCHASERS_PRICE,
#     input_index_label: str = IO_TABLE_DEFAULT_INDEX_NAME,
#     output_column_label: str = IO_TABLE_DEFAULT_COLUMNS_NAME,
# ) -> DataFrame:
#     """Standardise CPA rows and columns.
#
#     Todo:
#         * Check if cpa parameters are too UK sepecific
#         * If possible more to estios/uk/intput_output_table.py
#         * See https://www.ons.gov.uk/economy/grossdomesticproductgdp/compendium/unitedkingdomnationalaccountsthebluebook/2022/pdf
#     """
#     io_table.loc[sector_desc_column_name][0] = cpa_row_name
#     io_table.loc[cpa_row_name][0] = cpa_row_name
#     io_table.columns = io_table.loc[sector_desc_column_name]
#     io_table.drop(sector_desc_column_name, inplace=True)
#
#     io_table.loc[cpa_import_cpa_row_name][cpa_row_name] = imports_row_name
#     io_table.loc[cpa_taxes_net_subsidies_row_name][
#         cpa_row_name
#     ] = net_subsidies_row_name
#     io_table.loc[cpa_intermediate_at_purchase_price_row_name][
#         cpa_row_name
#     ] = intermediate_row_name
#     io_table.index.name = input_index_label
#     io_table.columns.name = output_column_label
#     return io_table


def _pymrio_download_wrapper(
    url: str | None = None,  # Included to match DataSaveReadCallable
    local_path: MetaFileOrDataFrameType = DEFAULT_OECD_STORAGE_PATH,
    pymrio_download_func=download_oecd,
    metadata_file_name: Path = Path("metadata.json"),
    **kwargs
    # pymrio_parse_func=parse_oecd,
    # download_kwargs: dict | None = None,
    # parse_kwargs: dict | None =
) -> MRIOMetaData:
    glob_str: str = "*" + metadata_file_name.suffix
    if isinstance(local_path, MetaData):
        assert local_path.path is not None
        local_path = local_path.path
    assert isinstance(local_path, Path)
    local_path_names: Generator[str, None, None] = (
        p.name for p in local_path.glob(glob_str)
    )
    if str(metadata_file_name) in local_path_names:
        current: MRIOMetaData = MRIOMetaData(location=local_path, **kwargs)
        if "version" in kwargs:
            version = kwargs["version"]
            if version != current.version:
                raise ValueError(
                    f"Local save {current} version different "
                    f"from requested {version}. Consider a "
                    f"different path from {local_path}."
                )
        return current
    else:
        return pymrio_download_func(storage_folder=local_path, **kwargs)


OECD_INPUT_OUTPUT_TABLES: MetaData = MetaData(
    name="OECD Input-Output Tables",
    year=2021,
    dates=list(range(1995, 2018)),
    authors="Organisation for Economic Co-operation and Development (OECD)",
    region="UK",
    url="https://stats.oecd.org/Index.aspx?DataSetCode=IOTS_2021",
    unit="USD with PPP of time of download",
    cite_as="OECD (2021), OECD Inter-Country Input-Output Database, http://oe.cd/icio",
    description=(
        "The 2021 edition of OECD Inter-Country Input-Output (ICIO) Tables"
        "has 45 unique industries based on ISIC Revision 4. "
        "Tables are provided for 66 countries for the years 1995 to 2018. "
        "Data can be downloaded for free in the form of zipped .csv and "
        ".Rdata format. Users are kindly asked to read the information "
        "files beforehand. Any questions or comments should be sent to "
        "icio-tiva.contact@oecd.org mentioning ICIO in the title of the "
        'message. Please cite this database "OECD (2021), OECD Inter-Country '
        'Input-Output Database, http://oe.cd/icio".'
    ),
    license=OECDTermsAndConditions,
    auto_download=False,
    file_name_from_url=False,
    needs_scaling=False,
    path=DEFAULT_OECD_STORAGE_PATH,
    _package_data=True,
    _save_func=_pymrio_download_wrapper,  # type: ignore
    _save_kwargs=dict(pymrio_download_func=download_oecd, years=[2017]),
    _reader_func=parse_oecd,
    _reader_kwargs=dict(path=DEFAULT_OECD_STORAGE_PATH, year=2017),
)

# def _download_and_save_file(
#     url_or_path: str | Request,
#     local_path: Optional[FilePathType] = None,


@dataclass(kw_only=True)
class InputOutputTableOECD(InputOutputTable):

    """Add OECD query infrastructure via `pymrio`."""

    raw_io_table: MetaFileOrDataFrameType = field(
        default_factory=lambda: OECD_INPUT_OUTPUT_TABLES
    )
    # raw_io_table: MetaFileOrDataFrameType = OECD_INPUT_OUTPUT_TABLES
    # all_sector_labels: dict[str, str] | DataFrame = OECD_
    final_demand_column_names = OECD_FINAL_DEMAND_COLUMN_NAMES


# @dataclass(repr=False)
# @dataclass(kw_only=True)
@dataclass(kw_only=True, repr=False)
class InputOutputCPATable(InputOutputTable):

    """Manage processing and aggregating CPA format Input Output Tables.

    Note:
     * CPA stands for Classification of Products by Activity, see
       https://ec.europa.eu/eurostat/web/cpa/cpa-2008
     * Sector aggregation defaults to 10 see
       https://ec.europa.eu/eurostat/documents/1965803/1978839/NACEREV.2INTRODUCTORYGUIDELINESEN.pdf/f48c8a50-feb1-4227-8fe0-935b58a0a332

    Todo:
        * Quote better https://ec.europa.eu/ example
        * Compare UK vs EU CPA standard row/column names
    """

    # path: FilePathType = ons_IO_2017.EXCEL_FILE_NAME

    all_sectors: Sequence[str] | str = field(default_factory=lambda: [])
    # all_sector_labels: Sequence[str] | str = field(default_factory=list)
    all_sector_labels: Sequence[str] | str = "Product"
    cpa_column_name: str = CPA_COLUMN_NAME
    sector_prefix_str: str = CPA_COLUMN_NAME
    # full_io_table_func: Callable[...,  DataFrame] = arrange_cpa_io_table
    _aggregate_sectors_func: Callable[
        ..., AggregatedSectorDictType
    ] = aggregate_sectors_by_dict_with_prefix
    _aggregate_io_table_func: Callable[
        ..., DataFrame | InputOutputTable
    ] | None = aggregate_io_table
    # process_base_io_table_func = arrange_cpa_io_table
    # process_base_io_table_kwargs: dict = field(default_factory=dict(cpa_row_name=CPA_COLUMN_NAME,))
    # sector_aggregation_dict: Optional[AggregatedSectorDictType] = field(
    #     default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
    # )
    # io_table_kwargs: dict[str, Any] = field(
    #     default_factory=lambda: {"sheet_name": IO_TABLE_NAME}
    # )
    # dog_leg_columns: dict[str, str] = field(
    #     default_factory=lambda: UK_DOG_LEG_CODES["columns"]
    # )
    # dog_leg_rows: dict[str, str] = field(
    #     default_factory=lambda: UK_DOG_LEG_CODES["rows"]
    # )

    _first_code_row: int = 1  # from ons_IO_2017.FIRST_CODE_ROW, avoiding circular import until refactor concluded
    _io_table_code_to_labels_func: Callable[
        [DataFrame, str], DataFrame
    ] = cpa_io_table_to_codes
    # _table_load_func: Callable[..., DataFrame] = load_io_table_excel

    def __post_init__(self) -> None:
        """Call the core _init_base_io_tables method and then set code_io_table.

        Todo:
            * Decide whether to make sector_names frozen
        """
        self.full_io_table_kwargs["cpa_row_name"] = self.cpa_column_name
        # # self._init_base_io_tables()
        # if not self.process_base_io_table_func and self.__class__.process_base_io_table_func:
        #     logger.debug(f"Fixing missing attribute from {type(self)}")
        #     self.process_base_io_table_func = self.__class__.process_base_io_table_func
        # if not self.process_full_io_table_func and self.__class__.process_full_io_table_func:
        #     logger.debug(f"Fixing missing attribute from {type(self)}")
        #     self.process_full_io_table_func = self.__class__.process_full_io_table_func
        super().__post_init__()
        # self.full_io_table = arrange_cpa_io_table(self.full_io_table)
        # self.column_name_codes = self.full_io_table.iloc[:, :1]
        # self.row_name_codes = self.full_io_table.iloc[0]
        self.code_io_table: DataFrame = self._io_table_code_to_labels_func(
            self.full_io_table, self.cpa_column_name
        )
        self.sector_names = self._CPA_sectors_to_names

    @property
    def _CPA_sectors_to_names(self) -> Series:
        """Series for mapping CPA codes to standard names."""
        return self.row_codes[
            self.row_codes.index.str.startswith(self.sector_prefix_str)
        ]

    @property
    def base_io_table(self) -> DataFrame:
        return self.code_io_table[self.sector_codes].loc[self.sector_codes]

    @property
    def _CPA_index(self) -> Index:
        return self._CPA_sectors_to_names.index

    @property
    def row_codes(self) -> Series:  # Default skip first row
        """Return the values in the index_column (intended to return sector codes)."""
        if self.full_io_table is None:
            raise self.NullIOTableError
        else:
            return (
                self.full_io_table.reset_index()
                .set_index(self.cpa_column_name)
                .iloc[:, 0][self._first_code_row :]
            )

    @property
    def sector_codes(self) -> list[str]:
        return list(self._CPA_index)

    @property
    def intermediate_demand(self) -> Series:
        # return self._get_or_raise_row(self.intermediate_demand_row_name)
        if not isinstance(self.full_io_table, DataFrame):
            raise self.FullIOTableNotSet(
                f"Intermediate Demand not available without `full_io_table`."
            )
        else:
            if self.intermediate_demand_row_name in self.full_io_table.index:
                return self.full_io_table.loc[self.intermediate_demand_row_name]
            else:
                raise self.MissingRowOrColumn(
                    f"Row {self.intermediate_demand_row_name} not in `self.code_io_table`."
                )
        # self._get_or_raise_row()

    # @property
    # def intermediate_demand_base(self) -> Series:
    #     # return self._get_or_raise_row(self.intertermediate_demand_base_price_row_name)
    #     if not isinstance(self.code_io_table, DataFrame):
    #         raise self.FullIOTableNotSet(
    #             f"Intermediate Base Demand not available without `code_io_table`."
    #         )
    #     else:
    #         return self.code_io_table.loc[
    #             self.intertermediate_demand_base_price_row_name
    #         ]

    # @property
    # def _aggregated_sectors(self) -> AggregatedSectorDictType:
    #     """Call aggregate_sector_dict on the _CPA_index property."""
    #     if self.sector_aggregation_dict:
    #         return aggregate_sectors_by_dict_with_prefix(
    #             self._CPA_index, self.sector_aggregation_dict, self.sector_prefix_str
    #         )
    #     else:
    #         raise self.NoSectorAggregationDictError
    def _get_or_raise_row(
        self,
        name: str | None,
        row_or_col: Literal["row", "col"] = "row",
        table_attr_name: str = "code_io_table",
    ) -> Series:
        super()._get_or_raise_row(
            name=name, row_or_col=row_or_col, table_attr_name=table_attr_name
        )

    @property
    def technical_coefficients(self) -> DataFrame:
        """Return the technical coefficients derived from self.io_table."""
        return technical_coefficients(
            self.base_io_table, self.sector_codes, self.intermediate_demand_base
        )

    @property
    def national_gross_value_added(self) -> Series:
        if not isinstance(self.full_io_table, DataFrame):
            raise self.FullIOTableNotSet(
                f"Gross Value Added not available without `full_io_table`."
            )
        return self.full_io_table.loc[self.gross_value_added_row_name]

    @property
    def intermediate_demand_base(self) -> Series:
        return self._get_or_raise_row(self.intertermediate_demand_base_price_row_name)

    @property
    def _aggregated_sectors_dict(self) -> AggregatedSectorDictType:
        """Call aggregate_sector_dict on the sectors property.

        Note:
            * self.sectors is replaced with self._CPA_index
        """
        if self.sector_aggregation_dict:
            return self._aggregate_sectors_func(
                self._CPA_index, self.sector_aggregation_dict, self.sector_prefix_str
            )
        else:
            raise self.NoSectorAggregationDictError

    def get_aggregated_io_table(self) -> DataFrame | "InputOutputCPATable":
        """Return aggregated io_table"""
        assert self._aggregate_io_table_func
        return self._aggregate_io_table_func(
            self._aggregated_sectors_dict,
            self.code_io_table,
            self.dog_leg_columns,
            self.dog_leg_rows,
        )
