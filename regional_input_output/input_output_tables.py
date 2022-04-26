#!/usr/bin/env python
# -*- coding: utf-8 -*-

from copy import deepcopy
from dataclasses import dataclass, field
from importlib.resources import open_binary
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Optional, Union

from pandas import DataFrame, Series, read_csv, read_excel

from . import uk_data
from .uk_data import ons_IO_2017
from .uk_data.employment import (
    DATE_COLUMN_NAME,
    UK_JOBS_BY_SECTOR_PATH,
    UK_NATIONAL_EMPLOYMENT_SHEET,
)
from .utils import (
    CITY_COLUMN,
    OTHER_CITY_COLUMN,
    SECTOR_10_CODE_DICT,
    SECTOR_COLUMN,
    AggregatedSectorDictType,
    FilePathType,
    enforce_date_format,
)

logger = getLogger(__name__)

IO_TABLE_NAME: Final[str] = "IOT"
COEFFICIENT_TABLE_NAME: Final[str] = "A"

CPA_COLUMN_NAME: Final[str] = "CPA"
TOTAL_PRODUCTION_COLUMN_NAME: Final[str] = "Total Sales"
IMPORTS_COLUMN_NAME: Final[str] = "Imports"
FINAL_DEMAND_COLUMN_NAMES: Final[list[str]] = [
    "Household Purchase",
    "Government Purchase",
    "Non-profit Purchase",
]
UK_EXPORT_COLUMN_NAMES: Final[list[str]] = [
    "Exports to EU",
    "Exports outside EU",
    "Exports of services",
]
COVID_FLAGS_COLUMN: Final[str] = "COVID_FLAGS"

SECTOR_DESC_COLUMN_NAME: Final[str] = "Product"
NET_SUBSIDIES_COLUMN_NAME: Final[str] = "Net subsidies"
INTERMEDIATE_COLUMN_NAME: Final[str] = "Intermediate/final use w/purchaser's prices"

TOTAL_OUTPUT_COLUMN_NAME: Final[str] = "Total Purchase"

UK_DOG_LEG_CODES: Final[dict[str, dict[str, str]]] = {
    "columns": {
        "Household Purchase": "P3 S14",
        "Government Purchase": "P3 S13",
        "Non-profit Purchase": "P3 S15",
        "Exports to EU": "P61EU",
        "Exports outside EU": "P61RW",
        "Exports of services": "P62",
        TOTAL_OUTPUT_COLUMN_NAME: "TD",
    },
    "rows": {
        "Intermediate Demand": "_T",
        "Imports": "Imports",
        "Net Subsidies": "Net subsidies",
        "Intermediate Demand purchase price": "Intermediate/final use w/purchaser's prices",
        "Employee Compensation": "D1",
        "Gross Value Added": "GVA",
        "Total Sales": "P1",
    },
}

IO_TABLE_SCALING: Final[float] = 100000.0
CITY_SECTOR_ENGINE: Final[str] = "python"


@dataclass
class InputOutputTable:

    """Manage processing and aggregating Input Output Tables.

    Note:
     * CPA stands for Classification of Products by Activity, see
       https://ec.europa.eu/eurostat/web/cpa/cpa-2008
     * Sector aggregation defaults to 10 see
       https://ec.europa.eu/eurostat/documents/1965800/1978839/NACEREV.2INTRODUCTORYGUIDELINESEN.pdf/f48c8a50-feb1-4227-8fe0-935b58a0a332
    """

    path: Optional[FilePathType] = None
    full_io_table: Optional[DataFrame] = None
    cpa_column_name: str = CPA_COLUMN_NAME
    sector_prefix_str: str = CPA_COLUMN_NAME
    io_scaling_factor: float = IO_TABLE_SCALING
    sector_aggregation_dict: AggregatedSectorDictType = field(
        default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
    )
    _first_code_row: int = ons_IO_2017.FIRST_CODE_ROW
    _sector_prefix_str: str = CPA_COLUMN_NAME

    class NullIOTable(Exception):
        pass

    def __post_init__(self) -> None:
        if self.full_io_table is None:
            raise self.NullIOTable("full_io_table attribute not set.")
        self.code_io_table: DataFrame = io_table_to_codes(
            self.full_io_table, self.cpa_column_name
        )
        self.io_table_scaled: DataFrame = self.code_io_table * self.io_scaling_factor

    @property
    def row_codes(self) -> Series:  # Default skip first row
        """Return the values in the index_column (intended to return sector codes)."""
        if self.full_io_table is None:
            raise self.NullIOTable

        return (
            self.full_io_table.reset_index()
            .set_index(self.cpa_column_name)
            .iloc[:, 0][self._first_code_row :]
        )

    @property
    def sectors(self) -> Series:
        """Return all column names preceded by the sector_prefix_str code."""
        return self.row_codes[
            self.row_codes.index.str.startswith(self._sector_prefix_str)
        ]

    def _aggregated_sectors_dict(
        self,  # sector_aggregation_dict: AggregatedSectorDictType = UK_SECTOR_10_CODE_DICT
    ) -> AggregatedSectorDictType:
        """Call aggregate_sector_dict on the sectors property."""
        return aggregate_sector_dict(
            self.sectors.index, self.sector_aggregation_dict, self.sector_prefix_str
        )

    def get_aggregated_io_table(
        self,
        # sector_aggregation_dict: AggregatedSectorDictType = UK_SECTOR_10_CODE_DICT,
        dog_leg_columns: dict[str, str] = UK_DOG_LEG_CODES["columns"],
        dog_leg_rows: dict[str, str] = UK_DOG_LEG_CODES["rows"],
    ) -> DataFrame:
        """Return an aggregated Input Output table via an aggregated mapping of sectors."""
        # agg_sector_dict: AggregatedSectorDictType = self._aggregated_sectors_dict(
        #     sector_aggregation_dict
        # )
        agg_sector_dict: AggregatedSectorDictType = self._aggregated_sectors_dict()
        aggregated_sector_io_table = DataFrame(
            columns=list(agg_sector_dict.keys()) + list(dog_leg_columns.keys()),
            index=list(agg_sector_dict.keys()) + list(dog_leg_rows.keys()),
        )

        for sector_column in agg_sector_dict:
            for sector_row in agg_sector_dict:
                sector_column_names: list[str] = agg_sector_dict[sector_column]
                sector_row_names: list[str] = agg_sector_dict[sector_row]
                aggregated_sector_io_table.loc[
                    sector_column, sector_row
                ] = (  # Check column row order
                    self.code_io_table.loc[sector_column_names, sector_row_names]
                    .sum()
                    .sum()
                )
                for dog_leg_column, source_column_name in dog_leg_columns.items():
                    aggregated_sector_io_table.loc[
                        sector_row, dog_leg_column
                    ] = self.code_io_table.loc[
                        sector_row_names, source_column_name
                    ].sum()
            for dog_leg_row, source_row_name in dog_leg_rows.items():
                aggregated_sector_io_table.loc[
                    dog_leg_row, sector_column
                ] = self.code_io_table.loc[source_row_name, sector_column_names].sum()
        return aggregated_sector_io_table


def aggregate_sector_dict(
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


def io_table_to_codes(
    full_io_table_df: DataFrame, sector_code_column: str = CPA_COLUMN_NAME
) -> DataFrame:
    """Convert an Input Output DataFrame table (default ONS) to use coded column names."""
    io_table: DataFrame = full_io_table_df.set_index(sector_code_column)
    io_table.columns = io_table.loc[sector_code_column]
    return io_table.drop(sector_code_column)


def load_io_table_excel(
    path: FilePathType = ons_IO_2017.EXCEL_PATH,
    sheet_name: str = IO_TABLE_NAME,
    usecols: Optional[str] = ons_IO_2017.USECOLS,
    skiprows: Optional[list[int]] = ons_IO_2017.SKIPROWS,  # Default skips Rows 3 and 4
    index_col: Optional[int] = ons_IO_2017.INDEX_COL,
    header: Optional[Union[int, list[int]]] = ons_IO_2017.HEADER,
    cpa_column_name: str = CPA_COLUMN_NAME,
    sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
    imports_column_name: str = IMPORTS_COLUMN_NAME,
    net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
    intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
    **kwargs,
) -> DataFrame:
    """Import a Input-Ouput Table as a DataFrame from an ONS xlsx file."""
    if path is ons_IO_2017.EXCEL_PATH and isinstance(path, Path):
        path = open_binary(uk_data, path)
    io_table: DataFrame = read_excel(
        path,
        sheet_name=sheet_name,
        usecols=usecols,
        skiprows=skiprows,
        index_col=index_col,
        header=header,
        **kwargs,
    )
    io_table.loc[sector_desc_column_name][0] = cpa_column_name
    io_table.loc[cpa_column_name][0] = cpa_column_name
    io_table.columns = io_table.loc[sector_desc_column_name]
    io_table.drop(sector_desc_column_name, inplace=True)

    io_table.loc["Use of imported products, cif"][cpa_column_name] = imports_column_name
    io_table.loc["Taxes less subsidies on products"][
        cpa_column_name
    ] = net_subsidies_column_name
    io_table.loc["Total intermediate/final use at purchaser's prices"][
        cpa_column_name
    ] = intermediate_column_name
    return io_table


def load_employment_by_region_and_sector_csv(
    path: FilePathType = ons_IO_2017.CITY_SECTOR_EMPLOYMENT_PATH,
    skiprows: int = ons_IO_2017.CITY_SECTOR_SKIPROWS,
    skipfooter: int = ons_IO_2017.CITY_SECTOR_SKIPFOOTER,
    engine: str = CITY_SECTOR_ENGINE,
    usecols: Callable[[str], bool] = ons_IO_2017.CITY_SECTOR_USECOLS,
    index_col: int = ons_IO_2017.CITY_SECTOR_INDEX_COLUMN,
    **kwargs,
) -> DataFrame:
    """Import region level sector employment data as a DataFrame."""
    if path is ons_IO_2017.CITY_SECTOR_EMPLOYMENT_PATH and isinstance(path, Path):
        path = open_binary(uk_data, path)
    return read_csv(
        path,
        skiprows=skiprows,
        skipfooter=skipfooter,
        engine=engine,
        usecols=usecols,
        index_col=index_col,
        **kwargs,
    )


def load_region_employment_excel(
    sheet: str = UK_NATIONAL_EMPLOYMENT_SHEET,
    path: FilePathType = UK_JOBS_BY_SECTOR_PATH,
    date_column_name: str = DATE_COLUMN_NAME,
    covid_flags_column: str = COVID_FLAGS_COLUMN,
    **kwargs,
) -> DataFrame:
    """Load regional employment data from https://www.nomisweb.co.uk/ excel exports."""
    if path is UK_JOBS_BY_SECTOR_PATH and isinstance(path, Path):
        path = open_binary(uk_data, path)
    region: DataFrame = read_excel(
        path,
        sheet_name=sheet,
        skiprows=5,
        skipfooter=4,
        usecols=lambda x: "Unnamed" not in x,
        dtype={date_column_name: str},
        **kwargs,
    )
    region[covid_flags_column] = region[date_column_name].apply(
        lambda cell: cell.strip().endswith(")")
    )
    region.index = region[date_column_name].apply(enforce_date_format)
    return region.drop([date_column_name], axis=1)


@dataclass
class InputOutputExcelTable(InputOutputTable):

    path: FilePathType = ons_IO_2017.EXCEL_PATH
    io_table_kwargs: dict[str, Any] = field(
        default_factory=lambda: {"sheet_name": IO_TABLE_NAME}
    )
    _table_load_func: Callable[..., DataFrame] = load_io_table_excel

    def __post_init__(self) -> None:
        if self.full_io_table is None:
            self.full_io_table: DataFrame = self._table_load_func(
                self.path, **self.io_table_kwargs  # sheet_name=self.io_sheet_name,
            )
        super().__post_init__()
