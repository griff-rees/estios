#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass, field
from datetime import datetime
from importlib.resources import open_binary, open_text
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Optional, Union

from geopandas import GeoDataFrame, read_file
from pandas import DataFrame, Series, read_csv, read_excel

from . import data

# https://www.ons.gov.uk/economy/nationalaccounts/supplyandusetables/datasets/inputoutputsupplyandusetables

AggregatedSectorDict = dict[str, list[str]]

SECTOR_10_CODE_DICT: Final[AggregatedSectorDict] = {
    "Agriculture": ["A"],
    "Production": ["B", "C", "D", "E"],
    "Construction": ["F"],
    "Distribution, transport, hotels and restaurants": ["G", "H", "I"],
    "Information and communication": ["J"],
    "Financial and insurance": ["K"],
    "Real estate": ["L"],
    "Professional and support activities": ["M", "N"],
    "Government, health & education": ["O", "P", "Q"],
    "Other services": ["R", "S", "T"],
}

# Example Data files

CENTRE_FOR_CITIES_PATH: Final[PathLike] = Path("data-tool-export.csv")
CITIES_TOWNS_SHAPE_PATH: Final[PathLike] = Path("cities_towns.geojson")
CENTRE_FOR_CITIES_INDEX_COL: Final[str] = "City"
CENTRE_FOR_CITIES_NROWS: Final[int] = 63
CENTRE_FOR_CITIES_DROP_COL_NAME: Final[str] = "Unnamed: 708"
CENTRE_FOR_CITIES_NA_VALUES: Final[str] = " "

# Input-Ouput Table excel data file and configuration

IO_TABLE_2017_EXCEL_PATH: Final[PathLike] = Path("nasu1719pr.xlsx")
IO_TABLE_NAME: Final[str] = "IOT"
IO_TABLE_USECOLS: Final[str] = "A:DO"
IO_TABLE_SKIPROWS: Final[list[int]] = [0, 1, 2, 5]  # Skips Rows 3 and 4
IO_TABLE_INDEX_COL: Final[int] = 1  # Sets index to what was the 4th (now 2nd) row
IO_TABLE_HEADER: Final[Union[int, list[int], None]] = None

CPA_COLUMN_NAME: Final[str] = "CPA"
SECTOR_DESC_COLUMN_NAME: Final[str] = "Product"
IMPORTS_COLUMN_NAME: Final[str] = "Imports"
NET_SUBSIDIES_COLUMN_NAME: Final[str] = "Net subsidies"
INTERMEDIATE_COLUMN_NAME: Final[str] = "Intermediate/final use w/purchaser's prices"

COEFFICIENT_TABLE_NAME: Final[str] = "A"

TOTAL_OUTPUT_COLUMN: Final[str] = "Total Purchase"

IO_DOG_LEG_CODES: Final[dict[str, dict[str, str]]] = {
    "columns": {
        "Household Purchase": "P3 S14",
        "Government Purchase": "P3 S13",
        "Non-profit Purchase": "P3 S15",
        "Exports to EU": "P61EU",
        "Exports outside EU": "P61RW",
        "Exports of services": "P62",
        TOTAL_OUTPUT_COLUMN: "TD",
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


# ONS jobs data

JOBS_BY_SECTOR_PATH: Final[PathLike] = Path("jobs05sep2021.xls")
DATE_COLUMN_NAME: Final[str] = "SIC 2007 section"
COVID_FLAGS_COLUMN: Final[str] = "COVID_FLAGS"

# Census export Nomis city and sector employment data
CITY_SECTOR_EMPLOYMENT_PATH: Final[PathLike] = Path("2423324239.csv")
CITY_SECTOR_SKIPROWS: Final[int] = 7
CITY_SECTOR_SKIPFOOTER: Final[int] = 8
CITY_SECTOR_ENGINE: Final[str] = "python"
CITY_SECTOR_USECOLS: Final[Callable[[str], bool]] = lambda x: "Unnamed" not in x
CITY_SECTOR_INDEX_COL: Final[int] = 0

CITY_SECTOR_REGION_PREFIX: Final[str] = "towncity:"


def load_centre_for_cities_csv(
    path: PathLike = CENTRE_FOR_CITIES_PATH,
    index_col: Optional[str] = CENTRE_FOR_CITIES_INDEX_COL,
    nrows: Optional[int] = CENTRE_FOR_CITIES_NROWS,
    na_values: Optional[str] = CENTRE_FOR_CITIES_NA_VALUES,
    drop_col_name: Optional[str] = CENTRE_FOR_CITIES_DROP_COL_NAME,
    **kwargs,
) -> DataFrame:
    """Load a Centre for Cities data tool export csv file."""
    if path is CENTRE_FOR_CITIES_PATH:
        path = open_text(data, path)
    base_centre_for_cities_df: DataFrame = read_csv(
        path, index_col=index_col, nrows=nrows, na_values=na_values, **kwargs
    )
    if drop_col_name:
        return base_centre_for_cities_df.drop(drop_col_name, axis=1)
    else:
        return base_centre_for_cities_df


def load_centre_for_cities_gis(
    path: PathLike = CITIES_TOWNS_SHAPE_PATH, driver: str = "GeoJSON", **kwargs
) -> GeoDataFrame:
    """Load a Centre for Cities Spartial file (defualt GeoJSON)."""
    if path is CITIES_TOWNS_SHAPE_PATH:
        path = open_text(data, path)
    return read_file(path, driver=driver, **kwargs)


def load_uk_io_table(
    path: PathLike = IO_TABLE_2017_EXCEL_PATH,
    sheet_name: str = IO_TABLE_NAME,
    usecols: Optional[str] = IO_TABLE_USECOLS,
    skiprows: Optional[list[int]] = IO_TABLE_SKIPROWS,  # Default skips Rows 3 and 4
    index_col: Optional[int] = IO_TABLE_INDEX_COL,
    header: Optional[Union[int, list[int]]] = IO_TABLE_HEADER,
    cpa_column_name: str = CPA_COLUMN_NAME,
    sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
    imports_column_name: str = IMPORTS_COLUMN_NAME,
    net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
    intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
    **kwargs,
) -> DataFrame:
    """Import a Input-Ouput Table as a DataFrame from an ONS xlsx file."""
    if path is IO_TABLE_2017_EXCEL_PATH:
        path = open_binary(data, path)
    uk_io_table: DataFrame = read_excel(
        path,
        sheet_name=sheet_name,
        usecols=usecols,
        skiprows=skiprows,
        index_col=index_col,
        header=header,
        **kwargs,
    )
    uk_io_table.loc[sector_desc_column_name][0] = cpa_column_name
    uk_io_table.loc[cpa_column_name][0] = cpa_column_name
    uk_io_table.columns = uk_io_table.loc[sector_desc_column_name]
    uk_io_table.drop(sector_desc_column_name, inplace=True)

    uk_io_table.loc["Use of imported products, cif"][
        cpa_column_name
    ] = imports_column_name
    uk_io_table.loc["Taxes less subsidies on products"][
        cpa_column_name
    ] = net_subsidies_column_name
    uk_io_table.loc["Total intermediate/final use at purchaser's prices"][
        cpa_column_name
    ] = intermediate_column_name
    return uk_io_table


def load_employment_by_city_and_sector(
    path: PathLike = CITY_SECTOR_EMPLOYMENT_PATH,
    skiprows: int = CITY_SECTOR_SKIPROWS,
    skipfooter: int = CITY_SECTOR_SKIPFOOTER,
    engine: str = CITY_SECTOR_ENGINE,
    usecols: Callable[[str], bool] = CITY_SECTOR_USECOLS,
    index_col: int = CITY_SECTOR_INDEX_COL,
    **kwargs,
) -> DataFrame:
    if path is CITY_SECTOR_EMPLOYMENT_PATH:
        path = open_binary(data, path)
    return read_csv(
        path,
        skiprows=skiprows,
        skipfooter=skipfooter,
        engine=engine,
        usecols=usecols,
        index_col=index_col,
        **kwargs,
    )


def filter_by_region_name_and_type(
    df: DataFrame,
    regions: list[str],
    region_type_prefix: str = CITY_SECTOR_REGION_PREFIX,
) -> DataFrame:
    """Filter a DataFrame with region indicies to specific regions."""
    df_filtered: DataFrame = df.loc[[region_type_prefix + place for place in regions]]
    return df_filtered.rename(lambda row: row.split(":")[1])


def ons_io_table_to_codes(
    full_io_table_df: DataFrame, sector_code_column: str = CPA_COLUMN_NAME
) -> DataFrame:
    """Convert an ONS Input Output DataFrame table to use coded column names."""
    io_table: DataFrame = full_io_table_df.set_index(sector_code_column)
    io_table.columns = io_table.loc[sector_code_column]
    return io_table.drop(sector_code_column)


def aggregate_sector_dict(
    sectors: Iterable,
    sector_aggregation_dict: AggregatedSectorDict = SECTOR_10_CODE_DICT,
    sector_code_prefix: str = CPA_COLUMN_NAME,
) -> AggregatedSectorDict:
    aggregated_sectors: AggregatedSectorDict = {}
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


@dataclass
class ONSInputOutputTable:

    path: PathLike = IO_TABLE_2017_EXCEL_PATH
    io_sheet_name: str = IO_TABLE_NAME
    cpa_column_name: str = CPA_COLUMN_NAME
    sector_prefix_str: str = CPA_COLUMN_NAME
    io_table_kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.full_io_table: DataFrame = load_uk_io_table(
            self.path, sheet_name=self.io_sheet_name, **self.io_table_kwargs
        )
        self.code_io_table: DataFrame = ons_io_table_to_codes(
            self.full_io_table, self.cpa_column_name
        )

    @property
    def row_codes(self, first_code_row: int = 1) -> Series:  # Default skip first row
        """Return the values in the index_column (intended to return sector codes)."""
        return (
            self.full_io_table.reset_index()
            .set_index(self.cpa_column_name)
            .iloc[:, 0][first_code_row:]
        )

    @property
    def sectors(self, sector_prefix_str: str = CPA_COLUMN_NAME) -> Series:
        """Return all column names preceded by the sector_prefix_str code."""
        return self.row_codes[self.row_codes.index.str.startswith(sector_prefix_str)]

    def _aggregated_sectors_dict(
        self, sector_aggregation_dict: AggregatedSectorDict = SECTOR_10_CODE_DICT
    ) -> AggregatedSectorDict:
        """Call aggregate_sector_dict on the sectors property."""
        return aggregate_sector_dict(
            self.sectors.index, sector_aggregation_dict, self.sector_prefix_str
        )

    def get_aggregated_io_table(
        self,
        sector_aggregation_dict: AggregatedSectorDict = SECTOR_10_CODE_DICT,
        dog_leg_columns: list[str] = IO_DOG_LEG_CODES["columns"],
        dog_leg_rows: list[str] = IO_DOG_LEG_CODES["rows"],
    ) -> DataFrame:
        agg_sector_dict: AggregatedSectorDict = self._aggregated_sectors_dict(
            sector_aggregation_dict
        )
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


def enforce_date_format(cell: str) -> str:
    """Set convert date strings for consistent formatting."""
    if cell.endswith("00:00"):
        return cell.split()[0]
    else:
        cell = cell.strip()
        if cell.endswith(")"):
            # Remove flags of the form " (r)" or " (p)" and " 4 (p)"
            cell = " ".join(cell.split()[:2])
        return str(datetime.strptime(cell, "%b %y")).split()[0]


def load_region_employment(
    sheet: str,
    path: str = JOBS_BY_SECTOR_PATH,
    date_column_name: str = DATE_COLUMN_NAME,
    covid_flags_column: str = COVID_FLAGS_COLUMN,
    **kwargs,
) -> DataFrame:
    """Load regional employment data from https://www.nomisweb.co.uk/ excel exports."""
    if path is JOBS_BY_SECTOR_PATH:
        path = open_binary(data, path)
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


def aggregate_rows(
    full_df: DataFrame,
    trim_column_names: bool = False,
    sector_dict: AggregatedSectorDict = SECTOR_10_CODE_DICT,
) -> DataFrame:
    if trim_column_names:
        full_df.rename(
            columns={column: column[0] for column in full_df.columns}, inplace=True
        )
    aggregated_df = DataFrame()
    for sector, letters in sector_dict.items():
        if len(letters) > 1:
            aggregated_df[sector] = full_df[letters].sum(axis=1)
        else:
            aggregated_df[sector] = full_df[letters]
    return aggregated_df
