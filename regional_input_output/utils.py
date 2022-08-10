#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from logging import getLogger
from os import PathLike
from pathlib import Path
from pkgutil import get_data
from typing import IO, Final, Generator, Iterable, Optional, Union

# from networkx import DiGraph
from numpy import log
from pandas import DataFrame, MultiIndex, Series, read_csv

from .uk_data.employment import CITY_SECTOR_REGION_PREFIX

logger = getLogger(__name__)

FilePathType = Union[str, IO, PathLike]
FolderPathType = Union[str, PathLike]
AggregatedSectorDictType = dict[str, list[str]]
AnnualConfigType = Union[Iterable[int], dict[int, dict]]
DateConfigType = Union[Iterable[date], dict[date, dict]]

CITY_COLUMN: Final[str] = "City"
OTHER_CITY_COLUMN: Final[str] = "Other_City"
SECTOR_COLUMN: Final[str] = "Sector"

UK_NATIONAL_COLUMN_NAME: Final[str] = "UK"

FINAL_Y_IJ_M_COLUMN_NAME: Final[str] = "y_ij_m"

THREE_UK_CITY_REGIONS: Final[dict[str, str]] = {
    "Leeds": "Yorkshire and the Humber",
    "Liverpool": "North West",  # LIVERPOOL & BIRKENHEAD
    "Manchester": "North West",  # MANCHESTER & SALFORD
}

# high-level SNA/ISIC aggregation A*10/11
# See https://ec.europa.eu/eurostat/documents/1965800/1978839/NACEREV.2INTRODUCTORYGUIDELINESEN.pdf/f48c8a50-feb1-4227-8fe0-935b58a0a332

SECTOR_10_CODE_DICT: Final[AggregatedSectorDictType] = {
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

UK_DATA_PATH: Final[Path] = Path("uk_data/data")
DOI_URL_PREFIX: Final[str] = "https://doi.org/"


@dataclass
class MonthDay:
    month: int = 1
    day: int = 1

    def from_year(self, year: int) -> date:
        return date(year, self.month, self.day)


DEFAULT_ANNUAL_MONTH_DAY: Final[MonthDay] = MonthDay()


@dataclass
class MetaData:

    """Manage info on source material."""

    name: str
    year: int
    region: str
    authors: Optional[Union[str, list[str], dict[str, str]]] = None
    url: Optional[str] = None
    doi: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.url and self.doi:
            self.url = DOI_URL_PREFIX + self.doi

    def __str__(self) -> str:
        return f"Source: {self.name} for {self.region} {self.year}"


def name_converter(names: list, name_mapper: dict[str, str]) -> list[str]:
    """Return region names with any conversions specified in _region_name_mapper"""
    return [name if not name in name_mapper else name_mapper[name] for name in names]


def invert_dict(d: dict) -> dict:
    return {v: k for k, v in d.items()}


def read_package_data(
    file_name: FilePathType, folder: FolderPathType = UK_DATA_PATH
) -> BytesIO:
    if isinstance(file_name, IO):
        raise NotImplementedError(f"Currently no means of reading {file_name} types.")
    else:
        raw_data = get_data(__package__, str(Path(folder) / file_name))
        if raw_data is None:
            raise NotImplementedError(f"Processing {file_name} returned None.")
        else:
            return BytesIO(raw_data)


def path_or_package_data(
    path: FilePathType,
    default_file: FilePathType,
    folder: FolderPathType = UK_DATA_PATH,
) -> Union[FilePathType, BytesIO]:
    if path is default_file:
        logger.info(f"Loading from package data {default_file}.")
        return read_package_data(default_file, folder)
    else:
        return path


def pandas_from_path_or_package_csv(
    path: FilePathType,
    default_file: FilePathType,
    **kwargs,
) -> DataFrame:
    """Import a csv file as a DataFrame, managing if package_data used."""
    path = path_or_package_data(path, default_file)
    return read_csv(path, **kwargs)


def generate_i_m_index(
    i_column: Iterable[str] = THREE_UK_CITY_REGIONS,
    m_column: Iterable[str] = SECTOR_10_CODE_DICT,
    include_national: bool = False,
    national_name: str = UK_NATIONAL_COLUMN_NAME,
    i_column_name: str = CITY_COLUMN,
    m_column_name: str = SECTOR_COLUMN,
) -> MultiIndex:
    """Return an IM index, conditionally adding `national_name` as a region."""
    if include_national:
        i_column = list(i_column) + [national_name]
    index_tuples: list = [(i, m) for i in i_column for m in m_column]
    return MultiIndex.from_tuples(index_tuples, names=(i_column_name, m_column_name))


def generate_ij_index(
    regions: Iterable[str] = THREE_UK_CITY_REGIONS,
    other_regions: Iterable[str] = THREE_UK_CITY_REGIONS,
    m_column_name: str = OTHER_CITY_COLUMN,
    **kwargs,
) -> MultiIndex:
    """Wrappy around generate_i_m_index with other_regions instead of sectors."""
    return generate_i_m_index(
        regions, other_regions, m_column_name=m_column_name, **kwargs
    )


def generate_ij_m_index(
    regions: Iterable[str] = THREE_UK_CITY_REGIONS,
    sectors: Iterable[str] = SECTOR_10_CODE_DICT,
    include_national: bool = False,
    national_name: str = UK_NATIONAL_COLUMN_NAME,
    region_name: str = CITY_COLUMN,
    alter_prefix: str = "Other_",
) -> MultiIndex:
    """Return an IJM index, conditionally adding `national_name` as a region."""
    if include_national:
        regions = list(regions) + [national_name]
    index_tuples: list[tuple[str, str, str]] = [
        (i, j, m) for i in regions for j in regions for m in sectors if i != j
    ]
    return MultiIndex.from_tuples(
        index_tuples, names=(region_name, alter_prefix + region_name, SECTOR_COLUMN)
    )


def filter_y_ij_m_by_city_sector(
    y_ij_m_results: DataFrame,
    city: str,
    sector: str,
    city_column_name: str = CITY_COLUMN,
    sector_column_name: str = SECTOR_COLUMN,
    column_index: Union[str, int] = -1,  # Default is last column/iteration
    final_column_name: str = FINAL_Y_IJ_M_COLUMN_NAME,
) -> Series:
    return (
        y_ij_m_results.query("City == @city & Sector == @sector")
        .iloc[:, column_index]
        .rename(final_column_name)
    )


# def filter_by_city_sector(
#     data: Union[DataFrame, Series],
#     city: str,
#     sector: str,
#     city_column_name: str,
#     sector_column_name: str,
# ) -> Union[DataFrame, Series]:
#     return y_ij_m_results.query("City == @city & Sector == @sector")


def column_to_series(
    df: DataFrame,
    column: Union[str, int],
    new_series_name: Optional[str] = None,
) -> Series:
    """Return column from passed df as Series with an optional specified nme."""
    if isinstance(column, str):
        return df[column].rename(new_series_name)
    else:
        return df.iloc[column].rename(new_series_name)


def log_x_or_return_zero(x: float) -> Optional[float]:
    if x < 0:
        logger.error(f"Cannot log {x} < 0")
        return None
    return log(x) if x > 0 else 0.0


def enforce_start_str(string: str, prefix: str, on: bool) -> str:
    """Ensure a string's prefix characters of a string are there or removed."""
    if on:
        logger.debug(f"Ensuring {string} starts with {prefix}")
        return string if string.startswith(prefix) else prefix + string
    else:
        logger.debug(f"Ensuring {string} doesn't start with {prefix}")
        return string.removeprefix(prefix)


def enforce_end_str(string: str, suffix: str, on: bool) -> str:
    """Ensure a string's suffix characters are there or removed."""
    if on:
        logger.debug(f"Ensuring {string} ends with {suffix}")
        return string if string.endswith(suffix) else string + suffix
    else:
        logger.debug(f"Ensuring {string} doesn't end with {suffix}")
        return string.removesuffix(suffix)


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


def filter_by_region_name_and_type(
    df: DataFrame,
    regions: Iterable[str],
    region_type_prefix: str = CITY_SECTOR_REGION_PREFIX,
) -> DataFrame:
    """Filter a DataFrame with region indicies to specific regions."""
    df_filtered: DataFrame = df.loc[[region_type_prefix + place for place in regions]]
    return df_filtered.rename(lambda row: row.split(":")[1])


def aggregate_rows(
    full_df: DataFrame,
    trim_column_names: bool = False,
    sector_dict: AggregatedSectorDictType = SECTOR_10_CODE_DICT,
) -> DataFrame:
    """Aggregate DataFrame rows to reflect aggregated sectors."""
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


def trim_year_range_generator(
    years: Iterable[Union[str, int]], first_year: int, last_year: int
) -> Generator[int, None, None]:
    for year in years:
        if first_year <= int(year) <= last_year:
            yield int(year)


def iter_ints_to_list_strs(labels: Iterable[Union[str, int]]) -> list[str]:
    return [str(label) for label in labels]


# def y_ij_m_to_networkx(y_ij_m_results: Series,
#                        city_column: str = CITY_COLUMN) -> DiGraph:
#     flows: DiGraph()
#     flows.add_nodes_from(y_ij_m_to_networkx.index.get_level_values(city_column))
#     y_ij_m.apply(lambda row: flows.add_edge())
#     flows.add_edges([])
