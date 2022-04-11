#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from logging import getLogger
from os import PathLike
from typing import IO, Final, Iterable, Optional, Union

# from networkx import DiGraph
from numpy import log
from pandas import DataFrame, MultiIndex, Series

from .uk_data.employment import (
    CITY_SECTOR_REGION_PREFIX,
    UK_JOBS_BY_SECTOR_PATH,
    UK_NATIONAL_EMPLOYMENT_SHEET,
)
from .uk_data.regions import (
    CENTRE_FOR_CITIES_PATH,
    CITIES_TOWNS_SHAPE_PATH,
    UK_CITY_REGIONS,
    UK_NATIONAL_COLUMN_NAME,
)

logger = getLogger(__name__)

FilePathType = Union[str, IO, PathLike]
AggregatedSectorDictType = dict[str, list[str]]

CITY_COLUMN: Final[str] = "City"
OTHER_CITY_COLUMN: Final[str] = "Other_City"
SECTOR_COLUMN: Final[str] = "Sector"

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


def generate_i_m_index(
    i_column: Iterable[str] = UK_CITY_REGIONS,
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
    regions: Iterable[str] = UK_CITY_REGIONS,
    other_regions: Iterable[str] = UK_CITY_REGIONS,
    m_column_name: str = OTHER_CITY_COLUMN,
    **kwargs,
) -> MultiIndex:
    """Wrappy around generate_i_m_index with other_regions instead of sectors."""
    return generate_i_m_index(
        regions, other_regions, m_column_name=m_column_name, **kwargs
    )


def generate_ij_m_index(
    regions: Iterable[str] = UK_CITY_REGIONS,
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
) -> Series:
    return y_ij_m_results.query("City == @city & Sector == @sector").iloc[:, -1]


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


# def y_ij_m_to_networkx(y_ij_m_results: Series,
#                        city_column: str = CITY_COLUMN) -> DiGraph:
#     flows: DiGraph()
#     flows.add_nodes_from(y_ij_m_to_networkx.index.get_level_values(city_column))
#     y_ij_m.apply(lambda row: flows.add_edge())
#     flows.add_edges([])
