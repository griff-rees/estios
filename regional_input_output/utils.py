#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger
from typing import Final, Iterable, Optional

# from networkx import DiGraph
from numpy import log
from pandas import DataFrame, MultiIndex, Series

from .uk_data.utils import (
    CENTRE_FOR_CITIES_PATH,
    CITIES_TOWNS_SHAPE_PATH,
    CITY_REGIONS,
    NATIONAL_COLUMN_NAME,
    SECTOR_10_CODE_DICT,
)

logger = getLogger(__name__)

CITY_COLUMN: Final[str] = "City"
OTHER_CITY_COLUMN: Final[str] = "Other_City"
SECTOR_COLUMN: Final[str] = "Sector"


def generate_i_m_index(
    i_column: Iterable[str] = CITY_REGIONS,
    m_column: Iterable[str] = SECTOR_10_CODE_DICT,
    include_national: bool = False,
    national_name: str = NATIONAL_COLUMN_NAME,
    i_column_name: str = CITY_COLUMN,
    m_column_name: str = SECTOR_COLUMN,
) -> MultiIndex:
    """Return an IM index, conditionally adding `national_name` as a region."""
    if include_national:
        i_column = list(i_column) + [national_name]
    index_tuples: list = [(i, m) for i in i_column for m in m_column]
    return MultiIndex.from_tuples(index_tuples, names=(i_column_name, m_column_name))


def generate_ij_index(
    regions: Iterable[str] = CITY_REGIONS,
    other_regions: Iterable[str] = CITY_REGIONS,
    m_column_name: str = OTHER_CITY_COLUMN,
    **kwargs,
) -> MultiIndex:
    """Wrappy around generate_i_m_index with other_regions instead of sectors."""
    return generate_i_m_index(
        regions, other_regions, m_column_name=m_column_name, **kwargs
    )


def generate_ij_m_index(
    regions: Iterable[str] = CITY_REGIONS,
    sectors: Iterable[str] = SECTOR_10_CODE_DICT,
    include_national: bool = False,
    national_name: str = NATIONAL_COLUMN_NAME,
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


# def y_ij_m_to_networkx(y_ij_m_results: Series,
#                        city_column: str = CITY_COLUMN) -> DiGraph:
#     flows: DiGraph()
#     flows.add_nodes_from(y_ij_m_to_networkx.index.get_level_values(city_column))
#     y_ij_m.apply(lambda row: flows.add_edge())
#     flows.add_edges([])
