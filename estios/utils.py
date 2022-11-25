#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import Counter, OrderedDict
from datetime import date, datetime
from logging import getLogger
from typing import (
    Any,
    Final,
    Generator,
    Hashable,
    Iterable,
    Optional,
    Sequence,
    TypeAlias,
    Union,
)

# from networkx import DiGraph
from numpy import log
from pandas import DataFrame, MultiIndex, Series

# from .uk.employment import CITY_SECTOR_REGION_PREFIX

logger = getLogger(__name__)

RegionConfigType = Union[Sequence[str], dict[str, str], dict[str, Sequence[str]]]
SectorConfigType: TypeAlias = RegionConfigType
AggregatedSectorDictType = dict[str, Sequence[str]]
AnnualConfigType = Union[Sequence[int], dict[int, dict], OrderedDict[int, dict]]
InputOutputConfigType = dict[str, Any]
DateConfigType = Union[Sequence[date], dict[date, dict], OrderedDict[date, dict]]

logger.warning(f"`InputOutputConfigType` and `DateConfigType` refactor  needed")

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


def name_converter(names: Sequence[str], name_mapper: dict[str, str]) -> list[str]:
    """Return region names with any conversions specified in name_mapper"""
    return [name if not name in name_mapper else name_mapper[name] for name in names]


def invert_dict(d: dict) -> dict:
    """Attempt to have dict values point to keys assuming unique mapping."""
    logger.warning(f"Inverting a dict assuming uniqueness of keys and values")
    return {v: k for k, v in d.items()}


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
        y_ij_m_results.query(
            f"{city_column_name} == @city & {sector_column_name} == @sector"
        )
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
    region_type_prefix: str,
) -> DataFrame:
    """Filter a DataFrame with region indicies to specific regions."""
    df_filtered: DataFrame = df.loc[[region_type_prefix + place for place in regions]]
    return df_filtered.rename(lambda row: row.split(":")[1])


def aggregate_rows(
    pre_agg_data: DataFrame | Series,
    trim_column_names: bool = False,
    sector_dict: AggregatedSectorDictType = SECTOR_10_CODE_DICT,
) -> DataFrame | Series:
    """Aggregate DataFrame rows to reflect aggregated sectors."""

    if isinstance(pre_agg_data, DataFrame):
        if pre_agg_data.columns.to_list() == list(sector_dict.keys()):
            logger.warning(
                f"aggregate_rows called on DataFrame with columns already aggregated"
            )
            return pre_agg_data
    else:
        assert isinstance(pre_agg_data, Series)
        if pre_agg_data.index.to_list() == list(sector_dict.keys()):
            logger.warning(
                f"aggregate_rows called on Series with index equal to sector_dict"
            )
            return pre_agg_data

            # if
            # self._aggregated_national_employment: DataFrame = aggregate_rows(
            #     self.national_employment,
            #     sector_dict=self.sector_aggregation,
            # )
            # self.national_employment = (
            #     self._aggregated_national_employment.loc[str(self.employment_date)]
            #     * self.national_employment_scale
            # )
            # logger.warning(f"Aggregating national employment by {len(self.sector_aggregation)} groups")
    if trim_column_names and isinstance(pre_agg_data, DataFrame):
        pre_agg_data.rename(
            columns={column: column[0] for column in pre_agg_data.columns}, inplace=True
        )
    aggregated_data: Series | DataFrame = type(pre_agg_data)()
    for sector, letters in sector_dict.items():
        if len(letters) > 1 or isinstance(pre_agg_data, Series):
            if isinstance(pre_agg_data, DataFrame):
                aggregated_data[sector] = pre_agg_data[letters].sum(axis=1)
            else:
                aggregated_data[sector] = pre_agg_data[letters].sum()
        else:  # Prevent extra summming when aggregating DataFrames
            aggregated_data[sector] = pre_agg_data[letters]
    return aggregated_data


def trim_year_range_generator(
    years: Iterable[Union[str, int]], first_year: int, last_year: int
) -> Generator[int, None, None]:
    for year in years:
        if first_year <= int(year) <= last_year:
            yield int(year)


def iter_ints_to_list_strs(labels: Iterable[Union[str, int]]) -> list[str]:
    return [str(label) for label in labels]


def collect_dupes(sequence: Iterable) -> dict[Any, int]:
    return {key: count for key, count in Counter(sequence).items() if count > 1}


def str_keys_of_dict(dict_to_stringify) -> dict[str, Any]:
    return {str(key): val for key, val in dict_to_stringify.items()}


def iter_attr_by_key(
    iter_instance: Sequence,
    val_attr_name: str,
    key_attr_name: str = "date",
    iter_attr_name: str = "dates",
) -> Generator[tuple[date, Any], None, None]:
    """Wrappy to manage retuing Generator dict attributes over time series."""
    if not hasattr(iter_instance, iter_attr_name):
        raise AttributeError(f"{iter_instance} must have a {iter_attr_name} attribute.")
    try:
        for model in iter_instance:
            yield getattr(model, key_attr_name), getattr(model, val_attr_name)
    except AttributeError:
        raise AttributeError(
            f"Failure iterating over {key_attr_name} from {iter_instance} for {val_attr_name}"
        )


def tuples_to_ordered_dict(tuple_iter: Iterable[tuple[Hashable, Any]]) -> OrderedDict:
    return OrderedDict([(key, val) for key, val in tuple_iter])


def sum_by_rows_cols(
    df: DataFrame, rows: int | str, columns: str | list[str] | list[int]
) -> float:
    """Return sum of DataFrame df grouped by passed indexs and columns.

    Todo:
        * Check if the index parameter should be a str or should use iloc.
    """
    return df.loc[rows][columns].sum()


# def y_ij_m_to_networkx(y_ij_m_results: Series,
#                        city_column: str = CITY_COLUMN) -> DiGraph:
#     flows: DiGraph()
#     flows.add_nodes_from(y_ij_m_to_networkx.index.get_level_values(city_column))
#     y_ij_m.apply(lambda row: flows.add_edge())
#     flows.add_edges([])
