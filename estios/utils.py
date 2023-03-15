#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import Counter, OrderedDict
from dataclasses import Field, field, fields
from datetime import date, datetime
from functools import wraps
from itertools import zip_longest
from logging import getLogger
from operator import attrgetter
from typing import (
    Any,
    Callable,
    Collection,
    Final,
    Generator,
    Hashable,
    Iterable,
    Optional,
    Sequence,
    Type,
    TypeAlias,
    Union,
)

# from networkx import DiGraph
from numpy import log
from pandas import DataFrame, MultiIndex, Series

# from .uk.employment import CITY_SECTOR_REGION_PREFIX

logger = getLogger(__name__)

DateType: TypeAlias = date
YearType: TypeAlias = int
RegionName: TypeAlias = str
SectorName: TypeAlias = str

RegionNamesListType = Union[list[RegionName], Collection[RegionName]]
SectorNamesListType = Union[list[SectorName], Collection[SectorName]]

RegionConfigType = Union[
    Sequence[RegionName], dict[RegionName, str], dict[RegionName, Sequence[str]]
]
SectorConfigType = Union[
    Sequence[SectorName], dict[SectorName, str], dict[SectorName, Sequence[str]]
]
AggregatedSectorDictType = dict[str, Sequence[str]]
AnnualConfigType = Union[Sequence[int], dict[int, dict], OrderedDict[int, dict]]
InputOutputConfigType = OrderedDict[DateType, Any]
DateConfigType = Union[
    Sequence[date], dict[DateType, dict], OrderedDict[DateType, dict]
]

logger.warning(f"`InputOutputConfigType` and `DateConfigType` refactor needed")

REGION_COLUMN_NAME: Final[str] = "Region"
AREA_LABEL: Final[str] = "Area"

CITY_COLUMN: Final[str] = "City"
OTHER_CITY_COLUMN: Final[str] = "Other_City"
SECTOR_COLUMN_NAME: Final[str] = "Sector"

FINAL_Y_IJ_M_COLUMN_NAME: Final[str] = "y_ij_m"

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
    i_column: Iterable[str],
    m_column: Iterable[str],
    national_column_name: str | None = None,
    include_national: bool = False,
    i_column_name: str = CITY_COLUMN,
    m_column_name: str = SECTOR_COLUMN_NAME,
) -> MultiIndex:
    """Return an IM index, conditionally adding `national_column_name` as a region.

    Todo:
        * This should be refactored along with calc_region_distances.
    """
    if include_national:
        assert national_column_name
        i_column = list(i_column) + [national_column_name]
    index_tuples: list = [(i, m) for i in i_column for m in m_column]
    return MultiIndex.from_tuples(index_tuples, names=(i_column_name, m_column_name))


def generate_ij_index(
    regions: Iterable[str],
    other_regions: Iterable[str],
    m_column_name: str = OTHER_CITY_COLUMN,
    **kwargs,
) -> MultiIndex:
    """Wrappy around generate_i_m_index with other_regions instead of sectors."""
    return generate_i_m_index(
        regions, other_regions, m_column_name=m_column_name, **kwargs
    )


def generate_ij_m_index(
    regions: Iterable[str],
    sectors: Iterable[str],
    national_column_name: str,
    include_national: bool = False,
    region_name: str = CITY_COLUMN,
    alter_prefix: str = "Other_",
) -> MultiIndex:
    """Return an IJM index, conditionally adding `national_column_name` as a region."""
    if include_national:
        regions = list(regions) + [national_column_name]
    index_tuples: list[tuple[str, str, str]] = [
        (i, j, m) for i in regions for j in regions for m in sectors if i != j
    ]
    return MultiIndex.from_tuples(
        index_tuples,
        names=(region_name, alter_prefix + region_name, SECTOR_COLUMN_NAME),
    )


def filter_y_ij_m_by_city_sector(
    y_ij_m_results: DataFrame,
    city: str,
    sector: str,
    city_column_name: str = CITY_COLUMN,
    sector_column_name: str = SECTOR_COLUMN_NAME,
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
    return log(x) if log(x) > 0 else 0.0


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
) -> Generator[tuple[DateType, Any], None, None]:
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


def filled_or_empty_dict(indexable: dict, key: str) -> dict[str, str]:
    return indexable[key] if key in indexable else {}


def sum_by_rows_cols(
    df: DataFrame, rows: int | str, columns: str | list[str] | list[int]
) -> float:
    """Return sum of DataFrame df grouped by passed indexs and columns.

    Todo:
        * Check if the index parameter should be a str or should use iloc.
    """
    return df.loc[rows, columns].sum()


def ordered_iter_overlaps(
    iter_a: Iterable, iter_b: Iterable
) -> Generator[Any, None, None]:
    return (x for x, y in zip(iter_a, iter_b) if x == y)


def filter_df_by_strs_or_sequences(
    rows: str | Sequence[str], columns: str | Sequence[str], df: DataFrame
) -> Sequence:
    if isinstance(rows, str):
        rows = [rows]
    if isinstance(columns, str):
        columns = [columns]
    return df.loc[rows, columns]


def ensure_type(
    var: Any,
    types_to_ensure: Type,
    type_ensurer: Callable[[Any], Type],
) -> Any:
    """Wrap var in type_ensured if var is a str."""
    if not isinstance(var, types_to_ensure):
        return type_ensurer(var)
    else:
        return var


def ensure_series(
    var: str | Sequence[Any] | Series | DataFrame,
) -> Series:
    return ensure_type(var, Series, Series)


def wrap_as_series(
    *args: str | Sequence[str] | dict[str, Any],
):
    """Ensure any str vars passed to *args are wrapped in a Series."""

    def callable_wrapper(func: Callable):
        @wraps(func)
        def ensure_str_wrapped_in_list(*func_args, **kwargs) -> Series:
            args_list = list(func_args)
            for i, arg in enumerate(args):
                if arg in kwargs:
                    kwargs[arg] = ensure_series(kwargs[arg])  # type: ignore
                else:
                    args_list[i] = ensure_series(args[i])
            return func(*args_list, **kwargs)

        return ensure_str_wrapped_in_list

    return callable_wrapper


def ensure_dtype(series_or_df: Series | DataFrame, dtype: str) -> Series | DataFrame:
    return series_or_df.astype(dtype)


def dtype_wrapper(final_type: str):
    def callable_wrapper(func: Callable):
        @wraps(func)
        def ensure_dtype_wrapper(*args, **kwargs) -> Series | DataFrame:
            return func(*args, **kwargs).astype(final_type)

        return ensure_dtype_wrapper

    return callable_wrapper


def len_less_or_eq(sequence: Sequence, count: int = 1) -> bool:
    return len(sequence) <= count


def get_df_nth_row(df: DataFrame, index: int = 0) -> Series:
    return df.iloc[index]


def get_df_first_row(df: DataFrame) -> Series:
    return get_df_nth_row(df)


def conditional_type_wrapper(
    condition_func: Callable[[Any], bool],
    type_wrapper: Callable[[Any], Any],
):
    def callable_wrapper(func: Callable):
        @wraps(func)
        def ensure_type_wrapper(*args, **kwargs) -> Any:
            result = func(*args, **kwargs)
            if condition_func(result):
                return type_wrapper(result)
            else:
                return result

        return ensure_type_wrapper

    return callable_wrapper


# <<<<<<< Updated upstream
def df_column_to_single_value(
    df: DataFrame,
    results_column_name: str,
) -> float:
    """Apply query_str to df and extract the first elements"""
    return df[results_column_name].values[0]


def filter_fields_by_type(cls: Any, field_type: Type | TypeAlias) -> tuple[Field, ...]:
    """Return tuple of cls attributes of field_type."""
    return tuple(field for field in fields(cls) if field.type == field_type)


def filter_fields_by_types(
    cls: Any, field_types: tuple[Type | TypeAlias, ...]
) -> tuple[Field, ...]:
    """Return tuple of cls attributes of field_type."""
    return tuple(field for field in fields(cls) if field.type in field_types)


def filter_by_attr(items: Iterable, attr_name: str) -> tuple[Any, ...]:
    """Return tuple of itmes which have `attr_name` attribute."""
    return tuple(item for item in items if hasattr(item, attr_name))


def filter_by_list_of_attrs(
    items: Iterable, attr_names: Sequence[str]
) -> tuple[Any, ...]:
    """Return tuple of itmes which have `attr_name` attribute."""
    return tuple(
        item for item in items if all(hasattr(item, attr) for attr in attr_names)
    )


def field_names(field_sequence: Sequence[Field]) -> tuple[str, ...]:
    """Return the names of passed sequence of Field objects."""
    return tuple(field.name for field in field_sequence)


# def filter_attrs_by_prefix(cls: Any, prefix: str) -> dict[str, Any]:
#     return {name: value for name, value in vars(cls).items() if name.startswith(prefix)}


def filter_attrs_by_substring(
    cls: Any, substring: str
) -> Generator[tuple[str, Any], None, None]:
    for attr_name, attr in vars(cls).items():
        if substring in attr_name:
            yield attr_name, attr


def match_ordered_iters(x: Iterable, y: Iterable, skip: Sequence = []) -> tuple:
    """From two iterables return a tuple of order marched values."""
    return tuple(
        x_value
        for x_value, y_value in zip_longest(x, y)
        if x_value == y_value and x_value not in skip and y_value not in skip
    )


def match_df_cols_rows(df: DataFrame, skip: Sequence = []) -> tuple[str, ...]:
    """From a DataFarme, return a tuple of marched column and row names."""
    return match_ordered_iters(df.columns, df.index, skip)


def gen_region_attr_multi_index(
    regions: Sequence[str],
    attrs: Sequence[str],
    names: tuple[str, str] = (REGION_COLUMN_NAME, SECTOR_COLUMN_NAME),
) -> MultiIndex:
    """Generated a nested MultiIndex of regions and attrs, including index naming."""
    return MultiIndex.from_product([regions, attrs], names=names)


def df_to_trimmed_multi_index(
    df: DataFrame,
    columns: MultiIndex,
    index: MultiIndex,
    multi_index_level: int = 1,
) -> DataFrame:
    trimmed_df: DataFrame = df.loc[
        index.get_level_values(multi_index_level),
        columns.get_level_values(multi_index_level),
    ]
    trimmed_df.columns = columns
    return trimmed_df.set_index(index)


# =======


def series_dict_to_multi_index(
    nested_series: dict[str | int, Series],
    column_names: tuple[str, str] = (REGION_COLUMN_NAME, SECTOR_COLUMN_NAME),
    # sector_row_names: Sequence[str],
    unstack: bool = True,
) -> Series | DataFrame:
    """Generate a Series or DataFrame label."""
    series: DataFrame | Series
    if unstack:
        series = DataFrame.from_dict(nested_series)
        series = series.T
        series.index.name = column_names[0]
        series.columns.name = column_names[1]
    else:
        series = DataFrame.from_dict(
            {
                (group, row[0]): row[1:]
                for group, series in nested_series.items()
                for row in series.items()
            }
        )
        series = series.iloc[0]
        assert isinstance(series, Series)
        series.name = column_names
    return series


def df_dict_to_multi_index(
    nested_df: dict[str | int, DataFrame],
    column_names: Sequence[str],
    invert: bool = True,
) -> DataFrame:
    df: DataFrame = DataFrame.from_dict(
        {
            (group, row[0]): row[1:]
            for group, df in nested_df.items()
            for row in df.itertuples()
        }
    )
    if invert:
        df = df.T
    df.columns = column_names
    return df


def sum_if_multi_column_df(df: DataFrame) -> Series:
    """Sum all columns to a series, or return single column as Series."""
    if len(df.columns) > 1:
        return df.sum(axis="columns")
    else:
        return df


def df_set_columns(df: DataFrame, column_names: Sequence[str]) -> DataFrame:
    """Force `df` columns to match `column_names`."""
    assert set(column_names) <= set(df.columns)
    df.columns = column_names
    return df


def value_in_dict_vals(value: Any, dictionary: dict) -> bool:
    """Test if `value` is in any `dictionary` values."""
    matches = []
    for val in dictionary.values():
        if isinstance(value, DataFrame | Series):
            matches.append(value.equals(val))
        else:
            matches.append(val == value)
    return any(matches)


def get_attr_from_attr_str(
    cls,
    attr_str: str,
    self_str: str | None = None,
    strict: bool = False,
    split_str: str = ".",
) -> Any:
    """Return value inferred from cls and attr_str."""
    if self_str:
        attr_str_list: list[str] = attr_str.split(split_str)
        if len(attr_str_list) > 1:
            attr_str = split_str.join(attr_str_list[1:])
            logger.debug(f"Dropped {self_str}, `attr_str` set to: '{attr_str}'")
        else:
            if attr_str_list[0] == self_str:
                logger.debug(
                    f"`attr_str`: '{attr_str}' == `self_str`: '{self_str}', "
                    f"returning {cls}"
                )
                return cls
            else:
                logger.info(f"Keeping '{self_str}' in `attr_str`: '{attr_str}'")
    get_attr_func: Callable = attrgetter(attr_str)
    try:
        value: Any = get_attr_func(cls)
    except AttributeError as err:
        if strict:
            raise err
        else:
            logger.debug(
                f"Attribute '{attr_str}' not part of {cls}, returning '{attr_str}'"
            )
            return attr_str
    else:
        logger.debug(f"Extracted '{attr_str}' from {cls}, returning '{value}'")
        return value

    # return  or attr_str
    # try:
    # except:
    #     if strict:
    #         raise AttributeError(f"Attribute {attr_str} not part of {cls}")
    #     else:
    #         logger.debug(f"Attribute {attr_str} not part of {cls}")

    # if param_str.startswith(f"{self_str}"):
    #     param_str = param_str[len(f"{self_str}."):]
    # param_attr_tuple: tuple[str, ...] =  param_str.split('.')
    # obj: Any = cls

    # for i, attr_name in enumerate(param_attr_tuple):
    #     if self_str and attr_name == self_str and i == 0:
    #         obj = cls
    #     # if  guessed_attr_name: str = param_str.split('.')
    #     if hasattr(obj, attr_name):
    #         obj = getattr(cls, attr_name)
    #         assert False
    #     else:
    #         if strict:
    #             raise AttributeError(f"Attribute {attr_name} not part of {obj}")
    #         else:
    #             logger.debug(f"Attribute {attr_name} not part of {obj}")
    #             continue
    # if obj is not None:
    #     return param_str
    # raise AttributeError(f"Failed to extract {param_str} from {obj}.")


# def expand_df_dict_to_multi_index(df, )


# def filled_or_empty_dict(indexable: dict, key: str) -> dict[str, str]:
#     return indexable[key] if key in indexable else {}
# >>>>>>> Stashed changes


# def y_ij_m_to_networkx(y_ij_m_results: Series,
#                        city_column: str = CITY_COLUMN) -> DiGraph:
#     flows: DiGraph()
#     flows.add_nodes_from(y_ij_m_to_networkx.index.get_level_values(city_column))
#     y_ij_m.apply(lambda row: flows.add_edge())
#     flows.add_edges([])
