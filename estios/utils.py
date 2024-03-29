#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import Counter, OrderedDict
from collections.abc import Sequence
from dataclasses import Field, fields
from datetime import date
from functools import wraps
from itertools import zip_longest
from logging import getLogger
from operator import attrgetter
from os import PathLike
from typing import (
    Any,
    Callable,
    Collection,
    Final,
    Generator,
    Hashable,
    Iterable,
    Optional,
    Type,
    TypeAlias,
    Union,
)

# from networkx import DiGraph
from numpy import log
from pandas import DataFrame, Index, MultiIndex, Series, read_csv

# from .uk.employment import CITY_SECTOR_REGION_PREFIX

logger = getLogger(__name__)

DateType: TypeAlias = date
YearType: TypeAlias = int
RegionName: TypeAlias = str
SectorName: TypeAlias = str

RegionNamesListType: TypeAlias = Union[list[RegionName], Collection[RegionName]]
SectorNamesListType: TypeAlias = Union[list[SectorName], Collection[SectorName]]

RegionsIterableType: TypeAlias = (
    Sequence[RegionName] | dict[RegionName, RegionName] | Series
)

RegionConfigType: TypeAlias = Union[
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

IJ_M_INDEX_NAMES: Final[list[str]] = [
    CITY_COLUMN,
    OTHER_CITY_COLUMN,
    SECTOR_COLUMN_NAME,
]

FINAL_Y_IJ_M_COLUMN_NAME: Final[str] = "y_ij^m"

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

DEFAULT_NUM_ABBREVIATION_MAGNITUDE_LABELS: Final[tuple[str, ...]] = (
    "",
    "K",
    "M",
    "B",
    "T",
)


def name_converter(names: Sequence[str], name_mapper: dict[str, str]) -> list[str]:
    """Return region names with any conversions specified in name_mapper."""
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
    """Filter `y_ij_m_results` `DataFrame` by `city` and `sector`.

    Args:
        y_ij_m_results: `DataFrame` of Input-Output convergance results.
        city: city name.
        sector: sector name.
        city_column_name: name of `y_ij_m_results` column to index by `city`.
        sector_column_name: name of `y_ij_m_results` column to index by `sector`.
        column_index: column to index by.
        final_column_name: column name of saved results.

    Returns:
        `Series` of queried `city` and `sector`.
    """
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
        return df.iloc[:, column].rename(new_series_name)


def log_x_or_return_zero(x: float) -> Optional[float]:
    """Return max of `log` of `x` or 0, or `None` if `x` < 0.

    Args:
        x: number to take `log` of if >= 0.

    Returns:
        $log(x)$ if $log(x)> 0$ else $0.0$, or `None` if $x < 0$.
    """
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
    years: Iterable[str | int], first_year: int, last_year: int
) -> Generator[int, None, None]:
    """Trim `years` from earlier than `first_year` and older than `last_year`.

    Args:
        years: `Iterable` of `str` (of `int`) or `int`
        first_year: `int` of earliest year to keep.
        last_year: `int` of latest year to keep.

    Yields:
        Years in range as `int`s.
    """
    for year in years:
        if first_year <= int(year) <= last_year:
            yield int(year)


def iter_ints_to_list_strs(labels: Iterable[str | int]) -> list[str]:
    """Return a list of `strs`, primarily intended to use as plot labels.

    Args:
        labels: `Iterable` of labels as `str` or `int`.

    Returns:
        `list` of `labels` as `strs`.
    """
    return [str(label) for label in labels]


def collect_dupes(sequence: Iterable) -> dict[Any, int]:
    """Collect cases of duplicate values in passed `Iterable`.

    Args:
        sequence: `Iterable` of values to count dupes of.

    Returns:
        `dict` with keys as values from `sequence` which appear at least
        twice and value for count of duplications of paired value.
    """
    return {key: count for key, count in Counter(sequence).items() if count > 1}


def str_keys_of_dict(dict_to_stringify) -> dict[str, Any]:
    """Ensure passed `dict` keys are `strs`.

    Args:
        dict_to_stringify: `dict` instance to enforce `key` are `str`.

    Returns:
        `dict` with all keys now `strs`.
    """
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
    """Convert an iterable of `tuples` to an `OrderedDict`.

    Args:
        tuple_iter: Iterable of `tuples` of two values:
            fist is to be `key`, second `value`.

    Returns:
        `OrderedDict` from `tuple_iter`.
    """
    return OrderedDict([(key, val) for key, val in tuple_iter])


def filled_or_empty_dict(indexable: dict, key: str) -> dict[str, str]:
    """Return value from `dict` indexed from `key, else an exmpty `dict`.

    Args:
        indexable: `dict` to index from.
        key: key to index `indexable` with.

    Returns:
        Either the value of `key` in `indexable` or {}

    Todo:
        Check if it should expect a `dict` of `dicts`.
    """
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
    """Generator of equal instances in the sampe postions of `iter_a` and `iter_b`.

    Args:
        iter_a: an iterable.
        iter_b: an iterable with the same length as `iter_a`.

    Yields:
        Elements that are the same and in the same position of `iter_a` an `iter_b`.
    """
    return (x for x, y in zip(iter_a, iter_b) if x == y)


def filter_df_by_strs_or_sequences(
    rows: str | Sequence[str], columns: str | Sequence[str], df: DataFrame
) -> Series | DataFrame:
    """Wrapper to ease filtering a `DataFrame` by a `str` or a `Sequence`.

    Args:
        rows: either a `str` of row index, or a `Sequence` to index rows by.
        columns: either a `str` of a column name, or a `Sequence` filter columns by.
        df: `DataFrame` to filter.

    Returns:
        A `Series` or `DataFrame` filtered from `df`.
    """
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
    """Ensure `var` is returned as a `Series` type."""
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
    """Convert `series_or_df` to specified `dtype`."""
    return series_or_df.astype(dtype)


def dtype_wrapper(final_type: str):
    """Decorator to ensuring `final_type` from function wrapped."""

    def callable_wrapper(func: Callable):
        @wraps(func)
        def ensure_dtype_wrapper(*args, **kwargs) -> Series | DataFrame:
            return func(*args, **kwargs).astype(final_type)

        return ensure_dtype_wrapper

    return callable_wrapper


def len_less_or_eq(sequence: Sequence, count: int = 1) -> bool:
    """Check if `len` of `sequence` is <= to `count`."""
    return len(sequence) <= count


def get_df_nth_row(df: DataFrame, index: int = 0) -> Series:
    """Get index location of `df`."""
    return df.iloc[index]


def get_df_first_row(df: DataFrame) -> Series:
    """Get first row from `df`."""
    return get_df_nth_row(df)


def conditional_type_wrapper(
    condition_func: Callable[[Any], bool],
    type_wrapper: Callable[[Any], Any],
):
    """Decorator for applying `type_wrapper` if `condition_func` returns `True`."""

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


def df_column_to_single_value(
    df: DataFrame,
    results_column_name: str,
) -> float:
    """Apply query_str to df and extract the first elements."""
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
    """Yield attributes of `cls` whose names include `substring`.

    Args:
        cls: an object.
        substring: a `str` that might be inclued in `cls` attribute names.

    Yields:
        A `tuple` of `attr` name and `value` from `cls` that include
        `substring` in `attr` name.
    """
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
    """Trim and index `df`.

    Args:
        df: `DataFrame` to trim and index.
        columns: which columns to keep.
        index: which rows to keep.
        multi_index_level: which level of multi index to use.

    Returns:
        Trimmed and indexed `df`.
    """
    trimmed_df: DataFrame = df.loc[
        index.get_level_values(multi_index_level),
        columns.get_level_values(multi_index_level),
    ]
    trimmed_df.columns = columns
    return trimmed_df.set_index(index)


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
    """Join a `dict` of `DataFames` into one.

    Args:
        nested_df: A `dict` with `DataFrame` values to join together.
        column_names: column names to set for returned `DataFrame`.
        invert: whether to invert the final `DataFrame`

    Returns:
        `DataFrame` from initial `nested_df`.
    """
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


def sum_if_multi_column_df(df_or_series: DataFrame | Series) -> DataFrame:
    """Sum all columns to a series, or return single column as Series."""
    if isinstance(df_or_series, Series):
        return DataFrame(df_or_series)
    elif len(df_or_series.columns) > 1:
        return df_or_series.sum(axis="columns")
    else:
        return df_or_series


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


class GetAttrStrictError(Exception):
    ...


def get_attr_from_str(
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
        logger.debug(f"Attribute '{attr_str}' not part of {cls}")
        if strict:
            raise GetAttrStrictError(f"Parameter `strict` set to {strict} and {err}")
        else:
            logger.debug(f"Parameter `strict` set to {strict}, returning '{attr_str}'")
            return attr_str
    else:
        logger.debug(f"Extracted '{attr_str}' from {cls}, returning '{value}'")
        return value


def ensure_list_of_strs(names: str | int | Sequence[str] | Sequence[int]) -> list[str]:
    """Ensure `names` type(s) are/is conversted into a list of strings."""
    if isinstance(names, str):
        return [names]
    if isinstance(names, int):
        return [str(names)]
    if isinstance(names, Sequence):
        return [str(x) for x in names]


def is_intable_from_str(x: str) -> bool:
    """Return if str x could validly be converted to an int."""
    try:
        int_x = int(x)
    except ValueError:
        return False
    else:
        return True


def gen_filter_by_func(
    iterable: Iterable, func: Callable[[Any], bool] = is_intable_from_str
) -> Generator[Any, None, None]:
    """Yield value from `iterable` if `func` of `value` returns `True`.

    Args:
        iterable: An iterable of any `type`.
        func: A function that returns a `True` or `False` of passed
            elements in `iterable`.

    Yields:
        Elements from `iterable` that return `True` when passed to `func`.
    """
    return (x for x in iterable if func(x))


def human_readable_num_abbrv(
    num: float,
    magnitude_labels: Sequence[str] = DEFAULT_NUM_ABBREVIATION_MAGNITUDE_LABELS,
) -> str:
    """Convert number into abbreviated str.

    Note:
        * Refactor from
        https://stackoverflow.com/questions/68005050/b-billions-instead-of-g-giga-in-python-plotly-customdata-si-prefix-d3
    """
    scaled_num: float = float("{:.3g}".format(num))
    magnitude: int = 0
    while abs(scaled_num) >= 1000:
        magnitude += 1
        scaled_num /= 1000.0
    num_str: str = "{:f}".format(scaled_num).rstrip("0").rstrip(".")
    unit_str: str = magnitude_labels[magnitude]
    return num_str + unit_str


def apply_func_to_df_var(
    df: DataFrame,
    var_name: str,
    # func: Callable[[float | str, Any, ...], str],
    func: Callable[..., str],
    axis="columns",
    **kwargs,
) -> DataFrame:
    """Apply `func` to `df` `var_name`, specifying which `axis`."""
    return df.apply(lambda vector: func(vector[var_name], **kwargs), axis=axis)


def regions_type_to_list(regions: RegionsIterableType):
    """Return list or `RegionNames` from any `RegionNamesListType."""
    if isinstance(regions, dict):
        return list(regions.keys())
    elif isinstance(regions, Sequence) and not isinstance(regions, list):
        return list(regions)
    elif isinstance(regions, Series) and not isinstance(regions, list):
        return regions.to_list()
    else:
        return regions


def df_columns_to_index(
    df: DataFrame, column_names: Sequence[str] | str
) -> Index | MultiIndex:
    """Filter columns from a `DataFrame` to an `Index` or `MultiIndex`.

    Examples:
        ```pytest
        >>> df: DataFrame = read_csv('tests/test_3_city_yijm.csv')
        >>> str_index: Index = df_columns_to_index(
        ...     df,
        ...     FINAL_Y_IJ_M_COLUMN_NAME)
        >>> str_index[:2]
        Index([434.205483722981, 3719952.537031151], dtype='float64', name='y_ij^m')
        >>> one_column_list_index: Index = df_columns_to_index(
        ...     df,
        ...     [FINAL_Y_IJ_M_COLUMN_NAME])
        >>> assert (one_column_list_index == str_index).all()
        >>> two_columns_list_index: MultiIndex = df_columns_to_index(
        ...     df,
        ...     [FINAL_Y_IJ_M_COLUMN_NAME, 'b_ij^m'])
        >>> two_columns_list_index[:2]
        MultiIndex([( 434.205483722981, 0.0023450336299189),
                    (3719952.537031151, 0.0200601727671955)],
                   names=['y_ij^m', 'b_ij^m'])
        >>> two_columns_list_index: MultiIndex = df_columns_to_index(
        ...     df,
        ...     [])  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        ValueError: `column_names` [] invalid: at least 1 required

        ```
    """
    if isinstance(column_names, str):
        column_names = [column_names]
    if len(column_names) == 1:
        return Index(df[column_names[0]])
    elif len(column_names) > 1:
        return MultiIndex.from_frame(df[column_names])
    else:
        raise ValueError(
            f"`column_names` {column_names} invalid: " f"at least 1 required"
        )


def load_series_from_csv(
    path: PathLike,
    column_name: str,
    index: MultiIndex | Index | None = None,
    index_column_names: Sequence[str] | str | None = None,
) -> Series:
    """Load a column of results for re-use (visualisation etc.).

    Examples:
        ```pytest
        >>> flows: Series = load_series_from_csv(
        ...     path='tests/test_3_city_yijm.csv',
        ...     column_name=FINAL_Y_IJ_M_COLUMN_NAME,
        ...     index_column_names=IJ_M_INDEX_NAMES,
        ... )
        >>> assert flows.name == FINAL_Y_IJ_M_COLUMN_NAME
        >>> flows.dtype
        dtype('float64')
        >>> three_city_df: DataFrame = read_csv('tests/test_3_city_yijm.csv')
        >>> three_city_df.set_index(IJ_M_INDEX_NAMES, inplace=True)
        >>> assert (flows == three_city_df[FINAL_Y_IJ_M_COLUMN_NAME]).all()

        ```
    """
    df: DataFrame = read_csv(path)
    if isinstance(index_column_names, str):
        index_column_names = [index_column_names]
    if not index:
        index = (
            df_columns_to_index(df, index_column_names)
            if index_column_names
            else Index(df.index)
        )
    elif index_column_names:
        f"Both `index` and `index_columns` passed, using `index`"
        if isinstance(index_column_names, str):
            index_column_names = [index_column_names]
        if len(index_column_names) > 1:
            assert index.names == index_column_names
        else:
            assert index.name == index_column_names[0]
    series: Series = df[column_name]
    series.index = index
    return series
