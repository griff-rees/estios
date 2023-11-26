from copy import deepcopy
from logging import getLogger
from pathlib import Path
from typing import Callable, Sequence

from pandas import DataFrame, Series

from ..sources import FilePathType, pandas_from_path_or_package
from ..utils import SECTOR_10_CODE_DICT, aggregate_rows, ensure_list_of_strs
from .nomis_contemporary_employment import (
    NOMIS_GEOGRAPHY_CODE_COLUMN_NAME,
    NOMIS_GEOGRAPHY_NAME_COLUMN_NAME,
    NOMIS_INDUSTRY_CODE_COLUMN_NAME,
    NOMIS_METADATA,
    NOMIS_OBSERVATION_VALUE_COLUMN_NAME,
    clean_nomis_employment_query,
    national_employment_query,
)
from .ons_population_estimates import (
    ONS_2017_ALL_AGES_COLUMN_NAME,
    ONS_CONTEMPORARY_POPULATION_META_DATA,
)
from .utils import (
    ONS_AGES_COLUMN_NAME,
    ONS_AREA_CODE_COLUMN_NAME,
    UK_NAME,
    UK_NATION_NAMES,
    GenericRegionsManager,
    PUASManager,
    get_working_cities_puas_manager,
    load_contemporary_ons_population,
    sum_for_regions_by_la_code,
    working_ages,
)

logger = getLogger(__name__)


def get_regional_mid_year_populations(
    year: int = 2017,
    region_names: Sequence[str] | str | None = None,
    ons_population_df: DataFrame | None = None,
    regions_manager: PUASManager | GenericRegionsManager | None = None,
    column_prefix: str = "Mid-",
    set_index_to_column: str | None = "Code",
) -> Series:
    """A wrapper to manage errors before calling `series_mid_year_population`.

    Args:
        year: Year of population requested.
        region_names: Names of regions to query populations of.
        ons_population_df: Population time series `DataFrame`
            (will load via `load_contemporary_ons_population` if `None`).
        regions_manager: `PUASManager` instance to match `region_names`.
        column_prefix: Optional `str` prefix for indexing `year` columns.
        set_index_to_column: Which column to use for indexing `region_names`.

    Returns:
        A `Series` from calling `series_mid_year_population`.

    Raises:
        AssertionError: If `ons_population_df`, `regions_manager` or
            `region_names` do not load correctly .
    """
    if not ons_population_df:
        ons_population_df = load_contemporary_ons_population()
    assert isinstance(ons_population_df, DataFrame)
    if not regions_manager:
        regions_manager = get_working_cities_puas_manager()
    assert isinstance(regions_manager, PUASManager)
    if not region_names:
        region_names = regions_manager.names
    return series_mid_year_population(
        df=ons_population_df,
        regions_manager=regions_manager,
        year=year,
        region_names=region_names,
        column_prefix=column_prefix,
        set_index_to_column=set_index_to_column,
    )


def series_mid_year_population(
    df: DataFrame,
    region_names: Sequence[str],
    regions_manager: PUASManager,
    year: int = 2017,
    set_index_to_column: str | None = "Code",
    column_prefix: str = "Mid-",
) -> Series:
    """Return a `DataFrame` of mid-year popluations for `region_names`.

    Args:
        df: `DataFrame` with mid-year population records.
        region_names: Names of regions to query populations of.
        regions_manager: `PUASManager` instance to match `region_names`.
        year: Year of population requested.
        set_index_to_column: Which column to use for indexing `region_names`.
        column_prefix: Optional `str` prefix for indexing `year` columns.

    Returns:
        A `Series` of populations for given `year` for `region_names`.
    """
    return Series(
        sum_for_regions_by_la_code(
            df=df,
            region_names=region_names,
            column_names=[f"{column_prefix}{year}"],
            regions=regions_manager,
            set_index_to_column=set_index_to_column,
        )
    )


def get_employment_by_region_by_sector(
    region_names: Sequence[str] | dict[str, str],
    sector_codes: Sequence[str] | None = None,
    year: int = 2017,
    nomis_employment_df: DataFrame | None = None,
    regions_manager: PUASManager | GenericRegionsManager | None = None,
    set_index_to_column: str | None = NOMIS_GEOGRAPHY_CODE_COLUMN_NAME,
    ignore_key_errors: bool = False,
) -> DataFrame:
    """Return a `DataFrame` of sector employment per region at `year`.

    Args:
        region_names: Names of regions to query populations of.
        sector_codes: Names of sectors to include.
        year: Year of population requested.
        nomis_employment_df: `DataFrame` of NOMIS regional employment estimates.
        regions_manager: `PUASManager` instance to match `region_names`.
        set_index_to_column: Which column to use for indexing `region_names`.
        ignore_key_errors: Whether to fail or ignore `key` indexing errors.

    Returns:
        A `DataFrame` of populations for given `year` for `region_names`.

    Raises:
        AssertionError: Raised if `nomis_employment_df`, `sector_codes`,
            `regions_manager` or `region_names` are not valid.
    """
    if nomis_employment_df is None:
        nomis_employment_df = clean_nomis_employment_query(year)
    assert nomis_employment_df is not None  # helps type checking
    assert isinstance(nomis_employment_df, DataFrame)
    if not sector_codes:
        sector_codes = nomis_employment_df[NOMIS_INDUSTRY_CODE_COLUMN_NAME].unique()
    assert sector_codes is not None
    try:
        assert isinstance(sector_codes, Sequence)
    except AssertionError:
        logger.warning(
            f"`sector_codes` is of type: {type(sector_codes)} in `get_nation_employment_by_sector`"
        )
    if not regions_manager:
        regions_manager = get_working_cities_puas_manager()
    assert isinstance(regions_manager, PUASManager)
    if not region_names:
        region_names = regions_manager.names
    if isinstance(region_names, dict):
        region_names = tuple(region_names)
    sectors_dict: dict[str, DataFrame] = {}
    for sector_code in sector_codes:
        sectors_dict[sector_code] = Series(
            sum_for_regions_by_la_code(
                df=nomis_employment_df[
                    nomis_employment_df[NOMIS_INDUSTRY_CODE_COLUMN_NAME] == sector_code
                ],
                region_names=region_names,
                column_names=[NOMIS_OBSERVATION_VALUE_COLUMN_NAME],
                regions=regions_manager,
                set_index_to_column=set_index_to_column,
                ignore_key_errors=ignore_key_errors,
            )
        )
    return DataFrame(sectors_dict)


def get_nation_employment_by_sector(
    year: int = 2017,
    quarter: str = "June",  # Mid year easest for comparison
    nation_names: Sequence[str] | str | None = UK_NATION_NAMES,
    # sector_codes: Sequence[str] = None,
    column_names: Sequence[str]
    | str
    | None = [
        NOMIS_OBSERVATION_VALUE_COLUMN_NAME,
        NOMIS_INDUSTRY_CODE_COLUMN_NAME,
    ],
    nomis_employment_df: DataFrame | None = None,
    set_index_to_column: str | None = NOMIS_GEOGRAPHY_NAME_COLUMN_NAME,
) -> float | Sequence | DataFrame | Series:
    """Return a national employment by sector by `year`.

    Args:
        region_names: Names of regions to query populations of.
        sector_codes: Names of sectors to include.
        year: Year of population requested.
        nomis_employment_df: `DataFrame` of NOMIS regional employment estimates.
        regions_manager: `PUASManager` instance to match `region_names`.
        set_index_to_column: Which column to use for indexing `region_names`.
        ignore_key_errors: Whether to fail or ignore `key` indexing errors.

    Returns:
        A `DataFrame` of populations for given `year` for `region_names`.

    Raises:
        AssertionError: Raised if `nomis_employment_df`, `sector_codes`,
            `regions_manager` or `region_names` are not valid.
    """
    if nomis_employment_df is None:
        nomis_employment_df = national_employment_query(year=year)  # , quarter=quarter
    # if not sector_codes:
    #     sector_codes = nomis_employment_df[NOMIS_INDUSTRY_CODE_COLUMN_NAME].unique()
    assert isinstance(nomis_employment_df, DataFrame)
    if set_index_to_column:
        nomis_employment_df.set_index(set_index_to_column, inplace=True)
    if isinstance(nation_names, str) or nation_names and len(nation_names) == 1:
        nomis_employment_df = nomis_employment_df.loc[nation_names, column_names]
        return nomis_employment_df.set_index(NOMIS_INDUSTRY_CODE_COLUMN_NAME)[
            NOMIS_OBSERVATION_VALUE_COLUMN_NAME
        ]
    else:
        return nomis_employment_df.loc[nation_names, column_names]


def meta_data_reader_wrapper(
    path: FilePathType,
    local_path: FilePathType,
    data_filter_func: Callable,
    df_kwarg: str = "nomis_employment_df",
    **kwargs,
) -> DataFrame | Series:
    """Wrapper to return data from `path`, and potentially download if necessary.

    Args:
        path: `Path` to original file.
        local_path: `Path` file is saved to.
        data_filter_func: `Callable` to convert data to `DataFrame` or `Series`.
        df_kwargs: Arguments to pass to `data_filter_func`.

    Returns:
        A `DataFrame` or `Series` from processing `path` via copy at `local_path`.
    """
    loaded_df = pandas_from_path_or_package(path, local_path)
    kwargs[df_kwarg] = loaded_df
    return data_filter_func(**kwargs)


def regional_population_projections(
    year: str | int,
    age_range: str | int | Sequence[str] | Sequence[int],
    population_projections_df: DataFrame = None,
    region_names: Sequence[str] | None = None,
    regions_manager: PUASManager | GenericRegionsManager | None = None,
    age_group_column_name: str = ONS_AGES_COLUMN_NAME,
    set_index_to_column: str | None = ONS_AREA_CODE_COLUMN_NAME,
    ignore_key_errors: bool = False,
) -> Series:
    """Return region level population projection for given `year`.

    Args:
        year: year of population projections.
        age_range: what ages to get population projections of.
        population_projections_df: `DataFrame` of population projections.
        region_names: Names of regions to query populations of.
        regions_manager: `PUASManager` instance to match `region_names`.
        age_group_column_name: Column name to query age groups from
            `population_projections_df`.
        set_index_to_column: Which column to use for indexing `region_names`.
        ignore_key_errors: Whether to fail or ignore `key` indexing errors.

    Returns:
        A `Series` of `region_names` populations projected for `year`.

    Raises:
        AssertionError: If `regions_manager` is not valid.
    """
    age_range = ensure_list_of_strs(age_range)
    if not region_names:
        if not regions_manager:
            regions_manager = get_working_cities_puas_manager()
        assert isinstance(regions_manager, PUASManager)
        region_names = regions_manager.names
    assert regions_manager
    populations_age_filtered: DataFrame = population_projections_df[
        population_projections_df[age_group_column_name].isin(age_range)
    ]
    return Series(
        sum_for_regions_by_la_code(
            df=populations_age_filtered,
            region_names=region_names,
            column_names=[str(year)],
            regions=regions_manager,
            set_index_to_column=set_index_to_column,
            ignore_key_errors=ignore_key_errors,
            # attr="la_codes",
        )
    )


def regional_population_projections_all_ages(year: str | int, **kwargs) -> Series:
    """Return a `Series` of populations for `year` for all ages.

    Args:
        year: year of population projections.

    Returns:
        A `Series` of `region_names` all age populations projected for `year`.
    """
    return regional_population_projections(
        year=year, age_range=ONS_2017_ALL_AGES_COLUMN_NAME, **kwargs
    )


def regional_population_projections_working_ages(year: str | int, **kwargs) -> Series:
    """Return a `Series` of working age populations for `year`.

    Args:
        year: year of population projections.

    Returns:
        A `Series` of `region_names` indexed, working age population `Series` for `year`.
    """
    if isinstance(year, str):
        year = int(year)
    return regional_population_projections(
        year=year, age_range=list(working_ages(year)), **kwargs
    )


NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA = deepcopy(NOMIS_METADATA)
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA.name = "NOMIS UK Annual National Employment"
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA.year = 2017
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA.auto_download = True
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._package_data = True
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._package_path = Path("uk/data")
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._api_func = national_employment_query
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._api_kwargs = dict(year=2017)
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._reader_func = meta_data_reader_wrapper
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._reader_kwargs = dict(
    data_filter_func=get_nation_employment_by_sector, nation_names=UK_NAME
)
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._post_read_func = aggregate_rows
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._post_read_kwargs = dict(
    sector_dict=SECTOR_10_CODE_DICT
)
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA.path = "nomis_national_employment.csv"

# NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._package_data = True

NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA = deepcopy(NOMIS_METADATA)
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA.name = "NOMIS UK Annual Regional Employment"
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA.year = 2017
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA.auto_download = True
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA._package_data = True
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA._package_path = Path("uk/data")
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA._api_func = clean_nomis_employment_query
NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA._api_kwargs = dict(year=2017)
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA._reader_func = meta_data_reader_wrapper
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA._reader_kwargs = dict(
    data_filter_func=get_employment_by_region_by_sector,
    ignore_key_errors=True,
    region_names="self.region_names",
)
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA._post_read_func = aggregate_rows
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA._post_read_kwargs = dict(
    sector_dict=SECTOR_10_CODE_DICT,
)
NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA.path = "nomis_regional_employment.csv"


ONS_MID_YEAR_POPULATIONS_2017_METADATA = deepcopy(ONS_CONTEMPORARY_POPULATION_META_DATA)
ONS_MID_YEAR_POPULATIONS_2017_METADATA.name = (
    "UK ONS 2017 Mid-Year Population Estimates"
)
ONS_MID_YEAR_POPULATIONS_2017_METADATA._post_read_func = series_mid_year_population
ONS_MID_YEAR_POPULATIONS_2017_METADATA._post_read_kwargs = dict(
    regions_manager=get_working_cities_puas_manager(),
    region_names="self.region_names",
)
ONS_MID_YEAR_POPULATIONS_2017_METADATA._package_data = True
