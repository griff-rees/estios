#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This module is a wrapper of queries using the `ukcensusapi.Nomisweb` package."""

from collections import deque
from datetime import date
from logging import getLogger
from os import environ
from pathlib import Path
from pprint import pformat
from typing import Callable, Final, Literal, Sequence, overload

import ukcensusapi.Nomisweb as census_api
from dateutil.parser import parse
from dotenv import load_dotenv
from pandas import DataFrame

from ..sources import MetaData, OpenGovernmentLicense

logger = getLogger(__name__)


load_dotenv()

NOMIS_API_KEY: str = ""


class APIKeyNomisError(Exception):
    ...


try:
    # Following the KEY as defined in https://github.com/virgesmith/UKCensusAPI/blob/main/ukcensusapi/Nomisweb.py
    NOMIS_API_KEY = environ["NOMIS_API_KEY"]
except KeyError:
    logger.warning("NOMIS token not found in local .env file.")

NOMIS_GEOGRAPHY_CODE_COLUMN_NAME: Final[str] = "GEOGRAPHY_CODE"
NOMIS_GEOGRAPHY_NAME_COLUMN_NAME: Final[str] = "GEOGRAPHY_NAME"
NOMIS_EMPLOYMENT_STATUS_COLUMN_NAME: Final[str] = "EMPLOYMENT_STATUS_NAME"
NOMIS_OBSERVATION_VALUE_COLUMN_NAME: Final[str] = "OBS_VALUE"
NOMIS_MEASURE_TYPE_COLUMN_NAME: Final[str] = "MEASURE_NAME"
NOMIS_INDUSTRY_CODE_COLUMN_NAME: Final[str] = "INDUSTRY_CODE"
NOMIS_EMPLOYMENT_COUNT_VALUE: Final[str] = "Employment"
NOMIS_MEASURE_COUNT_VALUE: Final[str] = "Count"

NOMIS_TOTAL_EMPLOYMENT_TABLE_CODE: Final[str] = "NM_57_1"
NOMIS_SECTOR_EMPLOYMENT_TABLE_CODE: Final[str] = "NM_189_1"

DEFAULT_PATH: Final[Path] = Path("estios/uk/data/nomisweb")

NOMIS_FIRST_YEAR: Final[int] = 1981
NOMIS_LAST_YEAR: Final[int] = 2022
NOMIS_LOCAL_AUTHORITY_LAST_EMPLOYMENT_YEAR: Final[int] = 2021
NOMIS_YEAR_RANGE: tuple[int, ...] = tuple(range(NOMIS_FIRST_YEAR, NOMIS_LAST_YEAR + 1))
NOMIS_LOCAL_AUTHORITY_EMPLOYMENT_YEAR_RANGE: tuple[int, ...] = tuple(
    range(NOMIS_FIRST_YEAR, NOMIS_LOCAL_AUTHORITY_LAST_EMPLOYMENT_YEAR + 1)
)
URL: Final[str] = "https://www.nomisweb.co.uk"
INFO_URL: Final[str] = "https://www.nomisweb.co.uk/default.asp"


NOMIS_GEO_PARAM_STR: Final[str] = "geography"
NOMIS_DATE_PARAM_STR: Final[str] = "date"
NOMIS_INDUSTRY_PARAM_STR: Final[str] = "industry"
NOMIS_EMPLOYMENT_STATUS_PARAM_STR: Final[str] = "employment_status"
NOMIS_MEASURE_PARAM_STR: Final[str] = "measure"
NOMIS_MEASURES_PARAM_STR: Final[str] = "measures"
NOMIS_SELECT_PARAM_STR: Final[str] = "select"
NOMIS_SEX_PARAM_STR: Final[str] = "sex"
NOMIS_ITEM_PARAM_STR: Final[str] = "item"

# K02000001 for all UK, but not available in NM_189_1
#                  United Kingdom (not including Northern Ireland), all other local authorities (missing data from Northern Ireland)
NOMIS_LOCAL_AUTHORITY_GEOGRAPHY_CODES_STR: Final[
    str
] = "K03000001,1879048193...1879048572"
NOMIS_INDUSTRY_SECTIONS_BY_LETTER_CODES_STR: Final[str] = "150994945...150994965"
NOMIS_EMPLOYMENT_STATUS_CODES_STR: Final[str] = "1,4"
NOMIS_EMPLOYMENT_MEASURE_CODES_STR: Final[str] = "1,2"
NOMIS_EMPLOYMENT_MEASURES_CODE_STR: Final[str] = "20100"
NOMIS_EMPLOYMENT_SELECT_COLUMNS: Final[
    str
] = "date_name,geography_name,geography_code,industry_name,employment_status_name,measure_name,measures_name,obs_value,obs_status_name,industry_code"

NOMIS_LETTER_SECTOR_QUERY_PARAM_DICT: dict[str, str] = {
    NOMIS_GEO_PARAM_STR: NOMIS_LOCAL_AUTHORITY_GEOGRAPHY_CODES_STR,
    NOMIS_INDUSTRY_PARAM_STR: NOMIS_INDUSTRY_SECTIONS_BY_LETTER_CODES_STR,
    NOMIS_EMPLOYMENT_STATUS_PARAM_STR: NOMIS_EMPLOYMENT_STATUS_CODES_STR,
    NOMIS_MEASURE_PARAM_STR: NOMIS_EMPLOYMENT_MEASURE_CODES_STR,
    NOMIS_MEASURES_PARAM_STR: NOMIS_EMPLOYMENT_MEASURES_CODE_STR,
    NOMIS_SELECT_PARAM_STR: NOMIS_EMPLOYMENT_SELECT_COLUMNS,
}

# https://www.nomisweb.co.uk/api/v01/dataset/NM_131_1.data.csv?geography=2092957699,2092957702,2092957701,2092957697,2092957700&date=latestMINUS21&industry=150994945...150994964&sex=1...4,7&item=1...5&measures=20100&select=date,date_name,geography,geography_name,geography_code,geography_typecode,industry,industry_name,industry_code,industry_typecode,sex_name,item_name,measures_name,obs_value,obs_status_name
# geography=2092957699,2092957702,2092957701,2092957697,2092957700&
# date=latestMINUS21
# &industry=150994945...150994964
# &sex=1...4,7
# &item=1...5
# &measures=20100
# &select=date,date_name,geography,geography_name,geography_code,geography_typecode,industry,industry_name,industry_code,industry_typecode,sex_name,item_name,measures_name,obs_value,obs_status_name

NOMIS_ALL_SEXES_VALUE: Final[str] = "Total"
NOMIS_TOTAL_WORKFORCE_VALUE: Final[str] = "total workforce jobs"
NOMIS_NATIONAL_EMPLOYMENT_TABLE_CODE: Final[str] = "NM_131_1"
NOMIS_NATIONAL_GEOGRAPHY_CODES_STR: Final[
    str
] = "2092957699,2092957702,2092957701,2092957697,2092957700"
NOMIS_NATIONAL_GEOGRAPHY_DATE_STR: Final[str] = "latestMINUS21"
NOMIS_NATIONAL_SEX_CODES_STR: Final[str] = "1...4,7"
NOMIS_NATIONAL_ITEM_CODES_STR: Final[str] = "1...5"
NOMIS_NATIONAL_EMPLOYMENT_SELECT_COLUMNS: Final[
    str
] = "date,date_name,geography,geography_name,geography_code,geography_typecode,industry,industry_name,industry_code,industry_typecode,sex_name,item_name,measures_name,obs_value,obs_status_name"
NOMIS_NATIONAL_LETTER_SECTOR_QUERY_PARAM_DICT: dict[str, str] = {
    NOMIS_GEO_PARAM_STR: NOMIS_NATIONAL_GEOGRAPHY_CODES_STR,
    NOMIS_INDUSTRY_PARAM_STR: NOMIS_INDUSTRY_SECTIONS_BY_LETTER_CODES_STR,
    NOMIS_SEX_PARAM_STR: NOMIS_NATIONAL_SEX_CODES_STR,
    NOMIS_ITEM_PARAM_STR: NOMIS_NATIONAL_ITEM_CODES_STR,
    NOMIS_MEASURES_PARAM_STR: NOMIS_EMPLOYMENT_MEASURES_CODE_STR,
    NOMIS_SELECT_PARAM_STR: NOMIS_NATIONAL_EMPLOYMENT_SELECT_COLUMNS,
}
# https://www.nomisweb.co.uk/api/v01/dataset/NM_189_1.data.csv?
# geography=1879048193...1879048572
# &date=latestMINUS4&industry=150994945...150994965
# &employment_status=1,4&measure=1,2&measures=20100&select=date_name,geography_name,geography_code,industry_name,employment_status_name,measure_name,measures_name,obs_value,obs_status_name
# &measure=1,2&measures=20100&select=date_name,geography_name,geography_code,industry_name,employment_status_name,measure_name,measures_name,obs_value,obs_status_name

NOMIS_UK_QUARTER_STRS: Final[tuple[str, ...]] = (
    "March",
    "June",
    "September",
    "December",
)

NOMIS_FINANCIAL_QUARTER_ORDER: deque[str] | tuple[str] = deque(NOMIS_UK_QUARTER_STRS)

NOMIS_FINANCIAL_QUARTER_ORDER.rotate()
"""Quarter name order for Nomis where December quarter covers Jan + Feb"""


@overload
def uk_quarter_indexing(
    month_or_date: date | str | int, quarter_names: Sequence[str], as_str: Literal[True]
) -> str:
    ...


@overload
def uk_quarter_indexing(
    month_or_date: date | str | int,
    quarter_names: Sequence[str],
    as_str: Literal[False],
) -> int:
    ...


def uk_quarter_indexing(
    month_or_date: date | str | int,
    quarter_names: Sequence[str] = NOMIS_FINANCIAL_QUARTER_ORDER,
    as_str: bool = True,
) -> str | int:
    """Return NOMIS quarter `str` or `int` from a `date` `int` or month `str`.

    Args:
        month_or_date: convert to an 1 <= int <= 12 if a `date` or `str`
        quarter_names: `Seuqence` of `str` of length 4 for quarter names
        as_str: Whether to return a `str` or an index `int`

    Return:
        Quarter name (months by default) for `month_or_date` or index (0-3).

    Example:
        ```pycon
        >>> uk_quarter_indexing("March")
        'March'
        >>> uk_quarter_indexing("Feb")
        'December'
        >>> uk_quarter_indexing(2)  # Equivalent of Feb
        'December'
        >>> uk_quarter_indexing(date(2017,12,1))
        'December'
        >>> uk_quarter_indexing(date(2017,11,1))
        'September'
        >>> uk_quarter_indexing(date(2017,11,1), as_str=False)
        3
        >>> uk_quarter_indexing(date(2017,12,1), as_str=False)
        0

        ```
    """
    if isinstance(month_or_date, date):
        month_or_date = month_or_date.month
    elif isinstance(month_or_date, str):
        month_or_date = parse(month_or_date).month
    elif month_or_date < 1 or month_or_date > 12:
        raise ValueError(
            f"`month_or_date` as `int` must be 0 < `month_or_date` "
            f"<= 12, recieved: {month_or_date} "
        )
    month_index: int = (month_or_date) // 3 if month_or_date < 12 else 0
    if as_str:
        return quarter_names[month_index]
    else:
        return month_index


DATE_OF_MODULE_IMPORT: Final[date] = date.today()

NOMIS_LATEST_AVAILABLE_QUARTER_STR: Final[str] = uk_quarter_indexing(
    month_or_date=DATE_OF_MODULE_IMPORT, as_str=True
)


def gen_year_query(
    year: int = 2017,
    default_str: str = "latest",
    modify_str: str = "MINUS",
    valid_year_range: tuple[int, ...] = NOMIS_LOCAL_AUTHORITY_EMPLOYMENT_YEAR_RANGE,
) -> str:
    """Generate a `NOMIS` year query `str`.

    Args:
        year: year to generate `query` `str` for.
        default_str: query `str` to use for current time.
        modify_str: query `str` to prefix date change.
        valid_year_range: which years are valid to query.

    Returns:
        Query `str` for given year, or `latest` if current.

    Raises:
        ValueError: If `year` is not within `valid_year_range`.
    """
    logger.info(f"Running `gen_year_query` for {year}, assuming annual releases.")
    if year not in valid_year_range:
        raise ValueError(
            f"`year`: {year} not available within NOMIS "
            f"`valid_year_range`: {valid_year_range}"
        )
    if year < max(valid_year_range):
        years_prior_to_latest: int = max(valid_year_range) - year
        return f"{default_str}{modify_str}{years_prior_to_latest}"
    else:
        return default_str


def nomis_query(
    year: int,
    nomis_table_code: str = NOMIS_TOTAL_EMPLOYMENT_TABLE_CODE,
    query_params: dict[str, str] | None = None,
    download_path: Path = DEFAULT_PATH,
    # quarter: str | None = None,
    api_key: str | None = None,
    require_api_key: bool = True,
    # simlink_api_key=True,
    date_func: Callable[[int, str, str, tuple[str, ...]], str] = gen_year_query,
    valid_years: Sequence[int] = NOMIS_YEAR_RANGE,
    # class_4_industries: str | None = None,
    # default_employment_by_letter_sections_config: bool = False,
) -> DataFrame:
    """Query Nomisweb for Local Authority employment data at `year`."""
    if not year in valid_years:
        raise ValueError(
            f"`year`: {year} not available within NOMIS `valid_years`: {valid_years}"
        )
    api = census_api.Nomisweb(download_path)
    if not api_key and NOMIS_API_KEY:
        api_key = NOMIS_API_KEY
    if require_api_key:
        assert api_key
    if api_key:
        api.key = api_key
    if not query_params:
        logger.info(f"Querying default NOMIS {year} employment status")
        query_params = {}
        query_params["geography"] = "2092957698,1946157057...1946157462"
        # query_params["date"] = "latestMINUS4"
        query_params["ITEM"] = "1,3"
        query_params["MEASURES"] = "20100,404423937...404423945"
        # query_params["measures"] = "20100,20701"
        query_params[
            "select"
        ] = "date_name,geography_name,geography_code,item_name,measures_name,obs_value,obs_status_name"
        # https://www.nomisweb.co.uk/api/v01/dataset/NM_100_1.data.csv?&select=date_name,geography_name,geography_code,cell_name,measures_name,obs_value,obs_status_name
    else:
        logger.info(f"Querying with:\n{pformat(query_params)}")

    query_params["date"] = date_func(year)
    return api.get_data(nomis_table_code, query_params)


def gen_date_query(
    year: int = 2017,
    quarter: str = "June",
    default_str: str = "latest",
    modify_str: str = "MINUS",
    valid_quarters: tuple[str, ...] = NOMIS_UK_QUARTER_STRS,
    valid_year_range: tuple[int, ...] = NOMIS_LOCAL_AUTHORITY_EMPLOYMENT_YEAR_RANGE,
    latest_quarter: str = NOMIS_LATEST_AVAILABLE_QUARTER_STR,
) -> str:
    """Generate a `NOMIS` date query `str`.

    Args:
        year: year to generate `query` `str` for.
        quarter: which of `valid_quarters` to get date of.
        default_str: query `str` to use for current time.
        modify_str: query `str` to prefix date change.
        valid_quarters: `str` of quarter names.
        valid_year_range: which years are valid to query.
        latest_quarter: latest quarter at time code is run.

    Returns:
        Query `str` for given date.

    Raises:
        ValueError: If `year` is not within `valid_year_range`.
    """
    if not quarter:
        return gen_year_query(
            year=year,
            default_str=default_str,
            modify_str=modify_str,
            valid_year_range=valid_year_range,
        )
    else:
        logger.info(
            f"Running `gen_date_query` for {quarter} {year}, assuming quarterly releases."
        )
        if year not in valid_year_range:
            raise ValueError(
                f"`year`: {year} not available within NOMIS `valid_year_range`: {valid_year_range}"
            )
        years_prior_to_latest: int = 0
        if year < max(valid_year_range):
            years_prior_to_latest = max(valid_year_range) - year
        quarter_difference: int = valid_quarters.index(
            latest_quarter
        ) - valid_quarters.index(quarter)
        if quarter_difference < 0:
            quarter_difference = len(valid_quarters) + quarter_difference
        total_difference: int = (years_prior_to_latest + 1) * len(
            valid_quarters
        ) + quarter_difference
        if total_difference:
            return f"{default_str}{modify_str}{total_difference}"
        else:
            return default_str


def trim_df_for_employment_count(
    df: DataFrame,
    first_column_name: str = NOMIS_MEASURE_TYPE_COLUMN_NAME,
    first_value: str = NOMIS_MEASURE_COUNT_VALUE,
    second_column_name: str = NOMIS_EMPLOYMENT_STATUS_COLUMN_NAME,
    second_value: str = NOMIS_EMPLOYMENT_COUNT_VALUE,
) -> DataFrame:
    """Trim rows for Employment (rather than Employees) Count.

    Args:
        df: `DataFrame` to trim employment records from.
        first_column_name: first `df` column query by `first_value`.
        first_value: value to filter `df` `first_column_name` by.
        second_column_name: second `df` column query by `second_value`.
        second_value: value to filter `df` `second_column_name` by.

    Return:
        `df` filtered by `first_column_name` and `second_column_name`.
    """
    return df[
        (df[first_column_name] == first_value)
        & (df[second_column_name] == second_value)
    ]


def clean_nomis_employment_query(
    year: int = 2017,
    sector_employment_df: DataFrame | None = None,
    download_path: Path = DEFAULT_PATH,
    api_key: str | None = None,
    nomis_table_code: str = NOMIS_SECTOR_EMPLOYMENT_TABLE_CODE,
    query_params: dict[str, str] = NOMIS_LETTER_SECTOR_QUERY_PARAM_DICT,
) -> DataFrame:
    """Return cleaned DataFrame with only employment counts per row."""
    if not sector_employment_df:
        sector_employment_df = nomis_query(
            year=year,
            nomis_table_code=nomis_table_code,
            query_params=query_params,
            download_path=download_path,
            api_key=api_key,
        )
    return trim_df_for_employment_count(sector_employment_df)


def national_employment_query(
    year: int = 2017,
    # quarter: str = "June",
    sector_employment_df: DataFrame | None = None,
    download_path: Path = DEFAULT_PATH,
    api_key: str | None = None,
    nomis_table_code: str = NOMIS_NATIONAL_EMPLOYMENT_TABLE_CODE,
    query_params: dict[str, str] = NOMIS_NATIONAL_LETTER_SECTOR_QUERY_PARAM_DICT,
    date_func: Callable[
        [int, str, str, str, tuple[str, ...], tuple[int, ...], str], str
    ] = gen_date_query,
) -> DataFrame:
    """Query wrapper for national level emloyment."""
    if not sector_employment_df:
        sector_employment_df = nomis_query(
            year=year,
            # quarter=quarter,
            nomis_table_code=nomis_table_code,
            query_params=query_params,
            download_path=download_path,
            date_func=date_func,
            api_key=api_key,
        )
    return trim_df_for_employment_count(
        sector_employment_df,
        first_column_name="SEX_NAME",
        first_value=NOMIS_ALL_SEXES_VALUE,
        second_column_name="ITEM_NAME",
        second_value=NOMIS_TOTAL_WORKFORCE_VALUE,
    )


NOMIS_METADATA: Final[MetaData] = MetaData(
    name="NOMIS UK Census Data",
    year=2023,
    dates=NOMIS_YEAR_RANGE,
    region="UK",
    url=URL,
    info_url=INFO_URL,
    description=(
        "Nomis is a service provided by Office for National Statistics (ONS),"
        "the UK’s largest independent producer of official statistics. On this"
        "website, we publish statistics related to population, society and the"
        "labour market at national, regional and local levels. These include"
        "data from current and previous censuses."
    ),
    # path=ONS_UK_2018_FILE_NAME,
    license=OpenGovernmentLicense,
    auto_download=False,
    needs_scaling=True,
    _package_data=False,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=nomis_query,
    _reader_kwargs=dict(year=2017),
)
