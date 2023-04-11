#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict
from datetime import date
from logging import getLogger
from pathlib import Path
from typing import Callable, Final

from pandas import DataFrame

from ..sources import MetaData, OpenGovernmentLicense, pandas_from_path_or_package
from ..utils import DateConfigType
from .utils import enforce_ons_date_format, generate_employment_quarterly_dates

# ONS jobs data

ONS_URL_PREFIX: str = "https://www.ons.gov.uk/file?"

logger = getLogger(__name__)

UK_JOBS_BY_SECTOR_XLS_FILE_NAME: Final[Path] = Path("jobs05sep2021.xls")
DATE_COLUMN_NAME: Final[str] = "SIC 2007 section"
UK_NATIONAL_EMPLOYMENT_SHEET: Final[str] = "15. United Kingdom"
UK_JOBS_BY_SECTOR_SCALING: Final[float] = 1000

UK_CITY_SECTOR_YEARS: Final[list] = [2011, 2017]

CITY_SECTOR_REGION_PREFIX: Final[str] = "towncity:"

# Note: These are *template* str that may be superfluous
CITY_SECTOR_AVERAGE_EARNINGS_COLUMN_STR: Final[
    str
] = "Average Weekly Workplace Earnings YEAR  (Â£)"
CITY_SECTOR_POPULATION_COLUMN_STR: Final[str] = "Population YEAR"  # 1981 - 2019
CITY_SECTOR_EDUCATION_COLUMN_STR: Final[
    str
] = "Pupils achieving 9-4 grades in Maths & English at GCSE YEAR  (%)"  # 2017 - 2019

CITY_SECTOR_AVERAGE_EARNINGS_COLUMN: Final[
    str
] = "Average Weekly Workplace Earnings 2017  (Â£)"
CITY_SECTOR_POPULATION_COLUMN: Final[str] = "Population 2017"  # 1981 - 2019
CITY_SECTOR_EDUCATION_COLUMN: Final[
    str
] = "Pupils achieving 9-4 grades in Maths & English at GCSE 2017  (%)"  # 2017 - 2019

COVID_FLAGS_COLUMN: Final[str] = "COVID_FLAGS"

CITY_SECTOR_ENGINE: Final[str] = "python"

# Census export Nomis city and sector employment data

CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME: Final[Path] = Path(
    "nomis-city-employment-2017.csv"
)
CITY_SECTOR_SKIPROWS: Final[int] = 7
CITY_SECTOR_SKIPFOOTER: Final[int] = 8
CITY_SECTOR_USECOLS: Final[Callable[[str], bool]] = lambda x: "Unnamed" not in x
CITY_SECTOR_INDEX_COLUMN: Final[int] = 0

CITY_SECTOR_READ_KWARGS: Final[dict[str, int | str | Callable]] = dict(
    skiprows=CITY_SECTOR_SKIPROWS,
    skipfooter=CITY_SECTOR_SKIPFOOTER,
    engine=CITY_SECTOR_ENGINE,
    usecols=CITY_SECTOR_USECOLS,
    index_col=CITY_SECTOR_INDEX_COLUMN,
)
#     path: FilePathType = CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME,
#     skiprows: int = CITY_SECTOR_SKIPROWS,
#     skipfooter: int = CITY_SECTOR_SKIPFOOTER,
#     engine: str = CITY_SECTOR_ENGINE,
#     usecols: Callable[[str], bool] = CITY_SECTOR_USECOLS,
#     index_col: int = CITY_SECTOR_INDEX_COLUMN,

ONS_AGGREGATE_SECTOR_COLUMNS: Final[tuple[str, ...]] = ("G-T", "A-T")


def add_covid_flags_and_drop_agg_sector_columns(
    df: DataFrame,
    date_column_name: str = DATE_COLUMN_NAME,
    covid_flags_column: str = COVID_FLAGS_COLUMN,
    agg_sector_columns: tuple[str, ...] = ONS_AGGREGATE_SECTOR_COLUMNS,
    date_index_name: str = "Date",
) -> DataFrame:
    df[covid_flags_column] = df[date_column_name].apply(
        lambda cell: cell.strip().endswith(")")
    )
    df.index = df[date_column_name].apply(enforce_ons_date_format)
    df.index.name = date_index_name
    df.drop([date_column_name], axis="columns", inplace=True)
    return df.drop([*agg_sector_columns], axis="columns")


NOMIS_2017_SECTOR_EMPLOYMENT_METADATA: Final[MetaData] = MetaData(
    name="NOMIS 2017 English City Employment",
    region="England",
    path=CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME,
    year=2017,
    auto_download=False,
    # name="NOMIS England City Employment",
    # region="England",
    # path=CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME,
    # year=2017,
    # auto_download=False,
    license=OpenGovernmentLicense,
    _package_data=True,
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=CITY_SECTOR_READ_KWARGS,
    # _post_read_func=add_covid_flags_and_drop_agg_sector_columns,
    # _post_read_kwargs=dict(
    #     date_column_name=DATE_COLUMN_NAME,
    #     covid_flags_column=COVID_FLAGS_COLUMN,
    # ),
)

ONS_CONTEMPORARY_JOBS_URL: str = (
    f"{ONS_URL_PREFIX}uri=/employmentandlabourmarket/peopleinwork/"
    "employmentandemployeetypes/datasets/workforcejobsbyregionandindustryjobs05/"
    "current/previous/v26/jobs05jul2021.xls"
)

#     region[covid_flags_column] = region[date_column_name].apply(
#         lambda cell: cell.strip().endswith(")")
#     )
#     region.index = region[date_column_name].apply(enforce_date_format)
#     return region.drop([date_column_name], axis=1)

# def load_employment_by_region_and_sector_csv(
#     path: FilePathType = CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME,
#     skiprows: int = CITY_SECTOR_SKIPROWS,
#     skipfooter: int = CITY_SECTOR_SKIPFOOTER,
#     engine: str = CITY_SECTOR_ENGINE,
#     usecols: Callable[[str], bool] = CITY_SECTOR_USECOLS,
#     index_col: int = CITY_SECTOR_INDEX_COLUMN,
#     **kwargs,
# ) -> DataFrame:
#     """Import region level sector employment data as a DataFrame.
#
#     Todo:
#         * Replace with sources MetaData options
#     """
#     path = path_or_package_data(path, CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME)
#     return read_csv(
#         path,
#         skiprows=skiprows,
#         skipfooter=skipfooter,
#         engine=engine,
#         usecols=usecols,
#         index_col=index_col,
#         **kwargs,
#     )
#

ONS_CONTEMPORARY_JOBS_TIME_SERIES_METADATA: Final[MetaData] = MetaData(
    name="ONS Region Employment Time Series",
    region="England",
    year=2017,
    url=ONS_CONTEMPORARY_JOBS_URL,
    path=UK_JOBS_BY_SECTOR_XLS_FILE_NAME,
    license=OpenGovernmentLicense,
    auto_download=False,
    _package_data=True,
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=dict(
        sheet_name=UK_NATIONAL_EMPLOYMENT_SHEET,
        skiprows=5,
        skipfooter=4,
        # usecols=lambda x: "Unnamed" not in x,
        usecols=CITY_SECTOR_USECOLS,
        dtype={DATE_COLUMN_NAME: str},
    ),
    _post_read_func=add_covid_flags_and_drop_agg_sector_columns,
    _post_read_kwargs=dict(
        date_column_name=DATE_COLUMN_NAME,
        covid_flags_column=COVID_FLAGS_COLUMN,
    ),
)
#
# def load_region_employment_excel(
#     sheet: str = UK_NATIONAL_EMPLOYMENT_SHEET,
#     path: FilePathType = UK_JOBS_BY_SECTOR_XLS_FILE_NAME,
#     date_column_name: str = DATE_COLUMN_NAME,
#     covid_flags_column: str = COVID_FLAGS_COLUMN,
#     **kwargs,
# ) -> DataFrame:
#     """Load regional employment data from https://www.nomisweb.co.uk/ excel exports.
#
#     Todo:
#         * Replace with sources MetaData options
#     """
#     path = path_or_package_data(path, UK_JOBS_BY_SECTOR_XLS_FILE_NAME)
#     region: DataFrame = read_excel(
#         path,
#         sheet_name=sheet,
#         skiprows=5,
#         skipfooter=4,
#         usecols=lambda x: "Unnamed" not in x,
#         dtype={date_column_name: str},
#         **kwargs,
#     )
#     logger.warning(f"Applying NOMIS fixes loading sheet {sheet} from {path}")
#     region[covid_flags_column] = region[date_column_name].apply(
#         lambda cell: cell.strip().endswith(")")
#     )
#     region.index = region[date_column_name].apply(enforce_date_format)
#     return region.drop([date_column_name], axis=1)


CONFIG_2017_QUARTERLY: Final[OrderedDict[date, dict["str", date]]] = OrderedDict(
    {
        date: {"employment_date": date}
        for date in generate_employment_quarterly_dates([2017], reverse=False)
    }
)
EMPLOYMENT_QUARTER_DEC_2017: Final[date] = tuple(CONFIG_2017_QUARTERLY)[-1]
EMPLOYMENT_QUARTER_JUN_2017: Final[date] = tuple(CONFIG_2017_QUARTERLY)[-3]

CONFIG_2015_TO_2017_QUARTERLY: Final[DateConfigType] = OrderedDict(
    {
        date: {"employment_date": date}
        for date in generate_employment_quarterly_dates(
            [2015, 2016, 2017], reverse=False
        )
    }
)
