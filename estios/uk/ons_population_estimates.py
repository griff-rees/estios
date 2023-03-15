#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from typing import Final

from pandas import Series

from ..sources import MetaData, OpenGovernmentLicense, pandas_from_path_or_package

FIRST_YEAR: Final[int] = 2001
LAST_YEAR: Final[int] = 2020
URL: Final[str] = (
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/"
    "populationandmigration/populationestimates/datasets/"
    "populationestimatesforukenglandandwalesscotlandandnorthernireland/"
    "mid2020/ukpopestimatesmid2020on2021geography.xls"
)


ONS_CONTEMPORARY_POPULATION_META_DATA: Final[MetaData] = MetaData(
    name="UK ONS Contemporary Population Estimates",
    year=2020,
    canonical_date=date(2020, 6, 1),
    description=(
        "National and subnational mid-year population "
        "estimates for the UK and its constituent countries "
        "by administrative area, age and sex (including "
        "components of population change, median age and "
        "population density). The mid-2001 to mid-2019 "
        "detailed time-series contains the latest available "
        "mid-year population estimates and components of "
        "change from mid-2019 back to mid-2001."
    ),
    info_url=(
        "https://www.ons.gov.uk/peoplepopulationandcommunity/"
        "populationandmigration/populationestimates/datasets/"
        "populationestimatesforukenglandandwalesscotlandandnorthernireland"
    ),
    date_created=date(2021, 6, 25),
    dates=list(range(FIRST_YEAR, LAST_YEAR + 1)),
    region="UK",
    other_regions=(
        "Scotland",
        "England",
        "Great Britain",
        "Wales",
        "England and Wales",
        "Northern Ireland",
    ),
    url=URL,
    # path=ONS_UK_2018_FILE_NAME,
    auto_download=True,
    license=OpenGovernmentLicense,
    dict_key_appreviation="uk_contemporary",
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=dict(
        sheet_name="MYE4",
        index_col="Name",
        skiprows=7,
    ),
)

ONS_2017_URL: Final[str] = (
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/"
    "populationandmigration/populationestimates/datasets/"
    "populationestimatesforukenglandandwalesscotlandandnorthernireland/"
    "mid2017/ukmidyearestimates2017finalversion.xls"
)

ONS_2017_POPULATION_META_DATA: Final[MetaData] = MetaData(
    name="UK ONS 2017 Population Estimates",
    year=2017,
    info_url=(
        "https://www.ons.gov.uk/peoplepopulationandcommunity/"
        "populationandmigration/populationestimates/datasets/"
        "populationestimatesforukenglandandwalesscotlandandnorthernireland"
    ),
    # date_created=date(2021, 6, 25),
    # dates=list(range(FIRST_YEAR, LAST_YEAR + 1)),
    region="UK",
    other_regions=(
        "Scotland",
        "England",
        "Great Britain",
        "Wales",
        "England and Wales",
        "Northern Ireland",
    ),
    url=ONS_2017_URL,
    # path=ONS_UK_2018_FILE_NAME,
    auto_download=True,
    license=OpenGovernmentLicense,
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=dict(
        sheet_name="MYE2 - All",
        index_col="Code",
        skiprows=4,
    ),
)

ONS_2017_ALL_AGES_COLUMN_NAME = "All ages"
