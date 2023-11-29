#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from typing import Final

from pandas import DataFrame

from ..sources import MetaData, OpenGovernmentLicense, pandas_from_path_or_package

FIRST_YEAR: Final[int] = 1971
LAST_YEAR: Final[int] = 2019
URL: Final[str] = (
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/"
    "populationandmigration/populationestimates/datasets/"
    "populationestimatestimeseriesdataset/current/pop.csv"
)
POPULATION_SCALING_FACTOR: Final[int] = 1000

ONS_UK_POPULATION_HISTORY_META_DATA: Final[MetaData] = MetaData(
    name="UK ONS Population History",
    year=2019,
    description=(
        "The mid-year estimates refer to the population "
        "on 30 June of the reference year and are produced "
        "in line with the standard UN definition for "
        "population estimates. They are the official "
        "set of population estimates for the UK and "
        "its constituent countries, the regions and "
        "counties of England, and local authorities "
        "and their equivalents."
    ),
    info_url=(
        "https://www.ons.gov.uk/peoplepopulationandcommunity/"
        "populationandmigration/populationestimates/datasets/"
        "populationestimatestimeseriesdataset/current"
    ),
    date_created=date(2021, 6, 21),
    dates=list(range(FIRST_YEAR, LAST_YEAR + 1)),
    region="UK",
    other_regions=(
        "Scotland",
        "England",
        "Great Britain",
        "Wales",
        "England and Wales",
    ),
    url=URL,
    # path=ONS_UK_2018_FILE_NAME,
    auto_download=True,
    license=OpenGovernmentLicense,
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=dict(  # skiprows=range(2, 6),
        # index_col="Title")
        index_col="CDID",
        skiprows=[0, *range(2, 7)],
        # quotechar='"',
    ),
)


def get_uk_population_history(year: int = 2017, region_column: str = "GBPOP"):
    """Return uk population for passed `year`."""
    ONS_UK_POPULATION_HISTORY_META_DATA.save_local()
    df: DataFrame = ONS_UK_POPULATION_HISTORY_META_DATA.read()
    return df.loc[year, region_column]


UK_NATIONAL_POPULATION_2017: int = get_uk_population_history(year=2017)
