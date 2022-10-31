from typing import Final, Optional

from pandas import DataFrame, Series

from ..utils import MetaData, OpenGovernmentLicense, pandas_from_path_or_package
from .ons_population_estimates import ONS_2017_ALL_AGES_COLUMN_NAME

FIRST_YEAR: Final[int] = 2018
LAST_YEAR: Final[int] = 2118
URL: Final[str] = (
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/"
    "populationandmigration/populationprojections/datasets/"
    "tablea11principalprojectionuksummary/2018based/ukpppsummary18.xls"
)
POPULATION_SCALING_FACTOR: Final[int] = 1000

INDEX_COL: Final[str] = "Ages"
SHEET_NAME: Final[str] = "PERSONS"
WORKING_AGE_COLUMN_NAME: Final[str] = "Working age"

ONS_UK_POPULATION_META_DATA: Final[MetaData] = MetaData(
    name="UK ONS Population Projection",
    year=2018,
    dates=list(range(FIRST_YEAR, LAST_YEAR + 1)),
    region="UK",
    url=URL,
    description=(
        "Principal projection for the UK including population "
        "by broad age group, components of change and summary "
        "statistics."
    ),
    # path=ONS_UK_2018_FILE_NAME,
    license=OpenGovernmentLicense,
    auto_download=False,
    needs_scaling=True,
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=dict(sheet_name=SHEET_NAME, skiprows=33, index_col=INDEX_COL),
)


def get_uk_pop_unscaled_projection(
    ons_uk_pop_projections: MetaData = ONS_UK_POPULATION_META_DATA,
) -> DataFrame:
    ons_uk_pop_projections.save_local()
    return ons_uk_pop_projections.read()


def scale_nth_of_row_name(
    pop_projection: Optional[DataFrame],
    row_name: str = ONS_2017_ALL_AGES_COLUMN_NAME,
    scaling_factor: float = POPULATION_SCALING_FACTOR,
    ons_uk_pop_meta_data: MetaData = ONS_UK_POPULATION_META_DATA,
    n: int = 0,
) -> Series:
    if pop_projection is None:
        pop_projection = get_uk_pop_unscaled_projection(ons_uk_pop_meta_data)
    assert pop_projection is not None
    return pop_projection.loc[row_name].iloc[n] * scaling_factor


def get_uk_pop_scaled_all_ages_ts(
    pop_projection: Optional[DataFrame] = None,
    all_ages_column_name: str = ONS_2017_ALL_AGES_COLUMN_NAME,
) -> Series:
    return scale_nth_of_row_name(pop_projection, all_ages_column_name)


def get_uk_pop_scaled_working_ages_ts(
    pop_projection: Optional[DataFrame] = None,
    working_age_col_name: str = WORKING_AGE_COLUMN_NAME,
) -> Series:
    return scale_nth_of_row_name(pop_projection, working_age_col_name)
