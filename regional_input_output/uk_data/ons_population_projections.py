#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass, field
from logging import getLogger
from os import PathLike
from pathlib import Path
from typing import Callable, Final, Generator, Iterable, Optional, Union

from pandas import DataFrame

from ..utils import (
    MetaData,
    invert_dict,
    iter_ints_to_list_strs,
    name_converter,
    pandas_from_path_or_package_csv,
    trim_year_range_generator,
)
from .region_names import METROPOLITAN_COUNTIES_ENGLAND

logger = getLogger(__name__)

ONS_POPULATION_PROJECTIONS_FILE_NAME: Final[PathLike] = Path(
    "2018 SNPP Population persons.csv"
)
NAME: Final[str] = "ONS Standard Population Projections"

WORKING_AGE_MINIMUM: Final[int] = 16
NATIONAL_RETIREMENT_AGE: Final[int] = 66

FIRST_YEAR: Final[int] = 2018
LAST_YEAR: Final[int] = 2043
ONS_PROJECTION_YEARS: Final[list[int]] = list(range(FIRST_YEAR, LAST_YEAR + 1))
RETIREMENT_AGE_INCREASE_YEAR: Final[int] = 2028

PENSION_AGES: Final[dict] = {
    year: NATIONAL_RETIREMENT_AGE
    if year < RETIREMENT_AGE_INCREASE_YEAR
    else NATIONAL_RETIREMENT_AGE + 1
    for year in ONS_PROJECTION_YEARS
}

REGION: Final[str] = "England"

REGION_COLUMN_NAME: Final[str] = "AREA_NAME"
AGE_COLUMN_NAME: Final[str] = "AGE_GROUP"
ALL_AGES_ROW_INDEX: Final[str] = "All ages"
AGE_ROW_NAMES_FILTER: Final[list[str]] = ["90 and over", ALL_AGES_ROW_INDEX]
SEX_FILTER_STR: Final[str] = "and SEX == 'persons'"
ALL_AGES_FILTER_STR: Final[
    str
] = f"{AGE_COLUMN_NAME} == '{ALL_AGES_ROW_INDEX}' {SEX_FILTER_STR}"
YOUNGEST_AGE_INT: Final[int] = 0
OLDEST_AGE_INT: Final[int] = 89

METROPOLITAN_COUNTIES_SUFFIX: Final[str] = " (Met County)"

METROPOLITAN_COUNTY_NAMES_WITH_SUFFIX: Final[dict[str, str]] = {
    name: name + METROPOLITAN_COUNTIES_SUFFIX for name in METROPOLITAN_COUNTIES_ENGLAND
}

CITY_AND_COUNTY_NAMES_WITH_SUFFIX: Final[dict[str, str]] = {
    "Bristol": "Bristol, City of",
    "Kingston upon Hull": "Kingston upon Hull, City of",
    "Herefordshire": "Herefordshire, County of",
}


ONS_NAME_CONVERSION_DICT: Final[dict[str, str]] = {
    **METROPOLITAN_COUNTY_NAMES_WITH_SUFFIX,
    **CITY_AND_COUNTY_NAMES_WITH_SUFFIX,
}


ONS_POPULATION_META_DATA: Final[MetaData] = MetaData(
    name=NAME,
    year=FIRST_YEAR,
    region=REGION,
)


def aggregate_region_by_age_range(
    df: DataFrame,
    # min_age: int = WORKING_AGE_MINIMUM,
    # max_age: int = NATIONAL_RETIREMENT_AGE,
    age_range: Iterable[Union[str, int]],
    region_column_name: str = REGION_COLUMN_NAME,
    age_column_name: str = AGE_COLUMN_NAME,
    # drop_row_values: list[str] = AGE_ROW_NAMES_FILTER,
    additional_filter_str: str = SEX_FILTER_STR,
    youngest_age_number: int = YOUNGEST_AGE_INT,
    oldest_age_number: int = OLDEST_AGE_INT,
) -> DataFrame:
    """Return a dataframe aggregating population counts between min and max ages."""
    # return  df.query('AGE_GROUP not in ["90 and over"] and SEX == "persons"').groupby(region_column_name).sum()
    # drop_row_values += [str(age) for age in list(range(youngest_age_number, min_age)) + list(range(max_age, oldest_age_number + 1))]
    # return  df.query(f'{age_column_name} not in {drop_row_values} {additional_filter_str}').groupby(region_column_name).sum()
    age_range = [str(age) for age in age_range]
    return (
        df.query(f"{age_column_name} in {age_range} {additional_filter_str}")
        .groupby(region_column_name)
        .sum()
    )


# df.groupby(region_column_name).query(
#         f"{min_age} <= {age_column_name} <= {max_age}"
#     ).sum()


# def filter_by_query_and_columns(df: DataFrame,
#                                 query: str = ALL_AGES_FILTER_STR,
#                                 columns: list[str] = ONS_PROJECTION_YEARS) -> DataFrame:
#     return df.query(query)[columns]


@dataclass
class PopulationProjection:

    years: list[int] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    age_projections: DataFrame = None
    first_trade_year: Optional[int] = None
    last_trade_year: Optional[int] = None
    min_working_age: int = WORKING_AGE_MINIMUM
    max_working_age: int = NATIONAL_RETIREMENT_AGE
    region_column_name: str = REGION_COLUMN_NAME
    age_column_name: str = AGE_COLUMN_NAME
    additional_person_filter_str: str = ""
    all_ages_query: Union[str, Callable[[DataFrame], DataFrame]] = ALL_AGES_FILTER_STR
    meta_data: Optional[MetaData] = None
    _region_name_mapper: Optional[dict[str, str]] = None
    _youngest_age_int: int = YOUNGEST_AGE_INT
    _oldest_age_int: int = OLDEST_AGE_INT

    def __post_init__(self) -> None:
        """Add a pandas table attribute to managing projections."""
        if self._region_name_mapper:
            logger.debug(f"Using {self._region_name_mapper} to map regions for {self}")
        if not self.years and self.first_trade_year and self.last_trade_year:
            self.years = list(range(self.first_trade_year, self.last_trade_year + 1))
        for year in self.years:
            if (
                year not in self.age_projections.columns
                and str(year) not in self.age_projections.columns
            ):
                logger.warning(
                    f"{year} not in {self.age_projections}, removing from {self}."
                )
                self.years.remove(year)
        if self.first_trade_year is None:
            self.first_trade_year = self.years[0]
        if self.last_trade_year is None:
            self.last_trade_year = self.years[-1]

    def __str__(self) -> str:
        return f"Population projections from {self.first_trade_year} to {self.last_trade_year}"

    # @cashed
    @property
    def working_age_projections(self) -> DataFrame:
        # return self.age_projections[k]
        # for region in self.regions:
        return aggregate_region_by_age_range(
            self.age_projections,
            self.working_ages,
            self.region_column_name,
            self.age_column_name,
            # self.min_working_age,
            # self.max_working_age,
            self.additional_person_filter_str,
            self._youngest_age_int,
            self._oldest_age_int,
        )

    @property
    def years_generator(self) -> Generator[int, None, None]:
        try:
            assert self.first_trade_year and self.last_trade_year
        except AssertionError:
            raise AssertionError(
                f"self.first_trade_year {self.first_trade_year} and "
                f"self.last_trade_year {self.last_trade_year} must be set"
            )
        return trim_year_range_generator(
            self.years, self.first_trade_year, self.last_trade_year
        )

    @property
    def working_ages(self) -> Generator[int, None, None]:
        for age in range(self.min_working_age, self.max_working_age + 1):
            yield age

    @property
    def years_column_names(self) -> list[str]:
        return iter_ints_to_list_strs(self.years_generator)

    @property
    def full_population_projections(self) -> DataFrame:
        if isinstance(self.all_ages_query, str):
            return self.age_projections.query(self.all_ages_query).set_index(
                self.region_column_name
            )[self.years_column_names]
        else:
            return self.all_ages_query(self.age_projections).set_index(
                self.region_column_name
            )[self.years_column_names]

    @property
    def region_work_population_projections(self) -> DataFrame:
        return self._revert_region_name(
            self.working_age_projections.loc[self.converted_regions]
        )

    @property
    def region_population_projections(self) -> DataFrame:
        return self._revert_region_name(
            self.full_population_projections.loc[self.converted_regions]
        )

    # @property
    # def all_regions(self) -> Series:
    #     """Return all possible regions from age_projections attribute."""
    #     return self.age_projections[self.region_column_name].unique()

    @property
    def converted_regions(self) -> list[str]:
        """Return region names with any conversions specified in _region_name_mapper"""
        if self._region_name_mapper:
            return name_converter(self.regions, self._region_name_mapper)
        else:
            logger.info(
                f"{self} ._region_name_mapper not set, returning regions attribute"
            )
            return self.regions

    @property
    def _inverse_region_name_mapper(self) -> Union[list[str], dict[str, str]]:
        if self._region_name_mapper:
            return invert_dict(self._region_name_mapper)
        else:
            logger.info(
                f"No _inverse_region_name_mapper set, returning regions from {self}."
            )
            return self.regions

    def _revert_region_name(self, df: DataFrame) -> DataFrame:
        """Return DataFrame with indexes consistent with self.regions.

        Todo:
            * Refactor as a decorator
        """
        if self._region_name_mapper:
            return df.rename(self._inverse_region_name_mapper)
        else:
            return df


@dataclass
class ONSPopulationProjection(PopulationProjection):

    """ONS data file customisation of PopulationProjection class."""

    first_trade_year: Optional[int] = FIRST_YEAR
    last_trade_year: Optional[int] = LAST_YEAR
    ons_path: Optional[PathLike] = ONS_POPULATION_PROJECTIONS_FILE_NAME
    meta_data: Optional[MetaData] = ONS_POPULATION_META_DATA
    additional_person_filter_str: str = SEX_FILTER_STR
    _region_name_mapper: Optional[dict[str, str]] = field(
        default_factory=lambda: ONS_NAME_CONVERSION_DICT
    )
    _pandas_file_reader: Callable[
        [PathLike, PathLike], DataFrame
    ] = pandas_from_path_or_package_csv

    def __post_init__(self) -> None:
        """Enfroce default values for ONS data."""
        if self.age_projections is None and self.ons_path:
            self.age_projections: DataFrame = self._pandas_file_reader(
                self.ons_path, ONS_POPULATION_PROJECTIONS_FILE_NAME
            )
        super().__post_init__()
