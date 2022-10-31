#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Final

import pytest
from geopandas import GeoDataFrame
from pandas import DataFrame, Series
from pandas.testing import assert_series_equal

from estios.uk.ons_population_projections import (
    FIRST_YEAR,
    LAST_YEAR,
    NATIONAL_RETIREMENT_AGE,
    ONS_ENGLAND_NAME_CONVERSION_DICT,
    PENSION_AGES,
    RETIREMENT_AGE_INCREASE_YEAR,
    ONSPopulationProjection,
    aggregate_region_by_age_range,
)
from estios.uk.ons_uk_population_projections import (
    ONS_UK_POPULATION_META_DATA,
    get_uk_pop_scaled_all_ages_ts,
    get_uk_pop_scaled_working_ages_ts,
    get_uk_pop_unscaled_projection,
)
from estios.uk.regions import (
    UK_CITY_REGIONS,
    get_all_centre_for_cities_dict,
    load_and_join_centre_for_cities_data,
    load_centre_for_cities_csv,
    load_centre_for_cities_gis,
)

YORK_WORK_POP_2038_TO_2043: Final[Series] = Series(
    {
        "2039": 135791.297,
        "2040": 135645.789,
        "2041": 135647.204,
        "2042": 135679.399,
        "2043": 135692.09,
    },
    name="York",
)

YORK_FULL_POP_2038_TO_2043: Final[Series] = Series(
    {
        "2039": 215801.971,
        "2040": 215869.215,
        "2041": 215993.632,
        "2042": 216137.253,
        "2043": 216313.215,
    },
    name="York",
)

TEST_REGIONS: Final[list[str]] = ["York", "Leeds", "Bristol"]


@pytest.fixture
def ons_2018_years() -> list[int]:
    return list(range(FIRST_YEAR, LAST_YEAR + 1))


class TestLoadingCentreForCitiesData:

    SECTION_OF_COLUMNS: tuple[str, ...] = (
        "Commuting by Bicycle 2001  (%)",
        "Commuting by Bicycle 2011  (%)",
        "Commuting by Bus, Train or Metro 2001  (%)",
        "Commuting by Bus, Train or Metro 2011  (%)",
        "Commuting by Other Methods 2001  (%)",
        "Commuting by Other Methods 2011  (%)",
    )

    def test_load_centre_for_cities_csv(self) -> None:
        """Test loading default Centre for Cities csv from the local package."""
        centre_for_cities: DataFrame = load_centre_for_cities_csv()
        for section in self.SECTION_OF_COLUMNS:
            assert section in centre_for_cities.columns

    def test_load_centre_for_cities_geojson(self) -> None:
        """Test loading Centre for Cities GeoJSON as a GeoDataFrame."""
        cities_geo: GeoDataFrame = load_centre_for_cities_gis()
        assert "Leeds" in cities_geo["NAME1"].values

    def test_load_and_join(self) -> None:
        cities_geo: GeoDataFrame = load_and_join_centre_for_cities_data()
        assert "Leeds" in cities_geo.index
        for section in self.SECTION_OF_COLUMNS:
            assert section in cities_geo.columns


def test_get_all_cities() -> None:
    """Test generating city: region dictionary from Centre for Cities.

    Note:
        * Currently this filters Blackburn, Newcastle and all cities from
        Scotland and Wales,
        * Total English cities 50
    """
    test_dict: dict[str, str] = get_all_centre_for_cities_dict()
    assert len(test_dict) == 48
    for city, region in UK_CITY_REGIONS.items():
        assert test_dict[city] == region


def test_aggregate_region_by_age_range(ons_2018_projection) -> None:
    test_aggregated: DataFrame = aggregate_region_by_age_range(
        ons_2018_projection.age_projections, ons_2018_projection.working_ages
    )
    assert_series_equal(test_aggregated.loc["York"][-5:], YORK_WORK_POP_2038_TO_2043)


def test_retirement_age_dict() -> None:
    for year, age in PENSION_AGES.items():
        if year < RETIREMENT_AGE_INCREASE_YEAR:
            assert age == NATIONAL_RETIREMENT_AGE
        else:
            assert age == NATIONAL_RETIREMENT_AGE + 1


class TestONSEnglandPopulationProjection:

    """Test processing ONSPopulation Projections from 2018."""

    def test_loading_populations(self, ons_2018_projection, ons_2018_years) -> None:
        assert ons_2018_projection.first_trade_year == FIRST_YEAR
        assert ons_2018_projection.last_trade_year == LAST_YEAR
        assert ons_2018_projection.years == ons_2018_years

    def test_loading_year_gap_warning(
        self, ons_2018_projection, ons_2018_years, caplog
    ) -> None:
        test_year: int = LAST_YEAR - 2
        dropped_2043_projection: DataFrame = ons_2018_projection.age_projections.drop(
            columns=str(test_year)
        )
        test_discontinuous_years: ONSPopulationProjection = ONSPopulationProjection(
            age_projections=dropped_2043_projection
        )
        ons_2018_years.remove(test_year)
        assert test_discontinuous_years.first_trade_year == FIRST_YEAR
        assert test_discontinuous_years.last_trade_year == LAST_YEAR
        assert test_discontinuous_years.years == ons_2018_years

    def test_aggregate_leeds(self, ons_york_leeds_bristol_projection) -> None:
        """Test aggregating employment by age for Leeds."""
        assert_series_equal(
            ons_york_leeds_bristol_projection.working_age_projections.loc["York"][-5:],
            YORK_WORK_POP_2038_TO_2043,
        )

    def test_full_population_projection_leeds(
        self, ons_york_leeds_bristol_projection
    ) -> None:
        """Test full population projection for Leeds."""
        assert_series_equal(
            ons_york_leeds_bristol_projection.full_population_projections.loc["York"][
                -5:
            ],
            YORK_FULL_POP_2038_TO_2043,
        )

    def test_specify_york_leeds_working_age(
        self, ons_york_leeds_bristol_projection
    ) -> None:
        """Test working population projection filtered for Leeds and Manchester."""
        assert_series_equal(
            ons_york_leeds_bristol_projection.region_work_population_projections.loc[
                "York"
            ][-5:],
            YORK_WORK_POP_2038_TO_2043,
        )
        assert (
            ons_york_leeds_bristol_projection.region_work_population_projections.index.to_list()
            == TEST_REGIONS
        )

    def test_specify_york_leeds_full_population(
        self, ons_york_leeds_bristol_projection
    ) -> None:
        """Test full population projection filtered for Leeds and Manchester."""
        assert_series_equal(
            ons_york_leeds_bristol_projection.full_population_projections.loc["York"][
                -5:
            ],
            YORK_FULL_POP_2038_TO_2043,
        )
        assert (
            ons_york_leeds_bristol_projection.region_population_projections.index.to_list()
            == TEST_REGIONS
        )

    def test_converted_region_names(self, ons_york_leeds_bristol_projection) -> None:
        """Test Bristol name is converted to ONS index for queries."""
        BRISTOL_ONS_ROW_NAME: str = ONS_ENGLAND_NAME_CONVERSION_DICT["Bristol"]
        CONVERTED_CITY_NAMES = TEST_REGIONS[:-1] + [
            BRISTOL_ONS_ROW_NAME,
        ]
        assert (
            ons_york_leeds_bristol_projection.converted_regions == CONVERTED_CITY_NAMES
        )
        # ons_york_leeds_bristol_projection.regions.append("Bristol")


@pytest.mark.remote_data
class TestONSWholeUKPopulationProjection:

    """Test loading ONS UK population"""

    @classmethod
    def setup_class(cls):
        cls.test_unscaled_projection = get_uk_pop_unscaled_projection()
        # ONS_UK_POPULATION_META_DATA.save_local()

    @classmethod
    def teardown_class(cls):
        ONS_UK_POPULATION_META_DATA.delete_local()

    def test_load(self, caplog):
        """Test importing and processing national population projection."""
        all_ages_ts = self.test_unscaled_projection.loc["All ages"].iloc[0]
        working_age_ts = self.test_unscaled_projection.loc["Working age"].iloc[0]
        assert working_age_ts[-1] == 47449.335
        assert all_ages_ts[-1] == 82461.846

    def test_scaling_working_ages(self, caplog):
        """Test scaling working age results by 1000"""
        working_ages_ts = get_uk_pop_scaled_working_ages_ts()
        assert working_ages_ts[-1] == 47449335.0

    def test_scaling_all_ages(self, caplog):
        """Test scaling all ages results by 1000"""
        all_ages_ts = get_uk_pop_scaled_all_ages_ts()
        assert all_ages_ts[-1] == 82461846.0


class TestONSWorkingPopulation2017:

    """Test extracting regional working population for 2017."""

    def test_2017(self, caplog) -> None:
        pass
