#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Final

import pytest
from geopandas import GeoDataFrame
from pandas import DataFrame, Series
from pandas.testing import assert_series_equal

from regional_input_output.uk_data.ons_population_projections import (
    FIRST_YEAR,
    LAST_YEAR,
    NATIONAL_RETIREMENT_AGE,
    ONS_NAME_CONVERSION_DICT,
    PENSION_AGES,
    RETIREMENT_AGE_INCREASE_YEAR,
    ONSPopulationProjection,
    aggregate_region_by_age_range,
)
from regional_input_output.uk_data.regions import (
    UK_CITY_REGIONS,
    get_all_centre_for_cities_dict,
    load_and_join_centre_for_cities_data,
    load_centre_for_cities_csv,
    load_centre_for_cities_gis,
)

YORK_WORK_POP_2038_TO_2043: Final[Series] = Series(
    {
        "2039": 139990.391,
        "2040": 139748.718,
        "2041": 139659.302,
        "2042": 139565.108,
        "2043": 139595.587,
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
def ons_2018_projection() -> ONSPopulationProjection:
    return ONSPopulationProjection(regions=TEST_REGIONS)


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


class TestONSPopulationProjection:
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

    def test_aggregate_leeds(self, ons_2018_projection) -> None:
        """Test aggregating employment by age for Leeds."""
        assert_series_equal(
            ons_2018_projection.working_age_projections.loc["York"][-5:],
            YORK_WORK_POP_2038_TO_2043,
        )

    def test_full_population_projection_leeds(self, ons_2018_projection) -> None:
        """Test full population projection for Leeds."""
        assert_series_equal(
            ons_2018_projection.full_population_projections.loc["York"][-5:],
            YORK_FULL_POP_2038_TO_2043,
        )

    def test_specify_york_leeds_working_age(self, ons_2018_projection) -> None:
        """Test working population projection filtered for Leeds and Manchester."""
        assert_series_equal(
            ons_2018_projection.region_work_population_projections.loc["York"][-5:],
            YORK_WORK_POP_2038_TO_2043,
        )
        assert (
            ons_2018_projection.region_work_population_projections.index.to_list()
            == TEST_REGIONS
        )

    def test_specify_york_leeds_full_population(self, ons_2018_projection) -> None:
        """Test full population projection filtered for Leeds and Manchester."""
        assert_series_equal(
            ons_2018_projection.full_population_projections.loc["York"][-5:],
            YORK_FULL_POP_2038_TO_2043,
        )
        assert (
            ons_2018_projection.region_population_projections.index.to_list()
            == TEST_REGIONS
        )

    def test_converted_region_names(self, ons_2018_projection) -> None:
        """Test Bristol name is converted to ONS index for queries."""
        BRISTOL_ONS_ROW_NAME: str = ONS_NAME_CONVERSION_DICT["Bristol"]
        CONVERTED_CITY_NAMES = TEST_REGIONS[:-1] + [
            BRISTOL_ONS_ROW_NAME,
        ]
        assert ons_2018_projection.converted_regions == CONVERTED_CITY_NAMES
        # ons_2018_projection.regions.append("Bristol")
