#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import INFO
from typing import Final, Sequence

import pytest
from geopandas import GeoDataFrame
from pandas import DataFrame, Series
from pandas.testing import assert_series_equal

from estios.uk.gdp_projections import (
    OECD_GDP_LONG_TERM_FORCASTS,
    get_uk_gdp_ts_as_series,
)
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
from estios.uk.sector_codes import get_uk_io_codes

YORK_WORK_POP_2038_TO_2043: Final[Series] = Series(
    {
        "2039": 137670.789,
        "2040": 137520.358,
        "2041": 137517.412,
        "2042": 137545.814,
        "2043": 137555.029,
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


@pytest.mark.remote_data
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


@pytest.mark.remote_data
class TestONSEnglandPopulationProjection:

    """Test processing ONSPopulation Projections from 2018."""

    def test_loading_populations(self, ons_2018_projection, ons_2018_years) -> None:
        assert ons_2018_projection.first_trade_year == FIRST_YEAR
        assert ons_2018_projection.last_trade_year == LAST_YEAR
        assert ons_2018_projection.years == ons_2018_years

    def test_loading_year_gap(self, ons_2018_projection, ons_2018_years) -> None:
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

    """Test extracting regional working population for 2017.

    Todo:
        * Expand this in refactoring to remove:
            ons_population_projections.PopulationProjections
    """

    def test_2017(self, caplog) -> None:
        pass


@pytest.mark.remote_data
class TestGDPProjections:

    """Test extracting OECD UK GDP projections calculated via PPP to pounds."""

    CORRECT_2021_CONV_RATE: Series = Series(
        {2017: 1987401.5049337712, 2020: 1840171.1632555155, 2025: 2190735.5306760003}
    )
    CORRECT_2010_CONV_RATE: Series = Series(
        {2017: 2014484.3620965388, 2020: 1865247.6727810295, 2025: 2220589.275534}
    )
    CONV_RATE_2021: float = 0.692802
    CONV_RATE_2010: float = 0.702243

    def gen_rate_logs(
        self,
        to_years: Sequence[int] = CORRECT_2021_CONV_RATE.index,
        from_years: Sequence[int] | int = 2021,
        rates: Sequence[float] | float = CONV_RATE_2021,
    ) -> list[str]:
        if isinstance(from_years, int):
            from_years = [from_years] * len(to_years)
        if isinstance(rates, float):
            rates = [rates] * len(to_years)
        assert len(to_years) == len(from_years) == len(rates)
        return [
            self.get_rate_log(from_year, to_year, rate)
            for from_year, to_year, rate in zip(from_years, to_years, rates)
        ]

    def get_rate_log(self, from_year: int, to_year: int, rate: float) -> str:
        return f"Using {rate} converter rate from {from_year} for {to_year}"

    def test_ppp_default_converter(self, caplog) -> None:
        """Test using OECD PPP converter from dollars to pounds per year."""
        caplog.set_level(INFO)
        gdp_ts: Series = get_uk_gdp_ts_as_series()
        assert_series_equal(gdp_ts, self.CORRECT_2021_CONV_RATE)
        assert caplog.messages == self.gen_rate_logs()

    @pytest.mark.xfail
    def test_ppp_converter_vary_by_year(self, caplog) -> None:
        """Test using OECD PPP converter for all dates provided."""
        caplog.set_level(INFO)
        gdp_ts: Series = get_uk_gdp_ts_as_series(
            years=OECD_GDP_LONG_TERM_FORCASTS.dates, use_constant_rate=False
        )
        assert_series_equal(
            gdp_ts[self.CORRECT_2021_CONV_RATE.index], self.CORRECT_2021_CONV_RATE
        )
        assert (gdp_ts.index.values == OECD_GDP_LONG_TERM_FORCASTS.dates).all()
        for log in self.gen_rate_logs():
            assert log in caplog.messages
        assert len(caplog.messages) == 2060 - 1990

    def test_ppp_converter_to_2017(self, caplog, three_cities_io) -> None:
        """Test with constant 2010 dollars to pounds rate."""
        caplog.set_level(INFO)
        # gdp_ts: Series = get_uk_gdp_ts_as_series(years=OECD_GDP_LONG_TERM_FORCASTS.dates)
        gdp_ts: Series = get_uk_gdp_ts_as_series(
            years=OECD_GDP_LONG_TERM_FORCASTS.dates,
            approximation_year=2010,
            use_constant_rate=True,
        )
        assert_series_equal(
            gdp_ts[self.CORRECT_2010_CONV_RATE.index], self.CORRECT_2010_CONV_RATE
        )
        assert (gdp_ts.index.values == OECD_GDP_LONG_TERM_FORCASTS.dates).all()
        for log in self.gen_rate_logs(rates=self.CONV_RATE_2010, from_years=2010):
            assert log in caplog.messages
        assert len(caplog.messages) == 2060 - 1989


def test_uk_io_codes() -> None:
    input_codes, output_codes = get_uk_io_codes()
    assert input_codes["CPA_S96"] == "Other personal services"
    assert output_codes["CPA_S96"] == "Other personal services"
    assert input_codes["GVA"] == "Gross value added"
    assert output_codes["P62"] == "Exports of services"
