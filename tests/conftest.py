#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Generator

import pytest

from estios.models import InterRegionInputOutput, InterRegionInputOutputTimeSeries
from estios.uk_data.employment import generate_employment_quarterly_dates
from estios.uk_data.ons_population_projections import (
    ONS_POPULATION_META_DATA,
    ONS_PROJECTION_YEARS,
)
from estios.uk_data.regions import get_all_centre_for_cities_dict
from estios.utils import THREE_UK_CITY_REGIONS, MetaData, MonthDay


@pytest.fixture
def three_cities() -> dict[str, str]:
    return THREE_UK_CITY_REGIONS


@pytest.fixture
def three_cities_io(three_cities: dict[str, str]) -> InterRegionInputOutput:
    return InterRegionInputOutput(regions=three_cities)


@pytest.fixture
def three_cities_results(
    three_cities_io: InterRegionInputOutput,
) -> InterRegionInputOutput:
    three_cities_io.import_export_convergence()
    return three_cities_io


@pytest.fixture
def quarterly_2017_employment_dates():
    return generate_employment_quarterly_dates(
        [
            2017,
        ]
    )


@pytest.fixture
def all_cities() -> dict[str, str]:
    return get_all_centre_for_cities_dict()


@pytest.fixture
def all_cities_io(all_cities: dict[str, str]) -> InterRegionInputOutput:
    return InterRegionInputOutput(regions=all_cities)


@pytest.fixture
def three_cities_2018_2043(three_cities) -> InterRegionInputOutputTimeSeries:
    return InterRegionInputOutputTimeSeries.from_years(
        years=ONS_PROJECTION_YEARS, regions=three_cities
    )


@pytest.fixture
def three_cities_2018_2020(three_cities) -> InterRegionInputOutputTimeSeries:
    return InterRegionInputOutputTimeSeries.from_years(
        years=range(2018, 2021), regions=three_cities
    )


@pytest.fixture
def month_day() -> MonthDay:
    return MonthDay()


# @pytest.fixture(scope="session") doesn't seem to speed up...
@pytest.fixture(scope="session")
def pop_projection(tmp_path_factory) -> Generator[MetaData, None, None]:
    """Extract ONS population projection for testing and remove when concluded."""
    pop_projection: MetaData = ONS_POPULATION_META_DATA
    pop_projection.set_folder(tmp_path_factory.mktemp("test-session"))
    pop_projection.save_local()
    yield pop_projection
    pop_projection.delete_local()
