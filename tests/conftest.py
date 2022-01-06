#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pytest

from regional_input_output.input_output_models import InterRegionInputOutput
from regional_input_output.uk_data.utils import (
    CITY_REGIONS,
    generate_employment_quarterly_dates,
)

THREE_CITIES: tuple[str, str, str] = ("Manchester", "Leeds", "Liverpool")


@pytest.fixture
def three_cities() -> dict[str, str]:
    return {
        city: region for city, region in CITY_REGIONS.items() if city in THREE_CITIES
    }


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
