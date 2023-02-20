#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Generator

import pytest
from pandas import DataFrame, Series

from estios.input_output_tables import InputOutputCPATable
from estios.models import InterRegionInputOutput, InterRegionInputOutputTimeSeries
from estios.sources import MetaData, MonthDay
from estios.temporal import annual_io_time_series
from estios.uk.employment import generate_employment_quarterly_dates
from estios.uk.ons_population_projections import (
    ONS_ENGLAND_POPULATION_META_DATA,
    ONS_PROJECTION_YEARS,
    ONSPopulationProjection,
)
from estios.uk.ons_uk_population_history import ONS_UK_POPULATION_HISTORY_META_DATA
from estios.uk.regions import get_all_centre_for_cities_dict
from estios.uk.utils import load_contemporary_ons_population
from estios.utils import THREE_UK_CITY_REGIONS


@pytest.fixture
def three_cities() -> dict[str, str]:
    return THREE_UK_CITY_REGIONS


@pytest.fixture
def three_city_names(three_cities) -> list[str]:
    return three_cities.keys()


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
    return annual_io_time_series(years=ONS_PROJECTION_YEARS, regions=three_cities)


@pytest.fixture
def three_cities_2018_2020(three_cities) -> InterRegionInputOutputTimeSeries:
    return annual_io_time_series(years=range(2018, 2021), regions=three_cities)


@pytest.fixture
def ons_cpa_io_table() -> InputOutputCPATable:
    return InputOutputCPATable()


@pytest.fixture
def month_day() -> MonthDay:
    return MonthDay()


# @pytest.fixture(scope="session") doesn't seem to speed up...
@pytest.fixture(scope="session")
def pop_projection(tmp_path_factory) -> Generator[MetaData, None, None]:
    """Extract ONS population projection for testing and remove when concluded."""
    pop_projection: MetaData = ONS_ENGLAND_POPULATION_META_DATA
    # pop_projection.auto_download = True
    pop_projection._package_data = False
    pop_projection.set_folder(tmp_path_factory.mktemp("test-session"))
    pop_projection.save_local()
    yield pop_projection
    pop_projection.delete_local()


@pytest.fixture
def ons_2018_projection(pop_projection, three_cities) -> ONSPopulationProjection:
    return ONSPopulationProjection(regions=three_cities, meta_data=pop_projection)


@pytest.fixture
def york_leeds_bristol() -> list[str]:
    return ["York", "Leeds", "Bristol"]


@pytest.fixture
def ons_york_leeds_bristol_projection(
    pop_projection, york_leeds_bristol
) -> ONSPopulationProjection:
    return ONSPopulationProjection(regions=york_leeds_bristol, meta_data=pop_projection)


@pytest.fixture
def pop_history(tmp_path_factory) -> Generator[DataFrame, None, None]:
    """Extract ONS population history to test and remove when concluded."""
    pop_history: MetaData = ONS_UK_POPULATION_HISTORY_META_DATA
    pop_history.set_folder(tmp_path_factory.mktemp("test-session"))
    pop_history.save_local()
    yield pop_history.read()
    pop_history.delete_local()


@pytest.fixture
def pop_recent() -> DataFrame:
    return load_contemporary_ons_population()


@pytest.fixture
def correct_uk_ons_X_m_national(three_cities_io) -> Series:
    return Series(
        [
            272997328683.096,
            5564510362211.610,
            2953222552470.090,
            6052276697689.360,
            1797685549558.830,
            2350643301666.180,
            3410315660836.110,
            4206339454407.310,
            4977015624637.480,
            933480385688.2780,
        ],
        index=three_cities_io.sectors,
    )


@pytest.fixture
def correct_uk_ons_I_m_national(three_cities_io) -> Series:
    return Series(
        [
            11829310350.839594,
            234754157531.46796,
            1623614765544.0493,
            125750040000.00002,
            394630091779.0043,
            0.0,
            86780010000.0,
            415841120000.0001,
            11130020000.0,
            -4360010000.0,
        ],
        index=three_cities_io.sectors,
    )


@pytest.fixture
def correct_uk_ons_S_m_national(three_cities_io) -> Series:
    return Series(
        [
            5219879538.987354,
            48726630179.360085,
            65923156176.74704,
            109917023800.91006,
            5657846676.360364,
            126155859356.99503,
            15693516329.211025,
            21591371070.04223,
            173752907187.63373,
            21733979683.75351,
        ],
        index=three_cities_io.sectors,
    )


@pytest.fixture
def correct_uk_gva_2017(three_cities_io) -> Series:
    return Series(
        [
            107415864611.57884,
            2440789852881.2056,
            1202628202044.299,
            3390433769651.8784,
            1131618902061.9211,
            1162302626594.8616,
            2597532933265.886,
            2551229010509.971,
            3229760696230.9946,
            626388142147.4033,
        ],
        index=three_cities_io.sectors,
    )


@pytest.fixture
def correct_uk_national_employment_2017(three_cities_io) -> Series:
    return Series(
        [
            422000,
            3129000,
            2330000,
            9036000,
            1459000,
            1114000,
            589000,
            6039000,
            8756000,
            1989000,
        ],
        dtype="int64",
        index=three_cities_io.sectors,
    )
