#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandas import DataFrame, Series
from pandas.testing import assert_series_equal

from regional_input_output.calc import calc_region_distances, scale_by_population


def test_3_city_distances(three_cities_io) -> None:
    """Test distance column calculation for three cities."""
    CORRECT_DISTANCES = Series(
        [
            104.05308373,
            58.24977679,
            104.05308373,
            49.31390539,
            58.24977679,
            49.31390539,
        ],
        index=three_cities_io.distances.index,
        name="Distance",
    )
    distances: DataFrame = calc_region_distances(
        three_cities_io.region_data, three_cities_io.region_names
    )
    assert_series_equal(distances["Distance"], CORRECT_DISTANCES)


def test_scale_by_population(three_cities, quarterly_2017_employment_dates) -> None:
    """Test scaling region by population."""
    CORRECT_REGIONAL_EMPLOYMENT_TS: Series = Series(
        [
            13868448.09,
            13868658.09,
            13868868.09,
            13869078.09,
        ]
    )
    national_population_ts: Series = Series([i * 1000 + 66040229 for i in range(4)])
    regional_population_ts: Series = national_population_ts * 0.3
    national_employment_ts: Series = national_population_ts * 0.7

    estimated_regional_employment_ts: Series = scale_by_population(
        national_population_ts, regional_population_ts, national_employment_ts
    )
    assert_series_equal(
        estimated_regional_employment_ts, CORRECT_REGIONAL_EMPLOYMENT_TS
    )
    # config_dict = {
    #     date: {"employment_date": date,
    #            "_national_employmentn":
    #            } for date in quarterly_2017_employment_dates
    # }
    # time_series = InterRegionInputOutputTimeSeries.from_dates(
    #     config_dict, regions=three_cities
    # )
    # time_series[0].employment == False
