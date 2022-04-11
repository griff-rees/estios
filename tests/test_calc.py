#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from pandas import DataFrame, Series
from pandas.testing import assert_series_equal

from regional_input_output.calc import calc_city_distances


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
    distances: DataFrame = calc_city_distances(
        three_cities_io.region_data, three_cities_io.region_names
    )
    assert_series_equal(distances["Distance"], CORRECT_DISTANCES)
