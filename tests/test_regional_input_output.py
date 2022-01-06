from datetime import date
from logging import INFO

import pytest
from pandas import DataFrame, MultiIndex, Series
from pandas.testing import assert_series_equal

from regional_input_output import __version__
from regional_input_output.input_output_models import (
    InterRegionInputOutput,
    InterRegionInputOutputTimeSeries,
)
from regional_input_output.uk_data.utils import (
    CITY_REGIONS,
    EMPLOYMENT_QUARTER_DEC_2017,
    SECTOR_10_CODE_DICT,
    get_all_centre_for_cities_dict,
)


@pytest.fixture
def all_cities() -> dict[str, str]:
    return get_all_centre_for_cities_dict()


@pytest.fixture
def all_cities_io(all_cities: dict[str, str]) -> InterRegionInputOutput:
    return InterRegionInputOutput(regions=all_cities)


def test_version() -> None:
    """Keep track of library version."""
    assert __version__ == "0.1.0"


class TestInputOutputModel:

    """Test constructing and running a 3 city InterRegionInputOutput model."""

    def test_default_construction(self) -> None:
        io_model = InterRegionInputOutput()
        assert str(io_model) == "Input output model of 2017: 10 sectors, 10 cities"

    def test_3_city_construction(self, three_cities) -> None:
        io_model = InterRegionInputOutput(regions=three_cities)
        assert str(io_model) == "Input output model of 2017: 10 sectors, 3 cities"

    def test_3_city_distances(self, three_cities_io) -> None:
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
        assert_series_equal(three_cities_io.distances["Distance"], CORRECT_DISTANCES)

    def test_3_city_singly_constrained(self, three_cities_results) -> None:
        CORRECT_Q_i_m = [
            40,
            22035,
            12000,
            71000,
            15000,
            21000,
            4500,
            76000,
            88000,
            14000,
            40,
            22035,
            12000,
            71000,
            15000,
            21000,
            4500,
            76000,
            88000,
            14000,
            10,
            18960,
            8000,
            75000,
            7000,
            10000,
            5000,
            40000,
            94000,
            15500,
            10,
            18960,
            8000,
            75000,
            7000,
            10000,
            5000,
            40000,
            94000,
            15500,
            75,
            13880,
            9000,
            88000,
            14000,
            20000,
            10000,
            96000,
            100000,
            18000,
            75,
            13880,
            9000,
            88000,
            14000,
            20000,
            10000,
            96000,
            100000,
            18000,
        ]
        B_j_m_im_column: str = "B_j^m * Q_i^m * exp(-β c_{ij})"
        CORRECT_B_j_m_Q_im_distance = [
            0.345347,
            0.610933,
            0.568745,
            0.443837,
            0.514507,
            0.509459,
            0.308007,
            0.439162,
            0.465360,
            0.434808,
            0.799714,
            0.537060,
            0.599571,
            0.485855,
            0.681430,
            0.677029,
            0.473239,
            0.654769,
            0.483070,
            0.474131,
            0.116699,
            0.575108,
            0.468307,
            0.457848,
            0.331301,
            0.331301,
            0.331301,
            0.292219,
            0.482248,
            0.460410,
            0.200286,
            0.462940,
            0.400429,
            0.514145,
            0.318570,
            0.322971,
            0.526761,
            0.345231,
            0.516930,
            0.525869,
            0.883301,
            0.424892,
            0.531693,
            0.542152,
            0.668699,
            0.668699,
            0.668699,
            0.707781,
            0.517752,
            0.539590,
            0.654653,
            0.389067,
            0.431255,
            0.556163,
            0.485493,
            0.490541,
            0.691993,
            0.560838,
            0.534640,
            0.565192,
        ]
        CORRECT_y_ij_m_df = DataFrame(
            {
                "Q_i^m": CORRECT_Q_i_m,
                "B_j^m": None,
                "exp(-β c_{ij})": None,
                B_j_m_im_column: CORRECT_B_j_m_Q_im_distance,
            },
            index=three_cities_results._ij_m_index,
        )
        assert_series_equal(
            CORRECT_y_ij_m_df["Q_i^m"], three_cities_results._y_ij_m["Q_i^m"]
        )
        assert_series_equal(
            CORRECT_y_ij_m_df[B_j_m_im_column],
            three_cities_results._y_ij_m[B_j_m_im_column],
        )


class TestInputOutputModelAllCities:

    """Test results for 48 cities in England over 10 aggregated sectors."""

    def test_all_city_construction(self, all_cities_io) -> None:
        assert str(all_cities_io) == "Input output model of 2017: 10 sectors, 48 cities"

    def test_all_city_distances(self, all_cities_io) -> None:
        CORRECT_HEAD_DISTANCES = Series(
            [
                256.638308,
                103.219690,
                122.786283,
                107.799854,
                316.213934,
            ],
            index=all_cities_io.distances.head().index,
            name="Distance",
        )
        CORRECT_TAIL_DISTANCES = Series(
            [
                169.575114,
                41.272173,
                118.191104,
                111.975798,
                353.505924,
            ],
            index=all_cities_io.distances.tail().index,
            name="Distance",
        )
        assert_series_equal(
            all_cities_io.distances["Distance"].head(), CORRECT_HEAD_DISTANCES
        )
        assert_series_equal(
            all_cities_io.distances["Distance"].tail(), CORRECT_TAIL_DISTANCES
        )


class TestInputOutputTimeSeries:

    """Test a TimeSeries of InputOutput models."""

    def test_one_time_point(self) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = InterRegionInputOutputTimeSeries.from_dates(employment_date_list)
        assert len(time_series) == 1

    def test_two_dupe_time_points(self) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = InterRegionInputOutputTimeSeries.from_dates(employment_date_list)
        assert len(time_series) == 2
        assert len(time_series[1:]) == 1

    def test_two_dupe_time_points_append(self, three_cities_io) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = InterRegionInputOutputTimeSeries.from_dates(employment_date_list)
        time_series.append(three_cities_io)
        assert len(time_series) == 3
        assert len(time_series[1:]) == 2
        assert list(time_series.years) == 3 * [EMPLOYMENT_QUARTER_DEC_2017.year]

    def test_pass_1_io_model(self, three_cities_io, caplog) -> None:
        caplog.set_level(INFO)
        time_series = InterRegionInputOutputTimeSeries(
            io_models=[
                three_cities_io,
            ]
        )
        assert len(time_series) == 1
        assert list(time_series.years) == [EMPLOYMENT_QUARTER_DEC_2017.year]
        assert str(time_series) == (
            "Input output models from 2017-12-01 to 2017-12-01: 10 sectors, 3 cities"
        )

    def test_pass_2_io_models(self, three_cities_io, caplog) -> None:
        caplog.set_level(INFO)
        time_series = InterRegionInputOutputTimeSeries(
            io_models=[three_cities_io, three_cities_io]
        )
        assert len(time_series) == 2
        assert list(time_series.years) == [
            EMPLOYMENT_QUARTER_DEC_2017.year,
            EMPLOYMENT_QUARTER_DEC_2017.year,
        ]
        assert str(time_series) == (
            "Input output models from 2017-12-01 to 2017-12-01: 10 sectors, 3 cities"
        )

    def test_2017_quarters(
        self, quarterly_2017_employment_dates, three_cities, caplog
    ) -> None:
        config_dict = {
            date: {"employment_date": date} for date in quarterly_2017_employment_dates
        }
        time_series = InterRegionInputOutputTimeSeries.from_dates(
            config_dict, regions=three_cities
        )
        assert time_series[0].date == date(2017, 3, 1)
        assert time_series.dates.index(time_series[0].date) == 0
        assert len(time_series) == 4
        assert str(time_series) == (
            "Input output models from 2017-03-01 to 2017-12-01: 10 sectors, 3 cities"
        )
        time_series.calc_models()
        for model in time_series:
            assert hasattr(model, "y_ij_m_model")
