#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from numpy.testing import assert_almost_equal
from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal, assert_series_equal

from estios.calc import (
    I_m,
    S_m,
    X_m,
    calc_ratio,
    calc_region_distances,
    gross_value_added,
)


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


def test_X_m_national(three_cities_io, correct_uk_ons_X_m_national) -> None:
    """Test X_m calculation from a national table"""
    national_X_m: Series = X_m(
        full_io_table=three_cities_io.io_table,
        gva=three_cities_io.GVA_m_national,
        net_subsidies=three_cities_io.S_m_national,
    )
    assert_series_equal(national_X_m, correct_uk_ons_X_m_national)


def test_I_m_national(three_cities_io, correct_uk_ons_I_m_national) -> None:
    national_I_m: Series = I_m(
        full_io_table=three_cities_io.io_table,
        investment_column_names=three_cities_io.national_gov_investment_column_names,
        sector_row_names=three_cities_io.sector_names,
    )
    assert_series_equal(national_I_m, correct_uk_ons_I_m_national)
    correct_sum: float = 289997.0
    assert_almost_equal(
        correct_sum, national_I_m.sum() / three_cities_io.io_table_scale, decimal=0
    )


def test_S_m_national(three_cities_io, correct_uk_ons_S_m_national) -> None:
    national_S_m: Series = S_m(
        full_io_table=three_cities_io.io_table,
        subsidy_row_names=three_cities_io.national_net_subsidies_row_name,
        sector_column_names=three_cities_io.sector_names,
    )
    correct_sum: float = 59437.0
    assert_almost_equal(
        correct_sum, national_S_m.sum() / three_cities_io.io_table_scale, decimal=0
    )
    assert_series_equal(national_S_m, correct_uk_ons_S_m_national)


def test_gva_national(three_cities_io, correct_uk_gva_2017) -> None:
    national_gva_m: Series = gross_value_added(
        full_io_table=three_cities_io.io_table,
        gva_row_names=three_cities_io.national_gva_row_name,
        sector_column_names=three_cities_io.sector_names,
    )
    correct_sum: float = 1844010.0
    assert_almost_equal(
        correct_sum, national_gva_m.sum() / three_cities_io.io_table_scale, decimal=0
    )
    assert_series_equal(national_gva_m, correct_uk_gva_2017)


class TestProportionalProjection:

    """Test projecting proportionately"""

    national_populations: Series = Series([i * 1000 for i in range(1, 5)])
    regional_populations: Series = national_populations * 0.4
    national_employment: Series = national_populations * 0.5

    PROJECTED_REGIONAL_EMPLOYMENT: DataFrame = DataFrame(
        {
            "x": [
                1250.09,
                13868658.09,
                13868868.09,
                13869078.09,
            ],
            "y": [
                13868448.09,
                13868658.09,
                13868868.09,
                13869078.09,
            ],
            "z": [
                13868448.09,
                13868658.09,
                13868868.09,
                13869078.09,
            ],
        }
    )

    @pytest.mark.remote_data
    def test_scale_series_by_population(self, pop_history) -> None:
        """Test scaling region by population.

        Todo:
            * Assess whether that tolerance is applicable.
        """
        uk_column: str = "UKPOP"
        country_columns: list[str] = ["NIPOP", "SCPOP", "WAPOP", "ENPOP"]
        projected_country_pops = calc_ratio(
            pop_history[country_columns].loc[2018],
            pop_history[uk_column][2018],
            pop_history[uk_column][2019],
        )
        assert (projected_country_pops > pop_history[country_columns].loc[2018]).all()
        assert_series_equal(
            projected_country_pops,
            pop_history[country_columns].loc[2019].astype("float64"),
            check_exact=False,
            atol=6000.0,
            check_names=False,
        )

    @pytest.mark.xfail
    def test_scale_data_frame(self) -> None:
        """Check scaling a data is a duplicate calc of doctest."""
        test_df: DataFrame = DataFrame(
            {
                "x": self.national_employment,
                "y": self.national_employment * 0.5,
                "z": self.national_employment * 0.2,
            }
        )
        projected_regional_employment_df: DataFrame = calc_ratio(
            test_df, self.regional_populations, self.national_employment
        )
        assert_frame_equal(
            projected_regional_employment_df, self.PROJECTED_REGIONAL_EMPLOYMENT
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
