#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Literal

import pytest
from numpy import absolute, exp, log, maximum
from numpy.testing import assert_almost_equal
from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal, assert_series_equal

from estios.calc import (
    I_m,
    S_m,
    X_m,
    calc_ratio,
    calc_region_distances,
    calc_transport_table,
    gross_value_added,
)
from estios.sources import MetaData


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


@pytest.mark.xfail
def test_3_city_distance_table(region_geo_data, three_city_names) -> None:
    """Test distance table calculation for three cities.

    Note:
        * Fails prossing listed example with many non-distance attributes.
    """
    CORRECT_DISTANCES = DataFrame(
        {
            "Leeds": [
                0.0,
                104.05308373,
                58.24977679,
            ],
            "Liverpool": [
                104.05308373,
                0.0,
                49.31390539,
            ],
            "Manchester": [58.24977679, 49.31390539, 0.0],
        },
        index=three_city_names,
    )
    distances: DataFrame = calc_transport_table(region_geo_data, three_city_names)
    assert_series_equal(distances, CORRECT_DISTANCES)


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


# Here is the entropy maximising approach for a known beta.
# Plug in the required values in this function to solve.

POWER_FUCTIONS_NAMES = Literal["power", "pow"]
EXPONENTIAL_FUCTIONS_NAMES = Literal["exponential", "exp"]
COST_FUNCTION_NAMES = Literal[POWER_FUCTIONS_NAMES, EXPONENTIAL_FUCTIONS_NAMES]

balance_doubly_constrained_metadata = MetaData(
    name="Entropy Doubly Constrained",
    authors={
        "Prof Adam Dennett": "https://www.ucl.ac.uk/bartlett/casa/adam-dennett",
        "Daniel Lewis": "https://www.ucl.ac.uk/bartlett/casa/dalien-lewis",
        "Philip Wilkinson": "https://www.ucl.ac.uk/bartlett/casa/philip-wilkinson",
    },
    year=2018,
    url="https://github.com/PhilipDW183/SIM_urbansim/blob/main/Urbsim%20Prac%202.ipynb",
)


def balance_doubly_constrained(
    pd: DataFrame,
    orig_field: str,
    dest_field: str,
    Oi_field: str,
    Dj_field: str,
    cij_field: str,
    beta: float,
    cost_function: COST_FUNCTION_NAMES,
    Ai_name: str = "Ai_new",
    Bj_name: str = "Bj_new",
    converge: float = 0.001,
) -> DataFrame:
    # Define some variables
    Oi = pd[[orig_field, Oi_field]]
    Dj = pd[[dest_field, Dj_field]]
    if cost_function.lower() in ["power", "pow"]:
        beta_cij = exp(beta * log(pd[cij_field]))
    elif cost_function.lower() in ["exponential", "exp"]:
        beta_cij = exp(beta * pd[cij_field])
    else:
        raise ValueError(
            f"Cost function {cost_function} not specified properly, "
            f"only {COST_FUNCTION_NAMES} supported."
        )

    # Create some helper variables
    cnvg = 1
    iteration = 0
    # Now iteratively rebalance the Ai and Bj terms until convergence
    while cnvg > converge:
        if iteration == 0:
            # This first condition sets starting values for Ai and Bj
            # NB sets starting value of Ai assuming Bj is a vector of 1s.
            # We've already established beta_cij with the appropriate cost function, so...
            Oi = Oi.assign(Ai=Dj[Dj_field] * beta_cij)
            # Aggregate Ai and take inverse
            Ai = 1.0 / Oi.groupby(orig_field)["Ai"].sum().to_frame()
            # Merge new Ais
            Oi = Oi.merge(
                Ai, left_on=orig_field, right_index=True, suffixes=("", "_old")
            )
            # Drop the temporary Ai field we created, leaving Ai_old
            Oi.drop("Ai", axis=1, inplace=True)

            # Now set up Bjs using starting values of Ai
            Dj = Dj.assign(Bj=Oi["Ai_old"] * Oi[Oi_field] * beta_cij)
            # Aggregate Bj and take inverse
            Bj = 1.0 / Dj.groupby(dest_field)["Bj"].sum().to_frame()
            # Merge new Bjs
            Dj = Dj.merge(
                Bj, left_on=dest_field, right_index=True, suffixes=("", "_old")
            )
            # Drop the temporary Bj field we created, leaving Bj_old
            Dj.drop("Bj", axis=1, inplace=True)

            # Increment loop
            iteration += 1
        else:
            # This bit is the iterated bit of the loop which refines the values of Ai and Bj
            # First Ai
            Oi["Ai"] = Dj["Bj_old"] * Dj[Dj_field] * beta_cij
            # Aggregate Ai and take inverse
            Ai = 1.0 / Oi.groupby(orig_field)["Ai"].sum().to_frame()
            # Drop temporary Ai
            Oi.drop("Ai", axis=1, inplace=True)
            # Merge new Ais
            Oi = Oi.merge(Ai, left_on=orig_field, right_index=True)
            # Calculate the difference between old and new Ais
            Oi["diff"] = absolute((Oi["Ai_old"] - Oi["Ai"]) / Oi["Ai_old"])
            # Set new Ais to Ai_old
            Oi["Ai_old"] = Oi["Ai"]
            # Drop the temporary Ai field we created, leaving Ai_old
            Oi.drop("Ai", axis=1, inplace=True)

            # Then Bj
            Dj["Bj"] = Oi["Ai_old"] * Oi[Oi_field] * beta_cij
            # Aggregate Bj and take inverse
            Bj = 1.0 / Dj.groupby(dest_field)["Bj"].sum().to_frame()
            # Drop temporary Bj
            Dj.drop("Bj", axis=1, inplace=True)
            # Merge new Bjs
            Dj = Dj.merge(Bj, left_on=dest_field, right_index=True)
            # Calculate the difference between old and new Bjs
            Dj["diff"] = absolute((Dj["Bj_old"] - Dj["Bj"]) / Dj["Bj_old"])
            # Set new Bjs to Bj_old
            Dj["Bj_old"] = Dj["Bj"]
            # Drop the temporary Bj field we created, leaving Bj_old
            Dj.drop("Bj", axis=1, inplace=True)

            # Assign higher sum difference from Ai or Bj to cnvg
            cnvg = maximum(Oi["diff"].sum(), Dj["diff"].sum())

            # Print and increment loop
            print("Iteration:", iteration)
            iteration += 1

    # When the while loop finishes add the computed Ai_old and Bj_old to the dataframe and return
    pd[Ai_name] = Oi["Ai_old"]
    pd[Bj_name] = Dj["Bj_old"]
    return pd

    # config_dict = {
    #     date: {"employment_date": date,
    #            "_national_employmentn":
    #            } for date in quarterly_2017_employment_dates
    # }
    # time_series = InterRegionInputOutputTimeSeries.from_dates(
    #     config_dict, regions=three_cities
    # )
    # time_series[0].employment == False
