#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import DEBUG
from typing import Literal

import pytest
from numpy import absolute, exp, log, maximum
from numpy.testing import assert_almost_equal
from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal, assert_series_equal

from estios.calc import (
    E_i_m_scaled,
    E_i_m_scaled_by_regions,
    F_i_m_scaled,
    F_i_m_scaled_by_regions,
    FloatOrPandasTypes,
    I_m,
    M_i_m_scaled,
    M_i_m_scaled_by_regions,
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
    national_X_m: Series | DataFrame = X_m(
        full_io_table=three_cities_io.io_table,
        gva=three_cities_io.GVA_m_national,
        net_subsidies=three_cities_io.S_m_national,
    )
    assert_series_equal(national_X_m, correct_uk_ons_X_m_national)


def test_I_m_national(three_cities_io, correct_uk_ons_I_m_national) -> None:
    national_I_m: Series | DataFrame = I_m(
        full_io_table=three_cities_io.io_table,
        investment_column_names=three_cities_io.national_gov_investment_column_names,
        sector_row_names=three_cities_io.sector_names,
    )
    assert_series_equal(national_I_m, correct_uk_ons_I_m_national)
    # Replacing originaly calculated correct_sum
    # correct_sum: float = 289997.0
    correct_sum: float = 303141.0
    assert_almost_equal(
        correct_sum, national_I_m.sum() / three_cities_io.io_table_scale, decimal=0
    )


def test_S_m_national(three_cities_io, correct_uk_ons_S_m_national) -> None:
    national_S_m: Series | DataFrame = S_m(
        full_io_table=three_cities_io.io_table,
        subsidy_row_names=three_cities_io.national_net_subsidies_row_name,
        sector_column_names=three_cities_io.sector_names,
    )
    # Replacing originaly calculated correct_sum
    # correct_sum: float = 59437.0
    correct_sum: float = 61448.0
    assert_almost_equal(
        correct_sum, national_S_m.sum() / three_cities_io.io_table_scale, decimal=0
    )
    assert_series_equal(national_S_m, correct_uk_ons_S_m_national)


def test_F_m_leeds(three_cities_io, correct_leeds_2017_final_demand) -> None:
    leeds_F_m: Series | DataFrame = F_i_m_scaled(
        final_demand=three_cities_io.national_final_demand,
        regional_populations=three_cities_io.regional_populations["Leeds"],
        national_population=three_cities_io.national_population,
    )
    manchester_F_m: Series | DataFrame = F_i_m_scaled(
        final_demand=three_cities_io.national_final_demand,
        regional_populations=three_cities_io.regional_populations["Manchester"],
        national_population=three_cities_io.national_population,
    )
    leeds_calc_example: Series | DataFrame = calc_ratio(
        three_cities_io.national_final_demand,
        three_cities_io.national_population,
        three_cities_io.regional_populations["Leeds"],
    ).astype(float)
    assert_frame_equal(leeds_F_m, leeds_calc_example)
    assert (leeds_F_m <= manchester_F_m).all().all()
    assert (manchester_F_m <= three_cities_io.national_final_demand).all().all()
    assert_frame_equal(leeds_F_m, correct_leeds_2017_final_demand)


def test_regional_F_i_m(three_cities_io, correct_leeds_2017_final_demand) -> None:
    # Prior to refactor Final Demand Manchester > Leeds
    # correct_manchester_gt_leeds_trade_not_household: Series = Series(
    #     [True, True, True, True, True, False, True, True, True, True],
    #     index=three_cities_io.sector_names,
    # )

    scaled_F_i_m_regions: Series | DataFrame = F_i_m_scaled_by_regions(
        final_demand=three_cities_io.national_final_demand,
        regional_populations=three_cities_io.regional_populations,
        national_population=three_cities_io.national_population,
        sector_row_names=three_cities_io.sector_names,
    )
    assert (
        scaled_F_i_m_regions.index.levels[0].values == three_cities_io.region_names
    ).all()
    assert set(three_cities_io.sector_names) == set(
        scaled_F_i_m_regions.index.levels[1].values
    )
    assert_frame_equal(
        scaled_F_i_m_regions.loc["Leeds"], correct_leeds_2017_final_demand
    )
    assert (
        (
            scaled_F_i_m_regions.loc["Leeds"]["Household Purchase"]
            <= scaled_F_i_m_regions.loc["Manchester"]["Household Purchase"]
        )
        .all()
        .all()
    )
    # for column in three_cities_io.final_demand_column_names[1:]:
    #     assert (
    #         (
    #             scaled_F_i_m_regions.loc["Leeds"][column]
    #             < scaled_F_i_m_regions.loc["Manchester"][column]
    #         )
    #         == correct_manchester_gt_leeds_trade_not_household
    #     ).all()
    assert (
        (
            scaled_F_i_m_regions.loc["Manchester"]
            <= three_cities_io.national_final_demand
        )
        .all()
        .all()
    )


def test_M_m_leeds(three_cities_io, correct_leeds_2017_imports) -> None:
    leeds_M_m: Series | DataFrame = M_i_m_scaled(
        imports=three_cities_io.national_imports,
        regional_populations=three_cities_io.regional_populations["Leeds"],
        national_population=three_cities_io.national_population,
    )
    manchester_M_m: Series | DataFrame = M_i_m_scaled(
        imports=three_cities_io.national_imports,
        regional_populations=three_cities_io.regional_populations["Manchester"],
        national_population=three_cities_io.national_population,
    )
    leeds_calc_example: Series | DataFrame = calc_ratio(
        three_cities_io.national_imports,
        three_cities_io.national_population,
        three_cities_io.regional_populations["Leeds"],
    ).astype(float)
    assert_series_equal(leeds_M_m, leeds_calc_example)
    assert (leeds_M_m < manchester_M_m).all().all()
    assert (manchester_M_m < three_cities_io.national_imports).all().all()
    assert_series_equal(leeds_M_m, correct_leeds_2017_imports)


def test_regional_M_i_m(three_cities_io, correct_leeds_2017_imports) -> None:
    """Test M_i_m, case of a single column rathern that multiple categories.

    Todo:
        * Fix indexing disparities on last commented line
    """
    scaled_M_i_m_regions: Series | DataFrame = M_i_m_scaled_by_regions(
        imports=three_cities_io.national_imports,
        regional_populations=three_cities_io.regional_populations,
        national_population=three_cities_io.national_population,
        sector_row_names=three_cities_io.sector_names,
    )
    assert (scaled_M_i_m_regions.index.values == three_cities_io.region_names).all()
    assert set(three_cities_io.sector_names) == set(scaled_M_i_m_regions.columns)
    assert (
        scaled_M_i_m_regions.loc["Leeds"] < scaled_M_i_m_regions.loc["Manchester"]
    ).all()
    assert (
        scaled_M_i_m_regions.loc["Manchester"] < three_cities_io.national_imports
    ).all()
    # To be fixed, currently returns `Sector`, above returns `Imports`
    # assert_series_equal(scaled_M_i_m_regions.loc["Leeds"], correct_leeds_2017_imports)


@pytest.mark.remote_data
def test_E_m_leeds(three_cities_io, correct_leeds_2017_exports, caplog) -> None:
    caplog.set_level(DEBUG)
    leeds_E_m: Series | DataFrame = E_i_m_scaled(
        exports=three_cities_io.national_exports,
        regional_employment=three_cities_io.regional_employment.loc["Leeds"],
        national_employment=three_cities_io.national_employment,
    )
    manchester_E_m: Series | DataFrame = E_i_m_scaled(
        exports=three_cities_io.national_exports,
        regional_employment=three_cities_io.regional_employment.loc["Manchester"],
        national_employment=three_cities_io.national_employment,
    )
    leeds_calc_example: Series | DataFrame = calc_ratio(
        three_cities_io.national_exports,
        three_cities_io.national_employment,
        three_cities_io.regional_employment.loc["Leeds"],
    ).astype(float)
    assert_frame_equal(leeds_E_m, leeds_calc_example)
    assert (leeds_E_m <= manchester_E_m).all().all()
    assert (manchester_E_m <= three_cities_io.national_exports).all().all()
    assert_frame_equal(leeds_E_m, correct_leeds_2017_exports)


@pytest.mark.remote_data
def test_regional_E_i_m(three_cities_io, correct_leeds_2017_exports) -> None:
    scaled_E_i_m_regions: Series | DataFrame = E_i_m_scaled_by_regions(
        exports=three_cities_io.national_exports,
        regional_employment=three_cities_io.regional_employment,
        national_employment=three_cities_io.national_employment,
        sector_row_names=three_cities_io.sector_names,
    )
    assert (
        scaled_E_i_m_regions.index.levels[0].values == three_cities_io.region_names
    ).all()
    assert set(three_cities_io.sector_names) == set(
        scaled_E_i_m_regions.index.levels[1].values
    )
    assert_frame_equal(scaled_E_i_m_regions.loc["Leeds"], correct_leeds_2017_exports)


def test_gva_national(three_cities_io, correct_uk_gva_2017) -> None:
    national_gva_m: Series | DataFrame = gross_value_added(
        full_io_table=three_cities_io.io_table,
        gva_row_names=three_cities_io.national_gva_row_name,
        sector_column_names=three_cities_io.sector_names,
    )
    correct_sum: float = 1942619.0
    assert_almost_equal(
        correct_sum, national_gva_m.sum() / three_cities_io.io_table_scale, decimal=0
    )
    assert_series_equal(national_gva_m, correct_uk_gva_2017)


@pytest.mark.xfail
def test_3_city_distance_table(region_geo_data, three_city_names) -> None:
    """Test distance table calculation for three cities.

    Note:
        * Fails processing listed example with many non-distance attributes.
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
        projected_regional_employment_df: FloatOrPandasTypes = calc_ratio(
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
