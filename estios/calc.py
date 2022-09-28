#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger
from typing import Callable, Final, Iterable, Optional, Union

from geopandas import GeoDataFrame
from pandas import DataFrame, MultiIndex, Series

from .input_output_tables import SECTOR_10_CODE_DICT, TOTAL_OUTPUT_COLUMN_NAME
from .uk_data.regions import UK_CITY_REGIONS, UK_EPSG_GEO_CODE
from .utils import CITY_COLUMN, OTHER_CITY_COLUMN, generate_i_m_index, generate_ij_index

logger = getLogger(__name__)

DISTANCE_UNIT_DIVIDE: Final[float] = 1000
METRES_TO_KILOMETERS: Final[float] = 0.001

CITY_POPULATION_COLUMN_NAME: Final[str] = "Q_i^m"

DISTANCE_COLUMN: Final[str] = "Distance"
DISTANCE_COLUMN_SUFFIX: Final[str] = "_Point"

LATEX_e_i_m: Final[str] = "e_i^m"
LATEX_m_i_m: Final[str] = "m_i^m"
LATEX_y_ij_m: Final[str] = "y_{ij}^m"

INITIAL_E_COLUMN_PREFIX: str = "initial "

INITIAL_P: Final[float] = 0.1  # For initial e_m_iteration e calculation
DEFAULT_IMPORT_EXPORT_ITERATIONS: Final[int] = 15


def technical_coefficients(
    io_table: DataFrame,
    sectors: Iterable[str] = SECTOR_10_CODE_DICT.keys(),
    final_output_column: str = TOTAL_OUTPUT_COLUMN_NAME,
) -> DataFrame:
    """Calculate technical coefficients from IO matrix and a final output column."""
    io_matrix: DataFrame = io_table.loc[sectors, sectors]
    final_output: Series = io_table.loc[sectors][final_output_column]
    return (io_matrix / final_output).astype("float64")


def X_i_m(
    total_sales: Series, employment: DataFrame, national_employment: DataFrame
) -> DataFrame:
    """Return the total production of sector 𝑚 in region 𝑖and cache results.

    X_i^m = X_*^m * Q_i^m/Q_*^m
    """
    return total_sales * employment / national_employment


def M_i_m(
    imports: Series, employment: DataFrame, national_employment: DataFrame
) -> DataFrame:
    """Return the imports of sector 𝑚 in region 𝑖and cache results.

    M_i^m = M_*^m * Q_i^m/Q_*^m
    """
    return imports * employment / national_employment


def F_i_m(
    final_demand: Series, employment: DataFrame, national_employment: DataFrame
) -> DataFrame:
    """Return the final demand of sector 𝑚 in region 𝑖and cache results.

    F_i^m = F_*^m * Q_i^m/Q_*^m
    """
    return final_demand * employment / national_employment


def E_i_m(
    exports: Series, employment: DataFrame, national_employment: DataFrame
) -> DataFrame:
    """Return the final demand of sector 𝑚 in region 𝑖and cache results.

    E_i^m = E_*^m * Q_i^m/Q_*^m
    """
    return exports * employment / national_employment


def x_i_mn_summed(X_i_m: DataFrame, technical_coefficients: DataFrame) -> DataFrame:
    """Return sum of all total demands for good 𝑚 in region 𝑖.

    Equation 1:
    x_i^{mn} = a_i^{mn}X_i^n

    Equation 2:

        .. math::
            X_i^m + m_i^m + M_i^m = F_i^m + e_i^m + E_i^m + \\sum_n{a_i^{mn}X_i^n}

    Note: the \\s is to avoid a docstring warning, and should have a single \
    """
    return X_i_m.apply(
        lambda row: (row * technical_coefficients.T).sum(),
        axis=1,
    )


def generate_e_m_dataframe(
    E_i_m: DataFrame,
    initial_p: float = INITIAL_P,
    national_E: Optional[Series] = None,
    region_names: Iterable[str] = UK_CITY_REGIONS,
    sector_names: Iterable[str] = SECTOR_10_CODE_DICT,
    e_i_m_column_name: str = LATEX_e_i_m,
    initial_e_column_prefix: str = INITIAL_E_COLUMN_PREFIX,
) -> DataFrame:
    """Return an e_m dataframe with an intial e_i^m column."""
    index: MultiIndex
    if national_E:
        E_i_m = E_i_m.append(national_E)
        index = generate_i_m_index(region_names, sector_names, include_national=True)
    else:
        index = generate_i_m_index(region_names, sector_names)
    initial_e_column_name: str = initial_e_column_prefix + e_i_m_column_name
    e_m_iter_df = DataFrame(index=index, columns=[initial_e_column_name])
    e_m_iter_df[initial_e_column_name] = initial_p * E_i_m.stack().astype("float64")
    return e_m_iter_df


def calc_region_distances(
    cities_df: GeoDataFrame,
    regions: Iterable[str] = UK_CITY_REGIONS,
    other_regions: Optional[Iterable[str]] = None,
    distance_CRS: str = UK_EPSG_GEO_CODE,
    origin_region_column: str = CITY_COLUMN + DISTANCE_COLUMN_SUFFIX,
    destination_region_column: str = OTHER_CITY_COLUMN + DISTANCE_COLUMN_SUFFIX,
    final_distance_column: str = DISTANCE_COLUMN,
    unit_divide_conversion: float = DISTANCE_UNIT_DIVIDE,
) -> GeoDataFrame:
    """Calculate a GeoDataFrame with a Distance column between cities in metres.

    The ``rest_uk`` boolean adds a generic term for the rest of
    Note: This assumes the cities_df index has origin region as row.name[0],
    and destination region as row.name[].
    """
    if not other_regions:
        other_regions = regions
    projected_cities_df = cities_df.to_crs(distance_CRS)
    region_distances: GeoDataFrame = GeoDataFrame(
        index=generate_ij_index(regions, other_regions), columns=[final_distance_column]
    )
    region_distances[origin_region_column] = region_distances.apply(
        lambda row: projected_cities_df["geometry"][row.name[0]], axis=1
    )
    region_distances[destination_region_column] = region_distances.apply(
        lambda row: projected_cities_df["geometry"][row.name[1]], axis=1
    )
    region_distances[final_distance_column] = region_distances.apply(
        lambda row: row[origin_region_column].distance(row[destination_region_column])
        / unit_divide_conversion,
        axis=1,
    )
    region_distances = region_distances.drop(
        region_distances[region_distances[final_distance_column] == 0].index
    )
    return region_distances


# def calc_region_distances(
#     cities_df: GeoDataFrame,
#     regions: Iterable[str] = UK_CITY_REGIONS,
#     other_regions: Optional[Iterable[str]] = None,
#     distance_CRS: str = UK_EPSG_GEO_CODE,
#     origin_region_column: str = CITY_COLUMN + DISTANCE_COLUMN_SUFFIX,
#     destination_region_column: str = OTHER_CITY_COLUMN + DISTANCE_COLUMN_SUFFIX,
#     final_distance_column: str = DISTANCE_COLUMN,
#     unit_divide_conversion: float = DISTANCE_UNIT_DIVIDE,
# ) -> GeoDataFrame:


def centroid_distance_table(
    region_df: GeoDataFrame,
) -> DataFrame:
    """Return a table of region_df centroid distances divided by."""
    return region_df.centroid.apply(
        lambda origin_region: region_df.centroid.distance(origin_region)
    )


def calc_transport_table(
    regions_df: GeoDataFrame,
    region_names: Optional[Iterable[str]] = None,
    distance_CRS: Optional[str] = UK_EPSG_GEO_CODE,
    distance_func: Callable[[GeoDataFrame], DataFrame] = centroid_distance_table,
    scaling_factor: float = METRES_TO_KILOMETERS,
    region_names_column: Optional[str] = None,
) -> DataFrame:
    """Return a distance matrix calculated by `distance_func`.

    Note:
        * `region_names_index_or_column_name` assumes `index` is simply
        the DataFrame index."""
    if distance_CRS:
        regions_df = regions_df.to_crs(distance_CRS)
    if region_names_column:
        regions_df.set_index(region_names_column)
    if region_names:
        regions_df[region_names]
    return distance_func(regions_df) * scaling_factor


def doubly_constrained(regions_df: DataFrame) -> DataFrame:
    pass


# def doubly_constrained(regions_df: GeoDataFrame) -> GeoDataFrame:
#     doubly_constrained_df = regions_df.copy()
#     # create some Oi and Dj columns in the dataframe and store row and column totals in them:
#     # to create O_i, take cdatasub ...then... group by origcodenew ...then... summarise by calculating the sum of Total
#     # O_i <- cdatasub %>% group_by(OrigCodeNew) %>% summarise(O_i = sum(Total))
#     # cdatasub$O_i <- O_i$O_i[match(cdatasub$OrigCodeNew,O_i$OrigCodeNew)]
#     # D_j <- cdatasub %>% group_by(DestCodeNew) %>% summarise(D_j = sum(Total))
#     # cdatasub$D_j <- D_j$D_j[match(cdatasub$DestCodeNew,D_j$DestCodeNew)]
#     doubly_constrained_df["O_i"] = doubly_constrained_df.groupby(["Orig"])[
#         "Total"
#     ].transform("sum")
#     doubly_constrained_df["D_j"] = doubly_constrained_df.groupby(["Dest"])[
#         "Total"
#     ].transform("sum")
#
#     # if(tail(names(coef(doubSim)),1)=="dist"):
#     #     doubly_constrained_df$beta = coef(doubSim)["dist"]
#     #     disdecay = 0
#     # else:
#     #     doubly_constrained_df$beta = coef(doubSim)["log(dist)"]
#     #     disdecay = 1
#     if not log_dist:
#         beta = poisson_model_dist.params["dist"]
#         disdecay = False
#     else:
#         beta = poisson_model_log_dist.params["log_dist"]
#         disdecay = True
#     # Create some new Ai and Bj columns and fill them with starting values
#     # doubly_constrained_df$Ai <- 1
#     # doubly_constrained_df$Bj <- 1
#     # doubly_constrained_df$OldAi <- 10
#     # doubly_constrained_df$OldBj <- 10
#     # doubly_constrained_df$diff <- abs((doubly_constrained_df$OldAi-doubly_constrained_df$Ai)/boroughs_df$OldAi)
#     doubly_constrained_df["Ai"] = 1
#     doubly_constrained_df["Bj"] = 1
#     doubly_constrained_df["OldAi"] = 10
#     doubly_constrained_df["OldBj"] = 10
#     doubly_constrained_df["Ai_diff"] = abs(
#         (doubly_constrained_df["OldAi"] - doubly_constrained_df["Ai"])
#         / boroughs_df["OldAi"]
#     )
#     # create convergence and iteration variables and give them initial values
#     cnvg = 1
#     its = 0
#     # This is a while-loop which will calculate Orig and Dest balancing
#     # factors until the specified convergence criteria is met
#     while cnvg > 0.001:
#         print("iteration ", its)
#         its += 1  # increment the iteration counter by 1
#         # First some initial calculations for Ai...
#         if not disdecay:
#             doubly_constrained_df["Ai"] = (
#                 doubly_constrained_df["Bj"]
#                 * doubly_constrained_df["D_j"]
#                 * exp(boroughs_df["dist"] * beta)
#             )
#         else:
#             doubly_constrained_df["Ai"] = (
#                 doubly_constrained_df["Bj"]
#                 * doubly_constrained_df["D_j"]
#                 * exp(log(boroughs_df["log_dist"] * beta))
#             )
#         # aggregate the results by your Origs and store in a new dataframe
#         # AiBF <- aggregate(Ai ~ Orig, data = cdatasub, sum)
#         AiBF_df: DataFrame = DataFrame(
#             data={"Ai_denom": doubly_constrained_df.groupby(["Orig"])["Ai"].sum()}
#         )
#         # now divide by 1
#         # AiBF$Ai <- 1/AiBF$Ai
#         AiBF_df["Ai"] = 1 / AiBF_df["Ai_denom"]
#         print("AiBF_df:", AiBF_df)
#         # and replace the initial values with the new balancing factors
#         # cdatasub$Ai = ifelse(!is.na(updates), updates, cdatasub$Ai)
#         updates: DataFrame = doubly_constrained_df.merge(
#             AiBF_df, how="outer", left_on="Orig", right_index=True
#         )["Ai_y"]
#         if not updates.isnull().values.any():
#             doubly_constrained_df["Ai"] = updates
#         # now, if not the first iteration, calculate the difference between
#         # the new Ai values and the old Ai values and once done, overwrite
#         # the old Ai values with the new ones.
#         # if(its==1){
#         #    cdatasub$OldAi <- cdatasub$Ai
#         # } else {
#         # cdatasub$diff <- abs((cdatasub$OldAi-cdatasub$Ai)/cdatasub$OldAi)
#         # cdatasub$OldAi <- cdatasub$Ai
#         # }
#         if its == 0:
#             doubly_constrained_df["OldAi"] = doubly_constrained_df["Ai"]
#         else:
#             doubly_constrained_df["diff"] = abs(
#                 (doubly_constrained_df["OldAi"] - doubly_constrained_df["Ai"])
#                 / boroughs_df["OldAi"]
#             )
#             print("Ai diff:", doubly_constrained_df["diff"])
#             doubly_constrained_df["OldAi"] = doubly_constrained_df["Ai"]
#         # Now some similar calculations for Bj...
#         # if(disdecay==0){
#         #   cdatasub$Bj <- (cdatasub$Ai*cdatasub$O_i*exp(cdatasub$dist*cdatasub$beta))
#         # } else {
#         #   cdatasub$Bj <- (cdatasub$Ai*cdatasub$O_i*exp(log(cdatasub$dist)*cdatasub$beta))
#         # }
#         if not disdecay:
#             doubly_constrained_df["Bj"] = (
#                 doubly_constrained_df["Ai"]
#                 * doubly_constrained_df["O_i"]
#                 * exp(boroughs_df["dist"] * beta)
#             )
#         else:
#             doubly_constrained_df["Bj"] = (
#                 doubly_constrained_df["Ai"]
#                 * doubly_constrained_df["O_i"]
#                 * exp(log(boroughs_df["log_dist"] * beta))
#             )
#         # #aggregate the results by your Dests and store in a new dataframe
#         # BjBF <- aggregate(Bj ~ Dest, data = cdatasub, sum)
#         BjBF_df: DataFrame = DataFrame(
#             data={"Bj_denom": doubly_constrained_df.groupby(["Dest"])["Bj"].sum()}
#         )
#         # #now divide by 1
#         # BjBF$Bj <- 1/BjBF$Bj
#         BjBF_df["Bj"] = 1 / BjBF_df["Bj_denom"]
#         print("BjBF_df:", BjBF_df)
#         # #and replace the initial values by the balancing factor
#         # updates = BjBF[match(cdatasub$Dest,BjBF$Dest),"Bj"]
#         # cdatasub$Bj = ifelse(!is.na(updates), updates, cdatasub$Bj)
#         updates: DataFrame = doubly_constrained_df.merge(
#             BjBF_df, how="outer", left_on="Dest", right_index=True
#         )["Bj_y"]
#         if not updates.isnull().values.any():
#             doubly_constrained_df["Bj"] = updates
#         # #now, if not the first iteration, calculate the difference between the new Bj values and the old Bj values and once done, overwrite the old Bj values with the new ones.
#         # if(its==1){
#         # cdatasub$OldBj <- cdatasub$Bj
#         # } else {
#         # cdatasub$diff <- abs((cdatasub$OldBj-cdatasub$Bj)/cdatasub$OldBj)
#         # cdatasub$OldBj <- cdatasub$Bj
#         # }
#         if its == 1:
#             doubly_constrained_df["OldBj"] = doubly_constrained_df["Bj"]
#         else:
#             doubly_constrained_df["diff"] = abs(
#                 (doubly_constrained_df["OldBj"] - doubly_constrained_df["Bj"])
#                 / boroughs_df["OldBj"]
#             )
#             doubly_constrained_df["OldBj"] = doubly_constrained_df["Bj"]
#         # #overwrite the convergence variable with
#         # cnvg = sum(cdatasub$diff)
#         cnvg = sum(doubly_constrained_df["diff"])
#         print("Converge:", cnvg)
#
#     print(doubly_constrained_df[["OldAi", "Ai", "OldBj", "Bj", "diff"]])
#     return doubly_constrained_df


def andrews_suggestion(
    F_i_m: DataFrame,
    E_i_m: DataFrame,
    x_i_mn_summed: DataFrame,
    X_i_m: DataFrame,
    M_i_m: DataFrame,
    employment: DataFrame,
) -> Series:
    """Implementation of Andrew's suggstion for enforcing constraints."""
    exogenous_i_m_constant: Series = (
        F_i_m.stack()
        + E_i_m.stack()
        + x_i_mn_summed.stack()
        - X_i_m.stack()
        - M_i_m.stack()
    )

    exogenous_i_m_constant.index.set_names(["City", "Sector"], inplace=True)

    # Equation 14
    # (Rearranged equation 2)
    # m_i^m = e_i^m + F_i^m + E_i^m + \sum_n{a_i^{mn}X_i^n} - X_i^m - M_i^m
    # exogenous_i_m_constant = F_i^m + E_i^m + \sum_n{a_i^{mn}X_i^n} - X_i^m - M_i^m
    # convergence_by_region = Q_i/\sum_j{Q_j} * \sum_i{exogenous_i_m_constant_i}
    # Convergence element
    # c_1 = Q_i/\sum_j{Q_j} * \sum_i{exogenous_i_m_constant_i}
    # convergence_by_region = Q_i/\sum_j{Q_j} * \sum_i{exogenous_i_m_constant_i}
    # Possibility I've messed up needing to sum the other employment (ie i != j)
    convergence_by_sector: Series = exogenous_i_m_constant.groupby("Sector").apply(
        lambda sector: employment[sector.name]
        * sector.sum()
        / employment[sector.name].sum()
        # groupby within.?
    )

    # Need to replace Area with City in future
    convergence_by_region: Series = convergence_by_sector.reorder_levels(
        ["Area", "Sector"]
    )
    convergence_by_region = convergence_by_region.reindex(exogenous_i_m_constant.index)

    # from pdb import set_trace; set_trace()

    net_constraint: Series = exogenous_i_m_constant - convergence_by_region
    # This accounts for economic activity outside the 3 cities included in the model enforcing convergence
    # net_constraint: Series = exogenous_i_m_constant - convergence_by_region
    return exogenous_i_m_constant, convergence_by_region, net_constraint


def import_export_convergence(
    e_m_cities: DataFrame,
    y_ij_m: DataFrame,
    exogenous_i_m: Series,
    # employment: DataFrame,
    iterations: int = DEFAULT_IMPORT_EXPORT_ITERATIONS,
    e_i_m_symbol: str = LATEX_e_i_m,
    m_i_m_symbol: str = LATEX_m_i_m,
    y_ij_m_symbol: str = LATEX_y_ij_m,
) -> tuple[DataFrame, DataFrame]:
    """Iterate i times of step 2 (eq 14, 15 18) of the spatial interaction model."""
    model_e_m: DataFrame = e_m_cities.copy()
    model_y_ij_m: DataFrame = y_ij_m.copy()

    for i in range(iterations):
        e_column: str = (
            f"{e_i_m_symbol} {i - 1}" if i > 0 else f"initial {e_i_m_symbol}"
        )

        # Equation 14 with exogenous_i_m_constant
        # Possibility I've messed up needing to sum the other employment (ie i != j)
        # m_i^m = e_i^m + exogenous_i_m_constant - convergence_by_region
        model_e_m[f"{m_i_m_symbol} {i}"] = model_e_m[e_column] + exogenous_i_m

        # Equation 15
        # y_{ij}^m = B_j^m Q_i^m m_j^m \exp(-\beta c_{ij})
        # Note: this groups by Other City and Sector
        model_y_ij_m[f"{y_ij_m_symbol} {i}"] = model_y_ij_m.apply(
            lambda row: row["B_j^m * Q_i^m * exp(-β c_{ij})"]
            * model_e_m[f"{m_i_m_symbol} {i}"][row.name[1]][row.name[2]],
            axis=1,
        )
        logger.info(f"Iteration {i}")
        logger.debug(model_y_ij_m[f"{y_ij_m_symbol} {i}"].head())
        logger.debug(model_y_ij_m[f"{y_ij_m_symbol} {i}"].tail())

        # Equation 18
        # e_i^m = \sum_j{y_{ij}^m}
        # Note: this section groups by City and Sector
        model_e_m[f"{e_i_m_symbol} {i}"] = (
            model_y_ij_m[f"{y_ij_m_symbol} {i}"].groupby(["City", "Sector"]).sum()
        )
    return model_e_m, model_y_ij_m


def scale_region_var_by_national(
    national_var: Union[float, Series],
    national_sector_var: Union[float, Series],
    region_var: Union[float, Series],
) -> Union[float, Series]:
    return national_sector_var * region_var / national_var


# def scale_var_by_national(
#     var: Union[float, Series],
#     national_var: Union[float, Series],
#     national_portion: Union[float, Series],
# ) -> Union[float, Series]:
#     return national_proportion * var / national_var


def proportional_projection(
    var_last_state: Union[float, Series],
    # last_var_date: date,
    proportion_var_last_state: Union[float, Series],
    proportion_var_next_state: Union[float, Series],
) -> Union[float, Series]:
    return var_last_state * proportion_var_last_state / proportion_var_next_state


# def diagonalise(series: Series) -> DataFrame:
#     return DataFrame(diag(series),index=series.index,columns=series.index)


def regional_io_projection(
    technical_coefficients: DataFrame, regional_output: Series
) -> DataFrame:
    """Return a regional projection of from regional data.

    Todo:
        * Test an option using diagonalise
    """
    # return technical_coefficients * diagonalise(regional_output)
    return technical_coefficients * regional_output