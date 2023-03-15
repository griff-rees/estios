#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import wraps
from logging import getLogger
from typing import Callable, Final, Iterable, Optional, Sequence

from geopandas import GeoDataFrame
from pandas import DataFrame, MultiIndex, Series

from .uk.regions import UK_EPSG_GEO_CODE
from .utils import (
    CITY_COLUMN,
    OTHER_CITY_COLUMN,
    REGION_COLUMN_NAME,
    SECTOR_COLUMN_NAME,
    df_dict_to_multi_index,
    dtype_wrapper,
    generate_i_m_index,
    generate_ij_index,
    ordered_iter_overlaps,
    series_dict_to_multi_index,
    wrap_as_series,
)

logger = getLogger(__name__)

FloatOrPandasTypes = float | Series | DataFrame
FloatOrSeriesType = float | Series

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


@dtype_wrapper("float64")
def technical_coefficients(
    io_table: DataFrame,
    final_output_column: str | Sequence[str],
    sectors: Iterable[str],
) -> DataFrame:
    """Calculate technical coefficients from IO matrix and a final output column.

    Todo:
        * Assess whether sectors filtering potentially leads to errors
        * Constrain parameters if necessary
    """
    io_matrix: DataFrame = io_table.loc[sectors, sectors]
    final_output: Series | DataFrame = io_table.loc[sectors, final_output_column]
    if not isinstance(final_output, Series):
        final_output = final_output.sum(axis="columns")
    # return (io_matrix / final_output).astype("float64")
    return io_matrix / final_output


@dtype_wrapper("float64")
def X_i_m_scaled(
    total_production: Series, employment: DataFrame, national_employment: Series
) -> DataFrame:
    """Estimate total production of sector $m$ in region $i$.

    $X_i^{(m)} = X_*^{(m)} * Q_i^{(m)}/Q_*^{(m)}$
    """
    return total_production * employment / national_employment


class InputOutputBaseException(Exception):
    ...


def infer_sector_names(
    io_table_var: str = "full_io_table", sector_var: str = "sector_names"
):
    """If `sector_names` parameter is null, infer from `full_io_table`.

    Raises:
        AssertionError: If `sector_names` is `None` and `columns` and
            `index` are not equal.
    """

    def wrap_callable(func: Callable):
        @wraps(func)
        def wrapper_for_sector_names_calc(*args, **kwargs) -> Series:
            if sector_var not in kwargs and not len(args) > 2:
                io_table: DataFrame = (
                    kwargs[io_table_var] if io_table_var in kwargs else args[0]
                )
                if io_table.columns.equals(io_table.index):
                    logger.info(f"Inferring sectors from square {io_table_var}.")
                    kwargs[sector_var] = io_table.index
                elif set(io_table.index) & set(io_table.columns):
                    logger.warning(
                        f"Inferring sectors from overlap columns and rows in {io_table_var}."
                    )
                    kwargs[sector_var] = list(
                        ordered_iter_overlaps(io_table.columns, io_table.index)
                    )
                    if not sector_var:
                        raise InputOutputBaseException(
                            f"No overlapping `column and index labels. Try specifying `{sector_var}`."
                        )
                else:
                    raise InputOutputBaseException(
                        f"`{sector_var}` not specified and ordered overlap of column and index labels."
                    )
            return func(*args, **kwargs)

        return wrapper_for_sector_names_calc

    return wrap_callable


@dtype_wrapper("float64")
def F_i_m_scaled(
    final_demand: Series | DataFrame,
    regional_populations: Series,
    national_population: float,
) -> DataFrame:
    """Estimate the final demand of sector $m$ in region $i$.

    $F_i^{(m)} = F_*^{(m)} * P_i/P_*$
    """
    return final_demand * regional_populations / national_population


@dtype_wrapper("float64")
@infer_sector_names(sector_var="sector_row_names")
# @wrap_as_series("final_demand_column_names")
def F_i_m_scaled_by_regions(
    final_demand: DataFrame,
    regional_populations: Series,
    national_population: float,
    sector_row_names: Sequence[str] | None = None,
) -> DataFrame:
    region_dict: dict[str, DataFrame] = {
        reg: F_i_m_scaled(
            final_demand=final_demand.loc[sector_row_names],
            regional_populations=reg_pop,
            national_population=national_population,
        )
        for reg, reg_pop in regional_populations.items()
    }
    return df_dict_to_multi_index(region_dict, final_demand.columns)


@dtype_wrapper("float64")
def E_i_m_scaled(
    exports: Series | DataFrame,
    regional_employment: DataFrame,
    national_employment: Series,
) -> DataFrame:
    """Estimate exports of sector $m$ in region $i$.

    $E_i^{(m)} = E_*^{(m)} * Q_i^{(m)}/Q_*^{(m)}$
    """
    return calc_ratio(exports, national_employment, regional_employment)


@dtype_wrapper("float64")
@infer_sector_names(sector_var="sector_row_names")
# @wrap_as_series("final_demand_column_names")
def E_i_m_scaled_by_regions(
    exports: DataFrame,
    regional_employment: DataFrame,
    national_employment: Series,
    sector_row_names: Sequence[str] | None = None,
) -> DataFrame:
    region_dict: dict[str, DataFrame] = {
        reg: E_i_m_scaled(
            exports=exports.loc[sector_row_names],
            regional_employment=reg_emp,
            national_employment=national_employment,
        )
        for reg, reg_emp in regional_employment.T.items()
    }
    return df_dict_to_multi_index(region_dict, exports.columns)


@dtype_wrapper("float64")
def M_i_m_scaled(
    imports: Series, regional_populations: Series, national_population: float
) -> DataFrame:
    """Estimate imports of sector $m$ in region $i$.

    $M_i^{(m)} = M_*^{(m)} * P_i^{(m)}/P_*^{(m)}$
    """
    return imports * regional_populations / national_population


@dtype_wrapper("float64")
@infer_sector_names(sector_var="sector_row_names")
# @wrap_as_series("final_demand_column_names")
def M_i_m_scaled_by_regions(
    imports: DataFrame | Series,
    regional_populations: Series,
    national_population: float,
    sector_row_names: Sequence[str] | None = None,
    default_region_sector_labels: tuple[str, str] = (
        REGION_COLUMN_NAME,
        SECTOR_COLUMN_NAME,
    ),
) -> DataFrame | Series:
    region_dict: dict[str, DataFrame] = {
        reg: M_i_m_scaled(
            imports=imports.loc[sector_row_names],
            regional_populations=reg_pop,
            national_population=national_population,
        )
        for reg, reg_pop in regional_populations.items()
    }
    if isinstance(imports, Series):
        return series_dict_to_multi_index(region_dict, default_region_sector_labels)
    else:
        return df_dict_to_multi_index(region_dict, imports.columns)


@dtype_wrapper("float64")
@infer_sector_names(sector_var="sector_names")
@wrap_as_series("gva", "net_subsidies")
def X_m(
    full_io_table: DataFrame,
    gva: Series,
    net_subsidies: Series,
    sector_names: Sequence[str] | None = None,
) -> Series:
    """Total national production of all sectors $m$.

    $X*_m = O*_m + G*_m + S*_m$

    $O*_m$: national output of sector $m$ at base prince
    $G*_m$: national Gross Value added of sector $m$
    $S*_m$: national net subsidies of sector $m$
    """
    return (
        full_io_table.loc[sector_names, sector_names].sum()
        + gva[sector_names]
        + net_subsidies[sector_names]
    )


@dtype_wrapper("float64")
@infer_sector_names(sector_var="sector_column_names")
@wrap_as_series(
    "gva_row_names",
)
def gross_value_added(
    full_io_table: DataFrame,
    gva_row_names: Sequence[str] | str,
    sector_column_names: Sequence[str],
) -> Series:
    """Aggregate Gross Value Added (GVA) summing `full_io_table` `gva_row_names`."""
    return full_io_table.loc[gva_row_names, sector_column_names].sum("index")


@dtype_wrapper("float64")
@infer_sector_names(sector_var="sector_column_names")
@wrap_as_series("subsidy_row_names")
def S_m(
    full_io_table: DataFrame,
    subsidy_row_names: Sequence[str],
    sector_column_names: Sequence[str],
) -> Series:
    """Aggregate full_io_table rows for a passed set of investment sector columns."""
    return full_io_table.loc[subsidy_row_names, sector_column_names].sum("index")


@dtype_wrapper("float64")
@infer_sector_names(sector_var="sector_row_names")
@wrap_as_series("investment_column_names")
def I_m(
    full_io_table: DataFrame,
    investment_column_names: Sequence[str],
    sector_row_names: Sequence[str],
) -> Series:
    """Aggregate full_io_table columns for a given set of investment sector column names."""
    return full_io_table.loc[sector_row_names, investment_column_names].sum("columns")


def x_i_mn_summed(X_i_m: DataFrame, technical_coefficients: DataFrame) -> DataFrame:
    """Return sum of all total demands for good $m$ in region $i$.

    Equation 1:
        $x_i^{mn} = a_i^{mn}X_i^n$

    Equation 2:
        $X_i^{(m)} + m_i^{(m)} + M_i^{(m)} = F_i^{(m)} + e_i^{(m)} + E_i^{(m)} + \\sum_n{{a_i^{mn}X_i^n}}$

    Todo:
        * Determine if adding gva here would be helpful
    """
    return X_i_m.apply(
        lambda row: (row * technical_coefficients.T).sum(),
        axis=1,
    )


def generate_e_m_dataframe(
    E_i_m: DataFrame,
    sector_names: Iterable[str],
    region_names: Iterable[str],
    national_E: Optional[Series] = None,
    initial_p: float = INITIAL_P,
    e_i_m_column_name: str = LATEX_e_i_m,
    initial_e_column_prefix: str = INITIAL_E_COLUMN_PREFIX,
) -> DataFrame:
    """Return an $e_m$ dataframe with an intial $e_i^{(m)}$ column."""
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
    regions_df: GeoDataFrame,
    regions: Iterable[str],
    other_regions: Optional[Iterable[str]] = None,
    national_column_name: Optional[str] = None,
    distance_CRS: str = UK_EPSG_GEO_CODE,
    origin_region_column: str = CITY_COLUMN + DISTANCE_COLUMN_SUFFIX,
    destination_region_column: str = OTHER_CITY_COLUMN + DISTANCE_COLUMN_SUFFIX,
    final_distance_column: str = DISTANCE_COLUMN,
    unit_divide_conversion: float = DISTANCE_UNIT_DIVIDE,
) -> GeoDataFrame:
    """Calculate a GeoDataFrame with a Distance column between regions in metres.

    Note: This assumes the regions_df index has origin region as row.name[0],
    and destination region as row.name[].

    Todo:
        * This should be refactored for calc_transport_table
        * national_column_name should be imported
    """
    if not other_regions:
        other_regions = regions
    if not national_column_name:
        national_column_name = "UK"
    projected_regions_df = regions_df.to_crs(distance_CRS)
    region_distances: GeoDataFrame = GeoDataFrame(
        index=generate_ij_index(
            regions, other_regions, national_column_name=national_column_name
        ),
        columns=[final_distance_column],
    )
    region_distances[origin_region_column] = region_distances.apply(
        lambda row: projected_regions_df["geometry"][row.name[0]], axis=1
    )
    region_distances[destination_region_column] = region_distances.apply(
        lambda row: projected_regions_df["geometry"][row.name[1]], axis=1
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
#     regions_df: GeoDataFrame,
#     regions: Iterable[str] = TEN_UK_CITY_REGIONS,
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
    """Return a table of centroid distances between all regions."""
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


def region_and_sector_convergence(
    F_i_m: DataFrame,
    E_i_m: DataFrame,
    x_i_mn_summed: DataFrame,
    X_i_m: DataFrame,
    M_i_m: DataFrame,
    employment: DataFrame,
) -> tuple[Series, Series, Series]:
    """Enforce exogenous constraints through convergence by region and sector."""
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
    # m_i^{(m)} = e_i^{(m)} + F_i^{(m)} + E_i^{(m)} + \sum_n{a_i^{mn}X_i^n} - X_i^{(m)} - M_i^{(m)}
    # exogenous_i_m_constant = F_i^{(m)} + E_i^{(m)} + \sum_n{a_i^{mn}X_i^n} - X_i^{(m)} - M_i^{(m)}
    # convergence_by_region = Q_i/\sum_j{Q_j} * \sum_i{exogenous_i_m_constant_i}

    # Convergence element
    # c_1 = Q_i/\sum_j{Q_j} * \sum_i{exogenous_i_m_constant_i}
    # convergence_by_region = Q_i/\sum_j{Q_j} * \sum_i{exogenous_i_m_constant_i}
    # Worth checking summation of other employment (ie i != j)
    convergence_by_sector: Series = exogenous_i_m_constant.groupby("Sector").apply(
        lambda sector: employment[sector.name]
        * sector.sum()
        / employment[sector.name].sum()
        # groupby within.?
    )
    convergence_by_sector.index.names = ["Sector", "Area"]

    convergence_by_region: Series = convergence_by_sector.reorder_levels(
        ["Area", "Sector"]
    )
    convergence_by_region = convergence_by_region.reindex(exogenous_i_m_constant.index)
    net_constraint: Series = exogenous_i_m_constant - convergence_by_region
    # This accounts for economic activity outside the 3 regions included in the model enforcing convergence
    return exogenous_i_m_constant, convergence_by_region, net_constraint


def import_export_convergence(
    e_m_regions: DataFrame,
    y_ij_m: DataFrame,
    exogenous_i_m: Series,
    # employment: DataFrame,
    iterations: int = DEFAULT_IMPORT_EXPORT_ITERATIONS,
    e_i_m_symbol: str = LATEX_e_i_m,
    m_i_m_symbol: str = LATEX_m_i_m,
    y_ij_m_symbol: str = LATEX_y_ij_m,
) -> tuple[DataFrame, DataFrame]:
    """Iterate $i$ times of step 2 (eq 14, 15 18) of the spatial interaction model."""
    model_e_m: DataFrame = e_m_regions.copy()
    model_y_ij_m: DataFrame = y_ij_m.copy()

    for i in range(iterations):
        e_column: str = (
            f"{e_i_m_symbol} {i - 1}" if i > 0 else f"initial {e_i_m_symbol}"
        )

        # Equation 14 with exogenous_i_m_constant
        # Possibility I've messed up needing to sum the other employment (ie i != j)
        # m_i^{(m)} = e_i^{(m)} + exogenous_i_m_constant - convergence_by_region
        model_e_m[f"{m_i_m_symbol} {i}"] = model_e_m[e_column] + exogenous_i_m

        # Equation 15
        # y_{ij}^{(m)} = B_j^{(m)} Q_i^{(m)} m_j^{(m)} \exp(-\beta c_{ij})
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
        # e_i^{(m)} = \sum_j{y_{ij}^{(m)}}
        # Note: this section groups by City and Sector
        model_e_m[f"{e_i_m_symbol} {i}"] = (
            model_y_ij_m[f"{y_ij_m_symbol} {i}"].groupby(["City", "Sector"]).sum()
        )
    return model_e_m, model_y_ij_m


# def scale_region_var_by_national(
#     national_var: Union[float, Series],
#     national_sector_var: Union[float, Series],
#     region_var: Union[float, Series],
# ) -> Union[float, Series]:
#     return national_sector_var * region_var / national_var


# def scale_var_by_national(
#     var: Union[float, Series],
#     national_var: Union[float, Series],
#     national_portion: Union[float, Series],
# ) -> Union[float, Series]:
#     return national_proportion * var / national_var


def calc_ratio(
    a: FloatOrPandasTypes, b: FloatOrSeriesType, d: FloatOrSeriesType
) -> FloatOrPandasTypes:
    """Return $(a*d)/b$; mathematically: calc $c$ from ratio $a:b = c:d$.

    Rearrange ratio:

    $a/b = c/d$

    to solve for $c$

    $c = (a*d)/b$

    Examples:
        The `type` returned should be the same as parameter `a`. Thus: if `a` is a `float`, a `float` is returned

        >>> calc_ratio(1, 5, 10)
        2.0

        similar if `a` is a `pandas` `Series` objects

        >>> a = Series([1, 4, 5])
        >>> b = Series([2, 8, 10])
        >>> d = Series([5, 7, 9])
        >>> calc_ratio(a, b, d)
        0    2.5
        1    3.5
        2    4.5
        dtype: float64

        or `a` is a `DataFrame`

        >>> a = DataFrame({ "x": a, "y": a*3 })
        >>> calc_ratio(a, b, d)
             x     y
        0  2.5   7.5
        1  3.5  10.5
        2  4.5  13.5

        the last is equivalent to

        >>> from numpy import array
        >>> array([3, 12, 15]) * array([5, 7, 9])
        array([ 15,  84, 135])
        >>> array([15, 84, 135]) / ([2, 8, 10])
        array([ 7.5, 10.5, 13.5])

    Todo:
        * Specify returned column or sequence name (**kwargs)
    """
    if isinstance(a, DataFrame):
        return a.apply((lambda column: calc_ratio(column, b, d)))
    else:
        return a * d / b


# def gdp_per_sector(
#     io_table: DataFrame,
#     intermediate_demand_row_name: str = INTERMEDIATE_COLUMN_NAME,
#     gross_value_added_row_name: str = GROSS_VALUE_ADDED_COLUMN_NAME
# ) -> Series:
#     return io_table


def regional_io_projection(
    technical_coefficients: DataFrame, regional_output: Series
) -> DataFrame:
    """Return a regional projection of from regional data.

    Todo:
        * Test an option using diagonalise
    """
    logger.warning("Using regional_io_projection, this needs testing!")
    # return technical_coefficients * diagonalise(regional_output)
    return technical_coefficients * regional_output


# def calc_full_io_table(
#     base_io_table: DataFrame,
#     dog_leg_columns: dict[str, str] | None = None,
#     dog_leg_rows: dict[str, str] | None = None,
# ) -> DataFrame:
#     if not dog_leg_columns:
#         dog_leg_columns = {}
#     if not dog_leg_rows:
#         dog_leg_rows = {}
#     raise NotImplementedError
