#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import wraps
from logging import getLogger
from typing import Any, Callable, Final, Iterable, Optional, Sequence, TypeAlias

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
    generate_ij_m_index,
    ordered_iter_overlaps,
    series_dict_to_multi_index,
    wrap_as_series,
)

logger = getLogger(__name__)

FloatOrPandasTypes: TypeAlias = float | Series | DataFrame
FloatOrSeriesType: TypeAlias = float | Series

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

RESIDUAL_SERIES_NAME: str = "Residual"

DEFAULT_BETA: float = 1.5


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


def set_attrs(attr_dict: dict[str, Any]):
    """Decorator for callables attributes are added to is returned.

    Args:
        attr_dict: `dict` where key is `attr` name an value is
            what is added to decorated callable returns.

    Returns:
        Wrapper to then add as decorator on functions or methods.
    """

    def wrapper(cls):
        for attr_name, attr_val in attr_dict.items():
            # def getAttr(self, attr_name=attr_name):
            #     return getattr(self, "_" + attr_name)
            # def setAttr(self, value, attr_name=attr_name):
            #     setattr(self, "_" + attr_name, value)
            # prop = property(getAttr, setAttr)
            if (
                isinstance(attr_val, Sequence)
                and len(attr_val) == 2
                and callable(attr_val[0])
            ):
                logger.debug(f"Calling {attr_val[0]} with " f"params: {attr_val[1]}")
                attr_val = attr_val[0](**attr_val[1])
            logger.debug(f"Setting {cls} with params: {attr_val[1]}")
            setattr(cls, attr_name, attr_val)
        return cls

    return wrapper


# def set_attrs(obj: object, setters: dict[str, Any])


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
    """Apply `F_i_m_scaled` to regional populations relative to national.

    Args:
        final_demand: National level Input-Output table Final Demand.
        regional_populations: Populations for each region.
        national_population: National population.
        sector_row_names: Sectors to include. By default this is
            managed by the
            `@infer_sector_names(sector_var="sector_row_names")`
            decorator.

    Returns:
        A MultiIndex DataFrame with of Final Demand for each region
        and sector with `dtype` `float64`.
    """
    region_dict: dict[str | int, DataFrame] = {
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
    """Apply `E_i_m_scaled` to regional populations relative to national.

    Args:
        exports: National level Input-Output table exports.
        regional_populations: Employment levels for each region.
        national_population: National employment level.
        sector_row_names: Sectors to include. By default this is
            managed by the
            `@infer_sector_names(sector_var="sector_row_names")`
            decorator.

    Returns:
        A MultiIndex DataFrame with of Exports for each region
        and sector with `dtype` `float64`.
    """
    region_dict: dict[str | int, DataFrame] = {
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
    """Apply `M_i_m_scaled` to regional populations relative to national.

    Args:
        imports: National level Input-Output table imports.
        regional_populations: Employment levels for each region.
        national_population: National employment level.
        sector_row_names: Sectors to include. By default this is
            managed by the
            `@infer_sector_names(sector_var="sector_row_names")`
            decorator.

    Returns:
        A MultiIndex DataFrame with of Imports for each region
        and sector with `dtype` `float64`.
    """
    region_dict: dict[str | int, DataFrame] = {
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

    Maths:
        $x_i^{(mn)} = a_i^{(mn)}X_i^{(n)}$

        $X_i^{(m)} + m_i^{(m)} + M_i^{(m)} = F_i^{(m)} + e_i^{(m)} + E_i^{(m)} + \\sum_n{{a_i^{(mn)}X_i^{(n)}}}$

    Todo:
        * Check if adding Gross Value Added (gva) would be helpful
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
    # if not national_column_name:
    #     national_column_name = "UK"
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
          the DataFrame index.
    """
    if distance_CRS:
        regions_df = regions_df.to_crs(distance_CRS)
    if region_names_column:
        regions_df.set_index(region_names_column)
    if region_names:
        regions_df[region_names]
    return distance_func(regions_df) * scaling_factor


def doubly_constrained(regions_df: DataFrame) -> DataFrame:
    """Function for calculating doubly constrained flows.

    Todo:
        * Refactor to replace with `iteration_for_AiBj`
    """
    pass


def region_and_sector_convergence(
    F_i_m: DataFrame,
    E_i_m: DataFrame,
    x_i_mn_summed: DataFrame,
    X_i_m: DataFrame,
    M_i_m: DataFrame,
    employment: DataFrame,
) -> tuple[Series, Series, Series]:
    """Enforce exogenous constraints through convergence by region and sector."""
    exogenous_i_m_constant_df: DataFrame = F_i_m + E_i_m + x_i_mn_summed - X_i_m - M_i_m
    exogenous_i_m_constant: Series = exogenous_i_m_constant_df.stack()

    exogenous_i_m_constant.index.set_names(["City", "Sector"], inplace=True)

    # Equation 14
    # (Rearranged equation 2)
    # m_i^{(m)} = e_i^{(m)} + F_i^{(m)} + E_i^{(m)} + \sum_n{a_i^{(mn)}X_i^{(n)}} - X_i^{(m)} - M_i^{(m)}
    # exogenous_i_m_constant = F_i^{(m)} + E_i^{(m)} + \sum_n{a_i^{(mn)}X_i^{(n)}} - X_i^{(m)} - M_i^{(m)}
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


@dtype_wrapper("float64")
# @infer_sector_names(sector_var="sector_row_names")
@set_attrs({"name": RESIDUAL_SERIES_NAME})
def residual_X_m(
    X_m_national: Series,
    X_i_m: DataFrame,
    sector_row_names: Sequence[str] | None = None,
) -> Series:
    """Return residual sales of `X_m_national` minus sum of `X_i_m."""
    return X_m_national - X_i_m.sum(axis="rows")


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


def A_i_m_cal(
    city_distances: DataFrame,
    city_employment: DataFrame,
    city_population: Series,
    national_column_name: str,
    national_population: Series,
    national_distance: float,
    include_national: bool = False,
    beta: float = DEFAULT_BETA,
    B_j_m_old=1,
) -> DataFrame:
    """Calculate B_j^m via the singly constrained import flow anchor (equation 16)."""
    ijm_index: MultiIndex = generate_ij_m_index(
        city_employment.index,
        city_employment.columns,
        include_national=include_national,
        national_column_name=national_column_name,
    )
    A_i_m: DataFrame = DataFrame({"P_i^m": None}, index=ijm_index)
    A_i_m["Distance"] = A_i_m.apply(
        lambda row: city_distances["Distance"][row.name[0]][row.name[1]]
        if national_column_name not in row.name
        else national_distance,
        axis=1,
    )
    if include_national:
        city_population = city_population.append(
            Series([national_population], index=[national_column_name])
        )
    A_i_m["P_i^m"] = A_i_m.apply(lambda row: city_population.loc[row.name[1]], axis=1)
    A_i_m["c_{ij}^-β"] = A_i_m["Distance"] ** (-1 * beta)
    A_i_m["P_i^m * c_{ij}^-β"] = A_i_m["P_i^m"] * A_i_m["c_{ij}^-β"]
    A_i_m["P_i^m * c_{ij}^-β"] = A_i_m.groupby(["City", "Sector"])[
        "P_i^m * c_{ij}^-β"
    ].transform("sum")
    A_i_m["B_j^m"] = B_j_m_old
    A_i_m["B_j^m * sum P_i^m *  c_{ij}^-β"] = (
        A_i_m["P_i^m * c_{ij}^-β"] * A_i_m["B_j^m"]
    )

    # Equation 16
    A_i_m["A_i^m"] = 1 / A_i_m["B_j^m * sum P_i^m *  c_{ij}^-β"]
    return A_i_m


def B_j_m_cal(
    city_distances: DataFrame,
    city_employment: DataFrame,
    national_column_name: str,
    national_employment: Series,
    national_distance: float,
    A_i_m_old=1,
    beta: float = DEFAULT_BETA,
    include_national: bool = False,
) -> DataFrame:
    """Calculate B_j^m via the singly constrained import flow anchor (equation 16)."""
    ijm_index: MultiIndex = generate_ij_m_index(
        city_employment.index,
        city_employment.columns,
        include_national=include_national,
        national_column_name=national_column_name,
    )
    B_j_m: DataFrame = DataFrame({"Q_i^m": None}, index=ijm_index)
    B_j_m["Distance"] = B_j_m.apply(
        lambda row: city_distances["Distance"][row.name[0]][row.name[1]]
        if national_column_name not in row.name
        else national_distance,
        axis=1,
    )
    B_j_m["Q_i^m"] = B_j_m.apply(
        lambda row: city_employment.loc[row.name[0]][row.name[2]]
        if f"{national_column_name}" != row.name[0]
        else national_employment[row.name[2]],
        axis=1,
    )
    B_j_m["c_{ij}^-β"] = B_j_m["Distance"] ** (-1 * beta)
    B_j_m["Q_i^m * c_{ij}^-β"] = B_j_m["Q_i^m"] * B_j_m["c_{ij}^-β"]
    B_j_m["sum Q_i^m * c_{ij}^-β"] = B_j_m.groupby(["Other_City", "Sector"])[
        "Q_i^m * c_{ij}^-β"
    ].transform("sum")
    B_j_m["A_i^m"] = A_i_m_old
    B_j_m["A_i^m * sum Q_i^m * c_{ij}^-β"] = (
        B_j_m["sum Q_i^m * c_{ij}^-β"] * B_j_m["A_i^m"]
    )

    B_j_m["B_j^m"] = 1 / B_j_m["A_i^m * sum Q_i^m * c_{ij}^-β"]
    return B_j_m


def iteration_for_AiBj(
    city_distances: DataFrame,
    city_employment: DataFrame,
    city_population: Series,
    national_column_name: str,
    national_employment: Series,
    national_distance: float,
    national_population: Series,
    beta: float = DEFAULT_BETA,
    include_national: bool = True,
    iteration_number: float = 20,
):
    """Iterate convergence for a spatial doubly constrained effect.

    Maths:
        $b_{ij}^{(m)} = K * A_{i}^{(m)} * B_{j}^{(m)} * Q_{i}^{(m)} * P_{j} * c_{ij}^{-\\beta}$

        $A_{i}^{(m)} = 1/\\sum_{j} B_{j}^{(m)}P_{j} c_{ij}^{-\\beta}$

        $B_{j}^{(m)} = 1/\\sum_{i} A_{i}^{(m)}Q_{i}^{(m)} c_{ij}^{-\\beta}$

        $K = 1/\\sum_{j}b_{ij}^{(m)}$

        $y_{ij}^{(m)} = b_{ij}^{(m)} pE_{i}^{(m)}$

        $\\sum_{j} y_{ij}^{(m)} = e_{i}^{(m)}$

    """
    A_i_m_init = A_i_m_cal(
        city_distances=city_distances,
        city_employment=city_employment,
        city_population=city_population,
        B_j_m_old=1,
        beta=beta,
        include_national=include_national,
        national_column_name=national_column_name,
        national_population=national_population,
        national_distance=national_distance,
    )
    A_i_m_res = A_i_m_init["A_i^m"]
    B_j_m_init = B_j_m_cal(
        city_distances=city_distances,
        city_employment=city_employment,
        A_i_m_old=A_i_m_res,
        beta=beta,
        include_national=include_national,
        national_column_name=national_column_name,
        national_employment=national_employment,
        national_distance=national_distance,
    )
    B_j_m_res = B_j_m_init["B_j^m"]

    for i in range(iteration_number):
        old_value = A_i_m_res * B_j_m_res
        A_i_m = A_i_m_cal(
            city_distances=city_distances,
            city_employment=city_employment,
            city_population=city_population,
            B_j_m_old=B_j_m_res,
            beta=beta,
            include_national=include_national,
            national_column_name=national_column_name,
            national_population=national_population,
            national_distance=national_distance,
        )
        A_i_m_res = A_i_m["A_i^m"]
        B_j_m = B_j_m_cal(
            city_distances=city_distances,
            city_employment=city_employment,
            A_i_m_old=A_i_m_res,
            beta=beta,
            include_national=include_national,
            national_column_name=national_column_name,
            national_employment=national_employment,
            national_distance=national_distance,
        )
        B_j_m_res = B_j_m["B_j^m"]
        new_value = A_i_m_res * B_j_m_res
        # print(abs(new_value-old_value).sum()/new_value.sum())
        # print(A_i_m_res[0])

    return A_i_m, B_j_m


def b_ij_m_cal(
    city_distances: DataFrame,
    city_employment: DataFrame,
    city_population: Series,
    A_i_m: DataFrame,
    B_j_m: DataFrame,
    national_column_name: str,
    national_population: Series,
    national_employment: Series,
    national_distance: float,
    beta: float = DEFAULT_BETA,
    include_national: bool = False,
) -> DataFrame:
    """Calculate B_j^m via the singly constrained import flow anchor (equation 16)."""
    ijm_index: MultiIndex = generate_ij_m_index(
        city_employment.index,
        city_employment.columns,
        include_national=include_national,
        national_column_name=national_column_name,
    )
    b_ij_m: DataFrame = DataFrame({"P_i^m": None}, index=ijm_index)
    b_ij_m["Distance"] = b_ij_m.apply(
        lambda row: city_distances["Distance"][row.name[0]][row.name[1]]
        if national_column_name not in row.name
        else national_distance,
        axis=1,
    )
    b_ij_m["c_{ij})^-β"] = b_ij_m["Distance"] ** (-1 * beta)
    if include_national:
        city_population = city_population.append(
            Series([national_population], index=[national_column_name])
        )
    b_ij_m["P_i^m"] = b_ij_m.apply(lambda row: city_population.loc[row.name[1]], axis=1)
    b_ij_m["Q_i^m"] = b_ij_m.apply(
        lambda row: city_employment.loc[row.name[0]][row.name[2]]
        if f"{national_column_name}" != row.name[0]
        else national_employment[row.name[2]],
        axis=1,
    )
    b_ij_m["A_i^m"] = A_i_m["A_i^m"]
    b_ij_m["B_j^m"] = B_j_m["B_j^m"]
    b_ij_m["init_b_ij^m"] = (
        b_ij_m["A_i^m"]
        * b_ij_m["B_j^m"]
        * b_ij_m["Q_i^m"]
        * b_ij_m["P_i^m"]
        * b_ij_m["c_{ij})^-β"]
    )
    b_ij_m["sum_j b_ij^m"] = b_ij_m.groupby(["Other_City", "Sector"])[
        "init_b_ij^m"
    ].transform("sum")
    b_ij_m["K"] = 1 / b_ij_m["sum_j b_ij^m"]
    b_ij_m["b_ij^m"] = b_ij_m["init_b_ij^m"] * b_ij_m["K"]
    return b_ij_m
