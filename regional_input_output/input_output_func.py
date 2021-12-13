#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger
from typing import Final, Iterable, Optional

from geopandas import GeoDataFrame
from pandas import DataFrame, MultiIndex, Series
from plotly.graph_objects import Figure

from .uk_data.utils import (
    CENTRE_FOR_CITIES_PATH,
    CITIES_TOWNS_SHAPE_PATH,
    CITY_REGIONS,
    CITY_SECTOR_EMPLOYMENT_PATH,
    JOBS_BY_SECTOR_PATH,
    NATIONAL_COLUMN_NAME,
    SECTOR_10_CODE_DICT,
    TOTAL_OUTPUT_COLUMN,
)
from .utils import (
    CITY_COLUMN,
    OTHER_CITY_COLUMN,
    SECTOR_COLUMN,
    generate_i_m_index,
    generate_ij_index,
)

logger = getLogger(__name__)

UK_CRS: Final[str] = "EPSG:27700"
DISTANCE_UNIT_DIVIDE: Final[float] = 1000

DISTANCE_COLUMN: Final[str] = "Distance"
DISTANCE_COLUMN_SUFFIX: Final[str] = "_Point"

LATEX_e_i_m: Final[str] = "e_i^m"
LATEX_m_i_m: Final[str] = "m_i^m"
LATEX_y_ij_m: Final[str] = "y_{ij}^m"

MODEL_APPREVIATIONS: Final[dict[str, str]] = {
    "export": LATEX_e_i_m,
    "import": LATEX_m_i_m,
    "flows": LATEX_y_ij_m,
}
INITIAL_E_COLUMN_PREFIX: str = "initial "

INITIAL_P: Final[float] = 0.1  # For initial e_m_iteration e calculation
DEFAULT_IMPORT_EXPORT_ITERATIONS: Final[int] = 15


def technical_coefficients(
    io_table: DataFrame,
    sectors: Iterable[str] = SECTOR_10_CODE_DICT.keys(),
    final_output_column: str = TOTAL_OUTPUT_COLUMN,
) -> DataFrame:
    """Calculate technical coefficients from IO matrix and a final output column."""
    io_matrix: DataFrame = io_table.loc[sectors, sectors]
    final_output: Series = io_table.loc[sectors][final_output_column]
    return (io_matrix / final_output).astype("float64")


def X_i_m(
    total_sales: Series, employment: DataFrame, national_employment: DataFrame
) -> DataFrame:
    """Return the total production of sector 𝑚 in city 𝑖and cache results.

    X_i^m = X_*^m * Q_i^m/Q_*^m
    """
    return total_sales * employment / national_employment


def M_i_m(
    imports: Series, employment: DataFrame, national_employment: DataFrame
) -> DataFrame:
    """Return the imports of sector 𝑚 in city 𝑖and cache results.

    M_i^m = M_*^m * Q_i^m/Q_*^m
    """
    return imports * employment / national_employment


def F_i_m(
    final_demand: Series, employment: DataFrame, national_employment: DataFrame
) -> DataFrame:
    """Return the final demand of sector 𝑚 in city 𝑖and cache results.

    F_i^m = F_*^m * Q_i^m/Q_*^m
    """
    return final_demand * employment / national_employment


def E_i_m(
    exports: Series, employment: DataFrame, national_employment: DataFrame
) -> DataFrame:
    """Return the final demand of sector 𝑚 in city 𝑖and cache results.

    E_i^m = E_*^m * Q_i^m/Q_*^m
    """
    return exports * employment / national_employment


def x_i_mn_summed(X_i_m: DataFrame, technical_coefficients: DataFrame) -> DataFrame:
    """Return sum of all total demands for good 𝑚 in city 𝑖.

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
    city_names: Iterable[str] = CITY_REGIONS,
    sector_names: Iterable[str] = SECTOR_10_CODE_DICT,
    e_i_m_column_name: str = LATEX_e_i_m,
    initial_e_column_prefix: str = INITIAL_E_COLUMN_PREFIX,
) -> DataFrame:
    """Return an e_m dataframe with an intial e_i^m column."""
    index: MultiIndex
    if national_E:
        E_i_m = E_i_m.append(national_E)
        index = generate_i_m_index(city_names, sector_names, include_national=True)
    else:
        index = generate_i_m_index(city_names, sector_names)
    initial_e_column_name: str = initial_e_column_prefix + e_i_m_column_name
    e_m_iter_df = DataFrame(index=index, columns=[initial_e_column_name])
    e_m_iter_df[initial_e_column_name] = initial_p * E_i_m.stack().astype("float64")
    return e_m_iter_df


def calc_city_distance(
    cities_df: GeoDataFrame,
    regions: Iterable[str] = CITY_REGIONS,
    other_regions: Iterable[str] = CITY_REGIONS,
    distance_CRS: str = UK_CRS,
    origin_city_column: str = CITY_COLUMN + DISTANCE_COLUMN_SUFFIX,
    destination_city_column: str = OTHER_CITY_COLUMN + DISTANCE_COLUMN_SUFFIX,
    final_distance_column: str = DISTANCE_COLUMN,
    unit_divide_conversion: float = DISTANCE_UNIT_DIVIDE,
) -> GeoDataFrame:
    """Calculate a GeoDataFrame with a Distance column between cities in metres.

    The ``rest_uk`` boolean adds a generic term for the rest of
    Note: This assumes the cities_df index has origin city as row.name[0],
    and destination city as row.name[].
    """
    projected_cities_df = cities_df.to_crs(distance_CRS)
    city_distances: GeoDataFrame = GeoDataFrame(
        index=generate_ij_index(regions, other_regions), columns=[final_distance_column]
    )
    city_distances[origin_city_column] = city_distances.apply(
        lambda row: projected_cities_df["geometry"][row.name[0]], axis=1
    )
    city_distances[destination_city_column] = city_distances.apply(
        lambda row: projected_cities_df["geometry"][row.name[1]], axis=1
    )
    city_distances[final_distance_column] = city_distances.apply(
        lambda row: row[origin_city_column].distance(row[destination_city_column])
        / unit_divide_conversion,
        axis=1,
    )
    city_distances = city_distances.drop(
        city_distances[city_distances[final_distance_column] == 0].index
    )
    return city_distances


def import_export_force_convergence(
    e_m_cities: DataFrame,
    y_ij_m: DataFrame,
    F_i_m: DataFrame,
    E_i_m: DataFrame,
    x_i_m_summed: DataFrame,
    X_i_m: DataFrame,
    M_i_m: DataFrame,
    employment: DataFrame,
    iterations: int = DEFAULT_IMPORT_EXPORT_ITERATIONS,
    e_i_m_symbol: str = LATEX_e_i_m,
    m_i_m_symbol: str = LATEX_m_i_m,
    y_ij_m_symbol: str = LATEX_y_ij_m,
) -> tuple[DataFrame, DataFrame]:
    """Iterate i times of step 2 (eq 14, 15 18) of the spatial interaction model."""
    model_e_m: DataFrame = e_m_cities.copy()
    model_y_ij_m: DataFrame = y_ij_m.copy()

    # Equation 14
    # (Rearranged equation 2)
    # Implies a constant in the current form of the model
    # m_i^m = F_i^m + e_i^m + E_i^m + \sum_n{a_i^{mn}X_i^n} - X_i^m - M_i^m
    # m_i^m = e_i^m + exogenous_i_m_constant
    exogenous_i_m_constant: Final[Series] = (
        F_i_m.stack()
        + E_i_m.stack()
        + x_i_m_summed.stack()
        - X_i_m.stack()
        - M_i_m.stack()
    )
    exogenous_i_m_constant.index.set_names(["City", "Sector"], inplace=True)

    # Convergence element
    convergence_by_sector: Series = exogenous_i_m_constant.groupby("Sector").apply(
        lambda sector: employment[sector.name]
        * sector.sum()
        / employment[sector.name].sum()
    )

    # Need to replace Area with City in future
    convergence_by_city: Series = convergence_by_sector.reorder_levels(
        ["Area", "Sector"]
    )
    convergence_by_city = convergence_by_city.reindex(exogenous_i_m_constant.index)

    # net_constraint = exogenous_i_m_constant - convergence_by_city
    # This accounts for economic activity outside the 3 cities included in the model enforcing convergence
    net_constraint = exogenous_i_m_constant - convergence_by_city

    for i in range(iterations):
        e_column: str = (
            f"{e_i_m_symbol} {i - 1}" if i > 0 else f"initial {e_i_m_symbol}"
        )

        # Equation 14
        # (Rearranged equation 2)
        # m_i^m = F_i^m + e_i^m + E_i^m + \sum_n{a_i^{mn}X_i^n} - X_i^m - M_i^m
        # m_i^m = e_i^m + exogenous_i_m_constant
        model_e_m[f"{m_i_m_symbol} {i}"] = model_e_m[e_column] + net_constraint

        # Equation 15
        # y_{ij}^m = B_j^m Q_i^m m_j^m \exp(-\beta c_{ij})
        # Note: this groups by Other City and Sector
        model_y_ij_m[f"{y_ij_m_symbol} {i}"] = model_y_ij_m.apply(
            lambda row: row["B_j^m * Q_i^m * exp(-β c_{ij})"]
            * model_e_m[f"{m_i_m_symbol} {i}"][row.name[1]][row.name[2]],
            axis=1,
        )
        logger.info("Iteration", i)
        logger.debug(model_y_ij_m[f"{y_ij_m_symbol} {i}"].head())
        logger.debug(model_y_ij_m[f"{y_ij_m_symbol} {i}"].tail())

        # Equation 18
        # e_i^m = \sum_j{y_{ij}^m}
        # Note: this section groups by City and Sector
        model_e_m[f"{e_i_m_symbol} {i}"] = (
            model_y_ij_m[f"{y_ij_m_symbol} {i}"].groupby(["City", "Sector"]).sum()
        )
    return model_e_m, model_y_ij_m


def plot_iterations(
    df: DataFrame,
    model_variable: str,
    model_abbreviations: dict[str, str] = MODEL_APPREVIATIONS,
    **kwargs,
) -> Figure:
    """Plot iterations of exports (e) or imports (m)."""
    if model_variable in model_abbreviations:
        column_char: str = model_abbreviations[model_variable]
        columns: list[str] = [col for col in df.columns.values if column_char in col]
    else:
        print(model_variable, "not implemented for plotting.")
        return
    plot_df = df[columns]
    plot_df.index = [" ".join(label) for label in plot_df.index.values]
    region_names: list[str] = list(df.index.get_level_values(0).unique().values)
    if len(region_names) < 4:
        regions_title_str = f'{", ".join(region_names[:-1])} and {region_names[-1]}'
    else:
        regions_title_str = f"{len(region_names)} Cities"
    print(plot_df.columns)
    return plot_df.transpose().plot(
        title=f"Iterations of {model_variable}s between {regions_title_str}", **kwargs
    )
