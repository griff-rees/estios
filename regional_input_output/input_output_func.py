#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Final, Iterable

from geopandas import GeoDataFrame
from pandas import DataFrame, MultiIndex, Series
from plotly.graph_objects import Figure

from .uk_data.utils import CENTRE_FOR_CITIES_PATH  # IO_DOG_LEG_COEFFICIENTS,
from .uk_data.utils import (
    CITIES_TOWNS_SHAPE_PATH,
    CITY_REGIONS,
    CITY_SECTOR_EMPLOYMENT_PATH,
    JOBS_BY_SECTOR_PATH,
    NATIONAL_COLUMN_NAME,
    SECTOR_10_CODE_DICT,
    TOTAL_OUTPUT_COLUMN,
)

UK_CRS: Final[str] = "EPSG:27700"
DISTANCE_UNIT_DIVIDE: Final[float] = 1000

CITY_COLUMN: Final[str] = "City"
OTHER_CITY_COLUMN: Final[str] = "Other_City"
SECTOR_COLUMN: Final[str] = "Sector"
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
    X_i^m + m_i^m + M_i^m = F_i^m + e_i^m + E_i^m + \sum_n{a_i^{mn}X_i^n}
    """
    return X_i_m.apply(
        lambda row: (row * technical_coefficients.T).sum(),
        axis=1,
    )


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


def plot_iterations(
    df: DataFrame,
    model_variable: str,
    model_abbreviations: dict[str, str] = MODEL_APPREVIATIONS,
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
    print(plot_df.columns)
    return plot_df.transpose().plot(
        title=f'Iterations of {model_variable}s between {", ".join(region_names[:-1])} and {region_names[-1]}'
    )


def generate_i_m_index(
    i_column: Iterable[str] = CITY_REGIONS,
    m_column: Iterable[str] = SECTOR_10_CODE_DICT,
    include_national: bool = False,
    national_name: str = NATIONAL_COLUMN_NAME,
    i_column_name: str = CITY_COLUMN,
    m_column_name: str = SECTOR_COLUMN,
) -> MultiIndex:
    """Return an IM index, conditionally adding `national_name` as a region."""
    if include_national:
        i_column = list(i_column) + [national_name]
    index_tuples: list = [(i, m) for i in i_column for m in m_column]
    return MultiIndex.from_tuples(index_tuples, names=(i_column_name, m_column_name))


def generate_ij_index(
    regions: Iterable[str] = CITY_REGIONS,
    other_regions: Iterable[str] = CITY_REGIONS,
    m_column_name: str = OTHER_CITY_COLUMN,
    **kwargs,
) -> MultiIndex:
    """Wrappy around generate_i_m_index with other_regions instead of sectors."""
    return generate_i_m_index(
        regions, other_regions, m_column_name=m_column_name, **kwargs
    )


def generate_ij_m_index(
    regions: Iterable[str] = CITY_REGIONS,
    sectors: Iterable[str] = SECTOR_10_CODE_DICT,
    include_national: bool = False,
    national_name: str = NATIONAL_COLUMN_NAME,
    region_name: str = CITY_COLUMN,
    alter_prefix: str = "Other_",
) -> MultiIndex:
    """Return an IJM index, conditionally adding `national_name` as a region."""
    if include_national:
        regions = list(regions) + [national_name]
    index_tuples: list[tuple[str, str, str]] = [
        (i, j, m) for i in regions for j in regions for m in sectors if i != j
    ]
    return MultiIndex.from_tuples(
        index_tuples, names=(region_name, alter_prefix + region_name, SECTOR_COLUMN)
    )
