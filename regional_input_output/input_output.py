#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass, field
from datetime import date, datetime
from copy import deepcopy
from functools import cached_property
from os import PathLike
from typing import Final, Iterable, Optional

from geopandas import GeoDataFrame
from numpy import exp
from pandas import DataFrame, MultiIndex
from plotly.graph_objects import Figure

from .utils import (
    # IO_DOG_LEG_COEFFICIENTS,
    CENTRE_FOR_CITIES_PATH,
    CITIES_TOWNS_SHAPE_PATH,
    CITY_SECTOR_EMPLOYMENT_PATH,
    JOBS_BY_SECTOR_PATH,
    IO_TABLE_2017_EXCEL_PATH,
    IO_TABLE_TOTAL_PRODUCTION_COLUMN,
    IO_TABLE_FINAL_DEMAND_COLUMN_NAMES,
    IO_TABLE_EXPORT_COLUMN_NAMES,
    SECTOR_10_CODE_DICT,
    TOTAL_OUTPUT_COLUMN,
    AggregatedSectorDict,
    ONSInputOutputTable,
    aggregate_rows,
    load_and_join_centre_for_cities_data,
    load_region_employment,
    load_employment_by_city_and_sector,
    filter_by_region_name_and_type,
)

UK_CRS: Final[str] = "EPSG:27700"
DISTANCE_UNIT_DIVIDE: Final[float] = 1000
NATIONAL_COLUMN_NAME: Final[str] = "UK"

CITY_COLUMN: Final[str] = "City"
OTHER_CITY_COLUMN: Final[str] = "Other_City"
SECTOR_COLUMN: Final[str] = "Sector"
DISTANCE_COLUMN: Final[str] = "Distance"
DISTANCE_COLUMN_SUFFIX: Final[str] = "_Point"
CITY_POPULATION_COLUMN_NAME: Final[str] = "Q_i^m"

CITY_REGIONS: Final[dict[str, str]] = {
    "Birmingham": "West Midlands",  # BIRMINGHAM & SMETHWICK
    "Bradford": "Yorkshire and the Humber",
    "Bristol": "South West",
    "Derby": "East Midlands",
    "Leeds": "Yorkshire and the Humber",
    "Liverpool": "North West",  # LIVERPOOL & BIRKENHEAD
    "Manchester": "North West",  # SALFORD 'MANCHESTER & SALFORD
    # Skip because of name inconsistency
    # 'Newcastle upon Tyne':  'North East',  # NEWCASTLE & GATESHEAD'
    "Nottingham": "East Midlands",
    "Southampton": "South East",
    "London": "London",
}

EMPLOYMENT_QUARTER_DEC_2017: Final[date] = date(2017, 12, 1)

LATEX_e_i_m: Final[str] = "e_i^m"
LATEX_m_i_m: Final[str] = "m_i^m"
LATEX_y_ij_m: Final[str] = "y_{ij}^m"

MODEL_APPREVIATIONS: Final[dict[str, str]] = {
    "export": LATEX_e_i_m,
    "import": LATEX_m_i_m,
    "flows": LATEX_y_ij_m,
}


@dataclass
class SpatialConstrainedBaseClass:

    distances: GeoDataFrame
    employment: DataFrame
    employment_column_name: str = CITY_POPULATION_COLUMN_NAME

    @property
    def y_ij_m(self) -> DataFrame:
        """A dataframe initial conditions for model y_ij_m DataFrame."""
        raise NotImplemented("This is not implemented in the BaseClass")


@dataclass
class SinglyConstrained(SpatialConstrainedBaseClass):

    beta: float = 0.0002
    constrained_column_name: str = "B_j^m * Q_i^m * exp(-β c_{ij})"

    def __repr__(self) -> str:
        """Return base config of model."""
        return f"Singly constrained attraction β = {self.beta}"

    @property
    def _ij_m_index(self) -> MultiIndex:
        """Return sector x city x other city MultiIndex."""
        return generate_ij_m_index(self.employment.index, self.employment.columns)

    def __post_init__(self) -> None:
        """Calculate core singly constrained spatial components."""
        self.B_j_m: DataFrame = DataFrame({"Q_i^m": None}, index=self._ij_m_index)

        # Inefficient, should just be reindexing distances
        self.B_j_m["Distance"] = self.B_j_m.apply(
            lambda row: self.distances["Distance"][row.name[0]][row.name[1]], axis=1
        )
        self.B_j_m[self.employment_column_name] = self.B_j_m.apply(
            lambda row: self.employment.loc[row.name[0]][row.name[2]], axis=1
        )
        self.B_j_m["-β c_{ij}"] = -1 * self.B_j_m["Distance"] * self.beta
        self.B_j_m["exp(-β c_{ij})"] = self.B_j_m["-β c_{ij}"].apply(lambda x: exp(x))
        self.B_j_m["Q_i^m * exp(-β c_{ij})"] = (
            self.B_j_m[self.employment_column_name] * self.B_j_m["exp(-β c_{ij})"]
        )
        self.B_j_m["sum Q_i^m * exp(-β c_{ij})"] = self.B_j_m.groupby(
            ["Other_City", "Sector"]
        )["Q_i^m * exp(-β c_{ij})"].transform("sum")

        # Equation 16
        self.B_j_m["B_j^m"] = 1 / self.B_j_m["sum Q_i^m * exp(-β c_{ij})"]

    @property
    def y_ij_m(self) -> DataFrame:
        """A dataframe initial conditions for model y_ij_m DataFrame."""
        return DataFrame(
            data={
                self.employment_column_name: self.B_j_m[self.employment_column_name],
                "B_j^m": self.B_j_m["B_j^m"],
                "exp(-β c_{ij})": self.B_j_m["exp(-β c_{ij})"],
                self.constrained_column_name: (
                    self.B_j_m["B_j^m"] * self.B_j_m["Q_i^m * exp(-β c_{ij})"]
                ),
            }
        )


@dataclass
class InterRegionInputOutput:

    """Manage Inter Region input output model runs."""

    # Add these to support multiple runs
    # _start_run_time: Optional[datetime] = field(default_factory=datetime.now)
    # _end_run_time: Optional[datetime] = None

    regions: dict[str, str] = field(default_factory=lambda: deepcopy(CITY_REGIONS))
    employment_date: date = EMPLOYMENT_QUARTER_DEC_2017

    sector_aggregation: AggregatedSectorDict = field(
        default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
    )

    centre_for_cities_path: PathLike = CENTRE_FOR_CITIES_PATH
    centre_for_cities_spatial_path: PathLike = CITIES_TOWNS_SHAPE_PATH
    io_table_file_path: PathLike = IO_TABLE_2017_EXCEL_PATH
    city_sector_employment_path: PathLike = CITY_SECTOR_EMPLOYMENT_PATH
    national_employment_path: PathLike = JOBS_BY_SECTOR_PATH
    distance_unit_factor: float = DISTANCE_UNIT_DIVIDE
    final_demand_column_names: list[str] = field(
        default_factory=lambda: IO_TABLE_FINAL_DEMAND_COLUMN_NAMES
    )
    export_column_names: list[str] = field(
        default_factory=lambda: IO_TABLE_EXPORT_COLUMN_NAMES
    )
    # self._spatial_model_cls: SpatialConstrainedBaseClass = SinglyConstrained

    def __post_init__(self) -> None:
        """Initialise model based on path attributes in preparation for run."""
        self._raw_region_data: GeoDataFrame = load_and_join_centre_for_cities_data(
            city_path=self.centre_for_cities_path,
            spatial_path=self.centre_for_cities_spatial_path,
        )
        self._raw_io_table: ONSInputOutputTable = ONSInputOutputTable(
            path=self.io_table_file_path
        )
        self._national_employment: DataFrame = load_region_employment(
            path=self.national_employment_path
        )
        self._employment_by_sector_and_city: DataFrame = (
            load_employment_by_city_and_sector(path=self.city_sector_employment_path)
        )

    def __repr__(self) -> str:
        return (
            f"Input output model of {self.year}: "
            f"{len(self.sectors)} sectors, {len(self.regions)} cities"
        )

    @property
    def _ij_index(self) -> MultiIndex:
        """Return self.city x self.city MultiIndex."""
        return generate_ij_index(self.regions, self.regions)

    @property
    def _ij_m_index(self) -> MultiIndex:
        """Return self.city x self.city MultiIndex."""
        return generate_ij_m_index(self.regions, self.sectors)

    @property
    def year(self) -> int:
        """Return the year of the employment_date record."""
        return self.employment_date.year

    @property
    def sectors(self) -> Iterable[str]:
        return [*self.sector_aggregation]

    @cached_property
    def national_employment(self) -> DataFrame:
        """Return DataFrame, aggregated if sector_aggregation set."""
        if self.sector_aggregation:
            self._aggregated_national_employment: DataFrame = aggregate_rows(
                self._national_employment
            )
            return self._aggregated_national_employment.loc[str(self.employment_date)]
        else:
            return self._national_employment.loc[str(self.employment_date)]

    @cached_property
    def region_data(self) -> GeoDataFrame:
        return self._raw_region_data.loc[self.regions]

    @cached_property
    def io_table(self) -> DataFrame:
        """Return national Input-Ouput table, aggregated by self.sector_aggregation.

        Todo:
            * Check ways of skipping aggregation
        """
        if self.sector_aggregation:
            return self._raw_io_table.get_aggregated_io_table(
                sector_aggregation_dict=self.sector_aggregation
            )
        else:
            raise NotImplementedError(
                "Currently io_table requires an " "aggregation dictionary."
            )

    @cached_property
    def technical_coefficients(self) -> DataFrame:
        """Return the technical coefficients derived from self.io_table."""
        return technical_coefficients(self.io_table, self.sector_aggregation)

    @cached_property
    def employment_table(self) -> DataFrame:
        """Return employment table, aggregated if self.sector_aggregation set."""
        if self.sector_aggregation:
            self._employment_by_sector_and_city_aggregated = aggregate_rows(
                self._employment_by_sector_and_city, True
            )
            return filter_by_region_name_and_type(
                self._employment_by_sector_and_city_aggregated, self.regions
            )
        else:
            return filter_by_region_name_and_type(
                self._employment_by_sector_and_city, self.regions
            )

    @cached_property
    def X_i_m(self) -> DataFrame:
        """Return the total production of sector 𝑚 in city 𝑖and cache results.

        X_i^m = X_*^m * Q_i^m/Q_*^m
        """
        return (
            self.io_table[self.sectors].loc["Total Sales"]
            * self.employment_table
            / self.national_employment
        )

    @cached_property
    def M_i_m(self) -> DataFrame:
        """Return the imports of sector 𝑚 in city 𝑖and cache results.

        M_i^m = M_*^m * Q_i^m/Q_*^m
        """
        return (
            self.io_table[self.sectors].loc["Imports"]
            * self.employment_table
            / self.national_employment
        )

    @cached_property
    def F_i_m(self) -> DataFrame:
        """Return the final demand of sector 𝑚 in city 𝑖and cache results.

        F_i^m = F_*^m * Q_i^m/Q_*^m
        """
        return (
            self.io_table.loc[self.sectors, self.final_demand_column_names].sum(axis=1)
            * self.employment_table
            / self.national_employment
        )

    @cached_property
    def E_i_m(self) -> DataFrame:
        """Return the final demand of sector 𝑚 in city 𝑖and cache results.

        E_i^m = E_*^m * Q_i^m/Q_*^m
        """
        return (
            self.io_table.loc[self.sectors, self.export_column_names].sum(axis=1)
            * self.employment_table
            / self.national_employment
        )

    @cached_property
    def distances(self) -> GeoDataFrame:
        """Return a GeoDataFrame of all distances between cities."""
        return calc_city_distance(
            self.region_data,
            self.regions,
            self.regions,
            unit_divide_conversion=self.distance_unit_factor,
        )

    @cached_property
    def x_i_mn_summed(self) -> DataFrame:
        """Return sum of all total demands for good 𝑚 in city 𝑖.

        Equation 1:
        x_i^{mn} = a_i^{mn}X_i^n

        Equation 2:
        X_i^m + m_i^m + M_i^m = F_i^m + e_i^m + E_i^m + \sum_n{a_i^{mn}X_i^n}
        """
        return self.X_i_m.apply(
            lambda row: (row * self.technical_coefficients.T).sum(),
            axis=1,
        )

    @cached_property
    def y_ij_m(self) -> DataFrame:
        return self.spatial_interaction.y_ij_m

    @cached_property
    def spatial_interaction(self) -> SpatialConstrainedBaseClass:
        return SinglyConstrained(self.distances, self.employment_table)


@dataclass
class InterRegionInputOutputTimeSeries(InterRegionInputOutput):

    years: list[int] = field(default_factory=list)


def technical_coefficients(
    io_table: DataFrame,
    sector_codes: Optional[list[str]] = SECTOR_10_CODE_DICT.keys(),
    final_output_code: str = TOTAL_OUTPUT_COLUMN,
) -> DataFrame:
    """Calculate technical coefficients from IO matrix and a final output column."""
    io_matrix: DataFrame = io_table.loc[sector_codes, sector_codes]
    final_output: Series = io_table.loc[sector_codes][final_output_code]
    return (io_matrix / final_output).astype("float64")


def generate_im_index(
    i_column: Iterable[str] = CITY_REGIONS,
    m_column: Iterable[str] = SECTOR_10_CODE_DICT,
    include_national: bool = False,
    national_name: str = NATIONAL_COLUMN_NAME,
    i_column_name: str = CITY_COLUMN,
    m_column_name: str = SECTOR_COLUMN,
) -> MultiIndex:
    """Return an IM index, conditionally adding `national_name` as a region."""
    if include_national:
        regions = list(regions) + [national_name]
    index_tuples: list = [(i, m) for i in i_column for m in m_column]
    return MultiIndex.from_tuples(index_tuples, names=(i_column_name, m_column_name))


def generate_ij_index(
    regions: Iterable[str] = CITY_REGIONS,
    other_regions: Iterable[str] = CITY_REGIONS,
    m_column_name: str = OTHER_CITY_COLUMN,
    *args,
    **kwargs,
) -> MultiIndex:
    """Wrappy around generate_im_index with CITY_REGIONS instead of SECTORS."""
    return generate_im_index(
        regions, other_regions, m_column_name=m_column_name, *args, **kwargs
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
    # generate_ij_index needs parameters passed
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
